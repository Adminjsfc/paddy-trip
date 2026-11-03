'use strict';
// Internal service adapter + Redis/Lua + repository; history rows and MySQL are controlled fixtures.
const assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const NOW=Date.parse('2026-10-02T10:00:00Z');Date.now=()=>NOW;
process.env.REDIS_PREFIX=`jsfc:archive-simulation:${require('node:crypto').randomUUID()}`;
const cfg=require('../src/config'),{Store,ingest}=require('../src/redis-store');
const {ArchiveStore}=require('../src/archive-store'),{HistoryService}=require('../src/archive-client');
const {ArchiveRecovery,coverageFor}=require('../src/archive-recovery');
const {flushVehicle}=require('../src/write-buffer'),{shardFor}=require('../src/domain');
const {observe}=require('../src/geofence-state'),{dbFixture,format,utcText}=require('./simulation-fixtures.cjs');
const {utcDateTimeToTimestamp}=require('../src/mqtt-packet');
const {BUILD}=require('../src/worker-metrics');
const v='JH06J6168',results=[];
async function main(){
 const redis=new(require('ioredis'))(process.env.REDIS_URL,{lazyConnect:true,enableReadyCheck:false,maxRetriesPerRequest:1});await redis.connect();
 let fixture;
 const historyService={getHistory:async p=>{
  fixture.calls.push(p);
  if(fixture.serviceError)throw Object.assign(Error('Influx unavailable'),{status:fixture.serviceError});
  if(fixture.rejectCursorOnce&&p.cursor){fixture.rejectCursorOnce=false;throw Object.assign(Error('Invalid cursor'),{status:400});}
  const clock=x=>utcDateTimeToTimestamp(x.date_time);
  const from=Date.parse(p.from_date),to=Date.parse(p.to_date),all=fixture.packets.filter(x=>clock(x)>=from&&clock(x)<to).sort((a,b)=>clock(a)-clock(b));
  const i=Number(p.cursor||0),data=all.slice(i,i+p.limit),more=i+p.limit<all.length;
  return {success:true,report:'history',count:data.length,data,filters:{vehicle_number:p.vehicle_number,from_date:p.from_date,to_date_exclusive:p.to_date},
   pagination:{has_more:more,next_cursor:more?String(i+p.limit):null}};
 }};
 const acfg={enabled:true,timeoutMs:1000,maxBytes:1048576,
  pageSize:2,windowMs:6*3600000,maxLookbackMs:31*86400000,rescanMs:900000,maxPackets:10000,importSize:2};
 async function scenario(name,days,fn){
  const keys=await redis.keys(`${cfg.prefix}:*`);if(keys.length)await redis.del(...keys);
  let store=new Store(redis,shardFor(v));assert.ok(await store.acquire());let archiveStore=new ArchiveStore(store),clock=NOW;
  const lift=NOW-days*86400000-3600000;
  const trip={id:'23210',vehicle_no:v,route_id:'10',trip_status:0,level:1,status:1,lifting_date:format(lift),
   source_lat:23,source_lng:85,source_radius:0.2,destination_lat:24,destination_lng:86,destination_radius:0.2,time_duration:60};
  const f=dbFixture([trip]),logs=[];fixture={packets:[],calls:[],serviceError:0,rejectCursorOnce:false};
  const create=()=>new ArchiveRecovery({store:archiveStore,client:new HistoryService(acfg,historyService),cfg:acfg,tripCfg:cfg,now:()=>clock,log:(kind,data)=>logs.push({kind,...data})});
  let recovery=create();
  const row=(seconds,where='road',extra={})=>({vehicle_number:v,packet_type:days?'H':'L',gps_rawdata:'$PVT,ACT1',gps_fix:'1',
   latitude:where==='source'?23:where==='dest'?24:23.1,longitude:where==='dest'?86:85,
   date_time:utcText(lift+seconds*1000),_time:new Date(lift+seconds*1000).toISOString(),gps_timestamp:lift+seconds*1000+19800000,
   server_date_time:NOW,...extra});
  const vehicles=()=>new Map([[v,f.state.rows.filter(t=>t.trip_status<2)]]);
  const tick=()=>recovery.tick(vehicles());
  const ready=()=>recovery.prepare(v,vehicles().get(v));
  const recover=async()=>{for(let i=0;i<100;i++){if((await ready()).ready)return;await tick();}throw Error('Archive did not finish');};
  const flush=async()=>flushVehicle(store,f.repo,v,{archiveCoverage:coverageFor(await archiveStore.load(v))});
  const restart=async()=>{await store.release();store=new Store(redis,shardFor(v));assert.ok(await store.acquire());archiveStore=new ArchiveStore(store);recovery=create();};
  const sendLive=async(seconds,where='road')=>{const p=row(seconds,where);await ingest(redis,{vehicle_no:v,lat:p.latitude,lng:p.longitude,
   packet_datetime:lift+seconds*1000,packet_type:'L',gps_fix:'1'});};
  await fn({f,trip,lift,row,tick,ready,recover,flush,restart,sendLive,logs,advance:ms=>{clock+=ms;},
   get store(){return store;},get archiveStore(){return archiveStore;}});
  results.push({scenario:name,offline_days:days,result:'PASS',writes:f.state.writes,service_calls:fixture.calls.length,
   last_error:logs.filter(x=>x.kind==='archive_recovery_retry').at(-1)?.code||null});
 }
 try{
  for(const days of [0,1,2])await scenario('History arrives behind latest live destination; earliest GPS start/end and Redis cleanup',days,async q=>{
   fixture.packets=[q.row(10,'source'),q.row(20),q.row(30,'dest')];await q.sendLive(100,'dest');
   await q.tick();assert.equal((await q.ready()).ready,false);assert.equal(q.f.state.writes.length,0);
   await q.recover();await q.flush();assert.equal(q.f.state.rows[0].trip_date,format(q.lift+20000));
   assert.equal(q.f.state.rows[0].completion_date,format(q.lift+30000));
   assert.equal(await redis.exists(`${q.store.base}:vehicle:${v}:trip:23210:geofence`),0);
   const latest=JSON.parse(await redis.hget(q.store.keys(v).live,'packet'));assert.equal(latest.gps_timestamp,q.lift+100000);
   await q.sendLive(100,'dest');await q.flush();assert.equal(q.f.state.writes.length,2);
  });
  await scenario('Actual source visit takes precedence over earlier missing-state OUT assumption',2,async q=>{
   fixture.packets=[q.row(5),q.row(10,'source'),q.row(20),q.row(30,'dest')];await q.recover();await q.flush();
   assert.equal(q.f.state.rows[0].trip_date,format(q.lift+20000));
  });
  await scenario('Existing OUTSIDE Redis state recovers a previously missed source visit',2,async q=>{
   fixture.packets=[q.row(10,'source'),q.row(20),q.row(30,'dest')];
   const state=observe(null,q.trip,{vehicle_no:v,lat:23.1,lng:85,ts:NOW-1000},cfg);
   await redis.set(`${q.store.base}:vehicle:${v}:trip:23210:geofence`,JSON.stringify(state));
   await q.recover();await q.flush();assert.equal(q.f.state.rows[0].trip_date,format(q.lift+20000));assert.equal(q.f.state.rows[0].trip_status,2);
  });
  await scenario('Already started trip finds historical arrival strictly after start',2,async q=>{
   q.f.state.rows[0].trip_status=1;q.f.state.rows[0].trip_date=format(q.lift+20000);
   fixture.packets=[q.row(10,'dest'),q.row(20,'dest'),q.row(30,'dest')];await q.recover();await q.flush();
   assert.equal(q.f.state.rows[0].completion_date,format(q.lift+30000));assert.equal(q.f.state.writes.length,1);
  });
  await scenario('Influx outage after page one preserves Redis staging/cursor until recovery',1,async q=>{
   fixture.packets=[q.row(10,'source'),q.row(20),q.row(30,'dest')];await q.tick();fixture.serviceError=503;await q.tick();
   const state=await q.archiveStore.load(v);assert.equal(state.cursor,'2');assert.equal(state.accepted,2);assert.equal((await q.ready()).ready,false);
   fixture.serviceError=0;q.advance(300000);await q.recover();await q.flush();assert.equal(q.f.state.rows[0].trip_status,2);
  });
  await scenario('Worker restart resumes saved service cursor and partial import',1,async q=>{
   fixture.packets=[q.row(10,'source'),q.row(20),q.row(30,'dest')];await q.tick();await q.restart();
   for(let i=0;i<20&&(await q.archiveStore.load(v)).status!=='IMPORTING';i++)await q.tick();
   await q.tick();assert.equal((await q.ready()).ready,false);await q.restart();await q.recover();await q.flush();
   assert.equal(q.f.state.rows[0].completion_date,format(q.lift+30000));
  });
  await scenario('Service signing-key change retries same window without duplicate writes',1,async q=>{
   fixture.packets=[q.row(10,'source'),q.row(20),q.row(30,'dest')];await q.tick();fixture.rejectCursorOnce=true;await q.tick();
   assert.equal((await q.archiveStore.load(v)).cursor,null);q.advance(300000);await q.recover();await q.flush();assert.equal(q.f.state.writes.length,2);
  });
  await scenario('Archive/live same-time conflicting coordinates block import without overwriting queued live packet',1,async q=>{
   fixture.packets=[q.row(10,'source')];await q.sendLive(10,'road');
   for(let i=0;i<20;i++){await q.tick();const job=await q.archiveStore.load(v);if(job.last_error)break;}
   assert.match((await q.archiveStore.load(v)).last_error.message,/ARCHIVE_CONFLICTING_PACKET/);
   assert.equal((await q.store.pendingBatch(v))[0].lat,23.1);assert.equal(q.f.state.writes.length,0);
  });
  await scenario('Lost MySQL commit retries fixed historical operations with no duplicate or sibling assignment',1,async q=>{
   fixture.packets=[q.row(10,'source'),q.row(20),q.row(30,'dest')];await q.recover();q.f.state.loseCommit=true;
   await assert.rejects(q.flush());assert.ok(await q.store.loadIntent(v));q.f.state.rows.push({...q.trip,id:'999'});
   await q.flush();assert.equal(q.f.state.writes.length,2);assert.equal(q.f.state.rows[1].trip_status,0);assert.equal(await q.store.loadIntent(v),null);
  });
  await scenario('MySQL outage keeps imported history in durable inbox and writes original GPS time after recovery',2,async q=>{
   fixture.packets=[q.row(10,'source'),q.row(20),q.row(30,'dest')];await q.recover();q.f.state.unavailable=true;
   await assert.rejects(q.flush());assert.equal((await q.store.pendingBatch(v)).length,3);
   assert.equal(await redis.ttl(q.store.keys(v).pendingData),-1);
   q.f.state.unavailable=false;await q.flush();assert.equal(q.f.state.rows[0].trip_date,format(q.lift+20000));
  });
  await scenario('Wrong vehicle in history data cannot enter this vehicle queue',1,async q=>{
   fixture.packets=[q.row(10,'source',{vehicle_number:'OTHER1'})];await q.tick();
   assert.equal((await q.archiveStore.load(v)).last_error.code,'ARCHIVE_WRONG_VEHICLE');assert.equal((await q.store.pendingBatch(v)).length,0);
  });
  await scenario('Invalid header/fix are excluded while valid history remains usable',1,async q=>{
   fixture.packets=[q.row(1,'dest',{packet_header:'$ALT'}),q.row(2,'dest',{gps_fix:'0'}),q.row(10,'source'),q.row(20),q.row(30,'dest')];
   await q.recover();assert.deepEqual((await q.archiveStore.load(v)).rejected,{NOT_PVT:1,INVALID_GPS_FIX:1});
   await q.flush();assert.equal(q.f.state.rows[0].trip_date,format(q.lift+20000));
  });
  await scenario('Lease loss prevents page checkpoint and payload staging',1,async q=>{
   fixture.packets=[q.row(10,'source')];await q.ready();const before=await q.archiveStore.load(v);
   await redis.set(q.store.lease,'another-owner');await assert.rejects(q.tick(),/LEASE_LOST/);
   assert.deepEqual(await q.archiveStore.load(v),before);assert.equal(await redis.hlen(q.archiveStore.keys(v).data),0);
  });
  await scenario('Mixed UTC and legacy shifted index rows retain GPS start/end and deduplicate live history',2,async q=>{
   fixture.packets=[q.row(10,'source',{_time:new Date(q.lift+10000+19800000).toISOString()}),q.row(20),q.row(30,'dest',{_time:new Date(q.lift+30000+19800000).toISOString()})];
   await q.sendLive(100,'dest');await q.recover();await q.flush();
   assert.equal(q.f.state.rows[0].trip_date,format(q.lift+20000));assert.equal(q.f.state.rows[0].completion_date,format(q.lift+30000));
   await q.sendLive(100,'dest');await q.flush();assert.equal(q.f.state.writes.length,2);
  });
  await scenario('Version-one cursor upgrade preserves staged payloads, live inbox and GPS decisions',2,async q=>{
   fixture.packets=[q.row(10,'source'),q.row(20),q.row(30,'dest')];await q.sendLive(100,'dest');await q.tick();
   const saved=await q.archiveStore.load(v),old={...saved,version:1,lastIndexed:q.lift+19800000,retryAt:NOW+300000};
   await q.archiveStore.save(v,saved,old);
   const staged=await redis.hgetall(q.archiveStore.keys(v).data),pending=await redis.hgetall(q.store.keys(v).pendingData);
   assert.equal((await q.ready()).ready,false);
   const upgraded=await q.archiveStore.load(v);assert.equal(upgraded.version,2);assert.equal(upgraded.cursor,null);assert.equal(upgraded.retryAt,0);
   assert.deepEqual(await redis.hgetall(q.archiveStore.keys(v).data),staged);
   assert.deepEqual(await redis.hgetall(q.store.keys(v).pendingData),pending);
   await q.restart();await q.recover();await q.flush();assert.equal(q.f.state.rows[0].trip_date,format(q.lift+20000));assert.equal(q.f.state.writes.length,2);
  });
  await scenario('Lease loss during query-version upgrade leaves Redis coverage and staged payloads intact',1,async q=>{
   fixture.packets=[q.row(10,'source'),q.row(20),q.row(30,'dest')];await q.tick();
   const saved=await q.archiveStore.load(v),old={...saved,version:1};await q.archiveStore.save(v,saved,old);
   const staged=await redis.hgetall(q.archiveStore.keys(v).data);await redis.set(q.store.lease,'another-owner');
   await assert.rejects(q.ready(),/LEASE_LOST/);assert.deepEqual(await q.archiveStore.load(v),old);
   assert.deepEqual(await redis.hgetall(q.archiveStore.keys(v).data),staged);
  });
  fs.writeFileSync(path.join(__dirname,'../simulation-results/influx-recovery-results.json'),JSON.stringify({
   build:BUILD,history:'in-process service fixture through real worker adapter; native Flux covered separately',redis:'isolated emulator, actual Lua',mysql:'simulated transactional adapter',production_access:false,results},null,2)+'\n');
  console.log(`Influx recovery simulations: ${results.length}/${results.length} passed`);
 }finally{
  const keys=await redis.keys(`${cfg.prefix}:*`);if(keys.length)await redis.del(...keys);redis.disconnect();
 }
}
main().catch(e=>{console.error(e);process.exitCode=1;});
