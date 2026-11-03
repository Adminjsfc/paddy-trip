'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const {ArchiveRecovery,coverageFor}=require('../src/archive-recovery');
const {dbFixture,format,utcText}=require('../scripts/simulation-fixtures.cjs');
const {observe}=require('../src/geofence-state');
const {utcDateTimeToTimestamp}=require('../src/mqtt-packet');
const tripCfg=require('../src/config');
const now=Date.parse('2026-10-02T10:00:00Z'),lifting=now-2*86400000,v='JH06J6168';
const trip={id:'23210',vehicle_no:v,route_id:'10',trip_status:0,level:1,status:1,lifting_date:format(lifting),
 source_lat:23,source_lng:85,source_radius:0.2,destination_lat:24,destination_lng:86,destination_radius:0.2,time_duration:60};
const config={enabled:true,windowMs:6*3600000,maxLookbackMs:31*86400000,rescanMs:900000,maxPackets:10000,importSize:2};
const gps=(offset,where='road',extra={})=>({vehicle_number:v,latitude:where==='source'?23:where==='dest'?24:23.1,
 longitude:where==='dest'?86:85,packet_type:'H',gps_rawdata:'$PVT,ACT1',gps_fix:'1',date_time:utcText(lifting+offset),
 _time:new Date(lifting+offset).toISOString(),...extra});
class MemoryArchive {
 job=null;staged=new Map();queue=new Map();failSave=false;
 load=async()=>this.job&&structuredClone(this.job);
 start=async(v,j)=>{this.job=structuredClone(j);this.staged.clear();};
 save=async(v,old,next,packets=[])=>{
  if(this.failSave){this.failSave=false;throw Error('Redis unavailable');}
  assert.equal(this.job.id,old.id);
  for(const p of packets){const old=this.staged.get(p.ts);if(old&&(old.lat!==p.lat||old.lng!==p.lng))throw Error('ARCHIVE_CONFLICTING_PACKET');}
  packets.forEach(p=>this.staged.set(p.ts,p));this.job=structuredClone(next);
 };
 importBatch=async(v,j,n)=>{for(const [ts,p]of [...this.staged].sort((a,b)=>a[0]-b[0]).slice(0,n)){this.queue.set(ts,p);this.staged.delete(ts);}return this.staged.size;};
 remove=async()=>{this.job=null;this.staged.clear();};
}
function fixture(packets=[],rows=[trip]){
 const store=new MemoryArchive(),f=dbFixture(rows),calls=[];let clock=now,fail=false;
 const client={page:async(v,from,to,cursor)=>{
  calls.push({from,to,cursor});if(fail)throw Object.assign(Error('Influx unavailable'),{code:'ARCHIVE_INFLUX_ERROR',status:503});
  const clock=p=>utcDateTimeToTimestamp(p.date_time);
  const all=packets.filter(p=>clock(p)>=from&&clock(p)<to).sort((a,b)=>clock(a)-clock(b));
  const index=Number(cursor||0),data=all.slice(index,index+2),more=index+2<all.length;
  return {data,pagination:{has_more:more,next_cursor:more?String(index+2):null}};
 }};
 const make=()=>new ArchiveRecovery({store,client,cfg:config,tripCfg,now:()=>clock});let recovery=make();
 const vehicles=()=>new Map([[v,f.state.rows.filter(t=>t.trip_status<2)]]);
 return {store,f,calls,get recovery(){return recovery;},set fail(value){fail=value;},advance:n=>{clock+=n;},restart:()=>{recovery=make();},
  tick:()=>recovery.tick(vehicles()),ready:()=>recovery.prepare(v,vehicles().get(v)),
  recover:async()=>{for(let i=0;i<100;i++){const ready=await recovery.prepare(v,vehicles().get(v));if(ready.ready)return ready;await recovery.tick(vehicles());}throw Error('Not ready');},
  flush:async(geofences={})=>f.repo.transitionBatch([...store.queue.values()],{geofences,archiveCoverage:coverageFor(store.job),save:async()=>{}})
 };
}
test('complete 2-day archive recovery starts at source exit and ends at destination GPS time',async()=>{
 const q=fixture([gps(0,'source'),gps(60000),gps(120000,'dest')]);await q.recover();await q.flush();
 assert.equal(q.f.state.rows[0].trip_date,format(lifting+60000));assert.equal(q.f.state.rows[0].completion_date,format(lifting+120000));
 assert.equal(q.store.job.evidence['23210'].destination_first_inside.gps_timestamp,lifting+120000);
});
test('earliest real source-OUT after dispatch starts even if later source-IN history exists',async()=>{
 const q=fixture([gps(1000),gps(2000,'source'),gps(3000),gps(4000,'dest')]);await q.recover();await q.flush();
 assert.equal(q.f.state.rows[0].trip_date,format(lifting+1000));
});
test('nearest point outside radius cannot complete a started trip',async()=>{
 const q=fixture([gps(1000),gps(2000)], [{...trip,trip_status:1,trip_date:format(lifting)}]);await q.recover();await q.flush();
 assert.equal(q.f.state.rows[0].trip_status,1);assert.equal(q.store.job.evidence['23210'].destination_nearest.inside,false);
});
test('existing OUTSIDE Redis state recovers source visit and actual exit from old history',async()=>{
 const q=fixture([gps(1000,'source'),gps(2000),gps(3000,'dest')]);await q.recover();
 const latest=observe(null,trip,{vehicle_no:v,lat:23.1,lng:85,ts:now-1000},tripCfg);
 await q.flush({'23210':latest});assert.equal(q.f.state.rows[0].trip_status,2);assert.equal(q.f.state.rows[0].trip_date,format(lifting+2000));
});
test('missing source history keeps the explicitly authorized missing-state fallback',async()=>{
 const q=fixture([gps(1000),gps(2000,'dest')]);await q.recover();const r=await q.flush();
 assert.equal(r.events[0].start_basis,'SOURCE_OUT_AFTER_DISPATCH');assert.equal(q.f.state.rows[0].trip_date,format(lifting+1000));
});
test('started trip scans from its saved start and ignores destination packets at/before start',async()=>{
 const start=lifting+60000,q=fixture([gps(30000,'dest'),gps(60000,'dest'),gps(90000,'dest')], [{...trip,trip_status:1,trip_date:format(start)}]);
 await q.recover();await q.flush();assert.equal(q.calls[0].from,start);assert.equal(q.f.state.rows[0].completion_date,format(lifting+90000));
});
test('paginated history remains staged until the complete window is recovered',async()=>{
 const q=fixture([gps(1000,'source'),gps(2000),gps(3000,'dest')]);await q.tick();
 assert.equal(q.store.job.cursor,'2');assert.equal(q.store.queue.size,0);assert.equal((await q.ready()).ready,false);
 q.restart();await q.recover();await q.flush();assert.equal(q.f.state.rows[0].trip_status,2);
});
test('history outage preserves cursor and staged data, and cannot release live queue early',async()=>{
 const q=fixture([gps(1000,'source'),gps(2000),gps(3000,'dest')]);await q.tick();q.fail=true;await q.tick();
 assert.equal(q.store.job.cursor,'2');assert.equal(q.store.staged.size,2);assert.equal((await q.ready()).ready,false);
 assert.equal(q.f.state.writes.length,0);q.fail=false;q.advance(300000);await q.recover();await q.flush();assert.equal(q.f.state.rows[0].trip_status,2);
});
test('failed Redis page save does not advance service cursor',async()=>{
 const q=fixture([gps(1000,'source'),gps(2000),gps(3000,'dest')]);await q.ready();q.store.failSave=true;await q.tick();
 assert.equal(q.store.job.cursor,null);assert.equal(q.store.staged.size,0);q.advance(300000);await q.recover();assert.equal(q.store.queue.size,3);
});
test('late archive inserts are discovered by a fresh periodic scan',async()=>{
 const packets=[gps(1000)],q=fixture(packets,[{...trip,trip_status:1,trip_date:format(lifting)}]);await q.recover();await q.flush();
 packets.push(gps(2000,'dest'));q.advance(config.rescanMs+1);await q.recover();await q.flush();
 assert.equal(q.f.state.rows[0].completion_date,format(lifting+2000));
});
test('successful empty history can release valid live processing, but an error cannot',async()=>{
 const q=fixture();await q.recover();assert.equal(q.store.job.status,'READY');assert.equal(q.store.job.accepted,0);assert.equal(q.f.state.writes.length,0);
});
test('older-than-configured trip reports a range blocker without silently truncating history',async()=>{
 const q=fixture([], [{...trip,lifting_date:format(now-40*86400000)}]);await q.tick();
 assert.equal(q.store.job.status,'BLOCKED');assert.equal(q.store.job.last_error.code,'ARCHIVE_RANGE_LIMIT');assert.equal(q.calls.length,0);
});
test('increasing the configured range releases a previously blocked archive job',async()=>{
 const q=fixture([], [{...trip,lifting_date:format(now-40*86400000)}]);await q.tick();
 const old=q.recovery.cfg;q.recovery.cfg={...old,maxLookbackMs:60*86400000};
 await q.tick();assert.equal(q.store.job.status,'SCANNING');assert.equal(q.calls.length,1);
});
test('timestamp indexing mismatch retains staged state and blocks writes',async()=>{
 const q=fixture([gps(1000,'source',{date_time:utcText(lifting+19801000)})]);await q.tick();
 assert.equal(q.store.job.last_error.code,'ARCHIVE_TIME_MISMATCH');assert.equal((await q.ready()).ready,false);assert.equal(q.store.queue.size,0);
});
test('primary row discovered after coverage is locked requires recovery and no SQL UPDATE',async()=>{
 const q=fixture([gps(1000)]);await q.recover();q.f.state.rows.push({...trip,id:'999'});
 await assert.rejects(q.flush(),{code:'ARCHIVE_RECOVERY_REQUIRED'});assert.equal(q.f.state.writes.length,0);
});
test('completed trips are removed from archive jobs and never searched again',async()=>{
 const q=fixture([], [{...trip,trip_status:2}]);await q.tick();assert.equal(q.store.job,null);assert.equal(q.calls.length,0);
});
test('worker retry persists sanitized error details without advancing cursor or releasing queued history',async()=>{
 const q=fixture([gps(1000,'source'),gps(2000),gps(3000,'dest')]);await q.tick();
 const logs=[];q.recovery.log=(kind,data)=>logs.push({kind,...data});
 const diagnostic={name:'Error',code:'ECONNRESET',message:'connection reset',query_phase:'details',page_size:200};
 q.recovery.client.page=async()=>{throw Object.assign(Error('Influx history query failed; retry retained'),{code:'ARCHIVE_INFLUX_ERROR',diagnostic,status:503});};
 await q.tick();
 assert.equal(q.store.job.cursor,'2');assert.equal(q.store.staged.size,2);assert.equal(q.store.queue.size,0);
 assert.equal(q.store.job.last_error.status,503);assert.deepEqual(q.store.job.last_error.diagnostic,diagnostic);
 assert.deepEqual(logs.find(x=>x.kind==='archive_recovery_retry').error.diagnostic,diagnostic);
 assert.equal((await q.ready()).ready,false);assert.equal(q.f.state.writes.length,0);
});
test('mixed UTC and shifted indexes start/end a 2-day offline trip at actual GPS timestamps',async()=>{
 const shifted=(offset,where)=>gps(offset,where,{_time:new Date(lifting+offset+19800000).toISOString()});
 const q=fixture([shifted(0,'source'),gps(60000),shifted(120000,'dest')]);
 await q.recover();await q.flush();
 assert.equal(q.f.state.rows[0].trip_date,format(lifting+60000));
 assert.equal(q.f.state.rows[0].completion_date,format(lifting+120000));
 assert.equal(q.store.job.version,2);
});
test('old cursor migration rewinds coverage and retains staged and pending packets',async()=>{
 const q=fixture([gps(1000,'source'),gps(2000),gps(3000,'dest')]);await q.tick();
 const id=q.store.job.id;q.store.job.version=1;q.store.job.lastIndexed=lifting+19800000;
 q.store.job.retryAt=now+300000;q.store.job.last_error={code:'ARCHIVE_TIME_MISMATCH'};
 q.store.queue.set(lifting+500,{ts:lifting+500,lat:23,lng:85});
 const before=JSON.stringify([...q.store.staged]);
 const ready=await q.ready();
 assert.equal(ready.ready,false);assert.equal(q.store.job.id,id);assert.equal(q.store.job.version,2);
 assert.equal(q.store.job.cursor,null);assert.equal(q.store.job.windowFrom,lifting);
 assert.equal(q.store.job.retryAt,0);assert.equal(q.store.job.last_error,null);
 assert.equal(JSON.stringify([...q.store.staged]),before);assert.equal(q.store.queue.size,1);
 await q.recover();assert.equal(q.store.queue.size,4);
});
test('failed query-version migration leaves the prior Redis job and staged packets intact',async()=>{
 const q=fixture([gps(1000,'source'),gps(2000),gps(3000,'dest')]);await q.tick();
 q.store.job.version=1;const before=JSON.stringify(q.store.job),staged=JSON.stringify([...q.store.staged]);
 q.store.failSave=true;await assert.rejects(q.ready(),/Redis unavailable/);
 assert.equal(JSON.stringify(q.store.job),before);assert.equal(JSON.stringify([...q.store.staged]),staged);
 await q.ready();assert.equal(q.store.job.version,2);
});
test('old READY coverage must rescan, but current READY coverage stays ready',async()=>{
 const q=fixture([gps(1000)]);await q.recover();q.store.job.version=1;
 const count=q.store.queue.size;
 assert.equal((await q.ready()).ready,false);assert.equal(q.store.queue.size,count);
 await q.recover();const id=q.store.job.id;
 assert.equal((await q.ready()).ready,true);assert.equal(q.store.job.id,id);
});
