'use strict';
// Uses the reported trip geometry/timestamps. Exit coordinates are synthetic,
// because the diagnostic did not include the original departure packet.
const assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
Date.now=()=>Date.parse('2026-10-02T05:30:00Z');
process.env.REDIS_PREFIX=`jsfc:independent-simulation:${require('node:crypto').randomUUID()}`;
const cfg=require('../src/config');
const {Store,ingest}=require('../src/redis-store');
const {mapMqttPacket}=require('../src/mqtt-packet');
const {shardFor,dbTime}=require('../src/domain');
const {observe}=require('../src/geofence-state');
const {eventId,transitionId}=require('../src/repository');
const {flushVehicle}=require('../src/write-buffer');
const {replayBatch}=require('../src/history-replay');
const {dbFixture,format,utcText}=require('./simulation-fixtures.cjs');
const v='JH02AK7064';
const a={id:'23398',vehicle_no:v,route_id:'529',trip_status:0,level:1,status:1,lifting_date:'2026-09-30 19:48:33',
 source_lat:24.290507,source_lng:86.633313,source_radius:0.2,destination_lat:24.3091682,destination_lng:86.6166546,destination_radius:0.2,time_duration:60};
const b={...a,id:'23808',route_id:'272',lifting_date:'2026-10-01 13:47:50',destination_lat:24.129632,destination_lng:86.74023};
const entered={vehicle_no:v,lat:a.source_lat,lng:a.source_lng,ts:1790851061000,packet_type:'H',gps_fix:'1'};
const exited={...entered,lat:a.source_lat+0.01,ts:1790910190000};
const arrived={...exited,lat:24.1298122,lng:86.7401123,ts:1790915162000};
const latest={...arrived,ts:1790918565000,packet_type:'L'};
const openA={...a,trip_status:1,trip_date:format(exited.ts),start_lat:exited.lat,start_lng:exited.lng};
async function main(){
 const redis=new(require('ioredis'))(process.env.REDIS_URL,{lazyConnect:true,enableReadyCheck:false,maxRetriesPerRequest:1});
 await redis.connect();const results=[];
 async function scenario(name,rows,fn){
  const keys=await redis.keys(`${cfg.prefix}:*`);if(keys.length)await redis.del(...keys);
  const store=new Store(redis,shardFor(v));assert.ok(await store.acquire());const f=dbFixture(rows);
  const send=async p=>ingest(redis,mapMqttPacket(`gps/${v}/data`,JSON.stringify({packet_header:'$PVT',date_time:utcText(p.ts),
   latitude:p.lat,longitude:p.lng,gps_fix:p.gps_fix,packet_type:p.packet_type,gps_timestamp:p.ts+19800000})));
  const flush=()=>flushVehicle(store,f.repo,v);
  const stateKey=id=>`${store.base}:vehicle:${v}:trip:${id}:geofence`;
  const replay=async()=>{const floor=dbTime(b.lifting_date,cfg.offset)-1;await store.rewind(v,floor+1);await replayBatch(store,v,floor);
   while((await store.pendingBatch(v)).length)await flush();};
  async function oldDeployment({keepEntry=true,keepExit=true}={}){
   for(const p of [keepEntry&&entered,keepExit&&exited,arrived,latest].filter(Boolean))await send(p);
   const k=store.keys(v);await redis.del(k.pendingOrder,k.pendingData);await redis.srem(`${store.base}:pending:vehicles`,v);
   for(const t of rows){let state=null;for(const p of [entered,exited,arrived,latest])state=observe(state,t,p,cfg);
    state.last_decision={reason:t.id===b.id?'OPEN_TRIP_EXISTS':'OUTSIDE_DESTINATION',blocking_trip_ids:t.id===b.id?[a.id]:[]};
    const value=JSON.stringify(state);await redis.set(stateKey(t.id),value);await redis.hset(`${store.base}:vehicle:${v}:trip-geofences`,t.id,value);}
   await redis.hset(`${store.base}:vehicle:${v}:completed-events`,eventId(exited),'1');
  }
  await fn({store,f,send,flush,replay,oldDeployment,stateKey});
  results.push({scenario:name,result:'PASS',writes:f.state.writes,trips:f.state.rows.map(t=>({id:t.id,status:t.trip_status,start_ist:t.trip_date||null,end_ist:t.completion_date||null})),pending_packets:(await store.pendingBatch(v)).length});
 }
 try{
  await scenario('Reported blocked trip recovers automatically from retained history; other trip stays open',[openA,b],async({oldDeployment,replay,f,store,stateKey})=>{
   await oldDeployment();await replay();assert.deepEqual(f.state.rows.map(t=>t.trip_status),[1,2]);
   assert.equal(f.state.rows[1].trip_date,'2026-10-02 08:33:10.000');assert.equal(f.state.rows[1].completion_date,'2026-10-02 09:56:02.000');
   assert.equal(f.state.writes.length,2);assert.equal(f.state.writes.every(w=>w.trip_id===b.id),true);
   assert.equal(await redis.exists(stateKey(a.id)),1);assert.equal(await redis.exists(stateKey(b.id)),0);
   assert.equal((await store.completedEvents(v,[transitionId(exited,b.id,'start')])).size,1);
   await replay();assert.equal(f.state.writes.length,2);
  });
  await scenario('Previously stored source crossing plus retained exit recovers without original inside packet',[openA,b],async({oldDeployment,replay,f})=>{
   await oldDeployment({keepEntry:false});await replay();assert.deepEqual(f.state.rows.map(t=>t.trip_status),[1,2]);
  });
  await scenario('Missing exit packet is not replaced with destination coordinates or a fabricated start',[openA,b],async({oldDeployment,replay,f})=>{
   await oldDeployment({keepEntry:false,keepExit:false});await replay();assert.equal(f.state.rows[1].trip_status,0);assert.equal(f.state.writes.length,0);
  });
  await scenario('Both pending trips start at shared exit and only matching destination trip completes',[a,b],async({send,flush,f})=>{
   for(const p of [entered,exited,arrived])await send(p);await flush();
   assert.deepEqual(f.state.writes.map(w=>[w.kind,w.trip_id]),[['start',a.id],['start',b.id],['end',b.id]]);
   assert.deepEqual(f.state.rows.map(t=>t.trip_status),[1,2]);
  });
  await scenario('Identical live and historical packets do not repeat trip-specific writes',[a,b],async({send,flush,f})=>{
   for(const p of [entered,exited,arrived]){await send(p);await flush();await send({...p,packet_type:'L'});await flush();}
   assert.equal(f.state.writes.length,3);
  });
  await scenario('Lost commit response after two starts retries only the fixed targets',[a,b],async({send,flush,f,store})=>{
   await send(entered);await flush();await send(exited);f.state.loseCommit=true;await assert.rejects(flush());
   assert.equal((await store.loadIntent(v)).operations.length,2);f.state.rows.push({...a,id:'999'});
   await flush();assert.equal(f.state.writes.length,2);assert.equal(f.state.rows[2].trip_status,0);
  });
  await scenario('Connection outage during blocked-history recovery retains packets then completes',[openA,b],async({oldDeployment,replay,flush,f,store})=>{
   await oldDeployment();f.state.unavailable=true;await assert.rejects(replay());assert.ok((await store.pendingBatch(v)).length);
   f.state.unavailable=false;await flush();assert.deepEqual(f.state.rows.map(t=>t.trip_status),[1,2]);
  });
  await scenario('Trip added after another trip used the same departure has independent eligibility',[a],async({send,flush,replay,f})=>{
   for(const p of [entered,exited,arrived])await send(p);await flush();assert.equal(f.state.rows[0].trip_status,1);
   f.state.rows.push(structuredClone(b));await replay();assert.deepEqual(f.state.rows.map(t=>t.trip_status),[1,2]);
   assert.equal(f.state.writes.length,3);
  });
  fs.writeFileSync(path.join(__dirname,'..','simulation-results','independent-trip-results.json'),JSON.stringify({
   mysql:'simulated',mqtt:'real parser; no broker connection',redis:'isolated emulator',
   input_note:'Source/destination geometry and GPS timestamps from supplied diagnostic. Source/exit coordinates are representative test fixtures; original departure packet was not provided.',results},null,2)+'\n');
  console.log(`Independent-trip simulations: ${results.length}/${results.length} passed`);
 }finally{const keys=await redis.keys(`${cfg.prefix}:*`);if(keys.length)await redis.del(...keys);redis.disconnect();}
}
main().catch(e=>{console.error(e);process.exitCode=1;});
