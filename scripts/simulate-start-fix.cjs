'use strict';
// Actual MQTT mapper, Redis commands/Lua and Repository SQL; simulated MySQL only.
const assert=require('node:assert/strict'), fs=require('node:fs'), path=require('node:path');
const NOW=Date.parse('2026-10-02T04:30:00Z');Date.now=()=>NOW;
process.env.REDIS_PREFIX=`jsfc:start-fix:${require('node:crypto').randomUUID()}`;
const cfg=require('../src/config');
const {Store,ingest}=require('../src/redis-store');
const {mapMqttPacket}=require('../src/mqtt-packet');
const {shardFor}=require('../src/domain');
const {flushVehicle}=require('../src/write-buffer');
const {replayBatch}=require('../src/history-replay');
const {dbFixture,format,utcText}=require('./simulation-fixtures.cjs');
const v='JH10V7046';
async function main(){
 const redis=new(require('ioredis'))(process.env.REDIS_URL,{lazyConnect:true,enableReadyCheck:false,maxRetriesPerRequest:1});
 await redis.connect();const results=[];
 async function scenario(name,days,fn){
  const keys=await redis.keys(`${cfg.prefix}:*`);if(keys.length)await redis.del(...keys);
  const store=new Store(redis,shardFor(v));assert.ok(await store.acquire());
  const lift=NOW-days*86400000-3600000;
  const trip={id:'22777',vehicle_no:v,route_id:'10',trip_status:0,level:1,status:1,
   lifting_date:format(lift),source_lat:'23.85851100',source_lng:'86.45655700',source_radius:'0.2',
   destination_lat:'23.81187300',destination_lng:'86.23693500',destination_radius:'0.2',time_duration:'43.00'};
  const f=dbFixture([trip]);
  const stateKey=`${store.base}:vehicle:${v}:trip:${trip.id}:geofence`;
  const hashKey=`${store.base}:vehicle:${v}:trip-geofences`;
  async function send(seconds,point='road'){
   const coords=point==='source'?[23.8582687,86.456604]:point==='destination'?[23.811873,86.236935]:[23.862,86.456604];
   const ts=lift+seconds*1000;
   return ingest(redis,mapMqttPacket(`gps/${v}/data`,JSON.stringify({packet_header:'$PVT',latitude:coords[0],longitude:coords[1],
    gps_fix:'1',packet_type:days?'H':'L',date_time:utcText(ts),gps_timestamp:ts+19800000,server_date_time:NOW})));
  }
  const flush=()=>flushVehicle(store,f.repo,v);
  const replay=async()=>{await store.rewind(v,lift);await replayBatch(store,v,lift-1);while((await store.pendingBatch(v)).length)await flush();};
  await fn({store,f,trip,lift,stateKey,hashKey,send,flush,replay});
  results.push({scenario:name,offline_days:days,result:'PASS',writes:f.state.writes,trip_status:f.state.rows.map(t=>({id:t.id,status:t.trip_status})),pending_packets:(await store.pendingBatch(v)).length});
 }
 try{
  for(const days of [0,1,2])await scenario('27 m source IN -> 388 m OUT -> destination IN; GPS writes and cleanup',days,async({send,flush,f,lift,stateKey,hashKey})=>{
   await send(-60,'source');await flush();assert.equal(JSON.parse(await redis.get(stateKey)).journey_state,'IN_SOURCE');
   await send(60);await flush();assert.equal(f.state.rows[0].trip_status,1);assert.equal(f.state.rows[0].trip_date,format(lift+60000));
   assert.equal(JSON.parse(await redis.get(stateKey)).journey_state,'IN_TRIP');
   await send(60);await flush();assert.equal(f.state.writes.length,1);
   await send(120,'destination');await flush();assert.equal(f.state.rows[0].completion_date,format(lift+120000));
   assert.equal(await redis.exists(stateKey),0);assert.equal(await redis.hexists(hashKey,'22777'),0);
   await send(120,'destination');await flush();assert.equal(f.state.writes.length,2);
  });
  await scenario('Individual vehicle-trip IN works even if hash index field is absent',0,async({send,flush,f,hashKey})=>{
   await send(1,'source');await flush();await redis.hdel(hashKey,'22777');
   await send(60);await flush();assert.equal(f.state.rows[0].trip_status,1);
  });
  await scenario('Individual IN is used instead of stale OUT in lookup hash',0,async({send,flush,f,hashKey})=>{
   await send(1,'source');await flush();const state=JSON.parse(await redis.hget(hashKey,'22777'));
   state.source_status='OUTSIDE';await redis.hset(hashKey,'22777',JSON.stringify(state));
   await send(60);await flush();assert.equal(f.state.rows[0].trip_status,1);
  });
  for(const days of [1,2])await scenario('Trip discovered after packets: replay includes last point before lifting',days,async({send,flush,f,trip,lift,replay})=>{
   f.state.rows=[];await send(-60,'source');await send(60);await send(120,'destination');await flush();
   f.state.rows=[structuredClone(trip)];await replay();assert.equal(f.state.rows[0].trip_status,2);
   assert.equal(f.state.rows[0].trip_date,format(lift+60000));assert.equal(f.state.rows[0].completion_date,format(lift+120000));
  });
  for(const days of [1,2])await scenario('Missing-state fallback completes before late source history; late entry cannot duplicate writes',days,async({send,flush,f,replay})=>{
   await send(60);await send(120,'destination');await flush();assert.equal(f.state.rows[0].trip_status,2);
   await send(-60,'source');await flush();await replay();assert.equal(f.state.rows[0].trip_status,2);assert.equal(f.state.writes.length,2);
  });
  for(const days of [0,1,2])await scenario('Missing Redis trip state: first OUT starts; later destination IN ends and cleans individual state',days,async({send,flush,f,lift,stateKey,hashKey})=>{
   assert.equal(await redis.exists(stateKey),0);
   await send(60);await flush();const s=JSON.parse(await redis.get(stateKey));
   assert.equal(s.source_exit_basis,'MISSING_REDIS_STATE');assert.equal(s.source_entered_at,null);
   assert.equal(f.state.rows[0].trip_date,format(lift+60000));
   await send(60);await flush();assert.equal(f.state.writes.length,1);
   await send(120,'destination');await flush();assert.equal(f.state.rows[0].completion_date,format(lift+120000));
   assert.equal(await redis.exists(stateKey),0);assert.equal(await redis.hexists(hashKey,'22777'),0);
   await send(120,'destination');await flush();assert.equal(f.state.writes.length,2);
  });
  await scenario('Pre-lifting OUT preserves explicit assumption across worker restart',0,async({send,flush,f,store,stateKey})=>{
   await send(-60);await flush();assert.equal(f.state.writes.length,0);
   assert.equal(JSON.parse(await redis.get(stateKey)).source_assumption_pending,true);
   await store.release();const restarted=new Store(redis,shardFor(v));assert.ok(await restarted.acquire());
   await send(60);await flushVehicle(restarted,f.repo,v);assert.equal(f.state.rows[0].trip_status,1);
  });
  await scenario('Worker recreation preserves Redis IN source',0,async({send,flush,f,store})=>{
   await send(1,'source');await flush();await store.release();
   const restarted=new Store(redis,shardFor(v));assert.ok(await restarted.acquire());
   await send(60);await flushVehicle(restarted,f.repo,v);assert.equal(f.state.rows[0].trip_status,1);
  });
  await scenario('Lost MySQL commit response retries same start without duplicate update',0,async({send,flush,f,store})=>{
   await send(60);f.state.loseCommit=true;
   await assert.rejects(flush(),/Commit response lost/);assert.ok(await store.loadIntent(v));
   await flush();assert.equal(f.state.writes.length,1);assert.equal(await store.loadIntent(v),null);
  });
  await scenario('MySQL unavailable keeps GPS in Redis then starts after recovery',0,async({send,flush,f,store})=>{
   await send(60);f.state.unavailable=true;
   await assert.rejects(flush());assert.equal((await store.pendingBatch(v)).length,1);
   f.state.unavailable=false;await flush();assert.equal(f.state.rows[0].trip_status,1);
  });
  const out=path.join(__dirname,'..','simulation-results');
  fs.writeFileSync(path.join(out,'start-fix-results.json'),JSON.stringify({mysql:'simulated',mqtt:'mapper invoked directly; no production broker',redis:'isolated emulator',results},null,2)+'\n');
  console.log(`Start-fix regression simulations: ${results.length}/${results.length} passed`);
 }finally{
  const keys=await redis.keys(`${cfg.prefix}:*`);if(keys.length)await redis.del(...keys);redis.disconnect();
 }
}
main().catch(e=>{console.error(e);process.exitCode=1;});
