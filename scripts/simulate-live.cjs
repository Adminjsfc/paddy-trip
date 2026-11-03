'use strict';
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
let clock = Date.parse('2026-09-30T04:30:00Z');
Date.now = () => clock;
process.env.REDIS_PREFIX = `jsfc:live-simulation:${require('node:crypto').randomUUID()}`;
const cfg = require('../src/config');
const { Store, ingest } = require('../src/redis-store');
const { mapMqttPacket } = require('../src/mqtt-packet');
const { shardFor, distance } = require('../src/domain');
const { flushVehicle } = require('../src/write-buffer');
const { format, utcText, dbFixture } = require('./simulation-fixtures.cjs');
const vehicle = 'JH16B5498';
async function main() {
 const redis = new (require('ioredis'))(process.env.REDIS_URL || 'redis://127.0.0.1:6379/0',
  {lazyConnect:true,enableReadyCheck:false,maxRetriesPerRequest:1});
 await redis.connect();
 const results=[];
 async function scenario(name,fn) {
  const keys=await redis.keys(`${cfg.prefix}:*`); if(keys.length) await redis.del(...keys);
  clock=Date.parse('2026-09-30T04:30:00Z');
  const lifting=clock;
  const store=new Store(redis,shardFor(vehicle)); assert.ok(await store.acquire());
  const base={id:'1',route_id:'10',vehicle_no:vehicle,status:1,level:1,trip_status:0,
   lifting_date:format(lifting),trip_date:null,source_lat:23,source_lng:85,source_radius:0.2,
   destination_lat:24,destination_lng:86,destination_radius:0.2,time_duration:120};
  const f=dbFixture([base]);
  let received=0;
  const send=async (seconds,point='road',extra={})=>{
   const ts=lifting+seconds*1000;
   clock=Math.max(clock,ts+1000); // Advance simulated receive time with each live point.
   const coords=Array.isArray(point)?point:point==='source'?[23,85]:point==='destination'?[24,86]:[23.1,85.1];
   const wire={packet_header: '$PVT', latitude:coords[0],longitude:coords[1],packet_type:'L',gps_fix:'1',
    date_time:utcText(ts),gps_timestamp:ts,server_date_time:clock,is_live:true,...extra};
   const result=await ingest(redis,mapMqttPacket(`gps/${vehicle}/data`,JSON.stringify(wire)));
   received++; return result;
  };
  const flush=()=>flushVehicle(store,f.repo,vehicle);
  const drain=async()=>{for(let n=0;n<100;n++){if(!(await store.pendingBatch(vehicle)).length)return;await flush();}throw Error('Queue did not drain');};
  await send(-600,'source'); await flush();
  await fn({send,flush,drain,store,f,base,lifting,redis});
  for (const t of f.state.rows.filter(t => t.trip_status === 2)) {
    assert.equal(await redis.exists(`${store.base}:vehicle:${vehicle}:trip:${t.id}:geofence`), 0);
    assert.equal((await store.loadGeofences(vehicle))[String(t.id)], undefined);
  }
  results.push({scenario:name,result:'PASS',packets_received:received,writes:f.state.writes,
   trips:f.state.rows.map(t=>({id:t.id,status:t.trip_status,start_ist:t.trip_date,end_ist:t.completion_date||null})),
   pending_packets:(await store.pendingBatch(vehicle)).length});
 }
 try {
  await scenario('Full live journey: source -> road -> destination',async({send,flush,f,lifting})=>{
   await send(5,'source');await flush();assert.equal(f.state.rows[0].trip_status,0);
   await send(60);await flush();assert.equal(f.state.rows[0].trip_status,1);
   await send(3600,'destination');await flush();assert.equal(f.state.rows[0].trip_status,2);
   assert.equal(f.state.rows[0].trip_date,format(lifting+60000));assert.equal(f.state.rows[0].completion_date,format(lifting+3600000));
  });
  await scenario('Repeated source points do not start',async({send,flush,f})=>{
   for(const s of [5,10,20]){await send(s,'source');await flush();}assert.equal(f.state.writes.length,0);
  });
  await scenario('At lifting time outside source stays pending',async({send,flush,f})=>{
   await send(0);await flush();assert.equal(f.state.rows[0].trip_status,0);
  });
  await scenario('Repeated road points start only once',async({send,flush,f})=>{
   for(const s of [1,2,3]){await send(s);await flush();}assert.equal(f.state.writes.length,1);
  });
  await scenario('Repeated destination points end only once',async({send,flush,f})=>{
   await send(1);await flush();for(const s of [60,61,62]){await send(s,'destination');await flush();}assert.equal(f.state.writes.length,2);
  });
  await scenario('Duplicate same-second redelivery has one effect',async({send,flush,f})=>{
   await send(1);await flush();await send(1);await flush();assert.equal(f.state.writes.length,1);
  });
  await scenario('Same-second conflicting location is rejected',async({send,flush,f})=>{
   await send(1);await assert.rejects(send(1,'destination'),/Conflicting/);await flush();assert.equal(f.state.writes.length,1);
  });
  await scenario('Source boundary counts as inside',async({send,flush,f})=>{
   const p=[23.001,85];f.state.rows[0].source_radius=distance(23,85,...p)/1000;
   await send(1,p);await flush();assert.equal(f.state.rows[0].trip_status,0);
  });
  await scenario('Destination boundary counts as inside',async({send,flush,f})=>{
   const p=[24.001,86];f.state.rows[0].destination_radius=distance(24,86,...p)/1000;
   await send(0,'source');await flush();
   await send(1);await flush();await send(60,p);await flush();assert.equal(f.state.rows[0].trip_status,2);
  });
  await scenario('Return to source does not undo a start',async({send,flush,f})=>{
   await send(1);await flush();await send(60,'source');await flush();assert.equal(f.state.rows[0].trip_status,1);
  });
  await scenario('Arrival after ETA still ends',async({send,flush,f})=>{
   await send(1);await flush();await send(10800,'destination');await flush();assert.equal(f.state.rows[0].trip_status,2);
  });
  await scenario('No destination evidence keeps trip open',async({send,flush,f})=>{
   await send(1);await flush();await send(10800);await flush();assert.equal(f.state.rows[0].trip_status,1);
  });
  await scenario('Missing source geofence blocks start',async({send,flush,f})=>{
   f.state.rows[0].source_lat=null;await send(1);await flush();assert.equal(f.state.rows[0].trip_status,0);
  });
  await scenario('Missing destination allows start; arrival waits for valid destination',async({send,flush,f})=>{
   f.state.rows[0].destination_radius=null;await send(0,'source');await flush();await send(1);await flush();assert.equal(f.state.rows[0].trip_status,1);
  });
  await scenario('Invalid duration starts trip with NULL ETA',async({send,flush,f})=>{
   f.state.rows[0].time_duration=-1;await send(1);await flush();assert.equal(f.state.rows[0].trip_status,1);assert.equal(f.state.rows[0].expected_arrival_time,null);
  });
  await scenario('Invalid GPS fix produces no queue entry',async({send,store})=>{
   await assert.rejects(send(1,'road',{gps_fix:'0'}));assert.equal((await store.pendingBatch(vehicle)).length,0);
  });
  await scenario('Invalid coordinates produce no queue entry',async({send,store})=>{
   await assert.rejects(send(1,[95,85]));assert.equal((await store.pendingBatch(vehicle)).length,0);
  });
  await scenario('Malformed date produces no queue entry',async({send,store})=>{
   await assert.rejects(send(1,'road',{date_time:'31092026 100000'}));assert.equal((await store.pendingBatch(vehicle)).length,0);
  });
  await scenario('Excessive future GPS date is rejected',async({send,store,lifting})=>{
   await assert.rejects(send(1,'road',{date_time:utcText(lifting+3600000)}));assert.equal((await store.pendingBatch(vehicle)).length,0);
  });
  await scenario('Close old trip before next live start',async({send,flush,f,base,lifting})=>{
   f.state.rows=[{...base,trip_status:1,trip_date:format(lifting)}, {...base,id:'2'}];
   await send(0,'source');await flush();await send(60,'destination');await flush();assert.deepEqual(f.state.writes.map(w=>w.kind),['end','start']);
  });
  await scenario('Unmatched open trip remains open while another trip starts',async({send,flush,f,base,lifting})=>{
   f.state.rows=[{...base,trip_status:1,trip_date:format(lifting)}, {...base,id:'2'}];
   await send(0,'source');await flush();await send(60);await flush();assert.deepEqual(f.state.rows.map(t=>t.trip_status),[1,1]);
  });
  await scenario('All matching open trips close',async({send,flush,f,base,lifting})=>{
   f.state.rows=[{...base,trip_status:1,trip_date:format(lifting)}, {...base,id:'2',trip_status:1,trip_date:format(lifting)}];
   await send(60,'destination');await flush();assert.deepEqual(f.state.rows.map(t=>t.trip_status),[2,2]);
  });
  await scenario('Live database outage retains then drains packets',async({send,flush,f,store})=>{
   f.state.unavailable=true;await send(1);await assert.rejects(flush());await send(60,'destination');
   assert.equal((await store.pendingBatch(vehicle)).length,2);f.state.unavailable=false;await flush();assert.equal(f.state.rows[0].trip_status,2);
  });
  await scenario('Lost commit response retries without duplicate write',async({send,flush,f})=>{
   await send(1);f.state.loseCommit=true;await assert.rejects(flush());await flush();assert.equal(f.state.writes.length,1);
  });
  await scenario('Redis cache failure after commit preserves journal for retry',async({send,flush,f,store})=>{
   await send(1);const publish=store.publish.bind(store);store.publish=async()=>{throw Error('Simulated Redis failure');};
   await assert.rejects(flush());assert.ok(await store.loadIntent(vehicle));assert.equal((await store.pendingBatch(vehicle)).length,1);
   store.publish=publish;await flush();assert.equal(f.state.writes.length,1);
  });
  await scenario('Lease loss prevents stale worker writing',async({send,flush,f,store,redis})=>{
   await send(1);await redis.del(store.lease);await assert.rejects(flush(),/LEASE_LOST/);assert.equal(f.state.writes.length,0);
   assert.ok(await store.acquire());await flush();assert.equal(f.state.writes.length,1);
  });
  await scenario('Live start followed by delayed history destination closes correctly',async({send,flush,f,store,lifting})=>{
   await send(1);await flush();await send(120);await flush();await send(60,'destination',{packet_type:'H',is_live:false,is_history:true});await flush();
   assert.equal(f.state.rows[0].completion_date,format(lifting+60000));
   const live=JSON.parse(await redis.hget(store.keys(vehicle).live,'packet'));assert.equal(live.gps_timestamp,lifting+120000);
  });
  await scenario('Source observed, first destination point starts only; later point ends',async({send,flush,f})=>{
   await send(1,'destination');await flush();assert.equal(f.state.rows[0].trip_status,1);
   await send(2,'destination');await flush();assert.equal(f.state.rows[0].trip_status,2);
  });
  await scenario('301-point backlog crosses write batches without lost arrival',async({send,drain,f})=>{
   for(let s=1;s<=300;s++)await send(s);await send(301,'destination');await drain();
   assert.equal(f.state.writes.length,2);assert.equal(f.state.rows[0].trip_status,2);
  });
  const folder=path.join(__dirname,'..','simulation-results');fs.mkdirSync(folder,{recursive:true});
  fs.writeFileSync(path.join(folder,'live-results.json'),JSON.stringify({scope:'Live packet simulation, Redis/Lua emulator and simulated MySQL transactions; no production access',count:results.length,results},null,2));
  const lines=['# Live GPS simulation','',`${results.length} asserted scenarios passed.`, '',
   'Live event and receive time advance together; ordinary packets use packet_type L. A mixed live/history case is labelled explicitly. Application parser, Redis Lua, queue, write journal and repository execute unchanged. MySQL transaction adapter and receive clock are simulated. No production MQTT transport, MySQL locking/failover, Redis disk crash or load benchmark was tested.', '',
   '| Scenario | Packets accepted | SQL updates | Final statuses | Result |','|---|---:|---:|---|---|'];
  for(const r of results)lines.push(`| ${r.scenario} | ${r.packets_received} | ${r.writes.length} | ${r.trips.map(t=>t.status).join(', ')} | PASS |`);
  lines.push('', 'Status: 0 pending, 1 started, 2 completed. PASS includes expected rejection or retained-open cases. Detailed saved GPS timestamps are in live-results.json.', '',
   'Reproduce: npm ci; python3 -m pip install "fakeredis[lua]"; python3 scripts/run-redis-emulator-check.py', '',
   'Business boundaries: these departures include observed source IN; missing-state fallback is additionally covered by start-fix simulations. Existing OUTSIDE state without entry evidence stays pending. No dwell/debounce is configured. A first point at destination needs a later point to end. Arrival must be later than saved start. Committed timestamps are not retroactively rewritten.');
  fs.writeFileSync(path.join(folder,'LIVE_SIMULATION.md'),lines.join('\n')+'\n');
  console.log(`${results.length} live scenarios PASS; reports in simulation-results/`);
 } finally {const keys=await redis.keys(`${cfg.prefix}:*`);if(keys.length)await redis.del(...keys);redis.disconnect();}
}
main().catch(e=>{console.error(e);process.exitCode=1;});
