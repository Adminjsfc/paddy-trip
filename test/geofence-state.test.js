'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const {observe,canStart}=require('../src/geofence-state');
const {Repository}=require('../src/repository');
const {dbFixture,format}=require('../scripts/simulation-fixtures.cjs');
const cfg={offset:'+00:00',radiusMultiplier:1000,departureRequiresCrossing:true};
const ts=Date.parse('2026-10-01T04:31:00Z');
const trip={id:'1',vehicle_no:'JH16B5498',trip_status:0,lifting_date:format(ts),source_lat:23,source_lng:85,source_radius:0.2,destination_lat:24,destination_lng:86,destination_radius:0.2,time_duration:60};
const source={vehicle_no:trip.vehicle_no,lat:23,lng:85,ts:ts+1000};
const outside={...source,lat:23.1,ts:ts+2000};
test('first outside point cannot start without observed source INSIDE',()=>{
 const s=observe(null,trip,outside,cfg);assert.equal(s.source_status,'OUTSIDE');assert.equal(canStart(s,trip,outside,cfg),false);
});
test('source INSIDE to OUTSIDE starts only on that exit timestamp',()=>{
 let s=observe(null,trip,source,cfg);s=observe(s,trip,outside,cfg);
 assert.equal(canStart(s,trip,outside,cfg),true);
 const later={...outside,ts:outside.ts+1000};s=observe(s,trip,later,cfg);assert.equal(canStart(s,trip,later,cfg),false);
});
test('older GPS point does not regress latest trip geofence state',()=>{
 let s=observe(null,trip,source,cfg);s=observe(s,trip,outside,cfg);
 assert.deepEqual(observe(s,trip,source,cfg),s);
});
test('changed route geometry invalidates previous source observation',()=>{
 const s=observe(null,trip,source,cfg);const changed={...trip,source_radius:0.3};
 assert.equal(canStart(observe(s,changed,outside,cfg),changed,outside,cfg),false);
});
test('per-trip source observation cannot be borrowed from another trip',()=>{
 const first=observe(null,trip,source,cfg);assert.ok(first);
 const second={...trip,id:'2'};assert.equal(canStart(observe(null,second,outside,cfg),second,outside,cfg),false);
});
test('repository missing state outside-only packet starts using the explicit assumption',async()=>{
 const f=dbFixture([trip]);let intent;
 const r=await f.repo.transitionBatch([outside],{geofences:{},save:async i=>{intent=i;}});
 assert.equal(r.events.length,1);assert.equal(f.state.writes.length,1);assert.equal(intent.geofences['1'].source_status,'OUTSIDE');
 assert.equal(r.events[0].start_basis,'SOURCE_OUT_AFTER_DISPATCH');
});
test('historical inside/exit/destination batch writes GPS start and end',async()=>{
 const f=dbFixture([trip]);const dest={...outside,lat:24,lng:86,ts:ts+60000};
 const r=await f.repo.transitionBatch([dest,outside,source],{geofences:{},save:async()=>{}});
 assert.deepEqual(r.events.map(e=>e.kind),['trip_started','trip_ended']);
 assert.equal(f.state.rows[0].trip_date,format(outside.ts));assert.equal(f.state.rows[0].completion_date,format(dest.ts));
 assert.equal(r.intent.geofences['1'].destination_status,'INSIDE');assert.equal(r.intent.geofences['1'].trip_status,2);
});

test('source-OUT starts immediately; older source-IN replay cannot regress the latest snapshot',async()=>{
 const f=dbFixture([trip]);let geofences={'1':observe(null,trip,{...outside,ts:ts-1000},cfg)};
 async function run(packets) {
  const r=await f.repo.transitionBatch(packets,{geofences,save:async()=>{}});
  geofences=r.intent.geofences;return r;
 }
 await run([outside]);assert.equal(f.state.rows[0].trip_status,1);
 await run([source]);assert.equal(geofences['1'].gps_timestamp,outside.ts);
 await run([source,outside]);assert.equal(f.state.rows[0].trip_status,1);
 assert.equal(f.state.rows[0].trip_date,format(outside.ts));
});
