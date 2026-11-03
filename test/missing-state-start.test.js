'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const {observe,observeEvent,canStart}=require('../src/geofence-state');
const {dbFixture,format}=require('../scripts/simulation-fixtures.cjs');
const cfg=require('../src/config');
const lifting=Date.parse('2026-09-30T04:30:00Z');
const trip={id:'901',vehicle_no:'JH06J6168',route_id:'10',trip_status:0,lifting_date:format(lifting),
 source_lat:23,source_lng:85,source_radius:0.2,destination_lat:24,destination_lng:86,destination_radius:0.2,time_duration:60};
const outside={vehicle_no:trip.vehicle_no,lat:23.1,lng:85,ts:lifting+1000,packet_type:'L'};
const inside={...outside,lat:23};
const destination={...outside,lat:24,lng:86,ts:lifting+3600000};
function session(rows=[trip],geofences={}){
 const f=dbFixture(rows);let intent;
 return {...f,get geofences(){return geofences;},get intent(){return intent;},run:async packets=>{
  const r=await f.repo.transitionBatch(packets,{geofences,intent,save:async value=>{intent=structuredClone(value);}});
  geofences=r.intent?.geofences||geofences;intent=null;return r;
 }};
}
for(const packet_type of ['L','H'])test(`missing state starts on first qualifying ${packet_type} GPS point`,async()=>{
 const f=session();const r=await f.run([{...outside,packet_type}]);
 assert.equal(f.state.rows[0].trip_date,format(outside.ts));assert.equal(r.events[0].start_basis,'SOURCE_OUT_AFTER_DISPATCH');
 assert.equal(r.events[0].assumed_source_in_at,lifting);assert.equal(f.geofences['901'].source_entered_at,null);
 assert.equal(f.geofences['901'].source_status,'OUTSIDE');assert.equal(f.geofences['901'].journey_state,'IN_TRIP');
});
for(const delta of [-1000,0])test(`missing state cannot start at lifting ${delta} ms`,async()=>{
 const f=session();await f.run([{...outside,ts:lifting+delta}]);assert.equal(f.state.writes.length,0);
 assert.equal(f.geofences['901'].source_assumption_pending,true);
 await f.run([outside]);assert.equal(f.state.rows[0].trip_date,format(outside.ts));
});
test('no packet cannot create a SQL transition',async()=>{
 const f=session();await f.run([]);assert.equal(f.state.writes.length,0);
});
test('actual source IN after lifting governs the next exit',async()=>{
 const f=session();await f.run([inside]);assert.equal(f.state.writes.length,0);
 assert.equal(f.geofences['901'].source_assumption_pending,false);
 const r=await f.run([{...outside,ts:outside.ts+1000}]);assert.equal(r.events[0].start_basis,'SOURCE_OUT_AFTER_DISPATCH');
});
test('pre-lifting actual source IN remains real evidence and is not fabricated',async()=>{
 const f=session();await f.run([{...inside,ts:lifting-1000}]);const r=await f.run([outside]);
 assert.equal(r.events[0].start_basis,'SOURCE_OUT_AFTER_DISPATCH');assert.equal(f.geofences['901'].source_entered_at,lifting-1000);
});
test('existing OUTSIDE without entry starts on real source-OUT after dispatch',async()=>{
 const f=session([trip],{'901':observe(null,trip,{...outside,ts:lifting-1000},cfg)});
 const r=await f.run([outside]);assert.equal(f.state.writes.length,1);assert.equal(r.intent.decisions[0].reason,'START');
});
test('existing IN state still starts by observed exit',async()=>{
 const f=session([trip],{'901':observe(null,trip,{...inside,ts:lifting-1000},cfg)});
 const r=await f.run([outside]);assert.equal(r.events[0].start_basis,'SOURCE_OUT_AFTER_DISPATCH');
});
for(const previous of [{}, {...observe(null,trip,inside,cfg),trip_id:'other'}, {...observe(null,trip,inside,cfg),geometry:'invalid'}])
 test(`incompatible Redis state cannot block valid source-OUT: ${JSON.stringify(previous).slice(0,30)}`,async()=>{
  const f=session([trip],{'901':previous});await f.run([outside]);assert.equal(f.state.writes.length,1);
 });
test('invalid source geometry cannot be overridden by assumption',async()=>{
 const f=session([{...trip,source_lat:null}]);const r=await f.run([outside]);assert.equal(f.state.writes.length,0);
 assert.equal(r.intent.decisions[0].reason,'INVALID_SOURCE_GEOFENCE');
});
test('invalid lifting date cannot be overridden by assumption',async()=>{
 const f=session([{...trip,lifting_date:null}]);await f.run([outside]);assert.equal(f.state.writes.length,0);
});
test('real historical source-OUT is eligible even when the latest snapshot is newer',()=>{
 const latest=observe(null,trip,{...outside,ts:lifting+86400000},cfg);
 const r=observeEvent(latest,trip,outside,cfg);assert.equal(canStart(r.current,trip,outside,cfg),true);
 assert.equal(r.current.source_assumed_in_at,null);assert.equal(r.snapshot.gps_timestamp,latest.gps_timestamp);
});
for(const delayDays of [1,2])test(`${delayDays}-day offline reversed history uses GPS departure and arrival, not receipt time`,async()=>{
 const f=session();const arrival={...destination,packet_type:'H',server_date_time:destination.ts+delayDays*86400000};
 const departure={...outside,packet_type:'H',server_date_time:arrival.server_date_time};
 const r=await f.run([arrival,departure]);assert.deepEqual(r.events.map(e=>e.kind),['trip_started','trip_ended']);
 assert.equal(f.state.rows[0].trip_date,format(departure.ts));assert.equal(f.state.rows[0].completion_date,format(arrival.ts));
 await f.run([arrival,departure]);assert.equal(f.state.writes.length,2);
});
test('first destination packet starts only; completion needs a later GPS timestamp',async()=>{
 const f=session();await f.run([destination]);assert.equal(f.state.rows[0].trip_status,1);
 await f.run([destination]);assert.equal(f.state.writes.length,1);
 await f.run([{...destination,ts:destination.ts+1000}]);assert.equal(f.state.rows[0].trip_status,2);
});
test('already started missing-state trip can complete but cannot restart',async()=>{
 const f=session([{...trip,trip_status:1,trip_date:format(lifting)}]);const r=await f.run([outside,destination]);
 assert.deepEqual(r.events.map(e=>e.kind),['trip_ended']);assert.equal(f.state.rows[0].trip_date,format(lifting));
});
test('completed trip cannot be restarted',async()=>{
 const f=session([{...trip,trip_status:2,trip_date:format(lifting),completion_date:format(destination.ts)}]);
 await f.run([outside]);assert.equal(f.state.writes.length,0);
});
test('lost commit retries recorded fallback exactly once without starting a newly loaded sibling',async()=>{
 const f=session();f.state.loseCommit=true;await assert.rejects(f.run([outside]));
 assert.equal(f.intent.operations[0].start_basis,'SOURCE_OUT_AFTER_DISPATCH');
 assert.equal(f.state.writes.length,1);f.state.rows.push({...trip,id:'902'});
 await f.run([outside]);assert.equal(f.state.writes.length,1);assert.equal(f.state.rows[1].trip_status,0);
});
test('MySQL outage retains work until reconnection and uses original GPS time',async()=>{
 const f=session();f.state.unavailable=true;await assert.rejects(f.run([outside]));assert.equal(f.state.writes.length,0);
 f.state.unavailable=false;await f.run([outside]);assert.equal(f.state.rows[0].trip_date,format(outside.ts));
});
