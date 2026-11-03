'use strict';
const test = require('node:test'), assert = require('node:assert/strict');
const cfg = require('../src/config');
const { observe, observeEvent, canStart } = require('../src/geofence-state');
const { dbFixture, format } = require('../scripts/simulation-fixtures.cjs');
const { transitionId } = require('../src/repository');
const { locationTripLog } = require('../src/location-log');
const ts = Date.parse('2026-10-01T04:30:00Z');
const trip = { id:'1', route_id:10, vehicle_no:'JH10V7046', trip_status:0,
  lifting_date:format(ts), source_lat:23.858511, source_lng:86.456557, source_radius:0.2,
  destination_lat:23.811873, destination_lng:86.236935, destination_radius:0.2, time_duration:43 };
const source = { vehicle_no:trip.vehicle_no, lat:23.8582687, lng:86.456604, ts:ts+1000, packet_type:'L' };
const outside = { ...source, lat:23.862, ts:ts+2000 };
const dest = { ...source, lat:trip.destination_lat, lng:trip.destination_lng, ts:ts+60000 };
function session(rows=[trip], geofences={}) {
  const f = dbFixture(rows);
  return { ...f, run: async packets => {
    const r = await f.repo.transitionBatch(packets, { geofences, save:async()=>{} });
    geofences=r.intent.geofences;return r;
  } };
}
test('confirmed database unit: real logged point 27 m from source lies inside 0.2 km',()=>{
 assert.equal(cfg.radiusMultiplier,1000);
 const log=locationTripLog(trip.vehicle_no,trip,{latitude:source.lat,longitude:source.lng,gps_timestamp:source.ts},cfg);
 assert.equal(log.source_status,'INSIDE');assert.equal(log.source_radius_km,0.2);
});
test('Redis IN source then live OUT updates MySQL once at GPS time',async()=>{
 const f=session();await f.run([source]);const r=await f.run([outside]);
 assert.equal(f.state.rows[0].trip_status,1);assert.equal(f.state.rows[0].trip_date,format(outside.ts));
 assert.equal(r.intent.geofences['1'].journey_state,'IN_TRIP');assert.equal(r.events.length,1);
 await f.run([outside]);assert.equal(f.state.writes.length,1);
});
test('DECIMAL strings, route ID string and DATETIME precision changes preserve source IN',async()=>{
 const state=observe(null,trip,source,cfg);
 const row={...trip,route_id:'10',lifting_date:trip.lifting_date.slice(0,19),source_lat:'23.85851100',source_lng:'86.45655700',source_radius:'0.200'};
 const f=session([row],{'1':state});await f.run([outside]);assert.equal(f.state.rows[0].trip_status,1);
});
test('legacy geometry and wrong metre radius recover retained point as IN without Redis flush',async()=>{
 const old=observe(null,trip,source,{...cfg,radiusMultiplier:1});
 delete old.radius_multiplier;
 old.geometry=JSON.stringify([trip.route_id,trip.lifting_date,trip.source_lat,trip.source_lng,trip.source_radius,trip.destination_lat,trip.destination_lng,trip.destination_radius]);
 assert.equal(old.source_status,'OUTSIDE');
 const f=session([trip],{'1':old});await f.run([outside]);assert.equal(f.state.rows[0].trip_status,1);
});
test('earlier pending row at another source does not block this trip source exit',async()=>{
 const other={...trip,id:'0',source_lat:outside.lat,source_lng:outside.lng};
 const f=session([other,trip],{'1':observe(null,trip,source,cfg)});const r=await f.run([outside]);
 assert.equal(f.state.rows[0].trip_status,0);assert.equal(f.state.rows[1].trip_status,1);
 assert.deepEqual(r.events.map(e=>e.trip_id),['1']);
});
test('missing duration starts with NULL ETA; no invented travel time',async()=>{
 const f=session([{...trip,time_duration:null}]);await f.run([source,outside]);
 assert.equal(f.state.rows[0].trip_status,1);assert.equal(f.state.rows[0].expected_completion_at,null);
});
test('missing destination does not block source exit and produces explicit end blocker',async()=>{
 const f=session([{...trip,destination_lat:null}]);await f.run([source,outside]);
 const r=await f.run([dest]);assert.equal(f.state.rows[0].trip_status,1);
 assert.equal(r.intent.decisions[0].reason,'INVALID_DESTINATION_GEOFENCE');
});
test('existing OUTSIDE state starts on a real source-OUT packet after dispatch',async()=>{
 const f=session([trip],{'1':observe(null,trip,{...outside,ts:ts-1000},cfg)});const r=await f.run([outside]);assert.equal(f.state.rows[0].trip_status,1);
 assert.equal(r.intent.decisions[0].reason,'START');
});
test('open trip does not block another trip with its own source crossing',async()=>{
 const open={...trip,id:'2',trip_status:1,trip_date:format(ts)};
 const f=session([trip,open]);await f.run([source]);const r=await f.run([outside]);
 assert.equal(r.intent.decisions.find(d=>d.trip_id==='1').reason,'START');
 assert.equal(f.state.rows[0].trip_status,1);
 assert.deepEqual(r.intent.decisions.find(d=>d.trip_id==='1').blocking_trip_ids,[]);
});
test('before-lifting IN evidence plus reversed historical departure/arrival writes both GPS times',async()=>{
 const f=session();const r=await f.run([{...dest,packet_type:'H'}, {...outside,packet_type:'H'}, {...source,ts:ts-1000,packet_type:'H'}]);
 assert.deepEqual(r.events.map(e=>e.kind),['trip_started','trip_ended']);
 assert.equal(f.state.rows[0].trip_date,format(outside.ts));assert.equal(f.state.rows[0].completion_date,format(dest.ts));
});
test('real historical source-OUT remains eligible without borrowing a newer IN observation',()=>{
 const future=observe(null,trip,{...source,ts:ts+5000},cfg);
 const r=observeEvent(future,trip,outside,cfg);assert.equal(canStart(r.current,trip,outside,cfg),true);
 assert.equal(r.snapshot.gps_timestamp,ts+5000);
});
test('replayed event used by an earlier trip remains usable as observation but never repeated UPDATE',async()=>{
 const f=dbFixture([trip]);const prior={...source,ts:ts-1000};
 const r=await f.repo.transitionBatch([prior,outside],{done:new Set([transitionId(prior, 'other-trip', 'end')]),save:async()=>{}});
 assert.equal(r.events.length,1);assert.equal(r.events[0].gps_timestamp,outside.ts);
});
