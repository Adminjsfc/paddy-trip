'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const cfg=require('../src/config');
const {dbFixture,format}=require('../scripts/simulation-fixtures.cjs');
const {observe}=require('../src/geofence-state');
const {transitionId,eventId}=require('../src/repository');
const v='JH02AK7064',ts=Date.parse('2026-10-02T03:03:10Z');
const base={id:'23398',vehicle_no:v,route_id:'529',trip_status:0,lifting_date:'2026-09-30 19:48:33',
 source_lat:24.290507,source_lng:86.633313,source_radius:0.2,destination_lat:24.3091682,destination_lng:86.6166546,destination_radius:0.2,time_duration:60};
const next={...base,id:'23808',route_id:'272',lifting_date:'2026-10-01 13:47:50',destination_lat:24.129632,destination_lng:86.74023};
const entry={vehicle_no:v,lat:base.source_lat,lng:base.source_lng,ts:1790851061000,packet_type:'H'};
const exit={...entry,lat:base.source_lat+0.01,ts}; // Synthetic exit coordinates; recorded timestamps are from the diagnostic.
const arrival={...exit,lat:24.1298122,lng:86.7401123,ts:1790915162000};
test('two pending trips sharing source start on same departure, only reached destination completes',async()=>{
 const f=dbFixture([base,next]);
 const r=await f.repo.transitionBatch([arrival,exit,entry],{save:async()=>{}});
 assert.deepEqual(f.state.writes.map(w=>[w.kind,w.trip_id]),[['start','23398'],['start','23808'],['end','23808']]);
 assert.equal(f.state.rows[0].trip_status,1);assert.equal(f.state.rows[1].trip_status,2);
 assert.equal(f.state.rows[1].trip_date,format(exit.ts));assert.equal(f.state.rows[1].completion_date,format(arrival.ts));
 assert.equal(r.intent.policy,'paddy-source-out-v4');
});
test('legacy vehicle-wide event marker does not suppress another trip sharing that GPS exit',async()=>{
 const f=dbFixture([{...base,trip_status:1,trip_date:format(ts)},next]);
 const r=await f.repo.transitionBatch([entry,exit,arrival],{done:new Set([eventId(exit)]),save:async()=>{}});
 assert.deepEqual(r.events.map(e=>e.trip_id),['23808','23808']);assert.equal(f.state.rows[0].trip_status,1);
});
test('same-trip marker suppresses only that trip/action, not other trips at same timestamp',async()=>{
 const f=dbFixture([base,next]);
 const r=await f.repo.transitionBatch([entry,exit],{done:new Set([transitionId(exit,base.id,'start')]),save:async()=>{}});
 assert.deepEqual(r.events.map(e=>e.trip_id),['23808']);assert.equal(f.state.rows[0].trip_status,0);
 assert.notEqual(transitionId(exit,base.id,'start'),transitionId(exit,base.id,'end'));
 assert.notEqual(transitionId(exit,base.id,'start'),transitionId(exit,next.id,'start'));
});
test('stored crossing timestamps recover from real exit packet even if inside packet is no longer retained',async()=>{
 let state=observe(null,next,entry,cfg);state=observe(state,next,exit,cfg);state=observe(state,next,arrival,cfg);
 const f=dbFixture([{...base,trip_status:1,trip_date:format(ts)},next]);
 await f.repo.transitionBatch([exit,arrival],{geofences:{[next.id]:state},save:async()=>{}});
 assert.equal(f.state.rows[1].trip_date,format(exit.ts));assert.equal(f.state.rows[1].completion_date,format(arrival.ts));
});
test('a real destination packet outside source starts pending trip but cannot also complete at that timestamp',async()=>{
 let state=observe(null,next,entry,cfg);state=observe(state,next,exit,cfg);state=observe(state,next,arrival,cfg);
 const f=dbFixture([next]);await f.repo.transitionBatch([arrival],{geofences:{[next.id]:state},save:async()=>{}});
 assert.equal(f.state.writes.length,1);assert.equal(f.state.rows[0].trip_status,1);assert.equal(f.state.rows[0].trip_date,format(arrival.ts));assert.equal(f.state.rows[0].trip_completed_at,null);
});
test('newly lifted sibling cannot start from an earlier shared departure',async()=>{
 const f=dbFixture([base,{...next,lifting_date:format(ts+1000)}]);
 await f.repo.transitionBatch([entry,exit],{save:async()=>{}});
 assert.equal(f.state.rows[0].trip_status,1);assert.equal(f.state.rows[1].trip_status,0);
});
test('lost commit retry for multiple fixed targets cannot assign packet to newly discovered trip',async()=>{
 const f=dbFixture([base,next]);let intent;
 const run=()=>f.repo.transitionBatch([entry,exit],{intent,save:async i=>{intent=structuredClone(i);}});
 f.state.loseCommit=true;await assert.rejects(run());assert.equal(f.state.writes.length,2);
 f.state.rows.push({...base,id:'999'});await run();assert.equal(f.state.writes.length,2);assert.equal(f.state.rows[2].trip_status,0);
});
