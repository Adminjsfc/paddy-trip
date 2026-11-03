'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const fixtures=require('./fixtures/capture-20261002-082854.json');
const {dbFixture,format}=require('../scripts/simulation-fixtures.cjs');
for(const item of fixtures){
 test(`historical IST capture ${item.row.vehicle_no}/${item.row.id} follows current source-OUT policy`,async()=>{
  // Captured files predate UTC storage. Convert their actual IST DB dates once.
  const row={...item.row};
  for(const key of ['lifting_date','trip_date','completion_date'])if(row[key])row[key]=format(Date.parse(row[key].replace(' ','T')+'+05:30'));
  const f=dbFixture([row]),p=item.latest_packet;
  const packet={vehicle_no:item.row.vehicle_no,lat:p.latitude,lng:p.longitude,ts:p.gps_timestamp,packet_type:p.packet_type};
  const r=await f.repo.transitionBatch([packet],{save:async()=>{},geofences:{[item.row.id]:item.redis_state}});
  const pending=Number(item.row.trip_status)===0;
  assert.equal(f.state.writes.length,pending?1:0);
  assert.equal(r.intent.decisions[0].reason,pending?'START':'INVALID_DESTINATION_GEOFENCE');
  if(pending){assert.equal(f.state.rows[0].trip_date,format(packet.ts));assert.equal(f.state.rows[0].trip_completed_at,null);}
 });
}
