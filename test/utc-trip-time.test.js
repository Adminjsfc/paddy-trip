'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const {utcPacketPartsToTimestamp,utcDateTimeToTimestamp,mapMqttPacket}=require('../src/mqtt-packet');
const {archivePacket,HistoryService}=require('../src/archive-client');
const {dbTime,sqlTime,normalizePacket}=require('../src/domain');
const {floorFor}=require('../src/archive-recovery');
const {dbFixture,utcText}=require('../scripts/simulation-fixtures.cjs');
const cfg=require('../src/config'),v='JH09M4164';
const raw='$PVT,ACT1,1.3.0,NR,01,L,866738089218273,JH09M4164,1,02102026,114531,24.50102017,N,86.64302050,E';
test('exact dataPacket[9]/[10] expression and direct-parts helper both mean UTC',()=>{
 const dataPacket=raw.split(','),safeValue=x=>x??'';
 const gpsDateTime=`${safeValue(dataPacket[9])} ${safeValue(dataPacket[10])}`;
 assert.equal(gpsDateTime,'02102026 114531');
 const ts=utcDateTimeToTimestamp(gpsDateTime);
 assert.equal(ts,utcPacketPartsToTimestamp(dataPacket[9],dataPacket[10]));
 assert.equal(new Date(ts).toISOString(),'2026-10-02T11:45:31.000Z');
 assert.equal(sqlTime(ts,'+05:30'),'2026-10-02 17:15:31.000');
});
test('numeric fields with lost leading zeroes, missing fields and invalid dates are rejected',()=>{
 for(const [d,t] of [['02102026',74653],['2102026','074653'],['31092026','114531'],['02102026',undefined],['02102026','240000']])
  assert.throws(()=>utcPacketPartsToTimestamp(d,t));
});
for(const [timezone,localOffset] of [['UTC',0],['Asia/Kolkata',-330],['America/New_York',240],['Pacific/Auckland',-780]])test('GPS epoch and MySQL output do not depend on server TZ='+timezone,()=>{
 const previous=process.env.TZ;
 try {
  process.env.TZ=timezone;
  assert.equal(new Date('2026-10-02T18:31:00Z').getTimezoneOffset(),localOffset);
  const ts=utcDateTimeToTimestamp('02102026 183100');
  assert.deepEqual([ts,new Date(ts).toISOString(),sqlTime(ts,'+05:30')],
   [Date.parse('2026-10-02T18:31:00Z'),'2026-10-02T18:31:00.000Z','2026-10-03 00:01:00.000']);
 } finally { if(previous===undefined)delete process.env.TZ;else process.env.TZ=previous; }
});
test('MySQL UTC lifting boundary becomes a UTC Influx query range exactly once',async()=>{
 const from=floorFor({trip_status:0,lifting_date:'2026-10-02 11:45:00'},cfg),until=Date.parse('2026-10-02T12:00:00Z');
 let requested;
 const client=new HistoryService({pageSize:2,maxBytes:10000,timeoutMs:100},{getHistory:async p=>{
  requested=p;return{success:true,report:'history',count:0,data:[],filters:{vehicle_number:v,from_date:p.from_date,to_date_exclusive:p.to_date},pagination:{has_more:false,next_cursor:null}};
 }});
 await client.page(v,from,until);client.close();
 assert.equal(requested.from_date,'2026-10-02T11:45:00.000Z');assert.equal(requested.to_date,'2026-10-02T12:00:00.000Z');
});
for(const transport of ['mqtt','influx'])for(const daysLate of [0,2])test(`${transport}: ${daysLate}-day late GPS start/end cross IST midnight with one conversion`,async()=>{
 const begin=Date.parse('2026-10-02T18:28:00Z'),now=begin+daysLate*86400000+600000;
 const trip={id:'1',vehicle_no:v,trip_status:0,route_id:'10',status:1,level:1,lifting_date:'2026-10-02 18:28:00',
  source_lat:23,source_lng:85,source_radius:0.2,destination_lat:24,destination_lng:86,destination_radius:0.2,time_duration:60};
 const packets=[[0,23,85],[60000,23.1,85],[180000,24,86]].map(([delta,lat,lng])=>{
  const ts=begin+delta,data={packet_header:'$PVT',vehicle_number:v,packet_type:daysLate?'H':'L',gps_fix:'1',latitude:lat,longitude:lng,
   date_time:utcText(ts),_time:new Date(ts).toISOString(),gps_timestamp:ts+19800000,server_date_time:now};
  return transport==='mqtt'?normalizePacket(mapMqttPacket(`gps/${v}/data`,JSON.stringify(data)),cfg,now):archivePacket(data,v,begin,begin+600000).packet;
 });
 const f=dbFixture([trip]);let intent;
 await f.repo.transitionBatch(packets.reverse(),{done:new Set(),geofences:{},save:async value=>{intent=value;}});
 const actual=f.state.rows[0];assert.equal(actual.trip_status,2);
 assert.equal(actual.trip_date,'2026-10-02 18:29:00.000');assert.equal(actual.completion_date,'2026-10-02 18:31:00.000');
 assert.equal(actual.expected_completion_at,null);assert.equal(f.state.writes.length,2);
 assert.deepEqual(intent.operations.map(x=>x.packet.ts),[begin+60000,begin+180000]);
 assert.equal(dbTime(actual.trip_date,'+00:00'),begin+60000);
 assert.equal(sqlTime(dbTime(actual.completion_date,'+00:00'),'+05:30'),'2026-10-03 00:01:00.000');
});
