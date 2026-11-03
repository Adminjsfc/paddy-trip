'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const {HistoryService,serviceOptions,archivePacket}=require('../src/archive-client');
const {utcText}=require('../scripts/simulation-fixtures.cjs');
const ts=Date.parse('2026-10-01T07:00:00Z'),v='JH06J6168';
const row={vehicle_number:v,packet_type:'H',gps_rawdata:'$PVT,ACT1,...',gps_fix:'1',latitude:23,longitude:85,
 date_time:utcText(ts),_time:new Date(ts).toISOString(),gps_timestamp:ts+19800000,server_date_time:ts+86400000};
const cfg={pageSize:2,maxBytes:10000,timeoutMs:50};
const page=(data=[row])=>({success:true,report:'history',data,count:data.length,
 filters:{vehicle_number:v,from_date:new Date(ts).toISOString(),to_date_exclusive:new Date(ts+10000).toISOString()},
 pagination:{has_more:false,next_cursor:null}});
test('archive uses confirmed UTC date_time; ignores wrong publisher epoch and late server receipt',()=>{
 assert.equal(archivePacket(row,v,ts,ts+10000).packet.ts,ts);
});
for(const packet_type of ['L','H','X'])test(`valid $PVT header accepts archive packet type ${packet_type}`,()=>{
 assert.equal(archivePacket({...row,packet_type},v,ts,ts+10000).packet.packet_type,packet_type);
});
for(const [name,change,reason]of [
 ['other header',{packet_header:'$ALT'},'NOT_PVT'],['missing header',{gps_rawdata:null},'NOT_PVT'],
 ['no GPS fix',{gps_fix:'0'},'INVALID_GPS_FIX'],['missing latitude',{latitude:null},'INVALID_COORDINATES'],
 ['invalid longitude',{longitude:181},'INVALID_COORDINATES']])test(name+' cannot become transition evidence',()=>{
 assert.equal(archivePacket({...row,...change},v,ts,ts+10000).rejected,reason);
});
test('vehicle contamination blocks the response',()=>{
 assert.throws(()=>archivePacket({...row,vehicle_number:'OTHER1'},v,ts,ts+10000),{code:'ARCHIVE_WRONG_VEHICLE'});
});
test('confirmed legacy index shift preserves UTC event time instead of adding 5:30 twice',()=>{
 assert.equal(archivePacket({...row,_time:new Date(ts+19800000).toISOString()},v,ts,ts+86400000).packet.ts,ts);
 assert.throws(()=>archivePacket({...row,_time:new Date(ts+3600000).toISOString()},v,ts,ts+86400000),{code:'ARCHIVE_TIME_MISMATCH'});
});
test('invalid UTC calendar date is rejected',()=>{
 assert.throws(()=>archivePacket({...row,date_time:'31092026 070000'},v,ts,ts+10000),{code:'ARCHIVE_INVALID_GPS_TIME'});
});
const clientFor=(value,overrides={})=>new HistoryService({...cfg,...overrides},{getHistory:async()=>value});
test('internal service receives exact vehicle, UTC range and cursor without type filtering',async()=>{
 let input,signal;
 const client=new HistoryService(cfg,{getHistory:async(p,o)=>{input=p;signal=o.signal;return page();}});
 await client.page(v,ts,ts+10000,'cursor');
 assert.equal(input.cursor,'cursor');assert.equal(input.from_date,'2026-10-01T07:00:00.000Z');
 assert.equal(input.vehicle_number,v);assert.equal(input.packet_type,undefined);assert.equal(input.order,'asc');
 assert.ok(signal instanceof AbortSignal);client.close();
});
for(const status of [400,401,409,503,504])test(`Influx status ${status} is a retry error, never empty history`,async()=>{
 const client=new HistoryService(cfg,{getHistory:async()=>{throw Object.assign(Error('secret query/token'),{statusCode:status});}});
 await assert.rejects(client.page(v,ts,ts+10000),e=>e.code==='ARCHIVE_INFLUX_ERROR'&&e.status===status&&!e.message.includes('secret'));
});
test('changed filters or non-advancing cursor blocks cursor advancement',async()=>{
 const wrong=page();wrong.filters.vehicle_number='OTHER1';
 await assert.rejects(clientFor(wrong).page(v,ts,ts+10000),{code:'ARCHIVE_FILTER_MISMATCH'});
 const loop=page();loop.pagination={has_more:true,next_cursor:'old'};
 await assert.rejects(clientFor(loop).page(v,ts,ts+10000,'old'),{code:'ARCHIVE_INVALID_CURSOR'});
});
test('oversized history page blocks import',async()=>{
 await assert.rejects(clientFor(page(),{maxBytes:10}).page(v,ts,ts+10000),{code:'ARCHIVE_RESPONSE_TOO_LARGE'});
});
test('request deadline cancels a stalled internal query',async()=>{
 let cancelled=false;
 const client=new HistoryService({...cfg,timeoutMs:5},{getHistory:(input,{signal})=>new Promise(()=>{
  signal.addEventListener('abort',()=>{cancelled=true;},{once:true});
 })});
 await assert.rejects(client.page(v,ts,ts+10000),{code:'ARCHIVE_TIMEOUT'});assert.equal(cancelled,true);
});
test('closing service cancels inflight work, destroys client once and prohibits reuse',async()=>{
 let closes=0;
 const client=new HistoryService(cfg,{getHistory:()=>new Promise(()=>{}),close:()=>{closes++;}});
 const inflight=client.page(v,ts,ts+10000);client.close();client.close();
 await assert.rejects(inflight,{code:'ARCHIVE_CLOSED'});assert.equal(closes,1);
 await assert.rejects(client.page(v,ts,ts+10000),{code:'ARCHIVE_CLOSED'});
});
test('disabled/lazy service can close without token; enabled query exposes missing configuration',async()=>{
 const client=new HistoryService(cfg);
 await assert.rejects(client.page(v,ts,ts+10000),{code:'ARCHIVE_INFLUX_CONFIG'});client.close();
});
test('cursor signing stays stable across workers/restarts and changes with source credentials',()=>{
 const c={...cfg,influx:{url:'http://influx.test:8086',org:'org',bucket:'b',token:'test-token'}};
 assert.equal(serviceOptions(c).env.CURSOR_SECRET,serviceOptions(structuredClone(c)).env.CURSOR_SECRET);
 assert.notEqual(serviceOptions(c).env.CURSOR_SECRET,serviceOptions({...c,influx:{...c.influx,token:'rotated'}}).env.CURSOR_SECRET);
 assert.equal(serviceOptions(c).env.DATE_TIMEZONE_OFFSET,'Z');assert.equal(serviceOptions(c).env.MAX_CONCURRENT_QUERIES,'1');
});
