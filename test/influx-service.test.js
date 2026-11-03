'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const {GpsReportService}=require('../src/influx/GpsReportService');
const {HistoryService,serviceOptions,archivePacket}=require('../src/archive-client');
const {utcText}=require('../scripts/simulation-fixtures.cjs');
const {utcDateTimeToTimestamp}=require('../src/mqtt-packet');
const from=Date.parse('2026-10-01T07:00:00Z'),to=from+60000,v='JH06J6168';
const cfg={influx:{url:'http://influx.test:8086',token:'test-only',bucket:'gps_bucket',org:'AISPL',measurement:'device_pvt_log'},timeoutMs:100,pageSize:2,maxBytes:10000};
const rows=[0,1,2].map(n=>({vehicle_number:v,imei_no:'865510087948487',packet_type:'H',gps_fix:'1',
 gps_rawdata:'$PVT,ACT1',latitude:23+n/10,longitude:85,_time:new Date(from+n*1000).toISOString(),
 date_time:utcText(from+n*1000),server_date_time:String(to),_server_sort:to}));
function fixture(batches,queries=[]){return{queryRows(query,observer){
 queries.push(query);observer.useCancellable({cancel(){}});
 for(const r of batches.shift()||[])observer.next({...r,_gps_time:new Date(utcDateTimeToTimestamp(r.date_time)).toISOString()},{toObject:x=>x});observer.complete();
}};}
test('embedded Flux service performs candidate/detail queries and resumes signed cursor after worker restart',async()=>{
 const queries=[],first=new GpsReportService({...serviceOptions(cfg),queryApi:fixture([rows,rows.slice(0,2)],queries)});
 const client=new HistoryService(cfg,first),p1=await client.page(v,from,to);client.close();
 assert.equal(p1.count,2);assert.equal(p1.pagination.has_more,true);
 assert.ok(queries[0].includes('from(bucket: "gps_bucket")'));assert.ok(queries[0].includes('r.vehicle_number == "JH06J6168"'));
 assert.ok(queries[0].includes('r._measurement == "device_pvt_log"'));assert.ok(queries[1].includes('pivot('));
 const restarted=new GpsReportService({...serviceOptions(cfg),queryApi:fixture([rows.slice(2),rows.slice(2)],queries)});
 const nextClient=new HistoryService(cfg,restarted),p2=await nextClient.page(v,from,to,p1.pagination.next_cursor);nextClient.close();
 assert.equal(p2.count,1);assert.equal(p2.pagination.has_more,false);assert.equal(p2.pagination.next_cursor,null);
 assert.ok(queries[2].includes('r._time >'));assert.equal(archivePacket(p2.data[0],v,from,to).packet.ts,from+2000);
});
test('changed packet between candidate and detail reads cannot be accepted as empty history',async()=>{
 const service=new GpsReportService({...serviceOptions(cfg),queryApi:fixture([[rows[0]],[]])});
 const client=new HistoryService(cfg,service);
 await assert.rejects(client.page(v,from,to),{code:'ARCHIVE_INFLUX_ERROR',status:409});client.close();
});
test('real query cancellation releases the embedded service concurrency slot',async()=>{
 let cancels=0;
 const service=new GpsReportService({...serviceOptions(cfg),queryApi:{queryRows(q,o){o.useCancellable({cancel(){cancels++;}});}}});
 const client=new HistoryService({...cfg,timeoutMs:10},service);
 await assert.rejects(client.page(v,from,to),{code:'ARCHIVE_TIMEOUT'});
 await new Promise(resolve=>setImmediate(resolve));assert.equal(cancels,1);assert.equal(service.activeQueries,0);client.close();
});
test('database authentication errors are sanitized and retain numeric status',async()=>{
 const service=new GpsReportService({...serviceOptions(cfg),queryApi:{queryRows(q,o){o.error(Object.assign(Error('secret token'),{statusCode:401}));}}});
 const client=new HistoryService(cfg,service);
 await assert.rejects(client.page(v,from,to),e=>e.status===401&&e.code==='ARCHIVE_INFLUX_ERROR'&&!e.message.includes('secret'));client.close();
});
test('changed cursor signing key explicitly requests a same-window restart',async()=>{
 const first=new HistoryService(cfg,new GpsReportService({...serviceOptions(cfg),queryApi:fixture([rows,rows.slice(0,2)])}));
 const page=await first.page(v,from,to);first.close();
 const changed={...cfg,cursorSecret:'rotated-secret'};
 const second=new HistoryService(changed,new GpsReportService({...serviceOptions(changed),queryApi:fixture([])}));
 await assert.rejects(second.page(v,from,to,page.pagination.next_cursor),{code:'ARCHIVE_INFLUX_ERROR',status:400});second.close();
});

for(const [label,start,end] of [
 ['UTC ISO','2026-10-02T12:00:00Z','2026-10-02T13:00:00Z'],
 ['UTC text','2026-10-02 12:00:00','2026-10-02 13:00:00'],
 ['explicit IST selection','2026-10-02T17:30:00+05:30','2026-10-02T18:30:00+05:30']
])test(label+' produces UTC Flux boundaries and matching IST display',async()=>{
 const queries=[],service=new GpsReportService({...serviceOptions(cfg),queryApi:fixture([[]],queries)});
 const p=await service.getHistory({vehicle_number:v,from_date:start,to_date:end});service.close();
 assert.ok(queries[0].includes('range(start: time(v: "2026-10-02T12:00:00.000Z"), stop: time(v: "2026-10-02T18:30:00.000Z"))'));
 assert.ok(queries[0].includes('r._gps_time >= "2026-10-02T12:00:00.000Z" and r._gps_time < "2026-10-02T13:00:00.000Z"'));
 assert.equal(p.filters.from_date,'2026-10-02T12:00:00.000Z');assert.equal(p.filters.to_date_exclusive,'2026-10-02T13:00:00.000Z');
 assert.equal(p.filters.from_date_ist,'2026-10-02 17:30:00.000');assert.equal(p.filters.to_date_exclusive_ist,'2026-10-02 18:30:00.000');
 assert.equal(p.filters.query_timezone,'UTC');assert.equal(p.display_timezone,'Asia/Kolkata');
});
test('an IST calendar day queries the correct previous-day UTC boundary',async()=>{
 const queries=[],service=new GpsReportService({...serviceOptions(cfg),queryApi:fixture([[]],queries)});
 const p=await service.getHistory({vehicle_number:v,from_date:'2026-10-02T00:00:00+05:30',to_date:'2026-10-03T00:00:00+05:30'});service.close();
 assert.equal(p.filters.from_date,'2026-10-01T18:30:00.000Z');assert.equal(p.filters.to_date_exclusive,'2026-10-02T18:30:00.000Z');
 assert.equal(p.filters.from_date_ist,'2026-10-02 00:00:00.000');assert.equal(p.filters.to_date_exclusive_ist,'2026-10-03 00:00:00.000');
 assert.ok(queries[0].includes('time(v: "2026-10-01T18:30:00.000Z")'));
});
test('IST packet display crosses midnight without changing raw UTC or archive event time',async()=>{
 const start=Date.parse('2026-10-02T18:30:00Z'),packet={...rows[0],_time:'2026-10-02T18:31:00.000Z',date_time:'02102026 183100',server_date_time:String(start+120000),_server_sort:start+120000};
 const service=new GpsReportService({...serviceOptions(cfg),queryApi:fixture([[packet],[packet]])});
 const client=new HistoryService(cfg,service),p=await client.page(v,start,start+300000);client.close();
 assert.equal(p.data[0]._time,packet._time);assert.equal(p.data[0].date_time,packet.date_time);assert.equal(p.data[0].gps_date_time_iso,packet._time);
 assert.equal(p.data[0].gps_date_time_ist,'2026-10-03 00:01:00.000');assert.equal(p.data[0].server_date_time_ist,'2026-10-03 00:02:00.000');
 assert.equal(archivePacket(p.data[0],v,start,start+300000).packet.ts,start+60000);
});
test('half-open UTC interval accepts the start and excludes exactly the end',()=>{
 assert.equal(archivePacket(rows[0],v,from,to).packet.ts,from);
 assert.throws(()=>archivePacket({...rows[0],_time:new Date(to).toISOString(),date_time:utcText(to)},v,from,to),{code:'ARCHIVE_OUT_OF_RANGE'});
});
test('worker range rejects ambiguous string boundaries before querying Influx',async()=>{
 let calls=0;const client=new HistoryService(cfg,{getHistory:async()=>{calls++;}});
 for(const [a,b] of [['2026-10-02 12:00:00',to],[from,'2026-10-02T13:00:00Z'],[from,from],[to,from],[NaN,to]])
  await assert.rejects(client.page(v,a,b),{code:'ARCHIVE_INVALID_RANGE'});
 assert.equal(calls,0);client.close();
});
