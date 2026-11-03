'use strict';
// Synthetic data only. The wrapper starts a fresh loopback-only InfluxDB.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const { HistoryService, serviceOptions, archivePacket } = require('../src/archive-client');
const { GpsReportService } = require('../src/influx/GpsReportService');
const { ArchiveRecovery, coverageFor } = require('../src/archive-recovery');
const { BUILD } = require('../src/worker-metrics');
const { utcText, format, dbFixture } = require('./simulation-fixtures.cjs');
const { probe } = require('./check-worker-history.cjs');
const tripCfg = require('../src/config');
const from = Date.parse('2026-10-02T03:03:10Z'), until = from + 21600000, shift = 19800000;
const url = process.env.JSFC_TEST_INFLUX_URL;
if (!url || new URL(url).hostname !== '127.0.0.1' || !process.env.JSFC_TEST_INFLUX_TOKEN)
  throw Error('Run scripts/run-native-influx-check.py with an influxd executable; production URLs are forbidden');
const cfg = { enabled: true, influx: { url, token: process.env.JSFC_TEST_INFLUX_TOKEN,
  org: 'jsfc-test', bucket: 'gps_fixture', measurement: 'device_pvt_log' }, cursorSecret: 'synthetic-cursor-key',
  timeoutMs: 20000, pageSize: 200, maxBytes: 8388608, windowMs: 21600000, maxLookbackMs: 31*86400000,
  rescanMs: 900000, maxPackets: 10000, importSize: 2 };
const quote = value => JSON.stringify(String(value));
function line(v, gps, offset=0, lat=24, lng=86, date=utcText(gps)) {
  return `device_pvt_log,vehicle_number=${v},imei_no=865510087948487,packet_type=H date_time=${quote(date)},server_date_time=${quote(until)},gps_fix="1",packet_header="$PVT",gps_rawdata="$PVT,ACT1",latitude=${lat.toFixed(7)},longitude=${lng.toFixed(7)} ${BigInt(gps+offset)*1000000n}`;
}
const input = (v, start=from, end=until, limit=200) => ({ vehicle_number:v,
  from_date:new Date(start).toISOString(),to_date:new Date(end).toISOString(),limit,order_by:'date_time',order:'asc' });
async function main() {
  const v='JH02AK7064', lines=[], expected=[from,from];
  lines.push(line(v,from),line(v,from,shift));
  for(let i=1;i<=201;i++){const t=from+i*60000;expected.push(t);lines.push(line(v,t,i%2?shift:0));}
  expected.push(until-1000);lines.push(line(v,until-1000,shift),line(v,until),line(v,until,shift));
  lines.push(line(v,from-1000,shift));
  // Exact production sample: physical index is inside the window, GPS event is outside.
  lines.push(line(v,Date.parse('2026-10-01T21:35:04Z'),shift));
  const badDateStart=Date.parse('2026-09-30T18:00:00Z');
  lines.push(line('OTHER1',from),line('BADDATE1',badDateStart+3600000,0,24,86,'31092026 000000'));
  lines.push(line('BADOFFSET1',from+3600000,3600000));
  const midnight=Date.parse('2026-10-02T18:31:00Z');lines.push(line('MIDNIGHT1',midnight,shift));
  const lifting=until-2*86400000;
  lines.push(line('OFFLINE2D',lifting,shift),line('OFFLINE2D',lifting+60000,0,24.1,86),line('OFFLINE2D',lifting+120000,shift,25,87));
  const written=await fetch(`${url}/api/v2/write?org=jsfc-test&bucket=gps_fixture&precision=ns`,{
    method:'POST',headers:{Authorization:`Token ${cfg.influx.token}`,'Content-Type':'text/plain'},body:lines.join('\n')});
  if(!written.ok)throw Error(`Synthetic write failed: ${written.status} ${await written.text()}`);
  const results=[];
  async function check(name, fn){const output=await fn();results.push({name,passed:true,...output});console.log('PASS '+name);}
  await check('mixed_index_full_pages_cursor_restart_and_production_sample_exclusion',async()=>{
    const first=new HistoryService(cfg);let p1;
    try{p1=await first.page(v,from,until);}finally{first.close();}
    assert.equal(p1.count,200);assert.equal(p1.pagination.has_more,true);
    const restarted=new HistoryService(cfg);let p2;
    try{p2=await restarted.page(v,from,until,p1.pagination.next_cursor);}finally{restarted.close();}
    assert.equal(p2.count,4);assert.equal(p2.pagination.has_more,false);
    const rows=[...p1.data,...p2.data];
    assert.deepEqual(rows.map(r=>archivePacket(r,v,from,until).packet.ts),expected);
    assert.deepEqual([...new Set(rows.map(r=>r.influx_index_offset_seconds))].sort((a,b)=>a-b),[0,19800]);
    assert.equal(new Set(rows.map(r=>r._time)).size,rows.length);
    return {input_records:lines.length,matched:rows.length,page_counts:[p1.count,p2.count],sample_excluded:true,
      first_gps_utc:rows[0].gps_date_time_iso,last_gps_utc:rows.at(-1).gps_date_time_iso};
  });
  await check('half_open_upper_boundary_moves_to_next_window_for_both_index_formats',async()=>{
    const c=new HistoryService(cfg);try{const p=await c.page(v,until,until+60000);
      assert.equal(p.count,2);for(const r of p.data)assert.equal(archivePacket(r,v,until,until+60000).packet.ts,until);
      return {matched:p.count,gps_utc:p.data[0].gps_date_time_iso};
    }finally{c.close();}
  });
  await check('shifted_index_crosses_midnight_without_changing_gps_event',async()=>{
    const c=new HistoryService(cfg);try{const p=await c.page('MIDNIGHT1',midnight-60000,midnight+60000);
      assert.equal(p.count,1);assert.equal(p.data[0]._time,'2026-10-03T00:01:00Z');
      assert.equal(p.data[0].gps_date_time_iso,'2026-10-02T18:31:00.000Z');
      assert.equal(p.data[0].gps_date_time_ist,'2026-10-03 00:01:00.000');return {packet:p.data[0]};
    }finally{c.close();}
  });
  await check('worker_probe_accepts_all_mixed_rows_and_signed_second_page',async()=>{
    const c=new HistoryService(cfg);try{const report=await probe({cfg,client:c,vehicleNo:v,from,until});
      assert.equal(report.success,true);assert.equal(report.accepted,204);return {pages:report.pages,accepted:report.accepted};
    }finally{c.close();}
  });
  await check('unsupported_offset_and_bad_calendar_fail_without_empty_success',async()=>{
    const c=new HistoryService(cfg);try{
      await assert.rejects(c.page('BADOFFSET1',from,until),{code:'ARCHIVE_TIME_MISMATCH'});
      await assert.rejects(c.page('BADDATE1',badDateStart,badDateStart+21600000),{code:'ARCHIVE_INVALID_GPS_TIME'});return {blocked:['ARCHIVE_TIME_MISMATCH','ARCHIVE_INVALID_GPS_TIME']};
    }finally{c.close();}
  });
  await check('nearby_and_nearest_use_gps_time_with_physical_index_lookup',async()=>{
    const service=new GpsReportService(serviceOptions(cfg));try{
      const nearby=await service.findNearby({...input(v),latitude:24,longitude:86,radius_m:100});
      assert.equal(nearby.count,200);assert.equal(nearby.pagination.has_more,true);
      const nearest=await service.findNearest({...input(v,from,until,3),latitude:24,longitude:86,radius_m:100});
      assert.equal(nearest.count,3);assert.deepEqual(nearest.data.map(r=>Date.parse(r.gps_date_time_iso)),expected.slice(0,3));
      return {nearby_count:nearby.count,nearest_count:nearest.count};
    }finally{service.close();}
  });
  await check('two_day_offline_recovery_uses_native_query_and_simulated_mysql_transitions',async()=>{
    const trip={id:'70001',vehicle_no:'OFFLINE2D',route_id:'10',trip_status:0,level:1,status:1,
      lifting_date:format(lifting),source_lat:24,source_lng:86,source_radius:0.2,
      destination_lat:25,destination_lng:87,destination_radius:0.2,time_duration:60};
    const f=dbFixture([trip]);let job=null;const staged=new Map(),queue=new Map();
    const store={load:async()=>job&&structuredClone(job),start:async(v,j)=>{job=structuredClone(j);staged.clear();},
      save:async(v,old,next,packets=[])=>{assert.equal(old.id,job.id);for(const p of packets)staged.set(p.ts,p);job=structuredClone(next);},
      importBatch:async(v,j,n)=>{for(const [ts,p]of [...staged].sort((a,b)=>a[0]-b[0]).slice(0,n)){queue.set(ts,p);staged.delete(ts);}return staged.size;},
      remove:async()=>{job=null;staged.clear();}};
    const c=new HistoryService({...cfg,pageSize:2});try{
      const recovery=new ArchiveRecovery({store,client:c,cfg,tripCfg,now:()=>until});
      const vehicles=new Map([['OFFLINE2D',[trip]]]);
      for(let n=0;n<50;n++){if((await recovery.prepare('OFFLINE2D',[trip])).ready)break;await recovery.tick(vehicles);}
      assert.equal(job.status,'READY');assert.equal(queue.size,3);
      await f.repo.transitionBatch([...queue.values()],{geofences:{},archiveCoverage:coverageFor(job),save:async()=>{}});
      assert.equal(f.state.rows[0].trip_status,2);assert.equal(f.state.rows[0].trip_date,format(lifting+60000));
      assert.equal(f.state.rows[0].completion_date,format(lifting+120000));
      return {queued_gps_utc:[...queue.keys()].sort((a,b)=>a-b).map(t=>new Date(t).toISOString()),
        trip_date_ist:f.state.rows[0].trip_date,completion_date_ist:f.state.rows[0].completion_date};
    }finally{c.close();}
  });
  const report={build:BUILD,engine:'InfluxDB OSS 2.9.1 local native Flux',production_access:false,
    mysql:'simulated',redis:'in-memory fixture for native test; separate Redis/Lua simulations also run',
    passed:results.length,failed:0,results};
  fs.writeFileSync('simulation-results/native-influx-index-validation.json',JSON.stringify(report,null,2)+'\n');
  console.log(`${results.length} native Influx scenarios passed`);
}
main().catch(e=>{console.error(e.code||e.name,e.message);process.exitCode=1;});
