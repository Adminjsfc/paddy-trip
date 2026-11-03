'use strict';
// Isolated simulation: real application code + Redis Lua; simulated MySQL adapter.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const NOW = Date.parse('2026-09-30T08:00:00Z');
Date.now = () => NOW;
process.env.REDIS_PREFIX = `jsfc:offline-simulation:${require('node:crypto').randomUUID()}`;
const cfg = require('../src/config');
const { Repository, START, END } = require('../src/repository');
const { Store, ingest } = require('../src/redis-store');
const { mapMqttPacket } = require('../src/mqtt-packet');
const { shardFor, sqlTime } = require('../src/domain');
const { flushVehicle } = require('../src/write-buffer');
const { replayBatch } = require('../src/history-replay');
const vehicle = 'JH16B5498';
const { format, utcText, dbFixture } = require('./simulation-fixtures.cjs');
async function main() {
  const redis = new (require('ioredis'))(process.env.REDIS_URL || 'redis://127.0.0.1:6379/0',
    { lazyConnect: true, enableReadyCheck: false, maxRetriesPerRequest: 1 });
  await redis.connect();
  const results = [];
  async function scenario(name, days, fn) {
    const store = new Store(redis, shardFor(vehicle));
    const old = await redis.keys(`${cfg.prefix}:*`); if (old.length) await redis.del(...old);
    assert.equal(await store.acquire(), true);
    const lifting = NOW - days*86400000 - 3*3600000;
    const base = { id: '1', route_id: '10', vehicle_no: vehicle, status: 1, level: 1,
      trip_status: 0, lifting_date: format(lifting), trip_date: null,
      source_lat: 23, source_lng: 85, source_radius: 0.2,
      destination_lat: 24, destination_lng: 86, destination_radius: 0.2, time_duration: 120 };
    const f = dbFixture([base]);
    const send = async (minutes, point='road', extra={}) => {
      const [latitude,longitude] = point==='source' ? [23,85] : point==='destination' ? [24,86] : [23.1,85.1];
      const ts = lifting + minutes*60000;
      const wire = { packet_header: '$PVT', latitude, longitude, packet_type: 'H', gps_fix: '1', date_time: utcText(ts),
        gps_timestamp: ts + 19800000, server_date_time: NOW, is_history: true, ...extra };
      const mapped = mapMqttPacket(`gps/${vehicle}/data`,JSON.stringify(wire));
      return ingest(redis,mapped);
    };
    const flush = () => flushVehicle(store,f.repo,vehicle);
    await send(-600,'source'); await flush();
    await fn({ f, store, lifting, base, send, flush });
  for (const t of f.state.rows.filter(t => t.trip_status === 2)) {
    assert.equal(await redis.exists(`${store.base}:vehicle:${vehicle}:trip:${t.id}:geofence`), 0);
    assert.equal((await store.loadGeofences(vehicle))[String(t.id)], undefined);
  }
    results.push({ scenario: name, offline_days: days, result: 'PASS',
      received_at_utc: new Date(NOW).toISOString(), trips: f.state.rows.map(t => ({
        id:t.id, status:t.trip_status, start_ist:t.trip_date, end_ist:t.completion_date||null,
        expected_arrival_ist:t.expected_arrival_time||null })), writes: f.state.writes });
  }
  try {
    for (const days of [1,2]) {
      await scenario('Source-only history stays pending',days,async ({send,flush,f}) => {
        await send(10,'source'); await flush(); assert.equal(f.state.rows[0].trip_status,0);
      });
      await scenario('Historical departure starts at GPS time',days,async ({send,flush,f,lifting}) => {
        await send(10,'road'); await flush(); assert.equal(f.state.rows[0].trip_date,format(lifting+600000));
      });
      await scenario('Whole offline journey starts and ends at historical times',days,async ({send,flush,f,lifting}) => {
        await send(5,'source'); await send(10,'road'); await send(90,'destination'); await flush();
        assert.equal(f.state.rows[0].trip_date,format(lifting+600000));
        assert.equal(f.state.rows[0].completion_date,format(lifting+90*60000));
      });
      await scenario('Already started vehicle closes on delayed destination history',days,async ({send,flush,f,lifting}) => {
        Object.assign(f.state.rows[0],{ trip_status:1, trip_date:format(lifting+600000) });
        await send(90,'destination'); await flush();
        assert.equal(f.state.rows[0].completion_date,format(lifting+90*60000));
      });
      await scenario('Reverse packet delivery sorts before writes',days,async ({send,flush,f,lifting}) => {
        await send(90,'destination'); await send(10,'road'); await send(5,'source'); await flush();
        assert.equal(f.state.rows[0].trip_date,format(lifting+600000)); assert.equal(f.state.rows[0].trip_status,2);
      });
      await scenario('Arrival after ETA still completes',days,async ({send,flush,f,lifting}) => {
        await send(10); await send(170,'destination'); await flush();
        assert.equal(f.state.rows[0].completion_date,format(lifting+170*60000));
      });
      await scenario('Duplicate live/history redelivery does not repeat writes',days,async ({send,flush,f}) => {
        await send(10); await send(90,'destination'); await flush();
        await send(10,'road',{packet_type:'L'}); await send(90,'destination'); await flush();
        assert.equal(f.state.writes.length,2);
      });
      await scenario('Packets at or before lifting do not start',days,async ({send,flush,f}) => {
        await send(-1); await send(0); await flush(); assert.equal(f.state.rows[0].trip_status,0);
      });
      await scenario('Destination before/equal start does not close',days,async ({send,flush,f,lifting}) => {
        Object.assign(f.state.rows[0],{trip_status:1,trip_date:format(lifting+600000)});
        await send(5,'destination'); await send(10,'destination'); await flush(); assert.equal(f.state.rows[0].trip_status,1);
      });
      await scenario('No destination evidence leaves started trip open',days,async ({send,flush,f}) => {
        await send(10); await send(90); await flush(); assert.equal(f.state.rows[0].trip_status,1);
      });
      await scenario('MySQL outage retains packets; recovery completes journey',days,async ({send,flush,f,store}) => {
        await send(10); await send(90,'destination'); f.state.unavailable=true;
        await assert.rejects(flush()); assert.equal((await store.pendingBatch(vehicle)).length,2);
        f.state.unavailable=false; await flush(); assert.equal(f.state.rows[0].trip_status,2);
        assert.equal((await store.pendingBatch(vehicle)).length,0);
      });
      await scenario('Lost commit response reconciles exact historical writes',days,async ({send,flush,f,store}) => {
        await send(10); await send(90,'destination'); f.state.loseCommit=true;
        await assert.rejects(flush()); assert.ok(await store.loadIntent(vehicle));
        await flush(); assert.equal(f.state.writes.length,2); assert.equal(await store.loadIntent(vehicle),null);
      });
      await scenario('New trip discovered after history ingestion replays journey',days,async ({send,flush,f,store,lifting,base}) => {
        f.state.rows=[]; await send(5,'source'); await send(10); await send(90,'destination'); await flush();
        f.state.rows=[structuredClone(base)]; await store.rewind(vehicle,lifting);
        await replayBatch(store,vehicle,lifting-1); await flush(); assert.equal(f.state.rows[0].trip_status,2);
      });
      await scenario('Source observed, destination point cannot both start and end',days,async ({send,flush,f}) => {
        await send(90,'destination'); await flush(); assert.equal(f.state.rows[0].trip_status,1);
        await send(91,'destination'); await flush(); assert.equal(f.state.rows[0].trip_status,2);
      });
      await scenario('Old arrival predating already committed newer start remains blocked',days,async ({send,flush,f}) => {
        await send(100); await flush(); await send(90,'destination'); await flush();
        assert.equal(f.state.rows[0].trip_status,1);
      });
      await scenario('Separate history deliveries start then close',days,async ({send,flush,f,lifting}) => {
        await send(10); await flush(); assert.equal(f.state.rows[0].trip_status,1);
        await send(90,'destination'); await flush();
        assert.equal(f.state.rows[0].trip_date,format(lifting+600000));
        assert.equal(f.state.rows[0].completion_date,format(lifting+90*60000));
      });
      await scenario('All matching previously opened trips close',days,async ({send,flush,f,base,lifting}) => {
        f.state.rows=[{...base,trip_status:1,trip_date:format(lifting+600000)},
          {...base,id:'2',trip_status:1,trip_date:format(lifting+1200000)}];
        await send(90,'destination'); await flush();
        assert.ok(f.state.rows.every(t=>t.trip_status===2)); assert.equal(f.state.writes.length,2);
      });
      await scenario('Other open trip stays open while independent historical journey starts and ends',days,async ({send,flush,f,base,lifting}) => {
        f.state.rows=[{...base,trip_status:1,trip_date:format(lifting+600000),destination_lat:25},
          {...base,id:'2'}];
        await send(20,'source'); await send(30); await send(90,'destination'); await flush();
        assert.deepEqual(f.state.rows.map(t=>t.trip_status),[1,2]);
      });
      await scenario('Finish old trip; missing-state next trip starts independently at the real GPS time',days,async ({send,flush,f,base,lifting}) => {
        f.state.rows=[{...base,trip_status:1,trip_date:format(lifting+600000)}, {...base,id:'2'}];
        await send(90,'destination'); await flush();
        assert.deepEqual(f.state.writes.map(w=>w.kind),['end','start']);
        assert.equal(f.state.rows[1].trip_status,1); // Same timestamp cannot also complete the new trip.
        assert.equal(f.state.rows[1].trip_date,format(lifting+90*60000));
      });
      await scenario('Invalid GPS fix rejected without queued SQL',days,async ({send,f,store}) => {
        await assert.rejects(send(10,'road',{gps_fix:'0'}));
        assert.equal((await store.pendingBatch(vehicle)).length,0); assert.equal(f.state.writes.length,0);
      });
    }
    const folder=path.join(__dirname,'..','simulation-results'); fs.mkdirSync(folder,{recursive:true});
    fs.writeFileSync(path.join(folder,'offline-results.json'),JSON.stringify({
      scope:'Application pipeline with Redis/Lua emulator; MySQL transaction adapter simulated, no production access',
      fixed_receive_time_utc:new Date(NOW).toISOString(), count:results.length, results },null,2));
    const lines=['# Offline historical GPS simulation','',
      `${results.length} scenarios passed. Each scenario is run with both one-day and two-day delayed GPS packets.`,
      '', 'Received: 30 September 2026, 08:00 UTC (13:30 IST). GPS date_time is UTC; saved SQL dates are IST.',
      '', 'Application MQTT parsing, Redis inbox/journal/Lua, history replay, repository decisions and SQL parameters are exercised. MySQL is a simulated transaction adapter; this is not a production database or broker test.',
      '', '| Scenario | Delay (days) | Status | Saved start (IST) | Saved end (IST) | Result |',
      '|---|---:|---:|---|---|---|'];
    for(const r of results) { const t=r.trips[0]||{}; lines.push(`| ${r.scenario} | ${r.offline_days} | ${t.status??'-'} | ${t.start_ist||'-'} | ${t.end_ist||'-'} | PASS |`); }
    lines.push('', 'Status: 0 pending, 1 started, 2 completed. Existing state requires source crossing evidence. A newly introduced pending trip with absent Redis state assumes source IN at lifting, then starts on actual OUT GPS after lifting.', '',
      'PASS means the expected result occurred, including scenarios that must remain open or pending. No arrival is inferred without GPS evidence. A destination packet at/before the saved start cannot close a trip. Sorting applies to currently queued packets; arbitrarily late evidence does not rewrite committed timestamps.', '',
      'Reproduce: npm ci; python3 -m pip install "fakeredis[lua]"; python3 scripts/run-redis-emulator-check.py');
    fs.writeFileSync(path.join(folder,'OFFLINE_SIMULATION.md'),lines.join('\n')+'\n');
    console.log(`${results.length} offline scenarios PASS; reports in simulation-results/`);
  } finally {
    const keys=await redis.keys(`${cfg.prefix}:*`); if(keys.length) await redis.del(...keys);
    redis.disconnect();
  }
}
main().catch(e=>{ console.error(e); process.exitCode=1; });
