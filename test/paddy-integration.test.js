'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const cfg = require('../src/config');
const { inside, plan, sqlTime } = require('../src/domain');
const { canStart } = require('../src/geofence-state');
const { locationTripLog } = require('../src/location-log');
const { gpsBinding, persistGps } = require('../src/gps-persistence');
const { Repository, START, END, INSERT_EVENT } = require('../src/repository');
const ts = Date.parse('2026-10-02T10:01:00Z');
const trip = { id: '1', vehicle_id: 12, vehicle_no: 'JH01AB1234', shipment_no: 'SHP-1',
  status: 'DISPATCHED', trip_status: 0, lifting_date: '2026-10-02 10:00:00',
  source_lat: 23, source_lng: 85, source_radius: 200 / 1000,
  destination_lat: 24, destination_lng: 86, destination_radius: 200 / 1000 };
const point = { vehicle_no: trip.vehicle_no, lat: 23.00045, lng: 85, ts, packet_type: 'L' };

test('200 metre paddy geofence includes a point 50 metres away and excludes 300 metres', () => {
  assert.equal(inside(point, trip, 'source', cfg.radiusMultiplier), true);
  assert.equal(inside({ ...point, lat: 23.003 }, trip, 'source', cfg.radiusMultiplier), false);
  const log = locationTripLog(trip.vehicle_no, trip, point, cfg);
  assert.equal(log.source_radius_km, 0.2);
  assert.equal(log.source_status, 'INSIDE');
  assert.equal(log.gps_datetime_ist, '2026-10-02 15:31:00.000');
  assert.equal(sqlTime(ts, cfg.offset), '2026-10-02 10:01:00.000');
});

test('source OUT after dispatch starts with absent, existing OUT or incompatible Redis state', () => {
  const outside = { ...point, lat: 23.003 };
  for (const state of [null, {}, { source_status: 'OUTSIDE', source_exited_at: null }]) {
    assert.equal(canStart(state, trip, outside, cfg), true);
    assert.equal(canStart(state, trip, point, cfg), false);
    assert.equal(canStart(state, trip, { ...outside, ts: ts - 60000 }, cfg), false);
  }
  assert.equal(canStart(null, { ...trip, source_lat: null }, outside, cfg), false);
});

function gpsDb() {
  const state = { device: { device_id: 9, imei_id: 7 }, packets: [], latest: null, calls: [] };
  const db = { writeExecute: async (sql, args) => {
    state.calls.push(sql);
    if (sql.includes('FROM gps_devices')) return state.device ? [state.device] : [];
    if (sql.includes('FROM gps_packets')) return state.packets.filter(p => p.when === args[1]);
    if (sql.startsWith('INSERT INTO gps_packets')) {
      state.packets.push({ when: args[1], latitude: args[2], longitude: args[3], raw: JSON.parse(args[5]) });
      return { affectedRows: 1 };
    }
    if (sql.startsWith('UPDATE gps_devices')) {
      if (!state.latest || state.latest.when < args[0]) state.latest = { when: args[0], latitude: args[1] };
      return { affectedRows: 1 };
    }
    throw new Error('Unexpected query ' + sql);
  } };
  return { db, state };
}
const intent = packets => ({ vehicle_no: trip.vehicle_no, gpsDevice: { device_id: 9, imei_id: 7 }, packets });

test('paddy history persistence is idempotent and late history cannot regress live location', async () => {
  const { db, state } = gpsDb();
  assert.deepEqual(await gpsBinding(db, 12), { device_id: 9, imei_id: 7 });
  const batch = intent([point, { ...point, ts: ts - 1000, lat: 23 }]);
  await persistGps(db, 12, batch);
  await persistGps(db, 12, batch);
  assert.equal(state.packets.length, 2);
  assert.equal(state.latest.when, '2026-10-02 10:01:00.000');
  assert.equal(state.latest.latitude, point.lat);
  assert.equal(state.packets[0].raw.speed_available, false);
});

test('GPS timestamp conflicts and device reassignment retain work instead of overwriting history', async () => {
  const { db, state } = gpsDb();
  await persistGps(db, 12, intent([point]));
  await assert.rejects(persistGps(db, 12, intent([{ ...point, lat: 22 }])), { code: 'GPS_TIMESTAMP_CONFLICT' });
  state.device = { device_id: 10, imei_id: 8 };
  await assert.rejects(persistGps(db, 12, intent([point])), { code: 'TRIP_BINDING_CHANGED' });
  assert.equal(state.packets.length, 1);
});

test('older saved intents never attach GPS to a newly assigned device', async () => {
  const { db, state } = gpsDb();
  await persistGps(db, 12, { packets: [point] });
  assert.equal(state.calls.length, 0);
});

test('actual repository commits corrected source departure, destination arrival and GPS history together', async () => {
  const { db, state } = gpsDb();
  const row = structuredClone(trip);
  const gpsExecute = db.writeExecute;
  let saved, committed = false;
  db.withTransaction = async fn => { const result = await fn(db); committed = true; return result; };
  db.writeExecute = async (sql, args) => {
    if (sql.includes('gps_devices') || sql.includes('gps_packets')) return gpsExecute(sql, args);
    if (sql.includes('FROM vehicles')) return [{ id: 12 }];
    if (sql.includes('FOR UPDATE')) return [{ id: row.id }];
    if (sql === START || sql === END) {
      assert.ok(saved, 'journal must precede writes');
      assert.equal(args[2], row.id);
      if (sql === START) Object.assign(row, {
        status: 'IN_TRANSIT', trip_status: 1, trip_date: args[0],
        trip_started_at: args[0], trip_completed_at: null, last_gps_at: args[1]
      });
      else Object.assign(row, {
        status: 'ARRIVED', trip_status: 2, completion_date: args[0],
        trip_completed_at: args[0], last_gps_at: args[1]
      });
      return { affectedRows: 1 };
    }
    if (sql === INSERT_EVENT) return { affectedRows: 1 };
    if (sql.includes('((s.status')) return [structuredClone(row)];
    return row.trip_status < 2 ? [structuredClone(row)] : [];
  };
  const result = await new Repository(db).transitionBatch([
    { ...point, lat: 24.00045, lng: 86, ts: ts + 1000 },
    { ...point, lat: 23.003 }
  ], { geofences: { '1': { source_status: 'OUTSIDE' } }, save: async value => { saved = value; } });
  assert.equal(committed, true);
  assert.deepEqual(result.events.map(e => e.kind), ['trip_started', 'trip_ended']);
  assert.equal(result.events[0].start_basis, 'SOURCE_OUT_AFTER_DISPATCH');
  assert.equal(row.trip_date, '2026-10-02 10:01:00.000');
  assert.equal(row.completion_date, '2026-10-02 10:01:01.000');
  assert.equal(row.trip_started_at, '2026-10-02 10:01:00.000');
  assert.equal(row.trip_completed_at, '2026-10-02 10:01:01.000');
  assert.equal(row.trip_status, 2);
  assert.equal(row.status, 'ARRIVED');
  assert.equal(state.packets.length, 2);
  assert.equal(state.latest.latitude, 24.00045);
});
