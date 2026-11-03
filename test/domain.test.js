'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const d = require('../src/domain');
const cfg = { offset: '+05:30', radiusMultiplier: 1, historyMs: 7 * 86400000, futureTolerance: 120000 };
const ms = s => Date.parse(s + '+05:30');
const pending = (extra = {}) => ({ id: '1', trip_status: 0, lifting_date: '2026-09-30 10:00:00',
  trip_date: null, source_lat: 23, source_lng: 85, source_radius: 200,
  destination_lat: 24, destination_lng: 86, destination_radius: 200, time_duration: 60, ...extra });
const packet = (extra = {}) => ({ vehicle_no: 'JH01AB1234', lat: 23.01, lng: 85,
  ts: ms('2026-09-30T10:01:00'), ...extra });
test('CRC32 agrees with standard reference and stable normalized shard', () => {
  assert.equal(d.crc32('123456789'), 0xcbf43926);
  assert.equal(d.shardFor(' jh01ab1234 '), d.shardFor('JH01AB1234'));
  const used = new Set(Array.from({ length: 1000 }, (_, i) => d.shardFor(`JH01AB${i}`)));
  assert.equal(used.size, 8);
});
test('start is strictly after lifting and outside source', () => {
  assert.equal(d.plan([pending()], packet(), cfg).start[0].id, '1');
  assert.equal(d.plan([pending()], packet({ ts: ms('2026-09-30T10:00:00') }), cfg).start.length, 0);
  assert.equal(d.plan([pending()], packet({ lat: 23 }), cfg).start.length, 0);
});
test('missing geofence is never treated as outside', () => {
  for (const field of ['source_lat', 'source_lng', 'source_radius']) {
    assert.equal(d.plan([pending({ [field]: null })], packet(), cfg).start.length, 0);
  }
});
test('end requires started status, inside destination, strictly later packet', () => {
  const t = pending({ trip_status: 1, trip_date: '2026-09-30 10:01:00' });
  assert.equal(d.plan([t], packet({ lat: 24, lng: 86 }), cfg).end.length, 0);
  assert.equal(d.plan([t], packet({ lat: 24, lng: 86, ts: ms('2026-09-30T10:02:00') }), cfg).end.length, 1);
  assert.equal(d.plan([t], packet({ ts: ms('2026-09-30T10:02:00') }), cfg).end.length, 0);
});
test('all matching open trips close; another open trip does not block independent start', () => {
  const a = pending({ trip_status: 1, trip_date: '2026-09-30 10:00:00' });
  const b = { ...a, id: '2' }, next = pending({ id: '3' });
  const p = packet({ lat: 24, lng: 86 });
  const plan = d.plan([a, b, next], p, cfg);
  assert.deepEqual(plan.end.map(t => t.id), ['1', '2']);
  assert.equal(plan.start[0].id, '3');
  assert.equal(d.plan([a, { ...b, destination_lat: 25 }, next], p, cfg).start[0].id, '3');
});
test('start all qualifying pending trips in stable order', () => {
  const a = pending({ id: '9007199254740993' }), b = pending({ id: '9007199254740992' });
  assert.deepEqual(d.plan([a, b], packet(), cfg).start.map(t => t.id), [b.id,a.id]);
  assert.equal(d.plan([pending({ source_lat: 23.01 }), pending({ id: '2' })], packet(), cfg).start[0].id, '2');
});
test('new trip cannot also end on its start packet', () => {
  const p = packet({ lat: 24, lng: 86 });
  const a = d.plan([pending()], p, cfg);
  assert.equal(a.end.length, 0); assert.equal(a.start[0].id, '1');
});
test('timezone conversion and seconds precision preserve SQL chronology', () => {
  const ts = ms('2026-09-30T10:00:00');
  assert.equal(d.sqlTime(ts, cfg.offset), '2026-09-30 10:00:00.000');
  assert.equal(d.dbTime('2026-09-30 10:00:00', cfg.offset), ts);
  assert.throws(() => d.packetTime('2026-09-30 10:00:00'));
  const n = d.normalizePacket({ vehicle_no: 'JH01AB1234', lat: 23, lng: 85, packet_datetime: ts + 123 }, cfg, ts + 1000);
  assert.equal(n.ts, ts);
});
test('reject bad coordinates, future packets and expired history', () => {
  const now = ms('2026-09-30T10:00:00');
  const raw = { vehicle_no: 'JH01AB1234', lat: 23, lng: 85, packet_datetime: now };
  assert.throws(() => d.normalizePacket({ ...raw, lat: null }, cfg, now));
  assert.throws(() => d.normalizePacket({ ...raw, lng: 181 }, cfg, now));
  assert.throws(() => d.normalizePacket({ ...raw, packet_datetime: now + 121000 }, cfg, now));
  assert.throws(() => d.normalizePacket({ ...raw, packet_datetime: now - cfg.historyMs - 1000 }, cfg, now));
});
test('metres and kilometres have explicit conversion', () => {
  const p = packet();
  assert.equal(d.inside(p, pending({ source_radius: 2 }), 'source', 1000), true);
  assert.equal(d.inside(p, pending({ source_radius: 2 }), 'source', 1), false);
});
test('ETA duration does not block departure; invalid lifting time does', () => {
  assert.equal(d.plan([pending({ time_duration: null })], packet(), cfg).start[0].id, '1');
  assert.equal(d.plan([pending({ time_duration: -1 })], packet(), cfg).start[0].id, '1');
  assert.equal(d.plan([pending({ lifting_date: null })], packet(), cfg).start.length, 0);
});
