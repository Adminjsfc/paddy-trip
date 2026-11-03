'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { flushVehicle, reconnectable } = require('../src/write-buffer');
const packet = { vehicle_no: 'JH16B5498', ts: 1790754413000 };
function setup({ failDB = false, failAck = false, failPublish = false } = {}) {
  const calls = []; let queued = true, committed = false, writes = 0, intent = null;
  const store = {
    assertOwner: async () => calls.push('lease'),
    loadGeofences: async () => ({}),
    loadIntent: async () => intent,
    completedEvents: async () => new Set(),
    saveIntent: async (v, value) => { intent = value; calls.push('journal'); },
    pendingBatch: async () => queued ? [packet] : [],
    publish: async () => { calls.push('cache'); if (failPublish) throw new Error('redis down'); },
    finishIntent: async () => { calls.push('ack'); if (failAck) throw new Error('ack lost'); queued = false; intent = null; }
  };
  const repo = { transitionBatch: async (packets, journal) => {
    if (!journal.intent) await journal.save({ id: "fixed-id", packets, operations: [] });
    calls.push('mysql'); if (failDB) throw new Error('mysql unavailable');
    if (!committed) { committed = true; writes++; }
    return { rows: [], events: [], intent };
  } };
  return { store, repo, calls, queued: () => queued, writes: () => writes,
    recover: () => { failDB = false; failAck = false; failPublish = false; } };
}
test('pending entry is acknowledged only after MySQL commit and cache update', async () => {
  const f = setup(); await flushVehicle(f.store, f.repo, packet.vehicle_no);
  assert.deepEqual(f.calls, ['lease','journal','mysql','cache','ack']); assert.equal(f.queued(), false);
});
test('database outage leaves queued payload intact until recovery', async () => {
  const f = setup({ failDB: true });
  await assert.rejects(flushVehicle(f.store, f.repo, packet.vehicle_no));
  assert.equal(f.queued(), true); assert.equal(f.calls.includes('ack'), false);
  f.recover(); await flushVehicle(f.store, f.repo, packet.vehicle_no);
  assert.equal(f.queued(), false); assert.equal(f.writes(), 1);
});
test('failed Redis acknowledgement retries without a second MySQL effect', async () => {
  const f = setup({ failAck: true });
  await assert.rejects(flushVehicle(f.store, f.repo, packet.vehicle_no));
  assert.equal(f.queued(), true); assert.equal(f.writes(), 1);
  f.recover(); await flushVehicle(f.store, f.repo, packet.vehicle_no);
  assert.equal(f.writes(), 1); assert.equal(f.queued(), false);
});
test('cache failure cannot discard a committed pending job', async () => {
  const f = setup({ failPublish: true });
  await assert.rejects(flushVehicle(f.store, f.repo, packet.vehicle_no));
  assert.equal(f.queued(), true); assert.equal(f.calls.includes('ack'), false);
});
test('only connection failures request pool recreation', () => {
  assert.equal(reconnectable({ code: 'ECONNRESET' }), true);
  assert.equal(reconnectable({ message: 'Pool is closed.' }), true);
  assert.equal(reconnectable({ code: 'ER_PARSE_ERROR' }), false);
});
