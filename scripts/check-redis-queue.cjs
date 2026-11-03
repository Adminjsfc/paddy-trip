'use strict';
// Run against an isolated test Redis, or through run-redis-emulator-check.py.
const assert = require('node:assert/strict');
process.env.REDIS_PREFIX = `jsfc:trip:queue-test:${require('node:crypto').randomUUID()}`;
const { connection, Store, ingest } = require('../src/redis-store');
const { shardFor } = require('../src/domain');
async function main() {
  const redis = new (require('ioredis'))(process.env.REDIS_URL || 'redis://127.0.0.1:6379/0',
    { lazyConnect: true, enableReadyCheck: false, maxRetriesPerRequest: 1 });
  await redis.connect();
  const vehicle = 'JH16B5498', store = new Store(redis, shardFor(vehicle));
  const ts = Math.floor(Date.now() / 1000) * 1000 - 10000;
  const packet = { vehicle_no: vehicle, lat: 24.64, lng: 87.82, packet_datetime: ts, gps_fix: '1', packet_type: 'L' };
  const k = store.keys(vehicle);
  try {
    assert.equal(await store.acquire(), true);
    await ingest(redis, packet);
    assert.equal(await redis.ttl(k.pendingOrder), -1);
    assert.equal(await redis.ttl(k.pendingData), -1);
    assert.equal((await store.pendingBatch(vehicle)).length, 1);
    // Delete rolling history only: the uncommitted inbox payload must survive.
    await redis.del(k.history, k.packets);
    assert.equal((await store.pendingBatch(vehicle))[0].ts, ts);
    // Restart-like Store instance with the current ownership token reads the same inbox.
    const restarted = new Store(redis, store.shard); restarted.token = store.token;
    assert.equal((await restarted.pendingBatch(vehicle)).length, 1);
    // Same event redelivery deduplicates queue membership.
    await ingest(redis, packet); await ingest(redis, { ...packet, packet_type: 'H' });
    assert.equal((await store.pendingBatch(vehicle)).length, 1);
    await assert.rejects(ingest(redis, { ...packet, lat: 25 }), /Conflicting/);
    // New arrivals must survive acknowledgement of the earlier committed batch.
    const committed = await store.pendingBatch(vehicle);
    await ingest(redis, { ...packet, packet_datetime: ts + 1000 });
    const intent = { id: 'first', vehicle_no: vehicle, packets: committed,
      operations: [{ trip_id: '1', kind: 'start', packet: committed[0] }],
      geofences: { '1': { source_status: 'OUTSIDE', destination_status: 'OUTSIDE', gps_timestamp: ts } } };
    await store.saveIntent(vehicle, intent);
    assert.deepEqual(await restarted.loadIntent(vehicle), intent);
    assert.equal(await redis.ttl(`${store.base}:vehicle:${vehicle}:write-intent`), -1);
    await assert.rejects(store.finishIntent(vehicle, { ...intent, id: 'wrong' }), /INTENT_CHANGED/);
    const stale = new Store(redis, store.shard);
    await assert.rejects(stale.finishIntent(vehicle, intent), /LEASE_LOST/);
    assert.equal((await store.pendingBatch(vehicle)).length, 2);
    await store.finishIntent(vehicle, intent);
    const eventId = require('../src/repository').transitionId(committed[0], '1', 'start');
    assert.equal((await store.completedEvents(vehicle, [eventId])).has(eventId), true);
    assert.equal(await store.loadIntent(vehicle), null);
    assert.deepEqual(await restarted.loadGeofences(vehicle), intent.geofences);
    const individualKey = `${store.base}:vehicle:${vehicle}:trip:1:geofence`;
    assert.deepEqual(JSON.parse(await redis.get(individualKey)), intent.geofences['1']);
    assert.equal(await redis.ttl(`${store.base}:vehicle:${vehicle}:trip-geofences`), -1);

    assert.equal((await store.pendingBatch(vehicle)).length, 1);
    assert.equal((await store.pendingBatch(vehicle))[0].ts, ts + 1000);
    const second = { id: 'second', packets: await store.pendingBatch(vehicle),
      operations: [{ trip_id: '1', kind: 'end', packet: { ...committed[0], ts: ts+1000 } }],
      geofences: { '1': { ...intent.geofences['1'], trip_status: 2 } } };
    await store.saveIntent(vehicle, second);
    assert.equal(await redis.exists(individualKey), 1); // Retained until confirmed finalization.
    await store.finishIntent(vehicle, second, []);
    assert.equal(await redis.exists(individualKey), 0);
    assert.deepEqual(await store.loadGeofences(vehicle), {});
    const surviving = { id: 'third', packets: [], operations: [], geofences: {
      '1': { trip_status: 2 }, '2': { trip_status: 1, source_status: 'OUTSIDE', destination_status: 'OUTSIDE' }
    } };
    await store.saveIntent(vehicle, surviving);
    await store.finishIntent(vehicle, surviving, [{ id: '2', trip_status: 1 }]);
    assert.equal(await redis.exists(individualKey), 0);
    assert.equal(await redis.exists(`${store.base}:vehicle:${vehicle}:trip:2:geofence`), 1);
    assert.deepEqual(Object.keys(await store.loadGeofences(vehicle)), ['2']);
    assert.equal((await store.completedEvents(vehicle, [eventId])).has(eventId), true);
    assert.deepEqual(await store.pendingVehicles(), []);
    // Historical ingestion cannot move the live location backwards.
    const live = JSON.parse(await redis.hget(k.live, 'packet'));
    await ingest(redis, { ...packet, packet_datetime: ts - 1000 });
    assert.equal(JSON.parse(await redis.hget(k.live, 'packet')).gps_timestamp, live.gps_timestamp);
    // Replaying retained history must not rewind itself or regress live location.
    await redis.del(k.rewind);
    const replay = { vehicle_no: vehicle, lat: packet.lat, lng: packet.lng,
      ts: ts - 2000, packet_type: 'H', gps_fix: '1' };
    await restarted.enqueueHistory(vehicle, [replay]);
    assert.equal(await redis.exists(k.rewind), 0);
    assert.equal(await redis.ttl(k.pendingOrder), -1);
    assert.ok((await store.pendingBatch(vehicle)).some(p => p.ts === replay.ts));
    assert.equal(JSON.parse(await redis.hget(k.live, 'packet')).gps_timestamp, live.gps_timestamp);
    await assert.rejects(stale.enqueueHistory(vehicle, [replay]), /LEASE_LOST/);
    console.log('Redis queue checks passed: no expiry, history independence, restart recovery, deduplication, safe acknowledgement, latest location.');
  } finally {
    const keys = await redis.keys(`${process.env.REDIS_PREFIX}:*`);
    if (keys.length) await redis.del(...keys);
    redis.disconnect();
  }
}
main().catch(e => { console.error(e); process.exitCode = 1; });
