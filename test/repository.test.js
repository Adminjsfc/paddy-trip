'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { Repository, START, END, INSERT_EVENT } = require('../src/repository');
const base = { id: '1', shipment_no: 'SHP-1', vehicle_id: 12, vehicle_no: 'JH01AB1234',
  status: 'DISPATCHED', trip_status: 0, lifting_date: '2026-09-30 10:00:00',
  trip_date: null, last_gps_at: null, source_lat: 23, source_lng: 85, source_radius: 0.2,
  destination_lat: 24, destination_lng: 86, destination_radius: 0.2, time_duration: 60 };
const packet = { vehicle_no: 'JH01AB1234', lat: 24, lng: 86, ts: Date.parse('2026-09-30T10:02:00Z') };
function fake(rows, { lastEnd = null, failStart = false, loseCommitResponse = false } = {}) {
  let snapshot, intent = null;
  const calls = [], c = {
    rows: structuredClone(rows.map(x => ({ ...x, status: Number(x.trip_status) === 0 ? 'DISPATCHED'
      : Number(x.trip_status) === 1 ? 'IN_TRANSIT' : 'ARRIVED' }))),
    commits: 0, rollbacks: 0, released: false,
    query: async sql => { calls.push(sql); return [[], []]; },
    beginTransaction: async () => { snapshot = structuredClone(c.rows); },
    commit: async () => { c.commits++; if (loseCommitResponse) { loseCommitResponse = false; throw new Error('commit response lost'); } },
    rollback: async () => { c.rows = snapshot; c.rollbacks++; },
    release: () => { c.released = true; },
    execute: async (sql, args) => {
      calls.push(sql);
      if (sql.includes('FROM vehicles') && sql.includes('FOR UPDATE')) return [[{ id: 12 }]];
      if (sql.includes('FROM shipments') && sql.includes('FOR UPDATE')) return [c.rows.map(x => ({ id: x.id }))];
      if (sql.includes('MAX(completion_date)')) return [[{ last_end: lastEnd }]];
      if (sql === START) {
        if (failStart) throw Object.assign(new Error('lock wait'), { code: 'ER_LOCK_WAIT_TIMEOUT' });
        assert.equal(args[0], '2026-09-30 10:02:00.000');
        assert.equal(args[1], args[0]);
        const t = c.rows.find(x => x.id === args[2]);
        if (!t || t.status !== 'DISPATCHED' || t.trip_status !== 0) return [{ affectedRows: 0 }];
        t.status = 'IN_TRANSIT'; t.trip_status = 1; t.trip_date = args[0]; t.last_gps_at = args[0];
        return [{ affectedRows: 1 }];
      }
      if (sql === END) {
        const t = c.rows.find(x => x.id === args[2]);
        if (!t || t.status !== 'IN_TRANSIT' || t.trip_status !== 1) return [{ affectedRows: 0 }];
        t.status = 'ARRIVED'; t.trip_status = 2; t.completion_date = args[0]; t.last_gps_at = args[0];
        return [{ affectedRows: 1 }];
      }
      if (sql === INSERT_EVENT) return [{ affectedRows: 1 }];
      const active = x => ['DISPATCHED', 'IN_TRANSIT'].includes(x.status) && x.trip_status < 2;
      return [structuredClone(c.rows.filter(x => active(x) ||
        (sql.includes('OR s.id IN') && args.slice(1).map(String).includes(String(x.id)))))];
    }
  };
  const db = {
    writeExecute: async (sql, args) => (await c.execute(sql, args))[0],
    withTransaction: async (work, options) => {
      assert.equal(options.isolationLevel, 'READ COMMITTED');
      await c.beginTransaction();
      let committing = false;
      try { const result = await work(db); committing = true; await c.commit(); return result; }
      catch (e) { if (!committing) await c.rollback(); throw e; }
      finally { c.release(); }
    }
  };
  const repo = new Repository(db);
  return { c, calls, repo, intent: () => intent,
    run: packets => repo.transitionBatch(Array.isArray(packets) ? packets : [packets], {
      intent, done: new Set(), geofences: Object.fromEntries(rows.map(t => [String(t.id),
        require('../src/geofence-state').observe(null,{ ...t, status: Number(t.trip_status) === 0 ? 'DISPATCHED' : 'IN_TRANSIT' },
          {...packet,lat:23,lng:85,ts:packet.ts-1000},require('../src/config'))])), save: async value => { intent = structuredClone(value); calls.push('REDIS_INTENT'); }
    }) };

}
test('end then start share one transaction with packet-based timestamps', async () => {
  const f = fake([{ ...base, trip_status: 1, trip_date: '2026-09-30 10:01:00' }, { ...base, id: '2' }]);
  const result = await f.run(packet);
  assert.deepEqual(result.events.map(e => e.kind), ['trip_ended', 'trip_started']);
  assert.equal(result.rows.length, 1); assert.equal(result.rows[0].id, '2');
  assert.equal(f.c.commits, 1); assert.ok(f.c.released);
  assert.ok(f.calls.indexOf(END) < f.calls.indexOf(START));
});
test('failed start rolls back end and releases connection', async () => {
  const f = fake([{ ...base, trip_status: 1, trip_date: '2026-09-30 10:01:00' }, { ...base, id: '2' }], { failStart: true });
  await assert.rejects(f.run(packet), /lock wait/);
  assert.equal(f.c.commits, 0); assert.equal(f.c.rollbacks, 1);
  assert.equal(f.c.rows[0].trip_status, 1); assert.ok(f.c.released);
});
test('already completed database trip is not changed by a retry', async () => {
  const f = fake([{ ...base, trip_status: 2 }]);
  assert.equal((await f.run(packet)).events.length, 0);
  assert.equal(f.calls.includes(START), false); assert.equal(f.calls.includes(END), false);
});
test('historical trip uses its own lifting time regardless of another trip completion', async () => {
  const f = fake([base], { lastEnd: '2026-09-30 10:03:00' });
  assert.equal((await f.run(packet)).events.length, 1);
  assert.equal(f.c.rows[0].trip_status, 1);
});


test('replica refresh filters completed primary state and repairs stale start state', async () => {
  const calls = [];
  const replica = [{ ...base, route_id: 10 }, { ...base, id: '2', route_id: 10 }];
  const primary = [{ ...base, route_id: 10, c_date: base.lifting_date, trip_status: 1, trip_date: '2026-09-30 10:01:00' }];
  const repo = new Repository({ readExecute: async (sql, args, options) => {
    calls.push(options?.primary ? 'primary' : 'read');
    return options?.primary ? primary : replica;
  } });
  const rows = await repo.refresh(0);
  assert.deepEqual(calls, ['read', 'primary']);
  assert.equal(rows.length, 1); assert.equal(rows[0].trip_status, 1);
  assert.equal(rows[0].trip_date, '2026-09-30 10:01:00');
});
test('refresh uses primary shipment geometry instead of stale replica geometry', async () => {
  const repo = new Repository({ readExecute: async (sql, args, options) =>
    [{ ...base, source_lat: options?.primary ? 22 : 23 }] });
  const rows = await repo.refresh(0);
  assert.equal(rows.length, 1);
  assert.equal(rows[0].source_lat, 22);
});


test('lost COMMIT response retries from fixed Redis intent without starting another pending trip', async () => {
  const f = fake([base, { ...base, id: '2', lifting_date: '2026-10-01 10:00:00' }], { loseCommitResponse: true });
  await assert.rejects(f.run(packet), /commit response lost/);
  assert.equal(f.intent().operations.length, 1);
  assert.equal(f.c.rows[0].trip_status, 1);
  // Simulate another actor completing the first trip before this queue retries.
  f.c.rows[0].status = 'ARRIVED'; f.c.rows[0].trip_status = 2;
  const retry = await f.run(packet);
  assert.equal(retry.events.length, 0);
  assert.equal(f.c.rows[1].trip_status, 0);
});
test('Redis intent survives SQL rollback for an exact-target retry', async () => {
  const f = fake([base], { failStart: true });
  await assert.rejects(f.run(packet));
  assert.equal(f.intent().operations.length, 1); assert.equal(f.c.rows[0].trip_status, 0);
});

test('outage backlog replays source departure then destination arrival in GPS order', async () => {
  const f = fake([base]);
  const departure = { ...packet, lat: 23.01, lng: 85 };
  const arrival = { ...packet, ts: packet.ts + 60000 };
  // Deliberately pass reversed order; repository must sort before applying changes.
  const result = await f.run([arrival, departure]);
  assert.deepEqual(result.events.map(e => e.kind), ['trip_started', 'trip_ended']);
  assert.equal(f.c.rows[0].trip_status, 2);
  assert.equal(f.c.rows[0].trip_date, '2026-09-30 10:02:00.000');
  assert.equal(f.c.rows[0].completion_date, '2026-09-30 10:03:00.000');
  assert.equal(f.intent().operations.length, 2); assert.equal(f.c.commits, 1);
});

test('Redis journal is persisted before shipment updates and events use the existing event table', async () => {
  const f = fake([base]); await f.run(packet);
  assert.ok(f.calls.indexOf('REDIS_INTENT') < f.calls.indexOf(START));
  assert.ok(f.calls.includes(INSERT_EVENT));
  assert.equal(f.calls.some(sql => /CREATE TABLE|tbl_trip_worker_|tbl_trip\b/.test(sql)), false);
});
test('changed trip route on saved-intent retry stays pending instead of overwriting', async () => {
  const f = fake([base], { failStart: true });
  await assert.rejects(f.run(packet), /lock wait/);
  f.c.rows[0].source_lat = 22;
  await assert.rejects(f.run(packet), { code: 'TRIP_INTENT_CONFLICT' });
  assert.equal(f.c.rows[0].trip_status, 0);
});
test('failure to persist Redis intent prevents every SQL UPDATE', async () => {
  const f = fake([base]);
  await assert.rejects(f.repo.transitionBatch([packet], {
    save: async () => { throw new Error('Redis unavailable'); }
  }), /Redis unavailable/);
  assert.equal(f.calls.includes(START), false);
});
test('Redis transition marker suppresses the same trip action during replay', async () => {
  const f = fake([base]);
  const result = await f.repo.transitionBatch([packet], {
    done: new Set([require('../src/repository').transitionId(packet, base.id, 'start')]),
    geofences: { [base.id]: require('../src/geofence-state').observe(null, base,
      {...packet, lat:23, lng:85, ts:packet.ts-1000}, require('../src/config')) }, save: async () => {}
  });
  assert.equal(result.events.length, 0); assert.equal(f.c.rows[0].trip_status, 0);
});

test('lost commit for an entire start/end backlog reconciles both fixed operations', async () => {
  const f = fake([base], { loseCommitResponse: true });
  const departure = { ...packet, lat: 23.01, lng: 85 };
  const arrival = { ...packet, ts: packet.ts + 60000 };
  await assert.rejects(f.run([arrival, departure]), /commit response lost/);
  assert.equal(f.c.rows[0].trip_status, 2);
  f.c.rows.push({ ...base, id: '2' });
  const result = await f.run([arrival, departure]);
  assert.equal(result.events.length, 0);
  assert.equal(f.c.rows[1].trip_status, 0);
});

test('offline history inside source keeps pending; departure after lifting starts at GPS time', async () => {
  const f = fake([base]);
  const source = { ...packet, lat: 23, lng: 85, ts: packet.ts - 60000, packet_type: 'H' };
  const result = await f.run([source, { ...packet, lat: 23.01, lng: 85, packet_type: 'H' }]);
  assert.deepEqual(result.events.map(e => e.kind), ['trip_started']);
  assert.equal(f.c.rows[0].trip_date, '2026-09-30 10:02:00.000');
});
test('historical departure at or before lifting cannot start trip', async () => {
  const f = fake([base]);
  await f.run([{ ...packet, ts: packet.ts - 120000 }, { ...packet, ts: packet.ts - 180000 }]);
  assert.equal(f.c.rows[0].trip_status, 0);
});
test('offline destination arrival closes using GPS time despite later server receipt', async () => {
  const f = fake([{ ...base, trip_status: 1, trip_date: '2026-09-30 10:01:00',
    expected_arrival_time: '2026-09-30 11:01:00' }]);
  const result = await f.run({ ...packet, packet_type: 'H', server_date_time: packet.ts + 6*3600000 });
  assert.deepEqual(result.events.map(e => e.kind), ['trip_ended']);
  assert.equal(f.c.rows[0].completion_date, '2026-09-30 10:02:00.000');
});
test('late actual arrival still closes rather than leaving trip permanently open after ETA', async () => {
  const f = fake([{ ...base, trip_status: 1, trip_date: '2026-09-30 10:01:00',
    expected_arrival_time: '2026-09-30 11:01:00' }]);
  await f.run({ ...packet, ts: packet.ts + 2*3600000, packet_type: 'H' });
  assert.equal(f.c.rows[0].completion_date, '2026-09-30 12:02:00.000');
});
test('destination points at or before saved start cannot close the trip', async () => {
  const f = fake([{ ...base, trip_status: 1, trip_date: '2026-09-30 10:02:00' }]);
  await f.run([{ ...packet, ts: packet.ts - 1000 }, packet]);
  assert.equal(f.c.rows[0].trip_status, 1);
});
test('offline trip remains open with only outside-destination history', async () => {
  const f = fake([{ ...base, trip_status: 1, trip_date: '2026-09-30 10:01:00' }]);
  await f.run({ ...packet, lat: 23.5, lng: 85.5, packet_type: 'H' });
  assert.equal(f.c.rows[0].trip_status, 1);
});
test('reversed mixed live/history backlog chooses earliest available qualifying arrival', async () => {
  const f = fake([base]);
  await f.run([{ ...packet, ts: packet.ts + 180000, packet_type: 'L' },
    { ...packet, ts: packet.ts + 60000, packet_type: 'H' },
    { ...packet, lat: 23.01, lng: 85, packet_type: 'H' }]);
  assert.equal(f.c.rows[0].trip_date, '2026-09-30 10:02:00.000');
  assert.equal(f.c.rows[0].completion_date, '2026-09-30 10:03:00.000');
});
test('after source observation, destination point starts only; same timestamp cannot also end', async () => {
  const f = fake([base]);
  const result = await f.run(packet);
  assert.deepEqual(result.events.map(e => e.kind), ['trip_started']);
  assert.equal(f.c.rows[0].trip_status, 1);
});
