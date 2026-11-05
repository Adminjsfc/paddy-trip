'use strict';
const cfg = require('./config');
const { observeEvent, canStart, journeyState } = require('./geofence-state');
const { decisionsFor } = require('./transition-decisions');
const { createHash, randomUUID } = require('node:crypto');
const { plan, sqlTime, dbTime } = require('./domain');
const { gpsBinding, persistGps } = require('./gps-persistence');
// Keep this projection independent of WHERE so replica and primary reads use
// identical paddy shipment geometry and state fields.
const DETAILS_SELECT = `SELECT s.id, s.shipment_no AS trip_unique_id, s.shipment_no,
 UPPER(TRIM(v.registration_no)) AS vehicle_no, s.vehicle_id, s.status, s.trip_status,
 s.dispatched_at AS lifting_date, s.trip_started_at AS trip_date,
 s.trip_completed_at AS completion_date,
 s.trip_started_at,s.trip_completed_at,s.expected_completion_at,s.last_gps_at,
 s.source_lat, s.source_lng, s.source_radius_m / 1000.0 AS source_radius,
 s.destination_lat, s.destination_lng, s.destination_radius_m / 1000.0 AS destination_radius,
 NULL AS time_duration, s.route_id
 FROM shipments s JOIN vehicles v ON v.id = s.vehicle_id`;
// GPS owns departure and arrival; planning, dispatch, delivery and cancellation
// belong to the backend shipment workflow.
const openTripWhere = (alias = '') => {
  const p = alias ? `${alias}.` : '';
  return `((${p}status='DISPATCHED' AND ${p}trip_status=0) OR (${p}status='IN_TRANSIT' AND ${p}trip_status=1))`;
};
const SELECT = `${DETAILS_SELECT} WHERE ${openTripWhere('s')}`;
const START = `UPDATE shipments SET status='IN_TRANSIT', trip_status=1,
 trip_started_at=?, trip_completed_at=NULL,
 expected_completion_at=TIMESTAMPADD(MINUTE,(SELECT r.duration_minutes FROM shipment_routes r WHERE r.id=shipments.route_id),trip_started_at),
 source_geofence='OUT', destination_geofence='UNKNOWN', last_gps_at=?
 WHERE id=? AND vehicle_id=? AND status='DISPATCHED' AND trip_status=0 AND dispatched_at<?
 AND (last_gps_at IS NULL OR last_gps_at<?)`;
const END = `UPDATE shipments SET status='ARRIVED', trip_status=2,
 trip_completed_at=?, destination_geofence='IN', last_gps_at=?
 WHERE id=? AND vehicle_id=? AND status='IN_TRANSIT' AND trip_status=1 AND trip_started_at<?
 AND (last_gps_at IS NULL OR last_gps_at<?)`;
const INSERT_EVENT = `INSERT INTO shipment_events (shipment_id, event_type, occurred_at, detail)
 VALUES (?, ?, ?, ?)`;
class Repository {
  constructor(database) { this.db = database; }
  async activeForVehicle(v) {
    return this.db.readExecute(`${SELECT} AND UPPER(TRIM(v.registration_no)) = ? ORDER BY s.id`, [v], { primary: true });
  }
  async refresh(shard) {
    const predicate = `MOD(CRC32(UPPER(TRIM(v.registration_no))), ${cfg.shardCount}) = ?`;
    const rows = await this.db.readExecute(`${SELECT} AND ${predicate} ORDER BY s.id`, [shard]);
    // Replica discovery, followed by a small primary-state projection. Never cache
    // completed/cancelled trips just because a secondary has not applied their update.
    const current = new Map();
    for (let i = 0; i < rows.length; i += 500) {
      const ids = rows.slice(i, i + 500).map(t => t.id);
      const states = await this.db.readExecute(`${DETAILS_SELECT}
        WHERE s.id IN (${ids.map(() => '?').join(',')})
        AND ${openTripWhere('s')}`, ids, { primary: true });
      for (const state of states) current.set(String(state.id), state);
    }
    return rows.flatMap(row => {
      const state = current.get(String(row.id));
      // Changed assignment/route must be rediscovered with matching joined data.
      if (!state || state.vehicle_no !== row.vehicle_no || String(state.vehicle_id) !== String(row.vehicle_id)) return [];
      return [{ ...row, ...state }];
    });
  }
  async transitionBatch(packets, journal) {
    if (!packets.length) return { rows: [], events: [], intent: null };
    if (!journal?.save) throw new Error('Redis write journal is required');
    const v = packets[0].vehicle_no;
    if (packets.some(p => p.vehicle_no !== v)) throw new Error('Mixed vehicle batch');
    return this.db.withTransaction(async db => {
      let intent = journal.intent;
      const vehicleRows = await db.writeExecute(
        'SELECT id FROM vehicles WHERE UPPER(TRIM(registration_no)) = ? FOR UPDATE', [v]);
      if (!vehicleRows.length) throw new Error(`Unknown paddy vehicle ${v}`);
      const vehicleId = vehicleRows[0].id;
      const targets = [...new Set((intent?.operations || []).map(o => o.trip_id))];
      const extra = targets.length ? ` OR id IN (${targets.map(() => '?').join(',')})` : '';
      await db.writeExecute(`SELECT id FROM shipments WHERE vehicle_id = ?
        AND (${openTripWhere()}${extra})
        ORDER BY id FOR UPDATE`, [vehicleId, ...targets]);
      const targetFilter = targets.length ? ` OR s.id IN (${targets.map(() => '?').join(',')})` : '';
      let rows = await db.writeExecute(`${DETAILS_SELECT} WHERE s.vehicle_id = ?
        AND (${openTripWhere('s')}${targetFilter})
        ORDER BY s.id`, [vehicleId, ...targets]);
      if (!intent) {
        if (journal.archiveCoverage && !require('./archive-recovery').covers(journal.archiveCoverage, rows, cfg)) {
          throw Object.assign(new Error('Primary trip configuration requires historical recovery before processing'),
            { code: 'ARCHIVE_RECOVERY_REQUIRED', rows });
        }
        const simulated = structuredClone(rows), operations = [];
        const geofences = structuredClone(journal.loadGeofences
          ? await journal.loadGeofences(rows.map(t => String(t.id))) : journal.geofences || {});
        const decisions = {};
        const ordered = [...packets].sort((a, b) => a.ts - b.ts);
        // Markers are scoped to vehicle + trip + action + GPS time. Legacy
        // vehicle-wide markers cannot suppress another trip's valid departure.
        const ids = rows.flatMap(t => ordered.flatMap(p =>
          ['start','end'].map(kind => transitionId(p, t.id, kind))));
        const done = journal.completed ? await journal.completed(ids) : journal.done || new Set();
        for (const p of ordered) {
          const eventGeofences = {};
          for (const t of simulated.filter(t => Number(t.trip_status) !== 2)) {
            // Prefer a recovered actual source visit over assuming an earlier outside point was departure.
            const observeCfg = journal.archiveCoverage?.sourceInside?.[String(t.id)]
              ? { ...cfg, allowMissingStateAssumption: false } : cfg;
            const observed = observeEvent(geofences[String(t.id)], t, p, observeCfg);
            geofences[String(t.id)] = observed.snapshot;
            eventGeofences[String(t.id)] = observed.current;
          }
          const duplicate = (t, kind) => done.has(transitionId(p, t.id, kind));
          const action = plan(simulated, p, cfg, (t, kind) => !duplicate(t, kind) &&
            (kind === 'end' || (!(cfg.departureRequiresCrossing !== false && journal.archiveCoverage?.sourceInside?.[String(t.id)] &&
              eventGeofences[String(t.id)]?.source_exit_basis === 'MISSING_REDIS_STATE') &&
              canStart(eventGeofences[String(t.id)], t, p, cfg))));
          Object.assign(decisions, decisionsFor(simulated, p, eventGeofences, action, duplicate, cfg));
          for (const t of action.end) {
            operations.push(makeOperation('end', t, p));
            t.status = 'ARRIVED'; t.trip_status = 2;
            t.completion_date = sqlTime(p.ts, cfg.offset); t.last_gps_at = t.completion_date;
          }
          for (const t of action.start) {
            operations.push(makeOperation('start', t, p, eventGeofences[String(t.id)]));
            t.status = 'IN_TRANSIT'; t.trip_status = 1;
            t.trip_date = sqlTime(p.ts, cfg.offset); t.last_gps_at = t.trip_date;
          }
        }
        for (const t of simulated) {
          const state = geofences[String(t.id)];
          if (state) {
            state.trip_status = Number(t.trip_status);
            state.journey_state = journeyState(t, state);
            state.last_decision = decisions[String(t.id)] || null;
          }
        }
        const gpsDevice = await gpsBinding(db, vehicleId);
        intent = { id: randomUUID(), policy: 'paddy-source-out-v4', vehicle_no: v, gpsDevice,
          packets: ordered, operations, geofences, decisions: Object.values(decisions) };
        // Persist exact target IDs and values BEFORE issuing any business UPDATE.
        await journal.save(intent);
      }
      await persistGps(db, vehicleId, intent);
      const events = [];
      for (const op of intent.operations) {
        const t = rows.find(row => String(row.id) === op.trip_id);
        if (!t) throw conflict(op, 'target row missing or reassigned');
        const when = sqlTime(op.packet.ts, cfg.offset);
        const status = t.status;
        const dateField = op.kind === 'start' ? t.trip_date : t.completion_date;
        const reached = op.kind === 'start'
          ? Number(t.trip_status) >= 1
          : Number(t.trip_status) === 2;
        if (reached && dbTime(dateField, cfg.offset) === op.packet.ts) {
          continue; // Commit already applied: never choose another shipment for this event.
        }
        if (t.last_gps_at && dbTime(t.last_gps_at, cfg.offset) >= op.packet.ts) {
          continue;
        }
        if (status !== (op.kind === 'start' ? 'DISPATCHED' : 'IN_TRANSIT') ||
            fingerprint(t) !== op.expected) {
          throw conflict(op, 'state or route changed; retained for review');
        }
        if (op.kind === 'start') {
          const r = await db.writeExecute(START, [when, when, op.trip_id, t.vehicle_id, when, when]);
          if (r.affectedRows !== 1) throw conflict(op, 'start precondition changed');
          t.status = 'IN_TRANSIT'; t.trip_status = 1; t.trip_date = when; t.last_gps_at = when;
        } else {
          const r = await db.writeExecute(END, [when, when, op.trip_id, t.vehicle_id, when, when]);
          if (r.affectedRows !== 1) throw conflict(op, 'end precondition changed');
          t.status = 'ARRIVED'; t.trip_status = 2; t.completion_date = when; t.last_gps_at = when;
        }
        const event = { kind: op.kind === 'start' ? 'trip_started' : 'trip_ended', trip_id: op.trip_id,
          vehicle_no: v, packet_datetime: when, gps_timestamp: op.packet.ts,
          latitude: op.packet.lat, longitude: op.packet.lng, packet_type: op.packet.packet_type,
          ...(op.kind === 'start' ? { start_basis: op.start_basis || 'OBSERVED_SOURCE_EXIT',
            assumed_source_in_at: op.assumed_source_in_at ?? null } : {}) };
        const eventType = op.kind === 'start' ? 'DEPARTED' : 'ARRIVED';
        await db.writeExecute(INSERT_EVENT, [op.trip_id, eventType, when, JSON.stringify(event)]);
        events.push(event);
      }
      const after = await db.writeExecute(`${SELECT} AND UPPER(TRIM(v.registration_no)) = ? ORDER BY s.id`, [v]);
      return { rows: after, evaluatedRows: rows, events, intent };
    }, { isolationLevel: 'READ COMMITTED' });
  }
}
function fingerprint(t) {
  return JSON.stringify([String(t.vehicle_id ?? ''), String(t.shipment_no ?? ''), t.status,
    dbTime(t.lifting_date, cfg.offset), dbTime(t.last_gps_at, cfg.offset),
    Number(t.trip_status) === 1 ? dbTime(t.trip_date, cfg.offset) : null,
    ...['source_lat','source_lng','source_radius','destination_lat','destination_lng','destination_radius','time_duration']
      .map(k => t[k] === null || t[k] === undefined ? null : Number(t[k]))]);
}
function makeOperation(kind, t, p, sourceState) {
  return { kind, trip_id: String(t.id), packet: p, expected: fingerprint(t),
    ...(kind === 'start' ? { start_basis: cfg.departureRequiresCrossing === false ? 'SOURCE_OUT_AFTER_DISPATCH' : sourceState?.source_exit_basis || 'OBSERVED_SOURCE_EXIT',
      assumed_source_in_at: sourceState?.source_assumed_in_at ?? null } : {}) };
}
function conflict(op, reason) { return Object.assign(new Error(`Trip ${op.trip_id}: ${reason}`), { code: 'TRIP_INTENT_CONFLICT' }); }
function eventId(p) { return createHash('sha256').update(`jsfc-trip-event-v1:${p.vehicle_no}:${p.ts}`).digest('hex'); }
function transitionId(p, tripId, kind) {
  return createHash('sha256').update(JSON.stringify(['jsfc-trip-transition-v2', p.vehicle_no, String(tripId), kind, p.ts])).digest('hex');
}
module.exports = { Repository, DETAILS_SELECT, SELECT, START, END, INSERT_EVENT, eventId, transitionId };
