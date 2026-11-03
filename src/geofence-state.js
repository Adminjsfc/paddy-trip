'use strict';
const { inside, dbTime, coordinate } = require('./domain');
const cfg = require('./config');
const FIELDS = ['source_lat','source_lng','source_radius','destination_lat','destination_lng','destination_radius'];
const numeric = value => value === null || value === undefined || value === '' ? null : Number(value);
function canonical(parts) {
  return JSON.stringify([parts[0] == null ? null : String(parts[0]),
    parts[1] == null ? null : dbTime(parts[1], cfg.offset), ...parts.slice(2).map(numeric)]);
}
function geometryKey(t) {
  return canonical([t.route_id ?? null, t.lifting_date ?? null, ...FIELDS.map(k => t[k] ?? null)]);
}
function compatibleGeometry(value, t) {
  if (value === geometryKey(t)) return true;
  // Migrate the old raw JSON signature: DECIMAL string/number formatting is not a route change.
  try { return canonical(JSON.parse(value)) === geometryKey(t); } catch { return false; }
}
const label = value => value === null ? 'UNKNOWN' : value ? 'INSIDE' : 'OUTSIDE';
function journeyState(t, state) {
  return Number(t.trip_status) === 2 ? 'END' : Number(t.trip_status) === 1 ? 'IN_TRIP'
    : state?.source_status === 'INSIDE' ? 'IN_SOURCE' : 'PENDING';
}
function restore(previous, trip, cfg) {
  if (!previous || String(previous.trip_id) !== String(trip.id) ||
      previous.vehicle_no !== trip.vehicle_no || !compatibleGeometry(previous.geometry, trip)) return null;
  let old = { ...previous, geometry: geometryKey(trip) };
  const multiplier = Number(old.radius_multiplier ?? 1); // Older releases assumed metres.
  if (multiplier !== cfg.radiusMultiplier) {
    // Reclassify retained coordinates under the corrected unit, without inventing an exit.
    if (!coordinate(old.latitude, old.longitude) || !Number.isFinite(old.gps_timestamp)) return null;
    const p = { lat: Number(old.latitude), lng: Number(old.longitude) };
    for (const side of ['source', 'destination']) {
      const status = label(inside(p, trip, side, cfg.radiusMultiplier));
      if (status !== old[`${side}_status`]) {
        old[`${side}_entered_at`] = status === 'INSIDE' ? old.gps_timestamp : null;
        old[`${side}_exited_at`] = null;
      }
      old[`${side}_status`] = status;
    }
    delete old.replay_state;
  }
  old.radius_multiplier = cfg.radiusMultiplier;
  old.trip_status = Number(trip.trip_status);
  old.journey_state = journeyState(trip, old);
  return old;
}
function observe(previous, trip, packet, cfg) {
  const old = restore(previous, trip, cfg);
  if (old && packet.ts <= old.gps_timestamp) return old;
  const source = label(inside(packet, trip, 'source', cfg.radiusMultiplier));
  const destination = label(inside(packet, trip, 'destination', cfg.radiusMultiplier));
  const state = { geometry: geometryKey(trip), radius_multiplier: cfg.radiusMultiplier,
    trip_id: String(trip.id), vehicle_no: packet.vehicle_no,
    gps_timestamp: packet.ts, latitude: packet.lat, longitude: packet.lng,
    source_status: source, destination_status: destination,
    source_entered_at: source === 'INSIDE' && old?.source_status !== 'INSIDE' ? packet.ts : old?.source_entered_at ?? null,
    source_exited_at: old?.source_status === 'INSIDE' && source === 'OUTSIDE' ? packet.ts : old?.source_exited_at ?? null,
    destination_entered_at: destination === 'INSIDE' && old?.destination_status !== 'INSIDE' ? packet.ts : old?.destination_entered_at ?? null,
    destination_exited_at: destination === 'OUTSIDE' && old?.destination_status === 'INSIDE' ? packet.ts : old?.destination_exited_at ?? null,
    source_assumed_in_at: old?.source_assumed_in_at ?? null,
    source_assumption_pending: old?.source_assumption_pending === true,
    source_exit_basis: old?.source_status === 'INSIDE' && source === 'OUTSIDE'
      ? 'OBSERVED_SOURCE_EXIT' : old?.source_exit_basis ?? null,
    trip_status: Number(trip.trip_status) };
  state.journey_state = journeyState(trip, state);
  return state;
}
function observeEvent(previous, trip, packet, cfg) {
  const old = restore(previous, trip, cfg);
  if (old && packet.ts <= old.gps_timestamp) {
    // An exact repeat preserves existing crossing evidence. An older pass has its own cursor.
    if (packet.ts === old.gps_timestamp && !old.replay_state) return { current: old, snapshot: old };
    let replay = old.replay_state || null;
    if (replay && packet.ts < replay.gps_timestamp) replay = null;
    const current = observe(replay, trip, packet, cfg);
    // Replays never manufacture a new missing-state assumption. They may reuse
    // an assumption that was explicitly recorded when this trip state was absent.
    if (old.source_assumption_pending && !current.source_assumed_in_at) {
      current.source_assumed_in_at = old.source_assumed_in_at;
      current.source_assumption_pending = true;
    }
    applySourceAssumption(current, trip, packet, cfg);
    // A previously recorded crossing is valid evidence even if the original
    // inside packet has expired. Still require the real exit packet/coordinates.
    if (current.source_status === 'OUTSIDE' && old.source_exited_at === packet.ts &&
        ((Number.isFinite(old.source_entered_at) && old.source_entered_at < packet.ts) ||
          (old.source_exit_basis === 'MISSING_REDIS_STATE' && Number.isFinite(old.source_assumed_in_at) && old.source_assumed_in_at < packet.ts))) {
      current.source_entered_at = old.source_entered_at;
      current.source_exited_at = packet.ts;
      current.source_exit_basis = old.source_exit_basis || 'OBSERVED_SOURCE_EXIT';
      current.source_assumed_in_at = old.source_assumed_in_at ?? null;
      current.source_assumption_pending = false;
    }
    return { current, snapshot: packet.ts === old.gps_timestamp ? current : { ...old, replay_state: current } };
  }
  const snapshot = observe(old, trip, packet, cfg);
  const lifting = dbTime(trip.lifting_date, cfg.offset);
  // Only genuine absence authorizes the requested fallback. Existing OUTSIDE,
  // invalid or mismatched state is not silently reclassified as source INSIDE.
  if (cfg.allowMissingStateAssumption !== false && previous == null && Number(trip.trip_status) === 0 && Number.isFinite(lifting)) {
    snapshot.source_assumed_in_at = lifting;
    snapshot.source_assumption_pending = true;
  }
  applySourceAssumption(snapshot, trip, packet, cfg);
  return { current: snapshot, snapshot };
}
function applySourceAssumption(state, trip, packet, cfg) {
  if (cfg.allowMissingStateAssumption === false) { state.source_assumption_pending = false; return; }
  if (Number(trip.trip_status) !== 0 || !state.source_assumption_pending) return;
  const lifting = dbTime(trip.lifting_date, cfg.offset);
  if (!Number.isFinite(lifting) || state.source_assumed_in_at !== lifting || packet.ts <= lifting) return;
  if (state.source_status === 'INSIDE') {
    state.source_assumption_pending = false; // Actual post-lifting observation now governs the next exit.
  } else if (state.source_status === 'OUTSIDE') {
    if (state.source_exited_at !== packet.ts) {
      state.source_exited_at = packet.ts;
      state.source_exit_basis = 'MISSING_REDIS_STATE';
    }
    state.source_assumption_pending = false;
    // source_entered_at remains a real GPS observation or null. Never insert
    // a fabricated GPS packet/coordinate at lifting time to implement the policy.
  }
}
function canStart(state, trip, packet, cfg) {
  if (cfg.departureRequiresCrossing === false) {
    return inside(packet, trip, 'source', cfg.radiusMultiplier) === false &&
      packet.ts > dbTime(trip.lifting_date, cfg.offset);
  }
  return state?.source_status === 'OUTSIDE' && state.source_exited_at === packet.ts &&
    packet.ts > dbTime(trip.lifting_date, cfg.offset);
}
module.exports = { observe, observeEvent, canStart, geometryKey, journeyState };
