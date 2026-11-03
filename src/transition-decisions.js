'use strict';
const { dbTime, sqlTime, distance, coordinate } = require('./domain');
const { canStart } = require('./geofence-state');
function decisionsFor(trips, packet, states, action, duplicate, cfg) {
  const ended = new Set(action.end.map(t => String(t.id)));
  const started = new Set(action.start.map(t => String(t.id)));
  const decisions = {};
  for (const t of trips.filter(t => Number(t.trip_status) < 2)) {
    const state = states[String(t.id)];
    const start = Number(t.trip_status) === 0;
    const floor = dbTime(start ? t.lifting_date : t.trip_date, cfg.offset);
    let reason;
    if (duplicate(t, start ? 'start' : 'end')) reason = 'EVENT_ALREADY_COMMITTED';
    else if (started.has(String(t.id))) reason = 'START';
    else if (ended.has(String(t.id))) reason = 'END';
    else if (!Number.isFinite(floor)) reason = start ? 'INVALID_LIFTING_TIME' : 'INVALID_START_TIME';
    else if (packet.ts <= floor) reason = start ? 'GPS_NOT_AFTER_LIFTING' : 'GPS_NOT_AFTER_START';
    else if (start) {
      if (state?.source_status === 'UNKNOWN') reason = 'INVALID_SOURCE_GEOFENCE';
      else if (state?.source_status === 'INSIDE') reason = 'INSIDE_SOURCE';
      else if (!canStart(state, t, packet, cfg)) reason =
        Number.isFinite(state?.source_entered_at) && state.source_entered_at < state.source_exited_at &&
        state.source_exited_at > floor && state.source_exited_at < packet.ts
          ? 'AWAITING_RECORDED_EXIT_REPLAY' : 'SOURCE_EXIT_NOT_OBSERVED';
      else reason = 'START_ELIGIBLE';
    } else reason = state?.destination_status === 'UNKNOWN' ? 'INVALID_DESTINATION_GEOFENCE' : 'OUTSIDE_DESTINATION';
    const km = side => coordinate(t[`${side}_lat`], t[`${side}_lng`])
      ? Math.round(distance(packet.lat, packet.lng, Number(t[`${side}_lat`]), Number(t[`${side}_lng`])))/1000 : null;
    decisions[String(t.id)] = { vehicle_no: packet.vehicle_no, trip_id: String(t.id),
      trip_status: Number(t.trip_status), reason, gps_timestamp: packet.ts,
      gps_datetime_ist: sqlTime(packet.ts, '+05:30'), packet_type: packet.packet_type || null,
      latitude: packet.lat, longitude: packet.lng, lifting_date: t.lifting_date, trip_date: t.trip_date || null,
      source_status: state?.source_status || 'UNKNOWN', destination_status: state?.destination_status || 'UNKNOWN',
      source_entered_at: state?.source_entered_at ?? null, source_exited_at: state?.source_exited_at ?? null,
      source_assumed_in_at: state?.source_assumed_in_at ?? null,
      source_assumption_pending: state?.source_assumption_pending === true,
      start_basis: started.has(String(t.id)) && cfg.departureRequiresCrossing === false
        ? 'SOURCE_OUT_AFTER_DISPATCH' : state?.source_exit_basis ?? null,
      distance_from_source_km: km('source'), distance_from_destination_km: km('destination'),
      blocking_trip_ids: [] };
  }
  return decisions;
}
module.exports = { decisionsFor };
