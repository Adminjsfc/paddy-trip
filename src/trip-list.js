'use strict';
const { dbTime } = require('./domain');

function tripListEntry(trip, offset) {
  const missing = [];
  if (!Number.isFinite(dbTime(trip.lifting_date, offset))) missing.push('dispatched_at');
  if (Number(trip.trip_status) === 1 && !Number.isFinite(dbTime(trip.trip_date, offset))) {
    missing.push('trip_started_at');
  }
  return {
    trip_id: String(trip.id), vehicle_no: trip.vehicle_no,
    status: trip.status, trip_status: Number(trip.trip_status),
    dispatched_at: trip.lifting_date ?? null,
    trip_started_at: trip.trip_date ?? null,
    missing_timestamps: missing
  };
}

module.exports = { tripListEntry };
