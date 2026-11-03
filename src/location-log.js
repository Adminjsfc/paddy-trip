'use strict';
const { coordinate, distance, inside, sqlTime } = require('./domain');
function locationTripLog(vehicleNo, trip, point, cfg, processedTimestamp = null) {
  const p = point && { lat: point.latitude ?? point.lat, lng: point.longitude ?? point.lng,
    ts: point.gps_timestamp ?? point.ts };
  const valid = p && coordinate(p.lat, p.lng) && Number.isFinite(p.ts);
  const geofence = side => {
    const lat = trip[`${side}_lat`], lng = trip[`${side}_lng`];
    const radius = trip[`${side}_radius`];
    const radiusValid = radius !== null && radius !== undefined && radius !== '' && Number.isFinite(Number(radius)) && Number(radius) > 0;
    const state = valid ? inside(p, trip, side, cfg.radiusMultiplier) : null;
    return { status: !valid ? 'NO_LOCATION' : state === null ? 'UNKNOWN' : state ? 'INSIDE' : 'OUTSIDE',
      distance_km: valid && coordinate(lat, lng)
        ? Math.round(distance(Number(p.lat), Number(p.lng), Number(lat), Number(lng))) / 1000 : null,
      radius_km: radiusValid ? Number(radius) * cfg.radiusMultiplier / 1000 : null };
  };
  const source = geofence('source'), destination = geofence('destination');
  return { vehicle_no: vehicleNo, trip_id: String(trip.id), trip_unique_id: trip.trip_unique_id ?? trip.trip_id ?? null,
    trip_status: Number(trip.trip_status), trip_status_name: ({0:'PENDING',1:'STARTED',2:'COMPLETED'})[Number(trip.trip_status)] || 'UNKNOWN',
    latitude: valid ? Number(p.lat) : null, longitude: valid ? Number(p.lng) : null,
    gps_timestamp: valid ? p.ts : null,
    gps_datetime_utc: valid ? new Date(p.ts).toISOString() : null,
    gps_datetime_ist: valid ? sqlTime(p.ts, '+05:30') : null,
    location_age_seconds: valid ? Math.max(0, Math.floor((Date.now() - p.ts) / 1000)) : null,
    packet_type: point?.packet_type ?? null,
    source_status: source.status, distance_from_source_km: source.distance_km, source_radius_km: source.radius_km,
    destination_status: destination.status, distance_from_destination_km: destination.distance_km, destination_radius_km: destination.radius_km,
    processed_packet_timestamp: processedTimestamp,
    processed_packet_older_than_last_location: valid && processedTimestamp !== null ? processedTimestamp < p.ts : false };
}
class LocationLogGate {
  constructor(intervalMs = 60000) { this.intervalMs = intervalMs; this.entries = new Map(); }
  shouldLog(key, data, now = Date.now()) {
    // Wall-clock age and the caller's reason must not make an identical point new.
    const { reason, location_age_seconds, ...stable } = data;
    const signature = JSON.stringify(stable), previous = this.entries.get(key);
    if (previous?.signature === signature && now - previous.at < this.intervalMs) return false;
    this.entries.set(key, { signature, at: now });
    return true;
  }
  retain(keys) { const active = new Set(keys); for (const k of this.entries.keys()) if (!active.has(k)) this.entries.delete(k); }
}
module.exports = { locationTripLog, LocationLogGate };
