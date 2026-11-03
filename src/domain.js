'use strict';
function vehicle(value) {
  const v = String(value ?? '').trim().toUpperCase();
  if (!/^[A-Z0-9-]{3,32}$/.test(v)) throw new Error('vehicle_no must be 3..32 letters/digits/hyphens, no internal spaces');
  return v;
}
function crc32(value) {
  let crc = 0xffffffff;
  for (const byte of Buffer.from(value)) {
    crc ^= byte;
    for (let b = 0; b < 8; b++) crc = (crc >>> 1) ^ ((crc & 1) ? 0xedb88320 : 0);
  }
  return (crc ^ 0xffffffff) >>> 0;
}
const shardFor = v => crc32(vehicle(v)) % 8;
function packetTime(value) {
  if (typeof value === 'number' && Number.isSafeInteger(value) && value > 1e12) return value;
  if (typeof value !== 'string' || !/^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d{1,3})?(?:Z|[+-]\d\d:\d\d)$/.test(value)) {
    throw new Error('packet_datetime requires epoch milliseconds or ISO 8601 with timezone');
  }
  const n = Date.parse(value);
  if (!Number.isFinite(n)) throw new Error('Invalid packet_datetime');
  return n;
}
function dbTime(value, offset) {
  if (!value || String(value).startsWith('0000')) return NaN;
  return Date.parse(String(value).replace(' ', 'T') + offset);
}
function sqlTime(ms, offset) {
  const sign = offset[0] === '-' ? -1 : 1;
  const [h, m] = offset.slice(1).split(':').map(Number);
  return new Date(ms + sign * (h * 60 + m) * 60000).toISOString().slice(0, 23).replace('T', ' ');
}
function number(value) {
  if (value === null || value === undefined || value === '' || typeof value === 'boolean') return NaN;
  return Number(value);
}
function coordinate(lat, lng) {
  return Number.isFinite(number(lat)) && Math.abs(number(lat)) <= 90 &&
    Number.isFinite(number(lng)) && Math.abs(number(lng)) <= 180;
}
function distance(lat1, lng1, lat2, lng2) {
  const r = Math.PI / 180;
  const a = Math.sin((lat2 - lat1) * r / 2) ** 2 +
    Math.cos(lat1 * r) * Math.cos(lat2 * r) * Math.sin((lng2 - lng1) * r / 2) ** 2;
  return 6371000 * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(Math.max(0, 1 - a)));
}
function inside(p, t, side, multiplier) {
  const lat = number(t[`${side}_lat`]), lng = number(t[`${side}_lng`]);
  const radius = number(t[`${side}_radius`]) * multiplier;
  if (!coordinate(lat, lng) || !Number.isFinite(radius) || radius <= 0) return null;
  return distance(p.lat, p.lng, lat, lng) <= radius;
}
function normalizePacket(input, cfg, now = Date.now()) {
  const p = { vehicle_no: vehicle(input.vehicle_no), lat: number(input.lat), lng: number(input.lng),
    ts: Math.floor(packetTime(input.packet_datetime ?? input.ts) / 1000) * 1000,
    packet_type: input.packet_type || 'L', gps_fix: String(input.gps_fix ?? '1') };
  if (p.gps_fix !== '1') throw new Error('No valid GPS fix');
  if (!coordinate(p.lat, p.lng)) throw new Error('Invalid GPS coordinates');
  if (p.ts > now + cfg.futureTolerance) throw new Error('Future GPS packet rejected');
  if (p.ts < now - cfg.historyMs) throw new Error('GPS packet older than history retention; use archive reconciliation');
  return p;
}
function plan(trips, p, cfg, eligible = () => true) {
  const active = trips.filter(t => Number(t.trip_status) === 1);
  const end = active.filter(t => p.ts > dbTime(t.trip_date, cfg.offset) &&
    inside(p, t, 'destination', cfg.radiusMultiplier) === true && eligible(t, 'end'));
  const pending = trips.filter(t => Number(t.trip_status) === 0).sort((a, b) => {
    const ta = dbTime(a.lifting_date, cfg.offset), tb = dbTime(b.lifting_date, cfg.offset);
    return (Number.isFinite(ta) ? ta : Infinity) - (Number.isFinite(tb) ? tb : Infinity) ||
      (BigInt(a.id) < BigInt(b.id) ? -1 : BigInt(a.id) > BigInt(b.id) ? 1 : 0);
  });
  // Every trip has independent geofences/times. One GPS exit may start multiple
  // trips from this source; an unrelated open/completed trip is not a blocker.
  const start = pending.filter(t => p.ts > dbTime(t.lifting_date, cfg.offset) &&
    inside(p, t, 'source', cfg.radiusMultiplier) === false &&
    eligible(t, 'start'));
  return { end, start };
}
module.exports = { vehicle, crc32, shardFor, packetTime, dbTime, sqlTime, coordinate, distance, inside, normalizePacket, plan };
