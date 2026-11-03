'use strict';
const { vehicle } = require('./domain');
// dataPacket[9] is DDMMYYYY; dataPacket[10] is HHmmss. Both are UTC.
// Keep the fields as strings so leading zeroes are preserved.
function utcPacketPartsToTimestamp(datePart, timePart) {
  const m = /^(\d{2})(\d{2})(\d{4}) (\d{2})(\d{2})(\d{2})$/.exec(`${String(datePart ?? '')} ${String(timePart ?? '')}`);
  if (!m) throw new Error('date_time must be UTC DDMMYYYY HHmmss');
  const [, day, month, year, hour, minute, second] = m.map(Number);
  if (year < 2000 || year > 2099) throw new Error('Invalid UTC GPS year');
  const ts = Date.UTC(year, month - 1, day, hour, minute, second);
  const d = new Date(ts);
  if (d.getUTCFullYear() !== year || d.getUTCMonth() !== month - 1 || d.getUTCDate() !== day ||
      d.getUTCHours() !== hour || d.getUTCMinutes() !== minute || d.getUTCSeconds() !== second) {
    throw new Error('Invalid UTC GPS date_time');
  }
  return ts;
}
function utcDateTimeToTimestamp(value) {
  const parts = /^(\d{8}) (\d{6})$/.exec(String(value ?? ''));
  if (!parts) throw new Error('date_time must be UTC DDMMYYYY HHmmss');
  return utcPacketPartsToTimestamp(parts[1], parts[2]);
}
function mapMqttPacket(topic, payload) {
  const match = /^gps\/([^/]+)\/data$/.exec(topic);
  if (!match) throw new Error('Unexpected topic; expected gps/{vehicle}/data');
  const data = JSON.parse(Buffer.isBuffer(payload) ? payload.toString('utf8') : payload);
  if (!data || typeof data !== 'object' || Array.isArray(data)) throw new Error('GPS payload must be an object');
  if (data.packet_header !== '$PVT') {
    throw Object.assign(new Error('Ignored MQTT packet: packet_header must be $PVT'), { code: 'MQTT_HEADER_IGNORED' });
  }
  if (String(data.gps_fix) !== '1') throw new Error('No valid GPS fix');
  if (typeof data.packet_type !== 'string' || !data.packet_type.trim()) throw new Error('Missing packet_type');
  // date_time is confirmed UTC. Rebuild the epoch instead of using the faulty supplied epoch.
  const timestamp = utcDateTimeToTimestamp(data.date_time);
  // Retain only five normalized fields; vehicle identity comes from the topic.
  return { vehicle_no: vehicle(match[1]), lat: data.latitude, lng: data.longitude,
    packet_type: data.packet_type.trim(), gps_fix: '1',
    packet_datetime: timestamp };
}
function publicPacket(p) {
  return { latitude: p.lat, longitude: p.lng, packet_type: p.packet_type || 'L',
    gps_fix: String(p.gps_fix ?? '1'), gps_timestamp: p.ts };
}
module.exports = { mapMqttPacket, publicPacket, utcDateTimeToTimestamp, utcPacketPartsToTimestamp };
