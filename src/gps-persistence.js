'use strict';
const { sqlTime } = require('./domain');

async function gpsBinding(db, vehicleId) {
  const devices = await db.writeExecute(`SELECT d.id AS device_id, d.imei_id
    FROM gps_devices d JOIN imeis i ON i.id=d.imei_id
    WHERE d.vehicle_id=? AND d.active=1 AND i.active=1 FOR UPDATE`, [vehicleId]);
  return devices.find(d => d.device_id != null) || null;
}

async function persistGps(db, vehicleId, intent) {
  // Older saved intents have no captured binding. Never attach historical GPS
  // to a newly mapped device when reconciling those intents.
  if (!intent.gpsDevice) return;
  const binding = await gpsBinding(db, vehicleId);
  if (!binding || String(binding.device_id) !== String(intent.gpsDevice.device_id) ||
      String(binding.imei_id) !== String(intent.gpsDevice.imei_id)) {
    throw Object.assign(new Error('GPS assignment changed since write intent was saved'), { code: 'TRIP_BINDING_CHANGED' });
  }
  for (const p of intent.packets) {
    const when = sqlTime(p.ts, '+00:00');
    const existing = await db.writeExecute(`SELECT latitude,longitude FROM gps_packets
      WHERE device_id=? AND gps_at=? FOR UPDATE`, [binding.device_id, when]);
    if (existing.length) {
      if (Number(existing[0].latitude) !== Number(Number(p.lat).toFixed(7)) ||
          Number(existing[0].longitude) !== Number(Number(p.lng).toFixed(7))) {
        throw Object.assign(new Error('Different GPS coordinates for the same device and timestamp'), { code: 'GPS_TIMESTAMP_CONFLICT' });
      }
    } else {
      // The receiver retains no speed or original raw payload; store only the
      // available normalized fields, with zero as the schema-required speed.
      await db.writeExecute(`INSERT INTO gps_packets
        (device_id,gps_at,latitude,longitude,speed_kph,packet_type,raw_payload,processing_result)
        VALUES (?,?,?,?,0,?,?,?)`, [binding.device_id, when, p.lat, p.lng,
        p.packet_type || 'L', JSON.stringify({ vehicle_no: intent.vehicle_no,
          latitude: p.lat, longitude: p.lng, gps_timestamp: p.ts,
          packet_type: p.packet_type || 'L', gps_fix: '1', speed_available: false }), 'TRIP_WORKER']);
    }
    await db.writeExecute(`UPDATE gps_devices SET last_gps_at=?,latitude=?,longitude=?,speed_kph=0
      WHERE id=? AND vehicle_id=? AND (last_gps_at IS NULL OR last_gps_at<?)`,
    [when, p.lat, p.lng, binding.device_id, vehicleId, when]);
  }
}
module.exports = { gpsBinding, persistGps };
