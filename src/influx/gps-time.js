'use strict';
const { utcDateTimeToTimestamp } = require('../mqtt-packet');
const LEGACY_INDEX_SHIFT_MS = 19800000;
const GPS_QUERY_VERSION = 2;
// date_time is the event clock. _time remains the immutable physical lookup key.
// Support the confirmed legacy UTC+05:30 index and correctly written UTC rows.
function gpsTime(row, checkAlias = true) {
  let ts;
  try { ts = utcDateTimeToTimestamp(row.date_time); }
  catch { throw Object.assign(Error('PVT date_time must be UTC DDMMYYYY HHmmss'), { code: 'ARCHIVE_INVALID_GPS_TIME' }); }
  const indexed = typeof row._time === 'string' && /Z$/.test(row._time) ? Date.parse(row._time) : NaN;
  const shift = Math.floor(indexed / 1000) * 1000 - ts;
  if (!Number.isFinite(indexed) || ![0, LEGACY_INDEX_SHIFT_MS].includes(shift)) {
    throw Object.assign(Error('Influx index offset must be UTC or the confirmed legacy UTC+05:30'), {
      code: 'ARCHIVE_TIME_MISMATCH', diagnostic: { influx_time: String(row._time).slice(0, 40),
        packet_date_time: row.date_time, parsed_gps_utc: new Date(ts).toISOString(),
        index_offset_seconds: Number.isFinite(shift) ? shift / 1000 : null }
    });
  }
  if (checkAlias && row.gps_date_time_iso != null && Date.parse(row.gps_date_time_iso) !== ts)
    throw Object.assign(Error('History GPS timestamp alias disagrees with UTC date_time'), { code: 'ARCHIVE_TIME_MISMATCH' });
  return { ts, indexed, shift };
}
module.exports = { LEGACY_INDEX_SHIFT_MS, GPS_QUERY_VERSION, gpsTime };
