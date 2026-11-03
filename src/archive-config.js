'use strict';
// Embedded Influx service configuration. Defaults may be edited here directly.
// MySQL continues to use the supplied Database.js read/write pools.
function positive(name, fallback) {
  const value = Number(process.env[name] ?? fallback);
  if (!Number.isSafeInteger(value) || value <= 0) throw Error(`Invalid ${name}`);
  return value;
}
const cfg = {
  enabled: process.env.ARCHIVE_RECOVERY_ENABLED !== 'false',
  influx: {
    url: process.env.INFLUX_URL || 'http://18.60.155.145:8086',
    org: process.env.INFLUX_ORG || 'AISPL',
    bucket: process.env.INFLUX_BUCKET || 'gps_bucket',
    measurement: process.env.INFLUX_MEASUREMENT || 'device_pvt_log',
    token: process.env.INFLUX_TOKEN || '0Ad_jDzyDyrP42HgEiJ3ScNTxGbEZmwpnW5KTlAtn4iOwkYk2DeSKps9SFGRLY1pnFPweca2cisIgakVRcL8Pw=='
  },
  cursorSecret: process.env.GPS_HISTORY_CURSOR_SECRET || '',
  timeoutMs: positive('GPS_HISTORY_TIMEOUT_MS', 20000),
  pageSize: positive('GPS_HISTORY_PAGE_SIZE', 200),
  maxBytes: positive('GPS_HISTORY_MAX_RESPONSE_BYTES', 8388608),
  windowMs: positive('GPS_HISTORY_WINDOW_HOURS', 6) * 3600000,
  maxLookbackMs: positive('GPS_HISTORY_MAX_DAYS', 31) * 86400000,
  rescanMs: positive('GPS_HISTORY_RESCAN_MINUTES', 15) * 60000,
  maxPackets: positive('GPS_HISTORY_MAX_PACKETS', 2000000),
  importSize: positive('GPS_HISTORY_IMPORT_SIZE', 500)
};
const url = new URL(cfg.influx.url);
if (!['http:', 'https:'].includes(url.protocol) || url.username || url.password || url.search || url.hash)
  throw Error('INFLUX_URL must be an HTTP(S) database URL without credentials/query');
if (cfg.pageSize > 500 || cfg.windowMs > 31 * 86400000) throw Error('History page/window exceeds service limits');
if (cfg.timeoutMs < 100 || cfg.timeoutMs > 120000) throw Error('GPS_HISTORY_TIMEOUT_MS must be 100..120000');
module.exports = cfg;
