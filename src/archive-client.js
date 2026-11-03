'use strict';
const { vehicle, coordinate } = require('./domain');
const { gpsTime } = require('./influx/gps-time');
const { errorDetails } = require('./archive-diagnostics');
const problem = (code, message) => Object.assign(Error(message), { code });
function packetHeader(row) {
  if (row.packet_header != null) return row.packet_header;
  if (typeof row.gps_rawdata === 'string') return row.gps_rawdata.trimStart().split(',', 1)[0];
  if (typeof row.device_data === 'string' && row.device_data.length < 65536) {
    try { return JSON.parse(row.device_data)?.packet_header; } catch { return null; }
  }
  return null;
}
function archivePacket(row, vehicleNo, from, until) {
  if (!row || typeof row !== 'object' || row.vehicle_number !== vehicle(vehicleNo))
    throw problem('ARCHIVE_WRONG_VEHICLE', 'History response contains another vehicle');
  if (packetHeader(row) !== '$PVT') return { rejected: 'NOT_PVT' };
  if (String(row.gps_fix) !== '1') return { rejected: 'INVALID_GPS_FIX' };
  if (!coordinate(row.latitude, row.longitude)) return { rejected: 'INVALID_COORDINATES' };
  if (typeof row.packet_type !== 'string' || !row.packet_type.trim()) return { rejected: 'MISSING_PACKET_TYPE' };
  const { ts } = gpsTime(row);
  if (ts < from || ts >= until) throw problem('ARCHIVE_OUT_OF_RANGE', 'History response is outside its requested GPS interval');
  // Publisher gps_timestamp/server receipt time never override the confirmed UTC field.
  return { packet: { vehicle_no: vehicleNo, lat: Number(row.latitude), lng: Number(row.longitude),
    ts, packet_type: row.packet_type.trim(), gps_fix: '1' } };
}
// Lazy construction permits Redis-only mode without an Influx token or client.
// There is no HTTP report server: every worker owns one embedded service instance.
function serviceOptions(cfg) {
  const influx = cfg.influx || {};
  const cursorSecret = cfg.cursorSecret || require('node:crypto').createHash('sha256')
    .update(JSON.stringify(['jsfc-history-v1', influx.url, influx.org, influx.bucket, influx.measurement, influx.token])).digest('hex');
  return { influx, env: { CURSOR_SECRET: cursorSecret, DATE_TIMEZONE_OFFSET: 'Z',
    MAX_CONCURRENT_QUERIES: '1', QUERY_TIMEOUT_MS: String(cfg.timeoutMs), MAX_RESULTS: '500',
    DEFAULT_PAGE_SIZE: String(cfg.pageSize), MAX_RANGE_DAYS: '31', DEFAULT_LOOKBACK_HOURS: '24' } };
}
class HistoryService {
  constructor(cfg, service = null) { this.cfg = cfg; this.service = service; this.controllers = new Set(); this.closed = false; }
  getService() {
    if (this.closed) throw problem('ARCHIVE_CLOSED', 'History service is closed');
    if (!this.service) {
      if (!this.cfg.influx?.token || this.cfg.influx.token.startsWith('YOUR_'))
        throw problem('ARCHIVE_INFLUX_CONFIG', 'Set the Influx token in src/archive-config.js or INFLUX_TOKEN');
      const { GpsReportService } = require('./influx/GpsReportService');
      this.service = new GpsReportService(serviceOptions(this.cfg));
    }
    return this.service;
  }
  async page(vehicleNo, from, until, cursor = null) {
    // Worker boundaries are absolute epoch milliseconds, never timezone-less strings.
    if (!Number.isSafeInteger(from) || !Number.isSafeInteger(until) || from >= until)
      throw problem('ARCHIVE_INVALID_RANGE', 'History range requires UTC epoch milliseconds with from < until');
    const c = this.cfg, v = vehicle(vehicleNo), controller = new AbortController();
    this.controllers.add(controller);
    let timer;
    const began = performance.now();
    const deadline = new Promise((resolve, reject) => {
      controller.signal.addEventListener('abort', () => reject(problem(
        this.closed ? 'ARCHIVE_CLOSED' : 'ARCHIVE_TIMEOUT', this.closed ? 'History service is closed' : 'Influx history query deadline exceeded'
      )), { once: true });
      timer = setTimeout(() => controller.abort(), c.timeoutMs);
    });
    try {
      const input = { vehicle_number: v, from_date: new Date(from).toISOString(),
        to_date: new Date(until).toISOString(), limit: c.pageSize, order_by: 'date_time', order: 'asc',
        ...(cursor ? { cursor } : {}) };
      const page = await Promise.race([this.getService().getHistory(input, { signal: controller.signal }), deadline]);
      if (Buffer.byteLength(JSON.stringify(page) || '') > c.maxBytes)
        throw problem('ARCHIVE_RESPONSE_TOO_LARGE', 'History page exceeded configured byte limit');
      if (page?.success !== true || page.report !== 'history' || !Array.isArray(page.data) ||
          page.data.length > c.pageSize || page.count !== page.data.length ||
          typeof page.pagination?.has_more !== 'boolean') throw problem('ARCHIVE_INVALID_RESPONSE', 'Invalid history page shape');
      if (page.filters?.vehicle_number !== v || Date.parse(page.filters.from_date) !== from ||
          Date.parse(page.filters.to_date_exclusive) !== until)
        throw problem('ARCHIVE_FILTER_MISMATCH', 'History service filters do not match request');
      const next = page.pagination.next_cursor;
      if (page.pagination.has_more ? !page.data.length || typeof next !== 'string' || !next || next.length > 4096 || next === cursor : next != null)
        throw problem('ARCHIVE_INVALID_CURSOR', 'History service returned inconsistent pagination');
      return page;
    } catch (e) {
      if (String(e.code || '').startsWith('ARCHIVE_')) throw e;
      // Do not log raw Influx query bodies, response bodies or credentials.
      const status = Number(e.status ?? e.statusCode);
      throw Object.assign(problem('ARCHIVE_INFLUX_ERROR', Number.isInteger(status)
        ? `Influx history service failed (status ${status})` : 'Influx history query failed; retry retained'),
        { ...(Number.isInteger(status) ? { status } : {}), diagnostic: {
          ...errorDetails(e, c), elapsed_ms: Math.round(performance.now() - began),
          page_size: c.pageSize, cursor_present: !!cursor,
          from_gps_utc: new Date(from).toISOString(), until_gps_utc_exclusive: new Date(until).toISOString()
        } });
    } finally { clearTimeout(timer); this.controllers.delete(controller); }
  }
  close() {
    if (this.closed) return;
    this.closed = true;
    for (const controller of this.controllers) controller.abort();
    this.service?.close?.();
  }
}
module.exports = { HistoryService, serviceOptions, archivePacket, packetHeader, problem };
