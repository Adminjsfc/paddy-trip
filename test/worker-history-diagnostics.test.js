'use strict';
const test = require('node:test'), assert = require('node:assert/strict');
const { HistoryService, serviceOptions } = require('../src/archive-client');
const { GpsReportService } = require('../src/influx/GpsReportService');
const { errorDetails, safeConfig } = require('../src/archive-diagnostics');
const { probe, parseArgs } = require('../scripts/check-worker-history.cjs');
const { utcText } = require('../scripts/simulation-fixtures.cjs');
const { utcDateTimeToTimestamp } = require('../src/mqtt-packet');
const from = Date.parse('2026-10-02T03:03:10Z'), until = from + 6 * 3600000, vehicleNo = 'JH02AK7064';
const cfg = { enabled: true, pageSize: 200, timeoutMs: 1000, maxBytes: 8388608, windowMs: 6 * 3600000,
  cursorSecret: 'fixture-signing-secret', influx: { url: 'http://influx.test:8086', org: 'AISPL',
    bucket: 'gps_bucket', measurement: 'device_pvt_log', token: 'fixture-token-do-not-log' } };
const rows = Array.from({ length: 201 }, (_, n) => ({ _time: new Date(from + n * 1000).toISOString(),
  vehicle_number: vehicleNo, imei_no: '865510087948487', packet_type: 'H', gps_fix: '1',
  latitude: 24, longitude: 86, gps_rawdata: '$PVT,ACT1', date_time: utcText(from + n * 1000),
  server_date_time: String(until), _server_sort: until }));
function fixture(batches, queries = []) {
  return { queryRows(query, observer) {
    queries.push(query); observer.useCancellable({ cancel() {} });
    const batch = batches.shift();
    if (batch instanceof Error) return observer.error(batch);
    for (const row of batch || []) observer.next({ ...row, _gps_time: new Date(utcDateTimeToTimestamp(row.date_time)).toISOString() }, { toObject: value => value });
    observer.complete();
  } };
}
test('200-packet worker page, two detail batches and signed next page pass UTC validation', async () => {
  const queries = [], api = fixture([rows, rows.slice(0, 100), rows.slice(100, 200), rows.slice(200), rows.slice(200)], queries);
  const client = new HistoryService(cfg, new GpsReportService({ ...serviceOptions(cfg), queryApi: api }));
  const report = await probe({ cfg, client, vehicleNo, from, until }); client.close();
  assert.equal(report.success, true); assert.equal(report.scanned, 201); assert.equal(report.accepted, 201);
  assert.deepEqual(report.pages.map(p => p.count), [200, 1]); assert.equal(report.pages[1].cursor_used, true);
  assert.equal(report.window_complete, true); assert.equal(queries.length, 5);
  assert.ok(queries[0].includes('limit(n: 201)')); assert.ok(queries[3].includes('r._time >'));
  assert.equal(report.query_range.from_gps_ist, '2026-10-02 08:33:10.000');
});
test('full-page detail failure keeps exact redacted cause and phase instead of disappearing', async () => {
  const error = Object.assign(new Error(`socket failed token=${cfg.influx.token}`), {
    cause: Object.assign(new Error('read ECONNRESET'), { code: 'ECONNRESET' }),
    body: cfg.influx.token, headers: { authorization: cfg.influx.token }, query: 'do not copy query'
  });
  const client = new HistoryService(cfg, new GpsReportService({ ...serviceOptions(cfg), queryApi: fixture([rows, error]) }));
  const report = await probe({ cfg, client, vehicleNo, from, until }); client.close();
  assert.equal(report.success, false); assert.equal(report.conclusion, 'WORKER_PAGE_QUERY_FAILED');
  assert.equal(report.error.code, 'ARCHIVE_INFLUX_ERROR');
  assert.equal(report.error.diagnostic.query_phase, 'details');
  assert.equal(report.error.diagnostic.causes[0].code, 'ECONNRESET');
  assert.equal(report.error.diagnostic.page_size, 200);
  const serialized = JSON.stringify(report);
  assert.ok(!serialized.includes(cfg.influx.token)); assert.ok(!serialized.includes('do not copy query'));
  assert.equal(report.error.diagnostic.body, undefined);
});
test('SDK HTTP status and compilation message are preserved without the credential', async () => {
  const error = Object.assign(new Error(`compilation failed: field type conflict; Authorization: Token ${cfg.influx.token}`), { statusCode: 400 });
  const client = new HistoryService(cfg, new GpsReportService({ ...serviceOptions(cfg), queryApi: fixture([error]) }));
  const report = await probe({ cfg, client, vehicleNo, from, until }); client.close();
  assert.equal(report.error.status, 400); assert.equal(report.error.diagnostic.query_phase, 'candidates');
  assert.match(report.error.diagnostic.message, /compilation failed/);
  assert.ok(!JSON.stringify(report).includes(cfg.influx.token));
});
test('single-page success is not reported as full-window recovery', async () => {
  const client = new HistoryService(cfg, new GpsReportService({ ...serviceOptions(cfg), queryApi: fixture([rows, rows.slice(0,100), rows.slice(100,200)]) }));
  const report = await probe({ cfg, client, vehicleNo, from, until, maxPages: 1 }); client.close();
  assert.equal(report.success, true); assert.equal(report.window_complete, false);
  assert.equal(report.conclusion, 'PAGES_VALIDATED_MORE_HISTORY_REMAINS');
});
test('unsupported negative index shift fails inside the history service', async () => {
  const bad = { ...rows[0], date_time: utcText(from + 19800000) };
  const client = new HistoryService(cfg, new GpsReportService({ ...serviceOptions(cfg), queryApi: fixture([[bad], [bad]]) }));
  const report = await probe({ cfg, client, vehicleNo, from, until }); client.close();
  assert.equal(report.success, false); assert.equal(report.error.code, 'ARCHIVE_TIME_MISMATCH');
  assert.equal(report.conclusion, 'WORKER_PAGE_QUERY_FAILED');
});
test('invalid PVT header is counted as rejection, never transition evidence', async () => {
  const bad = { ...rows[0], gps_rawdata: '$ALT,ACT1' };
  const client = new HistoryService(cfg, new GpsReportService({ ...serviceOptions(cfg), queryApi: fixture([[bad], [bad]]) }));
  const report = await probe({ cfg, client, vehicleNo, from, until }); client.close();
  assert.equal(report.success, true); assert.equal(report.accepted, 0); assert.equal(report.rejected.NOT_PVT, 1);
});
test('diagnostic masking handles cause cycles, credentials, long values and bounded text', () => {
  const error = new Error(`https://user:pass@influx.test Authorization: Bearer ${cfg.influx.token} ${cfg.cursorSecret} ${'x'.repeat(2000)}`);
  error.cause = error;
  const info = errorDetails(error, cfg), text = JSON.stringify(info);
  assert.ok(!text.includes(cfg.influx.token)); assert.ok(!text.includes(cfg.cursorSecret)); assert.ok(!text.includes('user:pass'));
  assert.ok(info.message.length <= 1500); assert.equal(info.causes, undefined);
  assert.equal(safeConfig(cfg).token_configured, true); assert.ok(!JSON.stringify(safeConfig(cfg)).includes(cfg.influx.token));
});
test('probe accepts explicit UTC only and enforces bounded range/page count', () => {
  assert.equal(parseArgs([vehicleNo, '2026-10-02T03:03:10Z', '2026-10-02T09:03:10Z']).maxPages, 2);
  for (const args of [
    [vehicleNo, '2026-10-02 03:03:10', '2026-10-02T09:03:10Z'],
    [vehicleNo, '2026-09-31T03:03:10Z', '2026-10-02T09:03:10Z'],
    [vehicleNo, '2026-10-02T03:03:10Z', '2026-10-02T09:03:11Z'],
    [vehicleNo, '2026-10-02T03:03:10Z', '2026-10-02T09:03:10Z', '11']
  ]) assert.throws(() => parseArgs(args));
});
