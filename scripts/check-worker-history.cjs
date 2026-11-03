#!/usr/bin/env node
'use strict';
// Read-only Influx probe: the same configuration order, page size, wrapper,
// signed pagination and PVT/UTC validation as the worker. No DB/Redis/MQTT.
const fs = require('node:fs');
const path = require('node:path');
const { HistoryService, archivePacket } = require('../src/archive-client');
const { safeConfig, errorDetails } = require('../src/archive-diagnostics');
const { vehicle, sqlTime } = require('../src/domain');
const { BUILD } = require('../src/worker-metrics');

function parseArgs(args) {
  if (![3, 4].includes(args.length)) throw Error('Usage: node scripts/check-worker-history.cjs VEHICLE FROM_UTC TO_UTC [MAX_PAGES=2]');
  const times = args.slice(1, 3).map(value => {
    if (!/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,3})?Z$/.test(value)) throw Error('Dates require explicit UTC ISO ending in Z');
    const ts = Date.parse(value);
    if (!Number.isFinite(ts) || new Date(ts).toISOString().slice(0, 19) !== value.slice(0, 19)) throw Error('Invalid UTC calendar date');
    return ts;
  });
  if (times[0] >= times[1] || times[1] - times[0] > 6 * 3600000) throw Error('Select a positive interval of at most six hours');
  const maxPages = Number(args[3] ?? 2);
  if (!Number.isInteger(maxPages) || maxPages < 1 || maxPages > 10) throw Error('MAX_PAGES must be 1..10');
  return { vehicleNo: vehicle(args[0]), from: times[0], until: times[1], maxPages };
}
async function probe({ cfg, client, vehicleNo, from, until, maxPages = 2 }) {
  const report = { kind: 'worker_history_check', build: BUILD, captured_at_utc: new Date().toISOString(),
    read_only: true, success: false, stage: 'page_query', vehicle_no: vehicleNo,
    config: safeConfig(cfg), config_load_order: 'src/config.js then src/archive-config.js (same as worker)',
    scope: 'Bounded history validation only; does not inspect running PM2 environment or write trip data.',
    query_range: { from_gps_utc: new Date(from).toISOString(), until_gps_utc_exclusive: new Date(until).toISOString(),
      from_gps_ist: sqlTime(from, '+05:30'), until_gps_ist_exclusive: sqlTime(until, '+05:30') },
    max_pages: maxPages, pages: [], scanned: 0, accepted: 0, rejected: {} };
  let cursor = null, lastGpsTimestamp = null;
  try {
    for (let n = 1; n <= maxPages; n++) {
      report.stage = 'page_query'; report.attempted_page = n;
      const began = performance.now();
      const page = await client.page(vehicleNo, from, until, cursor);
      report.stage = 'packet_validation';
      let accepted = 0;
      for (const row of page.data) {
        const result = archivePacket(row, vehicleNo, from, until);
        report.scanned++;
        if (result.rejected) report.rejected[result.rejected] = (report.rejected[result.rejected] || 0) + 1;
        else {
          if (lastGpsTimestamp != null && result.packet.ts < lastGpsTimestamp)
            throw Object.assign(Error('History is not in ascending GPS UTC order'), { code: 'ARCHIVE_UNSORTED_RESPONSE' });
          lastGpsTimestamp = result.packet.ts; accepted++; report.accepted++;
        }
      }
      report.pages.push({ page: n, count: page.count, accepted, has_more: page.pagination.has_more,
        cursor_used: !!cursor, elapsed_ms: Math.round(performance.now() - began),
        first_gps_utc: page.data[0]?.gps_date_time_iso ?? null, last_gps_utc: page.data.at(-1)?.gps_date_time_iso ?? null,
        first_influx_index_time: page.data[0]?._time ?? null,
        index_offset_seconds_in_page: [...new Set(page.data.map(r => r.influx_index_offset_seconds))] });
      report.window_complete = !page.pagination.has_more;
      cursor = page.pagination.next_cursor;
      if (report.window_complete) break;
    }
    report.success = true; report.stage = 'completed';
    report.conclusion = report.window_complete ? 'SELECTED_WINDOW_VALIDATED' : 'PAGES_VALIDATED_MORE_HISTORY_REMAINS';
    report.next_action = 'Check the restarted worker for archive_recovery_progress, archive_recovery_ready and committed trip events. This probe does not prove MySQL writes.';
  } catch (error) {
    report.error = { ...errorDetails(error, cfg), ...(error.diagnostic ? { diagnostic: error.diagnostic } : {}) };
    report.conclusion = report.stage === 'packet_validation' ? 'WORKER_PACKET_VALIDATION_FAILED' : 'WORKER_PAGE_QUERY_FAILED';
  }
  return report;
}
async function main() {
  let cfg, client, report;
  try {
    const args = parseArgs(process.argv.slice(2));
    // Matches worker.js; does not import Database, repository or Redis modules.
    require('../src/config');
    cfg = require('../src/archive-config');
    client = new HistoryService(cfg);
    report = await probe({ cfg, client, ...args });
  } catch (error) {
    report = { kind: 'worker_history_check', build: BUILD, read_only: true, success: false,
      stage: 'initialization', error: errorDetails(error, cfg) };
  } finally { client?.close(); }
  const filename = path.resolve(`worker-influx-check-${new Date().toISOString().replace(/[:.]/g, '-')}-${process.pid}.json`);
  const content = JSON.stringify(report, null, 2) + '\n';
  fs.writeFileSync(filename, content, { flag: 'wx', mode: 0o600 });
  console.log(`Saved: ${filename}`); console.log(content);
  if (!report.success) process.exitCode = 1;
}
if (require.main === module) main().catch(error => { console.error('Diagnostic failed:', error.code || error.name); process.exitCode = 1; });
module.exports = { parseArgs, probe };
