'use strict';
const BUILD = '2026-10-02-influx-gps-utc-index-fix';
function createCounts() {
  return { processed_packets: 0, history_packets_enqueued: 0, history_records_scanned: 0,
    history_context_packets: 0, missing_history_payloads: 0, flushed_packets: 0,
    started: 0, ended: 0, errors: 0, last_successful_flush_at: null,
    last_flushed_vehicle: null, last_flushed_gps_timestamp: null };
}
function recordReplay(counts, result) {
  // Compatibility alias: processed_packets has always counted history submissions,
  // including replays. It does NOT count unique packets or MySQL trip updates.
  counts.history_packets_enqueued += result.processed;
  counts.processed_packets = counts.history_packets_enqueued;
  counts.history_records_scanned += result.scanned;
  counts.history_context_packets += result.preceding;
  counts.missing_history_payloads += result.missing;
}
function recordFlush(counts, vehicleNo, result, now = Date.now()) {
  // Called only after the MySQL commit and Redis acknowledgement both succeed.
  counts.flushed_packets += result.flushed;
  counts.last_successful_flush_at = new Date(now).toISOString();
  counts.last_flushed_vehicle = vehicleNo;
  counts.last_flushed_gps_timestamp = Math.max(...result.intent.packets.map(p => p.ts));
}
module.exports = { BUILD, createCounts, recordReplay, recordFlush };
