'use strict';
const { randomUUID } = require('node:crypto');
const { geometryKey } = require('./geofence-state');
const { dbTime, inside, distance, sqlTime } = require('./domain');
const { archivePacket, problem } = require('./archive-client');
const { GPS_QUERY_VERSION } = require('./influx/gps-time');
const profile = t => JSON.stringify([String(t.id), t.vehicle_no, geometryKey(t)]);
const floorFor = (t, cfg) => dbTime(Number(t.trip_status) === 1 ? t.trip_date : t.lifting_date, cfg.offset);
function coverageFor(job) {
  return { from: job.from, profiles: job.profiles, sourceInside: job.sourceInside || {} };
}
function covers(coverage, rows, cfg) {
  return rows.every(t => coverage?.profiles?.[String(t.id)] === profile(t) &&
    (!Number.isFinite(floorFor(t, cfg)) || coverage.from <= floorFor(t, cfg)));
}
class ArchiveRecovery {
  constructor({ store, client, cfg, tripCfg, log = () => {}, now = Date.now }) {
    Object.assign(this, { store, client, cfg, tripCfg, log, now }); this.turn = 0;
  }
  async prepare(v, rows) {
    if (!this.cfg.enabled) return { ready: true, coverage: null };
    const active = (rows || []).filter(t => [0, 1].includes(Number(t.trip_status)));
    if (!active.length) { await this.store.remove(v); return { ready: true, coverage: { profiles: {}, from: 0 } }; }
    let job = await this.store.load(v);
    const profiles = Object.fromEntries(active.map(t => [String(t.id), profile(t)]));
    const same = job && covers(coverageFor(job), active, this.tripCfg) && Object.keys(job.profiles).length === active.length;
    const rangeUnblocked = job?.last_error?.code === 'ARCHIVE_RANGE_LIMIT' &&
      Math.floor(this.now() / 1000) * 1000 - job.from <= this.cfg.maxLookbackMs;
    if (same && job.version !== GPS_QUERY_VERSION && !rangeUnblocked) {
      // Old _time-ordered coverage/cursors cannot establish GPS-ordered coverage.
      // Re-scan from the original floor; retain staged valid packets and queued writes.
      const upgraded = { ...job, version: GPS_QUERY_VERSION,
        status: job.status === 'BLOCKED' ? 'BLOCKED' : job.from >= job.until ? 'READY' : 'SCANNING',
        windowFrom: job.from, windowTo: Math.min(job.from + this.cfg.windowMs, job.until),
        cursor: null, lastIndexed: null, lastGpsTimestamp: null,
        scanned: 0, accepted: 0, rejected: {}, retryAt: 0, failures: 0,
        last_error: job.status === 'BLOCKED' ? job.last_error : null,
        completedAt: null, queryUpgradedAt: new Date(this.now()).toISOString() };
      await this.store.save(v, job, upgraded); // No DEL: stage, pending queue and intent survive.
      job = upgraded;
      this.log('archive_recovery_query_upgraded', { vehicle_no: v, query_version: GPS_QUERY_VERSION,
        status: job.status, from_gps_utc: new Date(job.from).toISOString(),
        message: 'Restarting GPS-ordered history scan; existing staged and pending packets retained' });
    }
    if (same && !rangeUnblocked && (job.status !== 'READY' || this.now() < job.rescanAt))
      return { ready: job.status === 'READY', coverage: coverageFor(job), job };
    const floors = active.map(t => floorFor(t, this.tripCfg)).filter(Number.isFinite);
    const until = Math.floor(this.now() / 1000) * 1000, from = floors.length ? Math.min(...floors) : until;
    job = { id: randomUUID(), version: GPS_QUERY_VERSION, status: from >= until ? 'READY' : 'SCANNING', vehicle_no: v,
      profiles, from, until, windowFrom: from, windowTo: Math.min(from + this.cfg.windowMs, until), cursor: null,
      scanned: 0, accepted: 0, rejected: {}, sourceInside: {}, evidence: {}, failures: 0,
      createdAt: new Date(this.now()).toISOString(), rescanAt: this.now() + this.cfg.rescanMs };
    if (until - from > this.cfg.maxLookbackMs) {
      job.status = 'BLOCKED'; job.last_error = { code: 'ARCHIVE_RANGE_LIMIT',
        message: 'Trip begins before GPS_HISTORY_MAX_DAYS; increase the explicit archive limit to recover it' };
    }
    await this.store.start(v, job);
    this.log('archive_recovery_started', { vehicle_no: v, trip_ids: active.map(t => String(t.id)),
      status: job.status, from_gps_utc: new Date(from).toISOString(), until_gps_utc_exclusive: new Date(until).toISOString(),
      from_gps_ist: sqlTime(from, '+05:30'), until_gps_ist_exclusive: sqlTime(until, '+05:30'),
      reason: job.last_error?.code || (!same ? 'TRIP_DISCOVERED_OR_CHANGED' : 'PERIODIC_LATE_HISTORY_SCAN') });
    return { ready: job.status === 'READY', coverage: coverageFor(job), job };
  }
  // One internal history query/import batch per worker tick. Other vehicles keep their normal queues.
  async tick(vehicles) {
    if (!this.cfg.enabled || !vehicles.size) return;
    const entries = [...vehicles];
    for (let i = 0; i < entries.length; i++) {
      const [v, rows] = entries[(this.turn + i) % entries.length];
      const prepared = await this.prepare(v, rows), job = prepared.job;
      if (!job || prepared.ready || job.status === 'BLOCKED' || this.now() < (job.retryAt || 0)) continue;
      this.turn = (this.turn + i + 1) % entries.length;
      try { await this.step(v, rows, job); }
      catch (err) {
        if (/LEASE_LOST|ARCHIVE_JOB_CHANGED/.test(err.message)) throw err;
        const failures = (job.failures || 0) + 1;
        const delay = Math.min(300000, 2000 * 2 ** Math.min(failures - 1, 8));
        const next = { ...job, failures, retryAt: this.now() + delay,
          last_error: { code: err.code || 'ARCHIVE_RECOVERY_ERROR', message: err.message,
            ...(err.status != null ? { status: err.status } : {}),
            ...(err.diagnostic ? { diagnostic: err.diagnostic } : {}) } };
        // The service may have restarted with another signing secret: retry the SAME window from page one.
        if (err.status === 400 && job.cursor) { next.cursor = null; next.lastGpsTimestamp = null; }
        await this.store.save(v, job, next);
        this.log('archive_recovery_retry', { vehicle_no: v, code: next.last_error.code,
          message: next.last_error.message, error: next.last_error,
          retry_ms: delay, buffered_packets: job.accepted,
          trip_ids: Object.keys(job.profiles) });
      }
      return;
    }
  }
  async step(v, rows, job) {
    if (job.status === 'IMPORTING') {
      const remaining = await this.store.importBatch(v, job, this.cfg.importSize);
      if (!remaining) {
        const next = { ...job, status: 'READY', completedAt: new Date(this.now()).toISOString(),
          rescanAt: this.now() + this.cfg.rescanMs, retryAt: 0, failures: 0, last_error: null };
        await this.store.save(v, job, next);
        this.log('archive_recovery_ready', { vehicle_no: v, trip_ids: Object.keys(job.profiles),
          scanned: job.scanned, accepted: job.accepted, rejected: job.rejected, evidence: job.evidence,
          message: 'Historical packets are durably queued; MySQL transitions are still evaluated by the worker' });
      }
      return;
    }
    const page = await this.client.page(v, job.windowFrom, job.windowTo, job.cursor);
    const next = structuredClone(job), packets = [];
    for (const row of page.data) {
      const result = archivePacket(row, v, job.windowFrom, job.windowTo);
      if (result.rejected) { next.rejected[result.rejected] = (next.rejected[result.rejected] || 0) + 1; continue; }
      const p = result.packet;
      if (next.lastGpsTimestamp != null && p.ts < next.lastGpsTimestamp)
        throw problem('ARCHIVE_UNSORTED_RESPONSE', 'History rows must be in ascending GPS order within the requested interval');
      next.lastGpsTimestamp = p.ts; packets.push(p);
      for (const t of rows) {
        const floor = floorFor(t, this.tripCfg);
        if (!Number.isFinite(floor) || p.ts < floor) continue;
        const id = String(t.id), evidence = next.evidence[id] ||= {};
        for (const side of ['source', 'destination']) {
          const within = inside(p, t, side, this.tripCfg.radiusMultiplier);
          if (within === null) continue;
          const metres = distance(p.lat, p.lng, Number(t[`${side}_lat`]), Number(t[`${side}_lng`]));
          const point = { gps_timestamp: p.ts, latitude: p.lat, longitude: p.lng,
            distance_m: Math.round(metres * 1000) / 1000, inside: within };
          if (!evidence[`${side}_nearest`] || point.distance_m < evidence[`${side}_nearest`].distance_m)
            evidence[`${side}_nearest`] = point;
          if (within && !evidence[`${side}_first_inside`]) evidence[`${side}_first_inside`] = point;
          if (side === 'source' && within && Number(t.trip_status) === 0) next.sourceInside[id] = true;
        }
      }
    }
    next.scanned += page.data.length; next.accepted += packets.length;
    if (next.scanned > this.cfg.maxPackets) throw problem('ARCHIVE_SCAN_LIMIT', 'Archive packet limit exceeded; increase GPS_HISTORY_MAX_PACKETS');
    next.failures = 0; next.retryAt = 0; next.last_error = null;
    if (page.pagination.has_more) next.cursor = page.pagination.next_cursor;
    else {
      next.cursor = null; next.lastGpsTimestamp = null; next.windowFrom = job.windowTo;
      next.windowTo = Math.min(next.windowFrom + this.cfg.windowMs, job.until);
      if (next.windowFrom >= job.until) next.status = 'IMPORTING';
    }
    await this.store.save(v, job, next, packets); // A failed Redis save never advances the service cursor.
    this.log('archive_recovery_progress', { vehicle_no: v, trip_ids: Object.keys(job.profiles),
      page_count: page.data.length, scanned: next.scanned, accepted: next.accepted, rejected: next.rejected,
      status: next.status, cursor_present: !!next.cursor,
      window_from_utc: new Date(job.windowFrom).toISOString(), window_to_utc_exclusive: new Date(job.windowTo).toISOString() });
  }
  async health(vehicles) {
    if (!this.cfg.enabled) return { archive_recovery_enabled: false };
    const states = await Promise.all([...vehicles.keys()].map(v => this.store.load(v)));
    return { archive_recovery_enabled: true,
      archive_waiting_vehicles: states.filter(s => s && s.status !== 'READY').length,
      archive_blocked_vehicles: states.filter(s => s?.status === 'BLOCKED').length,
      archive_retrying_vehicles: states.filter(s => s?.last_error && s.status !== 'BLOCKED').length,
      archive_ready_vehicles: states.filter(s => s?.status === 'READY').length };
  }
}
module.exports = { ArchiveRecovery, coverageFor, covers, profile, floorFor };
