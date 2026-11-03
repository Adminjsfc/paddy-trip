'use strict';
const cfg = require('./config');
const { Repository } = require('./repository');
const { connection, Store } = require('./redis-store');
const { flushVehicle, reconnectable } = require('./write-buffer');
const { replayBatch } = require('./history-replay');
const { locationTripLog, LocationLogGate } = require('./location-log');
const { BUILD, createCounts, recordReplay, recordFlush } = require('./worker-metrics');
const { vehicle, shardFor, dbTime, inside } = require('./domain');
const archiveCfg = require('./archive-config');
const { HistoryService } = require('./archive-client');
const { safeConfig } = require('./archive-diagnostics');
const { ArchiveStore } = require('./archive-store');
const { ArchiveRecovery } = require('./archive-recovery');
const pause = ms => new Promise(resolve => setTimeout(resolve, ms));
const log = (kind, data = {}) => console.log(JSON.stringify({ at: new Date().toISOString(), shard: cfg.shardId, kind, ...data }));
const signature = rows => JSON.stringify(rows.map(t => [String(t.id), t.route_id, t.trip_status, t.lifting_date, t.trip_date,
  t.source_lat, t.source_lng, t.source_radius, t.destination_lat, t.destination_lng, t.destination_radius, t.time_duration]));
function historyFloor(rows) {
  const times = rows.map(t => dbTime(Number(t.trip_status) === 1 ? t.trip_date : t.lifting_date, cfg.offset)).filter(Number.isFinite);
  return Math.max(Date.now() - cfg.historyMs, times.length ? Math.min(...times) : Date.now()) - 1;
}
async function main() {
  const db = require('./Database');
  if (db.writeConfig.timezone !== cfg.offset) throw new Error('Database timezone must match paddy UTC datetime storage');
  const redis = connection(); await redis.connect();
  const store = new Store(redis);
  if (!(await store.acquire())) { await redis.quit(); throw new Error(`Shard ${cfg.shardId} already owned`); }
  const repository = new Repository(db);
  const historyService = new HistoryService(archiveCfg);
  const archive = new ArchiveRecovery({ store: new ArchiveStore(store), client: historyService,
    cfg: archiveCfg, tripCfg: cfg, log });
  let stop = false, fatal = null, lastRefresh = 0, lastAttempt = 0, lastHealth = 0;
  let vehicles = new Map();
  const retryState = new Map();
  const decisionLogs = new Map();
  const locationLogs = new LocationLogGate(cfg.refreshMs);
  let needReconnect = false, lastReconnect = 0;
  const counts = createCounts();
  const halt = () => { stop = true; historyService.close(); };
  process.on('SIGINT', halt); process.on('SIGTERM', halt);
  let renewing = false;
  const heartbeat = setInterval(async () => {
    if (renewing || stop) return;
    renewing = true;
    try { if (!(await store.renew())) throw new Error('LEASE_LOST'); }
    catch (e) { fatal = e; stop = true; }
    finally { renewing = false; }
  }, Math.floor(cfg.leaseMs / 3));
  function logTrips(trips, reason) {
    if (!cfg.logTripDetails) return;
    for (const status of [0, 1]) {
      const selected = trips.filter(t => Number(t.trip_status) === status);
      log(status === 0 ? 'pending_trip_list' : 'active_trip_list', {
        reason, trip_status: status, trip_count: selected.length,
        trip_ids: selected.map(t => String(t.id)),
        trips: selected.map(t => ({
          trip_id: String(t.id), vehicle_no: t.vehicle_no
        }))
      });
    }
  }
  async function logLocation(v, trips, reason, processedTimestamp = null) {
    if (!cfg.logVehicleLocation || !trips.length) return;
    try {
      const value = await redis.hget(store.keys(v).live, 'packet');
      const point = value ? JSON.parse(value) : null;
      const tracked = await store.loadGeofences(v, trips.map(t => String(t.id)));
      for (const trip of trips) {
        const status = Number(trip.trip_status);
        if (status !== 0 && status !== 1) continue;
        const entry = {
          reason, ...locationTripLog(v, trip, point, cfg, processedTimestamp),
          tracked_geofence: tracked[String(trip.id)] ? {
            journey_state: tracked[String(trip.id)].journey_state,
            last_decision: tracked[String(trip.id)].last_decision,
            source_status: tracked[String(trip.id)].source_status,
            destination_status: tracked[String(trip.id)].destination_status,
            gps_timestamp: tracked[String(trip.id)].gps_timestamp,
            source_entered_at: tracked[String(trip.id)].source_entered_at,
            source_exited_at: tracked[String(trip.id)].source_exited_at,
            source_assumed_in_at: tracked[String(trip.id)].source_assumed_in_at ?? null,
            source_assumption_pending: tracked[String(trip.id)].source_assumption_pending === true,
            source_exit_basis: tracked[String(trip.id)].source_exit_basis ?? null,
            destination_entered_at: tracked[String(trip.id)].destination_entered_at,
            destination_exited_at: tracked[String(trip.id)].destination_exited_at
          } : null
        };
        if (locationLogs.shouldLog(`${v}:${trip.id}`, entry)) {
          log(status === 0 ? 'pending_trip_location' : 'started_trip_location', entry);
        }
      }
    } catch (e) {
      // Observability failures must not turn an already committed write into a retry.
      log('vehicle_location_log_failed', { vehicle_no: v, message: e.message });
    }
  }
  async function refresh() {
    lastAttempt = Date.now();
    await store.assertOwner();
    const rows = await repository.refresh(cfg.shardId);
    const next = new Map();
    for (const row of rows) {
      // Enforce one database spelling so indexed lookup and shard assignment agree.
      let v;
      try {
        v = vehicle(row.vehicle_no);
        if (row.vehicle_no !== v) throw new Error('Normalize vehicle_no in MySQL to uppercase without surrounding spaces');
        if (shardFor(v) !== cfg.shardId) throw new Error('MySQL/JavaScript shard mismatch');
      } catch (e) { log('trip_configuration_error', { trip_id: String(row.id), message: e.message }); continue; }
      row.id = String(row.id);
      if (!next.has(v)) next.set(v, []);
      next.get(v).push(row);
      for (const side of ['source', 'destination']) {
        if (inside({ lat: 0, lng: 0 }, row, side, cfg.radiusMultiplier) === null) {
          log('invalid_geofence', { trip_id: row.id, vehicle_no: v, side,
            trip_status: Number(row.trip_status), route_id: row.route_id,
            store_id: row[`${side}_store_id`], latitude: row[`${side}_lat`],
            longitude: row[`${side}_lng`], radius_km: row[`${side}_radius`],
            affected_action: side === 'source' ? 'START' : 'END',
            reason: `INVALID_${side.toUpperCase()}_GEOFENCE` });
        }
      }
    }
    // Publishing is fenced; local map only changes after the entire refresh succeeds.
    for (const v of vehicles.keys()) if (!next.has(v)) {
      await store.publish(v, []);
      // An unfinished write intent must finish before discarding its recovery context.
      if (!(await store.loadIntent(v))) await archive.prepare(v, []);
    }
    for (const [v, trips] of next) {
      await store.publish(v, trips);
      if (signature(vehicles.get(v) || []) !== signature(trips)) {
        await store.rewind(v, historyFloor(trips) + 1);
      }
    }
    vehicles = next; lastRefresh = Date.now();
    const activeKeys = [...vehicles].flatMap(([v, trips]) => trips.map(t => `${v}:${t.id}`));
    locationLogs.retain(activeKeys);
    const activeSet = new Set(activeKeys);
    for (const key of decisionLogs.keys()) if (!activeSet.has(key)) decisionLogs.delete(key);
    log('trip_cache_refreshed', { vehicles: vehicles.size, trips: rows.length, duration_ms: lastRefresh - lastAttempt });
    logTrips([...vehicles.values()].flat(), 'trip_refresh');
    for (const [v, trips] of vehicles) {
      await logLocation(v, trips, 'minute_refresh');
    }
  }
  async function processVehicle(v) {
    const trips = vehicles.get(v);
    if (!trips?.length || stop) return;
    try {
      const result = await replayBatch(store, v, historyFloor(trips));
      recordReplay(counts, result);
      if (result.missing > 0) log('history_payload_missing', { vehicle_no: v, count: result.missing });
    } catch (e) {
      counts.errors++;
      log('vehicle_retry', { vehicle_no: v, message: e.message, code: e.code });
      if (String(e.message).includes('LEASE_LOST')) { fatal = e; stop = true; }
    }
  }
  async function flushPendingVehicle(v) {
    if (stop || Date.now() < (retryState.get(v)?.at || 0)) return;
    try {
      let archiveCoverage;
      // A previously journaled MySQL operation always reconciles its fixed targets first.
      if (archiveCfg.enabled && !(await store.loadIntent(v))) {
        let rows = vehicles.get(v);
        if (!rows) { rows = await repository.activeForVehicle(v); vehicles.set(v, rows); }
        const prepared = await archive.prepare(v, rows);
        if (!prepared.ready) return;
        archiveCoverage = prepared.coverage;
      }
      const result = await flushVehicle(store, repository, v, { archiveCoverage });
      if (!result) return;
      retryState.delete(v);
      vehicles.set(v, result.rows);
      recordFlush(counts, v, result);
      for (const event of result.events) {
        counts[event.kind === 'trip_started' ? 'started' : 'ended']++;
        log(event.kind, { ...event,
          trip_status: event.kind === 'trip_started' ? 1 : 2, mysql_write: 'COMMITTED',
          message: `Vehicle ${event.vehicle_no}: trip ${event.trip_id} ${event.kind === 'trip_started' ? 'STARTED' : 'ENDED'} at ${event.packet_datetime} IST`
        });
        decisionLogs.delete(`${v}:${event.trip_id}`);
      }
      for (const decision of result.intent.decisions || []) {
        if (['START','END','EVENT_ALREADY_COMMITTED'].includes(decision.reason) || decision.trip_status !== 0) continue;
        const key = `${v}:${decision.trip_id}`;
        const signature = JSON.stringify([decision.reason, decision.blocking_trip_ids]);
        const previous = decisionLogs.get(key);
        if (!previous || previous.signature !== signature || Date.now() - previous.at >= cfg.refreshMs) {
          log('trip_start_waiting', decision);
          decisionLogs.set(key, { signature, at: Date.now() });
        }
      }
      await logLocation(v, result.evaluatedRows || result.rows, 'after_packet_batch',
        Math.max(...result.intent.packets.map(p => p.ts)));
    } catch (e) {
      if (e.code === 'ARCHIVE_RECOVERY_REQUIRED') {
        vehicles.set(v, e.rows);
        log('archive_primary_trip_changed', { vehicle_no: v, trip_ids: e.rows.map(t => String(t.id)) });
        return;
      }
      counts.errors++;
      const failures = (retryState.get(v)?.failures || 0) + 1;
      const delay = Math.min(30000, 1000 * 2 ** Math.min(failures - 1, 5));
      retryState.set(v, { failures, at: Date.now() + delay });
      if (reconnectable(e)) needReconnect = true;
      log('pending_batch_retained', { vehicle_no: v, code: e.code, message: e.message, retry_ms: delay });
      if (String(e.message).includes('LEASE_LOST')) { fatal = e; stop = true; }
    }
  }
  async function recoverPoolIfNeeded() {
    if (!needReconnect || Date.now() - lastReconnect < 10000 || stop) return;
    lastReconnect = Date.now();
    try { if (await db.reinitialize()) needReconnect = false; }
    catch (e) { log('database_reinitialize_failed', { code: e.code, message: e.message }); }
  }
  async function maybeRefresh() {
    if (Date.now() - lastAttempt < cfg.refreshMs) return;
    try { await refresh(); }
    catch (e) {
      counts.errors++;
      if (reconnectable(e)) needReconnect = true;
      log('refresh_failed', { code: e.code, message: e.message, last_success: lastRefresh || null });
      if (String(e.message).includes('LEASE_LOST')) throw e;
    }
  }
  try {
    await maybeRefresh();
    // log('worker_ready', { build: BUILD, start_policy: archiveCfg.enabled ? 'RECOVER_ARCHIVE_BEFORE_MISSING_STATE_FALLBACK' : 'ASSUME_SOURCE_IN_ONLY_WHEN_REDIS_STATE_MISSING',
    //   gps_time_basis: 'UTC_DATE_TIME', mysql_datetime_timezone: cfg.offset, archive_transport: 'IN_PROCESS_INFLUX_SERVICE', archive_recovery_enabled: archiveCfg.enabled, archive_page_size: archiveCfg.pageSize, archive_rescan_ms: archiveCfg.rescanMs, shard_count: cfg.shardCount,
    //   radius_unit: 'km', radius_multiplier: cfg.radiusMultiplier, archive_config: safeConfig(archiveCfg) });
    while (!stop) {
      // Drain persisted packets even when replicas/refresh are unavailable or after restart.
      const pending = await store.pendingVehicles();
      for (let i = 0; i < pending.length && !stop; i += cfg.concurrency) {
        await Promise.all(pending.slice(i, i + cfg.concurrency).map(flushPendingVehicle));
        await recoverPoolIfNeeded();
      }
      const sweep = [...vehicles.keys()];
      for (let i = 0; i < sweep.length && !stop; i += cfg.concurrency) {
        await maybeRefresh();
        // Stop transitioning on stale configuration; recover via the next successful refresh.
        if (Date.now() - lastRefresh > cfg.cacheTtl * 1000) break;
        await Promise.all(sweep.slice(i, i + cfg.concurrency).map(processVehicle));
      }
      await maybeRefresh();
      await recoverPoolIfNeeded();
      if (!stop && Date.now() - lastRefresh <= cfg.cacheTtl * 1000) {
        try { await archive.tick(vehicles); }
        catch (e) {
          log('archive_recovery_storage_error', { code: e.code, message: e.message }); counts.errors++;
          if (/LEASE_LOST/.test(e.message)) throw e;
        }
      }
      if (Date.now() - lastHealth >= 30000) {
        lastHealth = Date.now();
        const pendingNow = await store.pendingVehicles();
        // Report a fresh queue sample, rather than the list from before this sweep.
        for (const v of retryState.keys()) if (!pendingNow.includes(v)) retryState.delete(v);
        const health = { build: BUILD, sampled_at: new Date().toISOString(),
          pending_vehicles: pendingNow.length, retrying_vehicles: retryState.size, shard: cfg.shardId, pid: process.pid, vehicles: vehicles.size,
          trips: [...vehicles.values()].reduce((n, a) => n + a.length, 0),
          last_refresh: lastRefresh ? new Date(lastRefresh).toISOString() : null, refresh_age_ms: lastRefresh ? Date.now() - lastRefresh : null,
          ...counts, ...await archive.health(vehicles) };
        await store.health(health);
        // log('health', health);
      }
      await pause(cfg.pollMs);
    }
    if (fatal) throw fatal;
  } finally {
    stop = true; clearInterval(heartbeat);
    historyService.close();
    await db.close().catch(() => {});
    await store.release().catch(() => {});
    await redis.quit().catch(() => redis.disconnect());
    log('worker_stopped', counts);
  }
}
main().catch(e => { log('fatal', { message: e.message, code: e.code }); process.exitCode = 1; });
