'use strict';
const http = require('node:http');
const https = require('node:https');
const { createHash, createHmac, randomBytes, timingSafeEqual } = require('node:crypto');
const { InfluxDB, fluxString } = require('@influxdata/influxdb-client');
const { ApiError, EARTH_M, RAD, boundingBox, collect } = require('./query-utils');
const { sqlTime } = require('../domain');
const { LEGACY_INDEX_SHIFT_MS, GPS_QUERY_VERSION, gpsTime } = require('./gps-time');

const TAGS = ['imei_no', 'vehicle_number', 'packet_type'];
const SORT = ['_gps_time', '_server_sort', ...TAGS, '_time'];
const ALLOWED = new Set(['vehicle_number', 'imei_no', 'imeno', 'latitude', 'longitude',
  'logitude', 'radius', 'radius_m', 'from_date', 'to_date', 'start', 'end', 'packet_type',
  'limit', 'cursor', 'order_by', 'order']);
const str = x => fluxString(String(x)).toString();
const flt = n => Number.isInteger(n) ? `${n}.0` : String(n);
const cmp = (a, b) => a < b ? -1 : a > b ? 1 : 0;
const timeKey = t => t.replace(/(?:\.(\d+))?Z$/, (_, f = '') => `.${f.padEnd(9, '0')}Z`);
const millis = v => /^\d{1,15}$/.test(String(v)) ? Number(v) : 0;
const rowKey = r => JSON.stringify([timeKey(r._time), ...TAGS.map(k => r[k])]);

function numeric(v, name, min, max, integer = false) {
  if (!['string', 'number'].includes(typeof v) || String(v).trim() === '') throw new ApiError(400, `${name} must be numeric`);
  const n = Number(v);
  if (!Number.isFinite(n) || n < min || n > max || (integer && !Number.isInteger(n)))
    throw new ApiError(400, `${name} must be ${integer ? 'an integer ' : ''}between ${min} and ${max}`);
  return n;
}
function alias(p, a, b) {
  if (p[a] !== undefined && p[b] !== undefined && String(p[a]) !== String(p[b])) throw new ApiError(400, `Conflicting ${a} and ${b}`);
  return p[a] ?? p[b];
}
function identifier(v, name) {
  if (v === undefined) return null;
  if (typeof v !== 'string' || !/^[A-Za-z0-9 _.:/-]{1,80}$/.test(v.trim())) throw new ApiError(400, `${name} must be a non-empty identifier string (max 80 characters)`);
  return v.trim();
}
function loadConfig(env = process.env) {
  const c = {
    url: env.INFLUX_URL || 'http://18.60.155.145:8086',
    org: env.INFLUX_ORG || 'AISPL', bucket: env.INFLUX_BUCKET || 'gps_bucket',
    measurement: env.INFLUX_MEASUREMENT || 'device_pvt_log', token: env.INFLUX_TOKEN || '',
    cursorSecret: env.CURSOR_SECRET || randomBytes(32).toString('hex'),
    timezone: env.DATE_TIMEZONE_OFFSET || 'Z',
    defaultRadius: numeric(env.DEFAULT_RADIUS_METERS ?? 500, 'DEFAULT_RADIUS_METERS', 0, Math.PI * EARTH_M),
    lookbackHours: numeric(env.DEFAULT_LOOKBACK_HOURS ?? 24, 'DEFAULT_LOOKBACK_HOURS', 0.01, 87600),
    maxDays: numeric(env.MAX_RANGE_DAYS ?? 31, 'MAX_RANGE_DAYS', 1, 3650),
    maxResults: numeric(env.MAX_RESULTS ?? 500, 'MAX_RESULTS', 1, 1000, true),
    pageSize: numeric(env.DEFAULT_PAGE_SIZE ?? 100, 'DEFAULT_PAGE_SIZE', 1, 1000, true),
    concurrency: numeric(env.MAX_CONCURRENT_QUERIES ?? 8, 'MAX_CONCURRENT_QUERIES', 1, 128, true),
    timeoutMs: numeric(env.QUERY_TIMEOUT_MS ?? 15000, 'QUERY_TIMEOUT_MS', 100, 120000, true)
  };
  if (!['http:', 'https:'].includes(new URL(c.url).protocol)) throw new Error('INFLUX_URL must use http or https');
  if (!/^(?:Z|[+-](?:0\d|1[0-4]):[0-5]\d)$/.test(c.timezone)) throw new Error('Invalid DATE_TIMEZONE_OFFSET');
  if (c.lookbackHours > c.maxDays * 24 || c.pageSize > c.maxResults) throw new Error('Default range or page size exceeds configured maximum');
  return c;
}
function dateValue(value, name, timezone, endDateOnly = false) {
  if (typeof value !== 'string') throw new ApiError(400, `${name} must be a date string`);
  const m = /^(\d{4})-(\d{2})-(\d{2})(?:[T ](\d{2}):(\d{2}):(\d{2})(\.\d{1,3})?(Z|[+-]\d{2}:\d{2})?)?$/.exec(value);
  if (!m) throw new ApiError(400, `${name}: use YYYY-MM-DD, YYYY-MM-DD HH:mm:ss or ISO with timezone`);
  const [, yy, mm, dd, hh = '00', mi = '00', ss = '00', fraction = '', tz] = m;
  const y = Number(yy), month = Number(mm), day = Number(dd);
  if (y < 1970 || y > 2200 || month < 1 || month > 12 || day < 1 ||
      day > new Date(Date.UTC(y, month, 0)).getUTCDate() || +hh > 23 || +mi > 59 || +ss > 59)
    throw new ApiError(400, `${name} is not a valid date`);
  let ms = Date.parse(`${yy}-${mm}-${dd}T${hh}:${mi}:${ss}${fraction}${tz || timezone}`);
  if (!Number.isFinite(ms)) throw new ApiError(400, `${name} has an invalid timezone`);
  if (!m[4] && endDateOnly) ms += 86400000;
  return ms;
}
function parseRequest(input, c, mode, cursor = null, now = Date.now()) {
  if (!input || Array.isArray(input) || typeof input !== 'object') throw new ApiError(400, 'Expected an object');
  for (const key of Object.keys(input)) if (!ALLOWED.has(key)) throw new ApiError(400, `Unknown parameter: ${key}`);
  if (input.order !== undefined && input.order !== 'asc') throw new ApiError(400, 'Reports use order=asc');
  if (input.order_by !== undefined && input.order_by !== 'date_time') throw new ApiError(400, 'Reports use order_by=date_time');
  const vehicle = identifier(input.vehicle_number, 'vehicle_number');
  const imei = identifier(alias(input, 'imei_no', 'imeno'), 'imei_no');
  const types = input.packet_type === undefined ? [] :
    (Array.isArray(input.packet_type) ? input.packet_type : String(input.packet_type).split(','));
  if (types.length > 20 || types.some(v => typeof v !== 'string')) throw new ApiError(400, 'packet_type accepts up to 20 string values');
  const packetTypes = [...new Set(types.map(v => identifier(v, 'packet_type')))].sort();
  let lat = null, lon = null, radius = null;
  if (mode === 'history') {
    if (input.latitude !== undefined || input.longitude !== undefined || input.logitude !== undefined || input.radius !== undefined || input.radius_m !== undefined)
      throw new ApiError(400, 'Use nearby for coordinate/radius filters');
  } else {
    lat = numeric(input.latitude, 'latitude', -90, 90);
    lon = numeric(alias(input, 'longitude', 'logitude'), 'longitude', -180, 180);
    const supplied = alias(input, 'radius_m', 'radius');
    radius = supplied === undefined ? (mode === 'nearest' ? null : c.defaultRadius) : numeric(supplied, 'radius_m', 0, Math.PI * EARTH_M);
  }
  const from = alias(input, 'from_date', 'start'), to = alias(input, 'to_date', 'end');
  const endMs = to === undefined ? (cursor ? Date.parse(cursor.end) : now) : dateValue(to, 'to_date', c.timezone, true);
  const startMs = from === undefined ? (cursor ? Date.parse(cursor.start) : endMs - c.lookbackHours * 3600000) : dateValue(from, 'from_date', c.timezone);
  if (startMs >= endMs) throw new ApiError(400, 'from_date must be before the end boundary');
  if (endMs - startMs > c.maxDays * 86400000) throw new ApiError(400, `Maximum date range is ${c.maxDays} days`);
  if (mode === 'nearest' && input.cursor !== undefined) throw new ApiError(400, 'nearest returns top N results; use nearby for paginated reports');
  return { mode, vehicle, imei, packetTypes, lat, lon, radius,
    start: new Date(startMs).toISOString(), end: new Date(endMs).toISOString(),
    limit: numeric(input.limit ?? (mode === 'nearest' ? 1 : c.pageSize), 'limit', 1, c.maxResults, true) };
}
function baseFilter(p, c) {
  const predicates = [`r._measurement == ${str(c.measurement)}`];
  if (p.vehicle) predicates.push(`r.vehicle_number == ${str(p.vehicle)}`);
  if (p.imei) predicates.push(`r.imei_no == ${str(p.imei)}`);
  if (p.packetTypes.length) predicates.push(`(${p.packetTypes.map(t => `r.packet_type == ${str(t)}`).join(' or ')})`);
  return predicates.join(' and ');
}
function afterCursor(last) {
  const value = (key, i) => key === '_time' ? `time(v: ${str(last[i])})` : key === '_server_sort' ? String(last[i]) : str(last[i]);
  return SORT.map((key, i) => '(' + SORT.slice(0, i).map((k, j) => `r.${k} == ${value(k, j)}`)
    .concat(`r.${key} > ${value(key, i)}`).join(' and ') + ')').join(' or ');
}
function candidateQuery(p, c, last = null) {
  const geo = p.mode !== 'history';
  const fields = geo ? ['date_time', 'latitude', 'longitude', 'server_date_time'] : ['date_time', 'server_date_time'];
  const indexStop = new Date(Date.parse(p.end) + LEGACY_INDEX_SHIFT_MS).toISOString();
  const lines = ['import "strings"', geo ? 'import "math"' : '', `from(bucket: ${str(c.bucket)})`,
    `  |> range(start: time(v: ${str(last ? last[0] : p.start)}), stop: time(v: ${str(indexStop)}))`,
    `  |> filter(fn: (r) => ${baseFilter(p, c)})`,
    `  |> filter(fn: (r) => ${fields.map(f => `r._field == ${str(f)}`).join(' or ')})`,
    '  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")',
    `  |> map(fn: (r) => {
    raw = if exists r.date_time then r.date_time else ""
    shaped = raw =~ /^[0-9]{8} [0-9]{6}$/
    gps = if shaped then strings.substring(v: raw, start: 4, end: 8) + "-" +
      strings.substring(v: raw, start: 2, end: 4) + "-" + strings.substring(v: raw, start: 0, end: 2) + "T" +
      strings.substring(v: raw, start: 9, end: 11) + ":" + strings.substring(v: raw, start: 11, end: 13) + ":" +
      strings.substring(v: raw, start: 13, end: 15) + ".000Z" else ""
    return {r with date_time: raw, _gps_time: gps}
  })`,
    // Missing/malformed dates surface as errors in JS; never silently declare an empty archive.
    `  |> filter(fn: (r) => r._gps_time == "" or (r._gps_time >= ${str(p.start)} and r._gps_time < ${str(p.end)}))`];
  if (geo) {
    lines.push('  |> filter(fn: (r) => exists r.latitude and exists r.longitude)',
      '  |> filter(fn: (r) => r.latitude >= -90.0 and r.latitude <= 90.0 and r.longitude >= -180.0 and r.longitude <= 180.0)');
    if (p.radius !== null) {
      const b = boundingBox(p.lat, p.lon, p.radius);
      const lon = b.allLon ? '' : ` and (r.longitude >= ${flt(b.minLon)} ${b.minLon > b.maxLon ? 'or' : 'and'} r.longitude <= ${flt(b.maxLon)})`;
      lines.push(`  |> filter(fn: (r) => r.latitude >= ${flt(b.minLat)} and r.latitude <= ${flt(b.maxLat)}${lon})`);
    }
    lines.push(`  |> map(fn: (r) => {
    dlat = (r.latitude - ${flt(p.lat)}) * ${flt(RAD)}
    dlon = (r.longitude - ${flt(p.lon)}) * ${flt(RAD)}
    s1 = math.sin(x: dlat / 2.0)
    s2 = math.sin(x: dlon / 2.0)
    a = s1 * s1 + ${flt(Math.cos(p.lat * RAD))} * math.cos(x: r.latitude * ${flt(RAD)}) * s2 * s2
    bounded = if a > 1.0 then 1.0 else if a < 0.0 then 0.0 else a
    return {r with distance_m: ${flt(2 * EARTH_M)} * math.asin(x: math.sqrt(x: bounded))}
  })`);
    if (p.radius !== null) lines.push(`  |> filter(fn: (r) => r.distance_m <= ${flt(p.radius)})`);
  }
  lines.push(`  |> map(fn: (r) => {
    server = if exists r.server_date_time then r.server_date_time else ""
    numericServer = if server =~ /^[0-9]{1,15}$/ then int(v: server) else 0
    return {r with server_date_time: server, _server_sort: numericServer}
  })`);
  const keep = [...SORT, 'date_time', 'server_date_time', ...(geo ? ['latitude', 'longitude', 'distance_m'] : [])];
  lines.push(`  |> keep(columns: ${JSON.stringify(keep)})`);
  if (last) lines.push(`  |> filter(fn: (r) => ${afterCursor(last)})`);
  lines.push('  |> group(columns: [])',
    `  |> sort(columns: ${JSON.stringify(p.mode === 'nearest' ? ['distance_m', ...SORT] : SORT)}, desc: false)`,
    `  |> limit(n: ${p.limit + (p.mode === 'nearest' ? 0 : 1)})`);
  return lines.filter(Boolean).join('\n');
}
function detailQuery(rows, p, c) {
  const times = rows.map(r => Date.parse(r._time));
  const matches = rows.map(r => '(' + ['_time', ...TAGS].map(k => k === '_time'
    ? `r._time == time(v: ${str(r[k])})` : `r.${k} == ${str(r[k])}`).join(' and ') + ')').join(' or ');
  return `from(bucket: ${str(c.bucket)})
  |> range(start: time(v: ${str(new Date(Math.min(...times)).toISOString())}), stop: time(v: ${str(new Date(Math.max(...times) + 1).toISOString())}))
  |> filter(fn: (r) => ${baseFilter(p, c)})
  |> filter(fn: (r) => ${matches})
  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")`;
}
const fingerprint = p => createHash('sha256').update(JSON.stringify({ ...p, limit: undefined, query_version: GPS_QUERY_VERSION })).digest('hex');

class GpsReportService {
  constructor(options = {}) {
    const influx = options.influx || {};
    this.config = loadConfig({ ...process.env,
      ...Object.fromEntries(Object.entries({ INFLUX_URL: influx.url, INFLUX_ORG: influx.org,
        INFLUX_BUCKET: influx.bucket, INFLUX_TOKEN: influx.token, INFLUX_MEASUREMENT: influx.measurement }).filter(([, v]) => v !== undefined)),
      ...(options.env || {}) });
    const c = this.config;
    if (!options.queryApi && (!c.token || c.token.startsWith('YOUR_'))) throw new Error('Set INFLUX_TOKEN in your environment');
    this.activeQueries = 0;
    this.agent = options.queryApi ? null : new (c.url.startsWith('https:') ? https.Agent : http.Agent)({ keepAlive: true, maxSockets: c.concurrency });
    this.queryApi = options.queryApi || new InfluxDB({ url: c.url, token: c.token, timeout: c.timeoutMs,
      transportOptions: { agent: this.agent } }).getQueryApi(c.org);
  }
  close() { this.agent?.destroy(); }
  findNearby(input, options) { return this._run('nearby', input, options); }
  getHistory(input, options) { return this._run('history', input, options); }
  findNearest(input, options) { return this._run('nearest', input, options); }
  iterateNearby(input, options) { return this._iterate('nearby', input, options); }
  iterateHistory(input, options) { return this._iterate('history', input, options); }
  async *_iterate(mode, input, options) {
    let cursor = input.cursor;
    do {
      const page = await this._run(mode, { ...input, ...(cursor ? { cursor } : {}) }, options);
      for (const row of page.data) yield row;
      cursor = page.pagination.next_cursor;
    } while (cursor);
  }
  _encodeCursor(p, last) {
    const body = Buffer.from(JSON.stringify({ v: GPS_QUERY_VERSION, start: p.start, end: p.end, hash: fingerprint(p), last: SORT.map(k => last[k]) })).toString('base64url');
    return body + '.' + createHmac('sha256', this.config.cursorSecret).update(body).digest('base64url');
  }
  _decodeCursor(token) {
    if (typeof token !== 'string' || token.length > 4096) throw new ApiError(400, 'Invalid cursor');
    try {
      const [body, signature, extra] = token.split('.');
      const expected = createHmac('sha256', this.config.cursorSecret).update(body).digest();
      const got = Buffer.from(signature || '', 'base64url');
      if (extra || got.length !== expected.length || !timingSafeEqual(got, expected)) throw new Error();
      const result = JSON.parse(Buffer.from(body, 'base64url').toString());
      if (result.v !== GPS_QUERY_VERSION || !Array.isArray(result.last) || result.last.length !== SORT.length) throw new Error();
      return result;
    } catch { throw new ApiError(400, 'Invalid or expired cursor'); }
  }
  async _run(mode, input, options = {}) {
    const c = this.config;
    const cursor = input?.cursor === undefined ? null : this._decodeCursor(input.cursor);
    const p = parseRequest(input, c, mode, cursor);
    if (cursor && cursor.hash !== fingerprint(p)) throw new ApiError(400, 'Cursor does not match the report filters; start a new report');
    if (this.activeQueries >= c.concurrency) throw new ApiError(503, 'Service busy; retry shortly');
    const controller = new AbortController();
    const forwardAbort = () => controller.abort(options.signal.reason || new ApiError(499, 'Request cancelled'));
    if (options.signal?.aborted) forwardAbort();
    else options.signal?.addEventListener('abort', forwardAbort, { once: true });
    const timer = setTimeout(() => controller.abort(new ApiError(504, 'Query timed out; shorten the date range')), c.timeoutMs);
    this.activeQueries++;
    try { return await this._query(p, cursor?.last, controller.signal); }
    finally { clearTimeout(timer); options.signal?.removeEventListener('abort', forwardAbort); this.activeQueries--; }
  }
  async _query(p, last, signal) {
    const began = performance.now(), c = this.config;
    let phase = 'candidates';
    try {
    const candidates = await collect(this.queryApi, candidateQuery(p, c, last), signal, p.limit + 1);
    const hasMore = p.mode !== 'nearest' && candidates.length > p.limit;
    const selected = candidates.slice(0, p.limit);
    const afterSearch = performance.now();
    const raw = [];
    phase = 'details';
    for (let offset = 0; offset < selected.length; offset += 100) {
      const part = selected.slice(offset, offset + 100);
      raw.push(...await collect(this.queryApi, detailQuery(part, p, c), signal, part.length));
    }
    phase = 'materialize';
    const byKey = new Map(raw.map(r => [rowKey(r), r]));
    const data = selected.map((s, index) => {
      const r = byKey.get(rowKey(s));
      if (!r || r.date_time !== s.date_time || String(r.server_date_time ?? '') !== String(s.server_date_time ?? '') ||
          (p.mode !== 'history' && (r.latitude !== s.latitude || r.longitude !== s.longitude)))
        throw new ApiError(409, 'Selected packets changed between reads; retry the page');
      const { result, table, _start, _stop, ...packet } = r;
      const ms = millis(r.server_date_time);
      const { ts, shift } = gpsTime(r, false);
      const gpsIso = new Date(ts).toISOString();
      if (s._gps_time !== gpsIso || ts < Date.parse(p.start) || ts >= Date.parse(p.end))
        throw new ApiError(409, 'GPS date_time changed or lies outside the selected UTC interval; retry the page');
      // Preserve physical _time; GPS aliases, trip decisions and display use UTC date_time.
      return { ...packet, gps_date_time_iso: gpsIso, gps_date_time_ist: sqlTime(ts, '+05:30'),
        influx_index_time: r._time, influx_index_offset_seconds: shift / 1000,
        server_date_time_iso: ms ? new Date(ms).toISOString() : null,
        server_date_time_ist: ms ? sqlTime(ms, '+05:30') : null,
        ...(p.mode !== 'history' ? { distance_m: s.distance_m } : {}),
        ...(p.mode === 'nearest' ? { nearest_rank: index + 1 } : {}) };
    });
    if (p.mode === 'nearest') data.sort((a, b) => cmp(a.gps_date_time_iso, b.gps_date_time_iso) ||
      cmp(millis(a.server_date_time), millis(b.server_date_time)) || TAGS.reduce((v, k) => v || cmp(a[k], b[k]), 0));
    return {
      success: true, report: p.mode, count: data.length, display_timezone: 'Asia/Kolkata',
      vehicle_count_in_page: new Set(data.map(r => r.vehicle_number)).size,
      filters: { vehicle_number: p.vehicle, imei_no: p.imei, packet_type: p.packetTypes,
        latitude: p.lat, longitude: p.lon, radius_m: p.radius,
        from_date: p.start, to_date_exclusive: p.end, query_timezone: 'UTC',
        gps_time_basis: 'UTC_DATE_TIME', accepted_index_offsets_seconds: [0, LEGACY_INDEX_SHIFT_MS / 1000],
        from_date_ist: sqlTime(Date.parse(p.start), '+05:30'),
        to_date_exclusive_ist: sqlTime(Date.parse(p.end), '+05:30'),
        order_by: 'date_time UTC ASC, server_date_time ASC, imei_no ASC, vehicle_number ASC, packet_type ASC, _time ASC' },
      pagination: { limit: p.limit, has_more: hasMore,
        next_cursor: hasMore ? this._encodeCursor(p, selected[selected.length - 1]) : null },
      timing_ms: { search: +(afterSearch - began).toFixed(2), total: +(performance.now() - began).toFixed(2) },
      data
    };
    } catch (error) {
      if (error && typeof error === 'object') error.query_phase ||= phase;
      throw error;
    }
  }
}

module.exports = { GpsReportService, loadConfig, ApiError, candidateQuery };
