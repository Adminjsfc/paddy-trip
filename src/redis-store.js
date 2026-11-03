'use strict';
const Redis = require('ioredis');
const { randomUUID } = require('node:crypto');
const cfg = require('./config');
const { publicPacket } = require('./mqtt-packet');
const { shardFor, vehicle, normalizePacket } = require('./domain');
function connection() {
  const r = new Redis(cfg.redisUrl, { maxRetriesPerRequest: 1, enableOfflineQueue: false,
    lazyConnect: true, connectTimeout: 10000, commandTimeout: 10000 });
  r.on('error', e => console.error(JSON.stringify({ kind: 'redis_error', message: e.message })));
  return r;
}
const INGEST = `
local ts = tonumber(ARGV[1])
local old = redis.call('HGET', KEYS[2], ARGV[1])
if old then
  local a = cjson.decode(old)
  local b = cjson.decode(ARGV[2])
  if (a.latitude or a.lat) == b.latitude and (a.longitude or a.lng) == b.longitude
    and (a.gps_timestamp or a.ts) == b.gps_timestamp then
    -- Same event may be redelivered after a crash. Requeue; Redis completion markers prevent replayed writes.
  else return -1 end
end
local pending = redis.call('HGET', KEYS[6], ARGV[1])
if pending then
  local a = cjson.decode(pending)
  local b = cjson.decode(ARGV[2])
  if a.latitude ~= b.latitude or a.longitude ~= b.longitude then return -1 end
end
-- Write-ahead inbox has NO expiry, independent of rolling history/cache TTLs.
redis.call('HSET', KEYS[6], ARGV[1], ARGV[2])
redis.call('ZADD', KEYS[5], ts, ARGV[1])
redis.call('SADD', KEYS[7], ARGV[5])
redis.call('PERSIST', KEYS[5])
redis.call('PERSIST', KEYS[6])
redis.call('PERSIST', KEYS[7])
redis.call('HSET', KEYS[2], ARGV[1], ARGV[2])
redis.call('ZADD', KEYS[1], ts, ARGV[1])
local latest = tonumber(redis.call('HGET', KEYS[3], 'ts') or '-1')
if ts > latest then redis.call('HSET', KEYS[3], 'ts', ARGV[1], 'packet', ARGV[2]) end
local rewind = tonumber(redis.call('GET', KEYS[4]) or '9007199254740991')
if ts < rewind then redis.call('SET', KEYS[4], ARGV[1]) end
local expired = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', '(' .. ARGV[3], 'LIMIT', 0, 1000)
for _, id in ipairs(expired) do
  redis.call('ZREM', KEYS[1], id)
  redis.call('HDEL', KEYS[2], id)
end
for i = 1, 4 do redis.call('EXPIRE', KEYS[i], ARGV[4]) end
if old then return 0 else return 1 end`;
const REWIND = `
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return redis.error_reply('LEASE_LOST') end
local old = tonumber(redis.call('GET', KEYS[2]) or '9007199254740991')
if tonumber(ARGV[2]) < old then redis.call('SET', KEYS[2], ARGV[2], 'EX', ARGV[3]) end
return 1`;
const BEGIN = `
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return redis.error_reply('LEASE_LOST') end
local cursor = tonumber(redis.call('GET', KEYS[2]) or ARGV[2])
local rewind = redis.call('GET', KEYS[3])
if rewind then cursor = math.min(cursor, tonumber(rewind) - 1) end
cursor = math.max(cursor, tonumber(ARGV[2]))
redis.call('SET', KEYS[2], tostring(cursor), 'EX', ARGV[3])
redis.call('DEL', KEYS[3])
return tostring(cursor)`;
const ADVANCE = `
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return redis.error_reply('LEASE_LOST') end
redis.call('SET', KEYS[2], ARGV[2], 'EX', ARGV[3])
return 1`;
const PUBLISH = `
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return redis.error_reply('LEASE_LOST') end
local previous = redis.call('HKEYS', KEYS[2])
local keep = {}
local rows = cjson.decode(ARGV[2])
for _, row in ipairs(rows) do
  local id = tostring(row.id)
  keep[id] = true
  local data = cjson.encode(row)
  redis.call('HSET', KEYS[2], id, data)
  redis.call('SET', ARGV[4] .. id, data, 'EX', ARGV[3])
end
for _, id in ipairs(previous) do
  if not keep[id] then
    redis.call('HDEL', KEYS[2], id)
    redis.call('DEL', ARGV[4] .. id)
  end
end
redis.call('EXPIRE', KEYS[2], ARGV[3])
return 1`;
// Scripts use one shard hash tag. This client targets a standalone Redis endpoint,
// NOT Redis Cluster; PUBLISH builds dynamic same-shard cache keys.
class Store {
  constructor(redis, shard = cfg.shardId) {
    this.r = redis; this.shard = shard; this.token = randomUUID();
    this.base = `${cfg.prefix}:{s${shard}}`;
    this.lease = `${this.base}:owner`;
  }
  keys(v) {
    const b = `${this.base}:vehicle:${vehicle(v)}`;
    return { history: `${b}:history`, packets: `${b}:packets`, live: `${b}:live`,
      rewind: `${b}:rewind`, cursor: `${b}:cursor`, trips: `${b}:trips`,
      pendingOrder: `${b}:pending:order`, pendingData: `${b}:pending:data` };
  }
  async acquire() { return (await this.r.set(this.lease, this.token, 'PX', cfg.leaseMs, 'NX')) === 'OK'; }
  async renew() {
    return Number(await this.r.eval(`if redis.call('GET',KEYS[1])==ARGV[1] then return redis.call('PEXPIRE',KEYS[1],ARGV[2]) else return 0 end`,
      1, this.lease, this.token, cfg.leaseMs)) === 1;
  }
  async assertOwner() { if (await this.r.get(this.lease) !== this.token) throw new Error('LEASE_LOST'); }
  async release() {
    await this.r.eval(`if redis.call('GET',KEYS[1])==ARGV[1] then return redis.call('DEL',KEYS[1]) else return 0 end`, 1, this.lease, this.token);
  }
  async publish(v, rows) {
    // IDs are strings so Lua JSON never rounds BIGINT identifiers.
    const payload = rows.map(row => ({ ...row, id: String(row.id) }));
    await this.r.eval(PUBLISH, 2, this.lease, this.keys(v).trips, this.token,
      JSON.stringify(payload), cfg.cacheTtl, `${this.base}:trip:`);
  }
  async rewind(v, ts) {
    await this.r.eval(REWIND, 2, this.lease, this.keys(v).rewind, this.token, ts,
      Math.ceil(cfg.historyMs / 1000));
  }
  async begin(v, floor) {
    const k = this.keys(v);
    return Number(await this.r.eval(BEGIN, 3, this.lease, k.cursor, k.rewind,
      this.token, floor, Math.ceil(cfg.historyMs / 1000)));
  }
  async batch(v, cursor) {
    const k = this.keys(v);
    const times = await this.r.zrangebyscore(k.history, `(${cursor}`, Date.now(), 'LIMIT', 0, cfg.batchSize);
    if (!times.length) return [];
    const payloads = await this.r.hmget(k.packets, ...times);
    return times.map((ts, i) => {
      const raw = payloads[i] ? JSON.parse(payloads[i]) : null;
      const packet = raw && ('latitude' in raw ? { vehicle_no: vehicle(v), lat: raw.latitude,
        lng: raw.longitude, ts: raw.gps_timestamp, packet_type: raw.packet_type, gps_fix: raw.gps_fix } : raw);
      return { ts: Number(ts), packet };
    });
  }
  async advance(v, ts) {
    await this.r.eval(ADVANCE, 2, this.lease, this.keys(v).cursor, this.token, ts,
      Math.ceil(cfg.historyMs / 1000));
  }
  async before(v, cursor) {
    const k = this.keys(v);
    const times = await this.r.zrevrangebyscore(k.history, cursor, Date.now() - cfg.historyMs, 'LIMIT', 0, 1);
    if (!times.length) return null;
    const value = await this.r.hget(k.packets, times[0]);
    if (!value) return null;
    const p = JSON.parse(value);
    return 'latitude' in p ? { vehicle_no: vehicle(v), lat: p.latitude, lng: p.longitude,
      ts: p.gps_timestamp, packet_type: p.packet_type, gps_fix: p.gps_fix } : p;
  }
  async enqueueHistory(v, packets) {
    if (!packets.length) return;
    const k = this.keys(v);
    // Replay into the durable inbox only: do not change latest location or rewind
    // the history cursor again, which would cause an endless replay loop.
    await this.r.eval(`
      if redis.call('GET',KEYS[1]) ~= ARGV[1] then return redis.error_reply('LEASE_LOST') end
      local packets = cjson.decode(ARGV[3])
      for _, p in ipairs(packets) do
        local ts = tostring(p.gps_timestamp)
        local old = redis.call('HGET',KEYS[3],ts)
        if old then
          local a = cjson.decode(old)
          if a.latitude ~= p.latitude or a.longitude ~= p.longitude then
            return redis.error_reply('CONFLICTING_HISTORY_PACKET')
          end
        end
      end
      for _, p in ipairs(packets) do
        local ts = tostring(p.gps_timestamp)
        redis.call('ZADD',KEYS[2],p.gps_timestamp,ts)
        redis.call('HSET',KEYS[3],ts,cjson.encode(p))
      end
      redis.call('SADD',KEYS[4],ARGV[2])
      return 1`, 4, this.lease, k.pendingOrder, k.pendingData,
      `${this.base}:pending:vehicles`, this.token, vehicle(v),
      JSON.stringify(packets.map(publicPacket)));
  }
  async loadGeofences(v, tripIds = []) {
    const values = await this.r.hgetall(`${this.base}:vehicle:${vehicle(v)}:trip-geofences`);
    const ids = [...new Set([...Object.keys(values), ...tripIds.map(String)])];
    if (!ids.length) return {};
    const individual = await this.r.mget(...ids.map(id => `${this.base}:vehicle:${vehicle(v)}:trip:${id}:geofence`));
    // The individual vehicle/trip key is authoritative; the hash remains an index
    // and an upgrade fallback. Missing hash fields cannot hide a persisted IN state.
    return Object.fromEntries(ids.flatMap((id, i) => {
      const value = individual[i] ?? values[id];
      return value ? [[id, JSON.parse(value)]] : [];
    }));
  }
  async loadIntent(v) {
    const text = await this.r.get(`${this.base}:vehicle:${vehicle(v)}:write-intent`);
    return text ? JSON.parse(text) : null;
  }
  async saveIntent(v, intent) {
    await this.r.eval(`
      if redis.call('GET',KEYS[1]) ~= ARGV[1] then return redis.error_reply('LEASE_LOST') end
      local old = redis.call('GET',KEYS[2])
      if old and cjson.decode(old).id ~= ARGV[3] then return redis.error_reply('INTENT_ALREADY_EXISTS') end
      redis.call('SET',KEYS[2],ARGV[2])
      redis.call('SADD',KEYS[3],ARGV[4])
      return 1`, 3, this.lease, `${this.base}:vehicle:${vehicle(v)}:write-intent`,
      `${this.base}:pending:vehicles`, this.token, JSON.stringify(intent), intent.id, vehicle(v));
  }
  async completedEvents(v, ids) {
    if (!ids.length) return new Set();
    const done = new Set();
    for (let i = 0; i < ids.length; i += 500) {
      const group = ids.slice(i, i + 500);
      const values = await this.r.hmget(`${this.base}:vehicle:${vehicle(v)}:completed-events`, ...group);
      group.forEach((id, n) => { if (values[n] !== null) done.add(id); });
    }
    return done;
  }
  async finishIntent(v, intent, activeRows) {
    const k = this.keys(v);
    const { transitionId } = require('./repository');
    const completed = [...new Set(intent.operations.map(op => transitionId(op.packet, op.trip_id, op.kind)))];
    const states = structuredClone(intent.geofences || {});
    for (const row of activeRows || []) {
      const state = states[String(row.id)];
      if (state) {
        state.trip_status = Number(row.trip_status);
        state.journey_state = require('./geofence-state').journeyState(row, state);
      }
    }
    await this.r.eval(`
      if redis.call('GET',KEYS[1]) ~= ARGV[1] then return redis.error_reply('LEASE_LOST') end
      local value = redis.call('GET',KEYS[2])
      if not value or cjson.decode(value).id ~= ARGV[2] then return redis.error_reply('INTENT_CHANGED') end
      local ids = cjson.decode(ARGV[4])
      for _, id in ipairs(ids) do redis.call('HSET',KEYS[3],id,'1') end
      local times = cjson.decode(ARGV[5])
      for _, ts in ipairs(times) do
        redis.call('ZREM',KEYS[4],ts)
        redis.call('HDEL',KEYS[5],ts)
      end
      local states = cjson.decode(ARGV[6])
      local ended = {}
      for _, id in ipairs(cjson.decode(ARGV[8])) do ended[id] = true end
      local active = {}
      for _, id in ipairs(cjson.decode(ARGV[9])) do active[id] = true end
      -- Individual state keys and vehicle lookup hash change atomically with acknowledgement.
      for id, state in pairs(states) do
        local key = ARGV[7] .. id .. ':geofence'
        if tonumber(state.trip_status) == 2 or ended[id] or (ARGV[10] == '1' and not active[id]) then
          redis.call('HDEL',KEYS[7],id)
          redis.call('DEL',key)
        else
          local data = cjson.encode(state)
          redis.call('HSET',KEYS[7],id,data)
          redis.call('SET',key,data)
        end
      end
      -- Handles journals created before geofence state was introduced.
      for id, _ in pairs(ended) do
        redis.call('HDEL',KEYS[7],id)
        redis.call('DEL',ARGV[7] .. id .. ':geofence')
      end
      redis.call('DEL',KEYS[2])
      if redis.call('ZCARD',KEYS[4]) == 0 then
        redis.call('DEL',KEYS[4],KEYS[5])
        redis.call('SREM',KEYS[6],ARGV[3])
      end
      return 1`, 7, this.lease, `${this.base}:vehicle:${vehicle(v)}:write-intent`,
      `${this.base}:vehicle:${vehicle(v)}:completed-events`, k.pendingOrder, k.pendingData,
      `${this.base}:pending:vehicles`, `${this.base}:vehicle:${vehicle(v)}:trip-geofences`, this.token, intent.id, vehicle(v), JSON.stringify(completed),
      JSON.stringify(intent.packets.map(p => String(p.ts))), JSON.stringify(states),
      `${this.base}:vehicle:${vehicle(v)}:trip:`,
      JSON.stringify([...new Set(intent.operations.filter(op => op.kind === 'end').map(op => String(op.trip_id)))]),
      JSON.stringify((activeRows || []).map(t => String(t.id))), activeRows ? '1' : '0');
  }
  async pendingVehicles() { return this.r.smembers(`${this.base}:pending:vehicles`); }
  async pendingBatch(v) {
    const k = this.keys(v);
    const ids = await this.r.zrangebyscore(k.pendingOrder, '-inf', Date.now(), 'LIMIT', 0, cfg.writeBatchSize);
    if (!ids.length) return [];
    const values = await this.r.hmget(k.pendingData, ...ids);
    return ids.map((id, i) => {
      if (!values[i]) throw new Error(`Pending payload missing for ${v}/${id}; not acknowledged`);
      const p = JSON.parse(values[i]);
      return { vehicle_no: vehicle(v), lat: p.latitude, lng: p.longitude,
        ts: p.gps_timestamp, packet_type: p.packet_type, gps_fix: p.gps_fix };
    });
  }
  async health(data) {
    await this.r.set(`${this.base}:health`, JSON.stringify(data), 'EX', 120);
  }
}
async function ingest(redis, input) {
  const p = normalizePacket(input, cfg);
  const store = new Store(redis, shardFor(p.vehicle_no)), k = store.keys(p.vehicle_no);
  const result = Number(await redis.eval(INGEST, 7, k.history, k.packets, k.live, k.rewind,
    k.pendingOrder, k.pendingData, `${store.base}:pending:vehicles`,
    p.ts, JSON.stringify(publicPacket(p)), Date.now() - cfg.historyMs, Math.ceil(cfg.historyMs / 1000), p.vehicle_no));
  if (result === -1) throw new Error('Conflicting GPS location for the same vehicle/event timestamp');
  return { accepted: result === 1, duplicate: result === 0, shard: store.shard, packet: publicPacket(p) };
}
async function verifyDurability(redis) {
  if (!cfg.requireDurableRedis) return;
  const required = { appendonly: 'yes', appendfsync: 'always', 'maxmemory-policy': 'noeviction',
    'no-appendfsync-on-rewrite': 'no' };
  for (const [key, expected] of Object.entries(required)) {
    const result = await redis.config('GET', key);
    if (result[1] !== expected) throw new Error(`Redis ${key} must be ${expected} for durable ingestion`);
  }
}
module.exports = { connection, Store, ingest, verifyDurability, INGEST, BEGIN, ADVANCE };
