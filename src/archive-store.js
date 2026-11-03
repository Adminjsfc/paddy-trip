'use strict';
const { vehicle } = require('./domain');
const { publicPacket } = require('./mqtt-packet');
const cfg = require('./config');
const FENCE = `if redis.call('GET',KEYS[1]) ~= ARGV[1] then return redis.error_reply('LEASE_LOST') end\n`;
const JOB = `local raw=redis.call('GET',KEYS[2])
if not raw or cjson.decode(raw).id ~= ARGV[2] then return redis.error_reply('ARCHIVE_JOB_CHANGED') end\n`;
class ArchiveStore {
  constructor(store) { this.store = store; this.r = store.r; }
  keys(v) {
    const b = `${this.store.base}:vehicle:${vehicle(v)}`;
    return { state: `${b}:archive-recovery`, order: `${b}:archive-stage:order`, data: `${b}:archive-stage:data` };
  }
  async load(v) { const raw = await this.r.get(this.keys(v).state); return raw ? JSON.parse(raw) : null; }
  async start(v, job) {
    const k = this.keys(v);
    await this.r.eval(FENCE + `redis.call('SET',KEYS[2],ARGV[2]); redis.call('DEL',KEYS[3],KEYS[4]); return 1`,
      4, this.store.lease, k.state, k.order, k.data, this.store.token, JSON.stringify(job));
  }
  async save(v, previous, next, packets = []) {
    const k = this.keys(v);
    // Validate the complete page first; Lua errors do not undo earlier commands.
    await this.r.eval(FENCE + JOB + `
local packets=cjson.decode(ARGV[4]); local seen={}
for _,p in ipairs(packets) do
 local ts=tostring(p.gps_timestamp); local old=seen[ts] or redis.call('HGET',KEYS[4],ts)
 if old then local a=cjson.decode(old)
  if a.latitude ~= p.latitude or a.longitude ~= p.longitude then return redis.error_reply('ARCHIVE_CONFLICTING_PACKET') end
 end
 seen[ts]=cjson.encode(p)
end
for ts,p in pairs(seen) do redis.call('ZADD',KEYS[3],tonumber(ts),ts); redis.call('HSET',KEYS[4],ts,p) end
redis.call('SET',KEYS[2],ARGV[3]); return 1`,
      4, this.store.lease, k.state, k.order, k.data, this.store.token, previous.id, JSON.stringify(next),
      JSON.stringify(packets.map(publicPacket)));
  }
  async importBatch(v, job, limit) {
    const a = this.keys(v), k = this.store.keys(v);
    const left = await this.r.eval(FENCE + JOB + `
local ids=redis.call('ZRANGE',KEYS[3],0,tonumber(ARGV[3])-1); local packets={}
for _,ts in ipairs(ids) do
 local raw=redis.call('HGET',KEYS[4],ts)
 if not raw then return redis.error_reply('ARCHIVE_STAGED_PAYLOAD_MISSING') end
 local p=cjson.decode(raw); packets[ts]=raw
 for _,key in ipairs({KEYS[6],KEYS[9]}) do
  local old=redis.call('HGET',key,ts)
  if old then local b=cjson.decode(old)
   if (b.latitude or b.lat) ~= p.latitude or (b.longitude or b.lng) ~= p.longitude then return redis.error_reply('ARCHIVE_CONFLICTING_PACKET') end
  end
 end
end
for _,ts in ipairs(ids) do
 local raw=packets[ts]
 redis.call('ZADD',KEYS[5],tonumber(ts),ts); redis.call('HSET',KEYS[6],ts,raw)
 redis.call('ZADD',KEYS[8],tonumber(ts),ts); redis.call('HSET',KEYS[9],ts,raw)
 local latest=tonumber(redis.call('HGET',KEYS[10],'ts') or '-1')
 if tonumber(ts)>latest then redis.call('HSET',KEYS[10],'ts',ts,'packet',raw) end
 redis.call('ZREM',KEYS[3],ts); redis.call('HDEL',KEYS[4],ts)
end
if #ids>0 then
 redis.call('SADD',KEYS[7],ARGV[4])
 for _,key in ipairs({KEYS[5],KEYS[6],KEYS[7]}) do redis.call('PERSIST',key) end
 for _,key in ipairs({KEYS[8],KEYS[9],KEYS[10]}) do redis.call('EXPIRE',key,ARGV[5]) end
end
return redis.call('ZCARD',KEYS[3])`, 10, this.store.lease, a.state, a.order, a.data,
      k.pendingOrder, k.pendingData, `${this.store.base}:pending:vehicles`, k.history, k.packets, k.live,
      this.store.token, job.id, limit, vehicle(v), Math.ceil(cfg.historyMs / 1000));
    return Number(left);
  }
  async remove(v) {
    const k = this.keys(v);
    await this.r.eval(FENCE + `redis.call('DEL',KEYS[2],KEYS[3],KEYS[4]); return 1`,
      4, this.store.lease, k.state, k.order, k.data, this.store.token);
  }
}
module.exports = { ArchiveStore };
