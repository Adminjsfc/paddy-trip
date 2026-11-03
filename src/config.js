'use strict';
require('dotenv').config({ path: require('node:path').join(__dirname, '..', '.env') });
function integer(name, fallback, min = 1) {
  const n = Number(process.env[name] ?? fallback);
  if (!Number.isSafeInteger(n) || n < min) throw new Error(`Invalid ${name}`);
  return n;
}
const cfg = {
  logVehicleLocation: process.env.LOG_VEHICLE_LOCATION !== 'false',
  logMqttData: process.env.LOG_MQTT_DATA !== 'false',
  logTripDetails: process.env.LOG_TRIP_DETAILS !== 'false',
  shardCount: integer('SHARD_COUNT', 8), shardId: integer('SHARD_ID', 0, 0),
  refreshMs: integer('REFRESH_MS', 60000), pollMs: integer('POLL_MS', 1000),
  concurrency: integer('VEHICLE_CONCURRENCY', 4), batchSize: integer('PACKET_BATCH_SIZE', 300),
  historyMs: integer('HISTORY_DAYS', 7) * 86400000,
  futureTolerance: integer('FUTURE_TOLERANCE_MS', 120000, 0),
  radiusMultiplier: 1000, // Projected kilometres -> distance() metres.
  departureRequiresCrossing: false, // Match paddy: source OUT after dispatch starts the trip.
  offset: '+00:00',
  cacheTtl: integer('CACHE_TTL_SECONDS', 300), leaseMs: integer('LEASE_MS', 30000),
  redisUrl: process.env.REDIS_URL || 'redis://127.0.0.1:6379/0',
  prefix: process.env.REDIS_PREFIX || 'jsfc:trip:v1',
  writeBatchSize: integer('WRITE_BATCH_SIZE', 50),
  requireDurableRedis: process.env.REDIS_REQUIRE_DURABILITY !== 'false'
};
if (cfg.shardCount !== 8 || cfg.shardId >= 8) throw new Error('Exactly eight shards (0..7) required');
if (!Number.isFinite(cfg.radiusMultiplier) || cfg.radiusMultiplier <= 0) throw new Error('Invalid RADIUS_MULTIPLIER');
if (!/^[+-](?:0\d|1[0-4]):[0-5]\d$/.test(cfg.offset)) throw new Error('Invalid DB_TIME_OFFSET');
if (cfg.leaseMs < 10000) throw new Error('LEASE_MS must be >= 10000');
module.exports = cfg;
