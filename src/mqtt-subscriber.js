'use strict';
require('./config');
const mqtt = require('mqtt');
const PacketIngest = require('./packet-ingest');
const { mapMqttPacket, publicPacket } = require('./mqtt-packet');
const { normalizePacket } = require('./domain');
const cfg = require('./config');
const { ActiveSubscriptions } = require('./active-subscriptions');
const { reconnectable } = require('./write-buffer');
const wait = ms => new Promise(resolve => setTimeout(resolve, ms));
const log = (kind, extra = {}) => console.log(JSON.stringify({ at: new Date().toISOString(), kind, ...extra }));
async function main() {
  // The former GPS_TIMESTAMP_OFFSET_MS setting is intentionally not used.
  // UTC date_time is authoritative, preventing double timezone adjustment.
  const url = process.env.MQTT_URL || 'ws://18.60.155.145:20012/';
  if (!/^wss?:\/\//.test(url)) throw new Error('MQTT_URL must use ws:// or wss://');
  const adapter = new PacketIngest(); await adapter.connect();
  let stopping = false;
  const metrics = { received: 0, accepted: 0, duplicate: 0, ignored_header: 0, ignored_inactive_vehicle: 0, rejected: 0, redis_retries: 0 };
  const database = require('./Database');
  const clientId = process.env.MQTT_CLIENT_ID || 'jsfc-trip-ingest-v1';
  const client = mqtt.connect(url, {
    clientId,
    protocolVersion: 4, clean: false, reconnectPeriod: 3000, connectTimeout: 15000,
    keepalive: 30, resubscribe: false, manualConnect: true,
    ...(process.env.MQTT_USERNAME ? { username: process.env.MQTT_USERNAME } : {}),
    ...(process.env.MQTT_PASSWORD ? { password: process.env.MQTT_PASSWORD } : {}),
    wsOptions: { maxPayload: 65536 }
  });
  const subscriptions = new ActiveSubscriptions({ client, redis: adapter.redis, database, clientId, prefix: cfg.prefix, log });
  await subscriptions.load();
  // MQTT.js handleMessage supplies backpressure and delays QoS 1 PUBACK until callback.
  // Do not also register an async 'message' handler; it would duplicate ingestion.
  client.handleMessage = (mqttPacket, callback) => {
    (async () => {
      metrics.received++;
      if (!subscriptions.accepts(mqttPacket.topic)) {
        metrics.ignored_inactive_vehicle++; callback(); return;
      }
      let packet, normalized;
      try {
        if (mqttPacket.payload.length > 65536) throw new Error('GPS payload too large');
        packet = mapMqttPacket(mqttPacket.topic, mqttPacket.payload);
        normalized = normalizePacket(packet, cfg); // Validate before retrying Redis failures.
      } catch (e) {
        if (e.code === 'MQTT_HEADER_IGNORED') {
          metrics.ignored_header++;
          callback(); return; // Acknowledge unrelated headers without Redis/MySQL work.
        }
        metrics.rejected++; log('mqtt_packet_rejected', { topic: mqttPacket.topic, message: e.message });
        callback(); return; // Invalid packets are deliberately acknowledged and skipped.
      }
      if (cfg.logMqttData) log('mqtt_packet_received', {
        topic: mqttPacket.topic, vehicle_no: packet.vehicle_no, packet_header: '$PVT',
        qos: mqttPacket.qos, retained: Boolean(mqttPacket.retain),
        data: publicPacket(normalized), gps_datetime_utc: new Date(normalized.ts).toISOString()
      });
      while (!stopping) {
        try {
          const result = await adapter.push(packet);
          metrics[result.accepted ? 'accepted' : 'duplicate']++;
          if (cfg.logMqttData) log('mqtt_packet_buffered', {
            topic: mqttPacket.topic, vehicle_no: packet.vehicle_no,
            shard: result.shard, duplicate: result.duplicate,
            gps_timestamp: normalized.ts, storage: 'redis'
          });
          callback(); return;
        } catch (e) {
          if (/Conflicting GPS|older than history retention|Future GPS/.test(e.message)) {
            metrics.rejected++; log('mqtt_packet_rejected', { topic: mqttPacket.topic, message: e.message });
            callback(); return;
          }
          metrics.redis_retries++;
          log('mqtt_storage_retry', { vehicle_no: packet.vehicle_no, message: e.message });
          await wait(1000); // Hold the current message; never accumulate an application queue.
        }
      }
      // No callback on forced shutdown: an unacknowledged QoS 1 packet can redeliver.
    })().catch(e => { log('mqtt_handler_error', { message: e.message }); void shutdown(1); });
  };
  async function refreshSubscriptions(reconnect = false) {
    try { await subscriptions.sync(reconnect); }
    catch (e) {
      log('mqtt_subscription_sync_failed', { message: e.message, code: e.code });
      if (reconnectable(e)) await database.reinitialize().catch(err => log('mqtt_database_reinitialize_failed', { message: err.message }));
    }
  }
  client.on('connect', () => {
    log('mqtt_connected', { mode: 'active_trip_vehicles', timestamp_source: 'date_time_utc' });
    void refreshSubscriptions(true);
  });
  client.on('error', e => log('mqtt_error', { message: e.message }));
  client.on('reconnect', () => log('mqtt_reconnecting'));
  client.on('offline', () => log('mqtt_offline'));
  client.on('close', () => log('mqtt_connection_closed'));
  const subscriptionTimer = setInterval(() => {
    if (client.connected && !stopping) void refreshSubscriptions();
  }, cfg.refreshMs);
  const timer = setInterval(() => log('mqtt_health', { connected: client.connected, ...metrics }), 30000);
  async function shutdown(code = 0) {
    if (stopping) return;
    stopping = true; clearInterval(timer); clearInterval(subscriptionTimer);
    await new Promise(resolve => client.end(true, {}, resolve));
    if (subscriptions.running) await subscriptions.running.catch(() => {});
    await database.close().catch(() => {});
    await adapter.close().catch(() => {});
    process.exitCode = code;
  }
  process.once('SIGINT', () => { void shutdown(); });
  process.once('SIGTERM', () => { void shutdown(); });
  client.connect();
}
main().catch(e => { log('mqtt_fatal', { message: e.message }); process.exitCode = 1; });
