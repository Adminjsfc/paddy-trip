'use strict';
const { vehicle } = require('./domain');
const { createHash } = require('node:crypto');
const ACTIVE_VEHICLES = `SELECT DISTINCT UPPER(TRIM(v.registration_no)) AS vehicle_no
 FROM shipments s JOIN vehicles v ON v.id = s.vehicle_id
 WHERE s.status IN ('DISPATCHED','IN_TRANSIT') AND s.trip_status IN (0,1)
 AND v.registration_no IS NOT NULL AND TRIM(v.registration_no) <> ''`;
class ActiveSubscriptions {
  constructor({ client, redis, database, clientId, prefix, log }) {
    Object.assign(this, { client, redis, database, log });
    this.key = `${prefix}:mqtt:subscriptions:${createHash('sha256').update(clientId).digest('hex')}`;
    this.known = new Set(); this.allowed = new Set(); this.running = null; this.restore = true; this.wildcardCleanup = true;
  }
  async load() {
    this.known = new Set(await this.redis.smembers(this.key));
    this.allowed = new Set(this.known);
  }
  accepts(topic) { return this.allowed.has(topic); }
  async request(method, topics) {
    if (!this.client.connected) throw new Error('MQTT disconnected during subscription sync');
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error(`MQTT ${method} acknowledgement timeout`)), 15000);
      const callback = (err, granted) => {
        clearTimeout(timer);
        if (err) return reject(err);
        if (method === 'subscribe' && (!granted || granted.length !== topics.length ||
            granted.some(g => g.qos === 128))) return reject(new Error('MQTT subscription denied'));
        resolve(granted);
      };
      if (method === 'subscribe') this.client.subscribe(topics, { qos: 1 }, callback);
      else this.client.unsubscribe(topics, callback);
    });
  }
  async sync(reconnect = false) {
    if (reconnect) { this.restore = true; this.wildcardCleanup = true; }
    if (this.running) { await this.running; if (reconnect) return this.sync(true); return; }
    this.running = this.perform(reconnect);
    try { await this.running; } finally { this.running = null; }
  }
  async perform(reconnect) {
    let desired;
    try {
      // Small authoritative primary projection avoids subscribing completed replica rows.
      const rows = await this.database.readExecute(ACTIVE_VEHICLES, [], { primary: true });
      desired = new Set();
      for (const row of rows) {
        try {
          const v = vehicle(row.vehicle_no);
          if (v !== row.vehicle_no) throw new Error('Vehicle number must be normalized in MySQL');
          desired.add(`gps/${v}/data`);
        } catch (e) { this.log('mqtt_vehicle_configuration_error', { vehicle_no: row.vehicle_no, message: e.message }); }
      }
    } catch (e) {
      this.log('mqtt_active_vehicle_refresh_failed', { message: e.message, preserved_topics: this.allowed.size });
      // Preserve last known vehicles during a DB outage; do not interpret failure as empty.
      if (!reconnect) throw e;
      desired = new Set(this.allowed);
    }
    this.allowed = desired;
    if (this.wildcardCleanup) {
      // Remove the wildcard left by older versions' persistent broker sessions.
      await this.request('unsubscribe', ['gps/+/data']);
      this.wildcardCleanup = false;
      this.log('mqtt_wildcard_removed', { topic: 'gps/+/data' });
    }
    const tracked = new Set([...this.known, ...await this.redis.smembers(this.key)]);
    const remove = [...tracked].filter(t => !desired.has(t));
    for (let i = 0; i < remove.length; i += 100) {
      const topics = remove.slice(i, i + 100);
      await this.request('unsubscribe', topics);
      await this.redis.srem(this.key, ...topics);
      topics.forEach(t => this.known.delete(t));
      this.log('mqtt_unsubscribed', { topics });
    }
    const add = [...desired].filter(t => this.restore || !this.known.has(t));
    for (let i = 0; i < add.length; i += 100) {
      const topics = add.slice(i, i + 100);
      // Record BEFORE SUBSCRIBE so an uncertain response can be reconciled after restart.
      await this.redis.sadd(this.key, ...topics);
      this.log('mqtt_subscribe_requested', { topics, qos: 1 });
      const granted = await this.request('subscribe', topics);
      topics.forEach(t => this.known.add(t));
      this.log('mqtt_subscribed', { granted });
    }
    this.restore = false;
    this.log('mqtt_active_vehicle_list', { count: desired.size,
      vehicles: [...desired].map(t => t.split('/')[1]) });
  }
}
module.exports = { ActiveSubscriptions, ACTIVE_VEHICLES };
