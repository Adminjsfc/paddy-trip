'use strict';
// Call this adapter from your existing GPS receiver. Create ONE instance per receiver process.
const { connection, ingest, verifyDurability } = require('./redis-store');
class PacketIngest {
  constructor() { this.redis = connection(); }
  async connect() {
    try { await this.redis.connect(); await verifyDurability(this.redis); }
    catch (e) { this.redis.disconnect(); throw e; }
  }
  async push(packet) { return ingest(this.redis, packet); }
  // Streaming/page-by-page backfill; do not load entire vehicle history into RAM.
  async pushHistory(packets) {
    let accepted = 0, duplicates = 0;
    for await (const packet of packets) {
      const result = await this.push(packet);
      if (result.accepted) accepted++; else duplicates++;
    }
    return { accepted, duplicates };
  }
  async getLive(vehicleNo) {
    const { Store } = require('./redis-store');
    const { shardFor } = require('./domain');
    const store = new Store(this.redis, shardFor(vehicleNo));
    const value = await this.redis.hget(store.keys(vehicleNo).live, 'packet');
    if (!value) return null;
    const p = JSON.parse(value);
    return 'latitude' in p ? p : require('./mqtt-packet').publicPacket(p);
  }
  async close() { await this.redis.quit(); }
}
module.exports = PacketIngest;
