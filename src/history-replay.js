"use strict";
async function replayBatch(store, vehicleNo, floor) {
  const cursor = await store.begin(vehicleNo, floor);
  const batch = await store.batch(vehicleNo, cursor);
  const packets = batch.flatMap(item => item.packet ? [item.packet] : []);
  // Count missing records before adding the predecessor used as crossing evidence.
  // Previously that extra point made a complete batch report -1 missing payload.
  const missing = batch.length - packets.length;
  let preceding = 0;
  // The last point before lifting can be the only observed source-IN packet.
  // Include it as evidence when beginning/rewinding a pass; it cannot start a trip
  // because the repository still requires departure GPS time > lifting time.
  if (batch.length && cursor === floor && store.before) {
    const prior = await store.before(vehicleNo, cursor);
    if (prior) { packets.unshift(prior); preceding = 1; }
  }
  // Never filter using a stale cached trip state: an earlier packet in this
  // batch may start/end a trip and change eligibility of all later packets.
  await store.enqueueHistory(vehicleNo, packets);
  if (batch.length) await store.advance(vehicleNo, batch.at(-1).ts);
  return { processed: packets.length, missing, scanned: batch.length, preceding };
}
module.exports = { replayBatch };
