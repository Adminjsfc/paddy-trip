'use strict';
async function flushVehicle(store, repository, vehicleNo, options = {}) {
  await store.assertOwner();
  const intent = await store.loadIntent(vehicleNo);
  const packets = intent ? intent.packets : await store.pendingBatch(vehicleNo);
  if (!packets.length) return null;
  const result = await repository.transitionBatch(packets, {
    intent, archiveCoverage: options.archiveCoverage, completed: ids => store.completedEvents(vehicleNo, ids),
    loadGeofences: ids => store.loadGeofences(vehicleNo, ids), save: value => store.saveIntent(vehicleNo, value)
  });
  await store.publish(vehicleNo, result.rows);
  // A previously evaluated history point may become eligible after this batch.
  // Rewind before finalization so crashes cannot lose the rescan request.
  if (result.intent.operations.length) {
    await store.rewind(vehicleNo, Math.min(...result.intent.operations.map(op => op.packet.ts)));
  }
  // Atomically store Redis completion markers, acknowledge the fixed packet IDs,
  // and remove only the matching journal. This runs AFTER confirmed MySQL COMMIT.
  await store.finishIntent(vehicleNo, result.intent, result.rows);
  return { ...result, flushed: packets.length };
}
function reconnectable(err) {
  return Boolean(err?.fatal) || /^(ECONN|ETIMEDOUT|EHOST|ENET|ENOTFOUND|EAI_|PROTOCOL_CONNECTION_LOST|PROTOCOL_ENQUEUE_AFTER_FATAL_ERROR|PROTOCOL_ENQUEUE_AFTER_QUIT|ER_SERVER_SHUTDOWN|ER_OPTION_PREVENTS_STATEMENT)/.test(err?.code || '') ||
    /pool is closed|closed state/i.test(err?.message || '');
}
module.exports = { flushVehicle, reconnectable };
