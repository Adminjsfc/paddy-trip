'use strict';

// IPC also requests graceful shutdown on Windows, where kill(SIGTERM) is abrupt.
let stopping = false;
function shutdown() {
  if (stopping) return;
  stopping = true;
  if (process.connected) process.disconnect();
  process.emit('SIGTERM');
}
process.on('message', message => {
  if (message === 'shutdown') shutdown();
});
process.on('disconnect', shutdown);
process.channel?.unref();
require(process.argv[2]);
