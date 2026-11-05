'use strict';
const { fork } = require('node:child_process');
const path = require('node:path');

function startAll({ spawn = fork, env = process.env, log = console.log,
  onExit = code => { process.exitCode = code; }, shutdownMs = 20000,
  restartMs = 1000, maxRestartMs = 30000 } = {}) {
  const root = path.resolve(__dirname, '..');
  const children = new Set();
  const restartTimers = new Set();
  let stopping = false, timer;
  function stop(code = 0) {
    if (stopping) return;
    stopping = true;
    for (const pending of restartTimers) clearTimeout(pending);
    restartTimers.clear();
    onExit(code);
    timer = setTimeout(() => {
      for (const child of children) child.kill();
    }, shutdownMs);
    timer.unref();
    for (const child of children) {
      if (child.connected) child.send('shutdown', error => {
        if (error) child.kill();
      });
      else child.kill();
    }
  }
  const services = [{ name: 'mqtt-subscriber', script: 'mqtt-subscriber.js' },
    ...Array.from({ length: 8 }, (_, shard) => ({
      name: `trip-${shard}`, script: 'worker.js', shard
    }))];
  function launch(service) {
    if (stopping) return;
    const started = Date.now();
    const child = spawn(path.join(__dirname, 'service-runner.cjs'),
      [path.join(root, 'src', service.script)], {
        cwd: root, env: { ...env, SHARD_COUNT: '8', SHARD_ID: String(service.shard ?? 0) },
        stdio: ['inherit', 'inherit', 'inherit', 'ipc'], windowsHide: true
      });
    children.add(child);
    let finished = false;
    function exited(code, signal) {
      if (finished) return;
      finished = true;
      children.delete(child);
      if (!stopping) {
        if (Date.now() - started >= 60000) service.failures = 0;
        const delay = Math.min(maxRestartMs, restartMs * 2 ** Math.min(service.failures || 0, 10));
        service.failures = (service.failures || 0) + 1;
        log(`[launcher] ${service.name} exited (${signal || code}); restarting in ${delay}ms`);
        const pending = setTimeout(() => {
          restartTimers.delete(pending);
          launch(service);
        }, delay);
        restartTimers.add(pending);
      }
      if (!children.size) clearTimeout(timer);
    }
    child.on('error', error => {
      log(`[launcher] ${service.name}: ${error.message}`);
      exited(1);
    });
    child.on('exit', exited);
  }
  for (const service of services) launch(service);
  log('[launcher] Started MQTT subscriber and all 8 trip shards (0-7)');
  return { stop };
}

if (require.main === module) {
  const launcher = startAll();
  process.once('SIGINT', () => launcher.stop());
  process.once('SIGTERM', () => launcher.stop());
}
module.exports = { startAll };
