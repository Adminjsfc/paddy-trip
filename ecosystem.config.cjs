// Fixed, explicit shard IDs; restart one app independently. Do not scale these apps.
module.exports = {
  apps: [{ name: 'jsfc-gps-mqtt', script: './src/mqtt-subscriber.js', cwd: __dirname,
    exec_mode: 'fork', instances: 1, autorestart: true, exp_backoff_restart_delay: 1000,
    kill_timeout: 10000, max_memory_restart: '256M', time: true }, ...Array.from({ length: 8 }, (_, shard) => ({
    name: `jsfc-trip-${shard}`,
    script: './src/worker.js', cwd: __dirname,
    exec_mode: 'fork', instances: 1,
    env: { NODE_ENV: 'production', SHARD_ID: String(shard), SHARD_COUNT: '8' },
    autorestart: true, exp_backoff_restart_delay: 1000,
    kill_timeout: 20000, max_memory_restart: '512M', time: true,
    merge_logs: true
  }))]
};
