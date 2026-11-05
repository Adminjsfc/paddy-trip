'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { EventEmitter } = require('node:events');
const { startAll } = require('../scripts/start-all.cjs');

function launch() {
  const spawned = [], exits = [];
  const launcher = startAll({ env: { SHARD_ID: '2', SHARD_COUNT: '1' }, log: () => {},
    onExit: code => exits.push(code), restartMs: 5, maxRestartMs: 20, spawn: (file, args, options) => {
      const child = new EventEmitter();
      child.connected = true;
      child.messages = [];
      child.send = message => child.messages.push(message);
      spawned.push({ file, args, options, child });
      return child;
    } });
  return { launcher, spawned, exits };
}

test('start launches one subscriber and exactly eight unique shards despite inherited shard settings', () => {
  const { launcher, spawned } = launch();
  assert.equal(spawned.length, 9);
  assert.match(spawned[0].args[0], /mqtt-subscriber\.js$/);
  assert.deepEqual(spawned.slice(1).map(c => c.options.env.SHARD_ID), ['0','1','2','3','4','5','6','7']);
  assert.ok(spawned.every(c => c.options.env.SHARD_COUNT === '8'));
  launcher.stop();
  assert.ok(spawned.every(c => c.child.messages[0] === 'shutdown'));
  for (const c of spawned) c.child.emit('exit', 0);
});

test('an unexpected exit restarts only the failed shard while siblings keep running', async () => {
  const { launcher, spawned, exits } = launch();
  spawned[2].child.emit('exit', 1);
  assert.deepEqual(exits, []);
  assert.ok(spawned.every(c => c.child.messages.length === 0));
  await new Promise(resolve => setTimeout(resolve, 30));
  assert.equal(spawned.length, 10);
  assert.equal(spawned[9].options.env.SHARD_ID, spawned[2].options.env.SHARD_ID);
  launcher.stop();
  assert.deepEqual(exits, [0]);
  for (const c of spawned.filter((_, i) => i !== 2)) c.child.emit('exit', 0);
});

test('Ctrl+C shutdown cancels pending restarts', async () => {
  const { launcher, spawned } = launch();
  spawned[1].child.emit('exit', 1);
  launcher.stop();
  await new Promise(resolve => setTimeout(resolve, 30));
  assert.equal(spawned.length, 9);
  for (const c of spawned.filter((_, i) => i !== 1)) c.child.emit('exit', 0);
});
