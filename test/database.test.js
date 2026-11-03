'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { EventEmitter } = require('node:events');
const mysql = require('mysql2');
const originalCreate = mysql.createPool;
let nextRole = 0;
mysql.createPool = config => {
  const role = nextRole++ % 2 === 0 ? 'write' : 'read';
  const pool = new EventEmitter(); pool.config = config;
  pool.calls = []; pool.releases = 0; pool.destroyed = 0; pool.delay = 0;
  const c = new EventEmitter(); c.threadId = nextRole;
  c.query = c.execute = (sql, args, cb) => {
    pool.calls.push(sql);
    if (sql === 'FAIL') return cb(Object.assign(new Error('test failure'), { code: 'ER_TEST' }));
    cb(null, [{ role }]);
  };
  c.beginTransaction = cb => { pool.calls.push('BEGIN'); cb(); };
  c.commit = cb => { pool.calls.push('COMMIT'); cb(); };
  c.rollback = cb => { pool.calls.push('ROLLBACK'); cb(); };
  c.release = () => { pool.releases++; pool.emit('release', c); };
  c.destroy = () => { pool.destroyed++; };
  pool.getConnection = cb => { setTimeout(() => { pool.emit('acquire', c); cb(null, c); }, pool.delay); };
  pool.end = cb => cb();
  return pool;
};
process.env.DB_LOCAL = 'true'; process.env.DB_LOG_POOL_EVENTS = 'false';
const singleton = require('../src/Database');
const Database = singleton.Database;
const env = { DB_LOCAL: 'true', DB_LOG_POOL_EVENTS: 'false' };
test.after(async () => { await singleton.close(); mysql.createPool = originalCreate; });
test('read/write calls use separate pools; primary option is honored', async () => {
  const db = new Database(env);
  assert.equal((await db.readExecute('SELECT 1'))[0].role, 'read');
  assert.equal((await db.writeExecute('SELECT 1'))[0].role, 'write');
  assert.equal((await db.readExecute('SELECT 1', [], { primary: true }))[0].role, 'write');
  assert.equal(db.metrics.read.queries, 1); assert.equal(db.metrics.write.queries, 2);
  assert.equal(db.readPool.releases, 1); assert.equal(db.writePool.releases, 2);
  await db.close();
});
test('transaction-scoped reads and writes share primary; no secondary checkout', async () => {
  const db = new Database(env);
  await db.withTransaction(async tx => {
    assert.equal((await tx.readExecute('SELECT locked'))[0].role, 'write');
    assert.equal((await tx.writeExecute('UPDATE sample'))[0].role, 'write');
  }, { isolationLevel: 'READ COMMITTED' });
  assert.equal(db.readPool.releases, 0); assert.equal(db.writePool.releases, 1);
  assert.equal(db.transactionMetrics.committed, 1);
  const calls = db.writePool.calls;
  assert.ok(calls.indexOf('SET TRANSACTION ISOLATION LEVEL READ COMMITTED') < calls.indexOf('BEGIN'));
  assert.equal(calls.at(-1), 'COMMIT'); await db.close();
});
test('failed transaction rolls back and releases once', async () => {
  const db = new Database(env);
  await assert.rejects(db.withTransaction(tx => tx.writeExecute('FAIL')), /test failure/);
  assert.equal(db.writePool.calls.at(-1), 'ROLLBACK');
  assert.equal(db.writePool.releases, 1); assert.equal(db.transactionMetrics.active, 0);
  assert.equal(db.transactionMetrics.rolledBack, 1); await db.close();
});
test('timed-out acquisition releases late connection without executing user SQL', async () => {
  const db = new Database({ ...env, DB_ACQUIRE_TIMEOUT: '5' });
  db.readPool.delay = 25;
  await assert.rejects(db.readExecute('SELECT should_not_run'), { code: 'DB_ACQUIRE_TIMEOUT' });
  await new Promise(resolve => setTimeout(resolve, 40));
  assert.equal(db.readPool.calls.length, 0); assert.equal(db.readPool.releases, 1);
  assert.equal(db.metrics.read.inFlightQueries, 0); await db.close();
});
test('manual transaction stays on supplied primary connection', async () => {
  const db = new Database(env);
  const c = await db.beginTransaction();
  assert.equal((await db.transactionQuery('SELECT manual', [], c))[0].role, 'write');
  await db.commit(c); await db.commit(c);
  assert.equal(db.writePool.releases, 1); assert.equal(db.readPool.releases, 0);
  await db.close();
});


test('pool reinitialization recreates both endpoints after operations settle', async () => {
  const db = new Database(env), oldRead = db.readPool, oldWrite = db.writePool;
  await db.readExecute('SELECT 1');
  assert.equal(await db.reinitialize(), true);
  assert.notEqual(db.readPool, oldRead); assert.notEqual(db.writePool, oldWrite);
  assert.equal(db.reinitializations, 1);
  assert.equal((await db.writeExecute('SELECT after_reconnect'))[0].role, 'write');
  await db.close();
});
test('default DB singleton honors local environment settings', () => {
  // DB_LOCAL=true is set before loading the module; both pools use local MySQL.
  assert.equal(singleton.isLocal, true);
  assert.equal(singleton.writeConfig.port, Number(process.env.DB_PORT || 3306));
  assert.equal(singleton.readConfig.port, Number(process.env.DB_PORT || 3306));
});
