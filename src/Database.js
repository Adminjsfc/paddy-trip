"use strict";

/** Database.js — paddy schema connection settings.
 * Queue/retry orchestration lives in write-buffer.js.
 * Scoped/manual transactions, read/write helpers and metrics are preserved.
 */
require('dotenv').config({ path: require('node:path').join(__dirname, '..', '.env') });
const DATABASE_CONFIG = Object.freeze({
  DB_LOCAL: process.env.DB_LOCAL || 'true',
  DB_WRITE_HOST: process.env.DB_WRITE_HOST || process.env.DB_HOST || '127.0.0.1',
  DB_WRITE_PORT: process.env.DB_WRITE_PORT || process.env.DB_PORT || 3306,
  DB_READ_HOST: process.env.DB_READ_HOST || process.env.DB_HOST || '127.0.0.1',
  DB_READ_PORT: process.env.DB_READ_PORT || process.env.DB_PORT || 3306,
  DB_HOST: process.env.DB_HOST || '127.0.0.1',
  DB_PORT: process.env.DB_PORT || 3306,
  DB_NAME: process.env.DB_NAME || 'sfc_cmr',
  DB_USER: process.env.DB_USER || 'sfc_cmr_app',
  DB_PASSWORD: process.env.DB_PASSWORD || '',
  DB_WRITE_POOL_LIMIT: process.env.DB_WRITE_POOL_LIMIT || process.env.DB_POOL_LIMIT || 4,
  DB_READ_POOL_LIMIT: process.env.DB_READ_POOL_LIMIT || process.env.DB_POOL_LIMIT || 2,
  DB_QUEUE_LIMIT: process.env.DB_QUEUE_LIMIT || 100,
  DB_CONNECT_TIMEOUT: process.env.DB_CONNECT_TIMEOUT || 10000,
  DB_ACQUIRE_TIMEOUT: process.env.DB_ACQUIRE_TIMEOUT || 30000,
  DB_TIMEZONE: process.env.DB_TIMEZONE || '+00:00',
  DB_LOG_POOL_EVENTS: process.env.DB_LOG_POOL_EVENTS || 'false',
  DB_SSL_CA: process.env.DB_SSL_CA_FILE || ''
});

const mysql = require("mysql2");
const { AsyncLocalStorage } = require("node:async_hooks");

// =========================================================
// COMMON HELPERS
// =========================================================

function integer(
  value,
  fallback,
  name,
  min = 1,
  max = Number.MAX_SAFE_INTEGER
) {
  const n = Number(value ?? fallback);

  if (!Number.isSafeInteger(n) || n < min || n > max) {
    throw new Error(
      `${name} must be an integer from ${min} to ${max}`
    );
  }

  return n;
}

const invoke = (target, method, ...args) =>
  new Promise((resolve, reject) => {
    target[method](...args, (err, result) => {
      if (err) {
        return reject(err);
      }

      resolve(result);
    });
  });

// =========================================================
// DATABASE CLASS
// =========================================================

class Database {
  constructor(env = DATABASE_CONFIG) {
    this.acquireTimeoutMs = integer(env.DB_ACQUIRE_TIMEOUT, 30000, 'DB_ACQUIRE_TIMEOUT');
    this.sessionTimezone = env.DB_TIMEZONE || env.DB_TIME_OFFSET || 'Z';
    this.logPoolEvents = env.DB_LOG_POOL_EVENTS !== 'false';
    this.preparedConnections = new WeakSet();

    this.recoveryPromise = null;
    this.reinitializations = 0;
    this.transactions = new AsyncLocalStorage();
    this.manualTransactions = new WeakMap();

    this.closed = false;
    this.lifecycle = "open";
    this.createdAt = new Date().toISOString();

    this.metrics = {};
    this.poolState = {};

    this.transactionMetrics = {
      active: 0,
      committed: 0,
      rolledBack: 0,
      failed: 0,
    };

    // Local mode bypasses the read/write Router split and points both pools at one MySQL instance.
    const isLocal = env.DB_LOCAL === "true";
    this.isLocal = isLocal;
    this.useShardColumn = env.DB_USE_SHARD_COLUMN === true || env.DB_USE_SHARD_COLUMN === 'true';

    if (!isLocal && !env.DB_PASSWORD) throw new Error('Set DB_PASSWORD for the paddy database connection');
    const common = {
      user: env.DB_USER || (isLocal ? "root" : "sfc_cmr_app"),
      password:
        env.DB_PASSWORD ?? "",
      database: env.DB_NAME || "sfc_cmr",

      waitForConnections: true,

      queueLimit: integer(
        env.DB_QUEUE_LIMIT,
        100,
        "DB_QUEUE_LIMIT",
        0
      ),

      connectTimeout: integer(
        env.DB_CONNECT_TIMEOUT,
        10000,
        "DB_CONNECT_TIMEOUT"
      ),

      enableKeepAlive: true,
      keepAliveInitialDelay: 10000,

      multipleStatements: false,

      timezone: this.sessionTimezone,
      dateStrings: true,
      supportBigNumbers: true,
      bigNumberStrings: true,
      ...(env.DB_SSL_CA ? { ssl: { ca: require('node:fs').readFileSync(env.DB_SSL_CA), rejectUnauthorized: true } } : {}),
    };

    this.writeConfig = {
      ...common,

      host: isLocal
        ? env.DB_HOST || "localhost"
        : env.DB_WRITE_HOST || "127.0.0.1",

      port: integer(
        isLocal ? env.DB_PORT : env.DB_WRITE_PORT,
        isLocal ? 3306 : 6446,
        isLocal ? "DB_PORT" : "DB_WRITE_PORT",
        1,
        65535
      ),

      connectionLimit: integer(
        env.DB_WRITE_POOL_LIMIT,
        4,
        "DB_WRITE_POOL_LIMIT"
      ),
    };

    this.readConfig = {
      ...common,

      host: isLocal
        ? env.DB_HOST || "localhost"
        : env.DB_READ_HOST || "127.0.0.1",

      port: integer(
        isLocal ? env.DB_PORT : env.DB_READ_PORT,
        isLocal ? 3306 : 6447,
        isLocal ? "DB_PORT" : "DB_READ_PORT",
        1,
        65535
      ),

      connectionLimit: integer(
        env.DB_READ_POOL_LIMIT,
        2,
        "DB_READ_POOL_LIMIT"
      ),
    };

    this.writePool = this._createPool(
      "write",
      this.writeConfig
    );

    this.readPool = this._createPool(
      "read",
      this.readConfig
    );

    // Compatibility with existing direct db.pool users.
    // Direct pool usage bypasses wrapper query metrics
    // and scoped transaction routing.
    this.pool = this.writePool;
    this.config = this.writeConfig;
  }

  // =========================================================
  // CREATE POOLS AND REGISTER EVENTS
  // =========================================================

  _createPool(role, config) {
    const m = (this.metrics[role] = {
      poolInitialized: 1,

      connectionsCreated: 0,
      connectionsEnded: 0,

      acquired: 0,
      released: 0,
      enqueued: 0,

      connectionErrors: 0,
      poolErrors: 0,

      queries: 0,
      successfulQueries: 0,
      errors: 0,

      inFlightQueries: 0,
      peakInFlightQueries: 0,

      totalQueryTimeMs: 0,
      lastQueryTimeMs: 0,
      maxQueryTimeMs: 0,
    });

    this.poolState[role] = {
      initializedAt: new Date().toISOString(),

      lastConnectedAt: null,
      lastDisconnectedAt: null,
      lastAcquiredAt: null,
      lastReleasedAt: null,
      lastEnqueuedAt: null,

      lastSuccessAt: null,
      lastError: null,
      lastConnectionError: null,
      lastHealthCheck: null,
      lastActivityAt: null,
    };

    const pool = mysql.createPool(config);

    const event = (
      name,
      connection = null,
      extra = {}
    ) => {
      const at = new Date().toISOString();

      this.poolState[role].lastActivityAt = at;

      this._logPoolEvent(role, name, {
        connectionId: connection?.threadId ?? null,
        ...extra,
      });

      return at;
    };

    // A new physical connection was established.
    pool.on("connection", (connection) => {
      m.connectionsCreated++;

      this.poolState[role].lastConnectedAt =
        event("CONNECTED", connection);

      connection.on("error", (err) => {
        m.connectionErrors++;

        this._recordError(role, err, true);

        event("CONNECTION_ERROR", connection, {
          code: err.code || "UNKNOWN",
        });
      });

      // Count observed connection end events.
      connection.once("end", () => {
        m.connectionsEnded++;

        this.poolState[role].lastDisconnectedAt =
          event("DISCONNECTED", connection);
      });
    });

    pool.on("acquire", (connection) => {
      m.acquired++;

      this.poolState[role].lastAcquiredAt =
        event("ACQUIRED", connection);
    });

    pool.on("release", (connection) => {
      m.released++;

      this.poolState[role].lastReleasedAt =
        event("RELEASED", connection);
    });

    pool.on("enqueue", () => {
      m.enqueued++;

      this.poolState[role].lastEnqueuedAt =
        event("QUEUED");
    });

    pool.on("error", (err) => {
      m.poolErrors++;

      this._recordError(role, err, true);

      event("POOL_ERROR", null, {
        code: err.code || "UNKNOWN",
      });
    });

    // Pool creation is lazy; this does not prove connectivity.
    event("POOL_INITIALIZED", null, {
      host: config.host,
      port: config.port,
      connectionLimit: config.connectionLimit,
    });

    return pool;
  }

  // =========================================================
  // LOGGING AND ERROR STATE
  // =========================================================

  _logPoolEvent(role, event, details = {}) {
    if (!this.logPoolEvents && !/ERROR|FAILED/.test(event)) {
      return;
    }

    console.log(
      JSON.stringify({
        time: new Date().toISOString(),
        component: "mysql-router-pool",
        pid: process.pid,
        pool: role,
        event,
        ...details,
      })
    );
  }

  _recordError(role, err, connectionError = false) {
    // No credentials, SQL text, or query parameters.
    const failure = {
      code: err.code || "UNKNOWN",
      sqlState: err.sqlState || null,
      fatal: Boolean(err.fatal),
      at: new Date().toISOString(),
    };

    this.poolState[role].lastError = failure;

    const networkError =
      /^(ECONN|ETIMEDOUT|EHOST|ENET|ENOTFOUND|EAI_|PROTOCOL_CONNECTION_LOST|ER_ACCESS_DENIED)/.test(
        err.code || ""
      );

    if (connectionError || err.fatal || networkError) {
      this.poolState[role].lastConnectionError =
        failure;
    }
  }

  // =========================================================
  // CONNECTION AND CONTEXT HELPERS
  // =========================================================

  async _acquireConnection(role) {
    if (this.recoveryPromise) await this.recoveryPromise;
    this._assertOpen();
    const pool = role === 'read' ? this.readPool : this.writePool;
    try {
      const connection = await new Promise((resolve, reject) => {
        let expired = false;
        const timer = setTimeout(() => {
          expired = true;
          reject(Object.assign(new Error('Database pool acquisition timed out'), { code: 'DB_ACQUIRE_TIMEOUT' }));
        }, this.acquireTimeoutMs);
        pool.getConnection((err, conn) => {
          clearTimeout(timer);
          if (expired) { if (conn) conn.release(); return; }
          if (err) reject(err); else resolve(conn);
        });
      });
      try {
        if (!this.preparedConnections.has(connection)) {
          await invoke(connection, 'query', 'SET SESSION time_zone = ?', [this.sessionTimezone]);
          this.preparedConnections.add(connection);
        }
      } catch (err) { connection.destroy(); throw err; }
      return connection;
    } catch (err) { this._recordError(role, err, true); throw err; }
  }

  _acquireWriteConnection() { return this._acquireConnection('write'); }

  // Call only after the worker drains its current batch of operations.
  // Failed physical connections are also replaced by mysql2 on the next checkout.
  async reinitialize() {
    if (this.recoveryPromise) return this.recoveryPromise;
    if (this.closed) throw new Error('Database intentionally closed; create a new instance to restart');
    if (this.transactionMetrics.active || this.metrics.read.inFlightQueries || this.metrics.write.inFlightQueries) {
      return false;
    }
    this.recoveryPromise = (async () => {
      this.lifecycle = 'reinitializing';
      this._logPoolEvent('all', 'REINITIALIZING');
      await Promise.allSettled([invoke(this.writePool, 'end'), invoke(this.readPool, 'end')]);
      this.preparedConnections = new WeakSet();
      this.writePool = this._createPool('write', this.writeConfig);
      this.readPool = this._createPool('read', this.readConfig);
      this.pool = this.writePool;
      this.reinitializations++;
      this.lifecycle = 'open';
      this._logPoolEvent('all', 'REINITIALIZED', { generation: this.reinitializations });
      return true;
    })();
    try { return await this.recoveryPromise; }
    finally { this.recoveryPromise = null; }
  }

  _assertOpen() {
    if (this.closed) {
      throw new Error("Database pools are closed");
    }
  }

  _context() {
    const store = this.transactions.getStore();

    if (store && store.state !== "active") {
      throw new Error(
        "Transaction has finished; await all work inside withTransaction"
      );
    }

    return store;
  }

  // =========================================================
  // CENTRAL QUERY EXECUTION AND METRICS
  // =========================================================

  async _query(
    sql,
    values,
    role,
    explicitConnection = null,
    method = "query"
  ) {
    this._assertOpen();

    const store = this._context();

    if (
      explicitConnection &&
      store &&
      explicitConnection !== store.connection
    ) {
      throw new Error(
        "Cannot use a different connection inside withTransaction"
      );
    }

    if (explicitConnection) {
      const state =
        this.manualTransactions.get(explicitConnection);

      if (state && state.state !== "active") {
        throw new Error(
          "Transaction connection is closed"
        );
      }
    }

    let target = explicitConnection || store?.connection;
    let ownedConnection = null;

    const actualRole =
      explicitConnection || store ? "write" : role;

    const m = this.metrics[actualRole];

    m.queries++;
    m.inFlightQueries++;

    m.peakInFlightQueries = Math.max(
      m.peakInFlightQueries,
      m.inFlightQueries
    );

    // Includes queue wait and database execution time.
    const started = process.hrtime.bigint();

    try {
      if (!target) {
        ownedConnection = await this._acquireConnection(actualRole);
        target = ownedConnection;
      }
      const result = await invoke(
        target,
        method,
        sql,
        values
      );

      m.successfulQueries++;

      this.poolState[actualRole].lastSuccessAt =
        new Date().toISOString();

      this.poolState[actualRole].lastConnectionError =
        null;

      return result;
    } catch (err) {
      m.errors++;

      this._recordError(actualRole, err);

      this._logPoolEvent(actualRole, "QUERY_ERROR", {
        code: err.code || "UNKNOWN",
      });

      throw err;
    } finally {
      if (ownedConnection) ownedConnection.release();
      const durationMs =
        Number(process.hrtime.bigint() - started) /
        1e6;

      m.lastQueryTimeMs = durationMs;
      m.totalQueryTimeMs += durationMs;

      m.maxQueryTimeMs = Math.max(
        m.maxQueryTimeMs,
        durationMs
      );

      m.inFlightQueries--;

      this.poolState[actualRole].lastActivityAt =
        new Date().toISOString();
    }
  }

  // =========================================================
  // RAW READ / WRITE QUERIES
  // =========================================================

  // Raw SQL defaults to the primary, including SELECT.
  query(sql, values = []) {
    return this._query(sql, values, "write");
  }

  writeQuery(sql, values = []) {
    return this.query(sql, values);
  }

  readQuery(sql, values = [], options = {}) {
    return this._query(
      sql,
      values,
      options.primary ? "write" : "read"
    );
  }

  // =========================================================
  // PREPARED STATEMENTS
  // =========================================================

  execute(sql, values = []) {
    return this._query(
      sql,
      values,
      "write",
      null,
      "execute"
    );
  }

  writeExecute(sql, values = []) {
    return this.execute(sql, values);
  }

  readExecute(sql, values = [], options = {}) {
    return this._query(
      sql,
      values,
      options.primary ? "write" : "read",
      null,
      "execute"
    );
  }

  // =========================================================
  // SELECT HELPERS
  // =========================================================

  async select(
    tableName,
    columns = ["*"],
    where = null,
    excludeId = null,
    limit = null,
    orderBy = null,
    options = {}
  ) {
    // columns/orderBy are SQL fragments.
    // Use trusted application constants, not raw HTTP input.

    let sql =
      `SELECT ${columns.join(",")} ` +
      `FROM ${mysql.escapeId(tableName)}`;

    const conditions = [];
    const values = [];

    for (const [column, value] of Object.entries(
      where || {}
    )) {
      if (value === null) {
        conditions.push(
          `${mysql.escapeId(column)} IS NULL`
        );
      } else {
        conditions.push(
          `${mysql.escapeId(column)} = ?`
        );

        values.push(value);
      }
    }

    if (
      excludeId !== null &&
      excludeId !== undefined
    ) {
      conditions.push("id != ?");
      values.push(excludeId);
    }

    if (conditions.length) {
      sql += ` WHERE ${conditions.join(" AND ")}`;
    }

    if (orderBy) {
      sql += ` ORDER BY ${orderBy}`;
    }

    if (limit !== null && limit !== undefined) {
      sql += ` LIMIT ${integer(
        limit,
        0,
        "limit",
        0
      )}`;
    }

    return this.readQuery(sql, values, options);
  }

  async selectOne(
    tableName,
    columns = ["*"],
    where = null,
    options = {}
  ) {
    const rows = await this.select(
      tableName,
      columns,
      where,
      null,
      1,
      null,
      options
    );

    return rows[0] || null;
  }

  joinSelect(sql, values = [], options = {}) {
    return this.readQuery(sql, values, options);
  }

  async count(
    tableName,
    where = null,
    options = {}
  ) {
    const row = await this.selectOne(
      tableName,
      ["COUNT(*) AS total"],
      where,
      options
    );

    // Preserve the driver's number/string representation.
    return row.total;
  }

  async exists(
    tableName,
    where = null,
    options = {}
  ) {
    return Boolean(
      await this.selectOne(
        tableName,
        ["1 AS found"],
        where,
        options
      )
    );
  }

  // =========================================================
  // INSERT / BULK INSERT / UPDATE / DELETE
  // =========================================================

  insert(tableName, data) {
    // Preserves your original INSERT IGNORE behavior.
    return this.query(
      `INSERT IGNORE INTO ${mysql.escapeId(
        tableName
      )} SET ?`,
      data
    );
  }

  async insertMany(tableName, dataArray) {
    if (
      !Array.isArray(dataArray) ||
      !dataArray.length
    ) {
      throw new Error(
        "Data array cannot be empty"
      );
    }

    // Full-row arrays in database column order.
    // Same format as the original insertMany helper.
    return this.query(
      `INSERT INTO ${mysql.escapeId(
        tableName
      )} VALUES ?`,
      [dataArray]
    );
  }

  async update(tableName, data, where) {
    if (!data || !Object.keys(data).length) {
      throw new Error(
        "Update data cannot be empty"
      );
    }

    if (!where || !Object.keys(where).length) {
      throw new Error(
        "Update WHERE condition is required"
      );
    }

    const setColumns = Object.keys(data)
      .map((column) => {
        return `${mysql.escapeId(column)} = ?`;
      })
      .join(", ");

    const whereColumns = Object.keys(where)
      .map((column) => {
        return `${mysql.escapeId(column)} <=> ?`;
      })
      .join(" AND ");

    const sql =
      `UPDATE ${mysql.escapeId(tableName)} ` +
      `SET ${setColumns} ` +
      `WHERE ${whereColumns}`;

    return this.query(sql, [
      ...Object.values(data),
      ...Object.values(where),
    ]);
  }

  async delete(tableName, where) {
    if (!where || !Object.keys(where).length) {
      throw new Error(
        "Delete WHERE condition is required"
      );
    }

    const whereColumns = Object.keys(where)
      .map((column) => {
        return `${mysql.escapeId(column)} <=> ?`;
      })
      .join(" AND ");

    return this.query(
      `DELETE FROM ${mysql.escapeId(tableName)} ` +
        `WHERE ${whereColumns}`,
      Object.values(where)
    );
  }

  // =========================================================
  // SCOPED TRANSACTION — RECOMMENDED
  // =========================================================

  async withTransaction(work, options = {}) {
    this._assertOpen();

    if (this._context()) {
      throw new Error(
        "Nested withTransaction is not supported"
      );
    }

    if (typeof work !== "function") {
      throw new TypeError(
        "Transaction callback is required"
      );
    }

    const isolation = options.isolationLevel || null;
    if (isolation && !['READ COMMITTED', 'REPEATABLE READ', 'SERIALIZABLE', 'READ UNCOMMITTED'].includes(isolation)) {
      throw new Error('Invalid transaction isolation level');
    }
    const connection = await this._acquireWriteConnection();

    const store = {
      connection,
      state: "starting",
    };

    let reusable = true;
    let began = false;
    let committing = false;

    try {
      if (isolation) await invoke(connection, 'query', `SET TRANSACTION ISOLATION LEVEL ${isolation}`, []);
      await invoke(
        connection,
        "beginTransaction"
      );

      began = true;
      this.transactionMetrics.active++;
      store.state = "active";

      const result = await this.transactions.run(
        store,
        () => work(this)
      );

      store.state = "committing";
      committing = true;

      await invoke(connection, "commit");

      this.transactionMetrics.committed++;

      return result;
    } catch (err) {
      this.transactionMetrics.failed++;
      this._recordError("write", err);

      // A failed COMMIT may have an unknown outcome.
      // Do not automatically replay the transaction.
      if (committing || !began) {
        reusable = false;
      } else {
        store.state = "rollingBack";

        try {
          await invoke(connection, "rollback");
          this.transactionMetrics.rolledBack++;
        } catch (rollbackError) {
          this._recordError(
            "write",
            rollbackError
          );

          reusable = false;
        }
      }

      throw err;
    } finally {
      store.state = "closed";

      if (began) {
        this.transactionMetrics.active--;
      }

      if (reusable) {
        connection.release();
      } else {
        connection.destroy();
      }
    }
  }

  // =========================================================
  // MANUAL TRANSACTIONS
  //
  // Always pass the returned connection to transactionQuery,
  // commit and rollback.
  //
  // Custom connections must be write connections and remain
  // caller-owned after a successful commit/rollback.
  // =========================================================

  async beginTransaction(customConn = null) {
    this._assertOpen();

    if (this._context()) {
      throw new Error(
        "Do not call beginTransaction inside withTransaction"
      );
    }

    const connection =
      customConn ||
      (await this._acquireWriteConnection());

    const previous =
      this.manualTransactions.get(connection);

    if (
      previous?.state === "active" ||
      previous?.state === "finishing"
    ) {
      throw new Error(
        "Connection already has a transaction"
      );
    }

    try {
      await invoke(
        connection,
        "beginTransaction"
      );
    } catch (err) {
      this.transactionMetrics.failed++;
      this._recordError("write", err);

      if (!customConn) {
        connection.destroy();
      }

      throw err;
    }

    this.manualTransactions.set(connection, {
      state: "active",
      owned: !customConn,
    });

    this.transactionMetrics.active++;

    return connection;
  }

  async _finishManual(connection, method) {
    if (this.transactions.getStore()) {
      throw new Error(
        "withTransaction manages commit and rollback"
      );
    }

    if (!connection) {
      throw new Error(
        `${method} requires the connection from beginTransaction`
      );
    }

    const state =
      this.manualTransactions.get(connection);

    if (!state) {
      throw new Error(
        "Connection was not started by beginTransaction"
      );
    }

    // Avoid double release, including after failed COMMIT.
    if (state.state === "closed") {
      return true;
    }

    if (state.state !== "active") {
      throw new Error(
        "Transaction is already finishing"
      );
    }

    state.state = "finishing";

    let reusable = true;

    try {
      await invoke(connection, method);

      const counter =
        method === "commit"
          ? "committed"
          : "rolledBack";

      this.transactionMetrics[counter]++;

      return true;
    } catch (err) {
      reusable = false;

      this.transactionMetrics.failed++;
      this._recordError("write", err);

      throw err;
    } finally {
      state.state = "closed";
      this.transactionMetrics.active--;

      if (!reusable) {
        connection.destroy();
      } else if (state.owned) {
        connection.release();
      }
    }
  }

  commit(connection = null) {
    return this._finishManual(
      connection,
      "commit"
    );
  }

  rollback(connection = null) {
    return this._finishManual(
      connection,
      "rollback"
    );
  }

  async transactionQuery(
    sql,
    values = [],
    connection = null
  ) {
    if (!connection && !this._context()) {
      throw new Error(
        "transactionQuery requires an explicit connection or withTransaction"
      );
    }

    return this._query(
      sql,
      values,
      "write",
      connection
    );
  }

  // =========================================================
  // STARTUP CONNECTION CHECK
  // =========================================================

  async connect() {
    const health = await this.healthCheck();

    if (!health.ok) {
      const err = new Error(
        "Router health check failed; inspect error.health for each endpoint"
      );

      err.code = "DB_HEALTH_CHECK_FAILED";
      err.health = health;

      throw err;
    }

    return {
      write: health.write.server,
      read: health.read.server,
    };
  }

  // =========================================================
  // LIVE HEALTH CHECK
  //
  // Checks one connection per endpoint.
  // Does not verify every member or replication freshness.
  // =========================================================

  async healthCheck() {
    this._assertOpen();

    if (this.transactions.getStore()) {
      throw new Error(
        "Run health checks outside a transaction"
      );
    }

    if (this.healthPromise) {
      return this.healthPromise;
    }

    this.healthPromise = this._probeHealth();

    try {
      return await this.healthPromise;
    } finally {
      this.healthPromise = null;
    }
  }

  async _probeHealth() {
    // Local mode only confirms connectivity. The Router role probe
    // relies on MySQL 8.0 system variables (@@super_read_only,
    // @@hostname, @@port) that a plain local MySQL may not expose.
    const sql = this.isLocal
      ? "SELECT 1 AS ok"
      : "SELECT @@hostname AS hostname, " +
        "@@port AS server_port, " +
        "@@read_only AS read_only, " +
        "@@super_read_only AS super_read_only";

    const probe = async (role) => {
      const started = Date.now();
      let result;

      try {
        const rows = await this._query(
          sql,
          [],
          role
        );

        const server = rows[0];

        if (this.isLocal) {
          result = {
            ok: true,
            status: "healthy",
            server,
            error: null,
          };
        } else {
          const expected = role === "write" ? 0 : 1;

          const ok =
            Number(server.read_only) === expected &&
            Number(server.super_read_only) === expected;

          result = {
            ok,

            status: ok
              ? "healthy"
              : "role_mismatch",

            server,

            error: ok
              ? null
              : { code: "DB_ROLE_MISMATCH" },
          };
        }
      } catch (err) {
        result = {
          ok: false,
          status: "unavailable",
          server: null,

          error: {
            code: err.code || "UNKNOWN",
            sqlState: err.sqlState || null,
          },
        };
      }

      result.checkedAt =
        new Date().toISOString();

      result.durationMs =
        Date.now() - started;

      this.poolState[role].lastHealthCheck =
        result;

      return result;
    };

    const [write, read] = await Promise.all([
      probe("write"),
      probe("read"),
    ]);

    return structuredClone({
      ok: write.ok && read.ok,
      write,
      read,
    });
  }

  // =========================================================
  // CONNECTION STATE AND METRICS
  //
  // Snapshot only: no SQL is executed.
  // Counts are per Node process.
  // Acquired/released counts are observed driver events;
  // do not subtract them to calculate current in-use count.
  // =========================================================

  getConnectionState() {
    const result = {};

    for (const [role, pool] of [
      ["write", this.writePool],
      ["read", this.readPool],
    ]) {
      // Optional mysql2 internals.
      // null means unavailable in this driver version.
      const total =
        pool._allConnections?.length ?? null;

      const idle =
        pool._freeConnections?.length ?? null;

      const config =
        role === "write"
          ? this.writeConfig
          : this.readConfig;

      const state = this.poolState[role];
      const metrics = this.metrics[role];

      let status =
        total === 0
          ? metrics.connectionsCreated
            ? "disconnected"
            : "initialized"
          : total > 0
            ? "connected"
            : "unknown";

      if (state.lastConnectionError) {
        status = "degraded";
      }

      if (this.lifecycle !== "open") {
        status = this.lifecycle;
      }

      const completed =
        metrics.successfulQueries + metrics.errors;

      result[role] = {
        status,

        endpoint: {
          host: config.host,
          port: config.port,
          database: config.database,
        },

        connectionLimit: config.connectionLimit,
        queueLimit: config.queueLimit,

        ...state,
        ...metrics,

        averageQueryTimeMs: completed
          ? metrics.totalQueryTimeMs / completed
          : 0,

        poolAllConnections: total,
        poolFreeConnections: idle,

        poolInUseConnections:
          total === null || idle === null
            ? null
            : Math.max(0, total - idle),

        poolWaitingQueue:
          pool._connectionQueue?.length ?? null,
      };
    }

    const totals = {};

    const sumKeys = [
      "poolInitialized",
      "connectionsCreated",
      "connectionsEnded",

      "acquired",
      "released",
      "enqueued",

      "connectionErrors",
      "poolErrors",

      "queries",
      "successfulQueries",
      "errors",
      "inFlightQueries",

      "totalQueryTimeMs",

      "poolAllConnections",
      "poolFreeConnections",
      "poolInUseConnections",
      "poolWaitingQueue",
    ];

    for (const key of sumKeys) {
      totals[key] =
        result.read[key] === null ||
        result.write[key] === null
          ? null
          : result.read[key] + result.write[key];
    }

    const completed =
      totals.successfulQueries + totals.errors;

    totals.averageQueryTimeMs = completed
      ? totals.totalQueryTimeMs / completed
      : 0;

    return structuredClone({
      lifecycle: this.lifecycle,
      reinitializations: this.reinitializations,
      createdAt: this.createdAt,
      capturedAt: new Date().toISOString(),

      transactions: this.transactionMetrics,

      totals,
      read: result.read,
      write: result.write,
    });
  }

  stats() {
    return this.getConnectionState();
  }

  // =========================================================
  // CLOSE BOTH POOLS
  //
  // Call after stopping new requests/jobs and awaiting
  // in-flight work. Do not call after each query.
  // =========================================================

  close() {
    if (!this.closePromise) {
      this.closed = true;
      this.lifecycle = "closing";

      this._logPoolEvent("all", "CLOSING");

      this.closePromise = Promise.allSettled([
        invoke(this.writePool, "end"),
        invoke(this.readPool, "end"),
      ]).then((results) => {
        const errors = results
          .filter((item) => {
            return item.status === "rejected";
          })
          .map((item) => item.reason);

        this.lifecycle = errors.length
          ? "close_failed"
          : "closed";

        this._logPoolEvent(
          "all",
          errors.length
            ? "CLOSE_FAILED"
            : "CLOSED"
        );

        if (errors.length) {
          throw new AggregateError(
            errors,
            "Failed to close database pools"
          );
        }

        return true;
      });
    }

    return this.closePromise;
  }
}

// Existing imports receive the shared singleton.
module.exports = new Database();

// Optional explicit class access.
module.exports.Database = Database;