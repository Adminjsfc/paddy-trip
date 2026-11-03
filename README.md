# JSFC Paddy Trip Worker — paddy shipments and recoverable Redis writes

## Paddy integration update — 23 October 2026

This section supersedes the legacy source-crossing and IST database assumptions
in the historical release notes below. Database datetimes remain **UTC**; fields
labelled IST in location and decision logs use **+05:30**.

Shipment radii are stored in metres and projected as kilometres. The configured
multiplier is now 1000 so distance comparisons and logged radii use the correct
units. Existing Redis state is reclassified from its retained coordinates when
its recorded multiplier differs; no Redis flush is needed.

Departure matches the paddy API: a real source-OUT GPS packet strictly after
dispatch can start a pending shipment, including when Redis already contains an
OUTSIDE or incompatible state. A prior source-IN observation is not required.
The start event records `SOURCE_OUT_AFTER_DISPATCH`. Existing saved intents
finish their original operations. Arrival still requires destination-IN at a
GPS timestamp strictly later than the saved departure.

New write intents capture the active `gps_devices` / `imeis` assignment. Within
the same MySQL transaction as trip updates, packets are persisted to
`gps_packets` and the newest GPS point updates `gps_devices`, making the existing
paddy location/history APIs usable. Replays do not duplicate history or regress
live location. A conflicting coordinate at the same device/time or a changed
device assignment fails the transaction and retains the intent for review.
Only normalized GPS fields are available: `raw_payload` is a normalized record,
and the schema-required speed is zero with `speed_available:false` in that record.
No speed is inferred. Vehicles without an active device/IMEI mapping have no
device history written. Older intents without a captured device binding are
reconciled without attaching their packets to a potentially new device.

Run `npm test` for the full suite: **220 tests pass** after migrating the shared
simulation fixture to current shipment SQL, UTC database dates and the
source-OUT-after-dispatch policy. `npm run test:paddy` runs the focused subset.
Tests retain coverage of delayed/history packets, IST display across midnight,
Redis recovery, commit retries and duplicate suppression. Strict source-crossing
unit tests use an explicit strict configuration rather than the production default.
Historical capture files remain unchanged; tests convert their original IST
database dates to UTC at the fixture boundary.
Validation uses mocked transactions plus read-only checks against the current
local database. Live broker/Redis end-to-end deployment has not been exercised.
Apply the current Paddy migrations through `065_trip_eta_and_on_route_alert.sql` before
starting this worker; this package introduces no additional table migration.

Current build: **2026-10-02-utc-ist-display**. Read `INFLUX_RECOVERY.txt`
first for archive recovery, configuration and deployment. The worker now recovers
GPS history through the embedded `src/influx/GpsReportService.js` before new transitions.
No report endpoint or extra PM2 process is needed. Configure Influx directly in
`src/archive-config.js` (or optional environment overrides). Recovered actual
source visits take precedence over the missing-state assumption below.
`MISSING_STATE_START.txt` describes the preceding release and capture findings.
When a pending shipment has no Redis geofence state, source-IN is assumed at its
dispatch time; the first received/retained source-OUT GPS packet strictly
after dispatch starts it at that actual GPS timestamp. Existing Redis states
continue to require a recorded source crossing. Missing GPS cannot start a trip.

This package reads and updates the existing `shipments`, `vehicles`, and `shipment_events` tables from the paddy service schema. All database queries go through `src/Database.js`, using the same `DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASSWORD`, and `DB_NAME` environment variables as `paddy`. The default database is `sfc_cmr`; configure credentials outside source control. Pool limits default to four write and two read connections per worker.

The service has one MQTT ingress process and eight independent vehicle-sharded PM2 workers. This package has been tested locally with mocked database connections and a Redis/Lua emulator; it has not been deployed to your broker, Redis, or MySQL cluster.

## 1. Files to configure and deploy

| File | Purpose |
| --- | --- |
| `src/Database.js` | Direct connection settings, read/write pools, CRUD APIs, transactions, metrics, pool reinitialization |
| `src/repository.js` | Replica refresh, primary verification, transactional trip changes and saved-intent reconciliation |
| `src/write-buffer.js` | Commit → cache update → Redis acknowledgement order |
| `src/redis-store.js` | Non-expiring inbox, history, live location, cache, ownership leases and cursor scripts |
| `src/archive-client.js` / `src/influx/` | Embedded Influx queries, bounded pages and cancellation |
| `src/worker.js` | Eight independent shard workers; flush retries and minute refresh |
| `src/mqtt-subscriber.js` | One MQTT WebSocket subscriber with reconnect and acknowledgement backpressure |
| `src/mqtt-packet.js` | UTC date_time conversion and the five retained packet fields |
| `src/packet-ingest.js` | Optional direct GPS receiver/history adapter |
| `src/domain.js` | Geofence, chronological start/end rules and vehicle hashing |
| `optional-indexes.sql` | Optional performance indexes/generated shard column |
| `.env.example` | Optional Redis/MQTT/worker/Influx settings; no MySQL settings |
| `ecosystem.config.cjs` | One subscriber and eight workers |

The original uploaded text file is not overwritten. Its class API is preserved in the integrated JavaScript module. The inconsistent `this.isLocal = true` assignment in the attachment is corrected to match the actual configured local/router mode. Connection acquisition deadlines, date strings/BIGINT preservation, transaction isolation options and recovery methods are integrated into that class.

## 2. Redis-only queue and write journal

**No extra trip tables are created.** Runtime discovers active `shipments`, joins `vehicles` for the MQTT vehicle number, and uses each shipment's stored source/destination geofence snapshot. GPS departure changes `DISPATCHED` to `IN_TRANSIT`; destination arrival changes `IN_TRANSIT` to `ARRIVED`. Both transitions update the existing trip timestamps and append to `shipment_events` in the same transaction. Apply the paddy migrations through `040_shipment_trip_tracking.sql` before starting this worker.

1. Store each valid packet in the non-expiring Redis pending inbox before processing MySQL.
2. A shard worker processes one vehicle batch in GPS-time order. Different vehicles run concurrently.
3. Inside a primary transaction, lock existing trip rows, read current state, and plan exact trip IDs, timestamps, coordinates, and state preconditions.
4. Save that fixed write intent in Redis **before the first SQL UPDATE**. A failed Redis journal save aborts the transaction without updating any trip.
5. Apply conditional shipment updates and insert matching `shipment_events`, then commit the transaction.
6. Publish the active cache, then atomically write Redis completed-event markers, remove the committed batch's pending entries, and delete its matching intent. New arrivals remain queued.
7. On failure, keep the intent and packet payloads. After reconnecting, retry those exact target IDs. Matching trip dates, coordinates, and status identify an already-applied commit; the event cannot be reassigned to the next pending trip.

Redis ownership tokens fence journal and acknowledgement operations; existing InnoDB trip-row locks serialize trip updates. No MySQL mutex table or receipt table is used. A changed route/state that cannot safely reconcile raises `TRIP_INTENT_CONFLICT`, retains the intent, and blocks that vehicle for investigation rather than overwriting changed business data. Inspect the saved intent and matching trip rows before any manual recovery; do not blindly delete intent keys.

Redis and MySQL do not share an atomic transaction. This recovery design depends on retaining the Redis journal/markers and durable MySQL commits. It does not promise general exactly-once behavior after lost Redis data, restored inconsistent backups, or external modifications to trip rows. Generic `Database.js` helper calls made outside this service are not automatically buffered.

## 3. MySQL configuration

Set the same environment variables used by the paddy API in `paddy_trip/.env` or the process environment. Do not commit credentials or paste them into logs; the included metrics omit them.

| Setting | Default |
| --- | --- |
| DB_LOCAL | `'true'` — direct MySQL connection; set `'false'` to use read/write Router endpoints |
| DB_HOST / DB_PORT | `127.0.0.1` / `3306` |
| DB_WRITE_HOST / DB_WRITE_PORT | Falls back to DB_HOST / DB_PORT |
| DB_READ_HOST / DB_READ_PORT | Falls back to DB_HOST / DB_PORT |
| DB_NAME | `sfc_cmr` |
| DB_USER / DB_PASSWORD | `sfc_cmr_app` / required from environment |
| DB_WRITE_POOL_LIMIT / DB_READ_POOL_LIMIT | `4` / `2` per worker |
| DB_QUEUE_LIMIT | `100` per pool |
| DB_CONNECT_TIMEOUT / DB_ACQUIRE_TIMEOUT | `10000` / `30000` milliseconds |
| DB_TIMEZONE | `+00:00` (UTC, matching paddy API storage) |
| DB_LOG_POOL_EVENTS | `'false'` — routine pool logs hidden; errors remain visible |
| DB_USE_SHARD_COLUMN | `false` until optional generated-column migration is installed |
| DB_SSL_CA | Empty by default; CA PEM path enables verified TLS |

For a single MySQL instance, use DB_HOST/DB_PORT and the paddy API credentials. For a remote MySQL Router, set `DB_LOCAL=false` and configure DB_READ_HOST/DB_READ_PORT and DB_WRITE_HOST/DB_WRITE_PORT. All database datetimes are UTC.

| Query or operation | Destination |
| --- | --- |
| Minute refresh with shipment/vehicle joins | Read pool |
| Current shipment validation for discovered IDs | Write pool, bounded batches |
| Queue batch eligibility, trip locks and retry reconciliation | Same write connection |
| Shipment state updates and shipment event inserts | Same write transaction |
| Transaction final state read | Same write connection |
| Statistics snapshot | In memory; no SQL |

Read-pool lag can delay discovery of a newly added trip or route change. Queue flushing is independent of successful replica refresh: it uses current primary state, including while the read endpoint is unavailable. The read-cache path never authorizes an update without primary revalidation.

## 4. Required setup

Requires Node.js 22+, MySQL 8/InnoDB, standalone Redis 6.2+, and PM2. This client does not implement Redis Cluster/Sentinel discovery.

Extract into `/home/ubuntu/services/jsfc-trip-service` and review the direct DB settings:

```bash
cd /home/ubuntu/services/jsfc-trip-service
nano src/Database.js
npm ci
```

Apply the paddy schema migrations through `040_shipment_trip_tracking.sql`. Runtime needs SELECT/UPDATE on `shipments`, SELECT on `vehicles`, and INSERT on `shipment_events`. There are no worker-specific tables or required worker migrations. Optional indexes are separate from correctness.

### Redis persistence is required for the requested durability

Review and set these in Redis server configuration before starting the subscriber:

```conf
appendonly yes
appendfsync always
no-appendfsync-on-rewrite no
maxmemory-policy noeviction
```

By default the ingress adapter verifies these settings using `CONFIG GET` before accepting packets. Its Redis account therefore needs permission to read those settings. A server with insufficient durability is rejected rather than silently described as safe. For a managed service where configuration cannot be read, `REDIS_REQUIRE_DURABILITY=false` disables this check only after you independently verify equivalent persistence; it does not enable durability itself.

`appendfsync always` trades write performance for durability. `everysec` can lose a recent window after a crash and is not this package's required default. Maintain adequate disk/RAM and a durable upstream packet archive. Redis persistence, MySQL durable commit/replication configuration and reliable storage are still required; application code cannot promise zero loss after storage destruction, manual key deletion, or loss of every replica.

Optional service settings (Redis/MQTT only):

```bash
cp .env.example .env
nano .env
npm test
pm2 startOrRestart ecosystem.config.cjs --update-env
pm2 save
```

On an existing installation, preserve service settings, stop the old trip updater, replace the code, and restart these apps. Do not run the old schema setup. Existing support tables, if previously created, are left untouched and are no longer used. Old DB_* environment variables are ignored by this version. No manual FLUSHDB/FLUSHALL is needed or used. The service removes only acknowledged pending entries, not the full Redis database.

## 5. MQTT, timestamps and retained fields

Defaults:

- URL: `ws://18.60.155.145:20012/`
- Topics: `gps/{vehicle_no}/data` for pending/started trip vehicles only
- One stable client ID, `jsfc-trip-ingest-v1`; one subscriber instance.
- Optional username/password in service settings.
- If your broker uses `/mqtt`, edit MQTT_URL accordingly. The endpoint/path has not been verified remotely.

The vehicle comes from `gps/{vehicle_no}/data`. The confirmed UTC `date_time` is strictly parsed as `DDMMYYYY HHmmss` and converted with Date.UTC. The faulty incoming `gps_timestamp` is ignored. The previous GPS_TIMESTAMP_OFFSET_MS setting is not used; remove it from existing service settings.

For `date_time: "30092026 074653"`, the only retained payload is:

```json
{
  "latitude": 24.6409413,
  "longitude": 87.825546,
  "packet_type": "L",
  "gps_fix": "1",
  "gps_timestamp": 1790754413000
}
```

That event time is 07:46:53 UTC / 13:16:53 IST. Vehicle identity resides in the Redis key. GPS fix must equal 1. Both live and history packets use the same processing path. Older history cannot move the latest location backwards. Invalid dates, invalid coordinates, excessive future dates and conflicting coordinates for the same vehicle/second are rejected and logged, not buffered for automatic correction.

MQTT QoS 1 acknowledgement waits for Redis acceptance. A transient Redis failure retries the current message with backpressure. A persistent broker session plus publisher QoS 1 is required for reliable redelivery; requesting QoS 1 cannot upgrade a publisher's QoS 0 messages. Rejected invalid packets are deliberately acknowledged; correct/replay them from the source.

## 6. Trip rules

| Rule | Behavior |
| --- | --- |
| Pending/started/completed | 0 / 1 / 2 |
| Start | Observed INSIDE → OUTSIDE, or explicit missing-Redis-state source-IN assumption followed by actual OUT GPS strictly after lifting |
| End | Inside destination radius; packet time strictly greater than trip start |
| Location boundary | Distance equal to radius is inside |
| Exit semantics | A prior inside-source observation is mandatory; outside-only packets do not start |
| Previously open trips | Close all whose own destination/time conditions match |
| Pending trips | Start every trip with its own qualifying source exit, even when another trip is open |
| Ordering | Lifting date, then numeric trip ID |
| Other completed trips | Do not restrict this trip; compare only its own lifting/start GPS times |
| Same event | Can end a prior trip and start the next; cannot also end the newly started trip |
| Expected arrival | Not stored by paddy shipments; this worker preserves paddy's existing fields |
| Saved dates | Qualifying GPS event time stored in UTC, never server receipt time |
| Missing source geofence | Blocks start with a diagnostic reason |
| Missing destination geofence | Does not block source departure; blocks end until corrected |
| Route duration | Not present in the paddy shipment schema |

The paddy schema stores geofence radii in metres; the repository converts them to kilometres before geofence evaluation. Vehicle registration numbers are normalized to uppercase for topic routing. No dwell/debounce is added beyond the existing single-packet rule.

Redis completion markers identify vehicle + trip ID + action + GPS timestamp. Replaying that same transition does not write again, while another trip may independently qualify on the same GPS packet. Existing committed timestamps/completed trips are not retroactively rewritten. A saved write intent always retries its fixed targets before any new planning.

## 7. Redis keys and retention

Keys are under `jsfc:trip:v1:{sN}` for shard 0–7. Vehicle assignment is stable `CRC32(UPPER(TRIM(vehicle_no))) % 8`. It distributes vehicles, not individual packets/trips, and does not guarantee exactly equal shard sizes.

| Key suffix | Purpose / retention |
| --- | --- |
| `:owner` | Worker lease, 30 seconds, renewed while running |
| `:pending:vehicles` | Vehicles with queued packets, **no expiry** |
| `:vehicle:V:pending:order` | Pending event-time sorted set, **no expiry** |
| `:vehicle:V:pending:data` | Pending complete normalized packets, **no expiry** |
| `:vehicle:V:write-intent` | Exact SQL transition targets and batch, **no expiry**, removed after confirmed commit/finalization |
| `:vehicle:V:completed-events` | IDs of successfully applied trip-changing events, **no expiry** |
| `:vehicle:V:history` / `:packets` | Rolling event history, seven days by default |
| `:vehicle:V:live` | Most recent event point |
| `:vehicle:V:trips` | Active trip map, five-minute cache TTL |
| `:trip:ID` | Full trip + route/geofence JSON, five-minute cache TTL |
| `:vehicle:V:rewind` / `:cursor` | Rolling-history replay progress |
| `:health` | Worker/pool state and queue retry counts |

Pending payloads do not depend on the rolling history keys. They remain through outages longer than seven days and are processed without reapplying the ingress age cutoff. Newly ingested packets older than HISTORY_DAYS are still rejected by the input policy. Once a packet is successfully evaluated and acknowledged, retrospective discovery of a future trip depends on retained history or your external archive.

A successful no-op evaluation is also a confirmed transaction and can be acknowledged; it does not retain a completed-event marker because no trip changed. The packet remains in rolling history while within retention.

Do not delete journals or completed-event markers while related events can replay. Markers have no automatic purge; budget storage and define a reconciled retention policy if necessary.

No queue size cap silently discards events. If Redis reaches capacity under noeviction, ingestion fails/retries instead; upstream durability is essential. Size storage for the longest outage you need to absorb. Do not change shard count from eight or change Redis prefix while writes remain pending, as that would leave the old queue unprocessed.

## 8. Connection recovery and retry

- Closed physical connections are discarded by mysql2 and later checkouts create new ones.
- Transport failures/closed pool errors request pool reinitialization after the current batch of operations settles. Only one reinitialization runs per process.
- Reinitialization creates read/write pools from the same direct file configuration. It does not change endpoints or reconnect midway through a transaction.
- Vehicle batches retry with exponential delay from 1 second up to 30 seconds. Pending entries have no retry-count deletion policy.
- SQL/schema/authentication errors remain visible and pending until fixed; they are not reported as successful writes.
- An uncertain COMMIT is not blindly resumed on a new connection. The saved Redis intent is reconciled against current primary trip status, dates, and coordinates before retrying its fixed operations.
- Deliberate `db.close()` during shutdown is final. It is not automatically reopened by the recovery loop.

`REINITIALIZING`, `REINITIALIZED`, `pending_batch_retained`, `pending_batch_committed`, `mqtt_health`, and periodic worker `health` provide progress. Pool generation query counters reset when pools are recreated; `reinitializations` counts generations. Transaction metrics remain per process. Operational event logs are not an audit outbox and can miss a committed event if a process dies before logging; MySQL trip state and the Redis intent/markers provide recovery evidence.

```bash
pm2 logs jsfc-gps-mqtt --lines 100
pm2 logs jsfc-trip-0 --lines 100
pm2 restart jsfc-trip-3
```

## 9. Optional indexes and scale

The minute-refresh hash expression can scan active rows in each worker. Review `optional-indexes.sql` and existing indexes before using the optional generated shard column. After the migration, set `DB_USE_SHARD_COLUMN: true` inside DATABASE_CONFIG. It is no longer an environment option.

The durable queue intentionally validates primary state for each vehicle batch, including vehicles without cached trips. Default batches are 50 packets, four concurrent vehicles per worker. This increases primary work compared with cache-only filtering, in exchange for independent outage recovery. Measure your actual packet rate and tune WRITE_BATCH_SIZE/VEHICLE_CONCURRENCY and indexes; no throughput claim or production load benchmark is made.

The eight workers configure up to 48 pool connections (32 write + 16 read). The MQTT subscriber now uses Database.js for its active-vehicle query and adds pool capacity of four write and two read connections (54 total configured capacity). Pools open connections on demand. Connection acquisition queues are bounded; connections arriving after an acquisition deadline are released without executing that timed-out statement.

## 10. Validation

- 78 automated tests pass: domain rules, direct DB configuration, read/write routing, same-connection transactions, timeout handling, pool recreation, rollback, fixed-intent replay after a lost commit response, journal-before-write ordering, route-conflict retention, and queue acknowledgement failures.
- Actual Node/ioredis methods and Lua scripts were exercised against a local fakeredis/Lua TCP emulator: non-expiring pending data, survival after deleting rolling history, restarted Store recovery, duplicates, preservation of newer arrivals during acknowledgement, and latest-location ordering.
- JavaScript syntax and ZIP integrity are checked.
- No real broker, MySQL cluster, Redis disk-persistence crash, PM2 deployment, or load benchmark was tested.

Run unit tests with `npm test`. Optional queue check against an isolated test Redis: `node scripts/check-redis-queue.cjs`. Optional emulator check: install `fakeredis[lua]` for Python and run `python3 scripts/run-redis-emulator-check.py`.

Before production cutover, verify a pending → started → completed trip, stop/restart MySQL during processing, confirm pending counts persist, recover MySQL, and verify that pending entries clear only after the expected trip values and Redis completion markers exist.

## References

- Redis AOF persistence: https://redis.io/docs/latest/operate/oss_and_stack/management/persistence/
- Redis Lua execution: https://redis.io/docs/latest/develop/programmability/eval-intro/
- MySQL transaction completion: https://dev.mysql.com/doc/refman/8.0/en/innodb-autocommit-commit-rollback.html
- MySQL Group Replication consistency: https://dev.mysql.com/doc/refman/8.0/en/group-replication-configuring-consistency-guarantees.html
- MQTT.js: https://github.com/mqttjs/MQTT.js
- PM2 ecosystem configuration: https://pm2.keymetrics.io/docs/usage/application-declaration/

## Offline and historical scenarios

Departure and arrival use GPS event time from UTC `date_time`, formatted to IST when saved to MySQL. Server receive time never determines eligibility. Route duration calculates expected arrival; it is not an expiry cutoff. An arrival after ETA still closes the trip at its actual GPS time.

| Scenario | Result |
| --- | --- |
| Pending trip; history inside source | Keep pending |
| History inside source then outside after lifting | Start at the exit event time |
| Outside source at/before lifting | Do not start |
| Started trip; historical destination entry after start | End at that event time, regardless of receive delay |
| Destination entry before ETA, received after ETA | End at original GPS time; receive delay does not make arrival late |
| Actual destination entry after ETA | End at actual GPS time |
| Destination packet at/before saved start | Do not end |
| Still outside destination or no valid packet | Keep open; do not infer arrival |
| Departure and arrival queued together, delivered in reverse order | Sort by GPS time and apply start then end |
| Duplicates, history/live copies | Redis marker prevents repeating a committed transition |
| Multiple open trips | Close every matching destination/time condition; independently start all qualifying pending trips |
| New trip discovered after packets were evaluated | Replay retained history |
| A prior start/end makes previously scanned history eligible | Rewind and reevaluate retained history |
| MySQL unavailable | Keep packets/intents in Redis and retry |
| First available packet is already at destination | Pending trip stays pending without prior source-inside evidence; an already-started trip can end |

`src/history-replay.js` queues all retained history points in a replay batch. It no longer filters them against a cached trip state that may change mid-batch. SQL evaluates each packet sequentially against simulated/current trip state. Replays do not update latest location or rewind themselves; successful transition intents request a rescan before acknowledgement.

**Ordering limits:** sorting covers packets available in the queue, not packets that have not arrived yet. If a newer packet already started a trip, a later-delivered destination packet timestamped before that saved start cannot close it. Likewise this package does not retroactively move committed start/end timestamps earlier when older evidence arrives. Such corrections need an explicit historical reconciliation policy, especially if subsequent trips already started. Completed trips are deliberately not reloaded. No finite waiting period can guarantee complete ordering for unlimited offline delay.

Newly received history is accepted for seven days by default (`HISTORY_DAYS`). Increase this setting before operation if the required offline window is longer. Packets already in the durable pending queue do not expire during a MySQL outage. Invalid GPS fixes, invalid dates/coordinates, and packets outside the configured age/future limits are rejected. Missing history cannot be reconstructed by this service.

Local validation includes 78 unit tests and Redis/Lua emulator checks. No production MQTT/MySQL outage or storage crash test was performed.

## Reproducible one-/two-day offline simulation

Run `python3 scripts/run-redis-emulator-check.py` after installing the optional Python `fakeredis[lua]` package. This runs the Redis checks and `scripts/simulate-offline.cjs` using an isolated emulator with uniquely prefixed keys. It never imports your credential-bearing Database.js or connects to production MySQL/MQTT. The MySQL adapter simulates transactions and records the actual repository UPDATE parameters.

The package includes `simulation-results/OFFLINE_SIMULATION.md` and `offline-results.json` with 40 scenario outcomes, GPS-based saved dates and update order. Both one- and two-day offline histories are tested. The configured seven-day history window already accepts these delays; no narrower age cutoff or extra MySQL table is introduced.

## Live simulation coverage

`python3 scripts/run-redis-emulator-check.py` now also runs `scripts/simulate-live.cjs`. The included `simulation-results/LIVE_SIMULATION.md` records 29 asserted live scenarios; `live-results.json` records saved timestamps, update counts, final statuses, and remaining pending packets. Live event and receive times advance together. These checks use the application MQTT payload parser and actual Redis/Lua queue/journal code with the shared simulated MySQL transaction adapter in `scripts/simulation-fixtures.cjs`.

Coverage includes a full point-by-point live journey, source/destination boundary equality, repeated positions, duplicate/conflicting packets, invalid fix/coordinates/date, excessive future timestamps, missing geofences, invalid route duration, late arrival, multiple trips, MySQL outage, lost commit confirmation, Redis cache failure, stale worker lease, mixed live/history arrival, and a 301-point backlog crossing multiple write batches.

Latest local validation: 174 unit tests, 40 one-/two-day offline simulations, 29 live simulations, 16 start-fix simulations, 8 independent-trip simulations, 15 archive-recovery simulations, and Redis queue/Lua checks pass. This is a defined scenario suite, not proof of every possible production condition. The suite does not test actual MQTT broker delivery, MySQL concurrency/locking or cluster failover, Redis on-disk durability under crash, PM2 deployment, or production throughput. Existing single-packet geofence rules remain: there is no dwell/debounce filter, and committed timestamps are not retroactively corrected.

## MQTT packet header filter

Only JSON packets with exact `packet_header: "$PVT"` enter GPS validation and processing. Other or missing headers are acknowledged and ignored before Redis/MySQL work. The `mqtt_health.ignored_header` counter tracks them without per-packet log noise. `packet_type` remains the live/history value (L/H); it is not compared with `$PVT`. Existing GPS fix, coordinate, date, and age checks still apply to accepted PVT packets. The header is used only for filtering and is not added to the five retained location fields.

## MQTT and trip detail logs

Logging is enabled by default. `LOG_MQTT_DATA=false` disables received/buffered data logs; `LOG_TRIP_DETAILS=false` disables full trip snapshots. These are optional service settings, not database settings.

- `mqtt_connected`, `mqtt_subscribe_requested`, and `mqtt_subscribed`: connection, requested topic/QoS, and granted subscription.
- `mqtt_packet_received`: valid $PVT packet, vehicle/topic, five normalized GPS fields, and UTC event time. This precedes the Redis write and is not a persistence confirmation.
- `mqtt_packet_buffered`: Redis acceptance, shard, GPS timestamp, and duplicate flag. This does not mean the MySQL trip has started/ended.
- `trip_cache_refreshed`: shard vehicle/trip totals each successful refresh.
- `vehicle_trip_list`: vehicle number, active trip count and IDs each refresh and after a transition.
- `trip_details`: one shipment object per log entry, including shipment number, vehicle and paddy source/destination geofences. Completed shipments are removed from the active list; trip_ended identifies arrival.
- `trip_started` / `trip_ended`: committed transition with GPS-based datetime.
- Routine pool initialization/acquire/release/close logs are hidden via `DB_LOG_POOL_EVENTS: 'false'` directly in Database.js. Error events remain visible. Worker health logs omit the nested database/pool snapshot; the Database.js metrics API still works.

Watch logs after restarting the package:

```bash
pm2 startOrRestart ecosystem.config.cjs --update-env
pm2 logs jsfc-gps-mqtt --lines 100
pm2 logs jsfc-trip-0 --lines 100
```

Each worker logs only its assigned vehicles. Full trip data is printed from actual successful reads, not example values. Normalized packet logs preserve the five-field GPS payload policy; raw tracker strings and connection credentials are not printed. Per-packet and per-trip logs can be large; use log rotation on your deployment or turn off the two detail flags when finished diagnosing.

## Active-trip-only MQTT subscriptions

The subscriber no longer subscribes to `gps/+/data`. Every 60 seconds it reads distinct vehicle registration numbers for shipments in `DISPATCHED` or `IN_TRANSIT` with `trip_status IN (0,1)`. Pending-dispatch trips must be included so source departure can be detected. One exact topic is subscribed per vehicle, even if that vehicle has multiple shipments. Once no active shipment remains, the topic is unsubscribed at the next successful refresh. The subscription projection reads the primary to avoid stale replica statuses.

Subscription changes are batched by 100 topics. Confirmed, unchanged topics are not resubscribed on every poll. The subscriber logs requested/granted topics, removals and `mqtt_active_vehicle_list`. Unrelated topic deliveries from old broker sessions are ignored. On reconnect it explicitly removes the legacy wildcard and restores the current individual topics. Redis stores the tracked topics under a client-ID-specific key so uncertain acknowledgements and process restarts can be reconciled without extra MySQL tables. Keep the same MQTT client ID and Redis prefix when upgrading.

On database failure the last known subscription list remains active; it is not cleared. This means completed vehicles can remain subscribed until a successful refresh. Newly created trips may take up to one successful refresh interval (normally 60 seconds) to be subscribed. Packets published before the individual subscription exists are not guaranteed to be delivered by MQTT; tracker history resend or external backfill is required. Redis history/queues already captured for a vehicle remain available after unsubscribe.

The 78-test suite includes nine subscription cases: unique active topics, minute additions/removals, unchanged lists, empty list cleanup, DB outage retention, reconnect restoration/wildcard removal, subscription denial retry, uncertain-topic cleanup, and failed wildcard cleanup retry. Broker callbacks are mocked in these tests; no production MQTT connection was made.

## Latest-location trip status and distances

`LOG_VEHICLE_LOCATION=true` (default) emits `vehicle_trip_location` after each processed vehicle batch and each successful minute refresh. Each entry includes vehicle number, internal/display trip IDs, numeric trip status and PENDING/STARTED/COMPLETED name; latest latitude/longitude; GPS UTC/IST time and age; source/destination INSIDE/OUTSIDE status; and `distance_from_source_km` / `distance_from_destination_km` rounded to three decimals. Radius values are also shown in kilometres.

Distances are great-circle distances to geofence centres, not driving distance or distance remaining along the route. Missing geometry produces UNKNOWN and null distance where coordinates are missing; absent GPS produces NO_LOCATION. Source/destination state uses the full-precision distance, independent of rounded display values.

The location comes from Redis's newest event-time point. Historical processing does not regress it. `processed_packet_timestamp` and `processed_packet_older_than_last_location` identify when the processed batch is older than the displayed latest point. A just-completed trip is logged from its post-transaction row before disappearing from the active cache. Logging errors are reported separately and do not undo or retry an already-finalized write. Pool logs remain hidden.

## Separate pending/started logs and transition-only messages

Current worker log categories:

| Kind | Content |
| --- | --- |
| `pending_trip_list` | Only status 0 trips, with full details, per vehicle on minute refresh |
| `started_trip_list` | Only status 1 trips, with full details, per vehicle on minute refresh |
| `pending_trip_location` | Latest GPS, source/destination status and km distances for status 0 |
| `started_trip_location` | Same location fields for status 1 |
| `trip_started` | Only an actual newly applied start, with vehicle, trip ID, status 1 and GPS datetime message |
| `trip_ended` | Only an actual newly applied end, with vehicle, trip ID, status 2 and GPS datetime message |

These categories replace the earlier mixed `vehicle_trip_list`, `trip_details`, and `vehicle_trip_location` output. Empty status groups are not printed. Routine `pending_batch_committed` messages are removed. Transition messages do not print every trip in the vehicle list. Duplicate or no-change packets do not generate start/end messages. Routine pending/started location snapshots continue separately; to show transitions without list/location snapshots set `LOG_TRIP_DETAILS=false` and `LOG_VEHICLE_LOCATION=false`. MQTT logging is controlled separately by `LOG_MQTT_DATA`.

Operational logs are not a durable audit trail: a crash after commit can prevent a transition message, although retry reconciliation preserves the trip state. Actual server log output was not tested here.

```bash
pm2 logs --raw | grep -E '"kind":"pending_trip_(list|location)"'
pm2 logs --raw | grep -E '"kind":"started_trip_(list|location)"'
pm2 logs --raw | grep -E '"kind":"trip_(started|ended)"'
```

## Per-vehicle, per-trip persistent geofence state (current rule)

Redis hash `jsfc:trip:v1:{sN}:vehicle:V:trip-geofences` stores one field per trip ID. Each state contains source/destination INSIDE/OUTSIDE/UNKNOWN status, last evaluated GPS point/time, entry and exit timestamps, trip status, and the route geometry identity. State is recorded in the saved write intent and atomically finalized with queue acknowledgement after MySQL commit, including no-op packets. Active state survives process restart without a TTL and is deleted after confirmed completion. No extra MySQL table is used.

A pending trip with existing Redis state starts on an observed source INSIDE → OUTSIDE transition after lifting. If its Redis state is genuinely absent, the missing-state policy assumes source-IN at lifting and accepts an actual OUT packet strictly after lifting. Open trips and completed-trip dates belonging to other IDs do not block it. Started trips end on destination INSIDE after saved start; no prior destination-OUT observation is required.

The same rules apply to live and historical data. Historical replay keeps a separate chronological replay state so an older source visit followed by a replayed exit can qualify without regressing the latest snapshot. Completed trips/timestamps are not retroactively rewritten. Actual geometry changes reset source evidence. Equivalent MySQL decimal strings/numbers and datetime precision do not reset it. State is isolated by vehicle AND trip: a newly added trip cannot borrow a different trip's source visit.

On upgrade, compatible existing state is reused. Older metre-based states are reclassified from stored coordinates under the corrected kilometre radius. Present but incompatible state does not authorize the missing-state fallback; real observations must reestablish its crossing. Existing saved intents finish their fixed operations. Completed geofence fields/keys are removed during confirmed write finalization; event markers remain for duplicate protection.

The live/offline simulators now explicitly seed a source-inside observation before departure scenarios. Their fixtures and reports distinguish this prerequisite. Source-only, outside-only, delayed history and restart-safe Redis state are covered by the updated tests. Location logs include `tracked_geofence` separately from the latest-location distance calculation.

## Individual vehicle–trip state and completion cleanup

Each active trip now has an individual JSON key:

```text
jsfc:trip:v1:{sN}:vehicle:VEHICLE_NO:trip:TRIP_ID:geofence
```

Its JSON contains `vehicle_no`, `trip_id`, `trip_status`, `source_status`, `destination_status`, source/destination entry/exit GPS timestamps, and the last evaluated latitude/longitude/time. The vehicle's `trip-geofences` hash provides an index/fallback copy; the individual vehicle–trip key is authoritative and is also looked up by active MySQL trip IDs, so a missing index field cannot hide source-IN state. Both are written together during Redis finalization after the MySQL transaction succeeds.

On trip completion, the individual geofence key and that trip's vehicle-hash field are deleted atomically with queue acknowledgement. Existing active-trip cache cleanup removes the trip's cached details. Other trips on the same vehicle remain. A failed MySQL write keeps state and pending work for retry. If MySQL commits but Redis is unavailable, cleanup waits for successful retry reconciliation. Historical replay does not recreate state for completed trips.

The event-ID deduplication markers and vehicle GPS history are intentionally separate: they remain to prevent repeated updates and support other trips. There is no FLUSHDB/FLUSHALL and no extra MySQL table. Old completed hash fields from earlier package versions are pruned when that vehicle's saved state is next finalized; this update does not run a global deletion across old Redis data.

```bash
redis-cli GET 'jsfc:trip:v1:{s0}:vehicle:JH16B5498:trip:123:geofence'
```

Replace shard, vehicle and internal trip ID with the actual values. After successful completion cleanup, GET returns nil. The Redis integration check verifies individual-key creation, retention before finalization, deletion after completion, and retention of duplicate-prevention markers.


## 2 October 2026 start investigation and fix

Build marker: `2026-10-02-utc-ist-display` in worker_ready. This package fixes reproduced code defects; it does not claim that a production server has been inspected or deployed.

Reproduced with Redis source INSIDE and no open MySQL trip:

- Raw geometry signatures lost source-IN when identical MySQL DECIMAL values changed between numbers and strings. Geometry comparison now uses canonical numeric values and date times.
- An unrelated earlier pending row prevented another trip's qualifying source exit. The worker now starts **every eligible** pending trip in stable lifting-time/ID order. An open trip with a different ID does not block it.
- Paddy shipments do not have the legacy route-duration field. The worker preserves paddy's existing `expected_completion_at` value and derives arrival from GPS geofences.
- Individual state keys were written but only the lookup hash was read. The individual vehicle–trip key now takes precedence, including when its hash field is absent.
- Replay started at lifting time and omitted the last pre-lifting source point when a trip was discovered later. A replay beginning at this floor includes its last retained predecessor as evidence. Starts still require GPS exit time strictly after lifting.

Geofence radii are stored in metres by paddy and converted for the worker's kilometre-based distance calculations. No radius migration or Redis flush is required.

### Processing order and state

1. MQTT accepts exact $PVT packets for subscribed active-trip vehicles and buffers normalized GPS data in Redis.
2. The assigned worker reads the authoritative MySQL pending/started rows and each vehicle–trip's Redis state. INSIDE source followed by OUTSIDE after lifting qualifies a start; genuinely absent Redis state can use the explicit source-IN-at-lifting fallback. Destination INSIDE after the saved GPS start qualifies an end.
3. Redis saves the exact desired operations and resulting geofence state in `:write-intent` before any MySQL UPDATE. Its desired state is IN_TRIP / END. This is the durable retry record while a database write is pending.
4. MySQL commits conditional status 0→1 or 1→2 with GPS timestamps. Missing/closed connections leave queued work for retry through the supplied Database.js.
5. After confirmed commit, Redis finalizes active state (`journey_state: IN_SOURCE / PENDING / IN_TRIP`) or removes the completed trip's individual state, hash field and active cache. A start and end can commit together for an offline batch. Duplicate event markers remain.

Historical points never borrow a future INSIDE observation as departure evidence. A packet used by another trip can still trigger this trip’s transition; only the same vehicle/trip/action/timestamp is a duplicate. Completed MySQL trips and saved timestamps are not rewritten. Existing OUTSIDE state without crossing evidence stays pending. Genuine state absence may use the explicitly logged missing-state assumption.

### Deployment and diagnosis

Replace the service code and scripts from this package. Preserve your deployed `src/Database.js` connection settings and service configuration. No table changes are needed. From the deployed project directory:

```bash
pm2 startOrRestart ecosystem.config.cjs --update-env
node scripts/diagnose-trip.cjs JH10V7046
```

Optional last argument limits the displayed trip. Completed trips can also be inspected with this argument after their Redis state has been removed:

```bash
node scripts/diagnose-trip.cjs JH10V7046 22777
```

The diagnostic command only reads MySQL and Redis. It prints primary trip status, source/destination distances, Redis state, any unfinished write, and an evaluation of the latest packet. A historical decision recorded in redis_state.last_decision can differ from the latest-location evaluation; compare their GPS timestamps. Run it on the actual service server to inspect that deployment. It must not be confused with confirmation that the delivered package has already been deployed.

Expected worker_ready: build `2026-10-02-utc-ist-display`, radius_unit `km`, radius_multiplier `1000`. `trip_started` and `trip_ended` include `mysql_write: COMMITTED`. `trip_start_waiting` logs once per reason change or per minute, with the evaluated packet coordinates/time and reason:

| Reason | Meaning |
| --- | --- |
| INSIDE_SOURCE | Vehicle has not departed this source |
| SOURCE_EXIT_NOT_OBSERVED | No qualifying source-IN then OUT sequence is available |
| GPS_NOT_AFTER_LIFTING | GPS time is at/before lifting |
| INVALID_SOURCE_GEOFENCE / INVALID_LIFTING_TIME | Source configuration or lifting date needs correction |
| AWAITING_RECORDED_EXIT_REPLAY | Recorded source crossing exists; replay needs the retained exit packet with its original coordinates |

Do not manually mark Redis as INSIDE or clear journals to force a start. Retained history is automatically reconsidered at worker startup and on active-trip changes. If earlier source points were never delivered or have expired, they must come from the tracker/history source.

### Validation for this fix

`npm test`: 174 unit/repository tests. `python3 scripts/run-redis-emulator-check.py`: Redis queue checks, 29 live simulations, 40 one/two-day offline simulations, 16 targeted start-fix regressions, 8 independent-trip cases and 15 archive-recovery cases. The independent-trip suite uses observed crossing evidence; the start-fix suite also covers the explicitly labelled missing-state assumption and includes the supplied source coordinates, missing hash entries, delayed pre-lifting source packets, restart, connection outage and lost commit response. Reports are in simulation-results. MySQL is a transaction mock and MQTT messages invoke the real parser directly; a production broker/database connection was not tested.


## Confirmed OPEN_TRIP_EXISTS fix: independent trips

The supplied production diagnostic identified pending trip 23808 (JH02AK7064) blocked by started trip 23398. Both use the same source but different destinations. Trip 23808 has recorded source entry, source exit and destination entry. The earlier vehicle-wide restriction was incompatible with processing each trip independently.

Current build: `2026-10-02-utc-ist-display`.

- Every active trip evaluates its own source/destination and lifting/start times.
- One exit packet can start multiple eligible trips loaded from the same source.
- One destination arrival can end all already-started trips whose own destination matches. Other trips stay open.
- Another open trip or another trip’s completion date cannot block a valid departure.
- Duplicate IDs now include vehicle, trip ID, start/end action and GPS timestamp. Legacy vehicle-wide markers remain in Redis but no longer suppress other trip IDs. Existing MySQL status/date guards prevent rewriting previously started/completed rows.
- Pending write intents from the older build first finish their original exact targets. They are not expanded during retry. Newly planned work uses independent trips.

### Recovery of the supplied example

On startup the worker rewinds retained history for active trips. If the original qualifying packets are still retained, the reported sequence allows trip 23808 to start at 2 October 08:33:10 IST and end at 09:56:02 IST. Trip 23398 remains started until its own destination/time rule matches. Historical processing uses actual packet coordinates and earliest qualifying retained events; these example times are from the diagnostic, not a manual SQL override.

If the source-IN packet is unavailable but compatible Redis state already records a valid source-IN/source-OUT sequence, the real retained exit packet can recover that departure. The worker never creates missing exit coordinates from the destination point or writes a backdated start based only on two timestamps. If the exit packet is unavailable, supply the tracker’s original history. No Redis flush is required.

Deploy updated code while preserving your Database.js connection settings, then run:

```bash
pm2 startOrRestart ecosystem.config.cjs --update-env
node scripts/diagnose-trip.cjs JH02AK7064 23808
```

After recovery, the diagnostic can show trip_status 2 and completion_date even though its Redis key is gone. requested_trip_found=false explicitly identifies a vehicle/trip mismatch. Confirm the worker_ready build marker to ensure the worker, not just the diagnostic command, uses the new code.

The included independent-trip simulation checks the provided trip IDs, route geometry and timestamps, using representative source/exit coordinates where the raw packet was not supplied. It verifies MySQL start/end values, keeping 23398 open, deleting only 23808’s Redis state, old marker compatibility, duplicate suppression and outage/lost-commit recovery. No production data was modified during development.


## Worker health update — 2 October 2026

See WORKER_FIX.txt for the latest capture findings and changes. History replay
counts missing payloads before adding predecessor evidence. Health separates
history enqueue attempts from committed-and-acknowledged packet flushes and
includes build, sampled_at and last_successful_flush_at. Repeated identical
location logs are suppressed within the refresh interval. Trip rules and the
existing Database.js connection configuration are unchanged.

On GPS source-OUT, START sets expected_completion_at to the actual trip_started_at plus the linked shipment route duration in minutes. It does not base final ETA on dispatch time.
