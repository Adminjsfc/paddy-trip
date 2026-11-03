# Live GPS simulation

29 asserted scenarios passed.

Live event and receive time advance together; ordinary packets use packet_type L. A mixed live/history case is labelled explicitly. Application parser, Redis Lua, queue, write journal and repository execute unchanged. MySQL transaction adapter and receive clock are simulated. No production MQTT transport, MySQL locking/failover, Redis disk crash or load benchmark was tested.

| Scenario | Packets accepted | SQL updates | Final statuses | Result |
|---|---:|---:|---|---|
| Full live journey: source -> road -> destination | 4 | 2 | 2 | PASS |
| Repeated source points do not start | 4 | 0 | 0 | PASS |
| At lifting time outside source stays pending | 2 | 0 | 0 | PASS |
| Repeated road points start only once | 4 | 1 | 1 | PASS |
| Repeated destination points end only once | 5 | 2 | 2 | PASS |
| Duplicate same-second redelivery has one effect | 3 | 1 | 1 | PASS |
| Same-second conflicting location is rejected | 2 | 1 | 1 | PASS |
| Source boundary counts as inside | 2 | 0 | 0 | PASS |
| Destination boundary counts as inside | 4 | 2 | 2 | PASS |
| Return to source does not undo a start | 3 | 1 | 1 | PASS |
| Arrival after ETA still ends | 3 | 2 | 2 | PASS |
| No destination evidence keeps trip open | 3 | 1 | 1 | PASS |
| Missing source geofence blocks start | 2 | 0 | 0 | PASS |
| Missing destination allows start; arrival waits for valid destination | 3 | 1 | 1 | PASS |
| Invalid duration starts trip with NULL ETA | 2 | 1 | 1 | PASS |
| Invalid GPS fix produces no queue entry | 1 | 0 | 0 | PASS |
| Invalid coordinates produce no queue entry | 1 | 0 | 0 | PASS |
| Malformed date produces no queue entry | 1 | 0 | 0 | PASS |
| Excessive future GPS date is rejected | 1 | 0 | 0 | PASS |
| Close old trip before next live start | 3 | 2 | 2, 1 | PASS |
| Unmatched open trip remains open while another trip starts | 3 | 1 | 1, 1 | PASS |
| All matching open trips close | 2 | 2 | 2, 2 | PASS |
| Live database outage retains then drains packets | 3 | 2 | 2 | PASS |
| Lost commit response retries without duplicate write | 2 | 1 | 1 | PASS |
| Redis cache failure after commit preserves journal for retry | 2 | 1 | 1 | PASS |
| Lease loss prevents stale worker writing | 2 | 1 | 1 | PASS |
| Live start followed by delayed history destination closes correctly | 4 | 2 | 2 | PASS |
| Source observed, first destination point starts only; later point ends | 3 | 2 | 2 | PASS |
| 301-point backlog crosses write batches without lost arrival | 302 | 2 | 2 | PASS |

Status: 0 pending, 1 started, 2 completed. PASS includes expected rejection or retained-open cases. Detailed saved GPS timestamps are in live-results.json.

Reproduce: npm ci; python3 -m pip install "fakeredis[lua]"; python3 scripts/run-redis-emulator-check.py

Business boundaries: these departures include observed source IN; missing-state fallback is additionally covered by start-fix simulations. Existing OUTSIDE state without entry evidence stays pending. No dwell/debounce is configured. A first point at destination needs a later point to end. Arrival must be later than saved start. Committed timestamps are not retroactively rewritten.
