# Offline historical GPS simulation

40 scenarios passed. Each scenario is run with both one-day and two-day delayed GPS packets.

Received: 30 September 2026, 08:00 UTC (13:30 IST). GPS date_time is UTC; saved SQL dates are IST.

Application MQTT parsing, Redis inbox/journal/Lua, history replay, repository decisions and SQL parameters are exercised. MySQL is a simulated transaction adapter; this is not a production database or broker test.

| Scenario | Delay (days) | Status | Saved start (IST) | Saved end (IST) | Result |
|---|---:|---:|---|---|---|
| Source-only history stays pending | 1 | 0 | - | - | PASS |
| Historical departure starts at GPS time | 1 | 1 | 2026-09-29 10:40:00.000 | - | PASS |
| Whole offline journey starts and ends at historical times | 1 | 2 | 2026-09-29 10:40:00.000 | 2026-09-29 12:00:00.000 | PASS |
| Already started vehicle closes on delayed destination history | 1 | 2 | 2026-09-29 10:40:00.000 | 2026-09-29 12:00:00.000 | PASS |
| Reverse packet delivery sorts before writes | 1 | 2 | 2026-09-29 10:40:00.000 | 2026-09-29 12:00:00.000 | PASS |
| Arrival after ETA still completes | 1 | 2 | 2026-09-29 10:40:00.000 | 2026-09-29 13:20:00.000 | PASS |
| Duplicate live/history redelivery does not repeat writes | 1 | 2 | 2026-09-29 10:40:00.000 | 2026-09-29 12:00:00.000 | PASS |
| Packets at or before lifting do not start | 1 | 0 | - | - | PASS |
| Destination before/equal start does not close | 1 | 1 | 2026-09-29 10:40:00.000 | - | PASS |
| No destination evidence leaves started trip open | 1 | 1 | 2026-09-29 10:40:00.000 | - | PASS |
| MySQL outage retains packets; recovery completes journey | 1 | 2 | 2026-09-29 10:40:00.000 | 2026-09-29 12:00:00.000 | PASS |
| Lost commit response reconciles exact historical writes | 1 | 2 | 2026-09-29 10:40:00.000 | 2026-09-29 12:00:00.000 | PASS |
| New trip discovered after history ingestion replays journey | 1 | 2 | 2026-09-29 10:40:00.000 | 2026-09-29 12:00:00.000 | PASS |
| Source observed, destination point cannot both start and end | 1 | 2 | 2026-09-29 12:00:00.000 | 2026-09-29 12:01:00.000 | PASS |
| Old arrival predating already committed newer start remains blocked | 1 | 1 | 2026-09-29 12:10:00.000 | - | PASS |
| Separate history deliveries start then close | 1 | 2 | 2026-09-29 10:40:00.000 | 2026-09-29 12:00:00.000 | PASS |
| All matching previously opened trips close | 1 | 2 | 2026-09-29 10:40:00.000 | 2026-09-29 12:00:00.000 | PASS |
| Other open trip stays open while independent historical journey starts and ends | 1 | 1 | 2026-09-29 10:40:00.000 | - | PASS |
| Finish old trip; missing-state next trip starts independently at the real GPS time | 1 | 2 | 2026-09-29 10:40:00.000 | 2026-09-29 12:00:00.000 | PASS |
| Invalid GPS fix rejected without queued SQL | 1 | 0 | - | - | PASS |
| Source-only history stays pending | 2 | 0 | - | - | PASS |
| Historical departure starts at GPS time | 2 | 1 | 2026-09-28 10:40:00.000 | - | PASS |
| Whole offline journey starts and ends at historical times | 2 | 2 | 2026-09-28 10:40:00.000 | 2026-09-28 12:00:00.000 | PASS |
| Already started vehicle closes on delayed destination history | 2 | 2 | 2026-09-28 10:40:00.000 | 2026-09-28 12:00:00.000 | PASS |
| Reverse packet delivery sorts before writes | 2 | 2 | 2026-09-28 10:40:00.000 | 2026-09-28 12:00:00.000 | PASS |
| Arrival after ETA still completes | 2 | 2 | 2026-09-28 10:40:00.000 | 2026-09-28 13:20:00.000 | PASS |
| Duplicate live/history redelivery does not repeat writes | 2 | 2 | 2026-09-28 10:40:00.000 | 2026-09-28 12:00:00.000 | PASS |
| Packets at or before lifting do not start | 2 | 0 | - | - | PASS |
| Destination before/equal start does not close | 2 | 1 | 2026-09-28 10:40:00.000 | - | PASS |
| No destination evidence leaves started trip open | 2 | 1 | 2026-09-28 10:40:00.000 | - | PASS |
| MySQL outage retains packets; recovery completes journey | 2 | 2 | 2026-09-28 10:40:00.000 | 2026-09-28 12:00:00.000 | PASS |
| Lost commit response reconciles exact historical writes | 2 | 2 | 2026-09-28 10:40:00.000 | 2026-09-28 12:00:00.000 | PASS |
| New trip discovered after history ingestion replays journey | 2 | 2 | 2026-09-28 10:40:00.000 | 2026-09-28 12:00:00.000 | PASS |
| Source observed, destination point cannot both start and end | 2 | 2 | 2026-09-28 12:00:00.000 | 2026-09-28 12:01:00.000 | PASS |
| Old arrival predating already committed newer start remains blocked | 2 | 1 | 2026-09-28 12:10:00.000 | - | PASS |
| Separate history deliveries start then close | 2 | 2 | 2026-09-28 10:40:00.000 | 2026-09-28 12:00:00.000 | PASS |
| All matching previously opened trips close | 2 | 2 | 2026-09-28 10:40:00.000 | 2026-09-28 12:00:00.000 | PASS |
| Other open trip stays open while independent historical journey starts and ends | 2 | 1 | 2026-09-28 10:40:00.000 | - | PASS |
| Finish old trip; missing-state next trip starts independently at the real GPS time | 2 | 2 | 2026-09-28 10:40:00.000 | 2026-09-28 12:00:00.000 | PASS |
| Invalid GPS fix rejected without queued SQL | 2 | 0 | - | - | PASS |

Status: 0 pending, 1 started, 2 completed. Existing state requires source crossing evidence. A newly introduced pending trip with absent Redis state assumes source IN at lifting, then starts on actual OUT GPS after lifting.

PASS means the expected result occurred, including scenarios that must remain open or pending. No arrival is inferred without GPS evidence. A destination packet at/before the saved start cannot close a trip. Sorting applies to currently queued packets; arbitrarily late evidence does not rewrite committed timestamps.

Reproduce: npm ci; python3 -m pip install "fakeredis[lua]"; python3 scripts/run-redis-emulator-check.py
