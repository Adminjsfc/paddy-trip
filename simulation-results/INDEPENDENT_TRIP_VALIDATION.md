# Independent trip validation — 2 October 2026

Build: `2026-10-02-independent-trips`.

- 97 unit/repository tests passed.
- Redis queue/Lua checks passed.
- 29 live, 40 offline, 12 start-fix and 8 independent-trip simulations passed (89 total).

The reported vehicle JH02AK7064 has trip 23398 started and trip 23808 pending, sharing a source with different destinations. Simulation of the supplied geometry and timestamps recovers 23808 with start 2026-10-02 08:33:10 IST and completion 2026-10-02 09:56:02 IST. It leaves 23398 started and deletes only 23808’s Redis state. Actual departure packet coordinates were not supplied; representative source/exit coordinates were used in this test.

Other checks: two pending trips start on the same exit, per-trip duplicate markers, legacy vehicle-wide markers do not block a sibling trip, a trip discovered later can use retained history independently, stored crossing evidence plus a real exit packet can recover missing source history, missing exit packets do not fabricate a start, GPS timestamps are retained across outage recovery, and lost COMMIT response retries only fixed targets.

MySQL is a simulated transactional adapter; Redis uses a local emulator. MQTT packets invoke the real parser directly without a broker connection. No production database, Redis or MQTT server was accessed. Reports show expected behavior and do not assert that recovery has already happened on the user’s server.
