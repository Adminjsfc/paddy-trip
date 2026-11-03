# Earlier start fix validation — 2 October 2026

Historical report for the previous build. See INDEPENDENT_TRIP_VALIDATION.md for the current per-trip behavior and validation.

Build: `2026-10-02-geofence-fix`.

- 90 unit/repository tests passed.
- Redis inbox, lease, journal, acknowledgment and cleanup checks passed.
- 29 live simulations passed.
- 40 offline simulations passed (one-day and two-day histories).
- 12 start-fix regressions passed; detailed outcomes in `start-fix-results.json`.

Three silent blockers were reproduced before changing the code: equivalent numeric/string geometry discarded source-IN; an unrelated earlier pending trip blocked a valid candidate; missing ETA duration blocked a valid source departure. The corrected service uses kilometres, preserves equivalent geometry, selects the earliest eligible trip and separates ETA availability from starting.

The targeted integration scenarios use the real MQTT parser and Redis/Lua store. They include the supplied 27-metre source point, individual-key-only state, a stale hash copy, pre-lifting source evidence, out-of-order history, worker recreation, database outage, lost COMMIT response and duplicate packets. They verify GPS-based SQL values and Redis removal after confirmed completion.

MySQL uses a simulated transaction adapter. MQTT packets invoke the parser directly, not a running broker. Redis uses a local emulator. No production database, broker or Redis was contacted. Existing committed trip timestamps are not corrected retroactively.
