#!/usr/bin/env bash
set -euo pipefail
# Usage: bash /path/to/extracted/jsfc-trip-service/scripts/install-influx-diagnostics.sh /path/to/running/service
# Copies the UTC index fix and diagnostics; preserves Database.js, both config files,
# .env, ecosystem configuration, Redis contents and installed dependencies.
source_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)
target_dir=$(cd -- "${1:?Pass the existing service directory}" && pwd -P)
if [[ "$source_dir" == "$target_dir" || ! -f "$target_dir/src/Database.js" || ! -f "$target_dir/src/archive-client.js" ]]; then
  echo 'Use a separate extracted folder and an existing JSFC trip service.' >&2
  exit 1
fi
files=(src/archive-diagnostics.js src/archive-client.js src/archive-recovery.js
  src/influx/gps-time.js src/influx/GpsReportService.js src/worker.js src/worker-metrics.js
  scripts/check-worker-history.cjs scripts/diagnose-trip.cjs test/worker-loop.test.js
  test/archive-client.test.js test/archive-recovery.test.js test/influx-service.test.js
  test/worker-history-diagnostics.test.js)
for file in "${files[@]}"; do node --check "$source_dir/$file"; done
backup_dir=$(mktemp -d "$target_dir/influx-diagnostics-backup-XXXXXXXX")
for file in "${files[@]}"; do
  mkdir -p -- "$backup_dir/$(dirname -- "$file")" "${target_dir}/$(dirname -- "$file")"
  if [[ -f "$target_dir/$file" ]]; then
    cp -p -- "$target_dir/$file" "$backup_dir/$file"
  else
    printf '%s\n' "$file" >> "$backup_dir/previously-absent.txt"
  fi
done
for file in "${files[@]}"; do
  tmp_file=$(mktemp "$target_dir/$(dirname -- "$file")/.influx-diagnostics-XXXXXXXX")
  cp -p -- "$source_dir/$file" "$tmp_file"
  mv -- "$tmp_file" "$target_dir/$file"
done
printf 'Installed UTC index fix and diagnostics. Backup: %s\n' "$backup_dir"
printf '%s\n' 'Restart the eight jsfc-trip workers, then run scripts/check-worker-history.cjs.'
