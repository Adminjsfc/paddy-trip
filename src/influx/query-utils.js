'use strict';
const EARTH_M=6371008.8;
const RAD=Math.PI/180;
class ApiError extends Error { constructor(status,message) { super(message); this.status=status; } }

function boundingBox(lat, lon, radius) {
  const a = radius / EARTH_M;
  const lo = Math.max(-90, lat - a / RAD), hi = Math.min(90, lat + a / RAD);
  // Small outward epsilon makes the box conservative at floating-point boundaries.
  const b = { minLat: Math.max(-90, lo - 1e-9), maxLat: Math.min(90, hi + 1e-9), allLon: lo <= -90 || hi >= 90 };
  if (b.allLon) return b;
  const delta = Math.asin(Math.min(1, Math.sin(a) / Math.cos(lat * RAD))) / RAD + 1e-9;
  const wrap = v => ((v + 540) % 360) - 180;
  return { ...b, minLon: wrap(lon - delta), maxLon: wrap(lon + delta) };
}

function collect(queryApi, query, signal, maxRows) {
  return new Promise((resolve, reject) => {
    let cancel, done = false;
    const rows = [];
    const finish = (err) => {
      if (done) return;
      done = true;
      signal?.removeEventListener('abort', abort);
      if (err) reject(err); else resolve(rows);
    };
    const abort = () => { finish(signal.reason || new ApiError(499, 'Client disconnected')); cancel?.cancel(); };
    if (signal?.aborted) return abort();
    signal?.addEventListener('abort', abort, { once: true });
    try {
      queryApi.queryRows(query, {
        useCancellable(c) { cancel = c; if (done) c.cancel(); },
        next(values, tableMeta) {
          if (done) return;
          try {
            if (rows.length >= maxRows) throw new ApiError(502, 'Unexpected number of database rows');
            rows.push(tableMeta.toObject(values));
          } catch (err) { finish(err); cancel?.cancel(); }
        },
        error: finish,
        complete() { finish(); }
      });
    } catch (err) { finish(err); }
  });
}

function haversine(lat1, lon1, lat2, lon2) {
  const s1 = Math.sin((lat2 - lat1) * RAD / 2), s2 = Math.sin((lon2 - lon1) * RAD / 2);
  const a = s1 * s1 + Math.cos(lat1 * RAD) * Math.cos(lat2 * RAD) * s2 * s2;
  return 2 * EARTH_M * Math.asin(Math.sqrt(Math.min(1, Math.max(0, a))));
}

module.exports={ApiError,EARTH_M,RAD,boundingBox,collect,haversine};
