'use strict';
// Only bounded, selected fields are exported. Never serialize SDK errors, tokens,
// query text, response bodies, headers or stack traces into Redis or logs.
function redact(value, cfg = {}) {
  let text = String(value ?? '');
  for (const secret of [cfg.influx?.token, cfg.cursorSecret].filter(Boolean)) {
    for (const form of [String(secret), encodeURIComponent(String(secret))])
      text = text.split(form).join('[REDACTED]');
  }
  return text
    .replace(/((?:authorization|token|password|secret)\s*[=:]\s*["']?)(?:Token\s+|Bearer\s+)?[^\s,"'\r\n}]+/gi, '$1[REDACTED]')
    .replace(/\b(?:Bearer|Token)\s+[^\s,"'\r\n}]+/gi, 'Token [REDACTED]')
    .replace(/(https?:\/\/)[^\s/@]+:[^\s/@]+@/gi, '$1[REDACTED]@')
    .replace(/[A-Za-z0-9_+\/-]{60,}={0,2}/g, '[REDACTED_LONG_VALUE]')
    .replace(/[\x00-\x08\x0b-\x1f]/g, ' ').slice(0, 1500);
}
function errorDetails(error, cfg = {}) {
  const status = error?.status ?? error?.statusCode;
  const info = {
    name: redact(error?.name || 'Error', cfg),
    code: error?.code == null ? null : redact(error.code, cfg),
    status: status != null && Number.isInteger(Number(status)) ? Number(status) : null,
    message: redact(error?.message || 'Unknown history error', cfg)
  };
  if (['candidates', 'details', 'materialize'].includes(error?.query_phase)) info.query_phase = error.query_phase;
  const causes = [], visited = new Set([error]);
  let cause = error?.cause;
  while (cause && !visited.has(cause) && causes.length < 3) {
    visited.add(cause);
    causes.push({ name: redact(cause.name || 'Error', cfg),
      code: cause.code == null ? null : redact(cause.code, cfg), message: redact(cause.message || '', cfg) });
    cause = cause.cause;
  }
  if (causes.length) info.causes = causes;
  return info;
}
function safeConfig(cfg) {
  return { enabled: cfg.enabled, url: redact(cfg.influx?.url, cfg), org: redact(cfg.influx?.org, cfg),
    bucket: redact(cfg.influx?.bucket, cfg), measurement: redact(cfg.influx?.measurement, cfg),
    token_configured: !!String(cfg.influx?.token || '').trim(), page_size: cfg.pageSize,
    timeout_ms: cfg.timeoutMs, window_ms: cfg.windowMs };
}
module.exports = { redact, errorDetails, safeConfig };
