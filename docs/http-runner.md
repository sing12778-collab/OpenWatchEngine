# HTTP snapshot runner (0.2.0 alpha)

The HTTP adapter reads the same authoritative JSON contract as the file adapter:

```json
{"complete":true,"items":[{"key":"library","value":{"version":"1.0"}}]}
```

This is not an arbitrary website scraper or a GitHub API adapter. The producer
must guarantee completeness; partial pagination must never say complete=true.
An explicitly complete empty list means removal of all items in that scope.

## Local demonstration

From the repository root, serve only the synthetic example folder in one terminal:

```console
python -m http.server 8765 --bind 127.0.0.1 --directory examples
```

In another terminal, run a bounded foreground monitor:

```console
python -m openwatch watch http://127.0.0.1:8765/releases.json --source local-releases --db local.db --interval 5 --cycles 3
```

The first scan is silent. Edit the example version while it runs to observe a
console change event. Reusing local.db preserves the baseline and pending delivery.
Omit --cycles for continuous foreground operation; Ctrl+C stops it cleanly.
No background process or OS scheduler is installed. Only the console sink is
configured by this command. Do not run two runners on the same database.

## HTTP behavior

- HTTP(S) GET only, TLS verification enabled by Python defaults.
- No redirects, proxy inheritance, cookies or authentication.
- Socket blocking-operation timeout (default 10s), not a total wall-clock deadline.
  A slow trickle or DNS resolution can take longer; do not claim a hard scan deadline.
- UTF-8 JSON, JSON content type, identity encoding only; default body limit 1 MiB.
- ETag and Last-Modified validators are sent only after complete schema validation.
  HTTP 304 reuses the validated in-memory snapshot; without one it is an error.
- Cache and validators are in memory. Restart fetches an unconditional snapshot,
  reconciled against the durable SQLite baseline. No validator can skip a lost commit.
- The cache stores validated, immutable JSON text. HTTP 200 returns its parsed
  snapshot; every cached 304 reparses the text into new objects. Mutating returned
  nested values cannot alter the private cache. This avoids adding deepcopy's
  lower recursion limit to otherwise supported JSON. Snapshots are not deeply
  immutable; retaining text and reparsing 304 responses have memory/CPU costs.
- Invalid, oversized, truncated or incomplete responses never advance the baseline.
- HTTP 429/503 Retry-After supports seconds and HTTP dates. Other failures back off.
- Scans never overlap. The interval starts after completion; failures use capped
  exponential backoff, with nonnegative jitter and Retry-After as a minimum delay.
- Cooldown is in memory and resets on process restart. Do not restart to evade
  server throttling. Delivery retries continue between scans.
- Runtime status output includes error type/status, not exception text or URLs.
  SQLite still contains complete item values; sanitize them before ingestion.
- A bounded `watch --cycles N` exits with status 1 if it observed any scan,
  sink-send or delivery-database failure during the run, even if a retry recovered.
  Status 0 means no such failure was observed; it does not promise every pending
  delivery was sent. Argument errors use 2, and Ctrl+C uses 130. CLI exceptions
  are sanitized to an error-type JSON object on stderr, without a traceback.
- Delivery-database exceptions are reported with exponential retry (2s to 1h),
  independently of scan scheduling. A known remote acceptance whose local ACK
  fails is retained in process memory: retries first repair those ACKs without
  resending. Other rows in the already-selected batch are still attempted.
  No new batch is sent until outstanding ACKs have been repaired, bounding
  pending in-memory receipts to one requested batch. Restarting loses these
  receipts and may duplicate delivery; ambiguous sink failures can also repeat.
  No exactly-once guarantee is added. Programming errors and corrupt/non-database
  SQLite errors terminate instead of being retried indefinitely.
- `delivery_incomplete=true` means counts cover only the progress observed before
  the storage failure. `sent` counts completed local acknowledgements, including
  repair of a previous send; it is not necessarily the number of new network calls.
  `pending_acknowledgements` counts accepted sends held in this process. Missing
  sink counts cover the selected dispatch window, not the total database backlog.
- Unavailable-sink counts are reported on change and with scan status, rather
  than emitting the same unavailable count every idle tick. Missing adapters
  are never treated as successful delivery or discarded.
- Bounded watch exits after the requested scan attempts, before the next scan's
  cooldown necessarily elapses. Supervisors must respect reported `next_scan_in`
  and must not immediately restart failed/rate-limited bounded jobs. This alpha
  does not install an automatic restart policy or persist cooldown across restarts.

## Verification and remaining work

Tests use an ephemeral loopback HTTP server, synthetic data and injected clocks.
They verify conditional requests, cache rejection, rate limits, response bounds,
timeouts, restart reconciliation, scheduling and delivery during scan cooldown.
They do not prove public endpoint compatibility, hosted CI, scale or production use.

Still needed before beta: persistent cooldown, total request deadlines, source
binding to endpoint/configuration, inspect/replay/retention, delivery leases and
two independently useful integrations.

Protocol references:
- https://docs.python.org/3/library/urllib.request.html
- https://www.rfc-editor.org/rfc/rfc9110.html#name-retry-after
