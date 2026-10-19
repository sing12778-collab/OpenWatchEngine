# OpenWatch Engine

A small Python monitoring core for turning complete snapshots into durable change
events. Intended for release feeds, public data catalogs, announcements, and
operator-owned inventories.

**Status: 0.2.0 local alpha.** This is a working reliability primitive, not yet a
production monitoring service. Python 3.11+, standard library runtime only.

## Why this exists

A failed scan must not look like an empty catalog. A notification outage must not
erase a detected change. Restarting a monitor must not announce the whole feed again.
OpenWatch commits the baseline and its events in the same SQLite transaction, then
tracks delivery separately for every configured sink.

## Try it without installation

From this directory:

```console
python -m unittest discover -s tests -v
python -m openwatch demo
python -m openwatch scan examples/releases.json --source releases --db local.db
```

The demo uses a temporary database: first observation creates a silent baseline;
a version change creates one event; replaying the same version creates none.
It prints the event to the terminal and removes the temporary data on exit.
The scan command records events only; it does not configure delivery recipients.
To install the CLI, run `python -m pip install -e .`.

## Monitor HTTP JSON

A foreground runner now supports bounded HTTP JSON reads, conditional requests,
and rate-limit-aware scheduling. See [the HTTP runner guide](docs/http-runner.md)
for a loopback-only demonstration and exact limitations.

```console
python -m openwatch watch http://127.0.0.1:8765/releases.json --source releases --db local.db --interval 5 --cycles 3
```

The endpoint must publish the OpenWatch complete-snapshot contract. This is not
an adapter for arbitrary HTML or an arbitrary public JSON API.

## Extend it

Implement `Source.scan() -> Snapshot` and `Sink.send(event) -> None`.
See [the contracts](docs/architecture.md). The JSON source and console sink provide
an offline reference. An item has a stable key and a JSON object containing only
meaningful fields. Exclude volatile timestamps if they should not trigger changes.

```python
from openwatch import Engine
from openwatch.adapters import JsonFileSource, ConsoleSink
import time

engine = Engine("local.db")
try:
    engine.scan("releases", JsonFileSource("examples/releases.json"), ("console",))
    engine.dispatch({"console": ConsoleSink()}, now=time.time())
finally:
    engine.close()
```

## Reliability boundaries

- Initial complete snapshot is silent; later additions, changes and removals emit events.
- Incomplete snapshots, source exceptions and invalid data preserve the baseline.
- A complete empty snapshot explicitly removes every item in that source scope.
- Delivery uses capped exponential retry, persists across restarts, and is at-least-once.
  A crash after remote acceptance and before local acknowledgement can duplicate delivery.
  Sinks should use the event ID as their idempotency key where supported.
- Run only one dispatcher per database. Concurrent dispatchers are not supported.
- Notifications are not guaranteed in event creation order; failed events can be overtaken.
- Configure sinks before scanning: recipients are captured per event, with no backfill.
- Events are retained indefinitely in this alpha. Plan storage and retention before deployment.
- Source adapters are trusted code. The JSON adapter does not contact websites.

## Project direction

The differentiator is reproducible failure handling, not site-specific scraping.
Implemented locally: HTTP conditional fetch, response bounds, socket timeouts,
and foreground scheduling with Retry-After and jitter.
Next: persistent cooldown, source configuration binding, inspect/replay tools,
retention, delivery leases, two independent integrations and external-user feedback.

See [roadmap and sponsorship evidence](docs/roadmap.md),
[contributing](CONTRIBUTING.md), and [security](SECURITY.md).
No existing monitor, account, browser profile, or notification credentials are required.

## Local acknowledgement recovery

Reuse one long-lived Engine for dispatch retries. A successful remote send whose
SQLite acknowledgement fails is remembered only by that Engine instance.
`pending_acknowledgements` counts these in-memory receipts. `dispatch()` may raise
`DeliveryStorageError` after a remote send; its sanitized counts describe partial
progress. Do not interpret an exception as proof that nothing was delivered.

Pending receipts are repaired before any new batch. If any remain unrepairable,
all new dispatch work pauses, including healthy sinks. This conservative alpha
policy bounds memory to one selected batch; a permanently invalid row requires
operator repair. Identified row constraint failures allow the remaining selected
rows to run. Global or unidentified database faults stop additional sends at once.
Each database operation can still wait for the configured SQLite busy timeout.

Closing the Engine does not attempt a potentially blocking repair. Closing,
restarting or crashing discards in-memory receipts and can repeat accepted sends.
The watch CLI warns on exit with pending receipts. Library callers must inspect
the property before closing. Sink-side event-ID deduplication remains necessary.
