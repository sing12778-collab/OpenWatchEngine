# Contracts and failure semantics

Source -> validated complete Snapshot -> SQLite baseline + events transaction
-> per-sink delivery rows -> single dispatcher -> Sink acknowledgement.

Source names identify stable authoritative scopes. Never reuse a name for a
different filter or account. Item keys must be nonempty unique strings. Values
must be JSON objects; canonical serialization makes object-key order irrelevant.
Sources must explicitly report incomplete pagination or uncertain parsing with
complete=False or an exception. A syntactically valid response alone does not
prove a complete snapshot.

The JSON parser rejects duplicate object keys at every depth and nonstandard
NaN/Infinity tokens. Item keys and serialized values must encode as UTF-8 before
the baseline or HTTP validators can advance. Python adapters must return Items
whose values use JSON objects with string keys, arrays as lists, finite numbers,
strings, booleans and null. Tuples, sets and arbitrary objects inside values are
rejected rather than silently coerced. Shared subobjects are supported; cycles
are rejected. Malformed snapshot shapes, unsupported types, invalid Unicode
and excessive parser/serialization depth raise ValueError. There is no promise
of unlimited nesting, arbitrary input size, thread safety or validation sandboxing.

Events include id, source, revision, key, kind, before and after. Revision increments
for every accepted scan. Event IDs derive from source, revision and key; an A->B->A
transition remains observable. IDs are stable for retries within the same database,
not globally stable across separate databases.

BEGIN IMMEDIATE serializes baseline comparison and event creation. Failure rolls
back baseline, revision, events and delivery rows together. Source collection takes
place before the transaction; operators must serialize scans for each source to
avoid committing stale overlapping responses.

Sink failures do not roll back successful scans. Missing sinks remain pending.
Successful recipients are not retried. Failed attempts wait 2, 4, 8 ... 3600 seconds;
the caller supplies Unix time and is responsible for invoking dispatch repeatedly.
Sink calls must implement their own timeouts. The dispatcher has no delivery
lease, TTL, dead-letter queue or exactly-once guarantee. The separate foreground
Runner schedules scans and drains deliveries; see [HTTP runner](http-runner.md).

Data is not encrypted. Event payloads include old and new values: redact secrets
and personal data before creating Items. Do not publish databases or operational
snapshots. Do not add login, cart, purchase, or account mutations to the core.

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
