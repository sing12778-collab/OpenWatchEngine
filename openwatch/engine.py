from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass
from typing import Any, Protocol


def canonical(value: Any) -> str:
    try:
        result = json.dumps(value, ensure_ascii=False, sort_keys=True,
                            separators=(",", ":"), allow_nan=False)
        # SQLite TEXT and event identities must be encodable before publication.
        result.encode("utf-8")
        return result
    except (TypeError, ValueError, OverflowError, RecursionError):
        raise ValueError("value must be valid UTF-8 JSON") from None


def validate_json_value(value: Any) -> None:
    """Check the JSON type contract iteratively, without reducing supported depth."""
    pending, seen = [value], set()
    while pending:
        node = pending.pop()
        if isinstance(node, (dict, list)):
            identity = id(node)
            if identity in seen:
                # Shared objects are valid; canonical() separately rejects cycles.
                continue
            seen.add(identity)
            if isinstance(node, dict):
                if any(not isinstance(key, str) for key in node):
                    raise ValueError("JSON object keys must be strings")
                pending.extend(node.values())
            else:
                pending.extend(node)
        elif node is not None and not isinstance(node, (str, bool, int, float)):
            raise ValueError("value contains unsupported JSON type")


@dataclass(frozen=True)
class Item:
    key: str
    value: dict[str, Any]


@dataclass(frozen=True)
class Snapshot:
    items: tuple[Item, ...]
    complete: bool = True


class Source(Protocol):
    def scan(self) -> Snapshot: ...


class Sink(Protocol):
    def send(self, event: dict[str, Any]) -> None:
        """Return only on acknowledged delivery; raise on failure."""


def validate_snapshot(snapshot: Snapshot) -> dict[str, str]:
    """Validate before state or HTTP validators can advance."""
    if not isinstance(snapshot, Snapshot):
        raise ValueError("source must return a Snapshot")
    if snapshot.complete is not True:
        raise ValueError("incomplete snapshot: baseline preserved")
    if not isinstance(snapshot.items, (tuple, list)):
        raise ValueError("snapshot items must be a tuple or list")
    current = {}
    for item in snapshot.items:
        if not isinstance(item, Item):
            raise ValueError("snapshot entries must be Items")
        if not isinstance(item.key, str) or not item.key or item.key in current:
            raise ValueError("empty or duplicate item key")
        try:
            item.key.encode("utf-8")
        except UnicodeError:
            raise ValueError("item key must be valid UTF-8") from None
        if not isinstance(item.value, dict):
            raise ValueError("item value must be an object")
        validate_json_value(item.value)
        current[item.key] = canonical(item.value)
    return current


def is_terminal_sqlite_error(error: sqlite3.Error) -> bool:
    code = getattr(error, "sqlite_errorcode", 0) or 0
    return (getattr(error, "terminal", False)
            or isinstance(error, (sqlite3.ProgrammingError, sqlite3.NotSupportedError))
            or code & 0xff in (sqlite3.SQLITE_CORRUPT, sqlite3.SQLITE_NOTADB))


def is_global_sqlite_error(error: sqlite3.Error) -> bool:
    # Only an identified constraint violation is local to a selected row.
    # Unknown storage failures are conservatively treated as global.
    code = getattr(error, "sqlite_errorcode", 0) or 0
    return is_terminal_sqlite_error(error) or code & 0xff != sqlite3.SQLITE_CONSTRAINT


class DeliveryStorageError(sqlite3.Error):
    """Sanitized partial progress; never includes the original exception text."""
    def __init__(self, error, counts, pending_acknowledgements):
        super().__init__("delivery state update failed")
        self.error_type = type(error).__name__
        self.counts = dict(counts)
        self.pending_acknowledgements = pending_acknowledgements
        self.terminal = is_terminal_sqlite_error(error)


class Engine:
    """Single dispatcher per database. Sources own snapshot scope and scheduling."""

    def __init__(self, path: str):
        self.db = sqlite3.connect(path, timeout=10)
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS sources(
                name TEXT PRIMARY KEY, revision INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS items(
                source TEXT NOT NULL, key TEXT NOT NULL, value TEXT NOT NULL,
                PRIMARY KEY(source, key));
            CREATE TABLE IF NOT EXISTS events(
                id TEXT PRIMARY KEY, payload TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS deliveries(
                event_id TEXT NOT NULL, sink TEXT NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                next_at REAL NOT NULL DEFAULT 0,
                sent INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY(event_id, sink));
        """)
        self._accepted = {}

    @property
    def pending_acknowledgements(self) -> int:
        return len(self._accepted)

    def close(self) -> None:
        self.db.close()

    def scan(self, name: str, source: Source, sinks: tuple[str, ...] = ()) -> int:
        if not isinstance(name, str) or not name.strip():
            raise ValueError("source name must be nonempty")
        if any(not isinstance(s, str) or not s.strip() for s in sinks):
            raise ValueError("sink names must be nonempty")
        snapshot = source.scan()
        current = validate_snapshot(snapshot)
        # BEGIN IMMEDIATE serializes state comparison and commits events with baseline.
        self.db.execute("BEGIN IMMEDIATE")
        try:
            row = self.db.execute(
                "SELECT revision FROM sources WHERE name=?", (name,)).fetchone()
            old = dict(self.db.execute(
                "SELECT key,value FROM items WHERE source=?", (name,)))
            revision = row[0] + 1 if row else 1
            created = 0
            if row:
                for key in sorted(old.keys() | current.keys()):
                    before, after = old.get(key), current.get(key)
                    if before == after:
                        continue
                    kind = "added" if before is None else "removed" if after is None else "changed"
                    identity = canonical([name, revision, key])
                    event_id = hashlib.sha256(identity.encode()).hexdigest()
                    payload = canonical(dict(
                        id=event_id, source=name, revision=revision, key=key,
                        kind=kind, before=json.loads(before) if before is not None else None,
                        after=json.loads(after) if after is not None else None))
                    self.db.execute("INSERT INTO events VALUES(?,?)", (event_id, payload))
                    for sink in set(sinks):
                        self.db.execute(
                            "INSERT INTO deliveries(event_id,sink) VALUES(?,?)",
                            (event_id, sink))
                    created += 1
            self.db.execute("DELETE FROM items WHERE source=?", (name,))
            self.db.executemany("INSERT INTO items VALUES(?,?,?)",
                                [(name, key, value) for key, value in current.items()])
            self.db.execute("INSERT OR REPLACE INTO sources VALUES(?,?)", (name, revision))
            self.db.commit()
            return created
        except BaseException:
            self.db.rollback()
            raise

    def dispatch(self, sinks: dict[str, Sink], now: float, limit: int = 100) -> dict[str, int]:
        if limit < 1:
            raise ValueError("limit must be positive")
        counts = dict(sent=0, failed=0, unavailable=0)
        storage_error = None
        # Repair acknowledged remote sends before making any further sink calls.
        # This memory is deliberately not an exactly-once or restart guarantee.
        for event_id, name in tuple(self._accepted):
            try:
                counts["sent"] += self._acknowledge(event_id, name)
            except sqlite3.Error as error:
                storage_error = storage_error or error
                if is_global_sqlite_error(error):
                    raise DeliveryStorageError(error, counts, len(self._accepted)) from None
        if storage_error is not None:
            raise DeliveryStorageError(storage_error, counts, len(self._accepted)) from None
        order = "d.next_at,d.event_id,d.sink"
        parameters = [now]
        if sinks:
            # Missing adapters remain pending, but cannot consume the entire batch
            # ahead of recipients that can actually be attempted this dispatch.
            placeholders = ",".join("?" for _ in sinks)
            order = f"CASE WHEN d.sink IN ({placeholders}) THEN 0 ELSE 1 END," + order
            parameters.extend(sinks)
        parameters.append(limit)
        try:
            rows = self.db.execute(f"""
                SELECT d.event_id,d.sink,d.attempts,e.payload
                FROM deliveries d JOIN events e ON e.id=d.event_id
                WHERE d.sent=0 AND d.next_at<=?
                ORDER BY {order} LIMIT ?
            """, parameters).fetchall()
        except sqlite3.Error as error:
            if any(counts.values()):
                raise DeliveryStorageError(error, counts, len(self._accepted)) from None
            raise
        for event_id, name, attempts, payload in rows:
            if name not in sinks:
                counts["unavailable"] += 1
                continue
            try:
                sinks[name].send(json.loads(payload))
            except Exception:
                # Exception messages may contain secrets: persist only retry metadata.
                counts["failed"] += 1
                delay = min(3600, 2 ** min(attempts + 1, 12))
                try:
                    with self.db:
                        self.db.execute("""
                            UPDATE deliveries SET attempts=attempts+1,next_at=?
                            WHERE event_id=? AND sink=?
                        """, (now + delay, event_id, name))
                except sqlite3.Error as error:
                    storage_error = storage_error or error
                    if is_global_sqlite_error(error):
                        raise DeliveryStorageError(error, counts, len(self._accepted)) from None
            else:
                self._accepted[event_id, name] = None
                try:
                    counts["sent"] += self._acknowledge(event_id, name)
                except sqlite3.Error as error:
                    storage_error = storage_error or error
                    if is_global_sqlite_error(error):
                        raise DeliveryStorageError(error, counts, len(self._accepted)) from None
        if storage_error is not None:
            raise DeliveryStorageError(storage_error, counts, len(self._accepted)) from None
        return counts

    def _acknowledge(self, event_id, name):
        with self.db:
            changed = self.db.execute("""
                UPDATE deliveries SET sent=1,attempts=attempts+1
                WHERE event_id=? AND sink=? AND sent=0
            """, (event_id, name)).rowcount
        self._accepted.pop((event_id, name), None)
        return changed
