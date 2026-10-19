"""Synchronous foreground runner with injectable clocks for offline verification."""
from __future__ import annotations

import math
import random
import sqlite3
import time
from threading import Event

from .engine import Engine, Source, Sink, DeliveryStorageError, is_terminal_sqlite_error
from .http import HttpFailure


class Runner:
    def __init__(self, engine: Engine, name: str, source: Source,
                 sinks: dict[str, Sink], *, interval: float = 60,
                 jitter: float = 0.1, monotonic=time.monotonic,
                 wall=time.time, random_value=random.random):
        if not math.isfinite(interval) or interval <= 0:
            raise ValueError("interval must be finite and positive")
        if not math.isfinite(jitter) or not 0 <= jitter <= 1:
            raise ValueError("jitter must be between zero and one")
        self.engine, self.name, self.source, self.sinks = engine, name, source, sinks
        self.interval, self.jitter = interval, jitter
        self.monotonic, self.wall, self.random_value = monotonic, wall, random_value
        self.due, self.failures, self.cycles = monotonic(), 0, 0
        self.scan_errors = self.delivery_errors = 0
        self.delivery_failures = 0
        self.delivery_due = monotonic()

    def tick(self) -> dict:
        report = {}
        if self.monotonic() >= self.due:
            delay = self.interval
            try:
                count = self.engine.scan(self.name, self.source, tuple(self.sinks))
            except Exception as error:
                self.failures += 1
                self.scan_errors += 1
                delay = max(delay, min(3600.0, 2.0 ** min(self.failures, 12)))
                if isinstance(error, HttpFailure) and error.retry_after is not None:
                    delay = max(delay, error.retry_after)
                # Never print exception text: it could contain URLs or secrets.
                report = {"scan": "failed", "error": type(error).__name__}
                if isinstance(error, HttpFailure):
                    report["http_status"] = error.status
            else:
                self.failures = 0
                report = {"scan": "ok", "events_created": count}
            self.cycles += 1
            # Completion-based schedule prevents overlap and catch-up bursts.
            delay += delay * self.jitter * self.random_value()
            self.due = self.monotonic() + delay
            report["next_scan_in"] = delay
        report["delivery"] = {"sent": 0, "failed": 0, "unavailable": 0}
        if self.monotonic() < self.delivery_due:
            report["delivery_backoff"] = self.delivery_due - self.monotonic()
            return report
        try:
            report["delivery"] = self.engine.dispatch(self.sinks, self.wall())
        except sqlite3.Error as error:
            if is_terminal_sqlite_error(error):
                raise
            self.delivery_errors += 1
            self.delivery_failures += 1
            delay = min(3600, 2 ** min(self.delivery_failures, 12))
            self.delivery_due = self.monotonic() + delay
            if isinstance(error, DeliveryStorageError):
                report["delivery"] = error.counts
                report["pending_acknowledgements"] = error.pending_acknowledgements
                self.delivery_errors += error.counts["failed"]
            report.update(delivery_error=getattr(error, "error_type", type(error).__name__), delivery_retry_in=delay,
                          delivery_incomplete=True)
        else:
            self.delivery_failures = 0
            self.delivery_due = self.monotonic()
            self.delivery_errors += report["delivery"]["failed"]
        return report

    def run(self, *, cycles: int = 0, stop: Event | None = None, report=print):
        if type(cycles) is not int or cycles < 0:
            raise ValueError("cycles must be a nonnegative integer")
        stop = stop if stop is not None else Event()
        started = self.cycles
        previous_unavailable = None
        while not stop.is_set():
            result = self.tick()
            attempted = "delivery_backoff" not in result and "delivery_error" not in result
            unavailable = result["delivery"]["unavailable"]
            changed = attempted and unavailable != previous_unavailable and (
                unavailable > 0 or previous_unavailable not in (None, 0))
            if ("scan" in result or result["delivery"]["sent"] or result["delivery"]["failed"]
                    or "delivery_error" in result or changed):
                report(result)
            if attempted:
                previous_unavailable = unavailable
            if cycles and self.cycles - started >= cycles:
                return
            # Remain responsive to shutdown and drain delivery retries between scans.
            stop.wait(min(1.0, max(0.01, self.due - self.monotonic())))
