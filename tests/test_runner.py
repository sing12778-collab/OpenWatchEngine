import tempfile
import threading
import unittest
from pathlib import Path

from openwatch import Engine, Item, Snapshot
from openwatch.http import HttpFailure
from openwatch.runner import Runner


class Clock:
    value = 0
    def __call__(self):
        return self.value


class Source:
    error = None
    value = 1
    def scan(self):
        if self.error is not None:
            raise self.error
        return Snapshot((Item("x", {"v": self.value}),))


class Sink:
    failed = True
    calls = 0
    def send(self, event):
        self.calls += 1
        if self.failed:
            raise OSError("secret")


class RunnerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.engine = Engine(str(Path(self.temp.name) / "state.db"))
        self.clock, self.source = Clock(), Source()
        self.runner = Runner(self.engine, "feed", self.source, {}, interval=10,
                             jitter=0, monotonic=self.clock, wall=self.clock)
    def tearDown(self):
        self.engine.close()
        self.temp.cleanup()
    def test_success_schedule_no_overlap(self):
        self.assertEqual(self.runner.tick()["scan"], "ok")
        self.assertNotIn("scan", self.runner.tick())
        self.clock.value = 10
        self.source.value = 2
        self.assertEqual(self.runner.tick()["events_created"], 1)
        self.assertEqual(self.runner.due, 20)
    def test_retry_after_is_minimum_and_failure_does_not_advance(self):
        self.runner.tick()
        self.clock.value = 10
        self.source.error = HttpFailure(429, 120)
        self.assertEqual(self.runner.tick()["next_scan_in"], 120)
        self.clock.value = 129
        self.assertNotIn("scan", self.runner.tick())
        self.clock.value = 130
        self.source.error, self.source.value = None, 2
        self.assertEqual(self.runner.tick()["events_created"], 1)
        self.assertEqual(self.runner.failures, 0)
    def test_backoff_jitter_and_redacted_errors(self):
        self.source.error = OSError("sensitive URL")
        self.runner.jitter, self.runner.random_value = 0.2, lambda: 0.5
        for delay in (11, 11, 11, 17.6):
            result = self.runner.tick()
            self.assertAlmostEqual(result["next_scan_in"], delay)
            self.assertNotIn("sensitive", str(result))
            self.clock.value = self.runner.due
    def test_deliveries_drain_while_scan_waits(self):
        sink = Sink()
        self.runner.sinks = {"a": sink}
        self.runner.tick()
        self.source.value, self.clock.value = 2, 10
        self.assertEqual(self.runner.tick()["delivery"]["failed"], 1)
        sink.failed, self.clock.value = False, 12
        result = self.runner.tick()
        self.assertNotIn("scan", result)
        self.assertEqual(result["delivery"]["sent"], 1)
        self.assertEqual(sink.calls, 2)
    def test_stop_and_bounded_cycles(self):
        stopped = threading.Event()
        stopped.set()
        self.runner.run(stop=stopped)
        self.assertEqual(self.runner.cycles, 0)
        reports = []
        self.runner.run(cycles=1, report=reports.append)
        self.assertEqual(self.runner.cycles, 1)
        self.assertEqual(len(reports), 1)
    def test_invalid_configuration(self):
        for interval in (0, -1, float("nan"), float("inf")):
            with self.assertRaises(ValueError):
                Runner(self.engine, "feed", self.source, {}, interval=interval)

    def test_interval_starts_after_scan_completion(self):
        clock = self.clock
        class SlowSource:
            def scan(self):
                clock.value += 7
                return Snapshot(())
        self.runner.source = SlowSource()
        self.runner.tick()
        self.assertEqual(self.runner.due, 17)
