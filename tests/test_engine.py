import tempfile
import unittest
from pathlib import Path
from openwatch import Engine, Item, Snapshot


class Replay:
    def __init__(self, items=(), complete=True):
        self.result = Snapshot(tuple(Item(k, v) for k, v in items), complete)
    def scan(self):
        return self.result


class Collector:
    def __init__(self, fail=False):
        self.fail, self.events = fail, []
    def send(self, event):
        if self.fail:
            raise OSError("simulated failure")
        self.events.append(event)


class EngineTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = str(Path(self.temp.name) / "state.db")
        self.engine = Engine(self.path)
    def tearDown(self):
        self.engine.close()
        self.temp.cleanup()
    def scan(self, value, sinks=("a",), name="feed"):
        return self.engine.scan(name, Replay([("x", {"v": value})]), sinks)
    def test_silent_baseline_and_unchanged(self):
        self.assertEqual(self.scan(1), 0)
        self.assertEqual(self.scan(1), 0)
    def test_change_and_restart(self):
        self.scan(1)
        self.assertEqual(self.scan(2), 1)
        self.engine.close()
        self.engine = Engine(self.path)
        self.assertEqual(self.scan(2), 0)
        sink = Collector()
        self.assertEqual(self.engine.dispatch({"a": sink}, 0)["sent"], 1)
        self.assertEqual(sink.events[0]["before"], {"v": 1})
    def test_partial_does_not_advance(self):
        self.scan(1)
        with self.assertRaises(ValueError):
            self.engine.scan("feed", Replay([("x", {"v": 2})], False))
        self.assertEqual(self.scan(2), 1)
    def test_exception_does_not_advance(self):
        self.scan(1)
        class Broken:
            def scan(self):
                raise TimeoutError()
        with self.assertRaises(TimeoutError):
            self.engine.scan("feed", Broken())
        self.assertEqual(self.scan(2), 1)
    def test_retry_independent_recipients(self):
        self.scan(1, ("a", "b"))
        self.scan(2, ("a", "b"))
        a, b = Collector(), Collector(True)
        self.assertEqual(self.engine.dispatch({"a": a, "b": b}, 0),
                         {"sent": 1, "failed": 1, "unavailable": 0})
        b.fail = False
        self.assertEqual(self.engine.dispatch({"a": a, "b": b}, 1)["sent"], 0)
        self.assertEqual(self.engine.dispatch({"a": a, "b": b}, 2)["sent"], 1)
        self.assertEqual(len(a.events), 1)
        self.assertEqual(a.events[0]["id"], b.events[0]["id"])
    def test_duplicate_keys_rejected(self):
        self.scan(1)
        with self.assertRaises(ValueError):
            self.engine.scan("feed", Replay([("x", {}), ("x", {})]))
        self.assertEqual(self.scan(2), 1)
    def test_removal_and_reappearance(self):
        self.scan(1)
        self.assertEqual(self.engine.scan("feed", Replay(), ("a",)), 1)
        self.assertEqual(self.scan(1), 1)
        sink = Collector()
        self.engine.dispatch({"a": sink}, 0)
        self.assertEqual({e["kind"] for e in sink.events}, {"removed", "added"})
        self.assertEqual(len({e["id"] for e in sink.events}), 2)
    def test_source_isolation(self):
        self.scan(1)
        self.assertEqual(self.scan(2, name="other"), 0)
        self.assertEqual(self.scan(1), 0)
    def test_serialization_failure_preserves_baseline(self):
        self.scan(1)
        with self.assertRaises(ValueError):
            self.scan(float("nan"))
        self.assertEqual(self.scan(2), 1)
    def test_unknown_sink_preserved(self):
        self.scan(1)
        self.scan(2)
        self.assertEqual(self.engine.dispatch({}, 0)["unavailable"], 1)
        self.assertEqual(self.engine.dispatch({"a": Collector()}, 0)["sent"], 1)
    def test_state_and_event_rollback_together(self):
        self.scan(1)
        self.engine.db.execute("""
            CREATE TRIGGER reject_update BEFORE INSERT ON items
            WHEN NEW.value='{"v":2}' BEGIN SELECT RAISE(ABORT,'test'); END
        """)
        with self.assertRaises(Exception):
            self.scan(2)
        self.assertEqual(self.engine.db.execute("SELECT count(*) FROM events").fetchone()[0], 0)
        self.engine.db.execute("DROP TRIGGER reject_update")
        self.assertEqual(self.scan(2), 1)


if __name__ == "__main__":
    unittest.main()
