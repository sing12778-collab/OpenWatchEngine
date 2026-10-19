"""Synthetic persistent acknowledgement failures must not repeatedly send."""
import sqlite3
import tempfile
import unittest
from pathlib import Path
from openwatch import Engine, Item, Snapshot
from openwatch.runner import Runner


class Source:
    def __init__(self, version, count=2): self.version, self.count = version, count
    def scan(self): return Snapshot(tuple(Item(f'k{i}', {'v': self.version}) for i in range(self.count)))


class Sink:
    def __init__(self): self.keys = []
    def send(self, event): self.keys.append(event['key'])


class Clock:
    value = 0
    def __call__(self): return self.value


class AckRecoveryTests(unittest.TestCase):
    def create_events(self, engine, count=2):
        engine.scan('feed', Source(0, count), ('sink',))
        engine.scan('feed', Source(1, count), ('sink',))

    def test_persistent_ack_failure_sends_each_row_once_until_repair(self):
        engine, sink, clock = Engine(':memory:'), Sink(), Clock()
        try:
            self.create_events(engine)
            engine.db.execute("CREATE TRIGGER reject_ack BEFORE UPDATE ON deliveries BEGIN SELECT RAISE(ABORT,'synthetic'); END")
            runner = Runner(engine, 'feed', Source(1), {'sink': sink}, monotonic=clock, wall=clock)
            runner.due = 1e9
            for moment in (0, 2, 6, 14):
                clock.value = moment
                report = runner.tick()
                self.assertEqual(report['pending_acknowledgements'], 2)
                self.assertEqual(report['delivery']['sent'], 0)
            self.assertEqual(sorted(sink.keys), ['k0', 'k1'])
            engine.db.execute('DROP TRIGGER reject_ack')
            clock.value = 30
            self.assertEqual(runner.tick()['delivery']['sent'], 2)
            self.assertEqual(len(sink.keys), 2)
            self.assertEqual(engine.db.execute('SELECT count(*) FROM deliveries WHERE sent=0').fetchone()[0], 0)
        finally: engine.close()

    def test_one_bad_ack_does_not_abort_other_rows_and_counts_are_partial(self):
        engine, sink = Engine(':memory:'), Sink()
        try:
            self.create_events(engine)
            first_id = engine.db.execute('SELECT id FROM events ORDER BY id LIMIT 1').fetchone()[0]
            # Hash is synthetic; use a trigger predicate fixed to this generated ID.
            engine.db.execute(f"CREATE TRIGGER reject_one BEFORE UPDATE ON deliveries WHEN OLD.event_id='{first_id}' BEGIN SELECT RAISE(ABORT,'synthetic'); END")
            with self.assertRaises(sqlite3.Error) as failure:
                engine.dispatch({'sink': sink}, 0)
            self.assertEqual(failure.exception.counts, {'sent': 1, 'failed': 0, 'unavailable': 0})
            self.assertEqual(failure.exception.pending_acknowledgements, 1)
            self.assertEqual(len(sink.keys), 2)
            with self.assertRaises(sqlite3.Error): engine.dispatch({'sink': sink}, 10)
            self.assertEqual(len(sink.keys), 2)
        finally: engine.close()

    def test_failed_sink_and_failed_retry_write_keep_partial_counts(self):
        class FailingSink:
            def send(self, event): raise OSError('synthetic')
        engine = Engine(':memory:')
        try:
            self.create_events(engine, count=1)
            engine.db.execute("CREATE TRIGGER reject_retry BEFORE UPDATE ON deliveries BEGIN SELECT RAISE(ABORT,'synthetic'); END")
            with self.assertRaises(sqlite3.Error) as failure:
                engine.dispatch({'sink': FailingSink()}, 0)
            self.assertEqual(failure.exception.counts['failed'], 1)
            self.assertEqual(failure.exception.pending_acknowledgements, 0)
        finally: engine.close()

    def test_programming_error_is_terminal_not_an_infinite_retry(self):
        engine = Engine(':memory:')
        runner = Runner(engine, 'feed', Source(0), {}, monotonic=lambda: 0, wall=lambda: 0)
        runner.due = 1e9
        engine.close()
        with self.assertRaises(sqlite3.ProgrammingError): runner.tick()

    def test_restart_still_has_documented_at_least_once_window(self):
        with tempfile.TemporaryDirectory() as folder:
            database = str(Path(folder) / 'synthetic.db')
            engine, sink = Engine(database), Sink()
            try:
                self.create_events(engine, count=1)
                engine.db.execute("CREATE TRIGGER reject_ack BEFORE UPDATE ON deliveries BEGIN SELECT RAISE(ABORT,'synthetic'); END")
                with self.assertRaises(sqlite3.Error): engine.dispatch({'sink': sink}, 0)
                self.assertEqual(len(sink.keys), 1)
            finally: engine.close()
            engine = Engine(database)
            try:
                engine.db.execute('DROP TRIGGER reject_ack')
                self.assertEqual(engine.dispatch({'sink': sink}, 0)['sent'], 1)
                self.assertEqual(len(sink.keys), 2)
            finally: engine.close()
