import sqlite3
import unittest
from unittest.mock import patch
from openwatch.engine import Engine, Item, Snapshot, DeliveryStorageError


class Source:
    def __init__(self, value): self.value = value
    def scan(self): return Snapshot(tuple(Item(str(i), {'v': self.value}) for i in range(3)))


class Sink:
    def __init__(self): self.calls = 0
    def send(self, event): self.calls += 1


class GlobalStorageFailureTests(unittest.TestCase):
    def test_global_ack_fault_stops_new_sends_and_repair_attempts(self):
        for code in (sqlite3.SQLITE_BUSY, sqlite3.SQLITE_READONLY, sqlite3.SQLITE_CORRUPT, 0):
            with self.subTest(code=code):
                engine, sink = Engine(':memory:'), Sink()
                try:
                    engine.scan('x', Source(0), ('sink',))
                    engine.scan('x', Source(1), ('sink',))
                    error = sqlite3.OperationalError('synthetic private detail')
                    error.sqlite_errorcode = code
                    with patch.object(engine, '_acknowledge', side_effect=error) as ack:
                        with self.assertRaises(DeliveryStorageError) as failure:
                            engine.dispatch({'sink': sink}, 0)
                        self.assertEqual(sink.calls, 1)
                        self.assertEqual(engine.pending_acknowledgements, 1)
                        self.assertNotIn('private', str(failure.exception))
                        self.assertEqual(failure.exception.terminal, code == sqlite3.SQLITE_CORRUPT)
                        ack.reset_mock()
                        with self.assertRaises(DeliveryStorageError): engine.dispatch({'sink': sink}, 2)
                        self.assertEqual(ack.call_count, 1)
                        self.assertEqual(sink.calls, 1)
                    self.assertEqual(engine.dispatch({'sink': sink}, 4)['sent'], 3)
                    self.assertEqual(sink.calls, 3)
                finally: engine.close()

    def test_successful_repair_progress_survives_select_failure(self):
        engine = Engine(':memory:')
        try:
            engine._accepted['synthetic', 'sink'] = None
            class BrokenDatabase:
                def execute(self, *args): raise sqlite3.OperationalError('synthetic')
            original = engine.db
            engine.db = BrokenDatabase()
            def repair(*args):
                engine._accepted.clear()
                return 1
            try:
                with patch.object(engine, '_acknowledge', side_effect=repair):
                    with self.assertRaises(DeliveryStorageError) as failure:
                        engine.dispatch({}, 0)
                self.assertEqual(failure.exception.counts['sent'], 1)
                self.assertEqual(failure.exception.pending_acknowledgements, 0)
            finally: engine.db = original
        finally: engine.close()
