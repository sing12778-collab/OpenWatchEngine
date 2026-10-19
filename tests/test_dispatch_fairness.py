"""Synthetic regressions: unavailable adapters must not starve available ones."""
import unittest
from openwatch import Engine, Item, Snapshot


class Source:
    def __init__(self, version):
        self.version = version
    def scan(self):
        return Snapshot((Item('sample', {'version': self.version}),))


class Sink:
    def __init__(self):
        self.ids = []
    def send(self, event):
        self.ids.append(event['id'])


class DispatchFairnessTests(unittest.TestCase):
    def test_available_recipient_progresses_with_limit_one(self):
        engine = Engine(':memory:')
        try:
            recipients = ('a_missing', 'z_available')
            engine.scan('feed', Source(0), recipients)
            engine.scan('feed', Source(1), recipients)
            available = Sink()
            report = engine.dispatch({'z_available': available}, now=0, limit=1)
            self.assertEqual(report['sent'], 1)
            self.assertEqual(len(available.ids), 1)
            self.assertEqual(engine.dispatch({}, now=0, limit=1)['unavailable'], 1)
            restored = Sink()
            self.assertEqual(engine.dispatch({'a_missing': restored}, now=0, limit=1)['sent'], 1)
            self.assertEqual(restored.ids, available.ids)
            self.assertEqual(engine.dispatch({'z_available': available}, now=0)['sent'], 0)
        finally:
            engine.close()

    def test_default_batch_progresses_past_one_hundred_missing_recipients(self):
        engine = Engine(':memory:')
        try:
            recipients = tuple(f'a_missing_{i:03}' for i in range(100)) + ('z_available',)
            engine.scan('feed', Source(0), recipients)
            engine.scan('feed', Source(1), recipients)
            available = Sink()
            report = engine.dispatch({'z_available': available}, now=0)
            self.assertEqual(report, {'sent': 1, 'failed': 0, 'unavailable': 99})
            self.assertEqual(engine.db.execute('SELECT count(*) FROM deliveries WHERE sent=0').fetchone()[0], 100)
            self.assertEqual(len(available.ids), 1)
        finally:
            engine.close()
