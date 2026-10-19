"""Returned HTTP snapshots must not expose the cached nested JSON objects."""
import json
import unittest
from email.message import Message
from unittest.mock import patch
from urllib.error import HTTPError

from openwatch import Engine
from openwatch.http import HttpJsonSource


def value():
    return {'version': 1, 'metadata': {'tags': [{'label': 'original'}]}}


class Response:
    status = 200
    def __init__(self, payload=None):
        self.payload = value() if payload is None else payload
        self.headers = Message()
        self.headers['Content-Type'] = 'application/json'
        self.headers['ETag'] = 'synthetic-one'
    def __enter__(self): return self
    def __exit__(self, *args): pass
    def read(self, size):
        return json.dumps({'complete': True, 'items': [{'key': 'release', 'value': self.payload}]}).encode()


def not_modified(source):
    return HTTPError(source.url, 304, 'synthetic', Message(), None)


class Replay:
    def __init__(self, snapshot): self.snapshot = snapshot
    def scan(self): return self.snapshot


class CacheIsolationTests(unittest.TestCase):
    def test_valid_deep_json_keeps_supported_depth_and_isolation(self):
        payload = {'leaf': 'original'}
        for _ in range(500):
            payload = {'nested': payload}
        source = HttpJsonSource('http://127.0.0.1/not-contacted')
        with patch.object(source._opener, 'open', side_effect=[Response(payload), not_modified(source)]):
            returned = source.scan()
            leaf = returned.items[0].value
            for _ in range(500):
                leaf = leaf['nested']
            leaf['leaf'] = 'mutated'
            cached = source.scan()
        leaf = cached.items[0].value
        for _ in range(500):
            leaf = leaf['nested']
        self.assertEqual(leaf['leaf'], 'original')

    def test_mutating_200_result_does_not_create_false_304_event(self):
        source = HttpJsonSource('http://127.0.0.1/not-contacted')
        engine = Engine(':memory:')
        try:
            with patch.object(source._opener, 'open', side_effect=[Response(), not_modified(source)]) as request:
                original = source.scan()
                self.assertEqual(engine.scan('feed', Replay(original)), 0)
                original.items[0].value['metadata']['tags'][0]['label'] = 'mutated'
                original.items[0].value['metadata']['tags'].append({'label': 'extra'})
                self.assertEqual(engine.scan('feed', source), 0)
                headers = request.call_args_list[1].args[0].header_items()
                self.assertIn(('If-none-match', 'synthetic-one'), headers)
                self.assertEqual(engine.db.execute('SELECT count(*) FROM events').fetchone()[0], 0)
        finally:
            engine.close()

    def test_mutating_304_result_does_not_poison_later_304_results(self):
        source = HttpJsonSource('http://127.0.0.1/not-contacted')
        with patch.object(source._opener, 'open', side_effect=[
                Response(), not_modified(source), not_modified(source)]):
            source.scan()
            first = source.scan()
            first.items[0].value['version'] = 999
            first.items[0].value['metadata']['tags'][0]['label'] = 'mutated'
            second = source.scan()
        self.assertEqual(second.items[0].value, value())
        self.assertIsNot(first.items[0].value, second.items[0].value)
        self.assertIsNot(first.items[0].value['metadata'], second.items[0].value['metadata'])
