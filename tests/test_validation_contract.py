"""Malformed input must fail before baseline or HTTP validator publication."""
import json
import sys
import unittest
from email.message import Message
from unittest.mock import patch

from openwatch import Engine, Item, Snapshot
from openwatch.adapters import parse_snapshot
from openwatch.http import HttpJsonSource


class Replay:
    def __init__(self, snapshot): self.snapshot = snapshot
    def scan(self): return self.snapshot


def document(value):
    return json.dumps({'complete': True, 'items': [{'key': 'release', 'value': value}]})


class Response:
    status = 200
    def __init__(self, text, etag):
        self.body = text.encode('utf-8')
        self.headers = Message()
        self.headers['Content-Type'] = 'application/json'
        self.headers['ETag'] = etag
    def __enter__(self): return self
    def __exit__(self, *args): pass
    def read(self, size): return self.body


class ValidationContractTests(unittest.TestCase):
    def test_duplicate_json_fields_rejected_at_any_depth(self):
        for text in (
            '{"complete":false,"complete":true,"items":[]}',
            '{"complete":true,"items":[],"items":[]}',
            '{"complete":true,"items":[{"key":"x","value":{"nested":{"n":1,"n":2}}}]}',
        ):
            with self.subTest(text=text), self.assertRaises(ValueError):
                parse_snapshot(text)

    def test_surrogate_input_cannot_advance_http_cache_or_baseline(self):
        invalid = (
            '{"complete":true,"items":[{"key":"\\ud800","value":{}}]}',
            '{"complete":true,"items":[{"key":"release","value":{"bad":"\\ud800"}}]}',
            '{"complete":true,"items":[{"key":"release","value":{"\\ud800":"bad"}}]}',
        )
        for text in invalid:
            with self.subTest(kind=invalid.index(text)):
                engine = Engine(':memory:')
                source = HttpJsonSource('http://127.0.0.1/not-contacted')
                try:
                    replies = [Response(document({'v': 1}), 'good'), Response(text, 'bad'),
                               Response(document({'v': 2}), 'next')]
                    with patch.object(source._opener, 'open', side_effect=replies) as request:
                        self.assertEqual(engine.scan('feed', source), 0)
                        with self.assertRaises(ValueError): engine.scan('feed', source)
                        self.assertEqual(engine.db.execute('SELECT count(*) FROM events').fetchone()[0], 0)
                        self.assertEqual(engine.scan('feed', source), 1)
                        self.assertIn(('If-none-match', 'good'), request.call_args_list[2].args[0].header_items())
                finally: engine.close()

    def test_non_string_object_keys_rejected_without_baseline_change(self):
        for value in ({1: 'one'}, {'nested': {1: 'one'}}):
            with self.subTest(value=value):
                engine = Engine(':memory:')
                try:
                    engine.scan('feed', Replay(Snapshot((Item('x', {'v': 1}),))))
                    with self.assertRaises(ValueError):
                        engine.scan('feed', Replay(Snapshot((Item('x', value),))))
                    self.assertEqual(json.loads(engine.db.execute('SELECT value FROM items').fetchone()[0]), {'v': 1})
                finally: engine.close()

    def test_unsupported_python_values_raise_value_error(self):
        for value in ({'x': {1}}, {'x': (1, 2)}, {'x': object()}):
            with self.subTest(kind=type(value['x']).__name__), self.assertRaises(ValueError):
                parse_or_validate(Snapshot((Item('x', value),)))

    def test_malformed_snapshot_shapes_raise_value_error(self):
        for snapshot in ({}, Snapshot(({},)), Snapshot(None)):
            with self.subTest(kind=type(snapshot).__name__), self.assertRaises(ValueError):
                parse_or_validate(snapshot)

    def test_non_finite_json_constants_rejected_in_entire_document(self):
        for constant in ('NaN', 'Infinity', '-Infinity'):
            with self.subTest(constant=constant), self.assertRaises(ValueError):
                parse_snapshot('{"complete":true,"items":[],"extra":' + constant + '}')

    def test_shared_subobjects_and_valid_unicode_remain_supported(self):
        child = {'label': '\U0001f600'}
        snapshot = Snapshot((Item('\U0001f680', {'a': child, 'b': child}),))
        result = parse_or_validate(snapshot)
        self.assertEqual(json.loads(result['\U0001f680']), {'a': child, 'b': child})

    def test_cyclic_python_values_fail_without_hanging(self):
        value = {}
        value['cycle'] = value
        with self.assertRaises(ValueError): parse_or_validate(Snapshot((Item('x', value),)))

    def test_deep_input_is_supported_or_rejected_with_value_error(self):
        depth = sys.getrecursionlimit() + 100
        text = '{"complete":true,"items":[{"key":"x","value":{"nested":' + '['*depth + '0' + ']'*depth + '}}]}'
        # Python's C JSON depth threshold need not equal sys.getrecursionlimit().
        # Valid input may succeed; an implementation depth failure must normalize.
        try:
            result = parse_snapshot(text)
        except ValueError:
            return
        self.assertTrue(result.complete)
        self.assertEqual(result.items[0].key, 'x')


def parse_or_validate(snapshot):
    from openwatch.engine import validate_snapshot
    return validate_snapshot(snapshot)
