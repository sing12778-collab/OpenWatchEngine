"""Failure signals and transient delivery-DB faults, synthetic data only."""
import contextlib
import io
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from openwatch import Engine, Item, Snapshot
from openwatch.runner import Runner
from openwatch.__main__ import main


class Source:
    def __init__(self, *args, **kwargs): pass
    def scan(self): return Snapshot((Item('x', {'v': 1}),))


class BrokenSource(Source):
    def scan(self): raise OSError('SYNTHETIC_PRIVATE_PAYLOAD')


class Sink:
    calls = 0
    def send(self, event): self.calls += 1


class Clock:
    value = 0
    def __call__(self): return self.value


class FailureReportingTests(unittest.TestCase):
    def invoke(self, args, source=Source, interrupt=False):
        output, error = io.StringIO(), io.StringIO()
        runner_patch = patch.object(Runner, 'run', side_effect=KeyboardInterrupt) if interrupt else contextlib.nullcontext()
        with patch.object(sys, 'argv', ['openwatch'] + args), patch('openwatch.http.HttpJsonSource', source), \
                contextlib.redirect_stdout(output), contextlib.redirect_stderr(error), runner_patch:
            try: main()
            except SystemExit as exited: code = exited.code
            else: code = 0
        return code, output.getvalue(), error.getvalue()

    def test_bounded_watch_failure_returns_one_without_secret(self):
        code, output, error = self.invoke(['watch', 'http://127.0.0.1/not-contacted',
            '--source', 'feed', '--db', ':memory:', '--cycles', '1'], BrokenSource)
        self.assertEqual(code, 1)
        self.assertEqual(json.loads(output)['scan'], 'failed')
        self.assertNotIn('SYNTHETIC_PRIVATE_PAYLOAD', output + error)

    def test_bounded_watch_success_returns_zero(self):
        code, output, error = self.invoke(['watch', 'http://127.0.0.1/not-contacted',
            '--source', 'feed', '--db', ':memory:', '--cycles', '1'])
        self.assertEqual(code, 0)
        self.assertEqual(json.loads(output)['scan'], 'ok')

    def test_interrupt_returns_130(self):
        code, output, error = self.invoke(['watch', 'http://127.0.0.1/not-contacted',
            '--source', 'feed', '--db', ':memory:'], interrupt=True)
        self.assertEqual(code, 130)
        self.assertEqual(json.loads(output), {'stopped': True})

    def test_scan_subprocess_error_is_nonzero_and_sanitized(self):
        with tempfile.TemporaryDirectory() as folder:
            file = Path(folder) / 'synthetic.json'
            file.write_text('{"SYNTHETIC_PRIVATE_PAYLOAD":', encoding='utf-8')
            result = subprocess.run([sys.executable, '-B', '-m', 'openwatch', 'scan', str(file),
                '--source', 'feed', '--db', str(Path(folder) / 'state.db')],
                cwd=Path(__file__).resolve().parents[1], capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 1)
        self.assertEqual(json.loads(result.stderr.splitlines()[-1]), {'error': 'JSONDecodeError'})
        self.assertNotIn('Traceback', result.stderr)
        self.assertNotIn('SYNTHETIC_PRIVATE_PAYLOAD', result.stderr + result.stdout)

    def test_delivery_ack_database_failure_backs_off_and_recovers(self):
        engine, clock, sink = Engine(':memory:'), Clock(), Sink()
        try:
            engine.scan('feed', type('Baseline', (), {'scan': lambda _: Snapshot(())})(), ('sink',))
            engine.scan('feed', Source(), ('sink',))
            engine.db.execute("CREATE TRIGGER fail_ack BEFORE UPDATE ON deliveries BEGIN SELECT RAISE(ABORT,'SYNTHETIC_PRIVATE_PAYLOAD'); END")
            runner = Runner(engine, 'feed', Source(), {'sink': sink}, monotonic=clock, wall=clock)
            runner.due = 100
            first = runner.tick()
            self.assertEqual(first['delivery_error'], 'IntegrityError')
            self.assertTrue(first['delivery_incomplete'])
            self.assertEqual(first['delivery_retry_in'], 2)
            self.assertNotIn('SYNTHETIC_PRIVATE_PAYLOAD', str(first))
            self.assertEqual(sink.calls, 1)
            clock.value = 1
            runner.tick()
            self.assertEqual(sink.calls, 1)
            engine.db.execute('DROP TRIGGER fail_ack')
            clock.value = 2
            self.assertEqual(runner.tick()['delivery']['sent'], 1)
            self.assertEqual(sink.calls, 1)  # Same process repairs ACK without sending again.
            self.assertEqual(runner.delivery_errors, 1)
        finally: engine.close()

    def test_unchanged_missing_sink_is_not_reported_each_tick(self):
        class Stop:
            count = 0
            def is_set(self): return self.count >= 3
            def wait(self, timeout): self.count += 1; return False
        engine = Engine(':memory:')
        try:
            engine.scan('feed', type('Baseline', (), {'scan': lambda _: Snapshot(())})(), ('missing',))
            engine.scan('feed', Source(), ('missing',))
            runner = Runner(engine, 'feed', Source(), {}, monotonic=lambda: 0, wall=lambda: 0)
            runner.due = 100
            reports = []
            runner.run(stop=Stop(), report=reports.append)
            self.assertEqual(len(reports), 1)
            self.assertEqual(reports[0]['delivery']['unavailable'], 1)
            self.assertEqual(engine.db.execute('SELECT count(*) FROM deliveries WHERE sent=0').fetchone()[0], 1)
        finally: engine.close()
