import json
import socket
import tempfile
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from openwatch import Engine
from openwatch.http import HttpFailure, HttpJsonSource, retry_after_seconds


def document(value=1, complete=True):
    return json.dumps({"complete": complete, "items": [
        {"key": "release", "value": {"version": value}}]}).encode()


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass
    def do_GET(self):
        self.server.seen.append(dict(self.headers))
        status, headers, body, delay = self.server.replies.pop(0)
        if delay:
            time.sleep(delay)
        try:
            self.send_response(status)
            for key, value in headers.items():
                self.send_header(key, value)
            self.end_headers()
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
            pass


class HttpTests(unittest.TestCase):
    def setUp(self):
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.server.replies, self.server.seen = [], []
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.url = f"http://127.0.0.1:{self.server.server_port}/snapshot"
        self.temp = tempfile.TemporaryDirectory()
        self.engine = Engine(str(Path(self.temp.name) / "state.db"))
    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()
        self.engine.close()
        self.temp.cleanup()
    def reply(self, body=None, *, status=200, headers=None, delay=0):
        self.server.replies.append((status, {
            "Content-Type": "application/json", **(headers or {})},
            document() if body is None else body, delay))
    def test_etag_304_and_restart(self):
        self.reply(headers={"ETag": '"one"'})
        self.reply(b"", status=304)
        self.reply(document(2), headers={"ETag": '"two"'})
        source = HttpJsonSource(self.url)
        self.assertEqual(self.engine.scan("feed", source), 0)
        self.assertEqual(self.engine.scan("feed", source), 0)
        self.assertEqual(self.server.seen[1]["If-None-Match"], '"one"')
        self.assertEqual(self.engine.scan("feed", source), 1)
        self.reply(document(2))
        self.engine.close()
        self.engine = Engine(str(Path(self.temp.name) / "state.db"))
        self.assertEqual(self.engine.scan("feed", HttpJsonSource(self.url)), 0)
        self.assertNotIn("If-None-Match", self.server.seen[-1])
    def test_last_modified_and_validator_removal(self):
        stamp = "Wed, 21 Oct 2015 07:28:00 GMT"
        source = HttpJsonSource(self.url)
        self.reply(headers={"Last-Modified": stamp})
        source.scan()
        self.reply(document(2))
        source.scan()
        self.assertEqual(self.server.seen[-1]["If-Modified-Since"], stamp)
        self.reply(document(2))
        source.scan()
        self.assertNotIn("If-Modified-Since", self.server.seen[-1])
    def test_partial_does_not_poison_conditional_cache(self):
        source = HttpJsonSource(self.url)
        self.reply(headers={"ETag": '"one"'})
        self.engine.scan("feed", source)
        self.reply(document(2, False), headers={"ETag": '"bad"'})
        with self.assertRaises(ValueError):
            self.engine.scan("feed", source)
        self.reply(document(2))
        self.assertEqual(self.engine.scan("feed", source), 1)
        self.assertEqual(self.server.seen[-1]["If-None-Match"], '"one"')
    def test_invalid_key_does_not_cache(self):
        source = HttpJsonSource(self.url)
        self.reply(b'{"complete":true,"items":[{"key":"","value":{}}]}',
                   headers={"ETag": '"bad"'})
        with self.assertRaises(ValueError):
            source.scan()
        self.reply()
        source.scan()
        self.assertNotIn("If-None-Match", self.server.seen[-1])
    def test_unsolicited_304_rejected(self):
        self.reply(b"", status=304)
        with self.assertRaises(HttpFailure):
            HttpJsonSource(self.url).scan()
    def test_rate_limit_preserves_baseline(self):
        source = HttpJsonSource(self.url)
        self.reply()
        self.engine.scan("feed", source)
        self.reply(b"secret", status=429, headers={"Retry-After": "120"})
        with self.assertRaises(HttpFailure) as error:
            self.engine.scan("feed", source)
        self.assertEqual(error.exception.retry_after, 120)
        self.assertNotIn("secret", str(error.exception))
        self.reply(document(2))
        self.assertEqual(self.engine.scan("feed", source), 1)
    def test_json_and_media_validation(self):
        for body, headers in [
            (b"{", {}),
            (b'{"complete":"true","items":[]}', {}),
            (b"{}", {"Content-Type": "text/html"}),
            (document(), {"Content-Encoding": "gzip"}),
        ]:
            with self.subTest(body=body, headers=headers):
                self.reply(body, headers=headers)
                with self.assertRaises(ValueError):
                    HttpJsonSource(self.url).scan()
    def test_response_limits_with_and_without_length(self):
        for headers in ({}, {"Content-Length": "9999"}):
            self.reply(b"x" * 100, headers=headers)
            with self.assertRaises(ValueError):
                HttpJsonSource(self.url, max_bytes=32).scan()
    def test_truncated_response_rejected(self):
        self.reply(document(), headers={"Content-Length": "999"})
        with self.assertRaises((ValueError, OSError)):
            HttpJsonSource(self.url).scan()
    def test_redirect_not_followed(self):
        self.reply(b"", status=302, headers={"Location": self.url + "/elsewhere"})
        with self.assertRaises(HttpFailure) as error:
            HttpJsonSource(self.url).scan()
        self.assertEqual(error.exception.status, 302)
        self.assertEqual(len(self.server.seen), 1)
    def test_socket_timeout(self):
        self.reply(delay=0.2)
        with self.assertRaises((TimeoutError, socket.timeout)):
            HttpJsonSource(self.url, timeout=0.03).scan()
    def test_constructor_validation(self):
        for url in ("file:///tmp/x", "http://user:pass@localhost/x",
                    "http://localhost/x#fragment"):
            with self.assertRaises(ValueError):
                HttpJsonSource(url)
        for timeout in (0, -1, float("nan"), float("inf")):
            with self.assertRaises(ValueError):
                HttpJsonSource(self.url, timeout=timeout)



    def test_cli_watch_and_restart(self):
        import subprocess
        import sys
        database = str(Path(self.temp.name) / "cli.db")
        command = [sys.executable, "-B", "-m", "openwatch", "watch", self.url,
                   "--source", "cli", "--db", database, "--interval", "0.01",
                   "--cycles"]
        self.reply(headers={"ETag": '"one"'})
        self.reply(document(2), headers={"ETag": '"two"'})
        self.reply(b"", status=304)
        result = subprocess.run(command + ["3"], capture_output=True, text=True,
                                cwd=Path(__file__).resolve().parents[1], timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)
        output = [json.loads(line) for line in result.stdout.splitlines()]
        self.assertEqual([row["events_created"] for row in output if "scan" in row],
                         [0, 1, 0])
        self.assertEqual(len([row for row in output if row.get("kind") == "changed"]), 1)
        self.reply(document(2))
        restarted = subprocess.run(command + ["1"], capture_output=True, text=True,
                                   cwd=Path(__file__).resolve().parents[1], timeout=10)
        self.assertEqual(restarted.returncode, 0, restarted.stderr)
        self.assertEqual(json.loads(restarted.stdout)["events_created"], 0)

    def test_304_recovers_after_database_commit_failure(self):
        source = HttpJsonSource(self.url)
        self.reply(headers={"ETag": '"one"'})
        self.engine.scan("feed", source)
        self.engine.db.execute("""
            CREATE TRIGGER reject_update BEFORE INSERT ON items
            BEGIN SELECT RAISE(ABORT,'test failure'); END
        """)
        self.reply(document(2), headers={"ETag": '"two"'})
        import sqlite3
        with self.assertRaises(sqlite3.IntegrityError):
            self.engine.scan("feed", source)
        self.engine.db.execute("DROP TRIGGER reject_update")
        self.reply(b"", status=304)
        self.assertEqual(self.engine.scan("feed", source), 1)
        self.assertEqual(self.server.seen[-1]["If-None-Match"], '"two"')


class RetryAfterTests(unittest.TestCase):
    def test_seconds_and_date(self):
        self.assertEqual(retry_after_seconds("120", 0), 120)
        self.assertEqual(retry_after_seconds("Thu, 01 Jan 1970 00:02:00 GMT", 10), 110)
        self.assertEqual(retry_after_seconds("Thu, 01 Jan 1970 00:00:00 GMT", 10), 0)
    def test_invalid_values(self):
        for value in (None, "", "-1", "1.5", "NaN", "nonsense", "9" * 400):
            self.assertIsNone(retry_after_seconds(value, 0))
