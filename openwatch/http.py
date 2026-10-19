"""Bounded GET adapter for the OpenWatch snapshot JSON contract."""
from __future__ import annotations

import math
import time
from datetime import timezone
from email.utils import parsedate_to_datetime
from urllib.error import HTTPError
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener

from .adapters import parse_snapshot
from .engine import Snapshot, validate_snapshot


class HttpFailure(Exception):
    """Sanitized HTTP failure; excludes URL, headers, and response body."""

    def __init__(self, status: int, retry_after: float | None = None):
        super().__init__(f"HTTP status {status}")
        self.status = status
        self.retry_after = retry_after


def retry_after_seconds(value: str | None, now: float) -> float | None:
    if value is None:
        return None
    value = value.strip()
    if value.isascii() and value.isdigit():
        try:
            result = float(value)
        except (ValueError, OverflowError):
            return None
        return result if math.isfinite(result) else None
    try:
        date = parsedate_to_datetime(value)
        if date.tzinfo is None:
            date = date.replace(tzinfo=timezone.utc)
        return max(0.0, date.timestamp() - now)
    except (ValueError, TypeError, OverflowError):
        return None


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class HttpJsonSource:
    """In-memory validated representation cache; restart fetches unconditionally."""

    def __init__(self, url: str, *, timeout: float = 10, max_bytes: int = 1048576):
        parsed = urlsplit(url)
        if (parsed.scheme not in ("http", "https") or not parsed.hostname
                or parsed.username is not None or parsed.password is not None
                or parsed.fragment):
            raise ValueError("expected HTTP(S) URL without credentials or fragment")
        if not math.isfinite(timeout) or timeout <= 0:
            raise ValueError("timeout must be finite and positive")
        if type(max_bytes) is not int or max_bytes < 1:
            raise ValueError("max_bytes must be a positive integer")
        self.url, self.timeout, self.max_bytes = url, timeout, max_bytes
        self._snapshot_text = None
        self._etag = None
        self._modified = None
        # Do not inherit machine proxy configuration, cookies, or authentication.
        self._opener = build_opener(ProxyHandler({}), NoRedirect())

    def scan(self) -> Snapshot:
        headers = {"Accept": "application/json", "Accept-Encoding": "identity",
                   "User-Agent": "OpenWatchEngine/0.2.0"}
        if self._snapshot_text is not None:
            if self._etag:
                headers["If-None-Match"] = self._etag
            if self._modified:
                headers["If-Modified-Since"] = self._modified
        request = Request(self.url, headers=headers, method="GET")
        try:
            response = self._opener.open(request, timeout=self.timeout)
        except HTTPError as error:
            try:
                if (error.code == 304 and self._snapshot_text is not None
                        and (self._etag or self._modified)):
                    return parse_snapshot(self._snapshot_text)
                delay = retry_after_seconds(error.headers.get("Retry-After"), time.time())
                raise HttpFailure(error.code, delay if error.code in (429, 503) else None) from None
            finally:
                error.close()
        with response:
            if response.status != 200:
                raise HttpFailure(response.status)
            media = response.headers.get_content_type()
            if media != "application/json" and not media.endswith("+json"):
                raise ValueError("response must have JSON content type")
            if response.headers.get("Content-Encoding", "identity").lower() != "identity":
                raise ValueError("compressed responses are not supported")
            size = response.headers.get("Content-Length")
            if size is not None and (not size.isdecimal() or int(size) > self.max_bytes):
                raise ValueError("response length invalid or exceeds byte limit")
            body = response.read(self.max_bytes + 1)
            if len(body) > self.max_bytes:
                raise ValueError("response exceeds byte limit")
            if size is not None and len(body) != int(size):
                raise ValueError("truncated response")
            snapshot_text = body.decode("utf-8")
            snapshot = parse_snapshot(snapshot_text)
            # Never cache validators for partial, malformed, or invalid snapshots.
            validate_snapshot(snapshot)
            etag = response.headers.get("ETag")
            modified = response.headers.get("Last-Modified")
        # Cache immutable text, never the mutable objects returned to callers.
        self._snapshot_text, self._etag, self._modified = snapshot_text, etag, modified
        return snapshot
