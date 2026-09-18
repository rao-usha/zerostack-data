"""
Single SEC HTTP client for bulk downloads (SPEC_107).

- One honest User-Agent with a monitored contact (SEC fair-access policy);
  callers cannot override it.
- One token bucket for every SEC host (www / data / efts / reports.adviserinfo)
  at 8 req/s in-process, plus the distributed Postgres bucket ``sec.gov`` when
  a session factory is supplied. The distributed limiter fails closed.
- Retries 429/5xx/network errors with backoff, honouring Retry-After (capped).
- ``stream_to_file`` streams to ``<dest>.part``, validates magic bytes, and
  renames atomically so a partial file never looks complete.

Synchronous on purpose: bulk loaders run in a worker thread (asyncio.to_thread).
"""

from __future__ import annotations

import email.utils
import hashlib
import logging
import os
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Optional
from urllib.parse import urlparse

import httpx

logger = logging.getLogger(__name__)

DEFAULT_USER_AGENT = "Nexdata research alexiusmichael@gmail.com"
SEC_BUCKET_DOMAIN = "sec.gov"
RETRY_STATUS = {429, 500, 502, 503, 504}
BACKOFF_SECONDS = (2.0, 6.0, 15.0, 30.0)
MAX_RETRY_AFTER = 60.0
DISTRIBUTED_MAX_WAIT = 60.0
CHUNK = 1024 * 256


class PayloadError(RuntimeError):
    """Downloaded body is not what the URL promised (HTML error page, truncated, ...)."""


class RateLimitTimeout(RuntimeError):
    """Distributed SEC bucket could not be acquired in time (fail closed)."""


@dataclass
class FetchedFile:
    path: Path
    url: str
    bytes: int
    sha256: str
    etag: Optional[str]
    last_modified: Optional[str]
    fetched_at: datetime


def validate_payload(name: str, head: bytes, content_type: Optional[str], size: Optional[int] = None) -> None:
    """Magic bytes + content type. Never trust a 200."""
    lower = name.lower()
    ctype = (content_type or "").lower()
    stripped = head.lstrip()
    if size is not None and size < 64:
        raise PayloadError(f"{name}: {size} bytes -- too small to be a real file ({ctype})")
    if lower.endswith(".zip") and not head.startswith(b"PK"):
        raise PayloadError(f"{name}: not a zip (starts {head[:8]!r}, {ctype})")
    if lower.endswith(".gz") and not head.startswith(b"\x1f\x8b"):
        raise PayloadError(f"{name}: not gzip (starts {head[:8]!r}, {ctype})")
    if lower.endswith(".json") and stripped[:1] not in (b"{", b"["):
        raise PayloadError(f"{name}: body is not JSON (starts {stripped[:40]!r}, {ctype})")
    if lower.endswith((".csv", ".tsv", ".txt", ".xml", ".idx")) and stripped[:14].lower().startswith(b"<!doctype html"):
        raise PayloadError(f"{name}: HTML served for a data file ({ctype})")


class _TokenBucket:
    """Thread-safe process-wide token bucket."""

    def __init__(self, rps: float):
        self.rps = rps
        self.capacity = max(1.0, rps)
        self.tokens = self.capacity
        self.updated = time.monotonic()
        self.lock = threading.Lock()

    def acquire(self, sleep: Callable[[float], None]) -> None:
        while True:
            with self.lock:
                now = time.monotonic()
                self.tokens = min(self.capacity, self.tokens + (now - self.updated) * self.rps)
                self.updated = now
                if self.tokens >= 1.0:
                    self.tokens -= 1.0
                    return
                wait = (1.0 - self.tokens) / self.rps
            sleep(wait)


_BUCKETS: dict = {}
_BUCKETS_LOCK = threading.Lock()


def _shared_bucket(rps: float) -> _TokenBucket:
    with _BUCKETS_LOCK:
        if rps not in _BUCKETS:
            _BUCKETS[rps] = _TokenBucket(rps)
        return _BUCKETS[rps]


def _retry_after_seconds(value: Optional[str]) -> Optional[float]:
    if not value:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        try:
            when = email.utils.parsedate_to_datetime(value)
            return max(0.0, (when - datetime.now(timezone.utc)).total_seconds())
        except (TypeError, ValueError):
            return None


class SecHttp:
    """Polite, rate-limited SEC client. Use as a context manager or call close()."""

    def __init__(
        self,
        user_agent: Optional[str] = None,
        rps: float = 8.0,
        session_factory: Optional[Callable] = None,
        timeout: float = 120.0,
        transport: Optional[httpx.BaseTransport] = None,
    ):
        if user_agent is None:
            try:
                from app.core.config import get_settings

                user_agent = get_settings().sec_user_agent
            except Exception:
                user_agent = DEFAULT_USER_AGENT
        self.user_agent = user_agent
        self._bucket = _shared_bucket(rps)
        self._session_factory = session_factory
        self._client = httpx.Client(
            headers={"User-Agent": user_agent, "Accept-Encoding": "gzip, deflate"},
            timeout=httpx.Timeout(timeout, connect=30.0),
            follow_redirects=True,
            transport=transport,
        )
        self._sleep = time.sleep
        self.requests = 0
        self.bytes_downloaded = 0

    # -- lifecycle --------------------------------------------------------
    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> "SecHttp":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    # -- rate limiting ----------------------------------------------------
    def _throttle(self, url: str) -> None:
        host = urlparse(url).hostname or ""
        if not host.endswith("sec.gov"):
            raise ValueError(f"SecHttp only talks to sec.gov hosts, got {host!r}")
        self._bucket.acquire(self._sleep)
        if self._session_factory is None:
            return
        from app.core.rate_limiter import acquire_distributed_token

        deadline = time.monotonic() + DISTRIBUTED_MAX_WAIT
        while True:
            db = self._session_factory()
            try:
                if acquire_distributed_token(db, SEC_BUCKET_DOMAIN):
                    return
            finally:
                db.close()
            if time.monotonic() >= deadline:
                raise RateLimitTimeout(f"distributed {SEC_BUCKET_DOMAIN} bucket not acquired in {DISTRIBUTED_MAX_WAIT}s")
            self._sleep(0.25)

    # -- requests ---------------------------------------------------------
    def _send(self, url: str, stream: bool):
        attempt = 0
        while True:
            self._throttle(url)
            try:
                request = self._client.build_request("GET", url)
                response = self._client.send(request, stream=stream)
            except (httpx.TransportError,) as e:
                if attempt < len(BACKOFF_SECONDS):
                    wait = BACKOFF_SECONDS[attempt]
                    attempt += 1
                    logger.warning(f"SEC network error on {url} ({e}); retry in {wait}s")
                    self._sleep(wait)
                    continue
                raise
            self.requests += 1
            if response.status_code in RETRY_STATUS and attempt < len(BACKOFF_SECONDS):
                retry_after = _retry_after_seconds(response.headers.get("Retry-After"))
                wait = min(MAX_RETRY_AFTER, retry_after if retry_after is not None else BACKOFF_SECONDS[attempt])
                attempt += 1
                response.close()
                logger.warning(f"SEC HTTP {response.status_code} on {url}; retry in {wait:.1f}s")
                self._sleep(wait)
                continue
            if response.status_code >= 400:
                status = response.status_code
                response.close()
                raise httpx.HTTPStatusError(f"HTTP {status} for {url}", request=request, response=response)
            return response

    def get_text(self, url: str) -> str:
        response = self._send(url, stream=False)
        self.bytes_downloaded += len(response.content)
        return response.text

    def get_bytes(self, url: str) -> bytes:
        response = self._send(url, stream=False)
        self.bytes_downloaded += len(response.content)
        return response.content

    def stream_to_file(self, url: str, dest: Path | str) -> FetchedFile:
        dest = Path(dest)
        dest.parent.mkdir(parents=True, exist_ok=True)
        part = dest.with_name(dest.name + ".part")
        response = self._send(url, stream=True)
        digest = hashlib.sha256()
        size = 0
        head = b""
        try:
            with open(part, "wb") as fh:
                for chunk in response.iter_bytes(CHUNK):
                    if len(head) < 512:
                        head += chunk[: 512 - len(head)]
                    digest.update(chunk)
                    size += len(chunk)
                    fh.write(chunk)
            validate_payload(dest.name, head, response.headers.get("Content-Type"), size)
            os.replace(part, dest)
        except BaseException:
            try:
                part.unlink()
            except FileNotFoundError:
                pass
            raise
        finally:
            response.close()
        self.bytes_downloaded += size
        return FetchedFile(
            path=dest,
            url=str(response.url),
            bytes=size,
            sha256=digest.hexdigest(),
            etag=response.headers.get("ETag"),
            last_modified=response.headers.get("Last-Modified"),
            fetched_at=datetime.utcnow(),
        )


def sha256_file(path: Path | str) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(CHUNK), b""):
            digest.update(chunk)
    return digest.hexdigest()
