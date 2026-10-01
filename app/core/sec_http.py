"""
Single SEC HTTP client for bulk downloads (SPEC_107).

- Every request goes through the SEC fair-access gate (SPEC_146,
  ``app/core/sec_gate.py``): one bucket for every SEC host shared by all
  NexData processes, the single SEC_USER_AGENT, global Retry-After / backoff /
  circuit breaker. A pause longer than the gate's max wait raises
  ``SecRateLimited`` at once (never retried here).
- Retries 5xx/network errors with backoff; a 429/403 is retried only after the
  gate's global cooldown (which honours Retry-After in full).
- ``stream_to_file`` streams to ``<dest>.part``, validates magic bytes, and
  renames atomically so a partial file never looks complete.

Synchronous on purpose: bulk loaders run in a worker thread (asyncio.to_thread).
"""

from __future__ import annotations

import hashlib
import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Callable, Optional
from urllib.parse import urlparse

import httpx

from app.core.config import SEC_DEFAULT_USER_AGENT as DEFAULT_USER_AGENT  # noqa: F401 (re-export)
from app.core.sec_gate import SecGatedTransport, SecRateLimited, is_sec_host, sec_user_agent

logger = logging.getLogger(__name__)

SEC_BUCKET_DOMAIN = "sec.gov"
RETRY_STATUS = {429, 500, 502, 503, 504}
BACKOFF_SECONDS = (2.0, 6.0, 15.0, 30.0)
CHUNK = 1024 * 256


class PayloadError(RuntimeError):
    """Downloaded body is not what the URL promised (HTML error page, truncated, ...)."""


@dataclass
class FetchedFile:
    path: Path
    url: str
    bytes: int
    sha256: str
    etag: Optional[str]
    last_modified: Optional[str]
    fetched_at: datetime
    # True when the server answered 304 to a conditional request: nothing was
    # written and ``path`` does not exist (SPEC_122)
    not_modified: bool = False


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


class SecHttp:
    """Polite, rate-limited SEC client. Use as a context manager or call close().

    ``user_agent``, ``rps`` and ``session_factory`` are accepted for backward
    compatibility and ignored: the SEC gate (SPEC_146) owns the User-Agent
    (SEC_USER_AGENT) and the one cross-process rate limit.
    """

    def __init__(
        self,
        user_agent: Optional[str] = None,
        rps: Optional[float] = None,
        session_factory: Optional[Callable] = None,
        timeout: float = 120.0,
        transport: Optional[httpx.BaseTransport] = None,
    ):
        self.user_agent = sec_user_agent()
        inner = transport if transport is not None else httpx.HTTPTransport()
        self._client = httpx.Client(
            headers={"User-Agent": self.user_agent, "Accept-Encoding": "gzip, deflate"},
            timeout=httpx.Timeout(timeout, connect=30.0),
            follow_redirects=True,
            transport=SecGatedTransport(inner),
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
        """Host check only; the gate in the transport does the limiting."""
        host = urlparse(url).hostname or ""
        if not is_sec_host(host):
            raise ValueError(f"SecHttp only talks to sec.gov hosts, got {host!r}")

    # -- requests ---------------------------------------------------------
    def _send(self, url: str, stream: bool, headers: Optional[dict] = None):
        attempt = 0
        while True:
            self._throttle(url)
            try:
                request = self._client.build_request("GET", url, headers=headers)
                response = self._client.send(request, stream=stream)
            except SecRateLimited:
                raise
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
                attempt += 1
                response.close()
                if response.status_code == 429:
                    # The gate already recorded the strike and set a GLOBAL cooldown of
                    # max(Retry-After, backoff); the next _send waits it out there (or
                    # raises SecRateLimited). No local sleep on top of it.
                    logger.warning(f"SEC HTTP 429 on {url}; retry after the gate's cooldown")
                    continue
                wait = BACKOFF_SECONDS[attempt - 1]
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

    def stream_to_file(
        self,
        url: str,
        dest: Path | str,
        etag: Optional[str] = None,
        last_modified: Optional[str] = None,
    ) -> FetchedFile:
        """Stream ``url`` to ``dest``.

        With ``etag`` / ``last_modified`` (validators from an earlier download of
        the same URL) the request is conditional (SPEC_122); a 304 writes nothing
        and returns ``FetchedFile(not_modified=True, bytes=0)``.
        """
        dest = Path(dest)
        dest.parent.mkdir(parents=True, exist_ok=True)
        part = dest.with_name(dest.name + ".part")
        conditional = {}
        if etag:
            conditional["If-None-Match"] = etag
        if last_modified:
            conditional["If-Modified-Since"] = last_modified
        response = self._send(url, stream=True, headers=conditional or None)
        if response.status_code == 304:
            response.close()
            logger.info(f"SEC 304 not modified: {url}")
            return FetchedFile(
                path=dest,
                url=str(response.url),
                bytes=0,
                sha256="",
                etag=response.headers.get("ETag") or etag,
                last_modified=response.headers.get("Last-Modified") or last_modified,
                fetched_at=datetime.utcnow(),
                not_modified=True,
            )
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
