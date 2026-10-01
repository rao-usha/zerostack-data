"""
Open-web fetch for company sites (SPEC_148) -- terms, robots, Retry-After, pacing.

`SecHttp` / `sec_gate` cover *.sec.gov only. This is the one sanctioned way for a
NexData collector to request a page from an arbitrary company website. Every
request -- the first one, every redirect hop and the robots.txt probe itself --
passes the same gates, in this order:

1. **Scheme.** http/https only.
2. **Terms.** The site's registrable domain must have a recorded terms review
   (`Review(verdict, citation, reviewed)`, loaded from a JSON registry such as
   `app/entities/data/site_terms.json`). "Nobody reviewed it" is not permission:
   an unreviewed site gets ZERO requests (`terms_unreviewed`), a `refused` one
   likewise (`terms_refused`). Same rule as the workbench's `ingest/hosts.py`;
   keyed by registrable domain here because one review covers a company's hosts.
3. **Backoff.** A host that answered 429 (or 503 + Retry-After) is not asked
   again until its Retry-After (default `DEFAULT_BACKOFF`) has passed; within a
   collector run that means never -- nothing is retried (`host_backed_off`).
4. **Public address.** The host must resolve only to globally routable
   addresses (`non_public_address` otherwise: no 10/8, 127/8, 169.254/16, ::1 ...).
5. **robots.txt** per origin (scheme, host, port), cached for the fetcher's life,
   matched against our product token `NexdataResearch`. RFC 9309: 2xx is parsed;
   401/403 = disallow all (conservative, as the workbench gate); other 4xx = no
   robots = allowed; 5xx or unreachable = disallow all; robots redirects are
   followed (<= 5 hops, RFC 9309 2.3.1.2), each hop through gates 2-4, and a hop
   that may not be asked leaves robots unavailable = disallow all. Crawl-delay is
   honoured; one above `MAX_CRAWL_DELAY` refuses the site.
6. **Pacing.** At most one request per host every max(Crawl-delay, MIN_INTERVAL)
   seconds, robots.txt included.

Redirects are followed by hand, hop by hop, only while they stay on the starting
registrable domain. A hop to another registrable domain is RECORDED
(`redirect_offsite`, `redirect_to`) and never requested -- that is the rebrand /
alias evidence, and the target site has its own terms to review.

The User-Agent is the honest NexdataResearch string with a contact address.
Bodies are capped at `max_bytes`. Nothing here retries.
"""

from __future__ import annotations

import ipaddress
import json
import socket
import time
from collections import namedtuple
from dataclasses import dataclass, field
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional
from urllib.parse import urljoin, urlsplit
from urllib.robotparser import RobotFileParser

import httpx

from app.core.config import SEC_DEFAULT_USER_AGENT
from app.entities import domains

USER_AGENT = SEC_DEFAULT_USER_AGENT          # "NexdataResearch/1.0 (research@nexdata.com; ...)"
ROBOTS_TOKEN = "NexdataResearch"
MIN_INTERVAL = 2.0                           # seconds between requests to one host
MAX_CRAWL_DELAY = 60.0
DEFAULT_BACKOFF = 300.0                      # a 429 with no Retry-After
MAX_HOPS = 5
MAX_BYTES = 512_000
TIMEOUT = 10.0

VERDICTS = ("allowed", "refused")
Review = namedtuple("Review", ("verdict", "citation", "reviewed"))


def parse_terms(entries) -> Dict[str, Review]:
    """[{"host": registrable domain, "verdict", "citation", "reviewed"}] -> registry.
    Raises ValueError on anything malformed: a bad registry must not open a site."""
    out: Dict[str, Review] = {}
    for e in entries or []:
        host = str(e.get("host") or "").strip().lower()
        if domains.registrable(host) != host:
            raise ValueError(f"terms entry host must be a registrable domain: {host!r}")
        verdict = e.get("verdict")
        if verdict not in VERDICTS:
            raise ValueError(f"{host}: verdict must be one of {VERDICTS}, got {verdict!r}")
        citation = str(e.get("citation") or "").strip()
        reviewed = str(e.get("reviewed") or "").strip()
        if not citation or len(reviewed) != 10:
            raise ValueError(f"{host}: a review needs a citation and a YYYY-MM-DD date")
        out[host] = Review(verdict, citation, reviewed)
    return out


def load_terms(path) -> Dict[str, Review]:
    return parse_terms(json.loads(Path(path).read_text(encoding="utf-8")))


def _system_resolve(host: str) -> List[str]:
    return sorted({ai[4][0] for ai in socket.getaddrinfo(host, None)})


def _retry_after_seconds(value: Optional[str], now_wall: float) -> Optional[float]:
    if not value:
        return None
    value = value.strip()
    if value.isdigit():
        return float(value)
    try:
        return max(0.0, parsedate_to_datetime(value).timestamp() - now_wall)
    except (TypeError, ValueError):
        return None


@dataclass
class FetchResult:
    outcome: str
    url: str
    status: Optional[int] = None
    final_url: Optional[str] = None
    body: Optional[str] = None
    hops: List[Dict[str, Any]] = field(default_factory=list)
    redirect_to: Optional[str] = None
    retry_after: Optional[float] = None
    error: Optional[str] = None
    terms_citation: Optional[str] = None


class _Refused(Exception):
    def __init__(self, outcome: str, error: Optional[str] = None, retry_after: Optional[float] = None):
        super().__init__(outcome)
        self.outcome, self.error, self.retry_after = outcome, error, retry_after


class OpenWebFetcher:
    def __init__(self, terms: Dict[str, Review], *, transport: Optional[httpx.BaseTransport] = None,
                 resolve_host: Callable[[str], List[str]] = _system_resolve,
                 clock: Callable[[], float] = time.monotonic, sleep: Callable[[float], None] = time.sleep,
                 user_agent: str = USER_AGENT, min_interval: float = MIN_INTERVAL,
                 max_bytes: int = MAX_BYTES, timeout: float = TIMEOUT):
        self.terms = terms
        self.resolve_host = resolve_host
        self.clock, self.sleep = clock, sleep
        self.user_agent = user_agent
        self.min_interval = min_interval
        self.max_bytes = max_bytes
        self.client = httpx.Client(transport=transport, timeout=timeout, follow_redirects=False,
                                   headers={"User-Agent": user_agent})
        self._robots: Dict[tuple, Optional[RobotFileParser]] = {}
        self._delay: Dict[str, float] = {}
        self._last: Dict[str, float] = {}
        self._backoff_until: Dict[str, float] = {}
        self._public: Dict[str, bool] = {}
        self.requests = 0

    def close(self) -> None:
        self.client.close()

    # -- gates ---------------------------------------------------------------

    def _review(self, url: str) -> Review:
        parts = urlsplit(url)
        if parts.scheme not in ("http", "https") or not parts.hostname:
            raise _Refused("bad_url", f"not an http(s) URL: {url}")
        reg = domains.registrable(parts.hostname.lower())
        review = self.terms.get(reg or "")
        if review is None:
            raise _Refused("terms_unreviewed", f"no terms review recorded for {reg}")
        if review.verdict != "allowed":
            raise _Refused("terms_refused", review.citation)
        return review

    def _host_gates(self, host: str) -> None:
        until = self._backoff_until.get(host)
        if until is not None and self.clock() < until:
            raise _Refused("host_backed_off", f"{host} asked us to wait until +{until - self.clock():.0f}s")
        if host not in self._public:
            try:
                addrs = self.resolve_host(host)
            except OSError as exc:
                raise _Refused("dns_error", str(exc))
            ok = bool(addrs)
            for a in addrs:
                try:
                    ok = ok and ipaddress.ip_address(a).is_global
                except ValueError:
                    ok = False
            self._public[host] = ok
        if not self._public[host]:
            raise _Refused("non_public_address", f"{host} resolves to a non-public address")

    def _pace(self, host: str) -> None:
        last = self._last.get(host)
        if last is not None:
            wait = last + max(self.min_interval, self._delay.get(host, 0.0)) - self.clock()
            if wait > 0:
                self.sleep(wait)
        self._last[host] = self.clock()

    def _send(self, url: str, read_body: bool):
        host = urlsplit(url).hostname.lower()
        self._host_gates(host)
        self._pace(host)
        self.requests += 1
        with self.client.stream("GET", url) as resp:
            body = None
            if read_body and 200 <= resp.status_code < 300:
                chunks, size = [], 0
                for chunk in resp.iter_bytes():
                    chunks.append(chunk)
                    size += len(chunk)
                    if size >= self.max_bytes:
                        break
                body = b"".join(chunks)[: self.max_bytes].decode(resp.encoding or "utf-8", errors="replace")
            if resp.status_code == 429 or (resp.status_code == 503 and resp.headers.get("retry-after")):
                ra = _retry_after_seconds(resp.headers.get("retry-after"), time.time())
                ra = DEFAULT_BACKOFF if ra is None else ra
                self._backoff_until[host] = self.clock() + ra
                raise _Refused("retry_after", f"HTTP {resp.status_code} from {host}", retry_after=ra)
            return resp.status_code, resp.headers.get("location"), body

    def _robots_for(self, url: str) -> RobotFileParser:
        parts = urlsplit(url)
        origin = (parts.scheme, parts.hostname.lower(), parts.port)
        if origin in self._robots:
            rp = self._robots[origin]
            if rp is None:
                raise _Refused("robots_disallowed", "robots.txt unavailable (disallow all)")
            return rp
        robots_url = f"{parts.scheme}://{parts.netloc}/robots.txt"
        rp: Optional[RobotFileParser] = None
        try:
            for hop in range(MAX_HOPS + 1):
                if hop:
                    # RFC 9309 2.3.1.2: follow robots redirects (<= 5), even off-site --
                    # but every hop passes the same gates; a hop that may not be asked
                    # leaves robots unavailable = disallow all
                    try:
                        self._review(robots_url)
                        self._host_gates(urlsplit(robots_url).hostname.lower())
                    except _Refused:
                        break
                else:
                    self._review(robots_url)
                status, location, body = self._send(robots_url, read_body=True)
                if 300 <= status < 400 and location:
                    robots_url = urljoin(robots_url, location)
                    continue
                if 200 <= status < 300:
                    rp = RobotFileParser()
                    rp.parse((body or "").splitlines())
                elif status in (401, 403) or status >= 500:
                    rp = None                                    # disallow all
                else:
                    rp = RobotFileParser()
                    rp.parse([])                                 # 4xx: no robots, allow all
                break
        except _Refused as ref:
            if ref.outcome in ("retry_after", "host_backed_off", "terms_unreviewed", "terms_refused",
                               "non_public_address", "dns_error"):
                raise
            rp = None
        except httpx.HTTPError:
            rp = None                                            # unreachable: disallow all
        self._robots[origin] = rp
        if rp is None:
            raise _Refused("robots_disallowed", "robots.txt unavailable (disallow all)")
        delay = rp.crawl_delay(ROBOTS_TOKEN)
        if delay is not None:
            self._delay[origin[1]] = float(delay)
        return rp

    def _allowed(self, url: str) -> None:
        rp = self._robots_for(url)
        host = urlsplit(url).hostname.lower()
        if self._delay.get(host, 0.0) > MAX_CRAWL_DELAY:
            raise _Refused("robots_crawl_delay_too_long", f"Crawl-delay {self._delay[host]}s")
        if not rp.can_fetch(ROBOTS_TOKEN, url):
            raise _Refused("robots_disallowed", f"robots.txt disallows {url}")

    # -- the one public call ---------------------------------------------------

    def get(self, url: str) -> FetchResult:
        """GET a page through every gate. Never raises for a refusal or an HTTP
        error: the outcome says what happened."""
        hops: List[Dict[str, Any]] = []
        current = url
        citation = None
        try:
            start_reg = domains.registrable((urlsplit(url).hostname or "").lower())
            for _hop in range(MAX_HOPS + 1):
                citation = self._review(current).citation
                self._host_gates(urlsplit(current).hostname.lower())
                self._allowed(current)
                status, location, body = self._send(current, read_body=True)
                if 300 <= status < 400 and location:
                    nxt = urljoin(current, location)
                    hops.append({"url": current, "status": status, "location": nxt})
                    reg = domains.registrable((urlsplit(nxt).hostname or "").lower())
                    if reg != start_reg:
                        return FetchResult("redirect_offsite", url, status, current, None, hops,
                                           redirect_to=reg, terms_citation=citation)
                    current = nxt
                    continue
                if 200 <= status < 300:
                    return FetchResult("fetched", url, status, current, body, hops, terms_citation=citation)
                return FetchResult("http_error", url, status, current, None, hops, terms_citation=citation)
            return FetchResult("too_many_redirects", url, None, current, None, hops, terms_citation=citation)
        except _Refused as ref:
            return FetchResult(ref.outcome, url, None, current, None, hops, retry_after=ref.retry_after,
                               error=ref.error, terms_citation=citation)
        except httpx.HTTPError as exc:
            return FetchResult("error", url, None, current, None, hops, error=f"{type(exc).__name__}: {exc}",
                               terms_citation=citation)
