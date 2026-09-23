"""
The caller's IP address for per-IP quotas (SPEC_127).

Browsers reach the API through the frontend nginx (`location /api/` proxies to
`api:8000`), and uvicorn runs without `--proxy-headers`, so
`request.client.host` is the nginx container's address for every visitor.
Keying a quota on it turns "per IP" into one global bucket.

nginx sets `X-Real-IP $remote_addr` (overwriting anything the client sent) and
appends `$remote_addr` to `X-Forwarded-For`. When — and only when — the direct
peer is a trusted proxy (`TRUSTED_PROXY_CIDRS`, private networks by default),
we take X-Real-IP, else the right-most X-Forwarded-For entry (the one the proxy
appended; entries to its left are client-supplied and forgeable). A direct,
untrusted peer's forwarding headers are ignored.
"""

import ipaddress
import logging
from functools import lru_cache
from typing import Optional, Tuple

from starlette.requests import Request

from app.core.config import get_settings

logger = logging.getLogger(__name__)


@lru_cache(maxsize=16)
def _parse_networks(spec: str) -> Tuple[ipaddress._BaseNetwork, ...]:
    nets = []
    for part in (spec or "").split(","):
        part = part.strip()
        if not part:
            continue
        try:
            nets.append(ipaddress.ip_network(part, strict=False))
        except ValueError:
            logger.warning("Ignoring invalid TRUSTED_PROXY_CIDRS entry: %r", part)
    return tuple(nets)


def _valid_ip(value: Optional[str]) -> Optional[str]:
    value = (value or "").strip()
    if not value:
        return None
    try:
        return str(ipaddress.ip_address(value))
    except ValueError:
        return None


def is_trusted_proxy(host: Optional[str]) -> bool:
    ip = _valid_ip(host)
    if ip is None:
        return False
    addr = ipaddress.ip_address(ip)
    return any(addr in net for net in _parse_networks(get_settings().trusted_proxy_cidrs))


def client_ip(request: Request) -> str:
    """Best-effort real client IP; 'unknown' when there is no peer."""
    peer = request.client.host if request.client else None
    if peer and is_trusted_proxy(peer):
        real = _valid_ip(request.headers.get("x-real-ip"))
        if real:
            return real
        xff = request.headers.get("x-forwarded-for") or ""
        hops = [h for h in (x.strip() for x in xff.split(",")) if h]
        if hops:
            last = _valid_ip(hops[-1])
            if last:
                return last
    return peer or "unknown"
