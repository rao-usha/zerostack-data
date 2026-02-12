"""
DNS MX Record Verifier for Email Inference.

Checks whether a domain has valid MX records before inferring email addresses.
Uses per-domain caching with 24-hour TTL to avoid redundant DNS lookups.
"""

import logging
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# Cache TTL: 24 hours (domains rarely change MX records)
MX_CACHE_TTL_SECONDS = 86400


@dataclass
class MXVerificationResult:
    """Result of an MX record verification."""
    has_mx: bool
    mx_records: List[str] = field(default_factory=list)
    error: Optional[str] = None


class MXVerifier:
    """
    DNS MX record verifier with per-domain caching.

    Checks whether a domain has valid mail exchange records,
    indicating it can receive email. Results are cached for 24 hours.
    """

    def __init__(self, cache_ttl: int = MX_CACHE_TTL_SECONDS):
        self._cache: Dict[str, Tuple[MXVerificationResult, float]] = {}
        self._cache_ttl = cache_ttl

    def verify_domain(self, domain: str) -> MXVerificationResult:
        """
        Check if a domain has valid MX records.

        Args:
            domain: The email domain to check (e.g., "prudential.com")

        Returns:
            MXVerificationResult with has_mx flag and MX records list.
            On DNS failure, assumes domain is valid (graceful degradation).
        """
        if not domain:
            return MXVerificationResult(has_mx=False, error="Empty domain")

        domain = domain.lower().strip()

        # Check cache
        cached = self._get_cached(domain)
        if cached is not None:
            return cached

        # Perform DNS lookup
        result = self._dns_lookup(domain)

        # Cache result
        self._cache[domain] = (result, time.time())

        return result

    def _get_cached(self, domain: str) -> Optional[MXVerificationResult]:
        """Return cached result if still valid, None otherwise."""
        if domain not in self._cache:
            return None

        result, timestamp = self._cache[domain]
        if time.time() - timestamp > self._cache_ttl:
            del self._cache[domain]
            return None

        return result

    def _dns_lookup(self, domain: str) -> MXVerificationResult:
        """Perform actual DNS MX lookup."""
        try:
            import dns.resolver

            answers = dns.resolver.resolve(domain, "MX")
            mx_records = sorted(
                [(r.preference, str(r.exchange).rstrip(".")) for r in answers],
                key=lambda x: x[0],
            )
            record_strings = [f"{pref} {exchange}" for pref, exchange in mx_records]

            logger.debug(f"MX records for {domain}: {record_strings}")
            return MXVerificationResult(
                has_mx=True,
                mx_records=record_strings,
            )

        except ImportError:
            logger.warning("dnspython not installed — assuming domain is valid")
            return MXVerificationResult(has_mx=True, error="dnspython not installed")

        except Exception as e:
            error_type = type(e).__name__
            # NXDOMAIN = domain doesn't exist, NoAnswer = no MX records
            if "NXDOMAIN" in error_type or "NXDOMAIN" in str(e):
                logger.debug(f"Domain {domain} does not exist (NXDOMAIN)")
                return MXVerificationResult(has_mx=False, error="Domain does not exist")
            elif "NoAnswer" in error_type or "NoAnswer" in str(e):
                # No MX records but domain exists — might still accept email via A record
                logger.debug(f"No MX records for {domain}, but domain exists")
                return MXVerificationResult(has_mx=True, error="No MX records (A record fallback)")
            elif "NoNameservers" in error_type or "NoNameservers" in str(e):
                logger.debug(f"No nameservers for {domain}")
                return MXVerificationResult(has_mx=False, error="No nameservers")
            else:
                # Graceful degradation: on unexpected DNS errors, assume valid
                logger.warning(f"DNS lookup failed for {domain}: {error_type}: {e}")
                return MXVerificationResult(has_mx=True, error=f"DNS error: {error_type}")

    def clear_cache(self) -> int:
        """Clear the MX cache. Returns number of entries cleared."""
        count = len(self._cache)
        self._cache.clear()
        return count

    @property
    def cache_size(self) -> int:
        """Number of domains currently cached."""
        return len(self._cache)
