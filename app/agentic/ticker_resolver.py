"""
Ticker Resolver - Resolve stock tickers to company names.

Uses yfinance for ticker lookups with LRU caching.
Falls back to SEC EDGAR company search if yfinance fails.
"""
import asyncio
import logging
import re
from functools import lru_cache
from typing import Dict, List, Optional, Tuple

import httpx

logger = logging.getLogger(__name__)

# Try to import yfinance
try:
    import yfinance as yf
    YFINANCE_AVAILABLE = True
except ImportError:
    YFINANCE_AVAILABLE = False
    yf = None


# Common ticker suffixes to handle
TICKER_SUFFIXES = [".A", ".B", "-A", "-B", "-PR", "-WT", "-WS", "-UN"]

# Cache for resolved tickers
_ticker_cache: Dict[str, Optional[str]] = {}
MAX_CACHE_SIZE = 2000


def normalize_ticker(ticker: str) -> str:
    """Normalize ticker symbol for lookup."""
    ticker = ticker.upper().strip()
    # Remove common suffixes for lookup
    for suffix in TICKER_SUFFIXES:
        if ticker.endswith(suffix):
            return ticker[:-len(suffix)]
    return ticker


@lru_cache(maxsize=1000)
def resolve_ticker_sync(ticker: str) -> Optional[str]:
    """
    Resolve a ticker to company name (synchronous, cached).

    Args:
        ticker: Stock ticker symbol (e.g., "AAPL")

    Returns:
        Company name or None if not found
    """
    if not YFINANCE_AVAILABLE:
        return None

    normalized = normalize_ticker(ticker)

    try:
        stock = yf.Ticker(normalized)
        info = stock.info

        # Try different fields for company name
        name = info.get("longName") or info.get("shortName")

        if name:
            logger.debug(f"Resolved {ticker} -> {name}")
            return name

    except Exception as e:
        logger.debug(f"yfinance lookup failed for {ticker}: {e}")

    return None


async def resolve_ticker(ticker: str) -> Optional[str]:
    """
    Resolve a ticker to company name (async wrapper).

    Args:
        ticker: Stock ticker symbol

    Returns:
        Company name or None if not found
    """
    # Check cache first
    if ticker in _ticker_cache:
        return _ticker_cache[ticker]

    # Run sync function in thread pool
    loop = asyncio.get_event_loop()
    name = await loop.run_in_executor(None, resolve_ticker_sync, ticker)

    # Cache result (even if None)
    if len(_ticker_cache) < MAX_CACHE_SIZE:
        _ticker_cache[ticker] = name

    return name


async def resolve_tickers_batch(tickers: List[str]) -> Dict[str, Optional[str]]:
    """
    Resolve multiple tickers in batch (with concurrency limit).

    Args:
        tickers: List of ticker symbols

    Returns:
        Dict mapping ticker -> company name (or None)
    """
    results = {}
    unique_tickers = list(set(tickers))

    # Process in batches to avoid overwhelming yfinance
    batch_size = 10
    semaphore = asyncio.Semaphore(5)

    async def resolve_with_limit(ticker: str) -> Tuple[str, Optional[str]]:
        async with semaphore:
            name = await resolve_ticker(ticker)
            return ticker, name

    # Process all tickers
    tasks = [resolve_with_limit(t) for t in unique_tickers]
    for coro in asyncio.as_completed(tasks):
        ticker, name = await coro
        results[ticker] = name

    return results


async def resolve_cusip(cusip: str) -> Optional[str]:
    """
    Resolve CUSIP to company name via SEC EDGAR.

    Args:
        cusip: 9-character CUSIP identifier

    Returns:
        Company name or None if not found
    """
    if not cusip or len(cusip) < 6:
        return None

    # SEC EDGAR CUSIP lookup
    base_cusip = cusip[:6]  # Issuer ID

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            # Search SEC EDGAR for CUSIP
            url = f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={base_cusip}&type=&dateb=&owner=exclude&count=1&output=atom"

            response = await client.get(
                url,
                headers={
                    "User-Agent": "Nexdata Research Bot (research@example.com)"
                }
            )

            if response.status_code == 200:
                # Parse company name from response
                text = response.text
                # Look for company name in the response
                match = re.search(r'<title>([^<]+)</title>', text)
                if match:
                    title = match.group(1)
                    # Extract company name from title
                    if " - " in title:
                        return title.split(" - ")[0].strip()

    except Exception as e:
        logger.debug(f"CUSIP lookup failed for {cusip}: {e}")

    return None


class TickerResolver:
    """
    Batch ticker resolver with caching and fallback strategies.

    Usage:
        resolver = TickerResolver()
        holdings = await resolver.resolve_holdings(sec_13f_holdings)
    """

    def __init__(self, use_cusip_fallback: bool = True):
        """
        Initialize resolver.

        Args:
            use_cusip_fallback: Try CUSIP lookup if ticker fails
        """
        self.use_cusip_fallback = use_cusip_fallback
        self._resolved_count = 0
        self._failed_count = 0

    async def resolve_holdings(
        self,
        holdings: List[Dict],
        ticker_field: str = "ticker",
        cusip_field: str = "cusip",
        name_field: str = "company_name",
    ) -> List[Dict]:
        """
        Resolve company names for a list of holdings.

        Args:
            holdings: List of holding dicts from SEC 13F
            ticker_field: Field name containing ticker
            cusip_field: Field name containing CUSIP
            name_field: Field name to store resolved name

        Returns:
            Holdings list with company names resolved
        """
        # Collect tickers to resolve
        tickers_to_resolve = []
        for h in holdings:
            ticker = h.get(ticker_field)
            if ticker and not h.get(name_field):
                tickers_to_resolve.append(ticker)

        # Batch resolve tickers
        if tickers_to_resolve:
            resolved = await resolve_tickers_batch(tickers_to_resolve)
        else:
            resolved = {}

        # Update holdings with resolved names
        for h in holdings:
            if h.get(name_field):
                # Already has name
                continue

            ticker = h.get(ticker_field)
            cusip = h.get(cusip_field)

            # Try ticker resolution
            if ticker and ticker in resolved and resolved[ticker]:
                h[name_field] = resolved[ticker]
                self._resolved_count += 1
                continue

            # Try CUSIP fallback
            if self.use_cusip_fallback and cusip:
                name = await resolve_cusip(cusip)
                if name:
                    h[name_field] = name
                    self._resolved_count += 1
                    continue

            # Use ticker as fallback name
            if ticker:
                h[name_field] = ticker
                self._failed_count += 1

        logger.info(
            f"Ticker resolution: {self._resolved_count} resolved, "
            f"{self._failed_count} using ticker as name"
        )

        return holdings

    @property
    def stats(self) -> Dict[str, int]:
        """Get resolution statistics."""
        return {
            "resolved": self._resolved_count,
            "failed": self._failed_count,
            "cache_size": len(_ticker_cache),
        }


def clear_cache():
    """Clear the ticker cache."""
    global _ticker_cache
    _ticker_cache = {}
    resolve_ticker_sync.cache_clear()
