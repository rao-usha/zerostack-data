"""
Unit tests for Ticker Resolver.

Tests the ticker resolution functionality with mocked yfinance and httpx.
"""
import asyncio
import pytest
from unittest.mock import Mock, MagicMock, patch, AsyncMock
from typing import Dict, Optional

# Import the module under test
from app.agentic.ticker_resolver import (
    normalize_ticker,
    resolve_ticker_sync,
    resolve_ticker,
    resolve_tickers_batch,
    resolve_cusip,
    TickerResolver,
    clear_cache,
    _ticker_cache,
    YFINANCE_AVAILABLE,
)


# =============================================================================
# Test: normalize_ticker
# =============================================================================

class TestNormalizeTicker:
    """Tests for the normalize_ticker function."""

    def test_uppercase_and_strip(self):
        """Should uppercase and strip whitespace."""
        assert normalize_ticker("aapl") == "AAPL"
        assert normalize_ticker("  msft  ") == "MSFT"
        assert normalize_ticker("gOoG") == "GOOG"

    def test_removes_class_a_suffix(self):
        """Should remove .A class share suffix."""
        assert normalize_ticker("BRK.A") == "BRK"
        assert normalize_ticker("brk.a") == "BRK"

    def test_removes_class_b_suffix(self):
        """Should remove .B class share suffix."""
        assert normalize_ticker("BRK.B") == "BRK"

    def test_removes_dash_suffixes(self):
        """Should remove -A, -B, -PR, -WT, -WS, -UN suffixes."""
        assert normalize_ticker("TEST-A") == "TEST"
        assert normalize_ticker("TEST-B") == "TEST"
        assert normalize_ticker("TEST-PR") == "TEST"
        assert normalize_ticker("TEST-WT") == "TEST"
        assert normalize_ticker("TEST-WS") == "TEST"
        assert normalize_ticker("TEST-UN") == "TEST"

    def test_preserves_ticker_without_suffix(self):
        """Should preserve tickers without known suffixes."""
        assert normalize_ticker("AAPL") == "AAPL"
        assert normalize_ticker("GOOGL") == "GOOGL"
        assert normalize_ticker("TSLA") == "TSLA"


# =============================================================================
# Test: resolve_ticker_sync (with mocked yfinance)
# =============================================================================

class TestResolveTickerSync:
    """Tests for synchronous ticker resolution."""

    def setup_method(self):
        """Clear caches before each test."""
        clear_cache()

    @patch("app.agentic.ticker_resolver.YFINANCE_AVAILABLE", False)
    def test_returns_none_when_yfinance_unavailable(self):
        """Should return None when yfinance is not installed."""
        # Need to reimport to get the patched value
        from app.agentic import ticker_resolver
        original = ticker_resolver.YFINANCE_AVAILABLE
        ticker_resolver.YFINANCE_AVAILABLE = False
        try:
            # Clear the lru_cache to force re-evaluation
            resolve_ticker_sync.cache_clear()
            result = resolve_ticker_sync("AAPL")
            assert result is None
        finally:
            ticker_resolver.YFINANCE_AVAILABLE = original

    @patch("app.agentic.ticker_resolver.yf")
    @patch("app.agentic.ticker_resolver.YFINANCE_AVAILABLE", True)
    def test_resolves_ticker_with_long_name(self, mock_yf):
        """Should resolve ticker using longName from yfinance."""
        from app.agentic import ticker_resolver
        ticker_resolver.YFINANCE_AVAILABLE = True

        # Setup mock
        mock_ticker = Mock()
        mock_ticker.info = {"longName": "Apple Inc.", "shortName": "Apple"}
        mock_yf.Ticker.return_value = mock_ticker

        resolve_ticker_sync.cache_clear()
        result = resolve_ticker_sync("AAPL")

        assert result == "Apple Inc."
        mock_yf.Ticker.assert_called_once_with("AAPL")

    @patch("app.agentic.ticker_resolver.yf")
    @patch("app.agentic.ticker_resolver.YFINANCE_AVAILABLE", True)
    def test_resolves_ticker_with_short_name_fallback(self, mock_yf):
        """Should fall back to shortName if longName is missing."""
        from app.agentic import ticker_resolver
        ticker_resolver.YFINANCE_AVAILABLE = True

        mock_ticker = Mock()
        mock_ticker.info = {"longName": None, "shortName": "Microsoft Corp"}
        mock_yf.Ticker.return_value = mock_ticker

        resolve_ticker_sync.cache_clear()
        result = resolve_ticker_sync("MSFT")

        assert result == "Microsoft Corp"

    @patch("app.agentic.ticker_resolver.yf")
    @patch("app.agentic.ticker_resolver.YFINANCE_AVAILABLE", True)
    def test_returns_none_when_no_name_found(self, mock_yf):
        """Should return None when neither longName nor shortName available."""
        from app.agentic import ticker_resolver
        ticker_resolver.YFINANCE_AVAILABLE = True

        mock_ticker = Mock()
        mock_ticker.info = {"longName": None, "shortName": None}
        mock_yf.Ticker.return_value = mock_ticker

        resolve_ticker_sync.cache_clear()
        result = resolve_ticker_sync("UNKNOWN")

        assert result is None

    @patch("app.agentic.ticker_resolver.yf")
    @patch("app.agentic.ticker_resolver.YFINANCE_AVAILABLE", True)
    def test_handles_yfinance_exception(self, mock_yf):
        """Should return None and not raise when yfinance throws."""
        from app.agentic import ticker_resolver
        ticker_resolver.YFINANCE_AVAILABLE = True

        mock_yf.Ticker.side_effect = Exception("Network error")

        resolve_ticker_sync.cache_clear()
        result = resolve_ticker_sync("ERROR")

        assert result is None

    @patch("app.agentic.ticker_resolver.yf")
    @patch("app.agentic.ticker_resolver.YFINANCE_AVAILABLE", True)
    def test_normalizes_ticker_before_lookup(self, mock_yf):
        """Should normalize ticker (e.g., BRK.A -> BRK) before lookup."""
        from app.agentic import ticker_resolver
        ticker_resolver.YFINANCE_AVAILABLE = True

        mock_ticker = Mock()
        mock_ticker.info = {"longName": "Berkshire Hathaway"}
        mock_yf.Ticker.return_value = mock_ticker

        resolve_ticker_sync.cache_clear()
        result = resolve_ticker_sync("brk.a")

        assert result == "Berkshire Hathaway"
        mock_yf.Ticker.assert_called_once_with("BRK")


# =============================================================================
# Test: resolve_ticker (async wrapper)
# =============================================================================

class TestResolveTickerAsync:
    """Tests for async ticker resolution."""

    def setup_method(self):
        """Clear caches before each test."""
        clear_cache()

    @pytest.mark.asyncio
    async def test_returns_cached_result(self):
        """Should return cached result without calling resolve_ticker_sync."""
        # Pre-populate cache
        from app.agentic import ticker_resolver
        ticker_resolver._ticker_cache["CACHED"] = "Cached Company"

        result = await resolve_ticker("CACHED")

        assert result == "Cached Company"

    @pytest.mark.asyncio
    @patch("app.agentic.ticker_resolver.resolve_ticker_sync")
    async def test_calls_sync_function_and_caches(self, mock_sync):
        """Should call sync function and cache the result."""
        mock_sync.return_value = "Test Company"

        from app.agentic import ticker_resolver
        ticker_resolver._ticker_cache.clear()

        result = await resolve_ticker("TEST")

        assert result == "Test Company"
        mock_sync.assert_called_once_with("TEST")
        assert ticker_resolver._ticker_cache["TEST"] == "Test Company"

    @pytest.mark.asyncio
    @patch("app.agentic.ticker_resolver.resolve_ticker_sync")
    async def test_caches_none_results(self, mock_sync):
        """Should cache None results to avoid repeated lookups."""
        mock_sync.return_value = None

        from app.agentic import ticker_resolver
        ticker_resolver._ticker_cache.clear()

        result = await resolve_ticker("NOTFOUND")

        assert result is None
        assert "NOTFOUND" in ticker_resolver._ticker_cache
        assert ticker_resolver._ticker_cache["NOTFOUND"] is None


# =============================================================================
# Test: resolve_tickers_batch
# =============================================================================

class TestResolveTickersBatch:
    """Tests for batch ticker resolution."""

    def setup_method(self):
        """Clear caches before each test."""
        clear_cache()

    @pytest.mark.asyncio
    @patch("app.agentic.ticker_resolver.resolve_ticker")
    async def test_resolves_multiple_tickers(self, mock_resolve):
        """Should resolve multiple tickers concurrently."""
        async def mock_resolve_fn(ticker):
            return f"{ticker} Inc."

        mock_resolve.side_effect = mock_resolve_fn

        result = await resolve_tickers_batch(["AAPL", "MSFT", "GOOGL"])

        assert result == {
            "AAPL": "AAPL Inc.",
            "MSFT": "MSFT Inc.",
            "GOOGL": "GOOGL Inc.",
        }
        assert mock_resolve.call_count == 3

    @pytest.mark.asyncio
    @patch("app.agentic.ticker_resolver.resolve_ticker")
    async def test_deduplicates_tickers(self, mock_resolve):
        """Should deduplicate tickers before resolving."""
        async def mock_resolve_fn(ticker):
            return f"{ticker} Inc."

        mock_resolve.side_effect = mock_resolve_fn

        result = await resolve_tickers_batch(["AAPL", "AAPL", "MSFT", "MSFT"])

        # Should only resolve each unique ticker once
        assert mock_resolve.call_count == 2
        assert "AAPL" in result
        assert "MSFT" in result

    @pytest.mark.asyncio
    @patch("app.agentic.ticker_resolver.resolve_ticker")
    async def test_handles_empty_list(self, mock_resolve):
        """Should handle empty ticker list."""
        result = await resolve_tickers_batch([])

        assert result == {}
        mock_resolve.assert_not_called()

    @pytest.mark.asyncio
    @patch("app.agentic.ticker_resolver.resolve_ticker")
    async def test_handles_partial_failures(self, mock_resolve):
        """Should handle mix of successful and failed resolutions."""
        async def mock_resolve_fn(ticker):
            if ticker == "FAIL":
                return None
            return f"{ticker} Inc."

        mock_resolve.side_effect = mock_resolve_fn

        result = await resolve_tickers_batch(["AAPL", "FAIL", "MSFT"])

        assert result["AAPL"] == "AAPL Inc."
        assert result["FAIL"] is None
        assert result["MSFT"] == "MSFT Inc."


# =============================================================================
# Test: resolve_cusip (SEC EDGAR fallback)
# =============================================================================

class TestResolveCusip:
    """Tests for CUSIP resolution via SEC EDGAR."""

    @pytest.mark.asyncio
    async def test_returns_none_for_empty_cusip(self):
        """Should return None for empty or None CUSIP."""
        assert await resolve_cusip("") is None
        assert await resolve_cusip(None) is None

    @pytest.mark.asyncio
    async def test_returns_none_for_short_cusip(self):
        """Should return None for CUSIP shorter than 6 characters."""
        assert await resolve_cusip("12345") is None
        assert await resolve_cusip("ABC") is None

    @pytest.mark.asyncio
    @patch("httpx.AsyncClient")
    async def test_resolves_cusip_from_sec_edgar(self, mock_client_class):
        """Should resolve CUSIP via SEC EDGAR API."""
        # Setup mock response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = "<title>Apple Inc. - CIK 0000320193</title>"

        mock_client = AsyncMock()
        mock_client.get.return_value = mock_response
        mock_client.__aenter__.return_value = mock_client
        mock_client.__aexit__.return_value = None
        mock_client_class.return_value = mock_client

        result = await resolve_cusip("037833100")  # Apple CUSIP

        assert result == "Apple Inc."

    @pytest.mark.asyncio
    @patch("httpx.AsyncClient")
    async def test_uses_first_6_chars_as_issuer_id(self, mock_client_class):
        """Should use first 6 characters of CUSIP as issuer ID."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = "<title>Test Company - CIK 123</title>"

        mock_client = AsyncMock()
        mock_client.get.return_value = mock_response
        mock_client.__aenter__.return_value = mock_client
        mock_client.__aexit__.return_value = None
        mock_client_class.return_value = mock_client

        await resolve_cusip("123456789")

        # Verify the URL uses first 6 chars
        call_args = mock_client.get.call_args
        assert "123456" in call_args[0][0]

    @pytest.mark.asyncio
    @patch("httpx.AsyncClient")
    async def test_returns_none_on_non_200_response(self, mock_client_class):
        """Should return None when SEC EDGAR returns non-200 status."""
        mock_response = Mock()
        mock_response.status_code = 404
        mock_response.text = "Not Found"

        mock_client = AsyncMock()
        mock_client.get.return_value = mock_response
        mock_client.__aenter__.return_value = mock_client
        mock_client.__aexit__.return_value = None
        mock_client_class.return_value = mock_client

        result = await resolve_cusip("000000000")

        assert result is None

    @pytest.mark.asyncio
    @patch("httpx.AsyncClient")
    async def test_returns_none_when_no_title_match(self, mock_client_class):
        """Should return None when response has no parseable title."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = "<html><body>No title here</body></html>"

        mock_client = AsyncMock()
        mock_client.get.return_value = mock_response
        mock_client.__aenter__.return_value = mock_client
        mock_client.__aexit__.return_value = None
        mock_client_class.return_value = mock_client

        result = await resolve_cusip("123456789")

        assert result is None

    @pytest.mark.asyncio
    @patch("httpx.AsyncClient")
    async def test_handles_network_exception(self, mock_client_class):
        """Should return None and not raise on network errors."""
        mock_client = AsyncMock()
        mock_client.get.side_effect = Exception("Connection timeout")
        mock_client.__aenter__.return_value = mock_client
        mock_client.__aexit__.return_value = None
        mock_client_class.return_value = mock_client

        result = await resolve_cusip("123456789")

        assert result is None

    @pytest.mark.asyncio
    @patch("httpx.AsyncClient")
    async def test_handles_httpx_timeout_error(self, mock_client_class):
        """Should handle httpx timeout errors gracefully."""
        import httpx

        mock_client = AsyncMock()
        mock_client.get.side_effect = httpx.TimeoutException("Timeout")
        mock_client.__aenter__.return_value = mock_client
        mock_client.__aexit__.return_value = None
        mock_client_class.return_value = mock_client

        result = await resolve_cusip("123456789")

        assert result is None


# =============================================================================
# Test: TickerResolver class
# =============================================================================

class TestTickerResolverClass:
    """Tests for the TickerResolver class."""

    def setup_method(self):
        """Clear caches before each test."""
        clear_cache()

    def test_init_defaults(self):
        """Should initialize with default settings."""
        resolver = TickerResolver()
        assert resolver.use_cusip_fallback is True
        assert resolver._resolved_count == 0
        assert resolver._failed_count == 0

    def test_init_cusip_fallback_disabled(self):
        """Should allow disabling CUSIP fallback."""
        resolver = TickerResolver(use_cusip_fallback=False)
        assert resolver.use_cusip_fallback is False

    @pytest.mark.asyncio
    @patch("app.agentic.ticker_resolver.resolve_tickers_batch")
    async def test_resolve_holdings_with_tickers(self, mock_batch):
        """Should resolve company names from tickers."""
        mock_batch.return_value = {
            "AAPL": "Apple Inc.",
            "MSFT": "Microsoft Corporation",
        }

        resolver = TickerResolver()
        holdings = [
            {"ticker": "AAPL", "shares": 100},
            {"ticker": "MSFT", "shares": 200},
        ]

        result = await resolver.resolve_holdings(holdings)

        assert result[0]["company_name"] == "Apple Inc."
        assert result[1]["company_name"] == "Microsoft Corporation"
        assert resolver._resolved_count == 2

    @pytest.mark.asyncio
    @patch("app.agentic.ticker_resolver.resolve_tickers_batch")
    async def test_resolve_holdings_skips_existing_names(self, mock_batch):
        """Should skip holdings that already have company names."""
        mock_batch.return_value = {}

        resolver = TickerResolver()
        holdings = [
            {"ticker": "AAPL", "company_name": "Apple Inc.", "shares": 100},
        ]

        result = await resolver.resolve_holdings(holdings)

        assert result[0]["company_name"] == "Apple Inc."
        # Should NOT call batch resolve when all holdings already have names
        # The code checks: if tickers_to_resolve: ... else: resolved = {}
        mock_batch.assert_not_called()

    @pytest.mark.asyncio
    @patch("app.agentic.ticker_resolver.resolve_tickers_batch")
    @patch("app.agentic.ticker_resolver.resolve_cusip")
    async def test_resolve_holdings_cusip_fallback(self, mock_cusip, mock_batch):
        """Should fall back to CUSIP when ticker resolution fails."""
        mock_batch.return_value = {"UNKNOWN": None}
        mock_cusip.return_value = "Unknown Corp via CUSIP"

        resolver = TickerResolver(use_cusip_fallback=True)
        holdings = [
            {"ticker": "UNKNOWN", "cusip": "123456789", "shares": 100},
        ]

        result = await resolver.resolve_holdings(holdings)

        assert result[0]["company_name"] == "Unknown Corp via CUSIP"
        mock_cusip.assert_called_once_with("123456789")
        assert resolver._resolved_count == 1

    @pytest.mark.asyncio
    @patch("app.agentic.ticker_resolver.resolve_tickers_batch")
    @patch("app.agentic.ticker_resolver.resolve_cusip")
    async def test_resolve_holdings_no_cusip_fallback_when_disabled(self, mock_cusip, mock_batch):
        """Should not use CUSIP fallback when disabled."""
        mock_batch.return_value = {"UNKNOWN": None}

        resolver = TickerResolver(use_cusip_fallback=False)
        holdings = [
            {"ticker": "UNKNOWN", "cusip": "123456789", "shares": 100},
        ]

        result = await resolver.resolve_holdings(holdings)

        # Should use ticker as fallback name
        assert result[0]["company_name"] == "UNKNOWN"
        mock_cusip.assert_not_called()
        assert resolver._failed_count == 1

    @pytest.mark.asyncio
    @patch("app.agentic.ticker_resolver.resolve_tickers_batch")
    async def test_resolve_holdings_uses_ticker_as_last_resort(self, mock_batch):
        """Should use ticker as company name when all else fails."""
        mock_batch.return_value = {"NOTFOUND": None}

        resolver = TickerResolver(use_cusip_fallback=False)
        holdings = [
            {"ticker": "NOTFOUND", "shares": 100},
        ]

        result = await resolver.resolve_holdings(holdings)

        assert result[0]["company_name"] == "NOTFOUND"
        assert resolver._failed_count == 1

    @pytest.mark.asyncio
    @patch("app.agentic.ticker_resolver.resolve_tickers_batch")
    async def test_resolve_holdings_custom_field_names(self, mock_batch):
        """Should support custom field names."""
        mock_batch.return_value = {"AAPL": "Apple Inc."}

        resolver = TickerResolver()
        holdings = [
            {"symbol": "AAPL", "cusip_id": "037833100", "qty": 100},
        ]

        result = await resolver.resolve_holdings(
            holdings,
            ticker_field="symbol",
            cusip_field="cusip_id",
            name_field="issuer_name",
        )

        assert result[0]["issuer_name"] == "Apple Inc."

    @pytest.mark.asyncio
    @patch("app.agentic.ticker_resolver.resolve_tickers_batch")
    async def test_resolve_holdings_empty_list(self, mock_batch):
        """Should handle empty holdings list."""
        resolver = TickerResolver()
        result = await resolver.resolve_holdings([])

        assert result == []
        mock_batch.assert_not_called()

    def test_stats_property(self):
        """Should return resolution statistics."""
        from app.agentic import ticker_resolver
        ticker_resolver._ticker_cache.clear()
        ticker_resolver._ticker_cache["A"] = "Company A"
        ticker_resolver._ticker_cache["B"] = "Company B"

        resolver = TickerResolver()
        resolver._resolved_count = 5
        resolver._failed_count = 2

        stats = resolver.stats

        assert stats["resolved"] == 5
        assert stats["failed"] == 2
        assert stats["cache_size"] == 2


# =============================================================================
# Test: Cache behavior
# =============================================================================

class TestCacheBehavior:
    """Tests for cache management."""

    def setup_method(self):
        """Clear caches before each test."""
        clear_cache()

    def test_clear_cache_empties_ticker_cache(self):
        """Should clear the module-level ticker cache."""
        from app.agentic import ticker_resolver
        ticker_resolver._ticker_cache["TEST"] = "Test Company"

        clear_cache()

        assert len(ticker_resolver._ticker_cache) == 0

    def test_clear_cache_clears_lru_cache(self):
        """Should clear the lru_cache on resolve_ticker_sync."""
        # This is tested implicitly by checking cache_info
        clear_cache()
        info = resolve_ticker_sync.cache_info()
        assert info.currsize == 0

    @pytest.mark.asyncio
    async def test_cache_hit_avoids_sync_call(self):
        """Cache hit should return immediately without calling sync resolver."""
        from app.agentic import ticker_resolver
        ticker_resolver._ticker_cache["CACHED"] = "Cached Result"

        # If resolve_ticker_sync is called, the test would fail
        # because we're not mocking it
        with patch("app.agentic.ticker_resolver.resolve_ticker_sync") as mock_sync:
            result = await resolve_ticker("CACHED")

            assert result == "Cached Result"
            mock_sync.assert_not_called()

    @pytest.mark.asyncio
    @patch("app.agentic.ticker_resolver.resolve_ticker_sync")
    async def test_cache_miss_calls_sync_resolver(self, mock_sync):
        """Cache miss should call the sync resolver."""
        mock_sync.return_value = "New Company"

        from app.agentic import ticker_resolver
        ticker_resolver._ticker_cache.clear()

        result = await resolve_ticker("NEW")

        assert result == "New Company"
        mock_sync.assert_called_once_with("NEW")

    @pytest.mark.asyncio
    @patch("app.agentic.ticker_resolver.resolve_ticker_sync")
    async def test_cache_respects_max_size(self, mock_sync):
        """Should respect MAX_CACHE_SIZE limit."""
        from app.agentic import ticker_resolver

        # Fill cache to just under limit
        ticker_resolver._ticker_cache.clear()
        original_max = ticker_resolver.MAX_CACHE_SIZE
        ticker_resolver.MAX_CACHE_SIZE = 3

        try:
            # Add entries up to limit
            ticker_resolver._ticker_cache["A"] = "A"
            ticker_resolver._ticker_cache["B"] = "B"
            ticker_resolver._ticker_cache["C"] = "C"

            mock_sync.return_value = "D Company"

            # Try to add one more
            await resolve_ticker("D")

            # Should not be cached because we're at limit
            assert "D" not in ticker_resolver._ticker_cache
        finally:
            ticker_resolver.MAX_CACHE_SIZE = original_max


# =============================================================================
# Test: Error handling
# =============================================================================

class TestErrorHandling:
    """Tests for error handling scenarios."""

    def setup_method(self):
        """Clear caches before each test."""
        clear_cache()

    @pytest.mark.asyncio
    @patch("app.agentic.ticker_resolver.yf")
    @patch("app.agentic.ticker_resolver.YFINANCE_AVAILABLE", True)
    async def test_handles_yfinance_timeout(self, mock_yf):
        """Should handle yfinance timeout gracefully."""
        from app.agentic import ticker_resolver
        ticker_resolver.YFINANCE_AVAILABLE = True

        mock_yf.Ticker.side_effect = TimeoutError("Request timed out")
        resolve_ticker_sync.cache_clear()

        result = await resolve_ticker("TIMEOUT")

        assert result is None

    @pytest.mark.asyncio
    @patch("app.agentic.ticker_resolver.yf")
    @patch("app.agentic.ticker_resolver.YFINANCE_AVAILABLE", True)
    async def test_handles_yfinance_connection_error(self, mock_yf):
        """Should handle connection errors gracefully."""
        from app.agentic import ticker_resolver
        ticker_resolver.YFINANCE_AVAILABLE = True

        mock_yf.Ticker.side_effect = ConnectionError("Network unreachable")
        resolve_ticker_sync.cache_clear()

        result = await resolve_ticker("CONNFAIL")

        assert result is None

    @pytest.mark.asyncio
    @patch("app.agentic.ticker_resolver.yf")
    @patch("app.agentic.ticker_resolver.YFINANCE_AVAILABLE", True)
    async def test_handles_invalid_ticker_format(self, mock_yf):
        """Should handle invalid ticker formats."""
        from app.agentic import ticker_resolver
        ticker_resolver.YFINANCE_AVAILABLE = True

        mock_ticker = Mock()
        mock_ticker.info = {}  # Empty info for invalid ticker
        mock_yf.Ticker.return_value = mock_ticker
        resolve_ticker_sync.cache_clear()

        result = await resolve_ticker("!!INVALID!!")

        assert result is None

    @pytest.mark.asyncio
    @patch("app.agentic.ticker_resolver.resolve_tickers_batch")
    @patch("app.agentic.ticker_resolver.resolve_cusip")
    async def test_resolver_handles_both_failures(self, mock_cusip, mock_batch):
        """Should handle case where both ticker and CUSIP resolution fail."""
        mock_batch.return_value = {"FAIL": None}
        mock_cusip.return_value = None

        resolver = TickerResolver(use_cusip_fallback=True)
        holdings = [
            {"ticker": "FAIL", "cusip": "000000000", "shares": 100},
        ]

        result = await resolver.resolve_holdings(holdings)

        # Should use ticker as fallback
        assert result[0]["company_name"] == "FAIL"
        assert resolver._failed_count == 1

    @pytest.mark.asyncio
    @patch("app.agentic.ticker_resolver.resolve_tickers_batch")
    async def test_resolver_handles_missing_ticker_field(self, mock_batch):
        """Should handle holdings without ticker field."""
        mock_batch.return_value = {}

        resolver = TickerResolver()
        holdings = [
            {"cusip": "123456789", "shares": 100},  # No ticker
        ]

        result = await resolver.resolve_holdings(holdings)

        # Should not crash, holding remains unchanged
        assert "company_name" not in result[0] or result[0].get("company_name") is None


# =============================================================================
# Test: Integration scenarios
# =============================================================================

class TestIntegrationScenarios:
    """Integration-style tests combining multiple components."""

    def setup_method(self):
        """Clear caches before each test."""
        clear_cache()

    @pytest.mark.asyncio
    @patch("app.agentic.ticker_resolver.resolve_tickers_batch")
    @patch("app.agentic.ticker_resolver.resolve_cusip")
    async def test_mixed_resolution_sources(self, mock_cusip, mock_batch):
        """Should resolve from multiple sources in a single batch."""
        # Ticker resolution works for some
        mock_batch.return_value = {
            "AAPL": "Apple Inc.",
            "UNKNOWN": None,
        }
        # CUSIP fallback works for others
        mock_cusip.return_value = "Unknown Corp"

        resolver = TickerResolver()
        holdings = [
            {"ticker": "AAPL", "cusip": "037833100", "shares": 100},
            {"ticker": "UNKNOWN", "cusip": "123456789", "shares": 50},
            {"ticker": None, "cusip": None, "shares": 25, "company_name": "Already Named"},
        ]

        result = await resolver.resolve_holdings(holdings)

        assert result[0]["company_name"] == "Apple Inc."  # From ticker
        assert result[1]["company_name"] == "Unknown Corp"  # From CUSIP
        assert result[2]["company_name"] == "Already Named"  # Preserved

    @pytest.mark.asyncio
    @patch("app.agentic.ticker_resolver.resolve_tickers_batch")
    async def test_large_batch_processing(self, mock_batch):
        """Should handle large batches efficiently."""
        # Create a batch of 100 tickers
        tickers = [f"TICK{i:03d}" for i in range(100)]
        mock_batch.return_value = {t: f"Company {i}" for i, t in enumerate(tickers)}

        resolver = TickerResolver()
        holdings = [{"ticker": t, "shares": 100} for t in tickers]

        result = await resolver.resolve_holdings(holdings)

        assert len(result) == 100
        assert all(h.get("company_name") for h in result)
        assert resolver._resolved_count == 100
