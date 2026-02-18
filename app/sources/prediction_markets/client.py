"""
Prediction Market API Clients.

Clients for Kalshi and Polymarket APIs.
Both provide FREE public APIs for reading market data (no authentication required).
"""
import httpx
import asyncio
import json
import logging
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta
from dataclasses import dataclass

from app.sources.prediction_markets.metadata import categorize_market, KALSHI_SERIES

logger = logging.getLogger(__name__)

# =============================================================================
# API BASE URLS
# =============================================================================

KALSHI_BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"
POLYMARKET_GAMMA_URL = "https://gamma-api.polymarket.com"
POLYMARKET_CLOB_URL = "https://clob.polymarket.com"


# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class MarketData:
    """Standardized market data from any platform."""
    source: str  # kalshi, polymarket, predictit
    market_id: str
    question: str
    description: Optional[str]
    
    # Probabilities
    yes_probability: float  # 0.0 to 1.0
    no_probability: Optional[float]
    
    # Activity
    volume_usd: Optional[float]
    volume_24h_usd: Optional[float]
    liquidity_usd: Optional[float]
    
    # Timing
    close_date: Optional[datetime]
    
    # Classification
    category: Optional[str]
    subcategory: Optional[str]
    
    # URL
    market_url: Optional[str]
    
    # Raw data for debugging
    raw_data: Optional[Dict[str, Any]] = None


# =============================================================================
# KALSHI CLIENT
# =============================================================================

class KalshiClient:
    """
    Client for Kalshi public API.
    
    Kalshi is CFTC-regulated and focuses on economic/political events.
    API docs: https://docs.kalshi.com/
    
    No authentication required for reading market data.
    """
    
    def __init__(self, timeout: int = 30, max_retries: int = 3):
        self.base_url = KALSHI_BASE_URL
        self.timeout = timeout
        self.max_retries = max_retries
        self._client: Optional[httpx.AsyncClient] = None
    
    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create HTTP client."""
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                timeout=self.timeout,
                headers={"User-Agent": "NexdataResearch/1.0"}
            )
        return self._client
    
    async def close(self):
        """Close the HTTP client."""
        if self._client and not self._client.is_closed:
            await self._client.aclose()
    
    async def _request(self, method: str, endpoint: str, params: Optional[Dict] = None) -> Dict:
        """Make an API request with retry logic."""
        client = await self._get_client()
        url = f"{self.base_url}{endpoint}"
        
        for attempt in range(self.max_retries):
            try:
                response = await client.request(method, url, params=params)
                response.raise_for_status()
                return response.json()
            except httpx.HTTPStatusError as e:
                logger.warning(f"Kalshi API error (attempt {attempt + 1}): {e.response.status_code}")
                if attempt == self.max_retries - 1:
                    raise
                await asyncio.sleep(2 ** attempt)
            except Exception as e:
                logger.error(f"Kalshi request error: {e}")
                if attempt == self.max_retries - 1:
                    raise
                await asyncio.sleep(2 ** attempt)
        
        return {}
    
    async def get_markets(
        self,
        status: str = "open",
        limit: int = 100,
        series_ticker: Optional[str] = None,
        cursor: Optional[str] = None,
    ) -> Dict:
        """
        Get list of markets from Kalshi.
        
        Args:
            status: Market status filter (open, closed, settled)
            limit: Maximum markets to return (max 1000)
            series_ticker: Filter by series (e.g., 'FED', 'CPI', 'PRES')
            cursor: Pagination cursor
            
        Returns:
            Dict with 'markets' list and 'cursor' for pagination
        """
        params = {
            "status": status,
            "limit": min(limit, 1000),
        }
        if series_ticker:
            params["series_ticker"] = series_ticker
        if cursor:
            params["cursor"] = cursor
        
        return await self._request("GET", "/markets", params)
    
    async def get_market(self, ticker: str) -> Dict:
        """
        Get single market by ticker.
        
        Args:
            ticker: Market ticker (e.g., 'KXFEDRATE-26JAN29-T4.50')
            
        Returns:
            Market data dict
        """
        return await self._request("GET", f"/markets/{ticker}")
    
    async def get_series(self, series_ticker: str) -> Dict:
        """
        Get series information.
        
        Args:
            series_ticker: Series ticker (e.g., 'FED', 'CPI')
            
        Returns:
            Series metadata dict
        """
        return await self._request("GET", f"/series/{series_ticker}")
    
    async def get_events(self, series_ticker: Optional[str] = None, status: str = "open") -> Dict:
        """
        Get events (collections of related markets).
        
        Args:
            series_ticker: Filter by series
            status: Event status filter
            
        Returns:
            Dict with 'events' list
        """
        params = {"status": status}
        if series_ticker:
            params["series_ticker"] = series_ticker
        
        return await self._request("GET", "/events", params)
    
    async def fetch_top_markets(
        self,
        categories: Optional[List[str]] = None,
        limit: int = 50,
    ) -> List[MarketData]:
        """
        Fetch top markets from Kalshi, optionally filtered by category.
        
        Args:
            categories: List of series to fetch (e.g., ['FED', 'CPI', 'PRES'])
                       If None, fetches all open markets
            limit: Max markets per category
            
        Returns:
            List of standardized MarketData objects
        """
        all_markets = []
        
        if categories:
            # Fetch specific series
            for series in categories:
                try:
                    data = await self.get_markets(series_ticker=series, limit=limit)
                    markets = data.get("markets", [])
                    all_markets.extend(markets)
                    logger.info(f"Kalshi: Fetched {len(markets)} markets from series {series}")
                    await asyncio.sleep(0.5)  # Rate limiting
                except Exception as e:
                    logger.error(f"Error fetching Kalshi series {series}: {e}")
        else:
            # Fetch all open markets
            try:
                data = await self.get_markets(limit=limit)
                all_markets = data.get("markets", [])
                logger.info(f"Kalshi: Fetched {len(all_markets)} open markets")
            except Exception as e:
                logger.error(f"Error fetching Kalshi markets: {e}")
        
        # Convert to standardized format
        return [self._parse_market(m) for m in all_markets]
    
    def _parse_market(self, raw: Dict) -> MarketData:
        """Convert Kalshi market response to standardized MarketData."""
        ticker = raw.get("ticker", "")
        title = raw.get("title", "")
        subtitle = raw.get("subtitle", "")
        question = f"{title}: {subtitle}" if subtitle else title
        
        # Parse probability from yes_bid or last_price (0-100 scale)
        yes_price = raw.get("yes_bid") or raw.get("last_price") or 0
        yes_prob = yes_price / 100 if yes_price > 1 else yes_price
        
        # Parse close date
        close_time = raw.get("close_time")
        close_date = None
        if close_time:
            try:
                close_date = datetime.fromisoformat(close_time.replace("Z", "+00:00"))
            except (ValueError, TypeError):
                pass
        
        # Auto-categorize
        cat_info = categorize_market(question)
        
        # Build market URL
        market_url = f"https://kalshi.com/markets/{ticker}" if ticker else None
        
        return MarketData(
            source="kalshi",
            market_id=ticker,
            question=question,
            description=raw.get("rules_primary"),
            yes_probability=yes_prob,
            no_probability=1 - yes_prob if yes_prob else None,
            volume_usd=raw.get("volume"),
            volume_24h_usd=raw.get("volume_24h"),
            liquidity_usd=raw.get("open_interest"),
            close_date=close_date,
            category=cat_info["category"],
            subcategory=cat_info["subcategory"],
            market_url=market_url,
            raw_data=raw,
        )


# =============================================================================
# POLYMARKET CLIENT
# =============================================================================

class PolymarketClient:
    """
    Client for Polymarket public API.
    
    Polymarket is a crypto-based prediction market with global coverage.
    Uses Gamma API for market discovery and CLOB API for prices.
    
    No authentication required for reading market data.
    """
    
    def __init__(self, timeout: int = 30, max_retries: int = 3):
        self.gamma_url = POLYMARKET_GAMMA_URL
        self.clob_url = POLYMARKET_CLOB_URL
        self.timeout = timeout
        self.max_retries = max_retries
        self._client: Optional[httpx.AsyncClient] = None
    
    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create HTTP client."""
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                timeout=self.timeout,
                headers={"User-Agent": "NexdataResearch/1.0"}
            )
        return self._client
    
    async def close(self):
        """Close the HTTP client."""
        if self._client and not self._client.is_closed:
            await self._client.aclose()
    
    async def _request(self, base_url: str, endpoint: str, params: Optional[Dict] = None) -> Any:
        """Make an API request with retry logic."""
        client = await self._get_client()
        url = f"{base_url}{endpoint}"
        
        for attempt in range(self.max_retries):
            try:
                response = await client.get(url, params=params)
                response.raise_for_status()
                return response.json()
            except httpx.HTTPStatusError as e:
                logger.warning(f"Polymarket API error (attempt {attempt + 1}): {e.response.status_code}")
                if attempt == self.max_retries - 1:
                    raise
                await asyncio.sleep(2 ** attempt)
            except Exception as e:
                logger.error(f"Polymarket request error: {e}")
                if attempt == self.max_retries - 1:
                    raise
                await asyncio.sleep(2 ** attempt)
        
        return {}
    
    async def get_events(
        self,
        active: bool = True,
        limit: int = 50,
        order: str = "volume24hr",
        ascending: bool = False,
    ) -> List[Dict]:
        """
        Get events from Gamma API.
        
        Events are collections of related markets (e.g., "2024 Presidential Election").
        
        Args:
            active: Only active events
            limit: Maximum events to return
            order: Sort field (volume24hr, liquidity, startDate)
            ascending: Sort direction
            
        Returns:
            List of event dicts
        """
        params = {
            "active": str(active).lower(),
            "limit": limit,
            "order": order,
            "ascending": str(ascending).lower(),
        }
        
        return await self._request(self.gamma_url, "/events", params)
    
    async def get_markets(
        self,
        active: bool = True,
        limit: int = 50,
        order: str = "volume24hr",
        ascending: bool = False,
    ) -> List[Dict]:
        """
        Get individual markets from Gamma API.
        
        Args:
            active: Only active markets
            limit: Maximum markets to return
            order: Sort field
            ascending: Sort direction
            
        Returns:
            List of market dicts
        """
        params = {
            "active": str(active).lower(),
            "limit": limit,
            "order": order,
            "ascending": str(ascending).lower(),
        }
        
        return await self._request(self.gamma_url, "/markets", params)
    
    async def get_event(self, event_id: str) -> Dict:
        """
        Get single event by ID.
        
        Args:
            event_id: Event ID
            
        Returns:
            Event data dict
        """
        return await self._request(self.gamma_url, f"/events/{event_id}")
    
    async def fetch_top_markets(
        self,
        limit: int = 50,
        include_events: bool = True,
    ) -> List[MarketData]:
        """
        Fetch top markets from Polymarket.
        
        Args:
            limit: Max markets to return
            include_events: Also fetch event-level data
            
        Returns:
            List of standardized MarketData objects
        """
        all_market_data = []
        
        # Fetch individual markets
        try:
            markets = await self.get_markets(limit=limit)
            logger.info(f"Polymarket: Fetched {len(markets)} markets")
            
            for m in markets:
                parsed = self._parse_market(m)
                if parsed:
                    all_market_data.append(parsed)
        except Exception as e:
            logger.error(f"Error fetching Polymarket markets: {e}")
        
        # Also fetch events for additional context
        if include_events:
            try:
                events = await self.get_events(limit=limit)
                logger.info(f"Polymarket: Fetched {len(events)} events")
                
                for event in events:
                    # Each event may have multiple markets
                    event_markets = event.get("markets", [])
                    for m in event_markets[:3]:  # Limit markets per event
                        parsed = self._parse_market(m, event_context=event)
                        if parsed:
                            # Check for duplicates
                            existing_ids = {md.market_id for md in all_market_data}
                            if parsed.market_id not in existing_ids:
                                all_market_data.append(parsed)
            except Exception as e:
                logger.error(f"Error fetching Polymarket events: {e}")
        
        return all_market_data
    
    def _parse_market(self, raw: Dict, event_context: Optional[Dict] = None) -> Optional[MarketData]:
        """Convert Polymarket market response to standardized MarketData."""
        try:
            question = raw.get("question") or raw.get("groupItemTitle") or raw.get("title", "")
            if not question:
                return None
            
            # Parse outcome prices
            outcome_prices = raw.get("outcomePrices", "")
            yes_prob = 0.0
            
            if outcome_prices:
                try:
                    prices = json.loads(outcome_prices) if isinstance(outcome_prices, str) else outcome_prices
                    if prices and len(prices) >= 1:
                        yes_prob = float(prices[0])
                except (ValueError, TypeError):
                    pass
            
            # Parse volumes
            volume = raw.get("volume", 0)
            volume_24h = raw.get("volume24hr", 0)
            liquidity = raw.get("liquidity", 0)
            
            try:
                volume = float(volume) if volume else 0
                volume_24h = float(volume_24h) if volume_24h else 0
                liquidity = float(liquidity) if liquidity else 0
            except (ValueError, TypeError):
                volume = volume_24h = liquidity = 0
            
            # Parse close date
            end_date = raw.get("endDate") or raw.get("endDateIso")
            close_date = None
            if end_date:
                try:
                    close_date = datetime.fromisoformat(end_date.replace("Z", "+00:00"))
                except (ValueError, TypeError):
                    pass
            
            # Market ID
            market_id = raw.get("conditionId") or raw.get("id") or raw.get("slug", "")
            
            # Auto-categorize
            cat_info = categorize_market(question)
            
            # Build market URL
            slug = raw.get("slug") or (event_context.get("slug") if event_context else None)
            market_url = f"https://polymarket.com/event/{slug}" if slug else None
            
            return MarketData(
                source="polymarket",
                market_id=str(market_id),
                question=question,
                description=raw.get("description"),
                yes_probability=yes_prob,
                no_probability=1 - yes_prob if yes_prob else None,
                volume_usd=volume,
                volume_24h_usd=volume_24h,
                liquidity_usd=liquidity,
                close_date=close_date,
                category=cat_info["category"],
                subcategory=cat_info["subcategory"],
                market_url=market_url,
                raw_data=raw,
            )
        except Exception as e:
            logger.error(f"Error parsing Polymarket market: {e}")
            return None


# =============================================================================
# CLIENT FACTORY FUNCTIONS
# =============================================================================

_kalshi_client: Optional[KalshiClient] = None
_polymarket_client: Optional[PolymarketClient] = None


def get_kalshi_client() -> KalshiClient:
    """Get or create global Kalshi client."""
    global _kalshi_client
    if _kalshi_client is None:
        _kalshi_client = KalshiClient()
    return _kalshi_client


def get_polymarket_client() -> PolymarketClient:
    """Get or create global Polymarket client."""
    global _polymarket_client
    if _polymarket_client is None:
        _polymarket_client = PolymarketClient()
    return _polymarket_client


async def close_all_clients():
    """Close all prediction market clients."""
    global _kalshi_client, _polymarket_client
    
    if _kalshi_client:
        await _kalshi_client.close()
        _kalshi_client = None
    
    if _polymarket_client:
        await _polymarket_client.close()
        _polymarket_client = None
