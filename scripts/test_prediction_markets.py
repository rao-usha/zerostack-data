"""
Test script for fetching prediction market data from Kalshi and Polymarket.
Both have FREE public APIs that don't require authentication for reading!

This is a standalone test - no database required.
"""
import httpx
import asyncio
from datetime import datetime
import json

# API Base URLs
KALSHI_BASE = "https://api.elections.kalshi.com/trade-api/v2"
POLYMARKET_GAMMA = "https://gamma-api.polymarket.com"


def print_section(title: str):
    print("\n" + "=" * 70)
    print(f"  {title}")
    print("=" * 70)


async def fetch_kalshi_markets():
    """Fetch top markets from Kalshi with category focus."""
    print_section("KALSHI MARKETS (CFTC-Regulated)")
    
    async with httpx.AsyncClient(timeout=30) as client:
        results = {"economics": [], "politics": [], "sports": []}
        
        # Fetch open markets
        try:
            response = await client.get(f"{KALSHI_BASE}/markets", params={"status": "open", "limit": 100})
            response.raise_for_status()
            data = response.json()
            markets = data.get("markets", [])
            
            print(f"\nFetched {len(markets)} open markets from Kalshi")
            
            # Categorize markets
            for m in markets:
                title = m.get("title", "") + " " + m.get("subtitle", "")
                ticker = m.get("ticker", "")
                yes_price = m.get("yes_bid") or m.get("last_price") or 0
                prob = yes_price / 100 if yes_price > 1 else yes_price
                volume = m.get("volume", 0)
                
                # Simple keyword categorization
                title_lower = title.lower()
                if any(w in title_lower for w in ["fed", "cpi", "inflation", "gdp", "unemployment"]):
                    results["economics"].append({"title": title, "prob": prob, "volume": volume, "ticker": ticker})
                elif any(w in title_lower for w in ["president", "senate", "house", "election"]):
                    results["politics"].append({"title": title, "prob": prob, "volume": volume, "ticker": ticker})
                elif any(w in title_lower for w in ["nfl", "nba", "mlb", "bills", "chiefs", "lakers"]):
                    results["sports"].append({"title": title, "prob": prob, "volume": volume, "ticker": ticker})
            
            # Print results by category
            for cat, markets_list in results.items():
                if markets_list:
                    print(f"\n{cat.upper()} ({len(markets_list)} markets):")
                    for m in markets_list[:5]:
                        print(f"  - {m['title'][:60]}...")
                        print(f"    Prob: {m['prob']:.1%} | Volume: ${m['volume']:,}")
            
        except Exception as e:
            print(f"Error fetching Kalshi: {e}")
        
        return results


async def fetch_polymarket_markets():
    """Fetch top markets from Polymarket."""
    print_section("POLYMARKET MARKETS (Global)")
    
    async with httpx.AsyncClient(timeout=30) as client:
        results = {"economics": [], "politics": [], "sports": [], "world": [], "crypto": []}
        
        try:
            # Fetch events (sorted by volume)
            response = await client.get(
                f"{POLYMARKET_GAMMA}/events",
                params={"active": "true", "limit": 50, "order": "volume24hr", "ascending": "false"}
            )
            response.raise_for_status()
            events = response.json()
            
            # Also fetch individual markets
            response2 = await client.get(
                f"{POLYMARKET_GAMMA}/markets",
                params={"active": "true", "limit": 50, "order": "volume24hr", "ascending": "false"}
            )
            response2.raise_for_status()
            markets = response2.json()
            
            print(f"\nFetched {len(events)} events and {len(markets)} markets from Polymarket")
            
            # Process markets
            for m in markets:
                question = m.get("question", "")
                outcome_prices = m.get("outcomePrices", "")
                volume = float(m.get("volume", 0) or 0)
                volume_24h = float(m.get("volume24hr", 0) or 0)
                
                # Parse probability
                prob = 0
                if outcome_prices:
                    try:
                        prices = json.loads(outcome_prices) if isinstance(outcome_prices, str) else outcome_prices
                        if prices and len(prices) >= 1:
                            prob = float(prices[0])
                    except:
                        pass
                
                market_data = {"question": question, "prob": prob, "volume": volume, "volume_24h": volume_24h}
                
                # Categorize
                q_lower = question.lower()
                if any(w in q_lower for w in ["fed", "interest rate", "cpi", "inflation", "gdp", "recession"]):
                    results["economics"].append(market_data)
                elif any(w in q_lower for w in ["president", "trump", "biden", "election", "democrat", "republican", "nominee"]):
                    results["politics"].append(market_data)
                elif any(w in q_lower for w in ["vs.", "nfl", "nba", "mlb", "packers", "bills", "chiefs", "lakers", "spread"]):
                    results["sports"].append(market_data)
                elif any(w in q_lower for w in ["iran", "khamenei", "strike", "war", "russia", "china", "ukraine"]):
                    results["world"].append(market_data)
                elif any(w in q_lower for w in ["bitcoin", "btc", "ethereum", "crypto"]):
                    results["crypto"].append(market_data)
            
            # Print results by category
            for cat, markets_list in results.items():
                if markets_list:
                    # Sort by 24h volume
                    sorted_markets = sorted(markets_list, key=lambda x: x["volume_24h"], reverse=True)
                    print(f"\n{cat.upper()} ({len(sorted_markets)} markets):")
                    for m in sorted_markets[:5]:
                        print(f"  - {m['question'][:60]}...")
                        print(f"    Prob: {m['prob']:.1%} | 24h Vol: ${m['volume_24h']:,.0f}")
            
        except Exception as e:
            print(f"Error fetching Polymarket: {e}")
        
        return results


async def main():
    print("\n" + "#" * 70)
    print("  PREDICTION MARKET INTELLIGENCE - DATA TEST")
    print("#" * 70)
    print(f"\nTimestamp: {datetime.now().isoformat()}")
    print("\nFetching real-time data from prediction market APIs...")
    
    # Fetch from both platforms
    kalshi_data = await fetch_kalshi_markets()
    polymarket_data = await fetch_polymarket_markets()
    
    # Summary
    print_section("SUMMARY")
    
    kalshi_total = sum(len(v) for v in kalshi_data.values())
    poly_total = sum(len(v) for v in polymarket_data.values())
    
    print(f"""
    Kalshi markets categorized: {kalshi_total}
      - Economics: {len(kalshi_data['economics'])}
      - Politics: {len(kalshi_data['politics'])}
      - Sports: {len(kalshi_data['sports'])}
    
    Polymarket markets categorized: {poly_total}
      - Economics: {len(polymarket_data['economics'])}
      - Politics: {len(polymarket_data['politics'])}
      - Sports: {len(polymarket_data['sports'])}
      - World/Geopolitics: {len(polymarket_data['world'])}
      - Crypto: {len(polymarket_data['crypto'])}
    
    Both APIs work without authentication!
    
    KEY FINDINGS (Polymarket - highest volume):
    """)
    
    # Show top economic markets
    econ_markets = sorted(polymarket_data["economics"], key=lambda x: x["volume_24h"], reverse=True)
    if econ_markets:
        print("    ECONOMICS:")
        for m in econ_markets[:3]:
            print(f"      - {m['question'][:50]}... -> {m['prob']:.1%}")
    
    # Show top political markets
    pol_markets = sorted(polymarket_data["politics"], key=lambda x: x["volume_24h"], reverse=True)
    if pol_markets:
        print("\n    POLITICS:")
        for m in pol_markets[:3]:
            print(f"      - {m['question'][:50]}... -> {m['prob']:.1%}")
    
    # Show geopolitics
    world_markets = sorted(polymarket_data["world"], key=lambda x: x["volume_24h"], reverse=True)
    if world_markets:
        print("\n    GEOPOLITICS:")
        for m in world_markets[:3]:
            print(f"      - {m['question'][:50]}... -> {m['prob']:.1%}")


if __name__ == "__main__":
    asyncio.run(main())
