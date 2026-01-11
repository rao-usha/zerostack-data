"""
Run foot traffic collection - test what works without API keys.

City Data strategy uses public pedestrian counters (no API key needed):
- Seattle pedestrian counters
- NYC MTA turnstile data
- SF pedestrian counts
- Chicago pedestrian data
"""
import asyncio
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

os.environ.setdefault("DATABASE_URL", "postgresql://nexdata:nexdata_dev_password@localhost:5433/nexdata")

from sqlalchemy import text
from app.core.database import get_session_factory


async def test_location_discovery():
    """Test discovering locations using available strategies."""
    from app.agentic.foot_traffic_agent import FootTrafficAgent
    
    SessionLocal = get_session_factory()
    db = SessionLocal()
    
    try:
        agent = FootTrafficAgent(db)
        
        print("\n=== TESTING LOCATION DISCOVERY ===\n")
        print("Trying to discover Starbucks locations in Seattle...")
        print("(Using city data strategy - no API key needed)\n")
        
        result = await agent.discover_locations(
            brand_name="Starbucks",
            city="Seattle",
            state="WA"
        )
        
        print(f"Status: {result.get('status')}")
        print(f"Locations found: {result.get('locations_found', 0)}")
        print(f"Strategies used: {result.get('strategies_used', [])}")
        
        if result.get('errors'):
            print(f"\nErrors:")
            for err in result.get('errors', []):
                print(f"  - {err.get('strategy')}: {err.get('error')}")
        
        if result.get('locations'):
            print(f"\nSample locations:")
            for loc in result.get('locations', [])[:5]:
                print(f"  - {loc.get('name')} @ {loc.get('address')}, {loc.get('city')}")
        
        return result
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        db.close()


async def test_city_data_collection():
    """Test collecting public city pedestrian data."""
    print("\n=== TESTING CITY PEDESTRIAN DATA ===\n")
    
    try:
        from app.agentic.traffic_strategies.city_data_strategy import CityDataStrategy
        from app.agentic.traffic_strategies.base import LocationContext
        
        strategy = CityDataStrategy()
        
        # Test Seattle data
        context = LocationContext(
            brand_name="Downtown Seattle",
            city="Seattle",
            state="WA",
            latitude=47.6062,
            longitude=-122.3321
        )
        
        print("Checking if city data strategy is applicable...")
        applicable, reason = strategy.is_applicable(context)
        print(f"Applicable: {applicable}")
        print(f"Reason: {reason}")
        
        if applicable:
            print("\nExecuting city data collection...")
            result = await strategy.execute(context)
            print(f"Success: {result.success}")
            print(f"Observations: {len(result.observations_found) if result.observations_found else 0}")
            print(f"Reasoning: {result.reasoning}")
            
            if result.observations_found:
                print("\nSample observations:")
                for obs in result.observations_found[:5]:
                    print(f"  - Date: {obs.get('observation_date')}, Visits: {obs.get('visit_count')}")
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()


async def main():
    print("=" * 60)
    print("FOOT TRAFFIC DATA COLLECTION TEST")
    print("=" * 60)
    
    # Check API key status first
    from app.core.config import get_settings
    settings = get_settings()
    
    print("\n=== API KEY STATUS ===\n")
    print(f"Foursquare: {'Configured' if settings.get_foursquare_api_key() else 'NOT SET'}")
    print(f"SafeGraph: {'Configured' if settings.get_safegraph_api_key() else 'NOT SET'}")
    print(f"Placer: {'Configured' if settings.get_placer_api_key() else 'NOT SET'}")
    print(f"Google Scraping: {'Enabled' if settings.is_google_scraping_enabled() else 'Disabled'}")
    
    # Test city data (no API key needed)
    await test_city_data_collection()
    
    # Test location discovery
    await test_location_discovery()
    
    print("\n" + "=" * 60)
    print("DONE")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
