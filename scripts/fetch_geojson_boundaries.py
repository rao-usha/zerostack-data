"""
Fetch GeoJSON boundaries from Census and store in database.

Uses Census Cartographic Boundary Files which are available as GeoJSON.
These are simplified boundaries optimized for web/visualization.
"""
import asyncio
import sys
import os
import logging
from pathlib import Path
from typing import Optional, List, Dict, Any
import httpx

# Add parent directory to path to import app modules
sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from app.core.config import get_settings
from app.core.models import GeoJSONBoundaries

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class CensusBoundaryFetcher:
    """
    Fetch GeoJSON boundaries from Census Cartographic Boundary Files.
    
    Uses the 500k scale files which are good for visualization.
    Source: https://www.census.gov/geographies/mapping-files/time-series/geo/cartographic-boundary.html
    """
    
    # Census Cartographic Boundary Files (GeoJSON format)
    # These are hosted on census.gov and are in GeoJSON format already!
    BASE_URL = "https://www2.census.gov/geo/tiger/GENZ2021/shp"
    
    # Alternative: Use raw.githubusercontent.com/plotly datasets (easier!)
    PLOTLY_BASE = "https://raw.githubusercontent.com/plotly/datasets/master/geojson-counties-fips.json"
    
    def __init__(self):
        self.settings = get_settings()
        self.engine = create_engine(self.settings.database_url)
        self.SessionLocal = sessionmaker(bind=self.engine)
    
    async def fetch_state_boundaries(self, year: int = 2021) -> Dict[str, Any]:
        """
        Fetch state boundaries from Census.
        
        Args:
            year: Census boundary vintage (2020, 2021, 2022, 2023)
        
        Returns:
            GeoJSON FeatureCollection
        """
        # Use census.gov cartographic boundary file
        # cb_YYYY_us_state_500k.zip contains shapefile, we need GeoJSON
        # Alternative: Use a reliable public GeoJSON source
        
        logger.info(f"Fetching state boundaries for year {year}...")
        
        # Use a reliable public GeoJSON API
        url = "https://eric.clst.org/assets/wiki/uploads/Stuff/gz_2010_us_040_00_500k.json"
        
        try:
            async with httpx.AsyncClient(timeout=60.0) as client:
                response = await client.get(url)
                response.raise_for_status()
                geojson = response.json()
                
                logger.info(f"Fetched {len(geojson.get('features', []))} state boundaries")
                return geojson
        
        except Exception as e:
            logger.error(f"Failed to fetch state boundaries: {e}")
            raise
    
    async def fetch_county_boundaries(self, state_fips: Optional[str] = None) -> Dict[str, Any]:
        """
        Fetch county boundaries.
        
        Args:
            state_fips: Optional state FIPS to filter (e.g., '06' for California)
        
        Returns:
            GeoJSON FeatureCollection
        """
        logger.info(f"Fetching county boundaries (state_fips={state_fips or 'all'})...")
        
        # Use Plotly's county GeoJSON (reliable and fast)
        url = "https://raw.githubusercontent.com/plotly/datasets/master/geojson-counties-fips.json"
        
        try:
            async with httpx.AsyncClient(timeout=60.0) as client:
                response = await client.get(url)
                response.raise_for_status()
                geojson = response.json()
                
                # Filter by state if requested
                if state_fips:
                    features = [
                        f for f in geojson.get('features', [])
                        if f.get('id', '').startswith(state_fips)
                    ]
                    geojson['features'] = features
                    logger.info(f"Filtered to {len(features)} counties in state {state_fips}")
                else:
                    logger.info(f"Fetched {len(geojson.get('features', []))} county boundaries")
                
                return geojson
        
        except Exception as e:
            logger.error(f"Failed to fetch county boundaries: {e}")
            raise
    
    def store_boundaries(
        self,
        geojson: Dict[str, Any],
        dataset_id: str,
        geo_level: str
    ) -> int:
        """
        Store GeoJSON features in database.
        
        Args:
            geojson: GeoJSON FeatureCollection
            dataset_id: Dataset identifier (e.g., "census_states_2021")
            geo_level: Geographic level (state, county, tract, zip)
        
        Returns:
            Number of features stored
        """
        db = self.SessionLocal()
        stored_count = 0
        
        try:
            features = geojson.get('features', [])
            logger.info(f"Storing {len(features)} {geo_level} boundaries...")
            
            for feature in features:
                properties = feature.get('properties', {})
                geometry = feature.get('geometry', {})
                
                # Extract geo_id and name based on level
                if geo_level == 'state':
                    geo_id = properties.get('STATE', properties.get('STATEFP', properties.get('GEO_ID', '')))
                    geo_name = properties.get('NAME', '')
                elif geo_level == 'county':
                    geo_id = feature.get('id', '')  # FIPS code
                    geo_name = properties.get('NAME', '')
                else:
                    geo_id = properties.get('GEOID', feature.get('id', ''))
                    geo_name = properties.get('NAME', '')
                
                if not geo_id:
                    logger.warning(f"Skipping feature with no geo_id: {properties}")
                    continue
                
                # Calculate bounding box
                bbox_minx = bbox_miny = bbox_maxx = bbox_maxy = None
                if geometry and geometry.get('coordinates'):
                    try:
                        coords = self._flatten_coordinates(geometry['coordinates'])
                        if coords:
                            lons = [c[0] for c in coords]
                            lats = [c[1] for c in coords]
                            bbox_minx = str(min(lons))
                            bbox_miny = str(min(lats))
                            bbox_maxx = str(max(lons))
                            bbox_maxy = str(max(lats))
                    except Exception as e:
                        logger.warning(f"Could not calculate bbox for {geo_id}: {e}")
                
                # Check if already exists
                existing = db.query(GeoJSONBoundaries).filter(
                    GeoJSONBoundaries.dataset_id == dataset_id,
                    GeoJSONBoundaries.geo_id == geo_id
                ).first()
                
                if existing:
                    logger.debug(f"Updating existing boundary for {geo_id}")
                    existing.geojson = feature
                    existing.geo_name = geo_name
                    existing.bbox_minx = bbox_minx
                    existing.bbox_miny = bbox_miny
                    existing.bbox_maxx = bbox_maxx
                    existing.bbox_maxy = bbox_maxy
                else:
                    boundary = GeoJSONBoundaries(
                        dataset_id=dataset_id,
                        geo_level=geo_level,
                        geo_id=geo_id,
                        geo_name=geo_name,
                        geojson=feature,
                        bbox_minx=bbox_minx,
                        bbox_miny=bbox_miny,
                        bbox_maxx=bbox_maxx,
                        bbox_maxy=bbox_maxy
                    )
                    db.add(boundary)
                
                stored_count += 1
                
                # Commit in batches
                if stored_count % 100 == 0:
                    db.commit()
                    logger.info(f"Stored {stored_count} boundaries...")
            
            db.commit()
            logger.info(f"Successfully stored {stored_count} {geo_level} boundaries")
            return stored_count
        
        except Exception as e:
            logger.error(f"Error storing boundaries: {e}")
            db.rollback()
            raise
        
        finally:
            db.close()
    
    def _flatten_coordinates(self, coords: Any, depth: int = 0) -> List[tuple]:
        """Flatten nested coordinate arrays."""
        result = []
        if isinstance(coords, list):
            if len(coords) == 2 and isinstance(coords[0], (int, float)):
                return [tuple(coords)]
            for item in coords:
                result.extend(self._flatten_coordinates(item, depth + 1))
        return result


async def main():
    """Main function to fetch and store boundaries."""
    print("Starting main function...")
    
    try:
        fetcher = CensusBoundaryFetcher()
        print("Fetcher created successfully")
    except Exception as e:
        print(f"ERROR creating fetcher: {e}")
        import traceback
        traceback.print_exc()
        return
    
    print("\n" + "="*60)
    print("  CENSUS GEOJSON BOUNDARY FETCHER")
    print("="*60 + "\n")
    
    # 1. Fetch and store state boundaries
    print("📍 STEP 1: Fetching US State Boundaries...")
    print("-" * 60)
    try:
        state_geojson = await fetcher.fetch_state_boundaries(year=2021)
        state_count = fetcher.store_boundaries(
            geojson=state_geojson,
            dataset_id="census_states_2021",
            geo_level="state"
        )
        print(f"✅ Stored {state_count} state boundaries\n")
    except Exception as e:
        print(f"❌ Failed to fetch state boundaries: {e}\n")
    
    # 2. Fetch and store California county boundaries
    print("📍 STEP 2: Fetching California County Boundaries...")
    print("-" * 60)
    try:
        county_geojson = await fetcher.fetch_county_boundaries(state_fips="06")
        county_count = fetcher.store_boundaries(
            geojson=county_geojson,
            dataset_id="census_counties_ca_2021",
            geo_level="county"
        )
        print(f"✅ Stored {county_count} California county boundaries\n")
    except Exception as e:
        print(f"❌ Failed to fetch county boundaries: {e}\n")
    
    # 3. Fetch and store ALL county boundaries (optional - large dataset)
    print("📍 STEP 3: Fetching ALL US County Boundaries...")
    print("-" * 60)
    print("⚠️  This will fetch ~3,200 counties. Proceed? (this may take a minute)")
    try:
        all_counties_geojson = await fetcher.fetch_county_boundaries()
        all_county_count = fetcher.store_boundaries(
            geojson=all_counties_geojson,
            dataset_id="census_counties_us_2021",
            geo_level="county"
        )
        print(f"✅ Stored {all_county_count} US county boundaries\n")
    except Exception as e:
        print(f"❌ Failed to fetch all county boundaries: {e}\n")
    
    print("="*60)
    print("  BOUNDARY FETCH COMPLETE")
    print("="*60 + "\n")
    
    # Query to verify
    print("Verifying stored boundaries...")
    try:
        engine = create_engine(fetcher.settings.database_url)
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT 
                    dataset_id,
                    geo_level,
                    COUNT(*) as count
                FROM geojson_boundaries
                GROUP BY dataset_id, geo_level
                ORDER BY dataset_id
            """))
            
            print("\n📊 Stored Boundaries Summary:")
            print("-" * 60)
            for row in result:
                print(f"  {row[0]:40s} | {row[1]:10s} | {row[2]:5d} features")
            print()
    except Exception as e:
        print(f"Could not query boundaries: {e}")


if __name__ == "__main__":
    print("Script __main__ block executing...")
    try:
        asyncio.run(main())
        print("Script completed successfully")
    except Exception as e:
        print(f"FATAL ERROR: {e}")
        import traceback
        traceback.print_exc()

