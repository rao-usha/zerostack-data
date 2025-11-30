"""
Real Estate / Housing ingestion orchestration.

High-level functions that coordinate data fetching, table creation, and data loading
for all real estate data sources.
"""
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy import text

from app.core.config import get_settings
from app.core.models import DatasetRegistry, IngestionJob, JobStatus
from app.sources.realestate.client import FHFAClient, HUDClient, RedfinClient, OSMClient
from app.sources.realestate import metadata

logger = logging.getLogger(__name__)


async def prepare_table_for_source(
    db: Session,
    source: str
) -> Dict[str, Any]:
    """
    Prepare database table for real estate data ingestion.
    
    Steps:
    1. Generate table name
    2. Generate CREATE TABLE SQL
    3. Execute table creation (idempotent)
    4. Register in dataset_registry
    
    Args:
        db: Database session
        source: Source identifier (fhfa_hpi, hud_permits, redfin, osm_buildings)
        
    Returns:
        Dictionary with table metadata
    """
    try:
        # 1. Generate table name
        table_name = metadata.generate_table_name(source)
        
        # 2. Generate CREATE TABLE SQL
        logger.info(f"Creating table {table_name} for {source}")
        create_sql = metadata.generate_create_table_sql(source)
        
        # 3. Execute table creation (idempotent)
        db.execute(text(create_sql))
        db.commit()
        
        # 4. Register in dataset_registry
        dataset_id = f"realestate_{source}"
        
        # Check if already registered
        existing = db.query(DatasetRegistry).filter(
            DatasetRegistry.table_name == table_name
        ).first()
        
        if existing:
            logger.info(f"Dataset {dataset_id} already registered")
            existing.last_updated_at = datetime.utcnow()
            existing.source_metadata = {
                "source_type": source
            }
            db.commit()
        else:
            dataset_entry = DatasetRegistry(
                source="realestate",
                dataset_id=dataset_id,
                table_name=table_name,
                display_name=metadata.get_source_display_name(source),
                description=metadata.get_source_description(source),
                source_metadata={
                    "source_type": source
                }
            )
            db.add(dataset_entry)
            db.commit()
            logger.info(f"Registered dataset {dataset_id}")
        
        return {
            "table_name": table_name,
            "source": source
        }
    
    except Exception as e:
        logger.error(f"Failed to prepare table for {source}: {e}")
        raise


async def ingest_fhfa_hpi(
    db: Session,
    job_id: int,
    geography_type: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None
) -> Dict[str, Any]:
    """
    Ingest FHFA House Price Index data.
    
    Args:
        db: Database session
        job_id: Ingestion job ID
        geography_type: Geography type filter (National, State, MSA, ZIP3)
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        
    Returns:
        Dictionary with ingestion results
    """
    settings = get_settings()
    source = "fhfa_hpi"
    
    # Initialize client
    client = FHFAClient(
        max_concurrency=settings.max_concurrency,
        max_retries=settings.max_retries,
        backoff_factor=settings.retry_backoff_factor
    )
    
    try:
        # Update job status to running
        job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
        if job:
            job.status = JobStatus.RUNNING
            job.started_at = datetime.utcnow()
            db.commit()
        
        # Set defaults
        if not start_date or not end_date:
            start_date, end_date = metadata.get_default_date_range(source)
        
        logger.info(f"Ingesting FHFA HPI: {start_date} to {end_date}")
        
        # Prepare table
        table_info = await prepare_table_for_source(db, source)
        table_name = table_info["table_name"]
        
        # Fetch data
        raw_data = await client.fetch_house_price_index(
            geography_type=geography_type,
            start_date=start_date,
            end_date=end_date
        )
        
        # Parse data
        parsed_data = metadata.parse_fhfa_data(raw_data)
        
        logger.info(f"Parsed {len(parsed_data)} FHFA records")
        
        # Insert data
        rows_inserted = 0
        if parsed_data:
            insert_sql = f"""
                INSERT INTO {table_name} 
                (date, geography_type, geography_id, geography_name, 
                 index_nsa, index_sa, yoy_pct_change, qoq_pct_change)
                VALUES 
                (:date, :geography_type, :geography_id, :geography_name,
                 :index_nsa, :index_sa, :yoy_pct_change, :qoq_pct_change)
                ON CONFLICT (date, geography_type, geography_id) 
                DO UPDATE SET
                    geography_name = EXCLUDED.geography_name,
                    index_nsa = EXCLUDED.index_nsa,
                    index_sa = EXCLUDED.index_sa,
                    yoy_pct_change = EXCLUDED.yoy_pct_change,
                    qoq_pct_change = EXCLUDED.qoq_pct_change,
                    ingested_at = NOW()
            """
            
            # Execute in batches
            batch_size = 1000
            for i in range(0, len(parsed_data), batch_size):
                batch = parsed_data[i:i + batch_size]
                db.execute(text(insert_sql), batch)
                rows_inserted += len(batch)
                db.commit()
                
                if (i + batch_size) % 10000 == 0:
                    logger.info(f"Inserted {rows_inserted}/{len(parsed_data)} rows")
            
            logger.info(f"Successfully inserted {rows_inserted} rows")
        
        # Update job status
        if job:
            job.status = JobStatus.SUCCESS
            job.completed_at = datetime.utcnow()
            job.rows_inserted = rows_inserted
            db.commit()
        
        return {
            "table_name": table_name,
            "source": source,
            "rows_inserted": rows_inserted,
            "date_range": f"{start_date} to {end_date}"
        }
    
    except Exception as e:
        logger.error(f"FHFA ingestion failed: {e}", exc_info=True)
        
        # Update job status to failed
        job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
        if job:
            job.status = JobStatus.FAILED
            job.completed_at = datetime.utcnow()
            job.error_message = str(e)
            db.commit()
        
        raise
    
    finally:
        await client.close()


async def ingest_hud_permits(
    db: Session,
    job_id: int,
    geography_type: str = "National",
    geography_id: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None
) -> Dict[str, Any]:
    """
    Ingest HUD Building Permits and Housing Starts data.
    
    Args:
        db: Database session
        job_id: Ingestion job ID
        geography_type: Geography type (National, State, MSA, County)
        geography_id: Geography identifier
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        
    Returns:
        Dictionary with ingestion results
    """
    settings = get_settings()
    source = "hud_permits"
    
    # Initialize client
    client = HUDClient(
        max_concurrency=settings.max_concurrency,
        max_retries=settings.max_retries,
        backoff_factor=settings.retry_backoff_factor
    )
    
    try:
        # Update job status to running
        job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
        if job:
            job.status = JobStatus.RUNNING
            job.started_at = datetime.utcnow()
            db.commit()
        
        # Set defaults
        if not start_date or not end_date:
            start_date, end_date = metadata.get_default_date_range(source)
        
        logger.info(f"Ingesting HUD Permits: {start_date} to {end_date}")
        
        # Prepare table
        table_info = await prepare_table_for_source(db, source)
        table_name = table_info["table_name"]
        
        # Fetch data
        raw_data = await client.fetch_permits_and_starts(
            geography_type=geography_type,
            geography_id=geography_id,
            start_date=start_date,
            end_date=end_date
        )
        
        # Parse data
        parsed_data = metadata.parse_hud_data(raw_data)
        
        logger.info(f"Parsed {len(parsed_data)} HUD records")
        
        # Insert data
        rows_inserted = 0
        if parsed_data:
            insert_sql = f"""
                INSERT INTO {table_name} 
                (date, geography_type, geography_id, geography_name,
                 permits_total, permits_1unit, permits_2to4units, permits_5plus,
                 starts_total, starts_1unit, starts_2to4units, starts_5plus,
                 completions_total, completions_1unit, completions_2to4units, completions_5plus)
                VALUES 
                (:date, :geography_type, :geography_id, :geography_name,
                 :permits_total, :permits_1unit, :permits_2to4units, :permits_5plus,
                 :starts_total, :starts_1unit, :starts_2to4units, :starts_5plus,
                 :completions_total, :completions_1unit, :completions_2to4units, :completions_5plus)
                ON CONFLICT (date, geography_type, geography_id) 
                DO UPDATE SET
                    geography_name = EXCLUDED.geography_name,
                    permits_total = EXCLUDED.permits_total,
                    permits_1unit = EXCLUDED.permits_1unit,
                    permits_2to4units = EXCLUDED.permits_2to4units,
                    permits_5plus = EXCLUDED.permits_5plus,
                    starts_total = EXCLUDED.starts_total,
                    starts_1unit = EXCLUDED.starts_1unit,
                    starts_2to4units = EXCLUDED.starts_2to4units,
                    starts_5plus = EXCLUDED.starts_5plus,
                    completions_total = EXCLUDED.completions_total,
                    completions_1unit = EXCLUDED.completions_1unit,
                    completions_2to4units = EXCLUDED.completions_2to4units,
                    completions_5plus = EXCLUDED.completions_5plus,
                    ingested_at = NOW()
            """
            
            # Execute in batches
            batch_size = 1000
            for i in range(0, len(parsed_data), batch_size):
                batch = parsed_data[i:i + batch_size]
                db.execute(text(insert_sql), batch)
                rows_inserted += len(batch)
                db.commit()
                
                if (i + batch_size) % 10000 == 0:
                    logger.info(f"Inserted {rows_inserted}/{len(parsed_data)} rows")
            
            logger.info(f"Successfully inserted {rows_inserted} rows")
        
        # Update job status
        if job:
            job.status = JobStatus.SUCCESS
            job.completed_at = datetime.utcnow()
            job.rows_inserted = rows_inserted
            db.commit()
        
        return {
            "table_name": table_name,
            "source": source,
            "rows_inserted": rows_inserted,
            "date_range": f"{start_date} to {end_date}"
        }
    
    except Exception as e:
        logger.error(f"HUD ingestion failed: {e}", exc_info=True)
        
        # Update job status to failed
        job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
        if job:
            job.status = JobStatus.FAILED
            job.completed_at = datetime.utcnow()
            job.error_message = str(e)
            db.commit()
        
        raise
    
    finally:
        await client.close()


async def ingest_redfin(
    db: Session,
    job_id: int,
    region_type: str = "zip",
    property_type: str = "All Residential"
) -> Dict[str, Any]:
    """
    Ingest Redfin housing market data.
    
    Args:
        db: Database session
        job_id: Ingestion job ID
        region_type: Region type (zip, city, neighborhood, metro)
        property_type: Property type filter
        
    Returns:
        Dictionary with ingestion results
    """
    settings = get_settings()
    source = "redfin"
    
    # Initialize client
    client = RedfinClient(
        max_concurrency=settings.max_concurrency,
        max_retries=settings.max_retries,
        backoff_factor=settings.retry_backoff_factor
    )
    
    try:
        # Update job status to running
        job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
        if job:
            job.status = JobStatus.RUNNING
            job.started_at = datetime.utcnow()
            db.commit()
        
        logger.info(f"Ingesting Redfin data: {region_type} / {property_type}")
        
        # Prepare table
        table_info = await prepare_table_for_source(db, source)
        table_name = table_info["table_name"]
        
        # Fetch data
        raw_data = await client.fetch_redfin_data(
            region_type=region_type,
            property_type=property_type
        )
        
        # Parse data
        parsed_data = metadata.parse_redfin_data(raw_data)
        
        logger.info(f"Parsed {len(parsed_data)} Redfin records")
        
        # Insert data
        rows_inserted = 0
        if parsed_data:
            insert_sql = f"""
                INSERT INTO {table_name} 
                (period_end, region_type, region_type_id, region, state_code, property_type,
                 median_sale_price, median_list_price, median_ppsf, homes_sold, pending_sales,
                 new_listings, inventory, months_of_supply, median_dom, avg_sale_to_list,
                 sold_above_list, price_drops, off_market_in_two_weeks)
                VALUES 
                (:period_end, :region_type, :region_type_id, :region, :state_code, :property_type,
                 :median_sale_price, :median_list_price, :median_ppsf, :homes_sold, :pending_sales,
                 :new_listings, :inventory, :months_of_supply, :median_dom, :avg_sale_to_list,
                 :sold_above_list, :price_drops, :off_market_in_two_weeks)
                ON CONFLICT (period_end, region_type, region_type_id, property_type) 
                DO UPDATE SET
                    region = EXCLUDED.region,
                    state_code = EXCLUDED.state_code,
                    median_sale_price = EXCLUDED.median_sale_price,
                    median_list_price = EXCLUDED.median_list_price,
                    median_ppsf = EXCLUDED.median_ppsf,
                    homes_sold = EXCLUDED.homes_sold,
                    pending_sales = EXCLUDED.pending_sales,
                    new_listings = EXCLUDED.new_listings,
                    inventory = EXCLUDED.inventory,
                    months_of_supply = EXCLUDED.months_of_supply,
                    median_dom = EXCLUDED.median_dom,
                    avg_sale_to_list = EXCLUDED.avg_sale_to_list,
                    sold_above_list = EXCLUDED.sold_above_list,
                    price_drops = EXCLUDED.price_drops,
                    off_market_in_two_weeks = EXCLUDED.off_market_in_two_weeks,
                    ingested_at = NOW()
            """
            
            # Execute in batches
            batch_size = 1000
            for i in range(0, len(parsed_data), batch_size):
                batch = parsed_data[i:i + batch_size]
                db.execute(text(insert_sql), batch)
                rows_inserted += len(batch)
                db.commit()
                
                if (i + batch_size) % 10000 == 0:
                    logger.info(f"Inserted {rows_inserted}/{len(parsed_data)} rows")
            
            logger.info(f"Successfully inserted {rows_inserted} rows")
        
        # Update job status
        if job:
            job.status = JobStatus.SUCCESS
            job.completed_at = datetime.utcnow()
            job.rows_inserted = rows_inserted
            db.commit()
        
        return {
            "table_name": table_name,
            "source": source,
            "rows_inserted": rows_inserted,
            "region_type": region_type,
            "property_type": property_type
        }
    
    except Exception as e:
        logger.error(f"Redfin ingestion failed: {e}", exc_info=True)
        
        # Update job status to failed
        job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
        if job:
            job.status = JobStatus.FAILED
            job.completed_at = datetime.utcnow()
            job.error_message = str(e)
            db.commit()
        
        raise
    
    finally:
        await client.close()


async def ingest_osm_buildings(
    db: Session,
    job_id: int,
    bounding_box: tuple[float, float, float, float],
    building_type: Optional[str] = None,
    limit: int = 10000
) -> Dict[str, Any]:
    """
    Ingest OpenStreetMap building footprints.
    
    Args:
        db: Database session
        job_id: Ingestion job ID
        bounding_box: (south, west, north, east) coordinates
        building_type: Building type filter (residential, commercial, etc.)
        limit: Maximum number of buildings to fetch
        
    Returns:
        Dictionary with ingestion results
    """
    settings = get_settings()
    source = "osm_buildings"
    
    # Initialize client
    client = OSMClient(
        max_concurrency=1,  # Very conservative for OSM
        max_retries=settings.max_retries,
        backoff_factor=settings.retry_backoff_factor
    )
    
    try:
        # Update job status to running
        job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
        if job:
            job.status = JobStatus.RUNNING
            job.started_at = datetime.utcnow()
            db.commit()
        
        logger.info(f"Ingesting OSM buildings: bbox={bounding_box}")
        
        # Prepare table
        table_info = await prepare_table_for_source(db, source)
        table_name = table_info["table_name"]
        
        # Fetch data
        raw_data = await client.fetch_buildings(
            bounding_box=bounding_box,
            building_type=building_type,
            limit=limit
        )
        
        # Parse data
        parsed_data = metadata.parse_osm_data(raw_data)
        
        logger.info(f"Parsed {len(parsed_data)} OSM building records")
        
        # Insert data
        rows_inserted = 0
        if parsed_data:
            insert_sql = f"""
                INSERT INTO {table_name} 
                (osm_id, osm_type, latitude, longitude, building_type, levels, height,
                 area_sqm, address, city, state, postcode, country, name, tags, geometry_geojson)
                VALUES 
                (:osm_id, :osm_type, :latitude, :longitude, :building_type, :levels, :height,
                 :area_sqm, :address, :city, :state, :postcode, :country, :name, :tags, :geometry_geojson)
                ON CONFLICT (osm_id, osm_type) 
                DO UPDATE SET
                    latitude = EXCLUDED.latitude,
                    longitude = EXCLUDED.longitude,
                    building_type = EXCLUDED.building_type,
                    levels = EXCLUDED.levels,
                    height = EXCLUDED.height,
                    area_sqm = EXCLUDED.area_sqm,
                    address = EXCLUDED.address,
                    city = EXCLUDED.city,
                    state = EXCLUDED.state,
                    postcode = EXCLUDED.postcode,
                    country = EXCLUDED.country,
                    name = EXCLUDED.name,
                    tags = EXCLUDED.tags,
                    geometry_geojson = EXCLUDED.geometry_geojson,
                    ingested_at = NOW()
            """
            
            # Execute in batches
            batch_size = 500  # Smaller batches for complex data
            for i in range(0, len(parsed_data), batch_size):
                batch = parsed_data[i:i + batch_size]
                
                # Convert dicts to JSON strings for JSONB columns
                for record in batch:
                    import json
                    if isinstance(record.get("tags"), dict):
                        record["tags"] = json.dumps(record["tags"])
                    if isinstance(record.get("geometry_geojson"), dict):
                        record["geometry_geojson"] = json.dumps(record["geometry_geojson"])
                
                db.execute(text(insert_sql), batch)
                rows_inserted += len(batch)
                db.commit()
                
                if (i + batch_size) % 5000 == 0:
                    logger.info(f"Inserted {rows_inserted}/{len(parsed_data)} rows")
            
            logger.info(f"Successfully inserted {rows_inserted} rows")
        
        # Update job status
        if job:
            job.status = JobStatus.SUCCESS
            job.completed_at = datetime.utcnow()
            job.rows_inserted = rows_inserted
            db.commit()
        
        return {
            "table_name": table_name,
            "source": source,
            "rows_inserted": rows_inserted,
            "bounding_box": bounding_box
        }
    
    except Exception as e:
        logger.error(f"OSM ingestion failed: {e}", exc_info=True)
        
        # Update job status to failed
        job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
        if job:
            job.status = JobStatus.FAILED
            job.completed_at = datetime.utcnow()
            job.error_message = str(e)
            db.commit()
        
        raise
    
    finally:
        await client.close()

