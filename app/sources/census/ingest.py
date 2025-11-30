"""
Census ingestion orchestration.

High-level functions that coordinate metadata fetching, table creation, and data loading.
"""
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy import text

from app.core.config import get_settings
from app.core.models import DatasetRegistry, IngestionJob, JobStatus, GeoJSONBoundaries, CensusVariableMetadata
from app.sources.census.client import CensusClient
from app.sources.census import metadata
from app.sources.census.geojson import GeoJSONFetcher

logger = logging.getLogger(__name__)


async def prepare_table_for_acs_table(
    db: Session,
    survey: str,
    year: int,
    table_id: str,
    geo_level: str
) -> Dict[str, Any]:
    """
    Prepare database table for ACS data ingestion.
    
    Steps:
    1. Fetch table metadata from Census API
    2. Parse metadata and map to Postgres schema
    3. Generate CREATE TABLE SQL
    4. Execute table creation (idempotent)
    5. Register in dataset_registry
    
    Args:
        db: Database session
        survey: Survey type (e.g., "acs5")
        year: Survey year
        table_id: Table identifier (e.g., "B01001")
        geo_level: Geographic level (state, county, etc.)
        
    Returns:
        Dictionary with:
        - table_name: Generated Postgres table name
        - column_mapping: Census var -> Postgres column mapping
        - variable_count: Number of variables
        
    Raises:
        Exception: On metadata fetch or table creation errors
    """
    settings = get_settings()
    api_key = settings.require_census_api_key()
    
    # Initialize Census client
    client = CensusClient(
        api_key=api_key,
        max_concurrency=settings.max_concurrency,
        max_retries=settings.max_retries,
        backoff_factor=settings.retry_backoff_factor
    )
    
    try:
        # 1. Fetch table metadata
        logger.info(f"Fetching metadata for {table_id} from Census API")
        census_metadata = await client.fetch_table_metadata(survey, year, table_id)
        
        # 2. Parse metadata and map to Postgres schema
        logger.info(f"Parsing metadata for table {table_id}")
        table_vars = metadata.parse_table_metadata(census_metadata, table_id)
        
        if not table_vars:
            raise ValueError(f"No variables found for table {table_id}")
        
        # 3. Generate CREATE TABLE SQL
        table_name = generate_table_name(survey, year, table_id)
        create_sql = metadata.generate_create_table_sql(table_name, table_vars)
        
        # 4. Execute table creation (idempotent)
        logger.info(f"Creating table {table_name}")
        db.execute(text(create_sql))
        db.commit()
        
        # 5. Store variable metadata for column mappings
        dataset_id = f"{survey}_{year}_{table_id.lower()}"
        await _store_variable_metadata(db, dataset_id, table_vars)
        
        # 6. Register in dataset_registry
        dataset_id = f"{survey}_{year}_{table_id.lower()}"
        
        # Check if already registered
        existing = db.query(DatasetRegistry).filter(
            DatasetRegistry.table_name == table_name
        ).first()
        
        if existing:
            logger.info(f"Dataset {dataset_id} already registered")
            existing.last_updated_at = datetime.utcnow()
            db.commit()
        else:
            # Get concept from first variable for description
            first_var = list(table_vars.values())[0]
            concept = first_var.get("concept", "")
            
            dataset = DatasetRegistry(
                source="census",
                dataset_id=dataset_id,
                table_name=table_name,
                display_name=f"ACS {survey.upper()} {year} - {table_id}",
                description=f"Census ACS data: {concept}",
                source_metadata={
                    "survey": survey,
                    "year": year,
                    "table_id": table_id,
                    "geo_level": geo_level,
                    "variable_count": len(table_vars)
                }
            )
            db.add(dataset)
            db.commit()
            logger.info(f"Registered dataset {dataset_id}")
        
        # Build column mapping
        column_mapping = metadata.build_column_mapping(table_vars)
        
        return {
            "table_name": table_name,
            "column_mapping": column_mapping,
            "variable_count": len(table_vars)
        }
    
    finally:
        await client.close()


async def ingest_acs_table(
    db: Session,
    job_id: int,
    survey: str,
    year: int,
    table_id: str,
    geo_level: str,
    geo_filter: Optional[Dict[str, str]] = None,
    include_geojson: bool = False
) -> Dict[str, Any]:
    """
    Ingest ACS table data into Postgres.
    
    Steps:
    1. Prepare table (if not already done)
    2. Determine sharding strategy (e.g., by state)
    3. Fetch data in parallel with bounded concurrency
    4. Normalize and validate data
    5. Batch insert into Postgres
    6. Update job record with row count
    
    Args:
        db: Database session
        job_id: Ingestion job ID for tracking
        survey: Survey type (e.g., "acs5")
        year: Survey year
        table_id: Table identifier (e.g., "B01001")
        geo_level: Geographic level (state, county, etc.)
        geo_filter: Optional geographic filters
        
    Returns:
        Dictionary with:
        - table_name: Postgres table name
        - rows_inserted: Total rows inserted
        - duration_seconds: Time taken
        
    Raises:
        Exception: On data fetch or insertion errors
    """
    import asyncio
    start_time = datetime.utcnow()
    
    settings = get_settings()
    api_key = settings.require_census_api_key()
    
    # 1. Prepare table
    logger.info(f"Preparing table for {survey} {year} {table_id}")
    prep_result = await prepare_table_for_acs_table(db, survey, year, table_id, geo_level)
    
    table_name = prep_result["table_name"]
    column_mapping = prep_result["column_mapping"]
    
    # Get list of variables to fetch (Census variable names)
    variables = list(column_mapping.keys())
    
    logger.info(f"Will fetch {len(variables)} variables for table {table_name}")
    
    # 2. Initialize Census client
    client = CensusClient(
        api_key=api_key,
        max_concurrency=settings.max_concurrency,
        max_retries=settings.max_retries,
        backoff_factor=settings.retry_backoff_factor
    )
    
    try:
        # 3. Fetch data (with bounded concurrency via client's semaphore)
        logger.info(f"Fetching data for {geo_level} level")
        
        if geo_filter:
            # Fetch specific geography
            data = await client.fetch_acs_data(
                survey, year, variables, geo_level, geo_filter
            )
            all_data = data
        elif geo_level == "state":
            # Fetch all states in one request
            all_data = await client.fetch_acs_data(
                survey, year, variables, geo_level, None
            )
        else:
            # For county/tract, we'd need to shard by state
            # For now, just fetch all (may be slow for large geographies)
            logger.warning(f"Fetching all {geo_level} in one request - may be slow")
            all_data = await client.fetch_acs_data(
                survey, year, variables, geo_level, None
            )
        
        logger.info(f"Fetched {len(all_data)} records")
        
        # 4. Normalize and insert data
        if all_data:
            await _batch_insert_data(
                db, table_name, all_data, column_mapping
            )
        
        # 5. Fetch and store GeoJSON if requested
        geojson_features = 0
        if include_geojson:
            logger.info("Fetching GeoJSON boundaries...")
            try:
                geojson_fetcher = GeoJSONFetcher(year=year)
                
                # Extract state/county from geo_filter
                state_fips = geo_filter.get("state") if geo_filter else None
                county_fips = geo_filter.get("county") if geo_filter else None
                
                geojson_data = await geojson_fetcher.fetch_geojson(
                    geo_level=geo_level,
                    state_fips=state_fips,
                    county_fips=county_fips
                )
                
                # Store GeoJSON features
                if geojson_data and "features" in geojson_data:
                    await _store_geojson_features(
                        db,
                        dataset_id=f"{survey}_{year}_{table_id.lower()}",
                        geo_level=geo_level,
                        features=geojson_data["features"]
                    )
                    geojson_features = len(geojson_data["features"])
                    logger.info(f"Stored {geojson_features} GeoJSON features")
            
            except Exception as e:
                logger.warning(f"Failed to fetch/store GeoJSON: {e}")
                # Don't fail the whole job if GeoJSON fails
        
        # 6. Calculate results
        rows_inserted = len(all_data)
        duration = (datetime.utcnow() - start_time).total_seconds()
        
        logger.info(
            f"Successfully ingested {rows_inserted} rows into {table_name} "
            f"in {duration:.2f}s (GeoJSON features: {geojson_features})"
        )
        
        return {
            "table_name": table_name,
            "rows_inserted": rows_inserted,
            "geojson_features": geojson_features,
            "duration_seconds": duration
        }
    
    finally:
        await client.close()


async def _batch_insert_data(
    db: Session,
    table_name: str,
    data: List[Dict[str, Any]],
    column_mapping: Dict[str, str],
    batch_size: int = 1000
) -> None:
    """
    Batch insert data into Postgres using parameterized queries.
    
    Args:
        db: Database session
        table_name: Target table name
        data: List of data records from Census API
        column_mapping: Mapping from Census variable names to Postgres columns
        batch_size: Number of rows per batch
    """
    if not data:
        return
    
    # Build list of all columns (data columns + geo columns)
    data_columns = list(column_mapping.values())
    geo_columns = ["geo_name", "geo_id", "state_fips"]
    all_columns = geo_columns + data_columns
    
    # Build INSERT statement
    columns_sql = ", ".join(all_columns)
    placeholders = ", ".join([f":{col}" for col in all_columns])
    insert_sql = f"INSERT INTO {table_name} ({columns_sql}) VALUES ({placeholders})"
    
    # Process in batches
    for i in range(0, len(data), batch_size):
        batch = data[i:i + batch_size]
        
        # Normalize batch data
        normalized_batch = []
        for record in batch:
            normalized = {}
            
            # Extract geography fields
            normalized["geo_name"] = record.get("NAME")
            normalized["geo_id"] = record.get("GEO_ID") or record.get("state") or ""
            normalized["state_fips"] = record.get("state", "")
            
            # Extract and normalize data fields
            for census_var, pg_col in column_mapping.items():
                raw_value = record.get(census_var)
                normalized[pg_col] = _normalize_value(raw_value)
            
            normalized_batch.append(normalized)
        
        # Execute batch insert using parameterized query
        db.execute(text(insert_sql), normalized_batch)
        db.commit()
        
        logger.debug(f"Inserted batch of {len(batch)} rows")


def _normalize_value(value: Any) -> Any:
    """
    Normalize a Census API value for database insertion.
    
    Handles null values, empty strings, type conversions.
    """
    if value is None or value == "" or value == "null":
        return None
    
    # Census API returns negative values for missing/unavailable data
    # Convert to None
    try:
        numeric = float(value)
        if numeric < 0:
            return None
        return value
    except (ValueError, TypeError):
        return value


async def _store_geojson_features(
    db: Session,
    dataset_id: str,
    geo_level: str,
    features: List[Dict[str, Any]]
) -> None:
    """
    Store GeoJSON features in the database.
    
    Args:
        db: Database session
        dataset_id: Dataset identifier
        geo_level: Geographic level
        features: List of GeoJSON features
    """
    for feature in features:
        properties = feature.get("properties", {})
        geometry = feature.get("geometry")
        
        # Extract geo_id based on level
        if geo_level == "state":
            geo_id = properties.get("STATEFP", properties.get("STATE", ""))
        elif geo_level == "county":
            state_fp = properties.get("STATEFP", "")
            county_fp = properties.get("COUNTYFP", "")
            geo_id = f"{state_fp}{county_fp}"
        elif geo_level == "tract":
            state_fp = properties.get("STATEFP", "")
            county_fp = properties.get("COUNTYFP", "")
            tract_fp = properties.get("TRACTCE", "")
            geo_id = f"{state_fp}{county_fp}{tract_fp}"
        elif "zip" in geo_level.lower():
            geo_id = properties.get("ZCTA5CE20", properties.get("ZCTA", ""))
        else:
            geo_id = properties.get("GEOID", "")
        
        geo_name = properties.get("NAME", "")
        
        # Calculate bounding box if geometry exists
        bbox_minx = bbox_miny = bbox_maxx = bbox_maxy = None
        if geometry and geometry.get("coordinates"):
            # Simple bbox calculation (could be improved)
            try:
                coords = geometry["coordinates"]
                # Flatten all coordinates to get min/max
                all_coords = _flatten_coordinates(coords)
                if all_coords:
                    lons = [c[0] for c in all_coords]
                    lats = [c[1] for c in all_coords]
                    bbox_minx = str(min(lons))
                    bbox_miny = str(min(lats))
                    bbox_maxx = str(max(lons))
                    bbox_maxy = str(max(lats))
            except:
                pass
        
        # Store the feature
        geojson_record = GeoJSONBoundaries(
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
        db.add(geojson_record)
    
    db.commit()


def _flatten_coordinates(coords: Any, depth: int = 0) -> List[tuple]:
    """
    Flatten nested coordinate arrays.
    
    GeoJSON can have deeply nested coordinate arrays.
    """
    result = []
    if isinstance(coords, list):
        # Check if this looks like a coordinate pair
        if len(coords) == 2 and isinstance(coords[0], (int, float)):
            return [tuple(coords)]
        
        # Recursively flatten
        for item in coords:
            result.extend(_flatten_coordinates(item, depth + 1))
    
    return result


async def _store_variable_metadata(
    db: Session,
    dataset_id: str,
    table_vars: Dict[str, Dict[str, Any]]
) -> None:
    """
    Store variable metadata for column name mappings.
    
    This allows users to understand what each column means.
    
    Args:
        db: Database session
        dataset_id: Dataset identifier
        table_vars: Variable metadata from parse_table_metadata
    """
    logger.info(f"Storing metadata for {len(table_vars)} variables")
    
    # Check if metadata already exists for this dataset
    existing_count = db.query(CensusVariableMetadata).filter(
        CensusVariableMetadata.dataset_id == dataset_id
    ).count()
    
    if existing_count > 0:
        logger.info(f"Variable metadata already exists for {dataset_id}, skipping")
        return
    
    # Store metadata for each variable
    for var_name, var_meta in table_vars.items():
        metadata_record = CensusVariableMetadata(
            dataset_id=dataset_id,
            variable_name=var_name,
            column_name=var_meta["column_name"],
            label=var_meta["label"],
            concept=var_meta.get("concept", ""),
            predicate_type=var_meta.get("predicate_type", ""),
            postgres_type=var_meta.get("postgres_type", "")
        )
        db.add(metadata_record)
    
    db.commit()
    logger.info(f"Stored metadata for {len(table_vars)} variables")


def generate_table_name(survey: str, year: int, table_id: str) -> str:
    """
    Generate a deterministic Postgres table name for an ACS table.
    
    Format: {survey}_{year}_{table_id_lower}
    Example: acs5_2023_b01001
    
    Args:
        survey: Survey type
        year: Survey year
        table_id: Table identifier
        
    Returns:
        Postgres table name
    """
    return f"{survey.lower()}_{year}_{table_id.lower()}"

