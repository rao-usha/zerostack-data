"""
Job management endpoints.
"""

import importlib
import logging
from typing import List, Dict, Tuple
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.core.models import IngestionJob, JobStatus
from app.core.schemas import JobCreate, JobResponse
from app.core.config import get_settings, MissingCensusAPIKeyError

logger = logging.getLogger(__name__)

# =============================================================================
# Universal Source Dispatch Table
# =============================================================================
# Maps dispatch key -> (module_path, function_name, [config_keys])
# Config keys are extracted from the job config dict and passed as kwargs.
#
# Dispatch key resolution order:
#   1. "{source}:{dataset}" — for multi-dataset sources (e.g. "fema:pa_projects")
#   2. "{source}"           — fallback for single-dataset sources
#
# Sources with complex config parsing (census, public_lp_strategies) are
# handled as special cases below.

SOURCE_DISPATCH: Dict[str, Tuple[str, str, List[str]]] = {
    # ── Treasury ──────────────────────────────────────────────────────────
    "treasury": (
        "app.sources.treasury.ingest",
        "ingest_treasury_daily_balance",
        ["start_date", "end_date"],
    ),
    "treasury:debt_outstanding": (
        "app.sources.treasury.ingest",
        "ingest_treasury_debt_outstanding",
        ["start_date", "end_date"],
    ),
    "treasury:interest_rates": (
        "app.sources.treasury.ingest",
        "ingest_treasury_interest_rates",
        ["start_date", "end_date", "security_type"],
    ),
    "treasury:monthly_statement": (
        "app.sources.treasury.ingest",
        "ingest_treasury_monthly_statement",
        ["start_date", "end_date", "classification"],
    ),
    "treasury:auctions": (
        "app.sources.treasury.ingest",
        "ingest_treasury_auctions",
        ["start_date", "end_date", "security_type"],
    ),
    # ── FRED ──────────────────────────────────────────────────────────────
    "fred": (
        "app.sources.fred.ingest",
        "ingest_fred_category",
        ["category", "series_ids", "observation_start", "observation_end", "api_key"],
    ),
    # ── Prediction Markets ────────────────────────────────────────────────
    "prediction_markets": (
        "app.sources.prediction_markets.ingest",
        "create_job",
        ["job_type", "target_platforms", "target_categories"],
    ),
    # ── EIA ───────────────────────────────────────────────────────────────
    "eia": (
        "app.sources.eia.ingest",
        "ingest_eia_petroleum_data",
        ["subcategory", "route", "frequency", "start", "end", "facets", "api_key"],
    ),
    "eia:natural_gas": (
        "app.sources.eia.ingest",
        "ingest_eia_natural_gas_data",
        ["subcategory", "route", "frequency", "start", "end", "facets", "api_key"],
    ),
    "eia:electricity": (
        "app.sources.eia.ingest",
        "ingest_eia_electricity_data",
        ["subcategory", "route", "frequency", "start", "end", "facets", "api_key"],
    ),
    "eia:retail_gas_prices": (
        "app.sources.eia.ingest",
        "ingest_eia_retail_gas_prices",
        ["frequency", "start", "end", "facets", "api_key"],
    ),
    "eia:steo": (
        "app.sources.eia.ingest",
        "ingest_eia_steo_projections",
        ["frequency", "start", "end", "facets", "api_key"],
    ),
    # ── NOAA ──────────────────────────────────────────────────────────────
    "noaa": (
        "app.sources.noaa.ingest",
        "create_noaa_table",
        ["dataset_key"],
    ),
    # ── BLS ───────────────────────────────────────────────────────────────
    "bls": (
        "app.sources.bls.ingest",
        "ingest_bls_dataset",
        ["dataset", "start_year", "end_year", "series_ids"],
    ),
    "bls:series": (
        "app.sources.bls.ingest",
        "ingest_bls_series",
        ["series_ids", "start_year", "end_year", "dataset"],
    ),
    # ── BEA ───────────────────────────────────────────────────────────────
    "bea": (
        "app.sources.bea.ingest",
        "ingest_nipa_data",
        ["table_name", "frequency", "year", "api_key"],
    ),
    "bea:regional": (
        "app.sources.bea.ingest",
        "ingest_regional_data",
        ["table_name", "line_code", "geo_fips", "year", "api_key"],
    ),
    "bea:gdp_industry": (
        "app.sources.bea.ingest",
        "ingest_gdp_by_industry_data",
        ["table_id", "frequency", "year", "industry", "api_key"],
    ),
    "bea:international": (
        "app.sources.bea.ingest",
        "ingest_international_data",
        ["indicator", "area_or_country", "frequency", "year", "api_key"],
    ),
    # ── FEMA ──────────────────────────────────────────────────────────────
    "fema": (
        "app.sources.fema.ingest",
        "ingest_disaster_declarations",
        ["state", "year", "disaster_type", "max_records"],
    ),
    "fema:pa_projects": (
        "app.sources.fema.ingest",
        "ingest_public_assistance_projects",
        ["state", "disaster_number", "max_records"],
    ),
    "fema:hma_projects": (
        "app.sources.fema.ingest",
        "ingest_hazard_mitigation_projects",
        ["state", "program_area", "max_records"],
    ),
    # ── FDIC ──────────────────────────────────────────────────────────────
    "fdic": (
        "app.sources.fdic.ingest",
        "ingest_bank_financials",
        ["cert", "report_date", "year", "limit"],
    ),
    "fdic:institutions": (
        "app.sources.fdic.ingest",
        "ingest_institutions",
        ["active_only", "state", "limit"],
    ),
    "fdic:failed_banks": (
        "app.sources.fdic.ingest",
        "ingest_failed_banks",
        ["year_start", "year_end", "limit"],
    ),
    "fdic:deposits": (
        "app.sources.fdic.ingest",
        "ingest_summary_of_deposits",
        ["year", "cert", "state", "limit"],
    ),
    # ── CMS ───────────────────────────────────────────────────────────────
    "cms": (
        "app.sources.cms.ingest",
        "ingest_medicare_utilization",
        ["year", "state", "limit"],
    ),
    "cms:hospital_cost_reports": (
        "app.sources.cms.ingest",
        "ingest_hospital_cost_reports",
        ["year", "limit"],
    ),
    "cms:drug_pricing": (
        "app.sources.cms.ingest",
        "ingest_drug_pricing",
        ["year", "brand_name", "limit"],
    ),
    # ── FBI Crime ─────────────────────────────────────────────────────────
    "fbi_crime": (
        "app.sources.fbi_crime.ingest",
        "ingest_fbi_crime_estimates",
        ["scope", "offenses", "states", "api_key"],
    ),
    "fbi_crime:summarized": (
        "app.sources.fbi_crime.ingest",
        "ingest_fbi_crime_summarized",
        ["states", "offenses", "since", "until", "api_key"],
    ),
    "fbi_crime:nibrs": (
        "app.sources.fbi_crime.ingest",
        "ingest_fbi_crime_nibrs",
        ["states", "variables", "api_key"],
    ),
    "fbi_crime:hate_crime": (
        "app.sources.fbi_crime.ingest",
        "ingest_fbi_hate_crime",
        ["states", "api_key"],
    ),
    "fbi_crime:leoka": (
        "app.sources.fbi_crime.ingest",
        "ingest_fbi_leoka",
        ["states", "api_key"],
    ),
    # ── IRS SOI ───────────────────────────────────────────────────────────
    "irs_soi": (
        "app.sources.irs_soi.ingest",
        "ingest_zip_income_data",
        ["year", "use_cache"],
    ),
    "irs_soi:county_income": (
        "app.sources.irs_soi.ingest",
        "ingest_county_income_data",
        ["year", "use_cache"],
    ),
    "irs_soi:migration": (
        "app.sources.irs_soi.ingest",
        "ingest_migration_data",
        ["year", "flow_type", "use_cache"],
    ),
    "irs_soi:business_income": (
        "app.sources.irs_soi.ingest",
        "ingest_business_income_data",
        ["year", "use_cache"],
    ),
    "irs_soi:all": (
        "app.sources.irs_soi.ingest",
        "ingest_all_soi_data",
        ["year", "use_cache"],
    ),
    # ── Data Commons ──────────────────────────────────────────────────────
    "data_commons": (
        "app.sources.data_commons.ingest",
        "ingest_statistical_variable",
        ["variable_dcid", "places", "api_key"],
    ),
    "data_commons:place_stats": (
        "app.sources.data_commons.ingest",
        "ingest_place_statistics",
        ["place_dcid", "variables", "api_key"],
    ),
    "data_commons:us_states": (
        "app.sources.data_commons.ingest",
        "ingest_us_state_data",
        ["variables", "api_key"],
    ),
    # ── FCC Broadband ─────────────────────────────────────────────────────
    "fcc_broadband": (
        "app.sources.fcc_broadband.ingest",
        "ingest_state_coverage",
        ["state_code", "include_summary"],
    ),
    "fcc_broadband:multiple_states": (
        "app.sources.fcc_broadband.ingest",
        "ingest_multiple_states",
        ["state_codes", "include_summary"],
    ),
    "fcc_broadband:all_states": (
        "app.sources.fcc_broadband.ingest",
        "ingest_all_states",
        ["include_summary"],
    ),
    "fcc_broadband:county": (
        "app.sources.fcc_broadband.ingest",
        "ingest_county_coverage",
        ["county_fips", "include_summary"],
    ),
    # ── Yelp ──────────────────────────────────────────────────────────────
    "yelp": (
        "app.sources.yelp.ingest",
        "ingest_businesses_by_location",
        ["location", "term", "categories", "limit", "api_key"],
    ),
    "yelp:multi_location": (
        "app.sources.yelp.ingest",
        "ingest_multiple_locations",
        ["locations", "term", "categories", "limit_per_location", "api_key"],
    ),
    "yelp:categories": (
        "app.sources.yelp.ingest",
        "ingest_business_categories",
        ["api_key"],
    ),
    # ── US Trade ──────────────────────────────────────────────────────────
    "us_trade": (
        "app.sources.us_trade.ingest",
        "ingest_exports_by_hs",
        ["year", "month", "hs_code", "country", "api_key"],
    ),
    "us_trade:imports_hs": (
        "app.sources.us_trade.ingest",
        "ingest_imports_by_hs",
        ["year", "month", "hs_code", "country", "api_key"],
    ),
    "us_trade:state_exports": (
        "app.sources.us_trade.ingest",
        "ingest_exports_by_state",
        ["year", "month", "state", "hs_code", "country", "api_key"],
    ),
    "us_trade:port_trade": (
        "app.sources.us_trade.ingest",
        "ingest_port_trade",
        ["year", "trade_type", "month", "district", "hs_code", "country", "api_key"],
    ),
    "us_trade:summary": (
        "app.sources.us_trade.ingest",
        "ingest_trade_summary",
        ["year", "month", "api_key"],
    ),
    # ── BTS ───────────────────────────────────────────────────────────────
    "bts": (
        "app.sources.bts.ingest",
        "ingest_border_crossing_data",
        ["start_date", "end_date", "state", "border", "measure", "app_token"],
    ),
    "bts:vmt": (
        "app.sources.bts.ingest",
        "ingest_vmt_data",
        ["start_date", "end_date", "state", "app_token"],
    ),
    "bts:faf": (
        "app.sources.bts.ingest",
        "ingest_faf_regional_data",
        ["version", "app_token"],
    ),
    # ── International Economics ────────────────────────────────────────────
    "international_econ": (
        "app.sources.international_econ.ingest",
        "ingest_worldbank_wdi",
        ["indicators", "countries", "start_year", "end_year"],
    ),
    "international_econ:worldbank_countries": (
        "app.sources.international_econ.ingest",
        "ingest_worldbank_countries",
        [],
    ),
    "international_econ:worldbank_indicators": (
        "app.sources.international_econ.ingest",
        "ingest_worldbank_indicators",
        ["search", "max_results"],
    ),
    "international_econ:imf_ifs": (
        "app.sources.international_econ.ingest",
        "ingest_imf_ifs",
        ["indicator", "countries", "start_year", "end_year"],
    ),
    "international_econ:oecd_mei": (
        "app.sources.international_econ.ingest",
        "ingest_oecd_mei",
        ["countries", "subjects", "start_period", "end_period"],
    ),
    "international_econ:oecd_kei": (
        "app.sources.international_econ.ingest",
        "ingest_oecd_kei",
        ["countries", "start_period", "end_period"],
    ),
    "international_econ:oecd_labor": (
        "app.sources.international_econ.ingest",
        "ingest_oecd_labor",
        ["countries", "start_period", "end_period"],
    ),
    "international_econ:oecd_trade": (
        "app.sources.international_econ.ingest",
        "ingest_oecd_trade",
        ["countries", "start_period", "end_period"],
    ),
    "international_econ:oecd_tax": (
        "app.sources.international_econ.ingest",
        "ingest_oecd_tax",
        ["countries", "start_period", "end_period"],
    ),
    "international_econ:bis_eer": (
        "app.sources.international_econ.ingest",
        "ingest_bis_eer",
        ["countries", "eer_type", "start_period", "end_period"],
    ),
    "international_econ:bis_property": (
        "app.sources.international_econ.ingest",
        "ingest_bis_property_prices",
        ["countries", "start_period", "end_period"],
    ),
    # ── Real Estate ───────────────────────────────────────────────────────
    "realestate": (
        "app.sources.realestate.ingest",
        "ingest_fhfa_hpi",
        ["geography_type", "start_date", "end_date"],
    ),
    "realestate:hud_permits": (
        "app.sources.realestate.ingest",
        "ingest_hud_permits",
        ["geography_type", "geography_id", "start_date", "end_date"],
    ),
    "realestate:redfin": (
        "app.sources.realestate.ingest",
        "ingest_redfin",
        ["region_type", "property_type"],
    ),
    "realestate:osm_buildings": (
        "app.sources.realestate.ingest",
        "ingest_osm_buildings",
        ["bounding_box", "building_type", "limit"],
    ),
    # ── USPTO ─────────────────────────────────────────────────────────────
    "uspto": (
        "app.sources.uspto.ingest",
        "ingest_patents",
        ["query", "start_date", "end_date", "limit"],
    ),
    "uspto:assignee": (
        "app.sources.uspto.ingest",
        "ingest_patents_by_assignee",
        ["assignee_name", "date_from", "date_to", "max_patents", "api_key"],
    ),
    "uspto:cpc": (
        "app.sources.uspto.ingest",
        "ingest_patents_by_cpc",
        ["cpc_code", "date_from", "date_to", "max_patents", "api_key"],
    ),
    "uspto:search": (
        "app.sources.uspto.ingest",
        "ingest_patents_by_search",
        ["search_query", "date_from", "date_to", "max_patents", "api_key"],
    ),
    # ── SEC ───────────────────────────────────────────────────────────────
    "sec": (
        "app.sources.sec.ingest",
        "ingest_company_filings",
        ["cik", "filing_types", "start_date", "end_date"],
    ),
    "sec:financial_data": (
        "app.sources.sec.ingest_xbrl",
        "ingest_company_financial_data",
        ["cik"],
    ),
    "sec:formadv": (
        "app.sources.sec.formadv_ingest",
        "ingest_family_offices",
        ["family_office_names", "max_concurrency", "max_requests_per_second"],
    ),
    "sec:formadv_crd": (
        "app.sources.sec.formadv_ingest",
        "ingest_firm_by_crd",
        ["crd_number"],
    ),
    # ── Kaggle ────────────────────────────────────────────────────────────
    "kaggle": (
        "app.sources.kaggle.ingest",
        "ingest_m5_dataset",
        ["force_download", "limit_items", "kaggle_username", "kaggle_key"],
    ),
    # ── Foot Traffic ──────────────────────────────────────────────────────
    "foot_traffic": (
        "app.sources.foot_traffic.ingest",
        "discover_brand_locations",
        ["brand_name", "city", "state", "latitude", "longitude", "limit"],
    ),
    # -- CFTC COT ----------------------------------------------------------
    "cftc_cot": (
        "app.sources.cftc_cot.ingest",
        "dispatch_cot_ingest",
        ["report_type", "year", "combined"],
    ),
    # -- USDA --------------------------------------------------------------
    "usda:crop": (
        "app.sources.usda.ingest",
        "dispatch_usda_crop",
        ["commodity", "year", "state", "all_stats", "api_key"],
    ),
    "usda:livestock": (
        "app.sources.usda.ingest",
        "dispatch_usda_livestock",
        ["commodity", "year", "state", "api_key"],
    ),
    "usda:annual_summary": (
        "app.sources.usda.ingest",
        "dispatch_usda_annual_summary",
        ["year", "api_key"],
    ),
    "usda:all_major_crops": (
        "app.sources.usda.ingest",
        "dispatch_usda_all_major_crops",
        ["year", "api_key"],
    ),
    # DUNL (S&P Global Data Unlocked)
    "dunl:currencies": (
        "app.sources.dunl.ingest",
        "ingest_dunl_currencies",
        [],
    ),
    "dunl:ports": (
        "app.sources.dunl.ingest",
        "ingest_dunl_ports",
        [],
    ),
    "dunl:uom": (
        "app.sources.dunl.ingest",
        "ingest_dunl_uom",
        [],
    ),
    "dunl:uom_conversions": (
        "app.sources.dunl.ingest",
        "ingest_dunl_uom_conversions",
        [],
    ),
    "dunl:calendars": (
        "app.sources.dunl.ingest",
        "ingest_dunl_calendars",
        ["years"],
    ),
    # ── Job Postings ────────────────────────────────────────────────────
    "job_postings:company": (
        "app.sources.job_postings.ingest",
        "ingest_job_postings_company",
        ["company_id", "force_rediscover"],
    ),
    "job_postings:all": (
        "app.sources.job_postings.ingest",
        "ingest_job_postings_all",
        ["limit", "skip_recent_hours"],
    ),
    "job_postings:discover": (
        "app.sources.job_postings.ingest",
        "ingest_job_postings_discover",
        ["company_id"],
    ),
}

router = APIRouter(prefix="/jobs", tags=["jobs"])


async def _run_quality_gate(db, job: IngestionJob):
    """
    Run data quality rules against a successfully completed job.

    Advisory only — logs results but never changes job status.
    Errors in the quality gate itself are swallowed so they never
    cause the ingestion job to appear failed.
    """
    try:
        from app.core.data_quality_service import evaluate_rules_for_job
        from app.core.models import DatasetRegistry

        # Find the most recent dataset registry entry for this source
        registry = (
            db.query(DatasetRegistry)
            .filter(DatasetRegistry.source == job.source)
            .order_by(DatasetRegistry.last_updated_at.desc())
            .first()
        )
        if not registry:
            logger.debug(
                f"Quality gate: no dataset registry entry for {job.source}, skipping"
            )
            return

        report = evaluate_rules_for_job(db, job, registry.table_name)
        if report.overall_status == "passed":
            logger.info(f"Quality gate passed for job {job.id} ({job.source})")
        else:
            logger.warning(
                f"Quality gate {report.overall_status} for job {job.id} ({job.source}): "
                f"{report.errors_count} errors, {report.warnings_count} warnings"
            )
    except Exception as e:
        logger.warning(f"Quality gate error for job {job.id}: {e}")


async def _handle_job_completion(db, job: IngestionJob):
    """
    Handle job completion: unblock dependent jobs, update chain status.

    Called after a job succeeds or fails (after retries exhausted).

    Args:
        db: Database session
        job: The completed job
    """
    from app.core import dependency_service

    # Check and unblock any dependent jobs
    unblocked_jobs = dependency_service.check_and_unblock_dependent_jobs(db, job.id)

    if unblocked_jobs:
        logger.info(
            f"Job {job.id} completion unblocked {len(unblocked_jobs)} dependent jobs: {unblocked_jobs}"
        )
        # Start the unblocked jobs
        await dependency_service.process_unblocked_jobs(db, unblocked_jobs)

    # Update chain execution status if this job is part of a chain
    execution = dependency_service.get_execution_for_job(db, job.id)
    if execution:
        dependency_service.update_chain_execution_status(db, execution.id)
        logger.info(f"Updated chain execution {execution.id} status")


async def _handle_job_failure(
    db, job: IngestionJob, error_message: str, error_type: str = None
):
    """
    Handle job failure: set status, schedule retry, send notifications.

    Args:
        db: Database session
        job: The failed job
        error_message: Error description
        error_type: Type of error (exception class name)
    """
    from datetime import datetime
    from app.core.retry_service import auto_schedule_retry
    from app.core import monitoring

    job.status = JobStatus.FAILED
    job.error_message = error_message
    if error_type:
        job.error_details = {"error_type": error_type}
    job.completed_at = datetime.utcnow()
    db.commit()

    # Try to schedule automatic retry
    retry_scheduled = auto_schedule_retry(db, job)

    if retry_scheduled:
        logger.info(
            f"Job {job.id} failed, retry scheduled (attempt {job.retry_count + 1}/{job.max_retries})"
        )
    else:
        # No more retries - send webhook notification and handle completion
        logger.warning(
            f"Job {job.id} failed permanently (exhausted {job.retry_count}/{job.max_retries} retries)"
        )
        try:
            await monitoring.notify_job_completion(
                job_id=job.id,
                source=job.source,
                status=JobStatus.FAILED,
                error_message=error_message,
                config=job.config,
            )
        except Exception as e:
            logger.error(f"Failed to send failure notification for job {job.id}: {e}")

        # Handle job completion (unblock dependent jobs, etc.)
        await _handle_job_completion(db, job)


async def _run_dispatched_job(db, job, job_id, source, config, monitoring):
    """Run a job via the SOURCE_DISPATCH registry."""
    from datetime import datetime

    # Resolve dispatch key: try "source:dataset" first, then "source"
    dataset = config.get("dataset") if config else None
    dispatch_key = (
        f"{source}:{dataset}"
        if dataset and f"{source}:{dataset}" in SOURCE_DISPATCH
        else source
    )
    module_path, func_name, config_keys = SOURCE_DISPATCH[dispatch_key]

    try:
        module = importlib.import_module(module_path)
        func = getattr(module, func_name)
    except (ImportError, AttributeError) as e:
        await _handle_job_failure(
            db,
            job,
            f"Failed to load ingest function for {source}: {e}",
            type(e).__name__,
        )
        return

    # Build kwargs from config, only passing keys that have non-None values
    kwargs = {}
    for key in config_keys:
        val = config.get(key)
        if val is not None:
            kwargs[key] = val

    try:
        result = await func(db=db, job_id=job_id, **kwargs)

        # Update job with success
        job.status = JobStatus.SUCCESS
        rows_inserted = 0
        if isinstance(result, dict):
            rows_inserted = (
                result.get("rows_inserted", 0) or result.get("total_records", 0) or 0
            )
        job.rows_inserted = rows_inserted
        job.completed_at = datetime.utcnow()
        db.commit()

        logger.info(f"Job {job_id} ({source}) completed successfully: {result}")

        # Run advisory quality gate
        await _run_quality_gate(db, job)

        # Send success notification
        try:
            await monitoring.notify_job_completion(
                job_id=job.id,
                source=job.source,
                status=JobStatus.SUCCESS,
                rows_inserted=rows_inserted,
                config=job.config,
            )
        except Exception as e:
            logger.error(f"Failed to send success notification for job {job_id}: {e}")

        await _handle_job_completion(db, job)

    except Exception as e:
        logger.exception(f"Error during {source} ingestion for job {job_id}")
        await _handle_job_failure(db, job, str(e), type(e).__name__)


async def _run_census_job(db, job, job_id, config, monitoring):
    """Handle census ingestion (complex config parsing)."""
    from datetime import datetime

    settings = get_settings()
    try:
        settings.require_census_api_key()
    except MissingCensusAPIKeyError as e:
        await _handle_job_failure(db, job, str(e), "MissingCensusAPIKeyError")
        return

    from app.sources.census.ingest import ingest_acs_table

    survey = config.get("survey", "acs5")
    year = config.get("year")
    table_id = config.get("table_id")
    geo_level = config.get("geo_level", "state")
    geo_filter = config.get("geo_filter")

    if not year or not table_id:
        await _handle_job_failure(
            db,
            job,
            "Missing required config: 'year' and 'table_id' are required",
            "ValidationError",
        )
        return

    try:
        include_geojson = config.get("include_geojson", False)
        result = await ingest_acs_table(
            db=db,
            job_id=job_id,
            survey=survey,
            year=year,
            table_id=table_id,
            geo_level=geo_level,
            geo_filter=geo_filter,
            include_geojson=include_geojson,
        )

        job.status = JobStatus.SUCCESS
        rows_inserted = result.get("rows_inserted", 0)
        job.rows_inserted = rows_inserted
        job.completed_at = datetime.utcnow()
        db.commit()

        logger.info(f"Job {job_id} completed successfully: {result}")

        # Run advisory quality gate
        await _run_quality_gate(db, job)

        try:
            await monitoring.notify_job_completion(
                job_id=job.id,
                source=job.source,
                status=JobStatus.SUCCESS,
                rows_inserted=rows_inserted,
                config=job.config,
            )
        except Exception as e:
            logger.error(f"Failed to send success notification for job {job_id}: {e}")

        await _handle_job_completion(db, job)

    except Exception as e:
        logger.exception(f"Error during Census ingestion for job {job_id}")
        await _handle_job_failure(db, job, str(e), type(e).__name__)


async def _run_public_lp_strategies_job(db, job, job_id, config, monitoring):
    """Handle public_lp_strategies ingestion (complex config parsing)."""
    from datetime import datetime
    from app.sources.public_lp_strategies.ingest import ingest_lp_strategy_document
    from app.sources.public_lp_strategies.types import (
        LpDocumentInput,
        DocumentTextSectionInput,
        StrategySnapshotInput,
        AssetClassAllocationInput,
        AssetClassProjectionInput,
        ThematicTagInput,
    )

    lp_name = config.get("lp_name")
    program = config.get("program")
    fiscal_year = config.get("fiscal_year")
    fiscal_quarter = config.get("fiscal_quarter")
    document_metadata = config.get("document_metadata", {})
    parsed_sections = config.get("parsed_sections", [])
    extracted_strategy = config.get("extracted_strategy", {})

    if not all([lp_name, program, fiscal_year, fiscal_quarter]):
        await _handle_job_failure(
            db,
            job,
            "Missing required config: 'lp_name', 'program', 'fiscal_year', 'fiscal_quarter'",
            "ValidationError",
        )
        return

    try:
        document_input = LpDocumentInput(lp_id=0, **document_metadata)
        text_sections = [DocumentTextSectionInput(**s) for s in parsed_sections]
        strategy_input = StrategySnapshotInput(
            lp_id=0,
            program=program,
            fiscal_year=fiscal_year,
            fiscal_quarter=fiscal_quarter,
            **extracted_strategy.get("strategy", {}),
        )
        allocations = [
            AssetClassAllocationInput(**a)
            for a in extracted_strategy.get("allocations", [])
        ]
        projections = [
            AssetClassProjectionInput(**p)
            for p in extracted_strategy.get("projections", [])
        ]
        thematic_tags = [
            ThematicTagInput(**t) for t in extracted_strategy.get("thematic_tags", [])
        ]

        result = ingest_lp_strategy_document(
            db=db,
            lp_name=lp_name,
            document_input=document_input,
            text_sections=text_sections,
            strategy_input=strategy_input,
            allocations=allocations,
            projections=projections,
            thematic_tags=thematic_tags,
        )

        job.status = JobStatus.SUCCESS
        rows_inserted = result.get("sections_count", 0) + result.get(
            "allocations_count", 0
        )
        job.rows_inserted = rows_inserted
        job.completed_at = datetime.utcnow()
        db.commit()

        logger.info(f"Job {job_id} completed successfully: {result}")

        # Run advisory quality gate
        await _run_quality_gate(db, job)

        try:
            await monitoring.notify_job_completion(
                job_id=job.id,
                source=job.source,
                status=JobStatus.SUCCESS,
                rows_inserted=rows_inserted,
                config=job.config,
            )
        except Exception as e:
            logger.error(f"Failed to send success notification for job {job_id}: {e}")

        await _handle_job_completion(db, job)

    except Exception as e:
        logger.exception(
            f"Error during public_lp_strategies ingestion for job {job_id}"
        )
        await _handle_job_failure(db, job, str(e), type(e).__name__)


async def run_ingestion_job(job_id: int, source: str, config: dict):
    """
    Background task to run ingestion job.

    On failure, automatically schedules retry if retries remain.
    Sends webhook notification on final failure (no retries left).
    """
    from datetime import datetime
    from app.core.database import get_session_factory
    from app.core import monitoring

    SessionLocal = get_session_factory()
    db = SessionLocal()

    try:
        # Update job status to running
        job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
        if not job:
            logger.error(f"Job {job_id} not found")
            return

        job.status = JobStatus.RUNNING
        job.started_at = datetime.utcnow()
        db.commit()

        # Route to appropriate source adapter
        if source == "census":
            await _run_census_job(db, job, job_id, config, monitoring)

        elif source == "public_lp_strategies":
            await _run_public_lp_strategies_job(db, job, job_id, config, monitoring)

        elif source in SOURCE_DISPATCH or any(
            k.startswith(f"{source}:") for k in SOURCE_DISPATCH
        ):
            await _run_dispatched_job(db, job, job_id, source, config, monitoring)

        else:
            await _handle_job_failure(
                db, job, f"Unknown source: {source}", "UnknownSourceError"
            )

    except Exception as e:
        logger.exception(f"Error running job {job_id}")
        job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
        if job:
            await _handle_job_failure(db, job, str(e), type(e).__name__)

    finally:
        db.close()


@router.post("", response_model=JobResponse, status_code=201)
async def create_job(
    job_request: JobCreate,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
) -> JobResponse:
    """
    Create a new ingestion job.

    The job will run asynchronously in the background.
    """
    # Validate source
    # Valid sources include base names (before ":") from composite keys
    _base_sources = {k.split(":")[0] for k in SOURCE_DISPATCH}
    valid_sources = sorted({"census", "public_lp_strategies"} | _base_sources)
    if job_request.source not in valid_sources:
        raise HTTPException(
            status_code=400, detail=f"Invalid source. Must be one of: {valid_sources}"
        )

    # Create job record
    job = IngestionJob(
        source=job_request.source, status=JobStatus.PENDING, config=job_request.config
    )
    db.add(job)
    db.commit()
    db.refresh(job)

    logger.info(f"Created job {job.id} for source {job.source}")

    # Audit trail
    try:
        from app.core import audit_service

        audit_service.log_collection(
            db,
            trigger_type="api",
            source=job.source,
            job_id=job.id,
            job_type="ingestion",
            trigger_source="/jobs",
            config_snapshot=job.config,
        )
    except Exception as e:
        logger.debug("Audit trail logging failed: %s", e)

    # Start ingestion — queue if WORKER_MODE is on, else background task
    from app.core.job_queue_service import submit_job

    submit_job(
        db=db,
        job_type="ingestion",
        payload={
            "source": job.source,
            "config": job.config or {},
            "ingestion_job_id": job.id,
        },
        priority=5,
        job_table_id=job.id,
        background_tasks=background_tasks,
        background_func=run_ingestion_job,
        background_args=(job.id, job.source, job.config),
    )

    return JobResponse.model_validate(job)


@router.get("/workers")
def get_worker_status(db: Session = Depends(get_db)):
    """
    Get active worker status.

    Shows which workers are running, what jobs they're processing,
    and heartbeat health.
    """
    from sqlalchemy import text as sa_text
    from datetime import datetime, timedelta

    # Get all workers with recent activity (heartbeat within last 5 min)
    cutoff = datetime.utcnow() - timedelta(minutes=5)

    rows = db.execute(
        sa_text("""
            SELECT
                worker_id,
                COUNT(*) FILTER (WHERE status = 'RUNNING') as running_jobs,
                COUNT(*) FILTER (WHERE status = 'CLAIMED') as claimed_jobs,
                MAX(heartbeat_at) as last_heartbeat,
                json_agg(json_build_object(
                    'job_id', id,
                    'job_type', job_type,
                    'status', status,
                    'source', payload->>'source',
                    'tier', payload->>'tier',
                    'progress_pct', progress_pct,
                    'progress_message', progress_message,
                    'started_at', started_at,
                    'heartbeat_at', heartbeat_at
                )) FILTER (WHERE status IN ('RUNNING', 'CLAIMED')) as active_jobs
            FROM job_queue
            WHERE worker_id IS NOT NULL
            AND heartbeat_at >= :cutoff
            GROUP BY worker_id
            ORDER BY last_heartbeat DESC
        """),
        {"cutoff": cutoff},
    ).fetchall()

    workers = []
    for row in rows:
        last_hb = row[3]
        stale = (datetime.utcnow() - last_hb).total_seconds() > 60 if last_hb else True
        workers.append({
            "worker_id": row[0],
            "running_jobs": row[1],
            "claimed_jobs": row[2],
            "last_heartbeat": last_hb.isoformat() if last_hb else None,
            "healthy": not stale,
            "active_jobs": row[4] or [],
        })

    # Queue summary
    summary = db.execute(
        sa_text("""
            SELECT
                COUNT(*) FILTER (WHERE status = 'PENDING') as pending,
                COUNT(*) FILTER (WHERE status = 'CLAIMED') as claimed,
                COUNT(*) FILTER (WHERE status = 'RUNNING') as running,
                COUNT(*) FILTER (WHERE status = 'SUCCESS'
                    AND completed_at >= NOW() - INTERVAL '1 hour') as recent_success,
                COUNT(*) FILTER (WHERE status = 'FAILED'
                    AND completed_at >= NOW() - INTERVAL '1 hour') as recent_failed
            FROM job_queue
        """)
    ).fetchone()

    return {
        "workers": workers,
        "total_active_workers": len(workers),
        "queue": {
            "pending": summary[0],
            "claimed": summary[1],
            "running": summary[2],
            "recent_success_1h": summary[3],
            "recent_failed_1h": summary[4],
        },
    }


@router.get("/{job_id}", response_model=JobResponse)
def get_job(job_id: int, db: Session = Depends(get_db)) -> JobResponse:
    """
    Get status and details of a specific job.
    """
    job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    return JobResponse.model_validate(job)


@router.get("", response_model=List[JobResponse])
def list_jobs(
    source: str = None,
    status: JobStatus = None,
    limit: int = 100,
    offset: int = 0,
    db: Session = Depends(get_db),
) -> List[JobResponse]:
    """
    List ingestion jobs with optional filtering.
    """
    query = db.query(IngestionJob)

    if source:
        query = query.filter(IngestionJob.source == source)
    if status:
        query = query.filter(IngestionJob.status == status)

    query = query.order_by(IngestionJob.created_at.desc()).offset(offset).limit(limit)

    jobs = query.all()
    return [JobResponse.model_validate(job) for job in jobs]


# =============================================================================
# Retry Endpoints
# =============================================================================


@router.get("/failed/summary")
def get_failed_jobs_summary(db: Session = Depends(get_db)):
    """
    Get summary of failed jobs by source.

    Returns counts of retryable vs exhausted jobs.
    """
    from app.core.retry_service import get_failed_jobs_summary as get_summary

    return get_summary(db)


@router.post("/{job_id}/retry", response_model=JobResponse)
async def retry_job(
    job_id: int, background_tasks: BackgroundTasks, db: Session = Depends(get_db)
) -> JobResponse:
    """
    Retry a failed job.

    The job must be in FAILED status and have retries remaining.
    """
    from app.core.retry_service import mark_job_for_immediate_retry

    job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    if job.status != JobStatus.FAILED:
        raise HTTPException(
            status_code=400,
            detail=f"Job is not in failed status (current: {job.status.value})",
        )

    if not job.can_retry:
        raise HTTPException(
            status_code=400,
            detail=f"Job has exhausted all retries ({job.retry_count}/{job.max_retries})",
        )

    # Mark job for retry
    updated_job = mark_job_for_immediate_retry(db, job_id)
    if not updated_job:
        raise HTTPException(status_code=500, detail="Failed to mark job for retry")

    # Start ingestion — queue or background
    from app.core.job_queue_service import submit_job

    submit_job(
        db=db,
        job_type="ingestion",
        payload={
            "source": updated_job.source,
            "config": updated_job.config or {},
            "ingestion_job_id": updated_job.id,
        },
        priority=5,
        job_table_id=updated_job.id,
        background_tasks=background_tasks,
        background_func=run_ingestion_job,
        background_args=(updated_job.id, updated_job.source, updated_job.config),
    )

    logger.info(
        f"Retrying job {job_id} (attempt {updated_job.retry_count}/{updated_job.max_retries})"
    )

    return JobResponse.model_validate(updated_job)


@router.post("/{job_id}/cancel", response_model=JobResponse)
async def cancel_job(
    job_id: int, db: Session = Depends(get_db)
) -> JobResponse:
    """
    Cancel a running or pending job.

    Sets the job status to FAILED with error_message "Cancelled by user".
    Does not kill the background task mid-execution, but marks it as cancelled.
    """
    from datetime import datetime

    job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    if job.status not in (JobStatus.RUNNING, JobStatus.PENDING):
        raise HTTPException(
            status_code=400,
            detail=f"Job is not running or pending (current: {job.status.value})",
        )

    job.status = JobStatus.FAILED
    job.error_message = "Cancelled by user"
    job.completed_at = datetime.utcnow()
    db.commit()
    db.refresh(job)

    logger.info(f"Job {job_id} cancelled by user")

    return JobResponse.model_validate(job)


@router.post("/{job_id}/restart", response_model=JobResponse)
async def restart_job(
    job_id: int, background_tasks: BackgroundTasks, db: Session = Depends(get_db)
) -> JobResponse:
    """
    Restart a pending, failed, or completed job.

    Resets the job to PENDING and re-dispatches it as a background task.
    Works for any non-running job status.
    """
    job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    if job.status == JobStatus.RUNNING:
        raise HTTPException(
            status_code=400,
            detail="Job is currently running. Wait for it to complete or fail.",
        )

    # Reset job state
    job.status = JobStatus.PENDING
    job.started_at = None
    job.completed_at = None
    job.error_message = None
    job.error_details = None
    job.rows_inserted = None
    db.commit()
    db.refresh(job)

    # Re-dispatch — queue or background
    from app.core.job_queue_service import submit_job

    submit_job(
        db=db,
        job_type="ingestion",
        payload={
            "source": job.source,
            "config": job.config or {},
            "ingestion_job_id": job.id,
        },
        priority=5,
        job_table_id=job.id,
        background_tasks=background_tasks,
        background_func=run_ingestion_job,
        background_args=(job.id, job.source, job.config),
    )

    logger.info(f"Restarting job {job_id} (source={job.source})")

    return JobResponse.model_validate(job)


@router.post("/retry/all")
async def retry_all_failed_jobs(
    background_tasks: BackgroundTasks,
    source: str = None,
    limit: int = 10,
    db: Session = Depends(get_db),
):
    """
    Retry all eligible failed jobs.

    Args:
        source: Optional filter by source (e.g., "fred", "sec")
        limit: Maximum number of jobs to retry (default 10)

    Returns summary of retry operations.
    """
    from app.core.retry_service import retry_all_eligible_jobs

    results = retry_all_eligible_jobs(db, source=source, limit=limit)

    # Schedule retried jobs — queue or background
    from app.core.job_queue_service import submit_job

    for job_info in results["retried"]:
        job = (
            db.query(IngestionJob).filter(IngestionJob.id == job_info["job_id"]).first()
        )
        if job:
            submit_job(
                db=db,
                job_type="ingestion",
                payload={
                    "source": job.source,
                    "config": job.config or {},
                    "ingestion_job_id": job.id,
                },
                priority=5,
                job_table_id=job.id,
                background_tasks=background_tasks,
                background_func=run_ingestion_job,
                background_args=(job.id, job.source, job.config),
            )

    return {"message": f"Scheduled {len(results['retried'])} jobs for retry", **results}


# =============================================================================
# Data Quality Validation Endpoints
# =============================================================================


@router.get("/{job_id}/validate")
def validate_job_data(
    job_id: int,
    table_name: str,
    expected_min_rows: int = 1,
    db: Session = Depends(get_db),
):
    """
    Validate data quality for a completed ingestion job.

    Performs checks including:
    - Row count validation
    - Null value detection
    - Duplicate detection
    - Range validation for numeric fields

    Args:
        job_id: The ingestion job ID
        table_name: The table that was populated
        expected_min_rows: Minimum expected row count (default 1)

    Returns:
        Validation results with pass/fail status for each check
    """
    from app.core.data_quality import (
        validate_ingestion_job,
        get_default_validation_config,
    )

    # Get job to determine source
    job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    if job.status != JobStatus.SUCCESS:
        raise HTTPException(
            status_code=400,
            detail=f"Job is not in success status (current: {job.status.value})",
        )

    # Get default validation config for source
    dataset = job.config.get("dataset") if job.config else None
    default_config = get_default_validation_config(job.source, dataset)

    # Override with provided parameters
    validation_config = {**default_config, "expected_min_rows": expected_min_rows}

    try:
        results = validate_ingestion_job(
            db=db,
            job_id=job_id,
            table_name=table_name,
            validation_config=validation_config,
        )
        return results
    except Exception as e:
        logger.error(f"Data validation failed for job {job_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Validation failed: {str(e)}")


# =============================================================================
# Monitoring Endpoints
# =============================================================================


@router.get("/monitoring/metrics")
def get_job_metrics(hours: int = 24, source: str = None, db: Session = Depends(get_db)):
    """
    Get job metrics for monitoring.

    Returns success/failure rates, durations, and recent failures.

    Args:
        hours: Time window in hours (default 24)
        source: Optional filter by source
    """
    from app.core.monitoring import JobMonitor

    monitor = JobMonitor(db)
    return monitor.get_job_metrics(hours=hours, source=source)


@router.get("/monitoring/health")
def get_source_health(db: Session = Depends(get_db)):
    """
    Get health status for each data source.

    Returns health scores based on recent job success rates.
    Sources are classified as:
    - healthy: 100% success rate
    - warning: Some failures but majority success
    - degraded: >50% failure rate
    - critical: 0% success rate
    """
    from app.core.monitoring import JobMonitor

    monitor = JobMonitor(db)
    return monitor.get_source_health()


@router.get("/monitoring/alerts")
def get_active_alerts(
    failure_threshold: int = 3,
    time_window_hours: int = 1,
    db: Session = Depends(get_db),
):
    """
    Get active alerts for job failures.

    Alert types:
    - high_failure_rate: Multiple failures for a source
    - stuck_job: Job running longer than 2 hours
    - data_staleness: No jobs for 24+ hours

    Args:
        failure_threshold: Number of failures to trigger alert (default 3)
        time_window_hours: Time window for failure count (default 1)
    """
    from app.core.monitoring import JobMonitor

    monitor = JobMonitor(db)
    return monitor.check_alerts(
        failure_threshold=failure_threshold, time_window_hours=time_window_hours
    )


@router.get("/monitoring/dashboard")
def get_monitoring_dashboard(db: Session = Depends(get_db)):
    """
    Get comprehensive monitoring dashboard.

    Returns all metrics, health status, and alerts in one call.
    Includes:
    - 24h and 1h metrics
    - Source health status
    - Active alerts
    """
    from app.core.monitoring import get_monitoring_dashboard as get_dashboard

    return get_dashboard(db)


# =============================================================================
# Nightly Batch Endpoints
# =============================================================================


@router.post("/nightly/launch")
async def launch_nightly(
    tiers: List[int] = None,
    sources: List[str] = None,
    db: Session = Depends(get_db),
):
    """
    Launch a nightly batch collection.

    Enqueues all data sources across 4 priority tiers:
    - Tier 1 (priority 10): Fast gov APIs (treasury, fred, bea, fdic, fema, bts, cftc, data_commons)
    - Tier 2 (priority 7): Medium APIs (eia, bls, noaa, cms, fbi, irs, usda, trade, fcc, etc.)
    - Tier 3 (priority 5): Complex sources (sec, kaggle, int'l econ, census, foot_traffic, yelp)
    - Tier 4 (priority 3): Agentic/LLM (site_intel, people, PE)

    Args:
        tiers: Optional list of tier levels to run (default: all)
        sources: Optional list of specific source keys to run
    """
    from app.core.nightly_batch_service import launch_nightly_batch

    batch = await launch_nightly_batch(db, tiers=tiers, sources=sources)
    return {
        "batch_id": batch.id,
        "total_jobs": batch.total_jobs,
        "status": batch.status,
        "started_at": batch.started_at.isoformat() if batch.started_at else None,
    }


@router.get("/nightly/{batch_id}")
def get_nightly_status(batch_id: int, db: Session = Depends(get_db)):
    """
    Get nightly batch progress.

    Returns overall status, per-tier breakdown, and individual job details.
    """
    from app.core.nightly_batch_service import get_batch_status

    result = get_batch_status(db, batch_id)
    if not result:
        raise HTTPException(status_code=404, detail="Batch not found")
    return result


@router.get("/nightly")
def list_nightly_batches(
    limit: int = 20,
    status: str = None,
    db: Session = Depends(get_db),
):
    """List recent nightly batch runs."""
    from app.core.nightly_batch_service import list_batches

    return list_batches(db, limit=limit, status=status)


