"""
Job management endpoints.
"""

import importlib
import logging
from datetime import datetime
from typing import List, Dict, Tuple
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.core.models import IngestionJob, IngestionSchedule, JobStatus, BatchTierConfig
from app.core.schemas import JobCreate, JobResponse, BackfillRequest
from app.core.config import get_settings, MissingCensusAPIKeyError
from app.core.safe_sql import qi

logger = logging.getLogger(__name__)


# =============================================================================
# API Key Pre-Flight Check
# =============================================================================


def _check_api_key_preflight(source: str):
    """Return error message if required API key is missing, else None."""
    from app.core.api_registry import API_REGISTRY, APIKeyRequirement
    from app.core.config import get_settings

    # Strip dataset suffix (e.g. "job_postings:all" → "job_postings")
    base_source = source.split(":")[0]

    api_config = API_REGISTRY.get(base_source)
    if not api_config or api_config.api_key_requirement != APIKeyRequirement.REQUIRED:
        return None

    settings = get_settings()
    try:
        settings.get_api_key(base_source, required=True)
        return None
    except Exception:
        return f"API key required for '{base_source}' but not configured"


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
    # ── USAspending ─────────────────────────────────────────────────────
    "usaspending": (
        "app.sources.usaspending.ingest",
        "ingest_usaspending_awards",
        ["naics_codes", "states", "start_date", "end_date", "award_type_codes", "min_amount", "max_pages"],
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
        "monitor_all_platforms",
        ["kalshi_categories", "limit_per_platform"],
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
    # ── NPPES NPI Registry ────────────────────────────────────────────────
    "nppes": (
        "app.sources.nppes.ingest",
        "ingest_nppes_providers",
        ["states", "taxonomy_codes", "taxonomy_description", "enumeration_type", "city", "postal_code", "limit"],
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
    # ── openFDA ─────────────────────────────────────────────────────────
    "fda": (
        "app.sources.fda.ingest",
        "ingest_device_registrations",
        ["states", "search_query", "limit_per_state"],
    ),
    # ── SAM.gov ──────────────────────────────────────────────────────────
    "sam_gov": (
        "app.sources.sam_gov.ingest",
        "ingest_sam_gov_entities",
        ["state", "naics_code", "legal_business_name", "max_pages"],
    ),
    # ── OSHA ─────────────────────────────────────────────────────────────
    "osha": (
        "app.sources.osha.ingest",
        "ingest_osha_all",
        ["dataset"],
    ),
    "osha:inspections": (
        "app.sources.osha.ingest",
        "ingest_osha_inspections",
        [],
    ),
    "osha:violations": (
        "app.sources.osha.ingest",
        "ingest_osha_violations",
        [],
    ),
    # ── CourtListener ────────────────────────────────────────────────────
    "courtlistener": (
        "app.sources.courtlistener.ingest",
        "ingest_courtlistener_dockets",
        ["query", "court", "filed_after", "filed_before", "max_pages"],
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

        # Phase 1: Auto-profile table after ingestion
        try:
            from app.core.data_profiling_service import profile_table
            snapshot = profile_table(db, registry.table_name, job_id=job.id, source=job.source)
            if snapshot:
                logger.info(f"Quality gate: profiled {registry.table_name} ({snapshot.row_count} rows)")

                # Phase 2: Run anomaly detection against the new profile
                try:
                    from app.core.anomaly_detection_service import detect_anomalies
                    anomalies = detect_anomalies(db, snapshot, registry.table_name)
                    if anomalies:
                        logger.warning(
                            f"Quality gate: {len(anomalies)} anomalies for {registry.table_name}"
                        )
                except Exception as ae:
                    logger.warning(f"Anomaly detection error for job {job.id}: {ae}")
        except Exception as pe:
            logger.warning(f"Profiling error for job {job.id}: {pe}")

        # Phase 3: Data continuity checks
        try:
            from app.core.data_quality_service import check_row_count_delta
            from sqlalchemy import text as sa_text

            count_result = db.execute(sa_text(f'SELECT COUNT(*) FROM {qi(registry.table_name)}'))
            current_count = count_result.scalar()
            check_row_count_delta(db, job, registry.table_name, current_count)
        except Exception as ce:
            logger.debug(f"Row count delta check skipped for job {job.id}: {ce}")

        # Phase 3b: Date gap check for tables with known date columns
        DATE_COLUMN_MAP = {
            "fred_": "date",
            "bls_": "date",
            "treasury_": "record_date",
            "eia_": "period",
        }
        try:
            from app.core.data_quality_service import check_date_gaps

            date_col = None
            for prefix, col in DATE_COLUMN_MAP.items():
                if registry.table_name.startswith(prefix):
                    date_col = col
                    break
            if date_col:
                check_date_gaps(db, registry.table_name, date_col, job.id)
        except Exception as de:
            logger.debug(f"Date gap check skipped for job {job.id}: {de}")

    except Exception as e:
        logger.warning(f"Quality gate error for job {job.id}: {e}")


def _advance_schedule_watermark(db, job: IngestionJob):
    """
    Advance schedule watermark after successful job completion.

    Only called when job.status == SUCCESS. This ensures that if a job fails,
    last_run_at stays unchanged and the next scheduled run retries the same window.
    """
    if not job.schedule_id:
        return  # Not a scheduled job — nothing to advance

    schedule = db.query(IngestionSchedule).filter_by(id=job.schedule_id).first()
    if schedule:
        schedule.last_run_at = job.completed_at or datetime.utcnow()
        db.commit()
        logger.info(f"Advanced watermark for schedule {schedule.id} to {schedule.last_run_at}")


async def _handle_job_completion(db, job: IngestionJob):
    """
    Handle job completion: unblock dependent jobs, update chain status,
    and advance schedule watermark on success.

    Called after a job succeeds or fails (after retries exhausted).

    Args:
        db: Database session
        job: The completed job
    """
    from app.core import dependency_service

    # Advance schedule watermark on success only
    if job.status == JobStatus.SUCCESS:
        _advance_schedule_watermark(db, job)

        # Also advance global source watermark (for future manual/batch jobs).
        # Backfill jobs are excluded to prevent moving the watermark backward.
        if job.trigger != "backfill" and not (job.config or {}).get("_backfill"):
            try:
                from app.core.watermark_service import advance_watermark

                advance_watermark(
                    db, job.source, job.completed_at or datetime.utcnow(), job.id
                )
            except Exception as e:
                logger.warning(
                    f"Failed to advance source watermark for {job.source}: {e}"
                )

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

    # Check if this completes a batch → send summary webhook
    if job.batch_run_id:
        try:
            from app.core.nightly_batch_service import check_and_notify_batch_completion

            await check_and_notify_batch_completion(db, job.batch_run_id)
        except Exception as e:
            logger.error(f"Batch completion check error for {job.batch_run_id}: {e}")


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

    # Strip split suffix (e.g. "cms:split_0" → "cms") for dispatch lookup
    base_source = source.split(":split_")[0] if ":split_" in source else source

    # Resolve dispatch key: try "source:dataset" first, then base source
    dataset = config.get("dataset") if config else None
    dispatch_key = (
        f"{base_source}:{dataset}"
        if dataset and f"{base_source}:{dataset}" in SOURCE_DISPATCH
        else base_source
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
        # Pass job_id only if the function accepts it
        import inspect
        sig = inspect.signature(func)
        if "job_id" in sig.parameters:
            result = await func(db=db, job_id=job_id, **kwargs)
        else:
            result = await func(db=db, **kwargs)

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

    if not year:
        year = 2023  # Latest full ACS5 release
    if not table_id:
        table_id = "B01001"  # Total population by sex/age — most commonly used

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

        # API key pre-flight: fail fast if required key is missing
        key_error = _check_api_key_preflight(source)
        if key_error:
            job.status = JobStatus.FAILED
            job.error_message = key_error
            job.completed_at = datetime.utcnow()
            db.commit()
            logger.warning(f"Job {job_id} failed pre-flight: {key_error}")
            # Trigger downstream: unblock dependents, batch completion check
            try:
                await _handle_job_completion(db, job)
            except Exception as e:
                logger.error(f"Completion handler error after pre-flight for job {job_id}: {e}")
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
        ) or (":split_" in source and source.split(":split_")[0] in SOURCE_DISPATCH):
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
        source=job_request.source,
        status=JobStatus.PENDING,
        config=job_request.config,
        trigger="manual",
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


# =============================================================================
# Batch Collection Endpoints
# (Must be above /{job_id} to avoid path parameter capturing "batch")
# =============================================================================


@router.post("/batch/launch")
async def launch_batch(
    tiers: List[int] = None,
    sources: List[str] = None,
    db: Session = Depends(get_db),
):
    """
    Launch a batch collection.

    Enqueues all data sources across 4 priority tiers.
    Each job is tagged with a shared batch_run_id. Status is always
    computed live from individual job statuses — nothing can get stuck.

    Args:
        tiers: Optional list of tier levels to run (default: all)
        sources: Optional list of specific source keys to run
    """
    from app.core.nightly_batch_service import launch_batch_collection

    result = await launch_batch_collection(db, tiers=tiers, sources=sources)
    return result


@router.get("/batch/runs/{batch_run_id}")
def get_batch_run(batch_run_id: str, db: Session = Depends(get_db)):
    """
    Get batch run status.

    Status is computed live from job statuses — nothing can get stuck.
    """
    from app.core.nightly_batch_service import get_batch_run_status

    result = get_batch_run_status(db, batch_run_id)
    if not result:
        raise HTTPException(status_code=404, detail="Batch run not found")
    return result


@router.post("/batch/{batch_run_id}/cancel")
def cancel_batch(batch_run_id: str, db: Session = Depends(get_db)):
    """
    Cancel all PENDING/RUNNING jobs in a batch.

    Running jobs stop within one heartbeat interval (30s).
    Already-completed jobs are unaffected.
    """
    from app.core.nightly_batch_service import cancel_batch_run

    result = cancel_batch_run(db, batch_run_id)
    if result is None:
        raise HTTPException(status_code=404, detail="Batch run not found")
    return result


@router.post("/batch/{batch_run_id}/rerun-failed")
async def rerun_failed_batch_jobs(
    batch_run_id: str,
    db: Session = Depends(get_db),
):
    """
    Rerun all FAILED jobs in a batch.

    Creates new queue entries for each failed job, resets IngestionJob to
    PENDING, and returns a summary. Respects tier ordering — tier 2+ jobs
    start as BLOCKED if lower tiers have jobs being rerun.
    """
    from app.core.nightly_batch_service import rerun_failed_in_batch

    result = rerun_failed_in_batch(db, batch_run_id)
    if result is None:
        raise HTTPException(status_code=404, detail="Batch run not found")
    return result


@router.get("/batch/runs")
def list_batch_runs_endpoint(
    limit: int = 20,
    status: str = None,
    db: Session = Depends(get_db),
):
    """List recent batch runs with live status."""
    from app.core.nightly_batch_service import list_batch_runs

    return list_batch_runs(db, limit=limit, status=status)


# =============================================================================
# Backfill
# =============================================================================


@router.post("/backfill")
async def launch_backfill_endpoint(
    request: BackfillRequest,
    db: Session = Depends(get_db),
):
    """
    Launch backfill jobs for one or more sources with a specific date range.

    Translates universal start/end dates into source-specific query parameters.
    Backfill jobs do NOT advance the source watermark.
    """
    from app.core.backfill_service import launch_backfill

    # Validate sources
    _base_sources = {k.split(":")[0] for k in SOURCE_DISPATCH}
    valid_sources = {"census", "public_lp_strategies"} | _base_sources
    invalid = [s for s in request.sources if s.split(":")[0] not in valid_sources]
    if invalid:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid sources: {invalid}. Valid: {sorted(valid_sources)}",
        )

    # Parse dates
    try:
        start_date = datetime.strptime(request.start_date, "%Y-%m-%d")
        end_date = datetime.strptime(request.end_date, "%Y-%m-%d")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid date format: {e}")

    if end_date < start_date:
        raise HTTPException(
            status_code=400, detail="end_date must be >= start_date"
        )

    result = launch_backfill(
        db,
        sources=request.sources,
        start_date=start_date,
        end_date=end_date,
        extra_config=request.config or None,
    )
    return result


# =============================================================================
# Deprecated Nightly Endpoints (wrappers for backwards compatibility)
# =============================================================================


@router.post("/nightly/launch", deprecated=True)
async def launch_nightly(
    tiers: List[int] = None,
    sources: List[str] = None,
    db: Session = Depends(get_db),
):
    """Deprecated: Use POST /batch/launch instead."""
    from app.core.nightly_batch_service import launch_batch_collection

    return await launch_batch_collection(db, tiers=tiers, sources=sources)


@router.get("/nightly/{batch_id}", deprecated=True)
def get_nightly_status(batch_id: str, db: Session = Depends(get_db)):
    """Deprecated: Use GET /batch/runs/{batch_run_id} instead."""
    from app.core.nightly_batch_service import get_batch_status

    result = get_batch_status(db, batch_id)
    if not result:
        raise HTTPException(status_code=404, detail="Batch not found")
    return result


@router.get("/nightly", deprecated=True)
def list_nightly_batches(
    limit: int = 20,
    status: str = None,
    db: Session = Depends(get_db),
):
    """Deprecated: Use GET /batch/runs instead."""
    from app.core.nightly_batch_service import list_batches

    return list_batches(db, limit=limit, status=status)


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
    batch_run_id: str = None,
    trigger: str = None,
    limit: int = 100,
    offset: int = 0,
    db: Session = Depends(get_db),
) -> List[JobResponse]:
    """
    List ingestion jobs with optional filtering.

    Args:
        batch_run_id: Filter to jobs in a specific batch run
        trigger: Filter by trigger type ("batch", "manual", "scheduled")
    """
    query = db.query(IngestionJob)

    if source:
        query = query.filter(IngestionJob.source == source)
    if status:
        query = query.filter(IngestionJob.status == status)
    if batch_run_id:
        query = query.filter(IngestionJob.batch_run_id == batch_run_id)
    if trigger:
        query = query.filter(IngestionJob.trigger == trigger)

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

    if job.status not in (JobStatus.RUNNING, JobStatus.PENDING, JobStatus.BLOCKED):
        raise HTTPException(
            status_code=400,
            detail=f"Job is not running, pending, or blocked (current: {job.status.value})",
        )

    job.status = JobStatus.FAILED
    job.error_message = "Cancelled by user"
    job.completed_at = datetime.utcnow()

    # Also mark the job_queue row so the worker sees the cancellation
    from app.core.models_queue import JobQueue, QueueJobStatus
    queue_row = db.query(JobQueue).filter(JobQueue.job_table_id == job_id).first()
    if queue_row and queue_row.status in (QueueJobStatus.PENDING, QueueJobStatus.CLAIMED, QueueJobStatus.RUNNING, QueueJobStatus.BLOCKED):
        queue_row.status = QueueJobStatus.FAILED
        queue_row.error_message = "Cancelled by user"
        queue_row.completed_at = datetime.utcnow()

    db.commit()
    db.refresh(job)

    # Trigger downstream: unblock dependents, batch completion check
    try:
        await _handle_job_completion(db, job)
    except Exception as e:
        logger.error(f"Completion handler error after cancel for job {job_id}: {e}")

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
# Batch Health & Unstick Endpoints
# =============================================================================


@router.post("/batch/{batch_run_id}/unstick")
def unstick_batch(batch_run_id: str, db: Session = Depends(get_db)):
    """
    Promote stuck BLOCKED jobs in a batch.

    Scans all tiers: if a tier's lower-tier dependencies are all terminal
    (SUCCESS or FAILED), promotes its BLOCKED jobs to PENDING.

    Also handles orphaned PENDING jobs that have been waiting >2 hours by
    re-submitting them to the job queue.

    Returns count of promoted jobs.
    """
    from app.core.job_queue_service import promote_blocked_jobs

    promoted = promote_blocked_jobs(db, batch_run_id)

    # Also check for orphaned pending ingestion_jobs with no queue entry
    from sqlalchemy import text

    orphaned = db.execute(
        text("""
            SELECT ij.id, ij.source
            FROM ingestion_jobs ij
            WHERE ij.batch_run_id = :batch_id
              AND ij.status = 'pending'
              AND ij.created_at < NOW() - INTERVAL '2 hours'
              AND NOT EXISTS (
                  SELECT 1 FROM job_queue jq
                  WHERE jq.job_table_id = ij.id
                    AND jq.status IN ('pending', 'claimed', 'running')
              )
        """),
        {"batch_id": batch_run_id},
    ).fetchall()

    resubmitted = 0
    if orphaned:
        from app.core.job_queue_service import submit_job, WORKER_MODE

        if WORKER_MODE:
            for row in orphaned:
                job_id, source = row[0], row[1]
                ing_job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
                if ing_job:
                    submit_job(
                        db=db,
                        job_type="ingestion",
                        payload={
                            "source": source,
                            "config": ing_job.config or {},
                            "ingestion_job_id": job_id,
                            "batch_id": batch_run_id,
                            "trigger": "batch",
                        },
                        priority=5,
                    )
                    resubmitted += 1

    return {
        "batch_run_id": batch_run_id,
        "promoted": promoted,
        "resubmitted": resubmitted,
        "message": f"Unstick complete: {promoted} promoted, {resubmitted} resubmitted",
    }


@router.get("/batch/{batch_run_id}/health")
def get_batch_health(batch_run_id: str, db: Session = Depends(get_db)):
    """
    Get detailed batch health status with per-tier breakdown and stuck job detection.

    Returns:
    - Overall batch status (running/stuck/complete/failed)
    - Per-tier job counts (pending/running/blocked/success/failed)
    - Stuck job detection (pending >1h, blocked with all lower tiers terminal)
    - Time elapsed and estimated completion
    """
    from sqlalchemy import text

    # Get all jobs in this batch
    jobs = (
        db.query(IngestionJob)
        .filter(IngestionJob.batch_run_id == batch_run_id)
        .all()
    )
    if not jobs:
        raise HTTPException(status_code=404, detail="Batch run not found")

    now = datetime.utcnow()
    batch_started = min(j.created_at for j in jobs)
    elapsed_minutes = (now - batch_started).total_seconds() / 60

    # Per-tier breakdown
    tier_stats = {}
    stuck_jobs = []
    total_by_status = {"pending": 0, "running": 0, "blocked": 0, "success": 0, "failed": 0}

    for j in jobs:
        tier = j.tier or 0
        if tier not in tier_stats:
            tier_stats[tier] = {"pending": 0, "running": 0, "blocked": 0, "success": 0, "failed": 0}
        status_key = j.status.value if hasattr(j.status, 'value') else str(j.status)
        if status_key in tier_stats[tier]:
            tier_stats[tier][status_key] += 1
        if status_key in total_by_status:
            total_by_status[status_key] += 1

        # Detect stuck jobs
        age_minutes = (now - j.created_at).total_seconds() / 60
        if j.status == JobStatus.PENDING and age_minutes > 60:
            stuck_jobs.append({
                "id": j.id, "source": j.source, "tier": tier,
                "status": "pending", "age_minutes": round(age_minutes),
                "reason": "Pending for over 1 hour",
            })
        elif j.status == JobStatus.RUNNING and j.started_at:
            run_minutes = (now - j.started_at).total_seconds() / 60
            if run_minutes > 120:
                stuck_jobs.append({
                    "id": j.id, "source": j.source, "tier": tier,
                    "status": "running", "age_minutes": round(run_minutes),
                    "reason": "Running for over 2 hours",
                })

    # Determine overall status
    total = len(jobs)
    terminal = total_by_status["success"] + total_by_status["failed"]
    if terminal == total:
        overall = "complete" if total_by_status["failed"] == 0 else "complete_with_failures"
    elif len(stuck_jobs) > 0:
        overall = "stuck"
    elif total_by_status["running"] > 0:
        overall = "running"
    else:
        overall = "waiting"

    # Check for blocked jobs that could be promoted
    promotable = 0
    for tier_level in sorted(tier_stats.keys()):
        lower_all_terminal = all(
            tier_stats.get(lt, {}).get("pending", 0) == 0
            and tier_stats.get(lt, {}).get("running", 0) == 0
            and tier_stats.get(lt, {}).get("blocked", 0) == 0
            for lt in tier_stats if lt < tier_level
        )
        if lower_all_terminal:
            promotable += tier_stats[tier_level].get("blocked", 0)

    return {
        "batch_run_id": batch_run_id,
        "overall_status": overall,
        "total_jobs": total,
        "elapsed_minutes": round(elapsed_minutes, 1),
        "by_status": total_by_status,
        "by_tier": {str(k): v for k, v in sorted(tier_stats.items())},
        "stuck_jobs": stuck_jobs,
        "promotable_blocked": promotable,
        "completion_pct": round(terminal / total * 100, 1) if total else 0,
    }


# =============================================================================
# Batch Tier Configuration Endpoints (P2 #6)
# =============================================================================


@router.get("/batch/tier-config")
def get_tier_config(db: Session = Depends(get_db)):
    """
    Get effective tier configuration (hardcoded defaults + DB overrides).

    Returns each tier with its effective settings and source list.
    """
    from app.core.nightly_batch_service import resolve_effective_tiers, TIERS
    from app.core.models import BatchTierConfig, BatchSourceTierOverride

    effective = resolve_effective_tiers(db)
    overrides = {tc.tier_level: tc for tc in db.query(BatchTierConfig).all()}
    source_overrides = {so.source_key: so for so in db.query(BatchSourceTierOverride).all()}

    result = []
    for tier in effective:
        tier_override = overrides.get(tier.level)
        result.append({
            "level": tier.level,
            "name": tier.name,
            "priority": tier.priority,
            "max_concurrent": tier.max_concurrent,
            "sources": [
                {
                    "key": s.key,
                    "default_config": s.default_config,
                    "has_override": s.key in source_overrides,
                }
                for s in tier.sources
            ],
            "has_override": tier_override is not None,
        })

    return {"tiers": result, "source_overrides_count": len(source_overrides)}


@router.put("/batch/tier-config/{tier_level}")
def update_tier_config(
    tier_level: int,
    priority: int = None,
    max_concurrent: int = None,
    enabled: bool = True,
    db: Session = Depends(get_db),
):
    """
    Update tier-level settings (priority, max_concurrent, enabled).

    Creates the override if it doesn't exist. Set enabled=false to disable an entire tier.
    """
    from app.core.models import BatchTierConfig
    from app.core.nightly_batch_service import TIER_BY_LEVEL

    if tier_level not in TIER_BY_LEVEL:
        raise HTTPException(status_code=404, detail=f"Tier {tier_level} not found")

    existing = db.query(BatchTierConfig).filter_by(tier_level=tier_level).first()
    if existing:
        if priority is not None:
            existing.priority = priority
        if max_concurrent is not None:
            existing.max_concurrent = max_concurrent
        existing.enabled = enabled
        existing.updated_at = datetime.utcnow()
    else:
        existing = BatchTierConfig(
            tier_level=tier_level,
            priority=priority,
            max_concurrent=max_concurrent,
            enabled=enabled,
        )
        db.add(existing)

    db.commit()
    db.refresh(existing)

    return {
        "tier_level": existing.tier_level,
        "priority": existing.priority,
        "max_concurrent": existing.max_concurrent,
        "enabled": existing.enabled,
        "updated_at": existing.updated_at.isoformat() if existing.updated_at else None,
    }


@router.put("/batch/source-override/{source_key}")
def update_source_override(
    source_key: str,
    tier_level: int = None,
    enabled: bool = True,
    default_config: Dict = None,
    db: Session = Depends(get_db),
):
    """
    Move source to different tier, disable, or override config.

    - Set tier_level to move source to a different tier.
    - Set enabled=false to remove source from batch entirely.
    - Set default_config to override/merge the source's default config.
    """
    from app.core.models import BatchSourceTierOverride

    existing = db.query(BatchSourceTierOverride).filter_by(source_key=source_key).first()
    if existing:
        if tier_level is not None:
            existing.tier_level = tier_level
        existing.enabled = enabled
        if default_config is not None:
            existing.default_config = default_config
        existing.updated_at = datetime.utcnow()
    else:
        existing = BatchSourceTierOverride(
            source_key=source_key,
            tier_level=tier_level,
            enabled=enabled,
            default_config=default_config,
        )
        db.add(existing)

    db.commit()
    db.refresh(existing)

    return {
        "source_key": existing.source_key,
        "tier_level": existing.tier_level,
        "enabled": existing.enabled,
        "default_config": existing.default_config,
        "updated_at": existing.updated_at.isoformat() if existing.updated_at else None,
    }


