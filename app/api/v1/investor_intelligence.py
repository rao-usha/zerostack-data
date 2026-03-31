"""
Investor Intelligence API — generalizable report data layer.

Provides structured data payloads for any sector or company deep dive,
mapping investor questions to live database data. Replaces hardcoded
estimates in IC reports with real FRED, BLS, AFDC, and EDGAR data.

Endpoints:
  GET /investor/sectors                          — list all 9 sectors
  GET /investor/sector/{slug}                    — live macro data for a sector
  GET /investor/company/{ticker}/context         — company job trends + financials
  GET /investor/report-context/{sector}/{type}   — structured report payload
  POST /investor/run-comps/{ticker}              — peer financial benchmarks
"""

import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.sources.investor_intel.sector_registry import (
    SECTOR_REGISTRY,
    QUESTION_TYPES,
    get_sector,
    list_sectors,
    get_question_type,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/investor", tags=["Investor Intelligence"])


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_query(db: Session, sql: str, params: dict) -> List[Any]:
    """Execute a parameterized query; return [] if table doesn't exist yet."""
    try:
        result = db.execute(text(sql), params)
        return result.fetchall()
    except Exception as exc:
        db.rollback()  # prevent InFailedSqlTransaction on subsequent queries
        msg = str(exc).lower()
        if "does not exist" in msg or "no such table" in msg or "undefined table" in msg:
            return []
        logger.warning("Query failed: %s — %s", sql[:80], exc)
        return []


def _rows_to_dicts(rows: List[Any]) -> List[Dict]:
    return [dict(r._mapping) for r in rows]


def _fetch_fred_latest(db: Session, category: str) -> List[Dict]:
    """Fetch latest value per series from a FRED table."""
    table = f"fred_{category}"
    sql = """
        SELECT DISTINCT ON (series_id)
               series_id, date, value
        FROM   {table}
        ORDER  BY series_id, date DESC
    """.format(table=table)  # table names can't be parameterized; safe — category is from registry
    rows = _safe_query(db, sql, {})
    return [
        {"series_id": r.series_id, "date": str(r.date), "value": float(r.value) if r.value is not None else None}
        for r in rows
    ]


def _fetch_bls_latest(db: Session, dataset: str, n_periods: int = 8) -> List[Dict]:
    """Fetch the most recent N periods from a BLS table."""
    from app.sources.bls.metadata import DATASET_TABLES
    table = DATASET_TABLES.get(dataset)
    if not table:
        return []
    sql = f"""
        SELECT series_id, series_title, year, period, value
        FROM   {table}
        ORDER  BY year DESC, period DESC
        LIMIT  :n
    """
    rows = _safe_query(db, sql, {"n": n_periods * 10})  # fetch more; client groups by series
    return _rows_to_dicts(rows)


def _fetch_afdc_latest(db: Session) -> List[Dict]:
    """Fetch latest EV station count per state."""
    sql = """
        SELECT DISTINCT ON (state)
               state, total_stations, ev_level2, ev_dc_fast, as_of_date
        FROM   afdc_ev_stations
        ORDER  BY state, as_of_date DESC
    """
    rows = _safe_query(db, sql, {})
    return [
        {
            "state": r.state,
            "total_stations": r.total_stations,
            "ev_level2": r.ev_level2,
            "ev_dc_fast": r.ev_dc_fast,
            "as_of_date": str(r.as_of_date),
        }
        for r in rows
    ]


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get("/sectors", summary="List all registered sectors")
async def get_sectors():
    """
    Return all 9 registered investment sectors with their data source mappings.

    Each sector entry includes: slug, label, description, FRED categories,
    BLS datasets, AFDC datasets, SIC codes, NAICS codes, and disruption vectors.
    """
    return {
        "count": len(SECTOR_REGISTRY),
        "sectors": list_sectors(),
    }


@router.get("/sector/{sector_slug}", summary="Live macro & sector data for a sector")
async def get_sector_data(
    sector_slug: str,
    db: Session = Depends(get_db),
):
    """
    Return live database data for a sector, aggregated from all relevant sources.

    Queries FRED, BLS, and AFDC tables defined in the sector registry.
    Tables that haven't been ingested yet return empty arrays gracefully.

    **Response structure:**
    - `sector`: sector metadata
    - `macro_indicators`: latest FRED values per series
    - `labor_trends`: latest BLS employment/wage data
    - `ev_infrastructure`: EV station counts by state (auto/energy sectors only)
    - `data_freshness`: which tables had data vs. were empty
    """
    try:
        sector = get_sector(sector_slug)
    except KeyError:
        raise HTTPException(
            status_code=404,
            detail=f"Unknown sector: '{sector_slug}'. Available: {sorted(SECTOR_REGISTRY)}",
        )

    macro_indicators: List[Dict] = []
    labor_trends: List[Dict] = []
    ev_infrastructure: List[Dict] = []
    data_freshness: Dict[str, bool] = {}

    # FRED data
    for category in sector.get("fred_categories", []):
        rows = _fetch_fred_latest(db, category)
        data_freshness[f"fred_{category}"] = len(rows) > 0
        for row in rows:
            row["category"] = category
            macro_indicators.append(row)

    # BLS data
    for dataset in sector.get("bls_datasets", []):
        rows = _fetch_bls_latest(db, dataset)
        data_freshness[f"bls_{dataset}"] = len(rows) > 0
        labor_trends.extend(rows)

    # AFDC data (EV infrastructure)
    if "ev_stations" in sector.get("afdc_datasets", []):
        ev_infrastructure = _fetch_afdc_latest(db)
        data_freshness["afdc_ev_stations"] = len(ev_infrastructure) > 0

    return {
        "sector": {
            "slug": sector_slug,
            "label": sector["label"],
            "description": sector["description"],
            "disruption_vectors": sector.get("disruption_vectors", []),
        },
        "macro_indicators": macro_indicators,
        "labor_trends": labor_trends,
        "ev_infrastructure": ev_infrastructure,
        "data_freshness": data_freshness,
        "as_of": "live",
    }


@router.get("/company/{ticker}/context", summary="Company-level job trends and financials")
async def get_company_context(
    ticker: str,
    db: Session = Depends(get_db),
):
    """
    Return company-specific data for a ticker: job posting trends and EDGAR financials.

    Job trend data requires the company to exist in `industrial_companies` and
    have postings collected via the job postings pipeline.

    Financials come from `public_company_financials` (populated via SEC EDGAR Company Facts).
    Returns 404 if neither job data nor financial data is found.
    """
    ticker_upper = ticker.upper()

    # EDGAR financials
    fin_rows = _safe_query(
        db,
        """
        SELECT ticker, period_of_report, revenue, gross_profit, net_income, eps_diluted
        FROM   public_company_financials
        WHERE  ticker = :ticker
        ORDER  BY period_of_report DESC
        LIMIT  8
        """,
        {"ticker": ticker_upper},
    )
    financials = _rows_to_dicts(fin_rows)

    # Job posting trend — monthly count of open postings
    job_rows = _safe_query(
        db,
        """
        SELECT DATE_TRUNC('month', first_seen) AS month,
               COUNT(*)                         AS postings
        FROM   job_postings jp
        JOIN   industrial_companies ic ON ic.id = jp.company_id
        WHERE  UPPER(ic.ticker_symbol) = :ticker
          AND  jp.first_seen >= NOW() - INTERVAL '18 months'
        GROUP  BY 1
        ORDER  BY 1 DESC
        LIMIT  18
        """,
        {"ticker": ticker_upper},
    )
    job_trend = [
        {"month": str(r.month)[:7], "postings": int(r.postings)}
        for r in job_rows
    ]

    if not financials and not job_trend:
        raise HTTPException(
            status_code=404,
            detail=f"No financial or job data found for ticker: {ticker_upper}",
        )

    # Derive sector from SIC if available
    sector_context: Dict[str, Any] = {}
    if financials and financials[0].get("ticker"):
        # Best effort: return which SECTOR_REGISTRY entries include this SIC
        sic_rows = _safe_query(
            db,
            "SELECT sic_code FROM industrial_companies WHERE UPPER(ticker_symbol) = :t LIMIT 1",
            {"t": ticker_upper},
        )
        if sic_rows:
            sic = str(sic_rows[0].sic_code or "")
            matched = [
                slug for slug, s in SECTOR_REGISTRY.items()
                if any(sic.startswith(code[:3]) for code in s.get("edgar_sic_codes", []))
            ]
            sector_context = {"matched_sectors": matched, "sic_code": sic}

    return {
        "ticker": ticker_upper,
        "financials": financials,
        "job_trend": job_trend,
        "sector_context": sector_context,
    }


@router.get(
    "/report-context/{sector_slug}/{question_type}",
    summary="Structured data payload for a report section",
)
async def get_report_context(
    sector_slug: str,
    question_type: str,
    db: Session = Depends(get_db),
):
    """
    Return a structured data payload for a specific investor question type.

    The payload maps directly to report components: kpi_cards, chart_data,
    and narrative_context. Reports can consume this endpoint instead of
    hardcoding estimates.

    **Question types:** disruption_analysis, market_sizing, operations_benchmarking, exit_readiness

    **KPI cards** include: id, label, value (live DB value or null), source, unit.
    **Chart data** includes arrays ready for Chart.js or D3 consumption.
    **Narrative context** includes key facts for LLM-assisted narrative generation.
    """
    try:
        sector = get_sector(sector_slug)
    except KeyError:
        raise HTTPException(
            status_code=404,
            detail=f"Unknown sector: '{sector_slug}'. Available: {sorted(SECTOR_REGISTRY)}",
        )

    try:
        qt = get_question_type(question_type)
    except KeyError:
        raise HTTPException(
            status_code=400,
            detail={
                "error": f"Unknown question type: '{question_type}'",
                "valid_types": list(QUESTION_TYPES),
            },
        )

    # Gather live data across sources
    fred_data: Dict[str, Any] = {}
    for category in sector.get("fred_categories", []):
        for row in _fetch_fred_latest(db, category):
            fred_data[row["series_id"]] = row

    bls_data: Dict[str, List] = {}
    for dataset in sector.get("bls_datasets", []):
        bls_data[dataset] = _fetch_bls_latest(db, dataset, n_periods=4)

    ev_data = _fetch_afdc_latest(db) if "ev_stations" in sector.get("afdc_datasets", []) else []

    # Build KPI cards
    kpi_cards = []
    for slot in qt["kpi_slots"]:
        value = None
        unit = None
        source_label = slot["source"]

        src = slot["source"]
        if src == "fred_consumer_sentiment" and "UMCSENT" in fred_data:
            value = fred_data["UMCSENT"]["value"]
            unit = "index"
        elif src == "fred_interest_rates" and "DGS10" in fred_data:
            value = fred_data["DGS10"]["value"]
            unit = "%"
        elif src == "fred_economic_indicators" and "GDP" in fred_data:
            value = fred_data["GDP"]["value"]
            unit = "$B"
        elif src == "afdc_ev_stations" and ev_data:
            total = sum(r["total_stations"] or 0 for r in ev_data)
            value = total
            unit = "stations"
        elif src in ("bls", "bls_auto_sector") and bls_data:
            first_dataset = next(iter(bls_data.values()), [])
            if first_dataset:
                value = first_dataset[0].get("value")
                unit = "thousands"
        elif src == "fred_auto_sector" and "TOTALSA" in fred_data:
            value = fred_data["TOTALSA"]["value"]
            unit = "M units SAAR"

        kpi_cards.append({
            "id": slot["id"],
            "label": slot["label"],
            "value": value,
            "unit": unit,
            "source": source_label,
            "live": value is not None,
        })

    # Build chart data arrays
    chart_data: Dict[str, Any] = {}
    for chart_type in qt["chart_types"]:
        if chart_type == "adoption_curve" and ev_data:
            chart_data["ev_station_by_state"] = sorted(
                [{"state": r["state"], "stations": r["total_stations"]} for r in ev_data],
                key=lambda x: x["stations"] or 0,
                reverse=True,
            )[:15]
        elif chart_type in ("comp_table", "margin_waterfall", "comp_multiples_bar"):
            chart_data[chart_type] = []  # populated by run-comps endpoint
        else:
            chart_data[chart_type] = []

    # Narrative context: key facts for LLM or human analyst
    narrative_context = {
        "sector_label": sector["label"],
        "question_type": qt["label"],
        "disruption_vectors": sector.get("disruption_vectors", []),
        "key_fred_values": {k: v for k, v in fred_data.items() if k in sector.get("key_fred_series", [])},
        "ev_station_total": sum(r["total_stations"] or 0 for r in ev_data) if ev_data else None,
        "data_sources_available": list(fred_data.keys()),
    }

    return {
        "sector": sector_slug,
        "question_type": question_type,
        "kpi_cards": kpi_cards,
        "chart_data": chart_data,
        "narrative_context": narrative_context,
    }


@router.post("/run-comps/{ticker}", summary="Pull peer financial benchmarks from EDGAR")
async def run_comps(
    ticker: str,
    db: Session = Depends(get_db),
):
    """
    Return financial benchmarks for EDGAR peers of the given ticker.

    Looks up the company's SIC code, finds other public companies in the same
    SIC, and returns their latest revenue, gross margin, and EBITDA margin
    from `public_company_financials`.

    Useful for anchoring private company valuations to public comps.
    """
    ticker_upper = ticker.upper()

    # Get SIC for this ticker
    sic_rows = _safe_query(
        db,
        "SELECT sic_code FROM industrial_companies WHERE UPPER(ticker_symbol) = :t LIMIT 1",
        {"t": ticker_upper},
    )
    sic_code = str(sic_rows[0].sic_code) if sic_rows and sic_rows[0].sic_code else None

    # Get own financials
    own_rows = _safe_query(
        db,
        """
        SELECT ticker, period_of_report, revenue, gross_profit, net_income
        FROM   public_company_financials
        WHERE  ticker = :ticker
        ORDER  BY period_of_report DESC
        LIMIT  4
        """,
        {"ticker": ticker_upper},
    )

    # Get peer financials by SIC (same 3-digit SIC prefix)
    peer_rows = []
    if sic_code:
        sic_prefix = sic_code[:3] + "%"
        peer_rows = _safe_query(
            db,
            """
            SELECT pcf.ticker,
                   pcf.period_of_report,
                   pcf.revenue,
                   pcf.gross_profit,
                   pcf.net_income
            FROM   public_company_financials pcf
            JOIN   industrial_companies ic
                   ON UPPER(ic.ticker_symbol) = pcf.ticker
            WHERE  ic.sic_code::text LIKE :sic_prefix
              AND  pcf.ticker != :ticker
              AND  pcf.period_of_report >= NOW() - INTERVAL '2 years'
            ORDER  BY pcf.period_of_report DESC
            LIMIT  40
            """,
            {"sic_prefix": sic_prefix, "ticker": ticker_upper},
        )

    def _calc_margins(rows):
        out = []
        for r in rows:
            rev = r.revenue or 0
            gp = r.gross_profit or 0
            ni = r.net_income or 0
            out.append({
                "ticker": r.ticker,
                "period": str(r.period_of_report)[:7] if r.period_of_report else None,
                "revenue": rev,
                "gross_margin_pct": round(gp / rev * 100, 1) if rev else None,
                "net_margin_pct": round(ni / rev * 100, 1) if rev else None,
            })
        return out

    return {
        "ticker": ticker_upper,
        "sic_code": sic_code,
        "own_financials": _calc_margins(own_rows),
        "peer_comps": _calc_margins(peer_rows),
        "peer_count": len(set(r.ticker for r in peer_rows)),
    }
