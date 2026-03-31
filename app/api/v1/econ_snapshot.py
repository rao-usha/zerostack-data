"""
Economic Snapshot API — aggregation layer over FRED, BLS, BEA, and Census data.

Transforms raw time-series data into PE-actionable intelligence:
  GET /econ-snapshot/ingest-status  — which tables are populated; ingest hints if missing
  GET /econ-snapshot/macro          — FRED key series: rates, unemployment, CPI
  GET /econ-snapshot/labor          — BLS JOLTS + CES sector employment deltas
  GET /econ-snapshot/regional       — BEA state GDP/income + Census demographics
  GET /econ-snapshot/pe-signals     — Derived PE signals computed from macro + labor + regional
"""

import logging
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.core.database import get_db

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/econ-snapshot", tags=["Economic Snapshot"])


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Key FRED series we care about for macro snapshot
MACRO_SERIES = {
    "DFF": "Fred Funds Rate (Effective)",
    "DGS10": "10-Year Treasury Yield",
    "DGS2": "2-Year Treasury Yield",
    "UNRATE": "Unemployment Rate",
    "CPIAUCSL": "CPI All Urban Consumers",
}

# FRED tables that may contain these series
FRED_TABLE_CANDIDATES = [
    "fred_interest_rates",
    "fred_economic_indicators",
    "fred_housing_market",
    "fred_consumer_sentiment",
    "fred_monetary_aggregates",
    "fred_industrial_production",
]

# BLS CES sector series — hardcoded mapping per PLAN_046 spec
SECTOR_SERIES: Dict[str, str] = {
    "Total Nonfarm": "CES0000000001",
    "Leisure & Hospitality": "CES7000000001",
    "Healthcare": "CES6500000001",
    "Construction": "CES2000000001",
    "Manufacturing": "CES3000000001",
    "Retail": "CES4200000001",
    "Financial Activities": "CES5500000001",
    "Professional Services": "CES6000000001",
    "Education": "CES6561000001",
    "Government": "CES9000000001",
    "Transportation": "CES4300000001",
}

AVG_HOURLY_EARNINGS_SERIES = "CES0500000003"

# JOLTS series we care about
JOLTS_SERIES = {
    "openings": "JTS000000000000000JOL",
    "quits": "JTS000000000000000QUL",
    "hires": "JTS000000000000000HIL",
}

# Key tables to check for ingest-status
KEY_TABLES = [
    "fred_interest_rates",
    "fred_economic_indicators",
    "bls_jolts",
    "bls_ces_employment",
    "bea_regional",
    "acs5_2023_b01003",
]

# Ingest hints paired to each key table
INGEST_HINTS = {
    "fred_interest_rates": "POST /fred/ingest with {\"category\": \"interest_rates\"}",
    "fred_economic_indicators": "POST /fred/ingest with {\"category\": \"economic_indicators\"}",
    "bls_jolts": "POST /bls/ingest/jolts",
    "bls_ces_employment": "POST /bls/ingest/ces",
    "bea_regional": "POST /bea/regional/ingest with {\"table_name\": \"SAGDP2N\", \"geo_fips\": \"STATE\", \"year\": \"2019,2020,2021,2022,2023\"}",
    "acs5_2023_b01003": "POST /census/state with {\"survey\": \"acs5\", \"year\": 2023, \"table_id\": \"B01003\"}",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_query(db: Session, sql: str, params: dict) -> List[Any]:
    """Execute a parameterized query; return [] if the table doesn't exist yet."""
    try:
        result = db.execute(text(sql), params)
        return result.fetchall()
    except Exception as exc:
        db.rollback()
        msg = str(exc).lower()
        if "does not exist" in msg or "no such table" in msg or "undefined" in msg:
            return []
        logger.warning("Query failed: %s — %s", sql[:100], exc)
        return []


def _rows_to_dicts(rows: List[Any]) -> List[Dict]:
    return [dict(r._mapping) for r in rows]


def _table_exists(db: Session, table_name: str) -> bool:
    """Check if a table exists in information_schema."""
    rows = _safe_query(
        db,
        "SELECT COUNT(*) AS cnt FROM information_schema.tables WHERE table_name = :t",
        {"t": table_name},
    )
    if rows:
        return int(rows[0][0]) > 0
    return False


def _table_row_count(db: Session, table_name: str) -> int:
    """Get row count for an existing table. Returns 0 if table is missing."""
    if not _table_exists(db, table_name):
        return 0
    rows = _safe_query(db, f"SELECT COUNT(*) AS cnt FROM {table_name}", {})
    if rows:
        return int(rows[0][0])
    return 0


def _today_str() -> str:
    return date.today().isoformat()


def _bls_period_to_date(year: int, period: str) -> Optional[date]:
    """Convert BLS year + period (M01..M12) to a date (first of month)."""
    if not period or not period.startswith("M"):
        return None
    try:
        month = int(period[1:])
        return date(year, month, 1)
    except (ValueError, TypeError):
        return None


def _find_fred_series(db: Session, series_ids: List[str]) -> Dict[str, Dict]:
    """
    Search across FRED table candidates for a list of series_ids.
    Returns dict: series_id -> {table, latest_row, history_rows}
    """
    found: Dict[str, Dict] = {}

    for table in FRED_TABLE_CANDIDATES:
        if not _table_exists(db, table):
            continue

        # Which series_ids haven't been found yet?
        missing = [s for s in series_ids if s not in found]
        if not missing:
            break

        # Check which of the missing series exist in this table
        placeholders = ", ".join(f":s{i}" for i in range(len(missing)))
        params = {f"s{i}": sid for i, sid in enumerate(missing)}
        check_rows = _safe_query(
            db,
            f"SELECT DISTINCT series_id FROM {table} WHERE series_id IN ({placeholders})",
            params,
        )
        present = {r[0] for r in check_rows}

        for sid in present:
            # Latest value
            latest_rows = _safe_query(
                db,
                f"""
                SELECT series_id, date, value
                FROM {table}
                WHERE series_id = :sid
                ORDER BY date DESC
                LIMIT 1
                """,
                {"sid": sid},
            )
            # 12-month-ago value
            prev_rows = _safe_query(
                db,
                f"""
                SELECT series_id, date, value
                FROM {table}
                WHERE series_id = :sid
                  AND date <= (CURRENT_DATE - INTERVAL '12 months')
                ORDER BY date DESC
                LIMIT 1
                """,
                {"sid": sid},
            )
            # 36-point history
            history_rows = _safe_query(
                db,
                f"""
                SELECT date, value
                FROM {table}
                WHERE series_id = :sid
                ORDER BY date DESC
                LIMIT 36
                """,
                {"sid": sid},
            )
            found[sid] = {
                "table": table,
                "latest": latest_rows[0] if latest_rows else None,
                "prev_12m": prev_rows[0] if prev_rows else None,
                "history": history_rows,
            }

    return found


def _bls_latest_value(db: Session, table: str, series_id: str) -> Optional[Any]:
    """Get the most recent row from a BLS table for a given series_id."""
    rows = _safe_query(
        db,
        f"""
        SELECT series_id, year, period, value
        FROM {table}
        WHERE series_id = :sid
        ORDER BY year DESC, period DESC
        LIMIT 1
        """,
        {"sid": series_id},
    )
    return rows[0] if rows else None


def _bls_prev_12m_value(db: Session, table: str, series_id: str, current_year: int, current_period: str) -> Optional[Any]:
    """
    Get the BLS value from approximately 12 months before the current period.
    period is like 'M03'. We subtract 12 months in year/month space.
    """
    if not current_period or not current_period.startswith("M"):
        return None
    try:
        current_month = int(current_period[1:])
    except ValueError:
        return None

    prev_month = current_month
    prev_year = current_year - 1  # Go back 12 months = same month, prior year

    prev_period = f"M{prev_month:02d}"

    rows = _safe_query(
        db,
        f"""
        SELECT series_id, year, period, value
        FROM {table}
        WHERE series_id = :sid
          AND year = :yr
          AND period = :per
        LIMIT 1
        """,
        {"sid": series_id, "yr": prev_year, "per": prev_period},
    )
    if rows:
        return rows[0]
    # Fallback: closest period in prior year
    fallback = _safe_query(
        db,
        f"""
        SELECT series_id, year, period, value
        FROM {table}
        WHERE series_id = :sid
          AND year = :yr
        ORDER BY period DESC
        LIMIT 1
        """,
        {"sid": series_id, "yr": prev_year},
    )
    return fallback[0] if fallback else None


def _bls_history(db: Session, table: str, series_id: str, limit: int = 24) -> List[Dict]:
    """Get the last N months of BLS history for a series."""
    rows = _safe_query(
        db,
        f"""
        SELECT year, period, value
        FROM {table}
        WHERE series_id = :sid
          AND period LIKE 'M%'
        ORDER BY year DESC, period DESC
        LIMIT :lim
        """,
        {"sid": series_id, "lim": limit},
    )
    result = []
    for r in rows:
        d = _bls_period_to_date(r.year, r.period)
        result.append({
            "date": d.isoformat() if d else f"{r.year}-{r.period}",
            "value": float(r.value) if r.value is not None else None,
        })
    return result


def _safe_float(val) -> Optional[float]:
    if val is None:
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _safe_delta(a: Optional[float], b: Optional[float]) -> Optional[float]:
    """Return a - b, or None if either is missing."""
    if a is None or b is None:
        return None
    return round(a - b, 4)


def _safe_pct_change(current: Optional[float], prior: Optional[float]) -> Optional[float]:
    """Return ((current - prior) / prior) * 100, guarded against zero."""
    if current is None or prior is None or prior == 0:
        return None
    return round((current - prior) / abs(prior) * 100, 2)


# ---------------------------------------------------------------------------
# Endpoint 1: ingest-status
# ---------------------------------------------------------------------------

@router.get("/ingest-status", summary="Check which econ tables are populated")
def get_ingest_status(db: Session = Depends(get_db)):
    """
    Return existence and row counts for all key econ tables.

    Includes ingest hints for any table that is missing or empty.
    """
    tables_status: Dict[str, Dict] = {}
    ingest_hints: List[str] = []

    for table_name in KEY_TABLES:
        exists = _table_exists(db, table_name)
        if exists:
            count = _table_row_count(db, table_name)
        else:
            count = 0

        tables_status[table_name] = {"exists": exists, "row_count": count}

        if not exists or count == 0:
            hint = INGEST_HINTS.get(table_name)
            if hint:
                ingest_hints.append(hint)

    # Also check for any acs5_% tables (Census ACS5)
    acs5_rows = _safe_query(
        db,
        "SELECT table_name FROM information_schema.tables WHERE table_name LIKE 'acs5_%'",
        {},
    )
    acs5_tables = [r[0] for r in acs5_rows]

    return {
        "as_of": _today_str(),
        "tables": tables_status,
        "acs5_tables_found": acs5_tables,
        "ingest_hints": ingest_hints,
    }


# ---------------------------------------------------------------------------
# Endpoint 2: macro (FRED key series)
# ---------------------------------------------------------------------------

@router.get("/macro", summary="FRED macro snapshot: rates, unemployment, CPI")
def get_macro(db: Session = Depends(get_db)):
    """
    Return latest values + 12-month delta + 36-point history for key FRED series.

    Key series: DFF (Fed Funds), DGS10, DGS2 (Treasuries), UNRATE, CPIAUCSL.
    Derived signals: fed policy direction, yield curve inversion, unemployment trend.
    """
    target_series = list(MACRO_SERIES.keys())
    found = _find_fred_series(db, target_series)

    missing_tables: List[str] = []
    if not found:
        missing_tables = ["fred_interest_rates", "fred_economic_indicators"]
        return {
            "status": "not_ingested",
            "as_of": _today_str(),
            "data": None,
            "missing": missing_tables,
        }

    def _extract_kpi(sid: str) -> Dict:
        entry = found.get(sid)
        if not entry or entry["latest"] is None:
            return {"value": None, "date": None, "prev_12m": None, "delta": None}
        latest_val = _safe_float(entry["latest"].value)
        prev_val = _safe_float(entry["prev_12m"].value) if entry["prev_12m"] else None
        return {
            "value": latest_val,
            "date": str(entry["latest"].date),
            "prev_12m": prev_val,
            "delta": _safe_delta(latest_val, prev_val),
        }

    # --- Fed Funds Rate ---
    dff = _extract_kpi("DFF")
    delta_dff = dff.get("delta")
    if delta_dff is not None:
        if delta_dff < -0.1:
            dff["signal"] = "easing"
        elif delta_dff > 0.1:
            dff["signal"] = "tightening"
        else:
            dff["signal"] = "stable"

    # --- 10Y Treasury ---
    dgs10 = _extract_kpi("DGS10")

    # --- 2Y Treasury ---
    dgs2 = _extract_kpi("DGS2")

    # --- Yield Spread (10Y - 2Y) ---
    spread_value: Optional[float] = None
    inverted = False
    inverted_months = 0

    if dgs10.get("value") is not None and dgs2.get("value") is not None:
        spread_value = round(dgs10["value"] - dgs2["value"], 4)
        inverted = spread_value < 0

    # Count inversion months from DGS10 history vs DGS2 history
    if "DGS10" in found and "DGS2" in found:
        h10 = {str(r.date): _safe_float(r.value) for r in found["DGS10"]["history"]}
        h2 = {str(r.date): _safe_float(r.value) for r in found["DGS2"]["history"]}
        common_dates = sorted(set(h10.keys()) & set(h2.keys()), reverse=True)
        for d in common_dates:
            v10 = h10.get(d)
            v2 = h2.get(d)
            if v10 is not None and v2 is not None and (v10 - v2) < 0:
                inverted_months += 1

    yield_spread = {
        "value": spread_value,
        "inverted": inverted,
        "inverted_months": inverted_months,
    }

    # --- Unemployment Rate ---
    unrate = _extract_kpi("UNRATE")
    delta_unrate = unrate.get("delta")
    if delta_unrate is not None:
        if delta_unrate > 0.2:
            unrate["signal"] = "rising"
        elif delta_unrate < -0.2:
            unrate["signal"] = "falling"
        else:
            unrate["signal"] = "stable"

    # --- CPI YoY ---
    cpi_entry = found.get("CPIAUCSL")
    cpi_yoy: Optional[float] = None
    cpi_trend: Optional[str] = None

    if cpi_entry and cpi_entry["latest"] and cpi_entry["prev_12m"]:
        latest_cpi = _safe_float(cpi_entry["latest"].value)
        prev_cpi = _safe_float(cpi_entry["prev_12m"].value)
        cpi_yoy = _safe_pct_change(latest_cpi, prev_cpi)

    cpi_kpi = {
        "value": cpi_yoy,
        "trend": cpi_trend,
    }

    # --- History arrays (most recent first) ---
    history: Dict[str, List[Dict]] = {}
    for sid in ["DFF", "DGS10", "DGS2", "UNRATE"]:
        entry = found.get(sid)
        if entry:
            history[sid] = [
                {"date": str(r.date), "value": _safe_float(r.value)}
                for r in entry["history"]
            ]
        else:
            history[sid] = []

    any_missing = [sid for sid in target_series if sid not in found]
    status = "partial" if any_missing else "ok"

    return {
        "status": status,
        "as_of": _today_str(),
        "data": {
            "kpis": {
                "fed_funds_rate": dff,
                "ten_year_yield": dgs10,
                "two_year_yield": dgs2,
                "yield_spread_10_2": yield_spread,
                "unemployment_rate": unrate,
                "cpi_yoy_pct": cpi_kpi,
            },
            "history": history,
        },
        "missing": [f"Series {sid} not found in any FRED table" for sid in any_missing],
    }


# ---------------------------------------------------------------------------
# Endpoint 3: labor (BLS JOLTS + CES)
# ---------------------------------------------------------------------------

@router.get("/labor", summary="BLS labor market: JOLTS + CES sector employment")
def get_labor(db: Session = Depends(get_db)):
    """
    Return JOLTS job openings/quits/hires + CES sector employment 12-month deltas.

    BLS tables: bls_jolts, bls_ces_employment.
    Sector series: hardcoded CES series IDs per PLAN_046 spec.
    """
    jolts_table = "bls_jolts"
    ces_table = "bls_ces_employment"

    jolts_exists = _table_exists(db, jolts_table)
    ces_exists = _table_exists(db, ces_table)
    missing_tables: List[str] = []

    if not jolts_exists:
        missing_tables.append(f"{jolts_table} — POST /bls/ingest/jolts")
    if not ces_exists:
        missing_tables.append(f"{ces_table} — POST /bls/ingest/ces")

    # ---- JOLTS ----
    jolts_data: Dict[str, Any] = {}
    jolts_history: List[Dict] = []

    if jolts_exists:
        # Get latest values for openings, quits, hires
        jolts_latest: Dict[str, Any] = {}
        for key, sid in JOLTS_SERIES.items():
            row = _bls_latest_value(db, jolts_table, sid)
            if row:
                jolts_latest[key] = {
                    "series_id": sid,
                    "year": row.year,
                    "period": row.period,
                    "value": _safe_float(row.value),
                }
            else:
                jolts_latest[key] = None

        jolts_data = {
            "openings": jolts_latest.get("openings"),
            "quits": jolts_latest.get("quits"),
            "hires": jolts_latest.get("hires"),
        }

        # History: merge openings/quits/hires by date
        openings_hist = _bls_history(db, jolts_table, JOLTS_SERIES["openings"], 24)
        quits_hist = _bls_history(db, jolts_table, JOLTS_SERIES["quits"], 24)
        hires_hist = _bls_history(db, jolts_table, JOLTS_SERIES["hires"], 24)

        # Index by date
        dates_set = (
            {r["date"] for r in openings_hist}
            | {r["date"] for r in quits_hist}
            | {r["date"] for r in hires_hist}
        )
        o_map = {r["date"]: r["value"] for r in openings_hist}
        q_map = {r["date"]: r["value"] for r in quits_hist}
        h_map = {r["date"]: r["value"] for r in hires_hist}

        for d in sorted(dates_set, reverse=True)[:24]:
            jolts_history.append({
                "date": d,
                "openings": o_map.get(d),
                "quits": q_map.get(d),
                "hires": h_map.get(d),
            })

    # ---- CES Sector Employment ----
    sector_employment: List[Dict] = []
    total_nonfarm_delta: Optional[float] = None
    avg_hourly_earnings_yoy: Optional[float] = None

    if ces_exists:
        for sector_name, sid in SECTOR_SERIES.items():
            latest_row = _bls_latest_value(db, ces_table, sid)
            if not latest_row:
                continue

            current_val = _safe_float(latest_row.value)
            prev_row = _bls_prev_12m_value(
                db, ces_table, sid, latest_row.year, latest_row.period
            )
            prev_val = _safe_float(prev_row.value) if prev_row else None

            delta = _safe_delta(current_val, prev_val)
            delta_pct = _safe_pct_change(current_val, prev_val)

            entry = {
                "sector": sector_name,
                "series_id": sid,
                "current": current_val,
                "prev_12m": prev_val,
                "delta": delta,
                "delta_pct": delta_pct,
                "period": f"{latest_row.year}-{latest_row.period}",
            }
            sector_employment.append(entry)

            if sector_name == "Total Nonfarm" and delta is not None:
                # CES nonfarm is in thousands — multiply by 1000 for actual count
                total_nonfarm_delta = delta * 1000 if delta else None

        # Avg hourly earnings
        ahe_row = _bls_latest_value(db, ces_table, AVG_HOURLY_EARNINGS_SERIES)
        if ahe_row:
            ahe_current = _safe_float(ahe_row.value)
            ahe_prev = _bls_prev_12m_value(
                db, ces_table, AVG_HOURLY_EARNINGS_SERIES, ahe_row.year, ahe_row.period
            )
            ahe_prev_val = _safe_float(ahe_prev.value) if ahe_prev else None
            avg_hourly_earnings_yoy = _safe_pct_change(ahe_current, ahe_prev_val)

    # Determine overall status
    if not jolts_exists and not ces_exists:
        status = "not_ingested"
    elif missing_tables:
        status = "partial"
    else:
        status = "ok"

    return {
        "status": status,
        "as_of": _today_str(),
        "data": {
            "jolts": {
                "latest": jolts_data,
                "history": jolts_history,
            },
            "sector_employment_12m": sector_employment,
            "avg_hourly_earnings_yoy_pct": avg_hourly_earnings_yoy,
            "total_nonfarm_12m_delta": total_nonfarm_delta,
        },
        "missing": missing_tables,
    }


# ---------------------------------------------------------------------------
# Endpoint 4: regional (BEA state GDP + income + Census ACS)
# ---------------------------------------------------------------------------

@router.get("/regional", summary="BEA state GDP/income + Census demographics")
def get_regional(db: Session = Depends(get_db)):
    """
    Return state-level economic health: BEA GDP growth + personal income + Census demographics.

    BEA tables: bea_regional (SAGDP2N, SAINC1, SAINC51).
    Census tables: acs5_2023_b01003 (population), acs5_2023_b19013 (median HH income).
    """
    bea_exists = _table_exists(db, "bea_regional")
    pop_exists = _table_exists(db, "acs5_2023_b01003")
    income_exists = _table_exists(db, "acs5_2023_b19013")

    missing_tables: List[str] = []
    if not bea_exists:
        missing_tables.append(
            "bea_regional — POST /bea/regional/ingest with {\"table_name\": \"SAGDP2N\", \"geo_fips\": \"STATE\"}"
        )
    if not pop_exists:
        missing_tables.append(
            "acs5_2023_b01003 — POST /census/state with {\"survey\": \"acs5\", \"year\": 2023, \"table_id\": \"B01003\"}"
        )
    if not income_exists:
        missing_tables.append(
            "acs5_2023_b19013 — POST /census/state with {\"survey\": \"acs5\", \"year\": 2023, \"table_id\": \"B19013\"}"
        )

    if not bea_exists:
        return {
            "status": "not_ingested",
            "as_of": _today_str(),
            "data": None,
            "missing": missing_tables,
        }

    # ---- BEA GDP (SAGDP2N) ----
    gdp_rows = _safe_query(
        db,
        """
        SELECT geo_fips, geo_name, time_period, data_value
        FROM bea_regional
        WHERE table_name = 'SAGDP2N' AND line_code = '1'
        ORDER BY geo_fips, time_period DESC
        """,
        {},
    )

    # Build per-state dict: {fips: {name, years: {year: value}}}
    state_data: Dict[str, Dict] = {}
    for r in gdp_rows:
        fips = str(r.geo_fips or "").strip()
        if not fips or fips in ("0", "00000"):
            continue
        if fips not in state_data:
            state_data[fips] = {"name": r.geo_name, "gdp_by_year": {}}
        year_str = str(r.time_period or "").strip()[:4]
        if year_str.isdigit() and r.data_value is not None:
            state_data[fips]["gdp_by_year"][int(year_str)] = _safe_float(r.data_value)

    # ---- BEA Personal Income (SAINC1) ----
    inc_rows = _safe_query(
        db,
        """
        SELECT geo_fips, geo_name, time_period, data_value
        FROM bea_regional
        WHERE table_name = 'SAINC1' AND line_code = '1'
        ORDER BY geo_fips, time_period DESC
        """,
        {},
    )
    for r in inc_rows:
        fips = str(r.geo_fips or "").strip()
        if not fips or fips in ("0", "00000"):
            continue
        if fips not in state_data:
            state_data[fips] = {"name": r.geo_name, "gdp_by_year": {}}
        state_data[fips].setdefault("income_by_year", {})
        year_str = str(r.time_period or "").strip()[:4]
        if year_str.isdigit() and r.data_value is not None:
            state_data[fips]["income_by_year"][int(year_str)] = _safe_float(r.data_value)

    # ---- BEA Per Capita Income (SAINC51) ----
    pc_rows = _safe_query(
        db,
        """
        SELECT geo_fips, geo_name, time_period, data_value
        FROM bea_regional
        WHERE table_name = 'SAINC51' AND line_code = '1'
        ORDER BY geo_fips, time_period DESC
        """,
        {},
    )
    for r in pc_rows:
        fips = str(r.geo_fips or "").strip()
        if not fips or fips in ("0", "00000"):
            continue
        if fips not in state_data:
            state_data[fips] = {"name": r.geo_name, "gdp_by_year": {}}
        state_data[fips].setdefault("per_capita_by_year", {})
        year_str = str(r.time_period or "").strip()[:4]
        if year_str.isdigit() and r.data_value is not None:
            state_data[fips]["per_capita_by_year"][int(year_str)] = _safe_float(r.data_value)

    # ---- Census Population (acs5_2023_b01003) ----
    pop_map: Dict[str, int] = {}
    pop_name_map: Dict[str, str] = {}
    if pop_exists:
        pop_rows = _safe_query(
            db,
            "SELECT geo_id, geo_name, b01003_001e FROM acs5_2023_b01003",
            {},
        )
        for r in pop_rows:
            geo_id = str(r.geo_id or "").strip()
            # Normalize to 2-char state FIPS
            state_fips = geo_id[-2:] if len(geo_id) >= 2 else geo_id
            pop_map[state_fips] = int(r.b01003_001e) if r.b01003_001e else 0
            if r.geo_name:
                pop_name_map[state_fips] = r.geo_name

    # ---- Census Median HH Income (acs5_2023_b19013) ----
    hh_income_map: Dict[str, int] = {}
    if income_exists:
        hh_rows = _safe_query(
            db,
            "SELECT geo_id, geo_name, b19013_001e FROM acs5_2023_b19013",
            {},
        )
        for r in hh_rows:
            geo_id = str(r.geo_id or "").strip()
            state_fips = geo_id[-2:] if len(geo_id) >= 2 else geo_id
            hh_income_map[state_fips] = int(r.b19013_001e) if r.b19013_001e else 0

    # ---- Assemble per-state records ----
    states: List[Dict] = []
    for fips, data in state_data.items():
        # Normalize FIPS: BEA uses 5-char like "06000", take first 2 as state FIPS
        state_fips_2 = fips[:2] if len(fips) >= 2 else fips

        name = data.get("name", "")
        gdp_years = data.get("gdp_by_year", {})
        inc_years = data.get("income_by_year", {})
        pc_years = data.get("per_capita_by_year", {})

        # GDP growth: (2023 - 2022) / 2022 * 100
        gdp_2023 = gdp_years.get(2023)
        gdp_2022 = gdp_years.get(2022)
        gdp_growth_yoy = _safe_pct_change(gdp_2023, gdp_2022)

        # Personal income growth
        inc_2023 = inc_years.get(2023)
        inc_2022 = inc_years.get(2022)
        income_growth_yoy = _safe_pct_change(inc_2023, inc_2022)

        # Latest per capita income
        per_capita = pc_years.get(2023) or pc_years.get(2022)

        # Census join
        population = pop_map.get(state_fips_2)
        median_hh_income = hh_income_map.get(state_fips_2)

        # Signal logic: green if both > 3%, red if both < 1%, yellow otherwise
        if gdp_growth_yoy is not None and income_growth_yoy is not None:
            if gdp_growth_yoy > 3.0 and income_growth_yoy > 3.0:
                signal = "green"
            elif gdp_growth_yoy < 1.0 and income_growth_yoy < 1.0:
                signal = "red"
            else:
                signal = "yellow"
        elif gdp_growth_yoy is not None:
            signal = "green" if gdp_growth_yoy > 3.0 else ("red" if gdp_growth_yoy < 1.0 else "yellow")
        else:
            signal = "grey"

        states.append({
            "fips": state_fips_2,
            "name": name,
            "gdp_growth_yoy_pct": gdp_growth_yoy,
            "personal_income_growth_pct": income_growth_yoy,
            "per_capita_income": per_capita,
            "population": population,
            "median_hh_income": median_hh_income,
            "signal": signal,
        })

    # Sort by GDP growth descending for top/bottom
    ranked = sorted(
        [s for s in states if s["gdp_growth_yoy_pct"] is not None],
        key=lambda x: x["gdp_growth_yoy_pct"],
        reverse=True,
    )
    top_5 = ranked[:5]
    bottom_5 = ranked[-5:] if len(ranked) >= 5 else ranked

    status = "partial" if missing_tables else "ok"

    return {
        "status": status,
        "as_of": _today_str(),
        "data": {
            "states": states,
            "top_5_growth": top_5,
            "bottom_5_growth": list(reversed(bottom_5)),
        },
        "missing": missing_tables,
    }


# ---------------------------------------------------------------------------
# Endpoint 5: pe-signals (derived from macro + labor + regional)
# ---------------------------------------------------------------------------

@router.get("/pe-signals", summary="Derived PE deal environment + sector + geographic signals")
def get_pe_signals(db: Session = Depends(get_db)):
    """
    Compute derived PE signals from live macro, labor, and regional data.

    - Deal Environment Score (0–100): penalized by rate level, yield inversion, unemployment, CPI
    - Sector Momentum Scores: based on CES 12-month employment delta_pct
    - Geographic Opportunity Scores: based on BEA GDP + income growth composite
    """
    # --- Pull macro data directly from DB ---
    macro_series = list(MACRO_SERIES.keys())
    found_fred = _find_fred_series(db, macro_series)

    def _get_latest_val(sid: str) -> Optional[float]:
        entry = found_fred.get(sid)
        if not entry or entry["latest"] is None:
            return None
        return _safe_float(entry["latest"].value)

    def _get_prev_val(sid: str) -> Optional[float]:
        entry = found_fred.get(sid)
        if not entry or entry["prev_12m"] is None:
            return None
        return _safe_float(entry["prev_12m"].value)

    fed_funds = _get_latest_val("DFF")
    dgs10_val = _get_latest_val("DGS10")
    dgs2_val = _get_latest_val("DGS2")
    unrate = _get_latest_val("UNRATE")
    unrate_prev = _get_prev_val("UNRATE")
    cpi_current = _get_latest_val("CPIAUCSL")
    cpi_prev = _get_prev_val("CPIAUCSL")

    yield_spread = None
    if dgs10_val is not None and dgs2_val is not None:
        yield_spread = round(dgs10_val - dgs2_val, 4)

    unemployment_delta = _safe_delta(unrate, unrate_prev)
    cpi_yoy = _safe_pct_change(cpi_current, cpi_prev)

    # --- Deal Environment Score ---
    score = 100
    drivers: List[Dict] = []

    # Rate penalty
    if fed_funds is not None:
        if fed_funds > 5.0:
            score -= 20
            drivers.append({
                "factor": "Rate environment",
                "reading": f"FFR {fed_funds:.2f}% — elevated",
                "impact": "negative",
            })
        elif fed_funds > 4.0:
            score -= 10
            drivers.append({
                "factor": "Rate environment",
                "reading": f"FFR {fed_funds:.2f}% — moderately elevated",
                "impact": "negative",
            })
        else:
            drivers.append({
                "factor": "Rate environment",
                "reading": f"FFR {fed_funds:.2f}% — accommodative",
                "impact": "positive",
            })
    else:
        drivers.append({
            "factor": "Rate environment",
            "reading": "FFR data unavailable",
            "impact": "neutral",
        })

    # Yield curve penalty
    if yield_spread is not None:
        if yield_spread < 0:
            score -= 15
            drivers.append({
                "factor": "Yield curve",
                "reading": f"Inverted ({yield_spread:.2f}%)",
                "impact": "negative",
            })
        else:
            drivers.append({
                "factor": "Yield curve",
                "reading": f"Normal ({yield_spread:.2f}%)",
                "impact": "positive",
            })
    else:
        drivers.append({
            "factor": "Yield curve",
            "reading": "DGS10/DGS2 data unavailable",
            "impact": "neutral",
        })

    # Unemployment rising penalty
    if unemployment_delta is not None:
        if unemployment_delta > 0.3:
            score -= 10
            drivers.append({
                "factor": "Labor market",
                "reading": f"Unemployment rising (+{unemployment_delta:.1f}pp YoY)",
                "impact": "negative",
            })
        elif unemployment_delta < -0.2:
            drivers.append({
                "factor": "Labor market",
                "reading": f"Unemployment falling ({unemployment_delta:.1f}pp YoY)",
                "impact": "positive",
            })
        else:
            drivers.append({
                "factor": "Labor market",
                "reading": f"Unemployment stable ({unemployment_delta:+.1f}pp YoY)",
                "impact": "neutral",
            })
    else:
        drivers.append({
            "factor": "Labor market",
            "reading": "UNRATE data unavailable",
            "impact": "neutral",
        })

    # CPI penalty / bonus
    if cpi_yoy is not None:
        if cpi_yoy > 4.0:
            score -= 10
            drivers.append({
                "factor": "Inflation (CPI YoY)",
                "reading": f"CPI +{cpi_yoy:.1f}% — elevated",
                "impact": "negative",
            })
        elif cpi_yoy < 2.5:
            score += 5
            drivers.append({
                "factor": "Inflation (CPI YoY)",
                "reading": f"CPI +{cpi_yoy:.1f}% — benign",
                "impact": "positive",
            })
        else:
            drivers.append({
                "factor": "Inflation (CPI YoY)",
                "reading": f"CPI +{cpi_yoy:.1f}% — within target range",
                "impact": "neutral",
            })
    else:
        drivers.append({
            "factor": "Inflation (CPI YoY)",
            "reading": "CPI data unavailable",
            "impact": "neutral",
        })

    # Clamp 0–100
    score = max(0, min(100, score))

    # Signal
    if score >= 70:
        deal_signal = "green"
    elif score >= 45:
        deal_signal = "yellow"
    else:
        deal_signal = "red"

    deal_environment = {
        "score": score,
        "signal": deal_signal,
        "drivers": drivers,
    }

    # --- Sector Momentum Scores ---
    ces_table = "bls_ces_employment"
    sector_momentum: List[Dict] = []

    if _table_exists(db, ces_table):
        for sector_name, sid in SECTOR_SERIES.items():
            if sector_name == "Total Nonfarm":
                continue  # Skip aggregate — not a sector signal
            latest_row = _bls_latest_value(db, ces_table, sid)
            if not latest_row:
                continue

            current_val = _safe_float(latest_row.value)
            prev_row = _bls_prev_12m_value(
                db, ces_table, sid, latest_row.year, latest_row.period
            )
            prev_val = _safe_float(prev_row.value) if prev_row else None
            delta_pct = _safe_pct_change(current_val, prev_val)

            if delta_pct is not None:
                if delta_pct > 3.0:
                    momentum_score = 80 + min(20, int((delta_pct - 3.0) * 4))
                elif delta_pct >= 1.0:
                    momentum_score = 60 + int((delta_pct - 1.0) / 2.0 * 20)
                elif delta_pct >= 0.0:
                    momentum_score = 50 + int(delta_pct * 10)
                else:
                    momentum_score = max(0, 50 + int(delta_pct * 10))
            else:
                momentum_score = 50  # neutral default when no data

            if momentum_score >= 70:
                sector_signal = "green"
            elif momentum_score >= 50:
                sector_signal = "yellow"
            else:
                sector_signal = "red"

            sector_momentum.append({
                "sector": sector_name,
                "series_id": sid,
                "momentum_score": momentum_score,
                "signal": sector_signal,
                "delta_pct": delta_pct,
            })

        sector_momentum.sort(key=lambda x: x["momentum_score"], reverse=True)

    # --- Geographic Opportunity Scores ---
    geographic_opportunity: List[Dict] = []

    if _table_exists(db, "bea_regional"):
        gdp_rows = _safe_query(
            db,
            """
            SELECT geo_fips, geo_name, time_period, data_value
            FROM bea_regional
            WHERE table_name = 'SAGDP2N' AND line_code = '1'
            ORDER BY geo_fips, time_period DESC
            """,
            {},
        )
        inc_rows = _safe_query(
            db,
            """
            SELECT geo_fips, geo_name, time_period, data_value
            FROM bea_regional
            WHERE table_name = 'SAINC1' AND line_code = '1'
            ORDER BY geo_fips, time_period DESC
            """,
            {},
        )

        # Build per-state year maps
        state_gdp: Dict[str, Dict[int, float]] = {}
        state_name_map: Dict[str, str] = {}
        for r in gdp_rows:
            fips = str(r.geo_fips or "").strip()[:2]
            if not fips or fips == "00":
                continue
            state_gdp.setdefault(fips, {})
            state_name_map[fips] = r.geo_name
            yr = str(r.time_period or "")[:4]
            if yr.isdigit() and r.data_value is not None:
                state_gdp[fips][int(yr)] = _safe_float(r.data_value)

        state_inc: Dict[str, Dict[int, float]] = {}
        for r in inc_rows:
            fips = str(r.geo_fips or "").strip()[:2]
            if not fips or fips == "00":
                continue
            state_inc.setdefault(fips, {})
            yr = str(r.time_period or "")[:4]
            if yr.isdigit() and r.data_value is not None:
                state_inc[fips][int(yr)] = _safe_float(r.data_value)

        for fips in state_gdp:
            gdp_g = _safe_pct_change(
                state_gdp[fips].get(2023), state_gdp[fips].get(2022)
            )
            inc_g = _safe_pct_change(
                state_inc.get(fips, {}).get(2023),
                state_inc.get(fips, {}).get(2022),
            )

            if gdp_g is None and inc_g is None:
                continue

            # Composite score
            g1 = gdp_g or 0.0
            g2 = inc_g or 0.0
            composite = (g1 + g2) / 2.0

            if composite > 4.0:
                opp_score = 85 + min(15, int((composite - 4.0) * 5))
            elif composite > 2.0:
                opp_score = 60 + int((composite - 2.0) / 2.0 * 25)
            elif composite > 0.0:
                opp_score = 50 + int(composite * 10)
            else:
                opp_score = max(0, 50 + int(composite * 10))

            opp_score = max(0, min(100, opp_score))

            if opp_score >= 80:
                geo_signal = "green"
            elif opp_score >= 50:
                geo_signal = "yellow"
            else:
                geo_signal = "red"

            geographic_opportunity.append({
                "fips": fips,
                "state": state_name_map.get(fips, fips),
                "gdp_growth_pct": gdp_g,
                "income_growth_pct": inc_g,
                "opportunity_score": opp_score,
                "signal": geo_signal,
            })

        geographic_opportunity.sort(key=lambda x: x["opportunity_score"], reverse=True)

    # Determine overall status
    has_macro = bool(found_fred)
    has_ces = _table_exists(db, ces_table)
    has_bea = _table_exists(db, "bea_regional")

    if not has_macro and not has_ces and not has_bea:
        status = "not_ingested"
    elif not has_macro or not has_ces:
        status = "partial"
    else:
        status = "ok"

    return {
        "status": status,
        "as_of": _today_str(),
        "data": {
            "deal_environment": deal_environment,
            "sector_momentum": sector_momentum,
            "geographic_opportunity": geographic_opportunity[:20],
            "geographic_opportunity_top5": geographic_opportunity[:5],
            "geographic_opportunity_bottom5": geographic_opportunity[-5:] if len(geographic_opportunity) >= 5 else geographic_opportunity,
        },
        "missing": [],
    }
