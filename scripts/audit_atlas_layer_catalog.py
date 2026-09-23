"""
Atlas Layer Catalog Audit — SPEC_065 / PLAN_066 v3 pre-flight.

Iterates every public-schema table on the connected DB, classifies each by
domain / geographic grain / vintage / coverage, and emits both:

  * data/reference/atlas_layer_catalog_<YYYY-MM-DD>.csv  (human-readable)
  * data/reference/atlas_layer_catalog_<YYYY-MM-DD>.json (machine-readable —
    consumed by app/services/atlas/layers.py at import time)

This is the *honest data foundation* for the general explorer. It surfaces
which tables can back a map layer, at what grain, with what coverage, and
which should be excluded (infra tables) or deferred (PLAN_067 backfill).

Run inside the api container with the cloud DATABASE_URL active:
    docker exec nexdata-api-1 python scripts/audit_atlas_layer_catalog.py
"""
from __future__ import annotations

import csv
import json
import logging
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import psycopg2
import psycopg2.extras

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("catalog-audit")

# No default: the credentials live in the environment, never in the repo.
DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    sys.exit("DATABASE_URL is required (e.g. postgresql://nexdata:<password>@host.docker.internal:5435/nexdata)")

# ─────────────────────────────────────────────────────────────────────────────
# Domain mapping — table-name prefix / pattern → domain. Order matters
# (first match wins). Anything unmatched → 'unclassified'.
# ─────────────────────────────────────────────────────────────────────────────
DOMAIN_RULES: List[Tuple[str, str]] = [
    # disaster & risk
    (r"^fema_", "disaster"),
    (r"^national_risk", "disaster"),
    (r"^flood_", "disaster"),
    # environment
    (r"^epa_", "environment"),
    (r"^brownfield", "environment"),
    (r"^wetland", "environment"),
    (r"^water_", "environment"),
    (r"^public_water", "environment"),
    (r"^environmental_facility", "environment"),
    # energy & infrastructure
    (r"^power_", "energy"),
    (r"^transmission_", "energy"),
    (r"^substation", "energy"),
    (r"^eia_", "energy"),
    (r"^electricity_", "energy"),
    (r"^renewable_", "energy"),
    (r"^utility_rate", "energy"),
    # transport & logistics
    (r"^rail_", "transport"),
    (r"^airport", "transport"),
    (r"^port_", "transport"),
    (r"^container_", "transport"),
    (r"^air_cargo", "transport"),
    (r"^bts_", "transport"),
    (r"^trade_gateway", "transport"),
    # infrastructure (data centers, broadband, etc.)
    (r"^data_center", "infrastructure"),
    (r"^fcc_", "infrastructure"),
    (r"^broadband_", "infrastructure"),
    # demographics & income
    (r"^acs", "demographics"),
    (r"^irs_soi", "demographics"),
    # economy
    (r"^census_(business|cbp|economic)", "economy"),
    (r"^bls_", "economy"),
    (r"^bea_", "economy"),
    # finance & banking
    (r"^fdic_", "finance"),
    (r"^ffiec_", "finance"),
    (r"^treasury_", "finance"),
    (r"^fred_", "finance"),
    (r"^sec_", "finance"),
    # health
    (r"^cms_", "health"),
    (r"^nppes_", "health"),
    (r"^medspa", "health"),
    # real estate
    (r"^realestate", "real_estate"),
    (r"^building_permit", "real_estate"),
    (r"^land_use", "real_estate"),
    (r"^zoning_", "real_estate"),
    (r"^opportunity_zone", "real_estate"),
    # trade
    (r"^us_trade_", "trade"),
    # federal spending
    (r"^usaspending", "federal"),
    # agriculture
    (r"^usda_", "agriculture"),
    # international
    (r"^intl_", "international"),
    (r"^oecd_", "international"),
    # markets
    (r"^cftc_", "markets"),
    (r"^prediction_market", "markets"),
    # generic geo helpers
    (r"^geojson_boundaries", "_canvas"),
    (r"^county_", "_geo_meta"),
    (r"^government_unit", "_geo_meta"),
    (r"^geocoded_", "_geo_meta"),
    (r"^zip_", "_geo_meta"),
]

# Hard-exclude infrastructure / app-internal tables — these are never layers.
EXCLUDE_PATTERNS: List[str] = [
    r"^alembic_",
    r"^apscheduler_",
    r"^ingestion_",
    r"^job_",
    r"^llm_",
    r"^dataset_",
    r"^dq_",
    r"^data_quality_",
    r"^data_profile_",
    r"^webhook",
    r"^audit_",
    r"^collection_audit",
    r"^rate_limit_",
    r"^source_watermarks",
    r"^users$",
    r"^login_codes",
    r"^api_keys",
    r"^leads$",
    r"^playground_",
    r"^reports$",
    r"^report_jobs",
    r"^export_jobs",
    r"^diligence_orders",
    # atlas internal
    r"^atlas_",
    # synthetic / generators
    r"^synthetic_",
    r"^generator_",
    # PE / people / orgchart / family-office collections (empty per PLAN_065 review)
    r"^pe_",
    r"^people$",
    r"^company_people",
    r"^org_chart",
    r"^family_office",
    r"^lp_",
    r"^industrial_companies",
    r"^pitchbook",
    r"^site_intel_",
    r"^datacenter_site_scores",
    r"^medspa_(prospects|opportunity|scores)",
    r"^zip_medspa",
    r"^search_index",
    r"^market_observations",
    r"^m5_",
    r"^dunl_",
]

EXCLUDE_RE = re.compile("|".join(EXCLUDE_PATTERNS))


def classify_domain(table: str) -> str:
    for pattern, domain in DOMAIN_RULES:
        if re.search(pattern, table):
            return domain
    return "unclassified"


# ─────────────────────────────────────────────────────────────────────────────
# Geo-grain detection
# ─────────────────────────────────────────────────────────────────────────────

GEO_COLUMN_HINTS = {
    "county": [
        "county_fips", "stcnty", "fips_county_code",
    ],
    "state": ["state_fips", "fips_state_code", "state_code", "stalp", "state_abbr"],
    "point": ["latitude", "longitude"],
    "zcta": [],  # detected by geo_id + zcta pattern in samples
}

DATE_COLUMN_CANDIDATES = [
    "year", "tax_year", "fiscal_year", "record_calendar_year", "fy_declared",
    "date", "record_date", "repdte", "declaration_date", "obligation_date",
    "period_of_performance_start", "ingested_at", "collected_at", "fetched_at",
]


def detect_geo_grain(cols: Dict[str, str]) -> Tuple[str, Optional[str]]:
    """Return (grain, geo_key_column). cols = {column_name: data_type}."""
    col_lower = {c.lower() for c in cols}
    for c in GEO_COLUMN_HINTS["county"]:
        if c in col_lower:
            return "county", c
    for c in GEO_COLUMN_HINTS["state"]:
        if c in col_lower:
            return "state", c
    if "latitude" in col_lower and "longitude" in col_lower:
        return "point", "lat/lon"
    if "state" in col_lower and "county" in col_lower:
        # site-intel pattern: 2-char state + county name (no FIPS)
        return "state", "state"
    # geo_id without an explicit county/state FIPS — likely ZCTA or unknown
    if "geo_id" in col_lower:
        return "zcta_or_other", "geo_id"
    return "national", None


def detect_date_column(cols: Dict[str, str]) -> Optional[str]:
    col_lower = {c.lower() for c in cols}
    for cand in DATE_COLUMN_CANDIDATES:
        if cand in col_lower:
            return cand
    return None


# ─────────────────────────────────────────────────────────────────────────────
# Verdict heuristic
# ─────────────────────────────────────────────────────────────────────────────

# Domains the explorer monetizes / lights up first when ✅
PRIORITY_DOMAINS = {
    "disaster", "environment", "energy", "transport",
    "demographics", "economy", "finance", "infrastructure",
}

# Tables explicitly excluded per the data review (PLAN_066 §4)
HARD_DEFERRED = {"usaspending_awards"}        # 🔴 thin + dateless → PLAN_067 SPEC_071
HARD_DEFERRED_COUNTY = {"acs5_2023_b19013"}   # ZIP-keyed → PLAN_067 SPEC_070


def verdict_for(table: str, domain: str, grain: str, rows: int,
                 distinct_geos: Optional[int]) -> Tuple[str, str]:
    """Return (verdict, note). Verdict ∈ {'✅','🟡','🔴','⏸','—'}."""
    if table in HARD_DEFERRED:
        return "⏸", "Deferred to PLAN_067 SPEC_071 — thin/dateless"
    if table in HARD_DEFERRED_COUNTY:
        return "⏸", "ZIP-keyed; county layer deferred to PLAN_067 SPEC_070"
    if domain.startswith("_"):
        return "—", "canvas / geo helper, not a layer"
    if domain == "unclassified":
        return "—", "unclassified — review manually"
    if rows == 0:
        return "🔴", "empty"
    if grain == "county" and distinct_geos is not None and distinct_geos >= 2500:
        return "✅", f"full county ({distinct_geos})"
    if grain == "county" and distinct_geos is not None:
        return "🟡", f"partial county coverage ({distinct_geos}/~3143)"
    if grain == "state" and distinct_geos is not None and distinct_geos >= 45:
        return "✅", f"state grain ({distinct_geos} states)"
    if grain == "state":
        return "🟡", f"state grain (partial: {distinct_geos})"
    if grain == "point" and rows >= 100:
        return "✅", f"point layer ({rows:,} features)"
    if grain == "national":
        return "—", "national time-series / context — not a layer"
    if grain == "zcta_or_other":
        return "🟡", "non-county geo_id; verify format"
    return "🟡", "review manually"


# ─────────────────────────────────────────────────────────────────────────────
# Driver
# ─────────────────────────────────────────────────────────────────────────────

def list_tables(cur) -> List[Tuple[str, int]]:
    cur.execute("""
        SELECT c.relname, c.reltuples::bigint AS approx_rows
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public'
          AND c.relkind = 'r'
        ORDER BY c.relname
    """)
    return [(r[0], r[1]) for r in cur.fetchall()]


def get_columns(cur, table: str) -> Dict[str, str]:
    cur.execute("""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = %s
    """, (table,))
    return {r[0]: r[1] for r in cur.fetchall()}


def safe_distinct_geo(cur, table: str, geo_col: str) -> Optional[int]:
    """COUNT(DISTINCT geo_col) — may be slow on big tables. Tolerate failure."""
    try:
        cur.execute(f"SELECT COUNT(DISTINCT {geo_col}) FROM {table}")
        return int(cur.fetchone()[0])
    except Exception as exc:  # noqa: BLE001
        log.warning("distinct(%s.%s) failed: %s", table, geo_col, exc)
        try:
            cur.execute("ROLLBACK")
        except Exception:
            pass
        return None


def safe_date_range(cur, table: str, date_col: str) -> Optional[str]:
    try:
        cur.execute(f"SELECT MIN({date_col})::text, MAX({date_col})::text FROM {table}")
        lo, hi = cur.fetchone()
        if lo is None and hi is None:
            return None
        return f"{lo} → {hi}"
    except Exception as exc:  # noqa: BLE001
        log.warning("date range %s.%s failed: %s", table, date_col, exc)
        try:
            cur.execute("ROLLBACK")
        except Exception:
            pass
        return None


def audit() -> List[Dict[str, Any]]:
    log.info("Connecting to %s", DATABASE_URL.split("@")[-1])
    conn = psycopg2.connect(DATABASE_URL, connect_timeout=10)
    conn.autocommit = True
    cur = conn.cursor()
    tables = list_tables(cur)
    log.info("Found %d public-schema tables", len(tables))

    rows: List[Dict[str, Any]] = []
    for i, (table, approx_rows) in enumerate(tables, 1):
        if EXCLUDE_RE.search(table):
            continue
        cols = get_columns(cur, table)
        if not cols:
            continue
        domain = classify_domain(table)
        grain, geo_key = detect_geo_grain(cols)
        date_col = detect_date_column(cols)

        # Skip distinct counts on huge non-priority tables — too slow
        distinct_geos: Optional[int] = None
        if geo_key and geo_key != "lat/lon" and approx_rows < 2_000_000:
            distinct_geos = safe_distinct_geo(cur, table, geo_key)

        vintage = safe_date_range(cur, table, date_col) if date_col else None
        verdict, note = verdict_for(table, domain, grain, approx_rows, distinct_geos)

        rec = {
            "table": table,
            "row_count": int(approx_rows),
            "domain": domain,
            "geo_grain": grain,
            "geo_key": geo_key or "",
            "distinct_geos": distinct_geos if distinct_geos is not None else "",
            "date_column": date_col or "",
            "vintage": vintage or "",
            "verdict": verdict,
            "note": note,
        }
        rows.append(rec)
        if i % 25 == 0:
            log.info("  ...%d tables scanned", i)
    cur.close()
    conn.close()
    log.info("Catalog complete: %d candidate tables", len(rows))
    return rows


def write_outputs(rows: List[Dict[str, Any]]) -> Tuple[Path, Path]:
    out_dir = ROOT / "data" / "reference"
    out_dir.mkdir(parents=True, exist_ok=True)
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    csv_path = out_dir / f"atlas_layer_catalog_{today}.csv"
    json_path = out_dir / f"atlas_layer_catalog_{today}.json"

    # Sort: layer-candidate domains first (✅ then 🟡 then others), then unclassified
    def sort_key(r: Dict[str, Any]):
        verdict_order = {"✅": 0, "🟡": 1, "🔴": 2, "⏸": 3, "—": 4}.get(r["verdict"], 9)
        return (r["domain"], verdict_order, r["table"])

    rows = sorted(rows, key=sort_key)

    cols = ["table", "row_count", "domain", "geo_grain", "geo_key",
            "distinct_geos", "date_column", "vintage", "verdict", "note"]
    with csv_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        w.writerows(rows)

    payload = {
        "_meta": {
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "database": DATABASE_URL.split("@")[-1],
            "row_count": len(rows),
            "spec": "SPEC_065",
        },
        "catalog": rows,
    }
    with json_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, default=str)
    return csv_path, json_path


def print_summary(rows: List[Dict[str, Any]]) -> None:
    by_domain: Dict[str, List[Dict[str, Any]]] = {}
    for r in rows:
        by_domain.setdefault(r["domain"], []).append(r)
    print()
    print("=" * 72)
    print(f"{'DOMAIN':<18} {'✅':>4} {'🟡':>4} {'🔴':>4} {'⏸':>4} {'—':>4} {'total':>6}")
    print("=" * 72)
    for dom in sorted(by_domain):
        rs = by_domain[dom]
        c = {k: sum(1 for r in rs if r["verdict"] == k) for k in ("✅", "🟡", "🔴", "⏸", "—")}
        print(f"{dom:<18} {c['✅']:>4} {c['🟡']:>4} {c['🔴']:>4} {c['⏸']:>4} {c['—']:>4} {len(rs):>6}")
    print("=" * 72)

    print("\nTOP CANDIDATE LAYERS (✅ only):")
    for r in sorted(rows, key=lambda r: (-r["row_count"], r["table"])):
        if r["verdict"] == "✅":
            print(f"  {r['domain']:<14} {r['table']:<32} "
                  f"{r['geo_grain']:<8} rows={r['row_count']:>10,}  "
                  f"geos={str(r['distinct_geos']):>6}  {r['note']}")


def main() -> int:
    rows = audit()
    csv_path, json_path = write_outputs(rows)
    print_summary(rows)
    print(f"\nWrote: {csv_path}")
    print(f"Wrote: {json_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
