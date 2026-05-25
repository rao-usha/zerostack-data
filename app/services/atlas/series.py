"""
Atlas place-level time series + migration flows — SPEC_066b / PLAN_070.

Two small additions over the SPEC_065 layer API:

  1. `fetch_place_series(db, geo_id, layer_id)` — return a small time series
     for one (place, layer) pair, used by the place panel's sparklines. Only
     a curated set of layers are series-capable (FEMA, FDIC, IRS migration);
     everything else returns an empty `points: []`.

  2. `fetch_top_migration_flows(db, top_n)` — top-N IRS county-to-county
     migration flows for the latest tax year, used by the dynamic-UI layer
     to render animated arcs on the map.

Both are deliberately narrow — sparkline data for ~5 layers and one
migration table. PLAN_070 §3 calls these out as Pillar 1 (coupling) and
Pillar 2 (motion) features that make the explorer feel alive without
requiring new ETL.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


# ── Which layers support sparkline series at a place ────────────────────────
# Each entry maps a layer_id → a "kind" the series builder switches on.
SERIES_CAPABLE: Dict[str, str] = {
    "disaster_fema_declarations":  "fema_per_year",
    "finance_fdic_county_deposits": "fdic_quarterly",
    "demo_irs_migration_net_agi":   "irs_migration_net_per_year",
}


def _safe_query(db: Session, sql: str, params: Optional[dict] = None) -> List[dict]:
    """Same rollback-on-error discipline as layers.py — a failed cloud query
    poisons the transaction otherwise."""
    try:
        return [dict(r) for r in db.execute(text(sql), params or {}).mappings().all()]
    except Exception as exc:  # noqa: BLE001
        logger.warning("Atlas series query failed: %s", exc)
        try:
            db.rollback()
        except Exception:  # noqa: BLE001
            pass
        return []


def fetch_place_series(
    db: Session,
    geo_id: str,
    layer_id: str,
) -> Dict[str, Any]:
    """Return `{layer_id, geo_id, kind, points: [{x, y}], unit}` for a series
    layer at a place, or `{points: []}` if the layer doesn't carry a series.

    `x` is either an ISO date string (FDIC quarterly) or a year integer
    (FEMA, migration). `y` is always a number.
    """
    base = {"layer_id": layer_id, "geo_id": geo_id, "points": [], "unit": None}
    kind = SERIES_CAPABLE.get(layer_id)
    if not kind:
        return base

    if kind == "fema_per_year":
        if len(geo_id) != 5:
            return base
        state, county = geo_id[:2], geo_id[2:]
        rows = _safe_query(db, """
            SELECT EXTRACT(YEAR FROM declaration_date)::int AS x,
                   COUNT(*)::int AS y
            FROM fema_disaster_declarations
            WHERE fips_state_code = :st AND fips_county_code = :cty
              AND declaration_date IS NOT NULL
            GROUP BY x ORDER BY x
        """, {"st": state, "cty": county})
        base["unit"] = "declarations/year"
        base["kind"] = "fema_per_year"
        base["points"] = [{"x": r["x"], "y": r["y"]} for r in rows]
        return base

    if kind == "fdic_quarterly":
        if len(geo_id) != 5:
            return base
        rows = _safe_query(db, """
            SELECT f.repdte::text AS x, AVG(f.dep)::numeric AS y
            FROM fdic_bank_financials f
            JOIN fdic_institutions i USING (cert)
            WHERE i.stcnty = :sc AND f.dep IS NOT NULL
            GROUP BY f.repdte ORDER BY f.repdte
        """, {"sc": geo_id})
        base["unit"] = "USD (avg deposits per bank)"
        base["kind"] = "fdic_quarterly"
        base["points"] = [{"x": r["x"], "y": float(r["y"])} for r in rows if r["y"] is not None]
        return base

    if kind == "irs_migration_net_per_year":
        if len(geo_id) != 5:
            return base
        rows = _safe_query(db, """
            SELECT tax_year AS yr,
              SUM(CASE WHEN flow_type='inflow'  AND dest_county_code = :gid THEN total_agi ELSE 0 END) AS in_agi,
              SUM(CASE WHEN flow_type='outflow' AND orig_county_code = :gid THEN total_agi ELSE 0 END) AS out_agi
            FROM irs_soi_migration
            WHERE (flow_type='inflow' AND dest_county_code = :gid)
               OR (flow_type='outflow' AND orig_county_code = :gid)
            GROUP BY tax_year ORDER BY tax_year
        """, {"gid": geo_id})
        base["unit"] = "USD thousands (net AGI in − out)"
        base["kind"] = "irs_migration_net_per_year"
        base["points"] = [
            {"x": r["yr"], "y": float((r["in_agi"] or 0) - (r["out_agi"] or 0))}
            for r in rows
        ]
        return base

    return base


# ─────────────────────────────────────────────────────────────────────────────
# SPEC_066c additions — raw place-level events + FEMA cascade payload
# ─────────────────────────────────────────────────────────────────────────────

# Sources supported by `fetch_place_events`. Whitelist (not lookup) so an
# unknown `source` query param returns 400 rather than silently empty.
EVENT_SOURCES = {"fema"}


def fetch_place_events(
    db: Session,
    geo_id: str,
    source: str = "fema",
    limit: int = 200,
) -> Dict[str, Any]:
    """Return raw, dated events for a place — the data layer the calendar
    heatmap consumes. The series endpoint aggregates these to per-year
    counts; this exposes the underlying rows for richer visualizations."""
    if source not in EVENT_SOURCES:
        raise ValueError(f"unknown event source {source!r}; "
                         f"expected one of {sorted(EVENT_SOURCES)}")
    if not (geo_id.isdigit() and len(geo_id) == 5):
        raise ValueError(f"geo_id must be 5-digit county FIPS; got {geo_id!r}")
    state_fips, county_fips = geo_id[:2], geo_id[2:]

    if source == "fema":
        rows = _safe_query(db, """
            SELECT disaster_number, declaration_date::text AS date,
                   incident_type AS type, declaration_title AS title
            FROM fema_disaster_declarations
            WHERE fips_state_code = :st AND fips_county_code = :cty
              AND declaration_date IS NOT NULL
            ORDER BY declaration_date DESC
            LIMIT :lim
        """, {"st": state_fips, "cty": county_fips, "lim": max(1, min(limit, 1000))})
        return {
            "geo_id": geo_id,
            "source": "fema",
            "events": [
                {"date": r["date"], "type": r["type"], "title": r["title"],
                 "disaster_number": r["disaster_number"]}
                for r in rows
            ],
        }
    return {"geo_id": geo_id, "source": source, "events": []}


def fetch_fema_cascade(db: Session) -> Dict[str, Any]:
    """Bulk-fetch FEMA declaration counts per (county, year). The frontend
    pre-fetches once when the time scrubber opens; year selection then
    is a client-side dict lookup, no per-step server round-trip."""
    rows = _safe_query(db, """
        SELECT fips_state_code || lpad(fips_county_code, 3, '0') AS geo_id,
               EXTRACT(YEAR FROM declaration_date)::int AS yr,
               COUNT(*)::int AS n
        FROM fema_disaster_declarations
        WHERE fips_state_code IS NOT NULL
          AND fips_county_code IS NOT NULL
          AND declaration_date IS NOT NULL
        GROUP BY geo_id, yr
    """)
    years_set = set()
    values_by_year: Dict[str, Dict[str, int]] = {}
    for r in rows:
        gid = r["geo_id"]
        if not gid or len(gid) != 5 or not gid.isdigit():
            continue
        y = int(r["yr"])
        years_set.add(y)
        values_by_year.setdefault(str(y), {})[gid] = int(r["n"])
    years = sorted(years_set)
    return {
        "years": years,
        "values_by_year": values_by_year,
        "meta": {
            "non_zero_cells": sum(len(v) for v in values_by_year.values()),
            "year_range": [years[0] if years else None,
                            years[-1] if years else None],
        },
    }


# ─────────────────────────────────────────────────────────────────────────────
# SPEC_067 — Recent Activity feed
# ─────────────────────────────────────────────────────────────────────────────

# Source whitelist for the recent-feed endpoint. SEC + USAspending lanes
# wire in here as PLAN_067 SPEC_074 / SPEC_071-stretch land.
RECENT_SOURCES = {"fema"}


def _fetch_recent_fema(db: Session, limit: int) -> List[Dict[str, Any]]:
    """Return the most-recent FEMA declarations (one row per
    declaration × county) for the recent-activity feed."""
    rows = _safe_query(db, """
        SELECT disaster_number AS event_id,
               declaration_date::text AS date,
               incident_type AS type,
               declaration_title AS title,
               fips_state_code || lpad(fips_county_code, 3, '0') AS place_id,
               COALESCE(county || ', ', '') || COALESCE(state, '') AS place_name
        FROM fema_disaster_declarations
        WHERE declaration_date IS NOT NULL
          AND fips_state_code IS NOT NULL
          AND fips_county_code IS NOT NULL
        ORDER BY declaration_date DESC, disaster_number DESC
        LIMIT :lim
    """, {"lim": max(1, min(limit, 200))})
    return [
        {
            "source": "fema",
            "event_id": str(r["event_id"]),
            "date": r["date"],
            "type": r["type"] or "Unknown",
            "title": (r["title"] or "").strip(),
            "place_id": r["place_id"],
            "place_name": r["place_name"].strip(", "),
        }
        for r in rows
    ]


def fetch_recent_events(
    db: Session,
    sources: List[str],
    limit: int = 50,
) -> List[Dict[str, Any]]:
    """Return the merged most-recent event stream across requested sources,
    sorted by date desc. v1: FEMA-only; SEC/USAspending added when their
    backfills land."""
    unknown = set(sources) - RECENT_SOURCES
    if unknown:
        raise ValueError(f"unknown sources: {sorted(unknown)}; "
                         f"supported: {sorted(RECENT_SOURCES)}")
    cap = max(1, min(limit, 200))
    items: List[Dict[str, Any]] = []
    if "fema" in sources:
        items.extend(_fetch_recent_fema(db, cap))
    items.sort(key=lambda x: x["date"], reverse=True)
    return items[:cap]


def fetch_top_migration_flows(
    db: Session,
    top_n: int = 100,
    flow_type: str = "inflow",
) -> Dict[str, Any]:
    """Top N county-to-county IRS migration flows for the latest tax year.

    Filters to real US county FIPS (state codes 01-56); the IRS migration
    table contains aggregation rows with state codes like 96/97/98 that we
    skip. Returns `{tax_year, flows: [{orig, dest, returns, agi}], n}`.
    """
    if top_n < 1 or top_n > 1000:
        raise ValueError("top_n must be in [1, 1000]")
    if flow_type not in ("inflow", "outflow"):
        raise ValueError("flow_type must be 'inflow' or 'outflow'")

    # The "real" US state FIPS regex: 01-56 prefix on a 5-char county code.
    real_county_re = "^(0[1-9]|[1-4][0-9]|5[0-6])[0-9]{3}$"

    latest = _safe_query(
        db,
        "SELECT MAX(tax_year) AS y FROM irs_soi_migration",
    )
    tax_year = (latest[0]["y"] if latest else None)
    if not tax_year:
        return {"tax_year": None, "flows": [], "n": 0}

    rows = _safe_query(db, """
        SELECT orig_county_code AS orig,
               dest_county_code AS dest,
               SUM(num_returns)::int AS returns,
               SUM(total_agi)::bigint AS agi
        FROM irs_soi_migration
        WHERE flow_type = :ft
          AND tax_year = :ty
          AND orig_county_code ~ :rx
          AND dest_county_code ~ :rx
          AND orig_county_code != dest_county_code
          AND total_agi IS NOT NULL
        GROUP BY orig_county_code, dest_county_code
        ORDER BY agi DESC NULLS LAST
        LIMIT :n
    """, {"ft": flow_type, "ty": tax_year, "rx": real_county_re, "n": top_n})

    return {
        "tax_year": int(tax_year),
        "flow_type": flow_type,
        "n": len(rows),
        "flows": [
            {"orig": r["orig"], "dest": r["dest"],
             "returns": int(r["returns"] or 0), "agi": int(r["agi"] or 0)}
            for r in rows
        ],
    }
