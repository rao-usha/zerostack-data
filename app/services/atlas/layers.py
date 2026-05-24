"""
Atlas layer registry — SPEC_065 / PLAN_066 v3.

Every Nexdata public-data table that can be a map layer is registered here
with **honest grain** and a query builder that returns either a choropleth
value map ({geo_id: value}) or a GeoJSON point/feature collection.

The registry was hand-curated from the catalog audit
(`data/reference/atlas_layer_catalog_2026-05-23.{csv,json}`) — the heuristic
auto-classification surfaced 30+ candidates; this file locks the v1 set with
overrides for the cases the auto-classifier got wrong (IRS county uses
`county_code` directly as 5-digit FIPS; FCC ships at state grain because the
table has no county rows; etc.).

Honest exclusions per the PLAN_066 §4 data review:
  * usaspending — thin, dateless slice → PLAN_067 SPEC_071 backfill
  * ACS-as-county-wealth — ZCTA-keyed, needs ZCTA→county crosswalk →
    PLAN_067 SPEC_070
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


# ── data shapes ─────────────────────────────────────────────────────────────

@dataclass
class LayerSpec:
    """One layer registry entry — describes the layer + how to build its data."""
    id: str
    label: str
    domain: str
    grain: str               # 'county' | 'state' | 'point'
    default_on: bool
    vintage: str             # human-readable
    coverage_note: str
    unit: str                # e.g. 'count', 'USD', 'score'
    description: str
    builder: Callable[[Session], "LayerResult"]


@dataclass
class LayerResult:
    layer_id: str
    grain: str
    values: Optional[Dict[str, float]] = None       # {fips: value} for choropleth
    features: Optional[List[Dict[str, Any]]] = None  # GeoJSON features for points
    legend: Dict[str, Any] = field(default_factory=dict)
    provenance: List[Dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "layer_id": self.layer_id,
            "grain": self.grain,
            "values": self.values,
            "features": self.features,
            "legend": self.legend,
            "provenance": self.provenance,
        }


# ── safe query helper (the SPEC_061 rollback-on-failure lesson) ─────────────

def _safe_query(db: Session, sql: str, params: Optional[Dict] = None) -> List[Dict]:
    """Run a query; on any failure, log + rollback (so the session stays
    usable for the next layer) + return []. Carries the SPEC_061
    `_safe_query` discipline — without the rollback, a missing table or bad
    column poisons the transaction for everything downstream."""
    try:
        rows = db.execute(text(sql), params or {}).mappings().all()
        return [dict(r) for r in rows]
    except Exception as exc:  # noqa: BLE001
        logger.warning("Atlas layer query failed: %s — %s",
                       sql.strip().splitlines()[0], exc)
        try:
            db.rollback()
        except Exception:  # noqa: BLE001
            pass
        return []


def _legend(values: List[float], unit: str) -> Dict[str, Any]:
    """Compute a simple legend from values — quintile breaks."""
    if not values:
        return {"unit": unit, "min": 0, "max": 0, "breaks": []}
    vs = sorted(v for v in values if v is not None)
    if not vs:
        return {"unit": unit, "min": 0, "max": 0, "breaks": []}
    n = len(vs)
    breaks = [vs[int(n * q)] for q in (0.2, 0.4, 0.6, 0.8)]
    return {"unit": unit, "min": vs[0], "max": vs[-1], "breaks": breaks}


def _points_from_rows(rows: List[Dict], name_col: str = "name",
                       extra_cols: Optional[List[str]] = None) -> List[Dict]:
    """Convert lat/lon rows to GeoJSON point features."""
    out = []
    for r in rows:
        lat, lon = r.get("latitude"), r.get("longitude")
        if lat is None or lon is None:
            continue
        props = {"name": r.get(name_col, "—")}
        for c in (extra_cols or []):
            if c in r:
                props[c] = r[c]
        out.append({
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [float(lon), float(lat)]},
            "properties": props,
        })
    return out


# ═════════════════════════════════════════════════════════════════════════════
# Layer builders — one function per layer
# ═════════════════════════════════════════════════════════════════════════════

# ─── Disaster ────────────────────────────────────────────────────────────────

def _build_nri(db: Session) -> LayerResult:
    rows = _safe_query(db, """
        SELECT county_fips AS geo_id, risk_score::numeric AS value
        FROM national_risk_index
        WHERE risk_score IS NOT NULL
    """)
    values = {r["geo_id"]: float(r["value"]) for r in rows if r["geo_id"]}
    return LayerResult(
        layer_id="disaster_nri", grain="county", values=values,
        legend=_legend(list(values.values()), "risk score"),
        provenance=[{"table": "national_risk_index", "rows": len(values)}],
    )


def _build_fema_declarations(db: Session) -> LayerResult:
    rows = _safe_query(db, """
        SELECT (fips_state_code || fips_county_code) AS geo_id,
               COUNT(*)::int AS value
        FROM fema_disaster_declarations
        WHERE fips_state_code IS NOT NULL AND fips_county_code IS NOT NULL
        GROUP BY 1
    """)
    values = {r["geo_id"]: int(r["value"]) for r in rows if r["geo_id"]}
    return LayerResult(
        layer_id="disaster_fema_declarations", grain="county", values=values,
        legend=_legend(list(values.values()), "declarations 1999-present"),
        provenance=[{"table": "fema_disaster_declarations", "rows": len(values)}],
    )


# ─── Environment (point layers) ──────────────────────────────────────────────

def _build_epa_facilities(db: Session) -> LayerResult:
    # 1.07M rows — sample for the point layer (the map can't render millions).
    rows = _safe_query(db, """
        SELECT facility_name AS name, latitude, longitude, state, county,
               compliance_status, violation_count
        FROM epa_echo_facilities
        WHERE latitude IS NOT NULL AND longitude IS NOT NULL
          AND violation_count > 5
        LIMIT 5000
    """)
    features = _points_from_rows(rows, extra_cols=["state", "county",
                                                    "compliance_status",
                                                    "violation_count"])
    return LayerResult(
        layer_id="env_epa_facilities", grain="point", features=features,
        legend={"unit": "facilities (high-violation sample)",
                "note": "sampled to 5,000 of 1.07M for map performance"},
        provenance=[{"table": "epa_echo_facilities", "rows": len(features),
                     "sampled_from": 1_068_232}],
    )


def _build_brownfields(db: Session) -> LayerResult:
    rows = _safe_query(db, """
        SELECT site_name AS name, latitude, longitude, state, county
        FROM brownfield_site
        WHERE latitude IS NOT NULL AND longitude IS NOT NULL
        LIMIT 10000
    """)
    features = _points_from_rows(rows, extra_cols=["state", "county"])
    return LayerResult(
        layer_id="env_brownfields", grain="point", features=features,
        legend={"unit": "brownfield sites"},
        provenance=[{"table": "brownfield_site", "rows": len(features)}],
    )


def _build_water_monitoring(db: Session) -> LayerResult:
    rows = _safe_query(db, """
        SELECT site_name AS name, latitude, longitude, state
        FROM water_monitoring_site
        WHERE latitude IS NOT NULL AND longitude IS NOT NULL
        LIMIT 10000
    """)
    features = _points_from_rows(rows, extra_cols=["state"])
    return LayerResult(
        layer_id="env_water_monitoring", grain="point", features=features,
        legend={"unit": "monitoring sites"},
        provenance=[{"table": "water_monitoring_site", "rows": len(features)}],
    )


# ─── Energy / infrastructure (point layers) ─────────────────────────────────

def _build_power_plants(db: Session) -> LayerResult:
    rows = _safe_query(db, """
        SELECT name, latitude, longitude, state, primary_fuel,
               nameplate_capacity_mw
        FROM power_plant
        WHERE latitude IS NOT NULL AND longitude IS NOT NULL
    """)
    features = _points_from_rows(rows, extra_cols=["state", "primary_fuel",
                                                    "nameplate_capacity_mw"])
    return LayerResult(
        layer_id="energy_power_plants", grain="point", features=features,
        legend={"unit": "power plants"},
        provenance=[{"table": "power_plant", "rows": len(features)}],
    )


def _build_substations(db: Session) -> LayerResult:
    rows = _safe_query(db, """
        SELECT name, latitude, longitude, state
        FROM substation
        WHERE latitude IS NOT NULL AND longitude IS NOT NULL
        LIMIT 10000
    """)
    features = _points_from_rows(rows, extra_cols=["state"])
    return LayerResult(
        layer_id="energy_substations", grain="point", features=features,
        legend={"unit": "substations"},
        provenance=[{"table": "substation", "rows": len(features)}],
    )


def _build_data_centers(db: Session) -> LayerResult:
    rows = _safe_query(db, """
        SELECT name, latitude, longitude, state, operator, power_mw
        FROM data_center_facility
        WHERE latitude IS NOT NULL AND longitude IS NOT NULL
    """)
    features = _points_from_rows(rows, extra_cols=["state", "operator", "power_mw"])
    return LayerResult(
        layer_id="infra_data_centers", grain="point", features=features,
        legend={"unit": "data centers"},
        provenance=[{"table": "data_center_facility", "rows": len(features)}],
    )


# ─── Transport ───────────────────────────────────────────────────────────────

def _build_airports(db: Session) -> LayerResult:
    rows = _safe_query(db, """
        SELECT name, latitude, longitude, state, city, airport_type,
               has_cargo_facility
        FROM airport
        WHERE latitude IS NOT NULL AND longitude IS NOT NULL
    """)
    features = _points_from_rows(rows, extra_cols=["state", "city",
                                                    "airport_type",
                                                    "has_cargo_facility"])
    return LayerResult(
        layer_id="transport_airports", grain="point", features=features,
        legend={"unit": "airports"},
        provenance=[{"table": "airport", "rows": len(features)}],
    )


def _build_rail_density(db: Session) -> LayerResult:
    """rail_line is line geometry — represent as state-level segment count."""
    rows = _safe_query(db, """
        SELECT state AS geo_id, COUNT(*)::int AS value
        FROM rail_line
        WHERE state IS NOT NULL
        GROUP BY state
    """)
    values = {r["geo_id"]: int(r["value"]) for r in rows if r["geo_id"]}
    return LayerResult(
        layer_id="transport_rail_density", grain="state", values=values,
        legend=_legend(list(values.values()), "rail segments per state"),
        provenance=[{"table": "rail_line", "rows": len(values)}],
    )


# ─── Demographics ────────────────────────────────────────────────────────────

def _build_irs_county_agi(db: Session) -> LayerResult:
    """IRS SOI county income. county_code IS the 5-digit FIPS (the corrected
    finding from the 2026-05-22 data review)."""
    rows = _safe_query(db, """
        SELECT county_code AS geo_id,
               AVG(total_agi / NULLIF(num_returns, 0))::numeric AS value
        FROM irs_soi_county_income
        WHERE tax_year = (SELECT MAX(tax_year) FROM irs_soi_county_income)
          AND county_code IS NOT NULL
          AND num_returns IS NOT NULL AND num_returns > 0
        GROUP BY county_code
    """)
    values = {r["geo_id"]: float(r["value"]) for r in rows if r["geo_id"]}
    return LayerResult(
        layer_id="demo_irs_county_agi_per_return", grain="county", values=values,
        legend=_legend(list(values.values()), "USD AGI per return"),
        provenance=[{"table": "irs_soi_county_income", "rows": len(values)}],
    )


def _build_federal_dollars(db: Session) -> LayerResult:
    """USAspending federal contract dollars per county — SPEC_071.
    Reads from `usaspending_county_fy_totals`, latest fiscal_year ×
    contracts. Populated by `scripts/ingest_usaspending_county.py`."""
    rows = _safe_query(db, """
        WITH latest AS (
            SELECT MAX(fiscal_year) AS fy
            FROM usaspending_county_fy_totals
            WHERE award_type_group = 'contracts'
        )
        SELECT geo_id, total_obligation AS value
        FROM usaspending_county_fy_totals, latest
        WHERE fiscal_year = latest.fy
          AND award_type_group = 'contracts'
          AND total_obligation > 0
    """)
    values = {r["geo_id"]: float(r["value"]) for r in rows if r["geo_id"]}
    return LayerResult(
        layer_id="econ_federal_dollars", grain="county", values=values,
        legend=_legend(list(values.values()), "USD federal contract obligations"),
        provenance=[{"table": "usaspending_county_fy_totals", "rows": len(values)}],
    )


def _build_acs_median_income(db: Session) -> LayerResult:
    """ACS B19013 median household income at county summary — SPEC_070.
    Reads from the grain-explicit `acs5_county_2023_b19013` table
    populated by `scripts/ingest_acs_county_b19013.py`."""
    rows = _safe_query(db, """
        SELECT geo_id, b19013_001e AS value
        FROM acs5_county_2023_b19013
        WHERE b19013_001e IS NOT NULL AND b19013_001e > 0
    """)
    values = {r["geo_id"]: int(r["value"]) for r in rows if r["geo_id"]}
    return LayerResult(
        layer_id="demo_acs_median_income", grain="county", values=values,
        legend=_legend(list(values.values()), "USD median household income"),
        provenance=[{"table": "acs5_county_2023_b19013", "rows": len(values)}],
    )


def _build_irs_migration_net(db: Session) -> LayerResult:
    """Net migration AGI per destination county — inflow minus outflow."""
    rows = _safe_query(db, """
        WITH net AS (
            SELECT (lpad(dest_state_code,2,'0') || lpad(dest_county_code,3,'0')) AS geo_id,
                   SUM(CASE WHEN flow_type ILIKE '%in%'  THEN total_agi
                            WHEN flow_type ILIKE '%out%' THEN -total_agi
                            ELSE 0 END)::numeric AS net_agi
            FROM irs_soi_migration
            WHERE tax_year = (SELECT MAX(tax_year) FROM irs_soi_migration)
              AND dest_state_code IS NOT NULL AND dest_county_code IS NOT NULL
            GROUP BY 1
        )
        SELECT geo_id, net_agi AS value FROM net WHERE net_agi IS NOT NULL
    """)
    values = {r["geo_id"]: float(r["value"]) for r in rows if r["geo_id"]}
    return LayerResult(
        layer_id="demo_irs_migration_net_agi", grain="county", values=values,
        legend=_legend(list(values.values()), "USD net migration AGI"),
        provenance=[{"table": "irs_soi_migration", "rows": len(values)}],
    )


# ─── Economy ─────────────────────────────────────────────────────────────────

def _build_cbp_industry_density_state(db: Session) -> LayerResult:
    """Census CBP at honest state grain. NAICS-2 to maximize coverage."""
    rows = _safe_query(db, """
        SELECT state_fips AS geo_id,
               SUM(establishments)::int AS value
        FROM census_business_patterns
        WHERE length(naics_code) = 2 AND state_fips IS NOT NULL
        GROUP BY state_fips
    """)
    values = {r["geo_id"]: int(r["value"]) for r in rows if r["geo_id"]}
    return LayerResult(
        layer_id="econ_cbp_establishments_state", grain="state", values=values,
        legend=_legend(list(values.values()), "establishments"),
        provenance=[{"table": "census_business_patterns", "rows": len(values)}],
    )


# ─── Finance — FDIC via cert→institutions.stcnty (the corrected join) ───────

def _build_fdic_county_deposits(db: Session) -> LayerResult:
    rows = _safe_query(db, """
        SELECT i.stcnty AS geo_id,
               AVG(f.dep)::numeric AS value
        FROM fdic_bank_financials f
        JOIN fdic_institutions i USING (cert)
        WHERE i.stcnty IS NOT NULL
          AND f.repdte = (SELECT MAX(repdte) FROM fdic_bank_financials)
          AND f.dep IS NOT NULL
        GROUP BY i.stcnty
    """)
    values = {r["geo_id"]: float(r["value"]) for r in rows if r["geo_id"]}
    return LayerResult(
        layer_id="finance_fdic_county_deposits", grain="county", values=values,
        legend=_legend(list(values.values()), "USD avg deposits per bank"),
        provenance=[{"table": "fdic_bank_financials × fdic_institutions",
                     "rows": len(values)}],
    )


# ─── Infrastructure — FCC at honest state grain ─────────────────────────────

def _build_fcc_broadband_state(db: Session) -> LayerResult:
    """FCC table is state-grain only — honest layer at that grain."""
    rows = _safe_query(db, """
        SELECT geography_id AS geo_id,
               COUNT(DISTINCT provider_id)::int AS value
        FROM fcc_broadband_coverage
        WHERE geography_type = 'state' AND geography_id IS NOT NULL
        GROUP BY geography_id
    """)
    values = {r["geo_id"]: int(r["value"]) for r in rows if r["geo_id"]}
    return LayerResult(
        layer_id="infra_fcc_providers_state", grain="state", values=values,
        legend=_legend(list(values.values()), "broadband providers per state"),
        provenance=[{"table": "fcc_broadband_coverage", "rows": len(values)}],
    )


# ─── Real Estate — bonus county layer surfaced by the audit ─────────────────

def _build_building_permits_county(db: Session) -> LayerResult:
    """building_permit has full county coverage (3,024 counties) — bonus."""
    rows = _safe_query(db, """
        SELECT county_fips AS geo_id, COUNT(*)::int AS value
        FROM building_permit
        WHERE county_fips IS NOT NULL
        GROUP BY county_fips
    """)
    values = {r["geo_id"]: int(r["value"]) for r in rows if r["geo_id"]}
    return LayerResult(
        layer_id="realestate_building_permits", grain="county", values=values,
        legend=_legend(list(values.values()), "building permits"),
        provenance=[{"table": "building_permit", "rows": len(values)}],
    )


# ─── Trade — state grain ─────────────────────────────────────────────────────

def _build_trade_exports_state(db: Session) -> LayerResult:
    rows = _safe_query(db, """
        SELECT state_code AS geo_id,
               SUM(value_monthly)::numeric AS value
        FROM us_trade_exports_state
        WHERE state_code IS NOT NULL AND value_monthly IS NOT NULL
          AND year = (SELECT MAX(year) FROM us_trade_exports_state)
        GROUP BY state_code
    """)
    values = {r["geo_id"]: float(r["value"]) for r in rows if r["geo_id"]}
    return LayerResult(
        layer_id="trade_exports_state", grain="state", values=values,
        legend=_legend(list(values.values()), "USD export value (latest year)"),
        provenance=[{"table": "us_trade_exports_state", "rows": len(values)}],
    )


# ═════════════════════════════════════════════════════════════════════════════
# The registry — single source of truth
# ═════════════════════════════════════════════════════════════════════════════
#
# Cuts enforced (PLAN_066 §4 review):
#   - usaspending_*  → not registered (thin/dateless; PLAN_067 SPEC_071 backfill)
#   - acs5_*_b19013 as county wealth → not registered (ZCTA-keyed; SPEC_070)

LAYERS: Dict[str, LayerSpec] = {
    # Disaster
    "disaster_nri": LayerSpec(
        id="disaster_nri", label="Natural-Hazard Risk Index",
        domain="disaster", grain="county", default_on=True,
        vintage="periodic", coverage_note="full county (3,564)",
        unit="risk score (0-100)",
        description="FEMA National Risk Index composite score per county.",
        builder=_build_nri,
    ),
    "disaster_fema_declarations": LayerSpec(
        id="disaster_fema_declarations", label="FEMA Disaster History",
        domain="disaster", grain="county", default_on=False,
        vintage="1999–2026", coverage_note="full county (3,282)",
        unit="declarations",
        description="Count of federal disaster declarations per county since 1999.",
        builder=_build_fema_declarations,
    ),
    # Environment
    "env_epa_facilities": LayerSpec(
        id="env_epa_facilities", label="EPA Regulated Facilities (high-violation)",
        domain="environment", grain="point", default_on=False,
        vintage="rolling", coverage_note="sample of 1.07M (top violations)",
        unit="facilities",
        description="Sampled EPA-regulated facilities with elevated violation counts.",
        builder=_build_epa_facilities,
    ),
    "env_brownfields": LayerSpec(
        id="env_brownfields", label="Brownfield Sites",
        domain="environment", grain="point", default_on=False,
        vintage="rolling", coverage_note="up to 10,000 of 45k",
        unit="sites",
        description="EPA brownfield site inventory.",
        builder=_build_brownfields,
    ),
    "env_water_monitoring": LayerSpec(
        id="env_water_monitoring", label="Water Monitoring Sites",
        domain="environment", grain="point", default_on=False,
        vintage="rolling", coverage_note="up to 10,000 of 33k",
        unit="sites",
        description="EPA water-quality monitoring sites.",
        builder=_build_water_monitoring,
    ),
    # Energy
    "energy_power_plants": LayerSpec(
        id="energy_power_plants", label="Power Plants",
        domain="energy", grain="point", default_on=False,
        vintage="rolling", coverage_note="full (14k)",
        unit="plants",
        description="EIA-listed power generation facilities.",
        builder=_build_power_plants,
    ),
    "energy_substations": LayerSpec(
        id="energy_substations", label="Electrical Substations",
        domain="energy", grain="point", default_on=False,
        vintage="rolling", coverage_note="up to 10,000 of 8.7k",
        unit="substations",
        description="HIFLD substation inventory.",
        builder=_build_substations,
    ),
    # Infrastructure
    "infra_data_centers": LayerSpec(
        id="infra_data_centers", label="Data Centers",
        domain="infrastructure", grain="point", default_on=False,
        vintage="rolling", coverage_note="full (1,383)",
        unit="data centers",
        description="PeeringDB data-center inventory.",
        builder=_build_data_centers,
    ),
    "infra_fcc_providers_state": LayerSpec(
        id="infra_fcc_providers_state", label="Broadband Providers (state)",
        domain="infrastructure", grain="state", default_on=False,
        vintage="recent", coverage_note="state grain only (no county data in FCC table)",
        unit="providers",
        description="Distinct broadband providers per state. State-grain only — "
                    "county coverage deferred to PLAN_067 SPEC_072.",
        builder=_build_fcc_broadband_state,
    ),
    # Transport
    "transport_airports": LayerSpec(
        id="transport_airports", label="Airports",
        domain="transport", grain="point", default_on=False,
        vintage="rolling", coverage_note="full (13k)",
        unit="airports",
        description="FAA airport inventory.",
        builder=_build_airports,
    ),
    "transport_rail_density": LayerSpec(
        id="transport_rail_density", label="Rail Segment Density (state)",
        domain="transport", grain="state", default_on=False,
        vintage="rolling", coverage_note="48 states",
        unit="segments",
        description="Rail-network segment count per state (FRA).",
        builder=_build_rail_density,
    ),
    # Demographics
    "demo_acs_median_income": LayerSpec(
        id="demo_acs_median_income", label="Median Household Income (ACS)",
        domain="demographics", grain="county", default_on=False,
        vintage="2023 ACS 5-year", coverage_note="3,222 counties",
        unit="USD",
        description="Median household income from ACS B19013 — county summary, "
                    "ingested directly from Census API (SPEC_070).",
        builder=_build_acs_median_income,
    ),
    "demo_irs_county_agi_per_return": LayerSpec(
        id="demo_irs_county_agi_per_return", label="Avg AGI per Tax Return",
        domain="demographics", grain="county", default_on=False,
        vintage="2018–2021 (latest)", coverage_note="full county (3,152)",
        unit="USD",
        description="IRS SOI: average adjusted gross income per tax return, "
                    "by county, latest tax year. (`county_code` is the 5-digit FIPS.)",
        builder=_build_irs_county_agi,
    ),
    "demo_irs_migration_net_agi": LayerSpec(
        id="demo_irs_migration_net_agi", label="Net Migration $ Flow",
        domain="demographics", grain="county", default_on=False,
        vintage="2018–2021", coverage_note="full county",
        unit="USD net AGI",
        description="IRS SOI migration — net AGI flowing in or out of each county.",
        builder=_build_irs_migration_net,
    ),
    # Economy
    "econ_federal_dollars": LayerSpec(
        id="econ_federal_dollars", label="Federal Contract Dollars (FY)",
        domain="economy", grain="county", default_on=False,
        vintage="FY2024 contracts",
        coverage_note="2,750 counties via USAspending spending_by_geography (SPEC_071)",
        unit="USD",
        description="Federal prime contract obligations to recipients with place "
                    "of performance in the county, latest fiscal year.",
        builder=_build_federal_dollars,
    ),
    "econ_cbp_establishments_state": LayerSpec(
        id="econ_cbp_establishments_state", label="Business Establishments (state)",
        domain="economy", grain="state", default_on=False,
        vintage="2022", coverage_note="state grain (county partial — PLAN_067 SPEC_073)",
        unit="establishments",
        description="Total business establishments per state, Census CBP, NAICS-2 totals.",
        builder=_build_cbp_industry_density_state,
    ),
    # Finance
    "finance_fdic_county_deposits": LayerSpec(
        id="finance_fdic_county_deposits", label="Bank Deposits per County (avg)",
        domain="finance", grain="county", default_on=False,
        vintage="latest quarterly call report",
        coverage_note="county via cert→fdic_institutions.stcnty join",
        unit="USD",
        description="Average bank deposits per institution, by county, latest "
                    "FDIC call report. Joins `cert`→`fdic_institutions.stcnty`.",
        builder=_build_fdic_county_deposits,
    ),
    # Real Estate
    "realestate_building_permits": LayerSpec(
        id="realestate_building_permits", label="Building Permits",
        domain="real_estate", grain="county", default_on=False,
        vintage="rolling", coverage_note="full county (3,024)",
        unit="permits",
        description="Building-permit count per county.",
        builder=_build_building_permits_county,
    ),
    # Trade
    "trade_exports_state": LayerSpec(
        id="trade_exports_state", label="State Exports (latest year)",
        domain="trade", grain="state", default_on=False,
        vintage="2019–2024", coverage_note="46 states",
        unit="USD",
        description="Total monthly export value summed for the latest year, by state.",
        builder=_build_trade_exports_state,
    ),
}

# Honest cuts — these are NOT registered, by design. The tests assert it.
EXCLUDED_BY_DESIGN = {
    # SPEC_070 (2026-05-23): ACS county wealth is implemented as
    # `demo_acs_median_income` (table `acs5_county_2023_b19013`).
    # SPEC_071 (2026-05-23): the `usaspending_awards` stub is now superseded
    # by `usaspending_county_fy_totals` (county-aggregate via
    # spending_by_geography) — surfaced as `econ_federal_dollars`. The
    # legacy stub table is left intact but unused.
}


# ── Public lookups ──────────────────────────────────────────────────────────

def get_layer(layer_id: str) -> LayerSpec:
    """Return the layer spec; raises KeyError if absent (API maps to 404)."""
    if layer_id not in LAYERS:
        raise KeyError(f"Unknown layer: {layer_id!r}")
    return LAYERS[layer_id]


def list_layers_by_domain() -> Dict[str, List[Dict[str, Any]]]:
    """Return layers grouped by domain, with deterministic ordering."""
    out: Dict[str, List[Dict[str, Any]]] = {}
    for layer_id in sorted(LAYERS):
        spec = LAYERS[layer_id]
        out.setdefault(spec.domain, []).append({
            "id": spec.id,
            "label": spec.label,
            "grain": spec.grain,
            "default_on": spec.default_on,
            "vintage": spec.vintage,
            "coverage_note": spec.coverage_note,
            "unit": spec.unit,
            "description": spec.description,
        })
    return {d: out[d] for d in sorted(out)}


def build_layer(db: Session, layer_id: str) -> LayerResult:
    """Build a layer's data. Wraps builder failure so the registry stays usable."""
    spec = get_layer(layer_id)
    try:
        return spec.builder(db)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Layer %s build failed: %s", layer_id, exc)
        try:
            db.rollback()
        except Exception:  # noqa: BLE001
            pass
        return LayerResult(layer_id=layer_id, grain=spec.grain,
                           values={} if spec.grain != "point" else None,
                           features=[] if spec.grain == "point" else None,
                           legend={"error": "build failed"},
                           provenance=[])


def place_aggregate(db: Session, geo_id: str) -> Dict[str, Any]:
    """For every registered choropleth layer, return its value at `geo_id`.
    Powers the click-a-place drill-down — the layer-aggregate sibling of
    SPEC_064's `/explore` cards."""
    out: Dict[str, Dict[str, Any]] = {}
    for layer_id, spec in LAYERS.items():
        if spec.grain == "point":
            continue
        # Only ask the layer if its grain could possibly contain geo_id
        if spec.grain == "county" and len(geo_id) != 5:
            continue
        if spec.grain == "state" and len(geo_id) != 2:
            continue
        result = build_layer(db, layer_id)
        if result.values and geo_id in result.values:
            out[layer_id] = {
                "label": spec.label,
                "domain": spec.domain,
                "grain": spec.grain,
                "value": result.values[geo_id],
                "unit": spec.unit,
                "vintage": spec.vintage,
            }
    return out
