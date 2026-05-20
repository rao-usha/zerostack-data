"""
Atlas insight-card builders — SPEC_064.

Reuses the SPEC_061 `MarketIntelligencePackTemplate.gather_data` layer as the
data source — every populated report section becomes an insight card. This
is the explicit guidance in the SPEC_061 2026-05-20 revision: "reuse this
template's gather layer so every populated section can become an insight
card."

`build_cards(db, resolved)` returns `(cards, coverage_notes, datasets_used)`:
  * cards          — list[Card], skip-on-empty (no card for an empty section)
  * coverage_notes — list[str], one per section that had no data
  * datasets_used  — sorted list of every dataset table that produced a row

The FCC connectivity card is a fresh query (the MIP report didn't have a
broadband section); everything else maps from gather_data output.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.services.atlas.types import (
    CONFIDENCE_HIGH, CONFIDENCE_LOW, CONFIDENCE_MEDIUM,
    Card, Metric, Provenance,
)
from app.services.atlas.resolver import ResolvedEntities

logger = logging.getLogger(__name__)


# ── helpers ──────────────────────────────────────────────────────────────────

def _fmt_int(v) -> str:
    try:
        return f"{int(v):,}"
    except (TypeError, ValueError):
        return "—"


def _fmt_usd(v) -> str:
    try:
        v = float(v)
    except (TypeError, ValueError):
        return "—"
    if abs(v) >= 1e9:
        return f"${v / 1e9:,.1f}B"
    if abs(v) >= 1e6:
        return f"${v / 1e6:,.1f}M"
    if abs(v) >= 1e3:
        return f"${v / 1e3:,.0f}K"
    return f"${v:,.0f}"


def _provenance_for(gather: Dict[str, Any], section: str) -> List[Provenance]:
    """Pull the gather_data `_provenance` rows belonging to one section."""
    out: List[Provenance] = []
    for p in (gather.get("_provenance") or []):
        if p.get("section") == section:
            out.append(Provenance(
                dataset=p.get("table", "—"),
                description=f"{section} source",
                rows_used=int(p.get("rows") or 0),
            ))
    return out


# ─────────────────────────────────────────────────────────────────────────────
# Individual card builders — each returns Card or None (skip-on-empty)
# ─────────────────────────────────────────────────────────────────────────────

def _card_industry_footprint(gather: Dict[str, Any]) -> Optional[Card]:
    d = gather.get("structural_density") or {}
    if not d or not d.get("establishments"):
        return None
    metrics = [
        Metric("Establishments", _fmt_int(d.get("establishments"))),
        Metric("Employees", _fmt_int(d.get("employees"))),
        Metric("Annual payroll", _fmt_usd((d.get("annual_payroll_thousands") or 0) * 1000)),
    ]
    if d.get("hhi_avg") is not None:
        metrics.append(Metric("Concentration (HHI)", f"{d['hhi_avg']:.3f}"))
    fallback = gather.get("_fallback_used")
    conf = CONFIDENCE_MEDIUM if fallback else CONFIDENCE_HIGH
    return Card(
        id="industry_footprint",
        title="Industry Footprint",
        summary=(
            f"{_fmt_int(d.get('establishments'))} establishments employing "
            f"{_fmt_int(d.get('employees'))} people in this sector × geography."
        ),
        metrics=metrics,
        why_it_matters=(
            "Establishment density and concentration tell a deal sourcer "
            "whether this is a fragmented roll-up market or a consolidated one."
        ),
        datasets_used=[d.get("source", "census_cbp")],
        confidence=conf,
        coverage={
            "source": d.get("source"),
            "fallback_used": bool(fallback),
            "msa_county_coverage_pct": gather.get("_msa_county_coverage_pct"),
        },
        provenance=_provenance_for(gather, "structural_density"),
        links=[],
    )


def _card_local_wealth_demand(gather: Dict[str, Any]) -> Optional[Card]:
    d = gather.get("demand_context") or {}
    mig = gather.get("migration_economy") or {}
    if not d and not mig:
        return None
    metrics: List[Metric] = []
    if d.get("acs_median_income"):
        rows = d["acs_median_income"]
        avg = sum(r.get("median_hh_income") or 0 for r in rows) / max(len(rows), 1)
        metrics.append(Metric("Median HH income (state avg)", _fmt_usd(avg)))
    soi = d.get("irs_soi_county") or {}
    if soi.get("total_agi"):
        metrics.append(Metric("Total AGI (in-scope counties)", _fmt_usd(soi["total_agi"])))
    if soi.get("total_business_income"):
        metrics.append(Metric("Business income", _fmt_usd(soi["total_business_income"])))
    if not metrics:
        return None
    ds = ["acs5_2023_b19013", "irs_soi_county_income"]
    prov = _provenance_for(gather, "demand_context") + _provenance_for(gather, "migration_economy")
    return Card(
        id="local_wealth_demand",
        title="Local Wealth & Demand",
        summary="Income, tax-return wealth and business-income signal for the market.",
        metrics=metrics,
        why_it_matters=(
            "Local wealth and business-income density indicate the demand pool "
            "an operator in this sector can actually sell into."
        ),
        datasets_used=ds,
        confidence=CONFIDENCE_MEDIUM,
        coverage={"has_acs": bool(d.get("acs_median_income")),
                  "has_irs_county": bool(soi)},
        provenance=prov,
        links=[],
    )


def _card_risk_context(gather: Dict[str, Any]) -> Optional[Card]:
    r = gather.get("risk_profile") or {}
    if not r:
        return None
    metrics: List[Metric] = []
    decls = r.get("fema_declarations") or []
    if decls:
        total = sum(d.get("n") or 0 for d in decls)
        top = decls[0]
        metrics.append(Metric("FEMA disaster declarations", _fmt_int(total),
                              detail=f"most frequent: {top.get('incident_type', '—')}"))
    nri = r.get("nri_summary") or {}
    if nri.get("risk_score_avg") is not None:
        metrics.append(Metric("National Risk Index (avg)", f"{nri['risk_score_avg']}"))
        if nri.get("flood_score_avg") is not None:
            metrics.append(Metric("Flood risk (avg)", f"{nri['flood_score_avg']}"))
    if not metrics:
        return None
    return Card(
        id="risk_context",
        title="Risk Context",
        summary="Natural-hazard exposure and federal disaster history for the market.",
        metrics=metrics,
        why_it_matters=(
            "Hazard exposure drives insurance cost, capex resilience spend, and "
            "business-interruption risk for any operator or asset in the market."
        ),
        datasets_used=["fema_disaster_declarations", "national_risk_index"],
        confidence=CONFIDENCE_HIGH if (decls and nri) else CONFIDENCE_MEDIUM,
        coverage={"has_fema": bool(decls), "has_nri": bool(nri)},
        provenance=_provenance_for(gather, "risk_profile"),
        links=[],
    )


def _card_macro_rates(gather: Dict[str, Any]) -> Optional[Card]:
    op = gather.get("operating_environment") or {}
    if not op:
        return None
    metrics: List[Metric] = []
    for rate in (op.get("rates") or []):
        metrics.append(Metric(rate.get("series_id", "—"), f"{rate.get('value', '—')}",
                              detail=f"as of {str(rate.get('date'))[:10]}"))
    ip = op.get("industrial_production") or {}
    if ip.get("value") is not None:
        metrics.append(Metric("Industrial production", f"{ip['value']}",
                              detail=f"as of {str(ip.get('date'))[:10]}"))
    if not metrics:
        return None
    return Card(
        id="macro_rates",
        title="Macro & Rates",
        summary="Interest-rate and industrial-production backdrop for the sector.",
        metrics=metrics,
        why_it_matters=(
            "Rate environment and industrial-production trend set the financing "
            "cost and demand cycle every deal in this sector underwrites against."
        ),
        datasets_used=["fred_interest_rates", "fred_industrial_production"],
        confidence=CONFIDENCE_HIGH,
        coverage={"rate_series": len(op.get("rates") or [])},
        provenance=_provenance_for(gather, "operating_environment"),
        links=[],
    )


def _card_logistics_trade(gather: Dict[str, Any]) -> Optional[Card]:
    t = gather.get("trade_exposure") or {}
    exports = t.get("state_exports") or []
    if not exports:
        return None
    total = sum(r.get("total_export_value_ytd") or 0 for r in exports)
    metrics = [
        Metric("State exports (YTD)", _fmt_usd(total)),
        Metric("Trade partners", _fmt_int(sum(r.get("export_partner_count") or 0 for r in exports))),
        Metric("HS codes traded", _fmt_int(sum(r.get("hs_code_count") or 0 for r in exports))),
    ]
    return Card(
        id="logistics_trade",
        title="Logistics & Trade",
        summary="State-level export economy the sector's geography sits inside.",
        metrics=metrics,
        why_it_matters=(
            "Trade intensity signals supply-chain exposure and the export "
            "demand a goods-producing operator in this market can reach."
        ),
        datasets_used=["us_trade_exports_state"],
        confidence=CONFIDENCE_MEDIUM,
        coverage={"states": len(exports)},
        provenance=_provenance_for(gather, "trade_exposure"),
        links=[],
    )


def _card_infrastructure(gather: Dict[str, Any]) -> Optional[Card]:
    i = gather.get("infra_proximity") or {}
    if not i:
        return None
    labels = [("power_plants", "Power plants"), ("transmission_lines", "Transmission lines"),
              ("rail_segments", "Rail segments"), ("airports", "Airports"),
              ("data_centers", "Data centers")]
    metrics = [Metric(label, _fmt_int(i[key])) for key, label in labels if i.get(key)]
    if not metrics:
        return None
    return Card(
        id="infrastructure",
        title="Infrastructure Proximity",
        summary="Power, transmission, rail, air and data-center assets in the state(s).",
        metrics=metrics,
        why_it_matters=(
            "Infrastructure density bounds where an operator can physically "
            "expand — power and logistics access gate industrial growth."
        ),
        datasets_used=["power_plant", "transmission_line", "rail_line",
                       "airport", "data_center_facility"],
        confidence=CONFIDENCE_MEDIUM,
        coverage={"asset_classes_present": len(metrics)},
        provenance=_provenance_for(gather, "infra_proximity"),
        links=[],
    )


def _card_named_operators(gather: Dict[str, Any]) -> Optional[Card]:
    np_ = gather.get("named_privates") or {}
    pub = gather.get("public_cos") or {}
    items = np_.get("items") or []
    pub_items = pub.get("items") or []
    if not items and not pub_items:
        return None
    metrics = [
        Metric("Named private operators", _fmt_int(len(items)),
               detail="EPA-regulated facilities"),
        Metric("Public-co operators in scope", _fmt_int(len(pub_items)),
               detail="SEC filers"),
    ]
    return Card(
        id="named_operators",
        title="Named Operators",
        summary=(
            f"{len(items)} named private operators (regulated-facility data) and "
            f"{len(pub_items)} public-company operators visible in this market."
        ),
        metrics=metrics,
        why_it_matters=(
            "A starting target list — who is actually operating here, and how "
            "much of the market the visible names represent."
        ),
        datasets_used=["epa_echo_facilities", "sec_company_metadata"],
        confidence=CONFIDENCE_MEDIUM if items else CONFIDENCE_LOW,
        coverage={"named_private_count": len(items),
                  "public_co_count": len(pub_items),
                  "note": np_.get("note")},
        provenance=(_provenance_for(gather, "named_privates")
                    + _provenance_for(gather, "public_cos")),
        links=[],
    )


def _card_connectivity(db: Session, resolved: ResolvedEntities) -> Optional[Card]:
    """Fresh query — the MIP report had no FCC broadband section.

    fcc_broadband_coverage is keyed by (geography_type, geography_id). For
    county rows the geography_id is a 5-digit county FIPS; we aggregate over
    the MSA's constituent counties.
    """
    counties = resolved.geographies or []
    if not counties:
        return None
    try:
        rows = db.execute(text("""
            SELECT count(DISTINCT provider_id)        AS providers,
                   count(DISTINCT technology_code)    AS technologies,
                   max(max_advertised_down_mbps)      AS max_down,
                   count(*)                           AS records
            FROM fcc_broadband_coverage
            WHERE geography_id = ANY(:geos)
        """), {"geos": counties}).mappings().first()
    except Exception as exc:  # noqa: BLE001
        logger.warning("Atlas connectivity card query failed: %s", exc)
        try:
            db.rollback()  # un-poison the session — see MIP _safe_query note
        except Exception:  # noqa: BLE001
            pass
        return None
    if not rows or not rows.get("records"):
        return None
    metrics = [
        Metric("Broadband providers", _fmt_int(rows.get("providers"))),
        Metric("Technologies", _fmt_int(rows.get("technologies"))),
    ]
    if rows.get("max_down"):
        metrics.append(Metric("Top advertised speed", f"{rows['max_down']:.0f} Mbps down"))
    return Card(
        id="connectivity",
        title="Connectivity",
        summary="Broadband provider and technology coverage across the market's counties.",
        metrics=metrics,
        why_it_matters=(
            "Connectivity is table-stakes infrastructure — thin broadband "
            "coverage constrains any operator's back-office and remote work."
        ),
        datasets_used=["fcc_broadband_coverage"],
        confidence=CONFIDENCE_MEDIUM,
        coverage={"records": int(rows.get("records") or 0),
                  "counties_queried": len(counties)},
        provenance=[Provenance("fcc_broadband_coverage", "FCC broadband coverage",
                               int(rows.get("records") or 0))],
        links=[],
    )


# ─────────────────────────────────────────────────────────────────────────────
# Orchestration
# ─────────────────────────────────────────────────────────────────────────────

# (card_id, human label) — used to generate coverage notes for skipped cards.
_EXPECTED_CARDS = [
    ("industry_footprint", "Industry Footprint"),
    ("local_wealth_demand", "Local Wealth & Demand"),
    ("risk_context", "Risk Context"),
    ("connectivity", "Connectivity"),
    ("logistics_trade", "Logistics & Trade"),
    ("macro_rates", "Macro & Rates"),
    ("infrastructure", "Infrastructure Proximity"),
    ("named_operators", "Named Operators"),
]


def build_cards(
    db: Session,
    resolved: ResolvedEntities,
) -> Tuple[List[Card], List[str], List[str]]:
    """Build all Atlas insight cards for a resolved query.

    Returns (cards, coverage_notes, datasets_used). Cards are skip-on-empty;
    every expected-but-missing card produces a coverage note instead.
    """
    cards: List[Card] = []
    coverage_notes: List[str] = []

    # The rich cards need both a NAICS-4 and an MSA — they go through the
    # SPEC_061 gather layer, which requires both.
    gather: Dict[str, Any] = {}
    if resolved.naics and resolved.msa:
        try:
            from app.reports.templates.market_intelligence_pack import (
                MarketIntelligencePackTemplate,
            )
            gather = MarketIntelligencePackTemplate().gather_data(db, {
                "naics_code": resolved.naics["code"],
                "geography_mode": "msa",
                "msa_code": resolved.msa["code"],
            })
        except Exception as exc:  # noqa: BLE001
            logger.warning("Atlas: gather_data failed: %s", exc)
            coverage_notes.append(
                "Cross-dataset sections could not be assembled — the data "
                "backend was unreachable for this exploration."
            )
    else:
        missing = "industry" if not resolved.naics else "metro area"
        coverage_notes.append(
            f"No {missing} resolved — only datasets that don't need it are shown."
        )

    # Build the gather-backed cards.
    builders = [
        _card_industry_footprint, _card_local_wealth_demand, _card_risk_context,
        _card_macro_rates, _card_logistics_trade, _card_infrastructure,
        _card_named_operators,
    ]
    for builder in builders:
        try:
            card = builder(gather) if gather else None
        except Exception as exc:  # noqa: BLE001
            logger.warning("Atlas card builder %s failed: %s", builder.__name__, exc)
            card = None
        if card:
            cards.append(card)

    # Connectivity card — fresh query, independent of gather_data.
    try:
        conn_card = _card_connectivity(db, resolved)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Atlas connectivity card failed: %s", exc)
        conn_card = None
    if conn_card:
        cards.append(conn_card)

    # Coverage notes for any expected card that didn't render.
    present = {c.id for c in cards}
    for card_id, label in _EXPECTED_CARDS:
        if card_id not in present:
            coverage_notes.append(
                f"{label}: no usable data for this sector × geography — card omitted."
            )

    # Datasets actually used.
    datasets: set = set()
    for c in cards:
        datasets.update(c.datasets_used)
    return cards, coverage_notes, sorted(datasets)
