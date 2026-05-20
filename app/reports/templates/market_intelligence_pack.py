"""
Market Intelligence Pack Report Template — SPEC_061 / PLAN_065.

Sector × geography landscape map produced from governed public data.
Buyer = deal sourcer ("find me deals I'm not yet looking at"), priced at $2.5K
for a single map. Built from Census CBP, ACS, IRS SOI, SEC, FDIC, FRED,
Treasury, FEMA, EPA, FCC, BTS, USDA, USAspending, CFTC + the site-intel
infrastructure tables (power_plant / transmission_line / rail_line / airport /
data_center_facility).

Inputs (params):
  * naics_code        4-digit NAICS, validated against taxonomies.load_naics
  * geography_mode    'msa' | 'state' | 'multi_county'
  * msa_code          required if mode='msa' — CBSA code from taxonomies.load_msa
  * state_fips        required if mode='state' — 2-digit state FIPS
  * county_fips_list  required if mode='multi_county' — list of 5-digit FIPS
  * client_note       optional free text shown on cover
  * named_operators_limit  optional, default 25

Critical design rules (per SPEC_061 acceptance criteria):
  * All section renderers SKIP-ON-EMPTY — if a query returns nothing, the
    section is omitted entirely (no header, no "no data" placeholder).
  * All SQL is parameterized; never f-string SQL.
  * §3 structural-density does NAICS-6 → NAICS-4 rollup from census_cbp county
    rows; falls back to census_business_patterns (state grain) for the 7 sparse
    MSAs (<50% county coverage) or when geography_mode != 'msa'.
  * §12 (named privates) currently stubs out — the real curator ships in
    SPEC_062. Template accepts a curator callable to ease that swap.
  * Deterministic section ordering (test T6): SAME inputs → byte-identical HTML.
  * render_excel raises NotImplementedError (T9).
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.reports.design_system import (
    BLUE, CHART_COLORS, GRAY, GREEN, ORANGE, RED, TEAL,
    build_bar_fallback, build_horizontal_bar_config, build_line_chart_config,
    callout, chart_container, chart_init_js, data_table,
    html_document, kpi_card, kpi_strip, page_footer, page_header,
    section_end, section_start, toc,
)
from app.services.diligence.taxonomies import (
    load_naics, load_msa,
    msa_counties, msa_title, naics_label, naics_parents, naics_to_sic,
    state_counties,
)

logger = logging.getLogger(__name__)

# Thresholds used by §3 rollup decision logic
SPARSE_MSA_COUNTY_COVERAGE_PCT = 50.0
MAX_PUBLIC_COS = 15
MAX_NAMED_PRIVATES = 25
DEFAULT_NAICS_DEPTH = 4   # we always validate inputs at NAICS-4


# ── module-level formatting helpers ──────────────────────────────────────────

def _fmt_int(val: Optional[int]) -> str:
    if val is None:
        return "—"
    return f"{int(val):,}"


def _fmt_pct(val: Optional[float], digits: int = 1) -> str:
    if val is None:
        return "—"
    return f"{val:.{digits}f}%"


def _fmt_usd(val: Optional[float], unit: str = "") -> str:
    if val is None:
        return "—"
    v = float(val)
    if unit == "" and abs(v) >= 1_000_000_000:
        return f"${v / 1e9:,.1f}B"
    if unit == "" and abs(v) >= 1_000_000:
        return f"${v / 1e6:,.1f}M"
    if unit == "" and abs(v) >= 1_000:
        return f"${v / 1e3:,.0f}K"
    return f"${v:,.0f}"


def _fmt_date(val) -> str:
    if val is None:
        return "—"
    if isinstance(val, str):
        return val[:10]
    try:
        return val.strftime("%Y-%m-%d")
    except Exception:  # noqa: BLE001
        return str(val)


# ── default named-privates curator stub (SPEC_062 replaces) ──────────────────

def _default_named_operators_curator(db: Session, naics_4: str, counties: List[str],
                                     state_abbrs: List[str], limit: int) -> Dict[str, Any]:
    """Stub curator — SPEC_062 ships the real implementation.

    Returns the SPEC_061 acceptance-criteria shape: {"items": [...], "note": str}.
    For sectors in regulated industries we DO populate from EPA ECHO since that
    table is already in cloud and the join is simple — even the stub can ship
    something useful.
    """
    if not counties or not state_abbrs:
        return {"items": [], "note": "Geography input missing."}

    # ECHO carries naics_codes as a JSONB array; we filter for any element
    # starting with the chosen NAICS-4 prefix.
    rows = db.execute(text("""
        SELECT facility_name, state, county, violation_count, inspection_count,
               compliance_status, last_inspection_date
        FROM epa_echo_facilities
        WHERE state = ANY(:states)
          AND EXISTS (
              SELECT 1 FROM jsonb_array_elements_text(naics_codes) AS code
              WHERE code LIKE :prefix
          )
        ORDER BY COALESCE(violation_count, 0) DESC, facility_name
        LIMIT :limit
    """), {
        "states": state_abbrs,
        "prefix": f"{naics_4}%",
        "limit": limit,
    }).mappings().all()

    items = [dict(r) for r in rows]
    if not items:
        note = (
            f"No EPA-regulated facilities match NAICS {naics_4} in the chosen "
            f"geography. Sectors outside EPA's regulated universe (consumer / "
            f"professional / certain healthcare services) need supplemental "
            f"sources — see SPEC_062."
        )
    else:
        note = (
            f"Showing {len(items)} EPA-ECHO-sourced operators (regulated "
            f"facilities only). Full curator with NPPES + USAspending blends "
            f"lands in SPEC_062."
        )
    return {"items": items, "note": note, "source": "epa_echo_facilities"}


# ─────────────────────────────────────────────────────────────────────────────
# Template
# ─────────────────────────────────────────────────────────────────────────────

class MarketIntelligencePackTemplate:
    """14-section sector × MSA market-intelligence map (PLAN_065 deliverable)."""

    name = "market_intelligence_pack"
    description = (
        "Sector × geography landscape map (NAICS × MSA / state) assembled from "
        "Census CBP, ACS, IRS SOI, SEC, FDIC, FRED, Treasury, FEMA, EPA, FCC, "
        "BTS, USAspending and regulated-facility data. Built for deal sourcers "
        "and operating-thesis owners — answers \"where is the herd\" not "
        "\"is this target a good buy\". PLAN_065."
    )

    def __init__(self, named_operators_curator: Optional[Callable] = None):
        # Injected so SPEC_062's real curator can swap in without template change.
        self._curator = named_operators_curator or _default_named_operators_curator

    # ── public API ────────────────────────────────────────────────────────────

    def gather_data(self, db: Session, params: Dict[str, Any]) -> Dict[str, Any]:
        """Validate inputs, query cloud, return a flat per-section data dict.

        Any individual section query failure is logged and the section's slice
        becomes empty (skip-on-empty in render_html). The whole report still
        renders; sections with data show, sections without data don't.
        """
        # --- 1. validate + resolve geography ------------------------------------
        naics_code = self._validate_naics(params)
        geo_mode, msa_code, state_fips, counties = self._resolve_geography(params)
        state_abbrs = self._derive_state_abbrs(msa_code, state_fips)
        msa_label = msa_title(msa_code) if msa_code else None
        provenance: List[Dict[str, Any]] = []

        # --- 2. structural density (§3) — the headline ---------------------------
        density, fallback_used, coverage_pct = self._gather_structural_density(
            db, naics_code, counties, state_fips, geo_mode, provenance
        )

        # --- 3. demand / wealth / labor (§4) ------------------------------------
        demand = self._gather_demand_context(db, counties, state_abbrs, provenance)

        # --- 4. migration economy (§5) ------------------------------------------
        migration = self._gather_migration_economy(db, counties, state_abbrs, provenance)

        # --- 5. operating environment (§6) — macro context, no per-MSA banks ----
        operating = self._gather_operating_environment(db, provenance)

        # --- 6. risk profile (§7) -----------------------------------------------
        risk = self._gather_risk_profile(db, counties, state_abbrs, provenance)

        # --- 7. infrastructure proximity (§8) -----------------------------------
        infra = self._gather_infra_proximity(db, state_abbrs, provenance)

        # --- 8. trade exposure (§9) ---------------------------------------------
        trade = self._gather_trade_exposure(db, state_abbrs, provenance)

        # --- 9. federal $ flow (§10) --------------------------------------------
        federal = self._gather_federal_spending(db, naics_code, state_abbrs, provenance)

        # --- 10. public-co operators (§11) --------------------------------------
        public_cos = self._gather_public_cos(db, naics_code, state_abbrs, provenance)

        # --- 11. named private operators (§12) ----------------------------------
        named_privates = self._curator(
            db, naics_code, counties, state_abbrs,
            int(params.get("named_operators_limit", MAX_NAMED_PRIVATES)),
        )
        if named_privates.get("items"):
            provenance.append({
                "section": "named_privates",
                "table": named_privates.get("source", "various"),
                "rows": len(named_privates["items"]),
            })

        # --- 12. diligence questions (§13) — synthesized from above -------------
        questions = self._synthesize_diligence_questions(
            naics_code, msa_label, density, demand, risk, public_cos,
            named_privates, fallback_used, coverage_pct,
        )

        return {
            "input": {
                "naics_code": naics_code,
                "naics_label": naics_label(naics_code),
                "naics_parents": [
                    {"code": p, "label": naics_label(p)} for p in naics_parents(naics_code)
                ],
                "geography_mode": geo_mode,
                "msa_code": msa_code,
                "msa_title": msa_label,
                "state_fips": state_fips,
                "state_abbrs": state_abbrs,
                "counties": counties,
                "client_note": params.get("client_note") or "",
                "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            },
            "structural_density": density,
            "demand_context": demand,
            "migration_economy": migration,
            "operating_environment": operating,
            "risk_profile": risk,
            "infra_proximity": infra,
            "trade_exposure": trade,
            "federal_spending": federal,
            "public_cos": public_cos,
            "named_privates": named_privates,
            "diligence_questions": questions,
            "_fallback_used": fallback_used,
            "_msa_county_coverage_pct": coverage_pct,
            "_provenance": provenance,
        }

    def render_html(self, data: Dict[str, Any]) -> str:
        """Render the 14-section HTML doc. Sections without data skip silently."""
        inp = data.get("input") or {}
        body = ""
        charts_js = ""

        # ── Header ──────────────────────────────────────────────────────────
        title = "Sector × Market Intelligence Pack"
        subtitle_parts = []
        if inp.get("naics_label") and inp.get("naics_code"):
            subtitle_parts.append(f"NAICS {inp['naics_code']} — {inp['naics_label']}")
        if inp.get("msa_title"):
            subtitle_parts.append(inp["msa_title"])
        elif inp.get("state_fips"):
            subtitle_parts.append(f"State FIPS {inp['state_fips']}")
        gen_dt = (inp.get("generated_at_utc") or "")[:10]
        if gen_dt:
            subtitle_parts.append(gen_dt)
        subtitle = " · ".join(subtitle_parts) if subtitle_parts else None
        body += page_header(title=title, subtitle=subtitle, badge="public-data intelligence")

        # ── TOC (built from sections that actually rendered) ────────────────
        toc_items = self._build_toc(data)
        if toc_items:
            body += toc(toc_items)

        # ── Sections ────────────────────────────────────────────────────────
        body += self._render_section_1_exec_summary(data)
        body += self._render_section_2_definition(data)

        # §3 needs a chart — collect its JS for the page-level charts_js
        section3_html, section3_js = self._render_section_3_structural_density(data)
        body += section3_html
        charts_js += section3_js

        body += self._render_section_4_demand_context(data)
        body += self._render_section_5_migration_economy(data)
        body += self._render_section_6_operating_environment(data)
        body += self._render_section_7_risk_profile(data)
        body += self._render_section_8_infra_proximity(data)
        body += self._render_section_9_trade_exposure(data)
        body += self._render_section_10_federal_spending(data)
        body += self._render_section_11_public_cos(data)
        body += self._render_section_12_named_privates(data)
        body += self._render_section_13_diligence_questions(data)
        body += self._render_section_14_provenance(data)

        # ── Footer ──────────────────────────────────────────────────────────
        body += page_footer(
            notes=[
                "This report assembles only governed public-data sources — Census, "
                "ACS, IRS SOI, SEC, FDIC, FRED, Treasury, FEMA, EPA, FCC, BTS, USAspending. "
                "No PII, no scraping, no private-company database depth.",
                "Sections that returned no data are silently omitted — absence of a "
                "section indicates absence of data for the chosen geography, not "
                "absence of the phenomenon.",
                "PLAN_065 / SPEC_061.",
            ],
            generated_line=f"Generated {inp.get('generated_at_utc', '—')[:19]} UTC | Nexdata Market Intelligence Pack",
        )

        return html_document(
            title=f"{title} | {' · '.join(subtitle_parts)}" if subtitle_parts else title,
            body_content=body,
            charts_js=charts_js,
        )

    def render_excel(self, data: Dict[str, Any]) -> bytes:
        raise NotImplementedError(
            "MarketIntelligencePackTemplate is HTML-only for v1. "
            "Excel export is a future enhancement (PLAN_066+)."
        )

    # ── input validation + geography resolution ──────────────────────────────

    def _validate_naics(self, params: Dict[str, Any]) -> str:
        code = params.get("naics_code")
        if not code:
            raise ValueError("Missing required param: naics_code")
        code = str(code)
        naics = load_naics()
        if code not in naics:
            raise ValueError(f"Unknown NAICS code: {code!r}")
        if naics[code].digits != DEFAULT_NAICS_DEPTH:
            raise ValueError(
                f"naics_code must be 4-digit (got {naics[code].digits}-digit {code!r})"
            )
        return code

    def _resolve_geography(self, params: Dict[str, Any]):
        mode = params.get("geography_mode") or "msa"
        if mode == "msa":
            msa_code = str(params.get("msa_code") or "")
            if not msa_code:
                raise ValueError("geography_mode='msa' requires msa_code")
            if msa_code not in load_msa():
                raise ValueError(f"Unknown MSA CBSA code: {msa_code!r}")
            counties = msa_counties(msa_code)
            return mode, msa_code, None, counties
        if mode == "state":
            state_fips = str(params.get("state_fips") or "").zfill(2)
            if not state_fips or len(state_fips) != 2:
                raise ValueError("geography_mode='state' requires 2-digit state_fips")
            counties = state_counties(state_fips)  # may raise — that's correct
            return mode, None, state_fips, counties
        if mode == "multi_county":
            counties = list(params.get("county_fips_list") or [])
            if not counties:
                raise ValueError("geography_mode='multi_county' requires county_fips_list")
            return mode, None, None, counties
        raise ValueError(f"Invalid geography_mode: {mode!r}")

    def _derive_state_abbrs(self, msa_code, state_fips) -> List[str]:
        if msa_code:
            rec = load_msa().get(msa_code)
            return list(rec.state_abbrs) if rec else []
        if state_fips:
            # Map 2-digit FIPS → state abbr via our MSA dict (best-effort).
            for rec in load_msa().values():
                for fips in rec.county_fips_list:
                    if fips.startswith(state_fips) and rec.state_abbrs:
                        return [rec.state_abbrs[0]]
            return []
        return []

    # ── §3 structural density (the centerpiece) ───────────────────────────────

    def _gather_structural_density(
        self, db, naics_4, counties, state_fips, geo_mode, provenance
    ):
        """NAICS-6 → NAICS-4 rollup from census_cbp county rows. Falls back to
        census_business_patterns (state grain) when MSA county coverage is
        sparse, or when geography_mode is not 'msa'."""
        coverage_pct = 100.0
        fallback = False

        if geo_mode == "msa" and counties:
            # Coverage check: how many of the MSA's counties have any census_cbp row?
            cov = self._safe_scalar(db, """
                SELECT count(DISTINCT county_fips) FROM census_cbp
                WHERE geo_level = 'county' AND county_fips = ANY(:counties)
            """, {"counties": counties})
            coverage_pct = (100.0 * (cov or 0) / len(counties)) if counties else 0.0
            if coverage_pct < SPARSE_MSA_COUNTY_COVERAGE_PCT:
                fallback = True
        else:
            fallback = True

        if not fallback:
            rows = self._safe_query(db, """
                SELECT SUM(establishments) AS establishments,
                       SUM(employees)      AS employees,
                       SUM(annual_payroll_thousands) AS annual_payroll_thousands,
                       SUM(estab_1_4)      AS estab_1_4,
                       SUM(estab_5_9)      AS estab_5_9,
                       SUM(estab_10_19)    AS estab_10_19,
                       SUM(estab_20_49)    AS estab_20_49,
                       SUM(estab_50_99)    AS estab_50_99,
                       SUM(estab_100_249)  AS estab_100_249,
                       SUM(estab_250_plus) AS estab_250_plus,
                       AVG(hhi)            AS hhi_avg,
                       AVG(small_biz_pct)  AS small_biz_pct_avg,
                       COUNT(DISTINCT county_fips) AS counties_rolled_up
                FROM census_cbp
                WHERE geo_level = 'county'
                  AND county_fips = ANY(:counties)
                  AND naics_code LIKE :prefix
            """, {"counties": counties, "prefix": f"{naics_4}%"})
            row = rows[0] if rows else {}
            if not row or not row.get("establishments"):
                fallback = True
            else:
                provenance.append({"section": "structural_density",
                                   "table": "census_cbp",
                                   "rows": int(row.get("counties_rolled_up") or 0)})
                size_dist = [
                    {"bucket": "1-4",     "n": int(row.get("estab_1_4")     or 0)},
                    {"bucket": "5-9",     "n": int(row.get("estab_5_9")     or 0)},
                    {"bucket": "10-19",   "n": int(row.get("estab_10_19")   or 0)},
                    {"bucket": "20-49",   "n": int(row.get("estab_20_49")   or 0)},
                    {"bucket": "50-99",   "n": int(row.get("estab_50_99")   or 0)},
                    {"bucket": "100-249", "n": int(row.get("estab_100_249") or 0)},
                    {"bucket": "250+",    "n": int(row.get("estab_250_plus") or 0)},
                ]
                return {
                    "establishments": int(row.get("establishments") or 0),
                    "employees":      int(row.get("employees") or 0),
                    "annual_payroll_thousands": int(row.get("annual_payroll_thousands") or 0),
                    "hhi_avg":          float(row.get("hhi_avg") or 0) or None,
                    "small_biz_pct_avg":float(row.get("small_biz_pct_avg") or 0) or None,
                    "counties_rolled_up": int(row.get("counties_rolled_up") or 0),
                    "size_distribution": size_dist,
                    "source": "census_cbp",
                }, fallback, coverage_pct

        # Fallback — census_business_patterns at state × NAICS-4
        if state_fips or counties:
            sf = state_fips
            if not sf and counties:
                sf = counties[0][:2]
            rows = self._safe_query(db, """
                SELECT SUM(establishments) AS establishments,
                       SUM(employees) AS employees,
                       SUM(annual_payroll_thousands) AS annual_payroll_thousands,
                       MAX(year) AS data_year
                FROM census_business_patterns
                WHERE state_fips = :state AND length(naics_code) = 4 AND naics_code = :naics
            """, {"state": sf, "naics": naics_4})
            row = rows[0] if rows else {}
            if row.get("establishments"):
                provenance.append({"section": "structural_density",
                                   "table": "census_business_patterns", "rows": 1})
                return {
                    "establishments": int(row.get("establishments") or 0),
                    "employees":      int(row.get("employees") or 0),
                    "annual_payroll_thousands": int(row.get("annual_payroll_thousands") or 0),
                    "data_year":      int(row.get("data_year") or 0),
                    "source": "census_business_patterns",
                    "fallback_reason": "MSA county coverage <50% or non-MSA mode",
                }, True, coverage_pct
        return {}, fallback, coverage_pct

    # ── §4 demand / wealth ───────────────────────────────────────────────────

    def _gather_demand_context(self, db, counties, state_abbrs, provenance):
        out: Dict[str, Any] = {}
        # ACS median household income — aggregate over counties via state_fips
        # geo_id format unverified; use state aggregation as the safe path.
        if counties:
            state_fips_set = sorted(set(c[:2] for c in counties))
            rows = self._safe_query(db, """
                SELECT state_fips, AVG(b19013_001e)::int AS median_hh_income
                FROM acs5_2023_b19013
                WHERE state_fips = ANY(:states)
                GROUP BY state_fips
            """, {"states": state_fips_set})
            if rows:
                out["acs_median_income"] = [dict(r) for r in rows]
                provenance.append({"section": "demand_context",
                                   "table": "acs5_2023_b19013", "rows": len(rows)})

        # IRS SOI county income — aggregate top AGI classes in our counties.
        # The table stores agi_class breakouts; sum across all classes for total.
        if counties:
            # county_code is a 3-digit county FIPS suffix; state_code is 2-digit state FIPS.
            # Build the 5-digit FIPS via concatenation.
            rows = self._safe_query(db, """
                SELECT SUM(num_returns) AS total_returns,
                       SUM(total_agi)::bigint AS total_agi,
                       SUM(total_wages)::bigint AS total_wages,
                       SUM(total_business_income)::bigint AS total_business_income,
                       MAX(tax_year) AS tax_year
                FROM irs_soi_county_income
                WHERE state_code IS NOT NULL AND county_code IS NOT NULL
                  AND (lpad(state_code, 2, '0') || lpad(county_code, 3, '0')) = ANY(:counties)
                  AND agi_class = '0'  -- '0' = total across AGI classes (Census convention)
            """, {"counties": counties})
            if rows and rows[0].get("total_returns"):
                out["irs_soi_county"] = dict(rows[0])
                provenance.append({"section": "demand_context",
                                   "table": "irs_soi_county_income", "rows": 1})

        return out

    # ── §5 migration economy ─────────────────────────────────────────────────

    def _gather_migration_economy(self, db, counties, state_abbrs, provenance):
        if not counties:
            return {}
        rows = self._safe_query(db, """
            SELECT flow_type,
                   SUM(num_returns) AS num_returns,
                   SUM(total_agi)::bigint AS total_agi
            FROM irs_soi_migration
            WHERE (lpad(dest_state_code, 2, '0') || lpad(dest_county_code, 3, '0')) = ANY(:counties)
            GROUP BY flow_type
            ORDER BY flow_type
        """, {"counties": counties})
        if not rows:
            return {}
        provenance.append({"section": "migration_economy",
                           "table": "irs_soi_migration", "rows": len(rows)})
        return {"flows": [dict(r) for r in rows]}

    # ── §6 operating environment (macro context; no per-MSA banking) ──────────

    def _gather_operating_environment(self, db, provenance):
        out: Dict[str, Any] = {}
        # FRED interest rates — latest values
        rows = self._safe_query(db, """
            SELECT series_id, value, date
            FROM (
                SELECT series_id, value, date,
                       ROW_NUMBER() OVER (PARTITION BY series_id ORDER BY date DESC) AS rn
                FROM fred_interest_rates
                WHERE series_id IN ('DFF', 'DGS10', 'DGS2')
            ) t WHERE rn = 1
        """, {})
        if rows:
            out["rates"] = [dict(r) for r in rows]
            provenance.append({"section": "operating_environment",
                               "table": "fred_interest_rates", "rows": len(rows)})

        # Industrial production latest
        rows = self._safe_query(db, """
            SELECT series_id, value, date
            FROM fred_industrial_production
            ORDER BY date DESC LIMIT 1
        """, {})
        if rows:
            out["industrial_production"] = dict(rows[0])
            provenance.append({"section": "operating_environment",
                               "table": "fred_industrial_production", "rows": 1})

        return out

    # ── §7 risk profile ──────────────────────────────────────────────────────

    def _gather_risk_profile(self, db, counties, state_abbrs, provenance):
        out: Dict[str, Any] = {}

        # FEMA disaster declarations: state-level filter via state abbr
        if state_abbrs:
            rows = self._safe_query(db, """
                SELECT incident_type, COUNT(*) AS n,
                       MAX(declaration_date) AS most_recent
                FROM fema_disaster_declarations
                WHERE state = ANY(:states)
                GROUP BY incident_type
                ORDER BY n DESC LIMIT 10
            """, {"states": state_abbrs})
            if rows:
                out["fema_declarations"] = [dict(r) for r in rows]
                provenance.append({"section": "risk_profile",
                                   "table": "fema_disaster_declarations", "rows": len(rows)})

        # National Risk Index by county
        if counties:
            rows = self._safe_query(db, """
                SELECT AVG(risk_score)::numeric(10,2) AS risk_score_avg,
                       AVG(flood_score)::numeric(10,2) AS flood_score_avg,
                       AVG(hurricane_score)::numeric(10,2) AS hurricane_score_avg,
                       AVG(tornado_score)::numeric(10,2) AS tornado_score_avg,
                       AVG(wildfire_score)::numeric(10,2) AS wildfire_score_avg,
                       COUNT(*) AS counties_covered
                FROM national_risk_index
                WHERE county_fips = ANY(:counties)
            """, {"counties": counties})
            if rows and rows[0].get("risk_score_avg") is not None:
                out["nri_summary"] = dict(rows[0])
                provenance.append({"section": "risk_profile",
                                   "table": "national_risk_index", "rows": int(rows[0]["counties_covered"])})

        return out

    # ── §8 infrastructure proximity ──────────────────────────────────────────

    def _gather_infra_proximity(self, db, state_abbrs, provenance):
        if not state_abbrs:
            return {}
        out: Dict[str, Any] = {}

        for tbl, key in [
            ("power_plant", "power_plants"),
            ("transmission_line", "transmission_lines"),
            ("rail_line", "rail_segments"),
            ("airport", "airports"),
            ("data_center_facility", "data_centers"),
        ]:
            rows = self._safe_query(db, f"""
                SELECT COUNT(*) AS n FROM {tbl} WHERE state = ANY(:states)
            """, {"states": state_abbrs})
            if rows and rows[0].get("n"):
                out[key] = int(rows[0]["n"])
                provenance.append({"section": "infra_proximity",
                                   "table": tbl, "rows": int(rows[0]["n"])})
        return out

    # ── §9 trade exposure ────────────────────────────────────────────────────

    def _gather_trade_exposure(self, db, state_abbrs, provenance):
        if not state_abbrs:
            return {}
        rows = self._safe_query(db, """
            SELECT state_code, state_name,
                   SUM(value_ytd)::bigint AS total_export_value_ytd,
                   COUNT(DISTINCT country_code) AS export_partner_count,
                   COUNT(DISTINCT hs_code) AS hs_code_count
            FROM us_trade_exports_state
            WHERE state_code = ANY(:states)
            GROUP BY state_code, state_name
            ORDER BY total_export_value_ytd DESC NULLS LAST
            LIMIT 10
        """, {"states": state_abbrs})
        if not rows:
            return {}
        provenance.append({"section": "trade_exposure",
                           "table": "us_trade_exports_state", "rows": len(rows)})
        return {"state_exports": [dict(r) for r in rows]}

    # ── §10 federal $ flow ───────────────────────────────────────────────────

    def _gather_federal_spending(self, db, naics_4, state_abbrs, provenance):
        if not state_abbrs:
            return {}
        rows = self._safe_query(db, """
            SELECT awarding_agency,
                   COUNT(*) AS award_count,
                   SUM(award_amount)::numeric AS total_amount
            FROM usaspending_awards
            WHERE place_of_performance_state = ANY(:states)
              AND naics_code LIKE :prefix
            GROUP BY awarding_agency
            ORDER BY total_amount DESC NULLS LAST
            LIMIT 10
        """, {"states": state_abbrs, "prefix": f"{naics_4}%"})
        if not rows:
            return {}
        provenance.append({"section": "federal_spending",
                           "table": "usaspending_awards", "rows": len(rows)})
        return {"agencies": [dict(r) for r in rows]}

    # ── §11 public-cos in scope ──────────────────────────────────────────────

    def _gather_public_cos(self, db, naics_4, state_abbrs, provenance):
        sics = naics_to_sic(naics_4)
        if not sics:
            return {"sics_searched": [], "items": [],
                    "note": "No SIC mapping for this NAICS-4; no SEC operators visible."}

        # In-scope state filter is best-effort — sec_company_metadata has
        # business_state which is a 2-letter code. May not be populated for all rows.
        params = {"sics": sics}
        sql_state = ""
        if state_abbrs:
            sql_state = " AND business_state = ANY(:states)"
            params["states"] = state_abbrs

        rows = self._safe_query(db, f"""
            SELECT cik, company_name, sic_code, sic_description, business_state
            FROM sec_company_metadata
            WHERE sic_code = ANY(:sics){sql_state}
            ORDER BY company_name
            LIMIT :limit
        """, {**params, "limit": MAX_PUBLIC_COS})
        if not rows:
            return {"sics_searched": sics, "items": [],
                    "note": "No SEC-listed companies in scope for this NAICS-4 × geography."}
        provenance.append({"section": "public_cos",
                           "table": "sec_company_metadata", "rows": len(rows)})
        return {"sics_searched": sics, "items": [dict(r) for r in rows]}

    # ── §13 diligence questions — synthesized ────────────────────────────────

    def _synthesize_diligence_questions(self, naics_4, msa_label, density, demand,
                                        risk, public_cos, named_privates,
                                        fallback_used, coverage_pct) -> List[str]:
        qs: List[str] = []
        if density.get("establishments"):
            n = density["establishments"]
            qs.append(
                f"Structural density: {n:,} establishments visible in this "
                f"sector × geography. What share is captured by the top 5–10 "
                f"named operators below, and what does that imply about roll-up "
                f"opportunity?"
            )
        if density.get("hhi_avg"):
            hhi = density["hhi_avg"]
            level = "fragmented" if hhi < 0.10 else "moderately concentrated" if hhi < 0.18 else "concentrated"
            qs.append(
                f"Concentration (avg HHI = {hhi:.3f}) suggests a {level} market. "
                f"How does that align with the buyer's preferred deal structure "
                f"(single-platform vs. add-on)?"
            )
        if (risk.get("nri_summary") or {}).get("risk_score_avg"):
            qs.append(
                "Risk profile shows elevated natural-hazard exposure. What's the "
                "buyer's insurance / asset-protection posture, and does the "
                "thesis tolerate the implied capex / opex headwinds?"
            )
        if fallback_used:
            qs.append(
                "Structural data used the state-grain fallback rather than the "
                f"MSA county rollup (coverage was {coverage_pct:.0f}%). What does "
                "the buyer want to see done about the missing county detail?"
            )
        if public_cos.get("items"):
            qs.append(
                "Public-co operators in scope provide a benchmark for size + "
                "margin shape. Is the buyer's target priced at a discount or "
                "premium to those comparables?"
            )
        elif public_cos.get("sics_searched"):
            qs.append(
                "No public-co operators visible in scope — typical for highly-"
                "fragmented private-services sectors. Does the buyer have a "
                "private comp set to anchor valuation?"
            )
        if not named_privates.get("items"):
            qs.append(
                "Named-private list is empty for this sector (no regulated-facility "
                "data hit). Does the buyer already have a target list, or is "
                "this a green-field thesis that needs primary sourcing work?"
            )
        if not qs:
            qs.append(
                "No structured data lit up for this combination — re-evaluate "
                "the sector × geography choice or supplement with primary research."
            )
        return qs

    # ── DB helpers ───────────────────────────────────────────────────────────

    def _safe_query(self, db, sql: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Run a query; on any failure log + return []. Keeps the report rendering.

        CRITICAL: on failure we MUST rollback. A failed statement aborts the
        whole Postgres transaction — every subsequent query on the same
        session would then fail with "current transaction is aborted" until a
        rollback clears it. Without this, running the template against a DB
        that lacks a queried table (e.g. Atlas calling gather_data on the
        local DB where cloud-only tables are absent) poisons the session for
        any caller that reuses it afterward.
        """
        try:
            return [dict(r) for r in db.execute(text(sql), params).mappings().all()]
        except Exception as exc:  # noqa: BLE001
            logger.warning("MIP query failed: %s — %s", sql.strip().splitlines()[0], exc)
            try:
                db.rollback()
            except Exception:  # noqa: BLE001
                pass
            return []

    def _safe_scalar(self, db, sql: str, params: Dict[str, Any]):
        try:
            r = db.execute(text(sql), params).scalar()
            return r
        except Exception as exc:  # noqa: BLE001
            logger.warning("MIP scalar query failed: %s — %s", sql.strip().splitlines()[0], exc)
            try:
                db.rollback()
            except Exception:  # noqa: BLE001
                pass
            return None

    # ── render helpers — one method per section ──────────────────────────────

    def _build_toc(self, data) -> List[Dict[str, Any]]:
        out = []
        i = 1
        if data.get("structural_density"):
            out.append({"number": i, "id": "sec-density", "title": "Structural Density"}); i += 1
        if data.get("demand_context"):
            out.append({"number": i, "id": "sec-demand", "title": "Demand & Wealth"}); i += 1
        if data.get("migration_economy"):
            out.append({"number": i, "id": "sec-migration", "title": "Net-Migration Economy"}); i += 1
        if data.get("operating_environment"):
            out.append({"number": i, "id": "sec-operating", "title": "Operating Environment"}); i += 1
        if data.get("risk_profile"):
            out.append({"number": i, "id": "sec-risk", "title": "Risk Profile"}); i += 1
        if data.get("infra_proximity"):
            out.append({"number": i, "id": "sec-infra", "title": "Infrastructure Proximity"}); i += 1
        if data.get("trade_exposure"):
            out.append({"number": i, "id": "sec-trade", "title": "Trade Exposure"}); i += 1
        if data.get("federal_spending"):
            out.append({"number": i, "id": "sec-federal", "title": "Federal $ Flow"}); i += 1
        if (data.get("public_cos") or {}).get("items"):
            out.append({"number": i, "id": "sec-public", "title": "Public-Co Operators"}); i += 1
        if (data.get("named_privates") or {}).get("items"):
            out.append({"number": i, "id": "sec-private", "title": "Named Private Operators"}); i += 1
        if data.get("diligence_questions"):
            out.append({"number": i, "id": "sec-questions", "title": "Diligence Questions"}); i += 1
        if data.get("_provenance"):
            out.append({"number": i, "id": "sec-sources", "title": "Source Appendix"})
        return out

    # §1 — exec summary
    def _render_section_1_exec_summary(self, data) -> str:
        inp = data.get("input") or {}
        d = data.get("structural_density") or {}
        body = section_start(1, "Executive Summary", "sec-exec")
        if inp.get("client_note"):
            body += callout(f"<strong>Client note:</strong> {inp['client_note']}", variant="info")
        cards = []
        if d.get("establishments") is not None:
            cards.append(kpi_card("Establishments in scope", _fmt_int(d["establishments"])))
        if d.get("employees") is not None:
            cards.append(kpi_card("Employees", _fmt_int(d["employees"])))
        if d.get("annual_payroll_thousands") is not None:
            cards.append(kpi_card("Annual payroll", _fmt_usd(d["annual_payroll_thousands"] * 1000)))
        if d.get("hhi_avg") is not None:
            cards.append(kpi_card("Avg HHI", f"{d['hhi_avg']:.3f}"))
        if cards:
            body += kpi_strip(cards)
        body += section_end()
        return body

    # §2 — definition
    def _render_section_2_definition(self, data) -> str:
        inp = data.get("input") or {}
        body = section_start(2, "Sector & Geography Definition", "sec-def")
        rows = []
        if inp.get("naics_code"):
            rows.append(["NAICS code", inp["naics_code"]])
            rows.append(["NAICS label", inp.get("naics_label", "—")])
        for p in (inp.get("naics_parents") or []):
            rows.append([f"NAICS-{len(p['code'])} parent", f"{p['code']} — {p['label']}"])
        if inp.get("msa_title"):
            rows.append(["MSA", f"{inp.get('msa_code')} — {inp['msa_title']}"])
            rows.append(["Constituent counties", str(len(inp.get("counties") or []))])
            rows.append(["States", ", ".join(inp.get("state_abbrs") or [])])
        elif inp.get("state_fips"):
            rows.append(["State FIPS", inp["state_fips"]])
        rows.append(["Geography mode", inp.get("geography_mode", "—")])
        body += data_table(["Field", "Value"], rows)
        body += section_end()
        return body

    # §3 — structural density (returns html + js for chart)
    def _render_section_3_structural_density(self, data):
        d = data.get("structural_density") or {}
        if not d:
            return "", ""
        body = section_start(3, "Structural Density", "sec-density")
        if data.get("_fallback_used"):
            body += callout(
                f"<strong>Note:</strong> County-grain CBP data was sparse for this "
                f"MSA (coverage = {data.get('_msa_county_coverage_pct', 0):.0f}%). "
                f"Density figures here use state-grain `census_business_patterns` "
                f"as a fallback. Other sections (demand, risk, infra) still use "
                f"finer geographies.",
                variant="warn",
            )
        rows = [
            ["Establishments",      _fmt_int(d.get("establishments"))],
            ["Employees",           _fmt_int(d.get("employees"))],
            ["Annual payroll",      _fmt_usd((d.get("annual_payroll_thousands") or 0) * 1000)],
        ]
        if d.get("hhi_avg") is not None:
            rows.append(["Concentration (HHI, avg)", f"{d['hhi_avg']:.4f}"])
        if d.get("small_biz_pct_avg") is not None:
            rows.append(["Small-biz share (avg)", _fmt_pct(d["small_biz_pct_avg"] * 100)])
        if d.get("counties_rolled_up"):
            rows.append(["Counties rolled up", str(d["counties_rolled_up"])])
        rows.append(["Source", d.get("source", "—")])
        body += data_table(["Metric", "Value"], rows)

        charts_js = ""
        size_dist = d.get("size_distribution") or []
        if size_dist:
            labels = [s["bucket"] for s in size_dist]
            values = [s["n"] for s in size_dist]
            cfg = build_horizontal_bar_config(labels=labels, values=values,
                                              dataset_label="Establishments")
            body += "<div style='margin-top:16px'></div>"
            body += chart_container(chart_id="mip_size_dist",
                                    chart_config_json=json.dumps(cfg),
                                    fallback_html=build_bar_fallback(labels, values),
                                    size="medium",
                                    title="Establishments by employee-count bucket")
            charts_js += chart_init_js("mip_size_dist", json.dumps(cfg))
        body += section_end()
        return body, charts_js

    # §4 — demand context
    def _render_section_4_demand_context(self, data) -> str:
        d = data.get("demand_context") or {}
        if not d:
            return ""
        body = section_start(4, "Demand & Wealth Context", "sec-demand")
        if d.get("acs_median_income"):
            rows = [[r["state_fips"], _fmt_usd(r["median_hh_income"])]
                    for r in d["acs_median_income"]]
            body += data_table(["State FIPS", "ACS 2023 median household income (state avg)"], rows)
        if d.get("irs_soi_county"):
            soi = d["irs_soi_county"]
            rows = [
                ["Tax year", str(soi.get("tax_year", "—"))],
                ["Returns filed (in-scope counties)", _fmt_int(soi.get("total_returns"))],
                ["Total AGI",                 _fmt_usd(soi.get("total_agi"))],
                ["Total wages",               _fmt_usd(soi.get("total_wages"))],
                ["Total business income",     _fmt_usd(soi.get("total_business_income"))],
            ]
            body += "<div style='margin-top:16px'></div>"
            body += data_table(["IRS SOI county aggregate", "Value"], rows)
        body += section_end()
        return body

    # §5 — migration
    def _render_section_5_migration_economy(self, data) -> str:
        m = data.get("migration_economy") or {}
        if not m.get("flows"):
            return ""
        body = section_start(5, "Net-Migration Economy", "sec-migration")
        rows = [[r["flow_type"], _fmt_int(r["num_returns"]), _fmt_usd(r["total_agi"])]
                for r in m["flows"]]
        body += data_table(["Flow type", "Returns", "Total AGI"], rows)
        body += section_end()
        return body

    # §6 — operating environment
    def _render_section_6_operating_environment(self, data) -> str:
        op = data.get("operating_environment") or {}
        if not op:
            return ""
        body = section_start(6, "Operating Environment (Macro Context)", "sec-operating")
        rates = op.get("rates") or []
        if rates:
            rows = [[r["series_id"], _fmt_pct(float(r["value"])), _fmt_date(r["date"])]
                    for r in rates]
            body += data_table(["Series", "Latest value", "As of"], rows)
        if op.get("industrial_production"):
            ip = op["industrial_production"]
            body += "<div style='margin-top:16px'></div>"
            body += callout(
                f"FRED Industrial Production index ({ip.get('series_id', '—')}) "
                f"latest value <strong>{ip.get('value', '—')}</strong> as of "
                f"{_fmt_date(ip.get('date'))}.",
                variant="info",
            )
        body += section_end()
        return body

    # §7 — risk profile
    def _render_section_7_risk_profile(self, data) -> str:
        r = data.get("risk_profile") or {}
        if not r:
            return ""
        body = section_start(7, "Risk Profile", "sec-risk")
        if r.get("fema_declarations"):
            rows = [[d["incident_type"], _fmt_int(d["n"]), _fmt_date(d["most_recent"])]
                    for d in r["fema_declarations"]]
            body += data_table(["Incident type", "Declarations", "Most recent"], rows)
        if r.get("nri_summary"):
            nri = r["nri_summary"]
            body += "<div style='margin-top:16px'></div>"
            body += data_table(
                ["NRI dimension", "Avg score"],
                [
                    ["Overall risk",   f"{nri.get('risk_score_avg', '—')}"],
                    ["Flood",          f"{nri.get('flood_score_avg', '—')}"],
                    ["Hurricane",      f"{nri.get('hurricane_score_avg', '—')}"],
                    ["Tornado",        f"{nri.get('tornado_score_avg', '—')}"],
                    ["Wildfire",       f"{nri.get('wildfire_score_avg', '—')}"],
                    ["Counties scored", _fmt_int(nri.get("counties_covered"))],
                ],
            )
        body += section_end()
        return body

    # §8 — infra proximity
    def _render_section_8_infra_proximity(self, data) -> str:
        i = data.get("infra_proximity") or {}
        if not i:
            return ""
        body = section_start(8, "Infrastructure Proximity (state-level)", "sec-infra")
        labels = ["Power plants", "Transmission lines", "Rail segments", "Airports", "Data centers"]
        keys =   ["power_plants",  "transmission_lines", "rail_segments", "airports", "data_centers"]
        rows = [[l, _fmt_int(i.get(k, 0))] for l, k in zip(labels, keys)]
        body += data_table(["Asset class", "Count in scope state(s)"], rows)
        body += section_end()
        return body

    # §9 — trade
    def _render_section_9_trade_exposure(self, data) -> str:
        t = data.get("trade_exposure") or {}
        if not t.get("state_exports"):
            return ""
        body = section_start(9, "Trade Exposure", "sec-trade")
        rows = [[r["state_code"], r["state_name"], _fmt_usd(r["total_export_value_ytd"]),
                 _fmt_int(r["export_partner_count"]), _fmt_int(r["hs_code_count"])]
                for r in t["state_exports"]]
        body += data_table(
            ["State code", "State", "YTD exports", "Trade partners", "HS codes"],
            rows,
        )
        body += section_end()
        return body

    # §10 — federal spending
    def _render_section_10_federal_spending(self, data) -> str:
        f = data.get("federal_spending") or {}
        if not f.get("agencies"):
            return ""
        body = section_start(10, "Federal $ Flow (USAspending)", "sec-federal")
        rows = [[r["awarding_agency"], _fmt_int(r["award_count"]), _fmt_usd(r["total_amount"])]
                for r in f["agencies"]]
        body += data_table(["Awarding agency", "Awards", "Total amount"], rows)
        body += section_end()
        return body

    # §11 — public-cos
    def _render_section_11_public_cos(self, data) -> str:
        p = data.get("public_cos") or {}
        if not p.get("items"):
            return ""
        body = section_start(11, "Public-Co Operators (SEC, NAICS↔SIC crosswalk)", "sec-public")
        rows = [[r["cik"], r["company_name"], r.get("sic_code", "—"),
                 r.get("business_state", "—")]
                for r in p["items"]]
        body += data_table(["CIK", "Company", "SIC", "Business state"], rows)
        body += section_end()
        return body

    # §12 — named privates
    def _render_section_12_named_privates(self, data) -> str:
        np_ = data.get("named_privates") or {}
        if not np_.get("items"):
            # Show the note even with no items, so the buyer understands why
            if np_.get("note"):
                body = section_start(12, "Named Private Operators", "sec-private")
                body += callout(np_["note"], variant="info")
                body += section_end()
                return body
            return ""
        body = section_start(12, "Named Private Operators (best-effort, regulated-facility data)", "sec-private")
        if np_.get("note"):
            body += callout(np_["note"], variant="info")
        rows = [[r.get("facility_name", "—"), r.get("state", "—"), r.get("county", "—"),
                 r.get("compliance_status", "—"),
                 _fmt_int(r.get("violation_count")),
                 _fmt_int(r.get("inspection_count"))]
                for r in np_["items"]]
        body += data_table(
            ["Facility", "State", "County", "Compliance", "Violations", "Inspections"],
            rows,
        )
        body += section_end()
        return body

    # §13 — diligence questions
    def _render_section_13_diligence_questions(self, data) -> str:
        qs = data.get("diligence_questions") or []
        if not qs:
            return ""
        body = section_start(13, "Diligence Questions", "sec-questions")
        body += "<ol style='line-height:1.6'>"
        for q in qs:
            body += f"<li>{q}</li>"
        body += "</ol>"
        body += section_end()
        return body

    # §14 — provenance
    def _render_section_14_provenance(self, data) -> str:
        prov = data.get("_provenance") or []
        if not prov:
            return ""
        body = section_start(14, "Source Appendix", "sec-sources")
        rows = [[p.get("section", "—"), p.get("table", "—"), _fmt_int(p.get("rows"))]
                for p in prov]
        body += data_table(["Section", "Source table", "Rows used"], rows)
        body += "<div style='margin-top:12px;font-size:12px;color:var(--gray-500)'>"
        body += "Every numeric value above traces back to one of these sources. "
        body += "Sections returning zero rows are omitted from the report rather "
        body += "than rendered with placeholder data."
        body += "</div>"
        body += section_end()
        return body
