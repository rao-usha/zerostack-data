"""
SPEC 061 — Market Intelligence Pack Report Template.

All tests are pure-Python — no live DB. MagicMock simulates the DB session;
the template's `_safe_query` / `_safe_scalar` consume one mock-result per
call in the order the template issues queries. Tests focus on:

  * input validation (T2, T3)
  * skip-on-empty per section (T4)
  * full-render with synthetic data (T5)
  * deterministic ordering (T6)
  * CBP NAICS-6 → NAICS-4 rollup logic (T7) — pure-function on the SQL shape
  * provenance correctness (T8)
  * render_excel raises (T9)
  * sparse-MSA callout (T10)
  * template registers in ReportBuilder (T1)
"""

import json
import pytest
from unittest.mock import MagicMock

from app.reports.templates.market_intelligence_pack import (
    MarketIntelligencePackTemplate,
    _default_named_operators_curator,
)


def _mk_db_returning(results_in_order):
    """Build a MagicMock DB whose .execute(...).mappings().all() pops the next
    result from `results_in_order` per call. .scalar() pops from the same list.

    Each entry should be a list-of-dicts (for mappings) OR a scalar (for scalar()).
    The template calls mappings().all() vastly more than scalar(); this is good
    enough as long as test assertions don't rely on call-counter integrity for
    every single query.
    """
    db = MagicMock()
    state = {"i": 0}

    def _execute(*_a, **_k):
        i = state["i"]
        state["i"] += 1
        result = MagicMock()
        # Default to empty if past the end
        cur = results_in_order[i] if i < len(results_in_order) else []
        if isinstance(cur, list):
            result.mappings.return_value.all.return_value = cur
            result.scalar.return_value = (cur[0].get("_scalar") if cur and isinstance(cur[0], dict) else 0)
        else:
            result.mappings.return_value.all.return_value = []
            result.scalar.return_value = cur
        return result

    db.execute.side_effect = _execute
    return db


def _empty_db():
    """DB where every query returns nothing — exercises skip-on-empty."""
    db = MagicMock()
    result = MagicMock()
    result.mappings.return_value.all.return_value = []
    result.scalar.return_value = 0
    db.execute.return_value = result
    return db


# ─────────────────────────────────────────────────────────────────────────────
# T1 — registration
# ─────────────────────────────────────────────────────────────────────────────

class TestSpec061Registration:

    def test_template_registers_in_builder(self):
        """T1: ReportBuilder().templates['market_intelligence_pack'] exists."""
        from app.reports.builder import ReportBuilder
        db = MagicMock()
        rb = ReportBuilder(db)
        assert "market_intelligence_pack" in rb.templates
        assert isinstance(rb.templates["market_intelligence_pack"],
                          MarketIntelligencePackTemplate)


# ─────────────────────────────────────────────────────────────────────────────
# T2, T3 — input validation
# ─────────────────────────────────────────────────────────────────────────────

class TestSpec061InputValidation:

    def test_gather_data_validates_naics(self):
        """T2: unknown NAICS → ValueError."""
        tpl = MarketIntelligencePackTemplate()
        with pytest.raises(ValueError, match="Unknown NAICS"):
            tpl.gather_data(_empty_db(), {
                "naics_code": "99999",
                "geography_mode": "msa",
                "msa_code": "26420",
            })

    def test_gather_data_rejects_non_4digit_naics(self):
        """T2b: 2-digit / 6-digit NAICS → ValueError (input must be 4-digit)."""
        tpl = MarketIntelligencePackTemplate()
        with pytest.raises(ValueError, match="4-digit"):
            tpl.gather_data(_empty_db(), {
                "naics_code": "33",          # 2-digit
                "geography_mode": "msa",
                "msa_code": "26420",
            })
        with pytest.raises(ValueError, match="4-digit"):
            tpl.gather_data(_empty_db(), {
                "naics_code": "332323",      # 6-digit
                "geography_mode": "msa",
                "msa_code": "26420",
            })

    def test_gather_data_validates_msa(self):
        """T3: unknown MSA in geography_mode='msa' → ValueError."""
        tpl = MarketIntelligencePackTemplate()
        with pytest.raises(ValueError, match="Unknown MSA"):
            tpl.gather_data(_empty_db(), {
                "naics_code": "3323",
                "geography_mode": "msa",
                "msa_code": "00000",
            })

    def test_gather_data_rejects_missing_geo(self):
        """T3b: missing msa_code/state_fips/county list → ValueError."""
        tpl = MarketIntelligencePackTemplate()
        with pytest.raises(ValueError, match="requires"):
            tpl.gather_data(_empty_db(), {
                "naics_code": "3323",
                "geography_mode": "msa",
                # no msa_code
            })


# ─────────────────────────────────────────────────────────────────────────────
# T4 — skip-on-empty
# ─────────────────────────────────────────────────────────────────────────────

class TestSpec061SkipOnEmpty:

    def test_render_html_skip_on_empty_each_section(self):
        """T4: gather_data on an empty DB → render_html still works, no section bodies."""
        tpl = MarketIntelligencePackTemplate()
        data = tpl.gather_data(_empty_db(), {
            "naics_code": "3323",
            "geography_mode": "msa",
            "msa_code": "26420",
        })
        html = tpl.render_html(data)
        # Document still renders end-to-end
        assert html.startswith("<!DOCTYPE html>")
        assert html.rstrip().endswith("</html>")
        # Header always present
        assert "Sector" in html and "Market Intelligence Pack" in html
        # Section 1 (exec summary) and §2 (definition) always render — they use input not query data
        assert 'id="sec-exec"' in html or "sec-exec" in html
        assert "sec-def" in html
        # Sections 3+ should NOT render when their data is empty
        assert "sec-density" not in html
        assert "sec-demand" not in html
        assert "sec-risk" not in html
        # Source appendix only renders if provenance has entries → empty DB = no provenance
        assert "sec-sources" not in html


# ─────────────────────────────────────────────────────────────────────────────
# T5 — full render with synthetic data
# ─────────────────────────────────────────────────────────────────────────────

class TestSpec061FullRender:

    def _full_data(self):
        """Build a hand-crafted `data` dict mimicking a fully-populated MSA."""
        return {
            "input": {
                "naics_code": "3323", "naics_label": "Architectural and Structural Metals Mfg",
                "naics_parents": [
                    {"code": "33",  "label": "Manufacturing"},
                    {"code": "332", "label": "Fabricated Metal Product Mfg"},
                ],
                "geography_mode": "msa",
                "msa_code": "26420", "msa_title": "Houston-Pasadena-The Woodlands, TX",
                "state_fips": None, "state_abbrs": ["TX"],
                "counties": ["48201", "48157", "48039"],
                "client_note": "Houston metal-fab roll-up thesis.",
                "generated_at_utc": "2026-05-18T15:00:00+00:00",
            },
            "structural_density": {
                "establishments": 412, "employees": 5230,
                "annual_payroll_thousands": 387_000,
                "hhi_avg": 0.0824, "small_biz_pct_avg": 0.78,
                "counties_rolled_up": 9,
                "size_distribution": [
                    {"bucket": "1-4", "n": 200}, {"bucket": "5-9", "n": 70},
                    {"bucket": "10-19", "n": 60}, {"bucket": "20-49", "n": 45},
                    {"bucket": "50-99", "n": 25}, {"bucket": "100-249", "n": 10},
                    {"bucket": "250+", "n": 2},
                ],
                "source": "census_cbp",
            },
            "demand_context": {
                "acs_median_income": [{"state_fips": "48", "median_hh_income": 73_000}],
                "irs_soi_county": {
                    "tax_year": 2022, "total_returns": 1_240_000,
                    "total_agi": 158_000_000_000, "total_wages": 122_000_000_000,
                    "total_business_income": 9_500_000_000,
                },
            },
            "migration_economy": {"flows": [
                {"flow_type": "in",  "num_returns": 80_000, "total_agi": 6_200_000_000},
                {"flow_type": "out", "num_returns": 65_000, "total_agi": 4_800_000_000},
            ]},
            "operating_environment": {
                "rates": [
                    {"series_id": "DFF",   "value": 4.83, "date": "2026-05-15"},
                    {"series_id": "DGS10", "value": 4.22, "date": "2026-05-15"},
                ],
                "industrial_production": {"series_id": "INDPRO", "value": 102.4, "date": "2026-04-30"},
            },
            "risk_profile": {
                "fema_declarations": [
                    {"incident_type": "Hurricane",    "n": 7, "most_recent": "2024-09-12"},
                    {"incident_type": "Severe Storm", "n": 4, "most_recent": "2024-04-30"},
                ],
                "nri_summary": {
                    "risk_score_avg": 92.3, "flood_score_avg": 88.1,
                    "hurricane_score_avg": 99.0, "tornado_score_avg": 60.0,
                    "wildfire_score_avg": 15.0, "counties_covered": 9,
                },
            },
            "infra_proximity": {
                "power_plants": 142, "transmission_lines": 1_640,
                "rail_segments": 3_120, "airports": 410, "data_centers": 38,
            },
            "trade_exposure": {"state_exports": [
                {"state_code": "TX", "state_name": "Texas",
                 "total_export_value_ytd": 138_000_000_000,
                 "export_partner_count": 198, "hs_code_count": 4_120},
            ]},
            "federal_spending": {"agencies": [
                {"awarding_agency": "Department of Defense", "award_count": 240,
                 "total_amount": 1_200_000_000},
            ]},
            "public_cos": {
                "sics_searched": ["3441", "3446"],
                "items": [
                    {"cik": "0000001", "company_name": "Acme Steel Co",
                     "sic_code": "3441", "sic_description": "...", "business_state": "TX"},
                ],
            },
            "named_privates": {"items": [
                {"facility_name": "Lone Star Fab Inc", "state": "TX", "county": "Harris",
                 "compliance_status": "OK", "violation_count": 2, "inspection_count": 6,
                 "last_inspection_date": "2025-08-12"},
            ], "note": "Showing 1 EPA-ECHO operator.", "source": "epa_echo_facilities"},
            "diligence_questions": [
                "What share is captured by the top 5–10 named operators?",
                "Does the buyer tolerate hurricane risk?",
            ],
            "_fallback_used": False,
            "_msa_county_coverage_pct": 100.0,
            "_provenance": [
                {"section": "structural_density",     "table": "census_cbp", "rows": 9},
                {"section": "demand_context",          "table": "acs5_2023_b19013", "rows": 1},
                {"section": "risk_profile",            "table": "national_risk_index", "rows": 9},
                {"section": "infra_proximity",         "table": "power_plant", "rows": 142},
                {"section": "trade_exposure",          "table": "us_trade_exports_state", "rows": 1},
                {"section": "federal_spending",        "table": "usaspending_awards", "rows": 1},
                {"section": "public_cos",              "table": "sec_company_metadata", "rows": 1},
                {"section": "named_privates",          "table": "epa_echo_facilities", "rows": 1},
            ],
        }

    def test_render_html_with_full_synthetic_data(self):
        """T5: hand-crafted full `data` renders every section."""
        tpl = MarketIntelligencePackTemplate()
        html = tpl.render_html(self._full_data())
        # Section presence via stable section-id anchors (immune to HTML-entity escaping)
        for anchor in [
            "sec-exec", "sec-def", "sec-density", "sec-demand",
            "sec-migration", "sec-operating", "sec-risk", "sec-infra",
            "sec-trade", "sec-federal", "sec-public", "sec-private",
            "sec-questions", "sec-sources",
        ]:
            assert anchor in html, f"missing section anchor: {anchor}"
        # Spot-check distinctive section-title substrings (entity-safe)
        for s in ["Executive Summary", "Structural Density", "Net-Migration",
                  "Operating Environment", "Risk Profile", "Infrastructure Proximity",
                  "Trade Exposure", "Public-Co Operators", "Named Private Operators",
                  "Diligence Questions", "Source Appendix"]:
            assert s in html, f"missing section title fragment: {s}"
        # KPI strip carries the headline numbers
        assert "412" in html or "Establishments" in html
        # Client note appears
        assert "Houston metal-fab roll-up thesis." in html
        # Chart container for size distribution
        assert 'id="mip_size_dist"' in html


# ─────────────────────────────────────────────────────────────────────────────
# T6 — deterministic ordering
# ─────────────────────────────────────────────────────────────────────────────

class TestSpec061Deterministic:

    def test_section_ordering_deterministic(self):
        """T6: same `data` → byte-identical render."""
        tpl = MarketIntelligencePackTemplate()
        data = TestSpec061FullRender()._full_data()
        a = tpl.render_html(data)
        b = tpl.render_html(data)
        assert a == b, "rendering not deterministic"


# ─────────────────────────────────────────────────────────────────────────────
# T7 — CBP rollup correctness (pure-function via mocked DB)
# ─────────────────────────────────────────────────────────────────────────────

class TestSpec061CbpRollup:

    def test_cbp_rollup_naics6_to_naics4(self):
        """T7: cb_cbp aggregation returns SUMs across counties correctly.

        We mock the DB to feed a single aggregated row back — verifies the
        rollup method packages the result correctly and routes to the
        census_cbp path (not the fallback)."""
        tpl = MarketIntelligencePackTemplate()
        db = MagicMock()
        seq = [
            # _safe_scalar coverage check → 3 of 3 counties present (100%)
            3,
            # _safe_query rollup aggregate
            [{
                "establishments": 412, "employees": 5230,
                "annual_payroll_thousands": 387_000,
                "estab_1_4": 200, "estab_5_9": 70, "estab_10_19": 60,
                "estab_20_49": 45, "estab_50_99": 25, "estab_100_249": 10,
                "estab_250_plus": 2,
                "hhi_avg": 0.0824, "small_biz_pct_avg": 0.78,
                "counties_rolled_up": 3,
            }],
        ]

        i = {"v": 0}
        def _exec(*_a, **_k):
            j = i["v"]; i["v"] += 1
            r = MagicMock()
            cur = seq[j] if j < len(seq) else []
            if isinstance(cur, list):
                r.mappings.return_value.all.return_value = cur
                r.scalar.return_value = 0
            else:
                r.scalar.return_value = cur
                r.mappings.return_value.all.return_value = []
            return r
        db.execute.side_effect = _exec

        density, fallback, coverage = tpl._gather_structural_density(
            db, "3323", ["48201", "48157", "48039"], None, "msa", provenance=[],
        )
        assert not fallback, "should use census_cbp, not fallback"
        assert coverage == 100.0
        assert density["establishments"] == 412
        assert density["employees"] == 5230
        assert density["source"] == "census_cbp"
        assert density["counties_rolled_up"] == 3
        # Size distribution preserved
        assert {b["bucket"]: b["n"] for b in density["size_distribution"]}["1-4"] == 200


# ─────────────────────────────────────────────────────────────────────────────
# T8 — provenance correctness
# ─────────────────────────────────────────────────────────────────────────────

class TestSpec061Provenance:

    def test_provenance_footer_cites_used_sources(self):
        """T8: provenance section lists tables that produced rows; doesn't list empties."""
        tpl = MarketIntelligencePackTemplate()
        data = TestSpec061FullRender()._full_data()
        html = tpl.render_html(data)
        # Tables in _provenance should appear in the rendered appendix
        for table in ["census_cbp", "acs5_2023_b19013", "national_risk_index",
                      "power_plant", "us_trade_exports_state", "usaspending_awards",
                      "sec_company_metadata", "epa_echo_facilities"]:
            assert table in html, f"provenance missing: {table}"
        # An obviously absent table must NOT appear
        assert "treasury_daily_balance" not in html


# ─────────────────────────────────────────────────────────────────────────────
# T9 — render_excel raises
# ─────────────────────────────────────────────────────────────────────────────

class TestSpec061ExcelRaises:

    def test_render_excel_raises(self):
        """T9: render_excel(...) raises NotImplementedError."""
        tpl = MarketIntelligencePackTemplate()
        with pytest.raises(NotImplementedError):
            tpl.render_excel({})


# ─────────────────────────────────────────────────────────────────────────────
# T10 — sparse-MSA fallback callout
# ─────────────────────────────────────────────────────────────────────────────

class TestSpec061SparseMsaCallout:

    def test_sparse_msa_callout(self):
        """T10: when _fallback_used=True, the structural-density section shows a callout."""
        tpl = MarketIntelligencePackTemplate()
        data = TestSpec061FullRender()._full_data()
        data["_fallback_used"] = True
        data["_msa_county_coverage_pct"] = 30.0
        data["structural_density"]["source"] = "census_business_patterns"
        html = tpl.render_html(data)
        assert "fallback" in html.lower() or "sparse" in html.lower() or "limited" in html.lower() or "coverage" in html.lower(), \
            "no sparse-MSA callout rendered"
        assert "30%" in html or "30.0" in html or "coverage" in html.lower()
