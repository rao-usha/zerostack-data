# SPEC 061 — Market Intelligence Pack Report Template

**Status:** Draft
**Task type:** report
**Date:** 2026-05-16
**Plan:** PLAN_065 (Sector × Market Intelligence Pack)
**Test file:** tests/test_spec_061_market_intelligence_pack.py
**Builds on:** SPEC_060 (taxonomies + CBP audit), commit `0829878`

## Goal

Build the report template that turns a `(naics_code, msa_code | state | counties)` input
into a 14-section, decision-grade sector × market intelligence map, rendered as a
single self-contained HTML doc consistent with the existing design system.
This is the artifact the paying customer receives — every other PLAN_065 spec
exists to source, price, deliver, or queue this template.

## Acceptance Criteria

- [ ] `MarketIntelligencePackTemplate` class registered in
      `app.reports.builder.ReportBuilder.templates` under the key
      `"market_intelligence_pack"`.
- [ ] `gather_data(db, params)` accepts:
  - `naics_code: str` (validated against `taxonomies.load_naics`)
  - `geography_mode: 'msa' | 'state' | 'multi_county'`
  - one of `msa_code` / `state_fips` / `county_fips_list` matching the mode
  - optional `client_note: str`
  - optional `named_operators_limit: int = 25`
- [ ] All DB queries are parameterized (`:param` style) — no string concat.
- [ ] All section renderers tolerate empty query results — never raise, never
      render placeholder rows like "no data".
- [ ] §3 (Structural density) implements **NAICS-6 → NAICS-4 rollup** from
      `census_cbp` county rows for MSA mode. Falls back to
      `census_business_patterns` for the 7 sparse MSAs and for state mode.
- [ ] §11 (Public-co operators) uses the SPEC_060 NAICS↔SIC crosswalk to find
      `sec_company_metadata` matches.
- [ ] §12 (Named private operators) calls the SPEC_062 curator (placeholder
      callable for now; real curator lands in SPEC_062).
- [ ] §13 + §14 (Diligence questions, Source appendix + provenance) render
      automatically from the rows that came back — every numeric value can be
      traced back to its source table + row count.
- [ ] Visual quality at or above `pe_deal_memo` — uses `page_header`,
      `kpi_strip`, `section_start/end`, `data_table`, `callout`,
      `chart_container`, `page_footer` from `design_system`.
- [ ] `render_excel` raises `NotImplementedError` for v1 (HTML only).
- [ ] Section ordering is deterministic — running the template twice on the
      same inputs produces byte-identical HTML.
- [ ] No regression: existing 36 tests (SPEC_059 / 060 / 056 / config) still pass.

## Test Cases

All tests use a synthetic `params` dict passed directly to `gather_data` — no
DB unless explicitly noted. Live cloud query is exercised in the manual
smoke test, not in pytest.

| ID  | Test Name                                       | What It Verifies                                                                                  |
|-----|-------------------------------------------------|---------------------------------------------------------------------------------------------------|
| T1  | test_template_registers_in_builder              | `ReportBuilder().templates["market_intelligence_pack"]` exists + is the right class               |
| T2  | test_gather_data_validates_naics                | unknown NAICS → ValueError                                                                        |
| T3  | test_gather_data_validates_msa                  | unknown MSA in mode='msa' → ValueError                                                            |
| T4  | test_render_html_skip_on_empty_each_section     | every section method called with `{}` produces a snippet that doesn't break the doc               |
| T5  | test_render_html_with_full_synthetic_data       | a hand-built complete `data` dict renders all 14 sections, each present in output                 |
| T6  | test_section_ordering_deterministic             | render twice on same `data` → byte-identical output                                                |
| T7  | test_cbp_rollup_naics6_to_naics4                | the rollup helper sums NAICS-6 county rows to NAICS-4 totals correctly (pure-function)            |
| T8  | test_provenance_footer_cites_used_sources       | footer mentions every table that produced rows; never mentions a table that returned 0 rows       |
| T9  | test_render_excel_raises                        | `render_excel(...)` raises `NotImplementedError`                                                   |
| T10 | test_sparse_msa_callout                         | when MSA has <50% county coverage in cbp, output contains a "limited county-grain data" callout    |

## Rubric Checklist

_(No `report.md` rubric in memory; generic report checklist.)_

- [ ] Inherits from / mirrors the existing `pe_deal_memo` template shape so
      `ReportBuilder.generate(template_name=..., format="html", params=...)`
      works without router changes.
- [ ] All section renderers are private methods (`_render_section_N_xxx`) for
      testability + naming clarity.
- [ ] Provenance: every chart, table, and KPI carries a footer source citation
      (e.g. `Source: census_cbp (county × NAICS-6 rolled up), n=12 counties`).
- [ ] No PII / no scraping / public-data only.
- [ ] Queries hit cloud DB; never write. Read-only.
- [ ] All `text()` SQL parameterized. No f-string SQL.
- [ ] Cloud-DB connectivity failure mode: report still renders, sections that
      couldn't query become a callout ("data unavailable — see source appendix").
      No 500.
- [ ] Renders in <10s for a typical MSA on db-f1-micro (one round-trip per
      section is the budget; ≤20 SELECTs total).

## Design Notes

### Class shape

```python
# app/reports/templates/market_intelligence_pack.py
from app.reports.templates._ic_report_base import ICReportBase  # if exists
from app.reports.design_system import (...)
from app.services.diligence.taxonomies import (
    naics_label, naics_parents, naics_to_sic,
    msa_title, msa_counties, state_counties, load_naics,
)

class MarketIntelligencePackTemplate(ICReportBase):
    name = "market_intelligence_pack"
    display_name = "Sector × Market Intelligence Pack"
    description = "..."

    def gather_data(self, db, params): ...     # → big dict, see below
    def render_html(self, data): ...           # → self-contained HTML string
    def render_excel(self, data):              # raises NotImplementedError
        raise NotImplementedError(...)
```

### `gather_data` returns

A flat dict keyed by section, so render_html can iterate. Each value is the
raw query result + a small `meta` block with provenance:

```python
{
  "input": {"naics_code": "3323", "naics_label": "...", "geography_mode": "msa",
            "msa_code": "26420", "msa_title": "Houston-...", "counties": [...],
            "client_note": "...", "generated_at_utc": "..."},

  "structural_density": {
      "primary": {"naics_4": "3323", "naics_label": "...",
                  "establishments": 412, "employees": 5_230, "payroll_k": 387_000,
                  "counties_rolled_up": 9, "msa_county_coverage_pct": 100.0},
      "size_distribution": [{"bucket": "1-4", "n": 200}, ...],
      "hhi_avg": 0.0824,
      "small_biz_pct_avg": 0.78,
      "fallback_used": False,
      "source": "census_cbp",
      "_meta": {"rows": 9, "queried_at": "..."}
  },

  "demand_context": { "acs_b19013": {...}, "irs_soi_county": {...}, "_meta": {...} },
  "migration_economy": {...},
  "operating_environment": {...},
  "risk_profile": {...},
  "infra_proximity": {...},
  "trade_exposure": {...},
  "federal_spending": {...},
  "public_cos": {...},
  "named_privates": {...},
  "diligence_questions": [str, ...],
  "_provenance": [
      {"section": "structural_density", "table": "census_cbp", "rows": 9},
      ...
  ],
}
```

### §3 NAICS-6 → NAICS-4 rollup (the key new logic)

```python
def _query_cbp_density(db, naics_4, county_fips_list):
    """Roll NAICS-6 county rows up to NAICS-4 by summing across the MSA's
    constituent counties. Returns aggregate + per-county breakout."""
    rows = db.execute(text("""
        SELECT county_fips, naics_code,
               establishments, employees, annual_payroll_thousands,
               estab_1_4, estab_5_9, estab_10_19, estab_20_49,
               estab_50_99, estab_100_249, estab_250_plus,
               hhi, small_biz_pct
        FROM census_cbp
        WHERE geo_level = 'county'
          AND county_fips = ANY(:counties)
          AND naics_code LIKE :prefix
    """), {"counties": county_fips_list, "prefix": f"{naics_4}%"}).mappings().all()
    # Aggregate: SUM the additive columns; AVG the ratio columns (HHI, small_biz_pct).
    ...
```

### Section dispatch in `render_html`

```python
def render_html(self, data):
    body = page_header(...)
    body += self._render_section_1_exec_summary(data)
    body += self._render_section_2_definition(data)
    body += self._render_section_3_structural_density(data)
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
    body += page_footer(...)
    return html_document(title=..., body_content=body, charts_js=charts_js)
```

Each `_render_section_N_*` is responsible for **skip-on-empty**: if its slice
of `data` is missing or empty, it returns an empty string (no section header,
no callout). The numbered list above is the ordering; only sections with
actual content are present in the final doc.

### Named-privates integration

For v1 the curator is a callable injected at template-init time. SPEC_062
ships the real one; this spec ships a stub `_default_named_operators_curator`
that returns `{"items": [], "note": "Curator not yet integrated (SPEC_062)."}`.
This decouples timing — the template can ship without blocking on SPEC_062.

## Files to Create/Modify

| File                                                | Action  | Description |
|-----------------------------------------------------|---------|-------------|
| `docs/specs/SPEC_061_market_intelligence_pack.md`   | Create  | This file |
| `tests/test_spec_061_market_intelligence_pack.py`   | Create  | Skeleton + T1-T10 |
| `docs/specs/.active_spec`                           | Modify  | → `SPEC_061_market_intelligence_pack` |
| `app/reports/templates/market_intelligence_pack.py` | Create  | The template (~600 LOC est.) |
| `app/reports/builder.py`                            | Modify  | One-line registration |

## Feedback History

_No corrections yet._
