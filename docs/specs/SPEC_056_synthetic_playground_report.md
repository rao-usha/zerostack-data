# SPEC 056 — Synthetic Playground Report Template + CTA

**Status:** Draft
**Task type:** report
**Date:** 2026-05-14
**Test file:** tests/test_spec_056_synthetic_playground_report.py
**Plan:** [PLAN_063](../plans/PLAN_063_synthetic_playground.md) — Components C5 + C7 / Step 5

## Goal

Turn a synthetic-generator run into a polished, self-contained HTML report carrying a "Made with Nexdata" CTA. This is the shareable artifact — the viral loop's payload. New report template registered in the existing `ReportBuilder`; the `reports` table gains a `short_code` so reports can be served at a public `/p/<code>` URL (Step 6/8). The CTA block (C7) ships *in* this template — it is the growth mechanism, not a deferred admin feature.

## Acceptance Criteria

- [ ] New `SyntheticPlaygroundTemplate` in `app/reports/templates/synthetic_playground.py`, inheriting `ICReportBase`, with class attrs `name = "synthetic_playground"`, `display_name`, `description`.
- [ ] `gather_data(db, params)` accepts a router-normalized payload — `{generator, generator_label, request_params, summary: {label: value}, rows: [{col: val}], chart: {labels, series, y_label} | None}` — and returns it shaped for rendering (no DB query needed; the playground passes generator output directly).
- [ ] `render_html(data)` returns a full self-contained HTML document via `design_system.html_document` (inline CSS/JS, Chart.js with CSS fallback). Includes: a `page_header`, a `kpi_strip` from `summary`, an optional chart from `chart`, a ≤20-row `data_table` from `rows`, and a CTA block.
- [ ] The CTA block is wrapped in a **stable anchor** — an HTML comment `<!-- CTA_BLOCK_PLACEHOLDER -->` immediately before `<div id="nexdata-cta">…</div>` — so Step 6/8 code can locate it. CTA copy: headline "Made with Nexdata — this is a synthetic sample. The full platform runs on your real data."; a primary "See the platform" link and a secondary "Run your own" link to `/playground.html`; a generator-aware vertical hint; links carry a `?ref=` placeholder + `?from=playground&gen=<generator>` params.
- [ ] `render_html` is resilient: missing/empty `rows`, missing `chart`, missing `summary` all render without raising (graceful "no data" states).
- [ ] `render_excel(data)` raises `NotImplementedError` (playground only requests HTML; explicit stub for clarity).
- [ ] `SyntheticPlaygroundTemplate` registered in `ReportBuilder.__init__`'s `self.templates` dict under `"synthetic_playground"`.
- [ ] `ReportBuilder._ensure_table()` additionally runs (idempotent): `ALTER TABLE reports ADD COLUMN IF NOT EXISTS short_code VARCHAR(16)`, `ADD COLUMN IF NOT EXISTS is_public BOOLEAN DEFAULT FALSE`, `ADD COLUMN IF NOT EXISTS view_count INTEGER DEFAULT 0`, plus `CREATE UNIQUE INDEX IF NOT EXISTS idx_reports_short_code ON reports(short_code)`. Wrapped so a re-run / non-Postgres backend can't crash builder construction.

## Test Cases

| ID | Test Name | What It Verifies |
|----|-----------|------------------|
| T1 | test_template_registered | `ReportBuilder(db).templates` contains `"synthetic_playground"` |
| T2 | test_gather_data_shapes_payload | `gather_data` passes through generator/summary/rows/chart and tolerates missing keys |
| T3 | test_render_html_is_self_contained | `render_html` returns `<!DOCTYPE html>` … `</html>` with inline `<style>` and no external CSS link |
| T4 | test_render_html_has_cta_anchor | output contains `<!-- CTA_BLOCK_PLACEHOLDER -->` and `id="nexdata-cta"` and the headline copy |
| T5 | test_render_html_each_generator | renders for `private-financials`, `macro-scenarios`, `consumer-crowd` payloads without raising |
| T6 | test_render_html_empty_rows_graceful | empty `rows` / missing `chart` / missing `summary` still render a valid document |
| T7 | test_render_excel_not_implemented | `render_excel` raises `NotImplementedError` |
| T8 | test_reports_short_code_column_exists | after `ReportBuilder(db)`, the `reports` table has `short_code`, `is_public`, `view_count` columns |

## Rubric Checklist

_No `report` rubric file exists in `memory/rubrics/` — generic checklist:_

- [ ] Template inherits `ICReportBase`, uses `design_system` helpers (no ad-hoc HTML scaffolding)
- [ ] Output is fully self-contained (inline CSS/JS) — shareable as a single file
- [ ] Renders gracefully with missing/empty data — never raises on a thin payload
- [ ] CTA anchor is stable and documented (other steps depend on it)
- [ ] Schema change idempotent (`ADD COLUMN IF NOT EXISTS`, `CREATE UNIQUE INDEX IF NOT EXISTS`)
- [ ] Registered in `ReportBuilder` so `POST /reports/generate` and the playground router can both reach it
- [ ] Tests cover registration, each generator, empty data, the CTA anchor, the schema change

## Design Notes

```python
# app/reports/templates/synthetic_playground.py
class SyntheticPlaygroundTemplate(ICReportBase):
    name = "synthetic_playground"
    display_name = "Synthetic Data Playground Report"
    description = "Shareable report for a Nexdata synthetic-data generator run."

    def gather_data(self, db, params) -> dict:        # pass-through reshape
    def render_html(self, data) -> str:               # design_system.html_document(...)
    def render_excel(self, data):                     # raise NotImplementedError

# CTA block — folds C7 into the template
CTA_HEADLINE = "Made with Nexdata — this is a synthetic sample. The full platform runs on your real data."
# generator -> most-relevant enterprise vertical hint
_VERTICAL_HINT = {
    "private-financials": "insurance underwriting & litigation finance",
    "macro-scenarios": "datacenter & IRA-energy investor intel",
    "consumer-crowd": "insurance underwriting (consumer risk)",
}
```

The router (Step 6) owns generator-output → normalized-payload translation; this template only renders the normalized payload. CTA links fall back to `/playground.html` anchors until canonical marketing URLs exist (PLAN_063 open decision #3).

## Files to Create/Modify

| File | Action | Description |
|------|--------|-------------|
| `app/reports/templates/synthetic_playground.py` | Create | `SyntheticPlaygroundTemplate` + CTA block |
| `app/reports/builder.py` | Modify | import + register template; `short_code`/`is_public`/`view_count` columns in `_ensure_table()` |
| `tests/test_spec_056_synthetic_playground_report.py` | Create | T1–T8 |

## Feedback History

_No corrections yet._
