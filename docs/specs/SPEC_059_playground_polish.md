# SPEC 059 — Synthetic Playground Post-MVP Polish

**Status:** Draft
**Task type:** api_endpoint
**Date:** 2026-05-15
**Test file:** tests/test_spec_059_playground_polish.py

## Goal

Replace the generic `_normalize()` in `app/api/v1/playground.py` with per-generator
normalizers so each generator produces a clean, chart-bearing report payload
(macro-scenarios in particular: flatten the nested `scenarios[*].paths[series]`
into median path lines + a terminal-percentile rows table). Make the CTA links
in the synthetic-playground report template configurable via settings, defaulting
to the current in-app anchors. This finishes the two open polish items from
PLAN_063 ("Post-MVP polish" — per-generator `/run` normalization, canonical
marketing URLs for the CTA).

## Acceptance Criteria

- [ ] `app/api/v1/playground.py` dispatches to dedicated `_normalize_<generator>`
      functions for each of `private-financials`, `macro-scenarios`,
      `consumer-crowd`, with the current generic `_normalize` retained as a
      fallback for unknown generators.
- [ ] `_normalize_macro` produces a `chart` dict shaped
      `{"labels": [str, ...], "series": [{"label": str, "data": [float, ...]}, ...], "y_label": "..."}`
      where each series carries the **median path** across the run's scenarios
      for one FRED series, and `rows` is the per-series terminal-percentile
      table (`[{"series","current","p10","p50","p90"}, ...]`).
- [ ] `_normalize_private_financials` produces a `summary` with sector, peer
      count, synthetic count, and the three ratio means rounded to 2dp; `rows`
      = the `companies` list unchanged.
- [ ] `_normalize_consumer_crowd` produces a `summary` containing the scenario
      category/event/description plus key aggregate scalars; `rows` = the
      per-persona response list.
- [ ] The synthetic-playground report template reads CTA URLs from new settings
      fields `playground_cta_platform_url` and `playground_cta_run_url`, with
      defaults matching the current behavior. Settings values are honored
      verbatim — neither the template nor the router appends extra query
      params if the configured URL already supplies them.
- [ ] All existing PLAN_063 tests (SPEC_052–058) still pass unchanged. The new
      spec adds ≥6 tests covering the four bullets above.

## Test Cases

| ID  | Test Name                                              | What It Verifies                                                                        |
|-----|--------------------------------------------------------|-----------------------------------------------------------------------------------------|
| T1  | test_normalize_macro_builds_median_path_chart          | Multi-scenario macro input → `chart` has one series per FRED series, median elementwise |
| T2  | test_normalize_macro_terminal_percentile_rows          | `rows` is one row per series with `series, current, p10, p50, p90` columns              |
| T3  | test_normalize_private_financials_summary_and_rows     | Summary includes sector + counts + 3 ratio means; rows = `companies` unchanged          |
| T4  | test_normalize_consumer_crowd_summary_includes_scenario| Summary carries scenario metadata + at least one aggregate scalar; rows = responses     |
| T5  | test_normalize_unknown_generator_falls_back_to_generic | An unknown generator name still produces a sane summary+rows payload                    |
| T6  | test_cta_urls_use_settings_defaults_when_unset         | With no env override, rendered HTML contains the in-app anchor URLs                     |
| T7  | test_cta_urls_honor_settings_override                  | Monkey-patched settings → rendered HTML contains the override URLs verbatim             |

## Rubric Checklist

_(No `api_endpoint.md` rubric present — using a generic api-endpoint checklist.)_

- [ ] Endpoint behavior unchanged for the public contract (HTTP method, path,
      request body, success status). Only the response `result`/`preview_rows`
      shapes get richer per-generator content.
- [ ] All new code paths have ≥1 unit test.
- [ ] No SQL string concatenation; existing parameterized queries preserved.
- [ ] No new dependencies — uses numpy (already imported by generators) only
      via the generator outputs, not directly in the router.
- [ ] Defensive: each per-generator normalizer tolerates missing keys without
      raising (returns an empty `chart` / empty `rows` where data is absent).
- [ ] No PII / no scraping concerns — pure in-memory reshape.
- [ ] Errors logged via existing `logger`; no `print()`.

## Design Notes

### Per-generator normalizers (router)

```python
def _normalize_macro(raw, params) -> dict:
    series_list = raw.get("series") or []
    scenarios   = raw.get("scenarios") or []
    summary_in  = raw.get("summary") or {}
    current     = raw.get("current_values") or {}
    horizon     = int(raw.get("horizon_months") or 0)

    # Median path per series, computed elementwise across scenarios
    chart_series = []
    for s in series_list:
        paths = [sc["paths"][s] for sc in scenarios
                 if isinstance(sc.get("paths"), dict) and s in sc["paths"]]
        if not paths:
            continue
        L = min(len(p) for p in paths)
        median = [float(np.median([p[t] for p in paths])) for t in range(L)]
        chart_series.append({"label": s, "data": median})

    labels = [f"M{t+1}" for t in range(horizon)]

    rows = [
        {
            "series": s,
            "current": current.get(s),
            "p10": (summary_in.get(s) or {}).get("p10_terminal"),
            "p50": (summary_in.get(s) or {}).get("p50_terminal"),
            "p90": (summary_in.get(s) or {}).get("p90_terminal"),
        }
        for s in series_list
    ]

    return {
        "generator": "macro-scenarios",
        "generator_label": GENERATORS["macro-scenarios"]["label"],
        "request_params": {k: v for k, v in params.items() if v is not None},
        "summary": {
            "Scenarios":   raw.get("n_scenarios"),
            "Horizon":     f"{horizon} mo",
            "Series":      len(series_list),
            "History (mo)":raw.get("training_history_months"),
        },
        "rows": rows,
        "chart": {"labels": labels, "series": chart_series, "y_label": "Level"},
    }
```

Median (not mean) across scenarios keeps tails (esp. unemployment & sentiment)
from compressing the visual.

```python
def _normalize_private_financials(raw, params) -> dict:
    rs = raw.get("ratio_stats") or {}
    fmt = lambda d, k: round(float((d.get(k) or {}).get("mean", 0.0)), 4)
    return {
        ...
        "summary": {
            "Sector":              raw.get("sector"),
            "Peer Count":          raw.get("peer_count"),
            "Synthetic Companies": raw.get("synthetic_count"),
            "Gross Margin (mean)": fmt(rs, "gross_margin"),
            "EBITDA Margin (mean)":fmt(rs, "ebitda_margin"),
            "Net Margin (mean)":   fmt(rs, "net_margin"),
        },
        "rows": raw.get("companies") or [],
        "chart": None,
    }

def _normalize_consumer_crowd(raw, params) -> dict:
    scen = raw.get("scenario") or {}
    agg  = raw.get("aggregate") or {}
    summary = {
        "Scenario":    scen.get("category"),
        "Event":       scen.get("event_type"),
        "Description": scen.get("description"),
    }
    for k, v in agg.items():
        if isinstance(v, (int, float)) and len(summary) < 6:
            summary[_titleize(k)] = round(v, 3) if isinstance(v, float) else v
    return {
        ...,
        "summary": summary,
        "rows":    raw.get("responses") or [],
        "chart":   None,
    }
```

The existing `_normalize` (generic) stays as a fallback for any future
generator added before its dedicated normalizer exists.

### Dispatch

```python
_NORMALIZERS = {
    "private-financials": _normalize_private_financials,
    "macro-scenarios":    _normalize_macro,
    "consumer-crowd":     _normalize_consumer_crowd,
}

def _normalize_dispatch(generator, raw, params):
    fn = _NORMALIZERS.get(generator)
    return fn(raw, params) if fn else _normalize(generator, raw, params)
```

`run_generator` swaps its `_normalize(...)` call for `_normalize_dispatch(...)`.

### CTA URL config

Two new settings fields in `app/core/config.py`:

```python
playground_cta_platform_url: str = Field(
    default="/playground.html?from=playground#platform",
    description="URL the 'See the platform' CTA button points to. "
                "`{gen}` and `{ref}` placeholders are substituted if present.",
)
playground_cta_run_url: str = Field(
    default="/playground.html?from=playground",
    description="URL the 'Run your own' CTA button points to. "
                "Same placeholder substitution as above.",
)
```

`SyntheticPlaygroundTemplate._render_cta_block` now formats those templates
with `{gen=..., ref=...}` using `str.format_map` against a `defaultdict(str)`,
so a configured URL without placeholders is honored verbatim (and a URL with
`{gen}`/`{ref}` gets per-run attribution).

## Files to Create/Modify

| File                                                       | Action  | Description                                                             |
|------------------------------------------------------------|---------|-------------------------------------------------------------------------|
| `app/api/v1/playground.py`                                  | Modify  | Add `_normalize_macro/_private_financials/_consumer_crowd` + dispatch.  |
| `app/reports/templates/synthetic_playground.py`             | Modify  | Read CTA URLs from settings; placeholder substitution for `gen`/`ref`.  |
| `app/core/config.py`                                        | Modify  | Two new fields: `playground_cta_platform_url`, `playground_cta_run_url`.|
| `tests/test_spec_059_playground_polish.py`                  | Create  | 7 unit tests (T1–T7).                                                   |
| `docs/specs/SPEC_059_playground_polish.md`                  | Create  | This file.                                                              |
| `docs/specs/.active_spec`                                   | Modify  | Set to `SPEC_059_playground_polish`.                                    |

## Feedback History

_No corrections yet._
