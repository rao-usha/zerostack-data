# SPEC 060 — Diligence Pack Taxonomies + Census CBP Coverage Audit

**Status:** Draft
**Task type:** service
**Date:** 2026-05-16
**Plan:** PLAN_065 (Sector × Market Intelligence Pack)
**Test file:** tests/test_spec_060_taxonomies_cbp_audit.py

## Goal

Build the foundational reference data + lookup module for PLAN_065's intake
UI and report-template work, and run a one-time coverage audit against the
cloud `census_business_patterns` and `census_cbp` tables to **decide the
intake taxonomy grain before SPEC_064 ships an MSA picker the data can't
back**. Three open-data dictionaries (NAICS 2022, OMB MSA delineation,
NAICS↔SIC crosswalk) are committed to `data/reference/` as JSON; a pure-
Python loader exposes O(1) lookups; an audit script writes a CSV showing
NAICS × county / state coverage for both CBP tables on cloud.

## Acceptance Criteria

- [ ] `data/reference/naics_2022.json` includes every NAICS 2-, 3-, 4-, 5-,
      and 6-digit code with `{code, label, parent_code}`; loaded as a tree.
- [ ] `data/reference/msa.json` includes all 384 official MSAs (per OMB 2023
      delineation file) with `{cbsa_code, title, state_abbrs, county_fips_list}`.
- [ ] `data/reference/naics_sic_crosswalk.json` maps NAICS-4 → list of SIC
      codes that occur in `sec_company_metadata` on cloud (built empirically,
      not just from Census's static crosswalk, so we know it's actually useful).
- [ ] `app/services/diligence/taxonomies.py` provides:
  - `load_naics() -> dict[str, NaicsNode]`
  - `naics_label(code) -> str` (raises `ValueError` on invalid)
  - `naics_parents(code) -> list[str]` (chain up to 2-digit sector)
  - `naics_children(code) -> list[str]`
  - `load_msa() -> dict[str, MsaRecord]`
  - `msa_title(cbsa_code) -> str`
  - `msa_counties(cbsa_code) -> list[str]` (5-digit county FIPS)
  - `state_counties(state_fips) -> list[str]`
  - `naics_to_sic(naics_code) -> list[str]`
- [ ] All loaders are cached at module level — re-imports don't re-parse JSON.
- [ ] `scripts/audit_cbp_coverage.py` writes `data/reference/cbp_coverage_2026-05-16.csv`
      with one row per `(table, naics_4, geo_level, geo_code)` populated tuple
      and a summary block at the top: which CBP table to prefer, max NAICS
      depth available, geo coverage % for top 50 MSAs.
- [ ] Audit script's summary block explicitly answers: **"Is NAICS-4 × MSA
      feasible from either CBP table?"** with yes / no / partial + rationale.
      That answer is the gate for SPEC_064's intake UX.
- [ ] No existing tests broken (`pytest tests/test_config.py tests/test_spec_059_playground_polish.py` green).

## Test Cases

| ID | Test Name | What It Verifies |
|----|-----------|------------------|
| T1 | test_naics_lookup_roundtrip | code → label → code roundtrips for 2/4/6-digit codes |
| T2 | test_naics_parents_chain | parents("332323") returns ["332", "33", and the 4/5-digit] |
| T3 | test_naics_invalid_code_raises | `naics_label("99999")` raises ValueError, not None |
| T4 | test_msa_has_384_records | MSA dict carries the full OMB 2023 set |
| T5 | test_msa_counties_nonempty | every MSA has ≥1 constituent county FIPS |
| T6 | test_naics_to_sic_covers_sec_companies | every NAICS-4 in the crosswalk has ≥1 SIC that actually appears in `sec_company_metadata` (pure JSON check; no live DB) |
| T7 | test_loaders_cached | second call to `load_naics()` is identity-equal to the first |
| T8 | test_state_counties_returns_expected_count | `state_counties("48")` (Texas) returns 254 FIPS codes |

## Rubric Checklist

_(No `service.md` rubric in memory; using a generic service checklist.)_

- [ ] Module is pure-Python — no DB connection at import time.
- [ ] Loaders are idempotent + cached (lru_cache or module-level dict).
- [ ] All lookup failures raise specific exceptions (`ValueError`), never
      silently return `None` — surfaces bugs early.
- [ ] Reference data committed as **JSON, not CSV/Parquet** — readable in
      diffs, no extra dependencies.
- [ ] Reference data files include a `_meta` block: source URL, fetched_at,
      hash — so we can verify and refresh without re-deriving.
- [ ] Audit script tolerates cloud-proxy outage gracefully (writes partial
      CSV, exits 0 with a warning).
- [ ] Audit output goes to `data/reference/cbp_coverage_<date>.csv` — not
      stdout — so it can be committed and reviewed.
- [ ] No PII / no scraping / no API keys required (Census + OMB are static
      open-data downloads).

## Design Notes

### Reference-data sources

| File | Source | Notes |
|---|---|---|
| `naics_2022.json` | Census NAICS 2022 master file (`https://www.census.gov/naics/2022NAICS/2-6%20digit_2022_Codes.xlsx`) | Convert to JSON; ~2k entries |
| `msa.json` | OMB 2023 MSA delineation (`https://www.census.gov/geographies/reference-files/time-series/demo/metro-micro/delineation-files.html`) | ~384 MSAs + constituent counties |
| `naics_sic_crosswalk.json` | Hybrid: start with Census NAICS-2022→SIC crosswalk, then *filter* to keep only SICs that actually appear in `sec_company_metadata` on cloud. Built once via `scripts/build_naics_sic_crosswalk.py` (deliverable here). | ~600 useful entries |

Each file carries:

```json
{
  "_meta": { "source": "...", "fetched_at": "2026-05-16T...", "row_count": ... },
  "data": { ... }
}
```

### Module shape

```python
# app/services/diligence/taxonomies.py
from functools import lru_cache
from pathlib import Path
import json
from typing import NamedTuple

REF_DIR = Path(__file__).resolve().parents[2] / "data" / "reference"

class NaicsNode(NamedTuple):
    code: str           # "332"
    label: str
    parent_code: str | None
    digits: int

class MsaRecord(NamedTuple):
    cbsa_code: str      # "26420"
    title: str          # "Houston-The Woodlands-Sugar Land, TX"
    state_abbrs: list[str]
    county_fips_list: list[str]

@lru_cache(maxsize=1)
def load_naics() -> dict[str, NaicsNode]: ...
@lru_cache(maxsize=1)
def load_msa() -> dict[str, MsaRecord]: ...
@lru_cache(maxsize=1)
def load_naics_sic_crosswalk() -> dict[str, list[str]]: ...

def naics_label(code: str) -> str: ...
def naics_parents(code: str) -> list[str]: ...
def naics_children(code: str) -> list[str]: ...
def msa_title(cbsa: str) -> str: ...
def msa_counties(cbsa: str) -> list[str]: ...
def state_counties(state_fips: str) -> list[str]: ...
def naics_to_sic(naics: str) -> list[str]: ...
```

### Audit script shape

```python
# scripts/audit_cbp_coverage.py
"""One-off audit: report Census CBP NAICS × geo coverage on cloud.
Output: data/reference/cbp_coverage_<YYYY-MM-DD>.csv

Summary block at top of CSV answers the gate question:
'Is NAICS-4 × MSA feasible from either CBP table?'
"""
# 1. Connect to cloud via the proxy (DATABASE_URL or hardcoded local-proxy URL).
# 2. For census_business_patterns (state × NAICS × year):
#    - count rows per (year, naics_digits)
#    - top-50 state coverage
# 3. For census_cbp (year × naics × geo_level × county):
#    - row count per (year, naics_digits, geo_level)
#    - top-50 MSA coverage (by aggregating county FIPS → MSA via the SPEC_060 MSA dict)
#    - per top-50 MSA, % of constituent counties that have any CBP row
# 4. Pick the preferred CBP table per intake-grain question.
# 5. Write CSV with summary header + per-NAICS rows.
```

The script depends on `app.services.diligence.taxonomies` for the MSA dict —
it's the first real consumer.

### What the audit's verdict gates

If audit says **"NAICS-4 × MSA is feasible from `census_cbp`"** → SPEC_064
intake ships an MSA picker as planned.

If audit says **"only state-grain works"** → PLAN_065 §4 SPEC_064 is updated
in this session before the spec is built; intake becomes NAICS × state with
optional county/MSA narrowing for OTHER sections (ACS, IRS SOI, FEMA — which
work at finer grain).

If audit says **"partial — some MSAs covered, others sparse"** → intake
shows an MSA picker but greys out under-covered MSAs, or surfaces a
"limited structural data for this geography" warning at order time.

## Files to Create/Modify

| File | Action | Description |
|---|---|---|
| `docs/specs/SPEC_060_taxonomies_cbp_audit.md` | Create | This file |
| `tests/test_spec_060_taxonomies_cbp_audit.py` | Create | Skeleton tests (T1–T8) |
| `docs/specs/.active_spec` | Modify | Set to `SPEC_060_taxonomies_cbp_audit` |
| `data/reference/naics_2022.json` | Create | NAICS 2022 dict (~2k entries) |
| `data/reference/msa.json` | Create | OMB MSA delineation (~384 records) |
| `data/reference/naics_sic_crosswalk.json` | Create | Built by helper script |
| `app/services/diligence/__init__.py` | Create | New package |
| `app/services/diligence/taxonomies.py` | Create | Loaders + lookups |
| `scripts/build_naics_sic_crosswalk.py` | Create | One-off builder for the crosswalk JSON |
| `scripts/audit_cbp_coverage.py` | Create | One-off coverage audit (CSV output) |
| `data/reference/cbp_coverage_2026-05-16.csv` | Create (via script) | Coverage audit result |

## Findings (post-audit, 2026-05-17 UTC run)

| Question | Result |
|---|---|
| `census_business_patterns` grain | state × NAICS, full NAICS depth (661×2d, 3,236×3d, 10,059×4d, 21,005×5d, 28,408×6d) |
| `census_cbp` grain | county × NAICS-6 only (7,160 rows, 2,792 counties, 55 states/terr.) |
| MSA coverage in `census_cbp` | 359/393 MSAs (91.3%) have ≥90% of their counties present; 386/393 (98.2%) have ≥50% |
| Initial verdict | NO (strict: required literal NAICS-4 in census_cbp) |
| **Final verdict** | **YES with one aggregation step** — NAICS-6 county rows roll up cleanly to NAICS-4 via prefix truncation + SUM |
| Intake decision (closes PLAN_065 task 23) | **NAICS-4 × MSA** (preserves v2 intake UX) — see SPEC_061 §3 for rollup logic |

The 7 MSAs with <50% county coverage in `census_cbp` get a "limited structural-density data" warning in the SPEC_061 report template.

## Feedback History

_No corrections yet._
