"""
NAICS / MSA / NAICS↔SIC taxonomy loaders + lookups for PLAN_065 SPEC_060.

Three reference dictionaries, all sourced from Census + OMB open data and
committed under `data/reference/`:

  * naics_2022.json           — Census NAICS 2022, 2/3/4/5/6-digit codes
  * msa.json                  — OMB 2023 Metropolitan Statistical Areas with
                                constituent county FIPS
  * naics_sic_crosswalk.json  — NAICS-4 → list of SICs, *empirically filtered*
                                to SICs actually present in
                                `sec_company_metadata` on cloud

Pure-Python: no DB at import time. Each loader is module-level cached
(`lru_cache(maxsize=1)`) so re-imports are free and dict identity is stable
across calls (test T7 relies on this).

All lookup failures raise `ValueError` rather than silently returning None —
surfaces bugs early, matches the "fail loud at the boundary" convention used
elsewhere in `app/services/`.

Consumed by SPEC_061 (Market Intelligence Pack report template), SPEC_063
(diligence orders intake API — validates NAICS + MSA codes against these
dicts), and SPEC_064 (frontend intake page — fetches both dicts via the
`/api/v1/diligence/taxonomies` endpoint).
"""

from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Dict, List, NamedTuple

# Repo root → data/reference/
# This file lives at app/services/diligence/taxonomies.py, so go up 3 levels
# to reach repo root, then into data/reference/.
REF_DIR = Path(__file__).resolve().parents[3] / "data" / "reference"


# ── Record types ────────────────────────────────────────────────────────────

class NaicsNode(NamedTuple):
    code: str               # e.g. "332"
    label: str
    parent_code: str | None
    digits: int


class MsaRecord(NamedTuple):
    cbsa_code: str          # e.g. "26420"
    title: str              # e.g. "Houston-Pasadena-The Woodlands, TX"
    state_abbrs: List[str]
    county_fips_list: List[str]


# ── Loaders (cached) ────────────────────────────────────────────────────────

@lru_cache(maxsize=1)
def load_naics() -> Dict[str, NaicsNode]:
    """Load NAICS 2022 dict. Cached; second call returns the same dict object."""
    raw = _load_json("naics_2022.json")
    out: Dict[str, NaicsNode] = {}
    for code, rec in raw["data"].items():
        out[code] = NaicsNode(
            code=rec["code"],
            label=rec["label"],
            parent_code=rec.get("parent_code"),
            digits=int(rec["digits"]),
        )
    return out


@lru_cache(maxsize=1)
def load_msa() -> Dict[str, MsaRecord]:
    """Load OMB MSA delineation. Cached."""
    raw = _load_json("msa.json")
    out: Dict[str, MsaRecord] = {}
    for cbsa, rec in raw["data"].items():
        out[cbsa] = MsaRecord(
            cbsa_code=rec["cbsa_code"],
            title=rec["title"],
            state_abbrs=list(rec.get("state_abbrs") or []),
            county_fips_list=list(rec.get("county_fips_list") or []),
        )
    return out


@lru_cache(maxsize=1)
def load_naics_sic_crosswalk() -> Dict[str, List[str]]:
    """Load empirically-filtered NAICS-4 → [SIC, ...] crosswalk. Cached."""
    raw = _load_json("naics_sic_crosswalk.json")
    return {k: list(v) for k, v in raw["data"].items()}


def _load_json(filename: str) -> dict:
    path = REF_DIR / filename
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


# ── NAICS lookups ───────────────────────────────────────────────────────────

def naics_label(code: str) -> str:
    """Return the official NAICS label for `code`. Raises ValueError if unknown."""
    naics = load_naics()
    node = naics.get(code)
    if node is None:
        raise ValueError(f"Unknown NAICS code: {code!r}")
    return node.label


def naics_parents(code: str) -> List[str]:
    """Return the ancestor codes for `code`, ordered shortest → longest.

    For "332323" returns ["33", "332", "3323", "33232"]. Empty list for
    a 2-digit code. Raises ValueError if `code` itself is unknown.
    """
    naics = load_naics()
    if code not in naics:
        raise ValueError(f"Unknown NAICS code: {code!r}")
    parents: List[str] = []
    # NAICS hierarchy is strictly prefix-based: a 6-digit code's parents are
    # its 2/3/4/5-digit prefixes, all of which exist in the dict.
    for d in range(2, len(code)):
        prefix = code[:d]
        if prefix in naics:
            parents.append(prefix)
    return parents


def naics_children(code: str) -> List[str]:
    """Return the immediate-child codes for `code` (next digit deeper)."""
    naics = load_naics()
    if code not in naics:
        raise ValueError(f"Unknown NAICS code: {code!r}")
    target_digits = len(code) + 1
    return sorted(
        c for c, node in naics.items()
        if node.digits == target_digits and c.startswith(code)
    )


# ── MSA lookups ─────────────────────────────────────────────────────────────

def msa_title(cbsa_code: str) -> str:
    """Return the MSA title for `cbsa_code`. Raises ValueError if unknown."""
    msa = load_msa()
    rec = msa.get(cbsa_code)
    if rec is None:
        raise ValueError(f"Unknown MSA CBSA code: {cbsa_code!r}")
    return rec.title


def msa_counties(cbsa_code: str) -> List[str]:
    """Return constituent county FIPS for `cbsa_code`."""
    msa = load_msa()
    rec = msa.get(cbsa_code)
    if rec is None:
        raise ValueError(f"Unknown MSA CBSA code: {cbsa_code!r}")
    return list(rec.county_fips_list)


# ── State → counties helper ─────────────────────────────────────────────────
#
# We don't yet host a full national counties dictionary (~3,143 counties), so
# this helper returns only counties that appear in at least one MSA in the
# given state — a partial-but-defensible subset. For state-mode report
# generation (SPEC_061) this is enough for the largest population centers.
# Full national counties is a follow-up data task.

@lru_cache(maxsize=64)
def _state_county_index() -> Dict[str, List[str]]:
    """Build a state_fips → [county_fips, ...] index from the MSA dict."""
    msa = load_msa()
    by_state: Dict[str, set] = {}
    for rec in msa.values():
        for fips in rec.county_fips_list:
            state = fips[:2]
            by_state.setdefault(state, set()).add(fips)
    return {state: sorted(counties) for state, counties in by_state.items()}


def state_counties(state_fips: str) -> List[str]:
    """Return county FIPS in `state_fips` (2-digit). Partial coverage — see module docstring."""
    idx = _state_county_index()
    if state_fips not in idx:
        raise ValueError(
            f"No MSA-included counties for state FIPS {state_fips!r}. "
            "Either the state has no MSAs (uncommon) or the FIPS code is wrong."
        )
    return list(idx[state_fips])


# ── NAICS↔SIC ───────────────────────────────────────────────────────────────

def naics_to_sic(naics_code: str) -> List[str]:
    """Return the list of SICs mapped to `naics_code`. Empty list if unmapped.

    The crosswalk is keyed at NAICS-4. For longer codes we walk up to the
    NAICS-4 ancestor. For codes that have no entry in the crosswalk at all,
    we return an empty list rather than raising — consumers (SPEC_061 §11
    public-co lookup) treat "no matching SECs" as a normal skip-on-empty case.
    """
    xwalk = load_naics_sic_crosswalk()
    if len(naics_code) > 4:
        naics_code = naics_code[:4]
    return list(xwalk.get(naics_code, []))
