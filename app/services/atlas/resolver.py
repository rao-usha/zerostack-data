"""
Atlas query resolver — SPEC_064.

Turns a free-text query ("Houston building equipment contractors") OR direct
`msa` + `naics` params into a `ResolvedEntities` object: an MSA, a NAICS-4
industry, the MSA's constituent counties, and a stable slug.

Resolution is deliberately simple + deterministic for v1 (token matching
against the SPEC_060 taxonomy dicts) — no LLM, no fuzzy-search dependency.
The smoke-test query is guaranteed to resolve; broader NL parsing is a
future enrichment.
"""

from __future__ import annotations

import re
from typing import Dict, List, Optional, Tuple

from app.services.atlas.types import ResolvedEntities
from app.services.diligence.taxonomies import (
    load_msa,
    load_naics,
    msa_counties,
)

# Tokens that carry no matching signal — dropped before scoring.
_STOPWORDS = {
    "the", "and", "of", "in", "for", "a", "an", "to", "with", "on",
    "all", "other", "n.e.c.", "nec", "&", "services", "service",
}

_TOKEN_RE = re.compile(r"[a-z0-9]+")


def _tokens(text: str) -> List[str]:
    return [t for t in _TOKEN_RE.findall((text or "").lower()) if t not in _STOPWORDS]


def slugify(*parts: str) -> str:
    """Build a stable, URL-safe slug from the parts."""
    raw = "-".join(p for p in parts if p)
    raw = raw.lower()
    raw = re.sub(r"[^a-z0-9]+", "-", raw).strip("-")
    return raw or "exploration"


# ─────────────────────────────────────────────────────────────────────────────
# MSA resolution
# ─────────────────────────────────────────────────────────────────────────────

def _msa_principal_tokens(title: str) -> List[str]:
    """The matchable tokens of an MSA title — its principal-city segment.

    "Houston-Pasadena-The Woodlands, TX" → ['houston', 'pasadena', 'woodlands']
    We keep all city tokens (drop the trailing state) so 'Pasadena' also hits.
    """
    # Drop the ", ST" state suffix.
    city_part = title.split(",")[0]
    return _tokens(city_part)


def resolve_msa(query_tokens: List[str]) -> Optional[Dict[str, str]]:
    """Find the best MSA whose principal-city tokens appear in the query."""
    msa = load_msa()
    best: Optional[Tuple[int, str]] = None  # (score, cbsa_code)
    for cbsa, rec in msa.items():
        city_tokens = _msa_principal_tokens(rec.title)
        if not city_tokens:
            continue
        hits = sum(1 for t in city_tokens if t in query_tokens)
        if hits == 0:
            continue
        # Prefer more hits; tie-break on the shorter (more specific) title.
        score = hits
        if best is None or score > best[0]:
            best = (score, cbsa)
    if best is None:
        return None
    rec = msa[best[1]]
    return {"code": rec.cbsa_code, "title": rec.title}


# ─────────────────────────────────────────────────────────────────────────────
# NAICS resolution
# ─────────────────────────────────────────────────────────────────────────────

def resolve_naics(query_tokens: List[str]) -> Optional[Dict[str, str]]:
    """Find the best NAICS-4 industry whose label tokens overlap the query.

    Scored by (label tokens present in query) / (total label tokens) — the
    fraction of the industry name that the query actually mentions. A high
    fraction means the query is *about* that industry, not just brushing it.
    """
    naics = load_naics()
    qset = set(query_tokens)
    best: Optional[Tuple[float, int, str]] = None  # (fraction, hits, code)
    for code, node in naics.items():
        if node.digits != 4:
            continue
        label_tokens = _tokens(node.label)
        if not label_tokens:
            continue
        hits = sum(1 for t in label_tokens if t in qset)
        if hits == 0:
            continue
        fraction = hits / len(label_tokens)
        score = (fraction, hits, code)
        if best is None or score > best:
            best = score
    # Require at least half the industry name to be present — avoids a single
    # incidental word ("equipment") matching an unrelated industry.
    if best is None or best[0] < 0.5:
        return None
    code = best[2]
    return {"code": code, "label": naics[code].label}


# ─────────────────────────────────────────────────────────────────────────────
# Top-level resolve
# ─────────────────────────────────────────────────────────────────────────────

def resolve(
    query: str,
    msa: Optional[str] = None,
    naics: Optional[str] = None,
) -> Tuple[ResolvedEntities, str]:
    """Resolve a query (+ optional direct overrides) into entities + a slug.

    Direct `msa` / `naics` params win over text inference — the API exposes
    them so the frontend's picker path can skip NL parsing entirely.

    Returns (ResolvedEntities, slug). Raises ValueError if neither an MSA nor
    a NAICS could be resolved (an exploration needs at least one anchor).
    """
    msa_dict = load_msa()
    naics_dict = load_naics()
    qtokens = _tokens(query)

    # ── MSA ──────────────────────────────────────────────────────────────────
    resolved_msa: Optional[Dict[str, str]] = None
    if msa:
        rec = msa_dict.get(str(msa))
        if rec:
            resolved_msa = {"code": rec.cbsa_code, "title": rec.title}
        else:
            raise ValueError(f"Unknown MSA code: {msa!r}")
    else:
        resolved_msa = resolve_msa(qtokens)

    # ── NAICS ────────────────────────────────────────────────────────────────
    resolved_naics: Optional[Dict[str, str]] = None
    if naics:
        node = naics_dict.get(str(naics))
        if not node:
            raise ValueError(f"Unknown NAICS code: {naics!r}")
        if node.digits != 4:
            raise ValueError(f"naics must be a 4-digit industry code; got {naics!r}")
        resolved_naics = {"code": node.code, "label": node.label}
    else:
        resolved_naics = resolve_naics(qtokens)

    if not resolved_msa and not resolved_naics:
        raise ValueError(
            f"Could not resolve a market or sector from query {query!r}. "
            f"Try naming a metro area and an industry — e.g. "
            f"'Houston building equipment contractors'."
        )

    # ── Geographies ──────────────────────────────────────────────────────────
    geographies: List[str] = []
    if resolved_msa:
        try:
            geographies = msa_counties(resolved_msa["code"])
        except ValueError:
            geographies = []

    entities = ResolvedEntities(
        msa=resolved_msa,
        naics=resolved_naics,
        geographies=geographies,
        datasets=[],  # populated by the card builder once it knows what had data
    )

    slug = slugify(
        (resolved_msa or {}).get("title", "").split(",")[0] if resolved_msa else "",
        (resolved_naics or {}).get("label", "") if resolved_naics else "",
    )
    return entities, slug
