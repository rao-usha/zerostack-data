"""
Catalog search and facets (SPEC_145, PLAN_088 §3 "SPEC_139").

An in-memory inverted index over the catalog (150 specs) and the committed
column dictionary (``columns.generated.json``, ~2.7k columns). It is built once
per process and never touches the database: search is a dictionary walk.

Weights (PLAN_088): key / display name 5, tables 3, column names 2, any other
text 1 (subtitle, description, grain, keywords, source, limitations, column
descriptions). Query tokens are ANDed; each matches an indexed token exactly
or as a prefix (a prefix hit scores half). A phrase bonus rewards the whole
query appearing in the display name or key, so ``form d`` ranks
``sec_form_d`` first.

Facets are disjunctive: facet X is counted over the text matches filtered by
every other facet, so a selected kind still shows the other kinds' counts.

Row estimates never count rows (``row_estimates``): the SPEC_123 live cache
when it holds the dataset, else the planner estimates from the shared
``quality.relations`` cache.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import Any, Dict, FrozenSet, Iterable, List, Mapping, Optional, Sequence, Tuple

from app.catalog.spec import DatasetSpec

logger = logging.getLogger(__name__)

W_NAME = 5.0
W_TABLE = 3.0
W_COLUMN = 2.0
W_TEXT = 1.0
PREFIX_FACTOR = 0.5
PHRASE_BONUS = 5.0
MIN_PREFIX = 2          # a one-letter query token matches exactly only ("d" in "form d")
HIGHLIGHT_CHARS = 160
UNVERIFIED = "unverified"

# facet name -> the filter query parameter that selects it
FACETS = ("kind", "source", "status_public", "data_state", "redistribution",
          "effective_redistribution", "pii_class", "origin", "identifier", "keyword")
MULTI_VALUED = frozenset({"identifier", "keyword"})

_TOKEN_RE = re.compile(r"[a-z0-9]+")


def tokens(text: Optional[str]) -> List[str]:
    return _TOKEN_RE.findall((text or "").lower())


@dataclass
class Doc:
    spec: DatasetSpec
    # token -> (best weight, field name)
    terms: Dict[str, Tuple[float, str]] = field(default_factory=dict)
    texts: List[Tuple[str, str]] = field(default_factory=list)   # (field, text) for highlights
    facets: Dict[str, Tuple[str, ...]] = field(default_factory=dict)
    name_text: str = ""

    def add(self, text: Optional[str], weight: float, field_name: str, whole: bool = False) -> None:
        if not text:
            return
        toks = tokens(text)
        if whole:
            toks.append(text.lower())
        for t in toks:
            if t not in self.terms or self.terms[t][0] < weight:
                self.terms[t] = (weight, field_name)


def _identifier_types(columns: Iterable[Mapping[str, Any]]) -> Tuple[str, ...]:
    from app.catalog.identifiers import join_types

    jt = join_types()
    return tuple(sorted({c.get("semantic_type") for c in columns if c.get("semantic_type") in jt}))


def data_state_of(spec: DatasetSpec) -> str:
    from app.catalog.quality import verified_state

    return verified_state(spec) or UNVERIFIED


def build_doc(spec: DatasetSpec, columns: Sequence[Tuple[str, Mapping[str, Any]]]) -> Doc:
    """``columns``: (table, column entry) pairs from the dictionary."""
    d = Doc(spec=spec)
    d.add(spec.key, W_NAME, "key", whole=True)
    d.add(spec.display_name, W_NAME, "name")
    d.name_text = f"{spec.display_name} {spec.key.replace('_', ' ')}".lower()
    for t in tuple(spec.tables) + tuple(p.rstrip("*") for p in spec.table_patterns):
        d.add(t, W_TABLE, "tables", whole=True)
    for _t, c in columns:
        d.add(c.get("name"), W_COLUMN, "columns", whole=True)
    other = [("subtitle", spec.subtitle), ("description", spec.description), ("grain", spec.grain),
             ("keywords", " ".join(spec.keywords)), ("source", spec.source),
             ("limitations", " ".join(spec.limitations))]
    for name, text in other:
        d.add(text, W_TEXT, name)
        if text:
            d.texts.append((name, text))
    for _t, c in columns:
        if c.get("description"):
            d.add(c["description"], W_TEXT, "column_descriptions")
    d.facets = {
        "kind": (spec.kind,),
        "source": (spec.source,),
        "status_public": (spec.status_public,),
        "data_state": (data_state_of(spec),),
        "redistribution": (spec.redistribution,),
        "effective_redistribution": (spec.effective_redistribution,),
        "pii_class": (spec.pii_class,),
        "origin": (spec.origin,),
        "identifier": _identifier_types(c for _t, c in columns),
        "keyword": tuple(spec.keywords),
    }
    return d


@dataclass
class Index:
    docs: List[Doc]
    vocabulary: FrozenSet[str]


def build_index(specs: Optional[Sequence[DatasetSpec]] = None,
                body: Optional[Dict[str, Any]] = None) -> Index:
    from app.catalog.dictionary import load_dictionary
    from app.catalog.registry import get_catalog

    specs = list(specs if specs is not None else get_catalog())
    body = body if body is not None else load_dictionary()
    by_ds: Dict[str, List[Tuple[str, Mapping[str, Any]]]] = {}
    for table, e in body.get("tables", {}).items():
        for ds in e.get("datasets", []):
            by_ds.setdefault(ds, []).extend((table, c) for c in e.get("columns", []))
    docs = [build_doc(s, by_ds.get(s.key, [])) for s in specs]
    vocab = frozenset(t for d in docs for t in d.terms)
    return Index(docs=docs, vocabulary=vocab)


_INDEX_CACHE: Dict[str, Any] = {}


def default_index() -> Index:
    """The process index, rebuilt when its inputs change: the dictionary object
    (``load_dictionary`` is itself cached, so clearing it or reloading the module
    swaps it) or the catalog tuple. No file stat, no DB: an identity check per call."""
    from app.catalog.dictionary import load_dictionary
    from app.catalog.registry import get_catalog

    body, specs = load_dictionary(), get_catalog()
    stamp = (id(body), id(specs))
    if _INDEX_CACHE.get("stamp") != stamp:
        _INDEX_CACHE["index"] = build_index(specs, body)
        _INDEX_CACHE["stamp"] = stamp
    return _INDEX_CACHE["index"]


def clear_index() -> None:
    _INDEX_CACHE.clear()


# ---------------------------------------------------------------------------
# Query
# ---------------------------------------------------------------------------


def _token_score(doc: Doc, qt: str) -> Tuple[float, Optional[str]]:
    hit = doc.terms.get(qt)
    best, where = (hit[0], hit[1]) if hit else (0.0, None)
    if len(qt) >= MIN_PREFIX:
        for term, (w, f) in doc.terms.items():
            if w * PREFIX_FACTOR > best and term != qt and term.startswith(qt):
                best, where = w * PREFIX_FACTOR, f
    return best, where


def score(doc: Doc, q: str) -> Tuple[float, List[str]]:
    """(score, matched fields); score 0 = no match (a query token matched nothing)."""
    qtoks = tokens(q)
    if not qtoks:
        return 0.0, []
    total, matched = 0.0, []
    for qt in qtoks:
        s, where = _token_score(doc, qt)
        if s <= 0:
            return 0.0, []
        total += s
        if where not in matched:
            matched.append(where)
    phrase = " ".join(qtoks)
    if phrase in doc.name_text:
        total += PHRASE_BONUS
    return total, matched


def highlight(doc: Doc, q: str) -> Optional[str]:
    """A short plain-text snippet around the first query token found in the
    doc's text fields (or the subtitle / description when it matched by name)."""
    qtoks = tokens(q)
    for _name, text in doc.texts:
        low = text.lower()
        for qt in qtoks:
            i = low.find(qt)
            if i >= 0:
                start = max(0, i - HIGHLIGHT_CHARS // 3)
                snippet = text[start:start + HIGHLIGHT_CHARS].strip()
                return ("…" if start else "") + snippet + ("…" if start + HIGHLIGHT_CHARS < len(text) else "")
    return None


def _passes(doc: Doc, filters: Mapping[str, Sequence[str]], skip: Optional[str] = None) -> bool:
    for name, wanted in filters.items():
        if name == skip or not wanted:
            continue
        if not set(doc.facets.get(name, ())) & set(wanted):
            return False
    return True


def facet_counts(docs: Sequence[Doc], filters: Mapping[str, Sequence[str]]) -> Dict[str, Dict[str, int]]:
    out: Dict[str, Dict[str, int]] = {}
    for name in FACETS:
        counts: Dict[str, int] = {}
        for d in docs:
            if not _passes(d, filters, skip=name):
                continue
            for v in d.facets.get(name, ()):
                counts[v] = counts.get(v, 0) + 1
        out[name] = dict(sorted(counts.items(), key=lambda kv: (-kv[1], kv[0])))
    return out


def summary(doc: Doc) -> Dict[str, Any]:
    from app.catalog.quality import static_flags

    s = doc.spec
    return {
        "key": s.key,
        "display_name": s.display_name,
        "subtitle": s.subtitle,
        "description": s.description,
        "kind": s.kind,
        "source": s.source,
        "grain": s.grain,
        "cadence": s.cadence,
        "status_public": s.status_public,
        "data_state": doc.facets["data_state"][0],
        "quality_flags": static_flags(s),
        "limitations_count": len(s.limitations),
        "redistribution": s.redistribution,
        "effective_redistribution": s.effective_redistribution,
        "reviewed": s.reviewed,
        "rights_gate": list(s.rights_gate),
        "storage": s.storage,
        "commercial_use": s.commercial_use,
        "license": s.license,
        "pii_class": s.pii_class,
        "origin": s.origin,
        "keywords": list(s.keywords),
        "identifiers": list(doc.facets["identifier"]),
        "spatial_coverage": s.spatial_coverage,
        "coverage_from": s.coverage_from,
        "coverage_basis": s.coverage_basis,
    }


def search(q: Optional[str] = None, filters: Optional[Mapping[str, Sequence[str]]] = None,
           limit: int = 200, offset: int = 0, index: Optional[Index] = None) -> Dict[str, Any]:
    """Ranked matches (all datasets, by name, when ``q`` is empty) with facets."""
    index = index or default_index()
    filters = {k: list(v) for k, v in (filters or {}).items() if v}
    q = (q or "").strip()
    scored: List[Tuple[float, List[str], Doc]] = []
    for d in index.docs:
        if q:
            s, matched = score(d, q)
            if s <= 0:
                continue
        else:
            s, matched = 0.0, []
        scored.append((s, matched, d))
    text_hits = [d for _s, _m, d in scored]
    facets = facet_counts(text_hits, filters)
    hits = [(s, m, d) for s, m, d in scored if _passes(d, filters)]
    hits.sort(key=lambda r: (-r[0], r[2].spec.display_name.lower(), r[2].spec.key))
    page = hits[offset:offset + limit]
    results = []
    for s, matched, d in page:
        row = summary(d)
        row["score"] = round(s, 2)
        row["matched"] = matched
        row["highlight"] = highlight(d, q) if q else None
        results.append(row)
    return {
        "q": q or None,
        "filters": filters,
        "count": len(hits),
        "total": len(index.docs),
        "text_matches": len(text_hits),
        "offset": offset,
        "limit": limit,
        "facets": facets,
        "results": results,
    }


# ---------------------------------------------------------------------------
# Row estimates (no counting)
# ---------------------------------------------------------------------------


def row_estimates(engine, specs: Sequence[DatasetSpec]) -> Dict[str, Dict[str, Any]]:
    """key -> {row_estimate, rows_exact, rows_from}. The live cache's exact
    totals when present; otherwise planner estimates from ``quality.relations``
    (cached 60 s, one catalog query). A row-filtered table has no whole-table
    estimate that applies, so such a dataset reports null. Never a count(*)."""
    from app.catalog.live import CACHE_TTL_S, _cached, resolve_tables
    from app.catalog.quality import base_tables, relations, row_estimate

    out: Dict[str, Dict[str, Any]] = {}
    rels: Optional[Dict[str, Dict[str, Any]]] = None
    base: set = set()
    for spec in specs:
        hit = _cached(spec.key, CACHE_TTL_S)
        if hit is not None:
            out[spec.key] = {"row_estimate": hit.get("rows_total"),
                             "rows_exact": bool(hit.get("rows_exact")), "rows_from": "live_cache"}
            continue
        if rels is None:
            try:
                rels = relations(engine, cached=True) if engine is not None else {}
            except Exception as e:
                logger.info(f"[catalog search] relation estimates unavailable: {type(e).__name__}")
                rels = {}
            base = base_tables(rels)
        if not rels:
            out[spec.key] = {"row_estimate": None, "rows_exact": False, "rows_from": None}
            continue
        filters = dict(spec.row_filters)
        total: Optional[int] = 0
        seen_any = False
        for t in resolve_tables(spec, base):
            if t not in base:
                continue
            est = None if t in filters else row_estimate(rels.get(t))
            if est is None:
                total = None
                break
            seen_any = True
            total += est
        # no existing table: unknown (a missing table is not "0 rows")
        out[spec.key] = {"row_estimate": total if seen_any else None,
                         "rows_exact": False, "rows_from": "estimate"}
    return out


__all__ = ["FACETS", "Index", "build_index", "clear_index", "default_index", "facet_counts",
           "row_estimates", "score", "search", "tokens"]
