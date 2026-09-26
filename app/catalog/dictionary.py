"""
Column dictionary at runtime (SPEC_137).

``columns.generated.json`` (built offline by ``dictionary_build``) is the
static half. This module merges it with what only the live database knows:

- ``information_schema`` types and nullability for tables the code does not
  describe statically (pattern tables such as ``fred_*`` or ``acs5_*``);
- ``census_variable_metadata`` labels (precedence just below curated);
- existing ``col_description`` comments (below model comments);
- the latest ``data_profile_columns`` stats (null %, distinct count, examples).

A live-only column still gets the package metadata text of its dataset's
source (``package_columns``) and the glossary rules, so the answer does not
depend on whether a table was declared in a model.
"""

from __future__ import annotations

import json
import logging
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from app.catalog.columns import SOURCE_CONFIDENCE, classify, max_pii
from app.catalog.identifiers import SEMANTIC_TYPES, join_types, normalize_expr
from app.catalog.spec import DatasetSpec

logger = logging.getLogger(__name__)

GENERATED_PATH = Path(__file__).resolve().parent / "columns.generated.json"

# precedence of description sources (lower = wins); census labels report as 'upstream'
_RANK = {"curated": 0, "upstream": 1, "census": 1, "bulk": 2, "metadata": 3, "model": 4,
         "pg_comment": 5, "glossary": 6, None: 9}

# PE / entity pack: the sellable Gold tier (PLAN_088 §2) — coverage gate in the tests
PE_ENTITY_PACK = (
    "sec_form_d", "sec_13f", "sec_insider", "sec_edgar_submissions", "sec_iapd_feed", "sec_adv_roster",
    "sec_adv_schedule_d", "sec_adv_private_funds", "pe_firms_sec", "pe_funds_sec", "pe_people_sec",
    "entity_source_records", "entity_cik_crd_bridge", "entity_master",
)


@lru_cache(maxsize=1)
def load_dictionary() -> Dict[str, Any]:
    with open(GENERATED_PATH, encoding="utf-8") as f:
        return json.load(f)


def dictionary_hash() -> str:
    return load_dictionary()["dictionary_hash"]


def table_entry(table: str, body: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
    body = body or load_dictionary()
    return body["tables"].get(table)


def packages_for(spec: DatasetSpec) -> List[str]:
    from app.catalog.dictionary_build import spec_packages

    return spec_packages(spec)


def _package_desc(name: str, packages: Sequence[str], body: Dict[str, Any]) -> Optional[str]:
    pc = body.get("package_columns", {})
    for p in packages:
        d = pc.get(p, {}).get(name)
        if d:
            return d
    return None


def merge_columns(
    table: str,
    live_cols: Optional[Sequence[Dict[str, Any]]] = None,
    packages: Sequence[str] = (),
    census: Optional[Dict[str, str]] = None,
    pg_comments: Optional[Dict[str, str]] = None,
    body: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """Dictionary columns of ``table`` merged with live facts.

    ``live_cols``: [{name, pg_type, nullable}] from information_schema (None =
    table not inspected; the static column list is returned). When given, the
    live list decides which columns exist.
    """
    body = body or load_dictionary()
    entry = table_entry(table, body) or {}
    static = {c["name"]: c for c in entry.get("columns", [])}
    packages = list(packages) or list(entry.get("packages", []))
    census = census or {}
    pg_comments = pg_comments or {}
    names = [c["name"] for c in live_cols] if live_cols is not None else list(static)
    live_by = {c["name"]: c for c in (live_cols or [])}
    out = []
    for name in names:
        base = dict(static.get(name) or {})
        if not base:
            g = classify(name) or {}
            base = {"name": name, "description": None, "unit": g.get("unit"), "example": None,
                    "semantic_type": g.get("semantic_type"), "pii": g.get("pii", "none"),
                    "source": None, "confidence": None}
            pd = _package_desc(name, packages, body)
            if pd:
                base.update(description=pd, source="metadata")
            elif g.get("description"):
                base.update(description=g["description"], source="glossary")
        if name in live_by:
            base["pg_type"] = live_by[name].get("pg_type")
            base["nullable"] = live_by[name].get("nullable")
        rank = _RANK.get(base.get("source"), 9)
        if census.get(name) and rank > _RANK["census"]:
            base.update(description=census[name], source="upstream")
        elif pg_comments.get(name) and rank > _RANK["pg_comment"]:
            base.update(description=pg_comments[name], source="pg_comment")
        base["confidence"] = SOURCE_CONFIDENCE.get(base.get("source")) if base.get("source") else None
        st = base.get("semantic_type")
        base["normalize_sql"] = normalize_expr(st, f'"{name}"') if st else None
        base.setdefault("pg_type", None)
        base.setdefault("nullable", None)
        out.append(base)
    return out


def coverage(columns: Iterable[Dict[str, Any]], include_glossary: bool = True) -> Dict[str, Any]:
    cols = list(columns)
    ok = [c for c in cols if c.get("description")
          and (include_glossary or c.get("source") != "glossary")]
    return {"columns": len(cols), "described": len(ok),
            "pct": round(100.0 * len(ok) / len(cols), 1) if cols else None}


def dataset_tables_offline(spec: DatasetSpec, body: Optional[Dict[str, Any]] = None) -> List[str]:
    body = body or load_dictionary()
    return sorted(t for t, e in body["tables"].items() if spec.key in e["datasets"])


def coverage_report(body: Optional[Dict[str, Any]] = None, specs=None) -> Dict[str, Any]:
    """Offline description coverage: per dataset, the PE/entity pack and overall."""
    from app.catalog.registry import get_catalog

    body = body or load_dictionary()
    specs = list(specs if specs is not None else get_catalog())
    per: Dict[str, Any] = {}
    seen: Dict[str, List[Dict[str, Any]]] = {}
    for s in specs:
        cols: List[Dict[str, Any]] = []
        for t in dataset_tables_offline(s, body):
            tc = body["tables"][t]["columns"]
            cols += tc
            seen[t] = tc
        per[s.key] = {**coverage(cols), "without_glossary": coverage(cols, False)["pct"],
                      "tables": len(dataset_tables_offline(s, body))}
    all_cols = [c for cs in seen.values() for c in cs]
    pack_cols = [c for t, cs in seen.items()
                 if set(body["tables"][t]["datasets"]) & set(PE_ENTITY_PACK) for c in cs]
    by_source: Dict[str, int] = {}
    for c in all_cols:
        by_source[c.get("source") or "undocumented"] = by_source.get(c.get("source") or "undocumented", 0) + 1
    return {
        "overall": {**coverage(all_cols), "without_glossary": coverage(all_cols, False)["pct"],
                    "tables": len(seen)},
        "pe_entity_pack": {**coverage(pack_cols), "without_glossary": coverage(pack_cols, False)["pct"]},
        "by_source": dict(sorted(by_source.items())),
        "datasets": per,
    }


# ---------------------------------------------------------------------------
# PII roll-up and join keys (offline)
# ---------------------------------------------------------------------------


def dataset_column_pii(spec: DatasetSpec, body: Optional[Dict[str, Any]] = None) -> Tuple[str, List[str]]:
    """Max column PII over the dataset's tables and the columns that set it."""
    body = body or load_dictionary()
    cols = [(t, c) for t in dataset_tables_offline(spec, body) for c in body["tables"][t]["columns"]]
    top = max_pii(c["pii"] for _, c in cols) if cols else "none"
    return top, sorted(f"{t}.{c['name']}" for t, c in cols if c["pii"] == top and top != "none")


def join_index(semantic_type: Optional[str] = None,
               body: Optional[Dict[str, Any]] = None) -> Dict[str, List[Dict[str, Any]]]:
    """semantic_type -> [{dataset, table, column, pg_type, normalize_sql}] for join-key types."""
    body = body or load_dictionary()
    jt = join_types()
    out: Dict[str, List[Dict[str, Any]]] = {}
    for table in sorted(body["tables"]):
        e = body["tables"][table]
        for c in e["columns"]:
            st = c.get("semantic_type")
            if st not in jt or (semantic_type and st != semantic_type):
                continue
            col_sql = f't."{c["name"]}"'
            for ds in e["datasets"]:
                out.setdefault(st, []).append({
                    "dataset": ds, "table": table, "column": c["name"], "pg_type": c.get("pg_type"),
                    "normalize_sql": normalize_expr(st, col_sql),
                })
    return out


def _q_table(table: str, alias: str) -> str:
    from app.catalog.tables import split

    schema, name = split(table)
    fq = f'"{name}"' if schema == "public" else f'"{schema}"."{name}"'
    return f"{fq} {alias}"


def related_datasets(key: str, body: Optional[Dict[str, Any]] = None,
                     limit_per_type: int = 25) -> List[Dict[str, Any]]:
    """Other datasets sharing a join-key semantic type with ``key``, most specific first."""
    body = body or load_dictionary()
    idx = join_index(body=body)
    out = []
    for st, rows in idx.items():
        mine = [r for r in rows if r["dataset"] == key]
        if not mine:
            continue
        theirs = [r for r in rows if r["dataset"] != key]
        seen = set()
        for other in theirs:
            if (other["dataset"], other["table"], other["column"]) in seen:
                continue
            seen.add((other["dataset"], other["table"], other["column"]))
            a = mine[0]
            left = normalize_expr(st, f'a."{a["column"]}"')
            right = normalize_expr(st, f'b."{other["column"]}"')
            out.append({
                "semantic_type": st,
                "specificity": SEMANTIC_TYPES[st].specificity,
                "dataset": other["dataset"],
                "table": other["table"],
                "column": other["column"],
                "from": {"table": a["table"], "column": a["column"]},
                "join_sql": (f"SELECT a.*, b.* FROM {_q_table(a['table'], 'a')} "
                             f"JOIN {_q_table(other['table'], 'b')} ON {left} = {right} LIMIT 100"),
            })
    out.sort(key=lambda r: (-r["specificity"], r["semantic_type"], r["dataset"], r["table"], r["column"]))
    # keep the list readable: at most ``limit_per_type`` rows per semantic type
    counts: Dict[str, int] = {}
    kept = []
    for r in out:
        counts[r["semantic_type"]] = counts.get(r["semantic_type"], 0) + 1
        if counts[r["semantic_type"]] <= limit_per_type:
            kept.append(r)
    return kept


def search_columns(q: Optional[str] = None, semantic_type: Optional[str] = None, pii: Optional[str] = None,
                   limit: int = 100, body: Optional[Dict[str, Any]] = None) -> Tuple[int, List[Dict[str, Any]]]:
    body = body or load_dictionary()
    needle = (q or "").strip().lower()
    hits = []
    for table in sorted(body["tables"]):
        e = body["tables"][table]
        for c in e["columns"]:
            if semantic_type and c.get("semantic_type") != semantic_type:
                continue
            if pii and c.get("pii") != pii:
                continue
            if needle and needle not in f"{c['name']} {c.get('description') or ''} {table}".lower():
                continue
            hits.append({"table": table, "datasets": e["datasets"], **c})
    return len(hits), hits[:limit]


__all__ = [
    "PE_ENTITY_PACK", "coverage", "coverage_report", "dataset_column_pii", "dictionary_hash",
    "join_index", "load_dictionary", "merge_columns", "packages_for", "related_datasets", "search_columns",
]
