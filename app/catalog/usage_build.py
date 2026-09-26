"""
Who reads a dataset (SPEC_144): a static usage map plus view dependencies.

Static part, generated and checked in (``app/catalog/usage.json``)::

    python -m app.catalog.usage_build          # regenerate
    python -m app.catalog.usage_build --check  # exit 1 when stale (the test does the same)

It scans the string literals of the consumer packages (``SCAN_DIRS``) for
``FROM``/``JOIN`` table references and the code for ORM model class names.
A SQL reference counts only when the name is a table or view declared in code
(``tables.declared_tables()`` plus ``CREATE VIEW`` statements), which drops
CTE names and English prose. The map is keyed by table, not by dataset, so it
changes only when the scanned code changes. It is a heuristic: dynamic names
(``f"FROM {table}"``) are invisible.

Runtime part: ``pg_depend`` gives view -> base table edges
(``public_company_financials`` reads ``sec_financial_facts`` ...), cached for
``VIEW_DEPS_TTL_S``. ``consumers_for`` joins the two: code that reads a
dataset's tables directly, the views over them, and code that reads those views.
"""

from __future__ import annotations

import io
import json
import logging
import re
import sys
import threading
import time
import tokenize
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

from sqlalchemy import text

from app.catalog.spec import DatasetSpec
from app.catalog.tables import REPO_ROOT, normalize

logger = logging.getLogger(__name__)

USAGE_PATH = Path(__file__).with_name("usage.json")

# consumer package -> consumer kind
SCAN_DIRS: Tuple[Tuple[str, str], ...] = (
    ("app/api/v1", "api_router"),
    ("app/services", "service"),
    ("app/graphql", "graphql"),
    ("app/reports", "report"),
    ("app/marts", "mart"),
    ("app/entities", "entity"),
)
# the catalog's own routers introspect every table; they are not consumers
EXCLUDE_PREFIXES = ("app/api/v1/catalog",)

_SQL_REF = re.compile(
    r"\b(?:from|join)\s+((?:\"?[a-z_][a-z0-9_]*\"?\.)?\"?[a-z_][a-z0-9_]*\"?)", re.I)
_VIEW_RE = re.compile(
    r"CREATE\s+(?:OR\s+REPLACE\s+)?(?:MATERIALIZED\s+)?VIEW\s+(?:IF\s+NOT\s+EXISTS\s+)?"
    r"(?:\"?([a-z_][a-z0-9_]*)\"?\.)?\"?([a-z_][a-z0-9_]*)\"?", re.I)

VIEW_DEPS_TTL_S = 600
_VIEW_DEPS_SQL = """
    SELECT DISTINCT vn.nspname, v.relname, tn.nspname, t.relname
    FROM pg_depend d
    JOIN pg_rewrite r ON r.oid = d.objid
    JOIN pg_class v ON v.oid = r.ev_class
    JOIN pg_namespace vn ON vn.oid = v.relnamespace
    JOIN pg_class t ON t.oid = d.refobjid
    JOIN pg_namespace tn ON tn.oid = t.relnamespace
    WHERE d.classid = 'pg_rewrite'::regclass
      AND d.refclassid = 'pg_class'::regclass
      AND v.relkind IN ('v', 'm')
      AND t.relkind IN ('r', 'p', 'v', 'm')
      AND v.oid <> t.oid
      AND tn.nspname NOT IN ('pg_catalog', 'information_schema')
"""


# =============================================================================
# Static scan
# =============================================================================


def _declared_views() -> Set[str]:
    out: Set[str] = set()
    for base in ("app", "alembic"):
        for path in sorted((REPO_ROOT / base).rglob("*.py")):
            body = path.read_text(encoding="utf-8", errors="ignore")
            for schema, name in _VIEW_RE.findall(body):
                out.add(normalize(schema or None, name))
    return out


def known_relations() -> Set[str]:
    from app.catalog.tables import declared_tables

    return set(declared_tables()) | _declared_views()


def model_tables() -> Dict[str, Set[str]]:
    """ORM class name -> tables it maps (a name may be reused across modules)."""
    import importlib

    from app.catalog.tables import MODEL_MODULES
    from app.core.models import Base

    for mod in MODEL_MODULES:
        importlib.import_module(mod)
    out: Dict[str, Set[str]] = {}
    for mapper in Base.registry.mappers:
        table = getattr(mapper, "local_table", None)
        if table is None or not hasattr(table, "name"):
            continue
        out.setdefault(mapper.class_.__name__, set()).add(normalize(table.schema, table.name))
    return out


def _tokens(source: str):
    try:
        return list(tokenize.generate_tokens(io.StringIO(source).readline))
    except (tokenize.TokenError, IndentationError, SyntaxError):
        return []


def scan_file(source: str, known: Set[str], models: Dict[str, Set[str]]) -> Dict[str, Set[str]]:
    """table -> {"sql", "orm"} for one module's source."""
    found: Dict[str, Set[str]] = {}
    for tok in _tokens(source):
        if tok.type == tokenize.STRING:
            for ref in _SQL_REF.findall(tok.string):
                name = ref.replace('"', "").lower()
                schema, _, rel = name.rpartition(".")
                key = normalize(schema or None, rel)
                if key in known:
                    found.setdefault(key, set()).add("sql")
        elif tok.type == tokenize.NAME and tok.string in models:
            for t in models[tok.string]:
                found.setdefault(t, set()).add("orm")
    return found


def build_usage(root: Path = REPO_ROOT) -> Dict[str, Any]:
    known = known_relations()
    models = model_tables()
    tables: Dict[str, List[Dict[str, Any]]] = {}
    for rel_dir, kind in SCAN_DIRS:
        base = root / rel_dir
        if not base.exists():
            continue
        for path in sorted(base.rglob("*.py")):
            rel = path.relative_to(root).as_posix()
            if rel.startswith(EXCLUDE_PREFIXES):
                continue
            source = path.read_text(encoding="utf-8", errors="ignore")
            for table, via in scan_file(source, known, models).items():
                tables.setdefault(table, []).append({"path": rel, "kind": kind, "via": sorted(via)})
    return {
        "generated_by": "python -m app.catalog.usage_build",
        "scanned": [d for d, _ in SCAN_DIRS],
        "tables": {t: sorted(v, key=lambda e: e["path"]) for t, v in sorted(tables.items())},
    }


def render(usage: Dict[str, Any]) -> str:
    return json.dumps(usage, indent=1, sort_keys=True) + "\n"


@lru_cache(maxsize=1)
def load_usage() -> Dict[str, Any]:
    try:
        return json.loads(USAGE_PATH.read_text(encoding="utf-8"))
    except (OSError, ValueError) as e:
        logger.warning(f"[catalog.usage] {USAGE_PATH.name} unreadable: {type(e).__name__}")
        return {"tables": {}}


# =============================================================================
# View dependencies (runtime)
# =============================================================================

_deps_cache: Dict[str, Tuple[float, Dict[str, List[str]]]] = {}
_deps_lock = threading.Lock()


def view_dependencies(engine, refresh: bool = False) -> Dict[str, List[str]]:
    """view -> relations it reads (PostgreSQL; {} elsewhere or on error)."""
    from app.catalog.quality import engine_of

    engine = engine_of(engine)
    if getattr(engine.dialect, "name", "") != "postgresql":
        return {}
    key = str(id(engine))
    with _deps_lock:
        hit = _deps_cache.get(key)
    if hit and not refresh and time.monotonic() - hit[0] < VIEW_DEPS_TTL_S:
        return hit[1]
    out: Dict[str, List[str]] = {}
    try:
        with engine.connect() as conn:
            with conn.begin():
                conn.execute(text("SET LOCAL statement_timeout = 5000"))
                for vs, v, ts, t in conn.execute(text(_VIEW_DEPS_SQL)):
                    out.setdefault(normalize(vs, v), []).append(normalize(ts, t))
    except Exception as e:
        logger.info(f"[catalog.usage] view dependencies unavailable: {type(e).__name__}")
        return {}
    out = {v: sorted(set(ts)) for v, ts in out.items()}
    with _deps_lock:
        _deps_cache[key] = (time.monotonic(), out)
    return out


def clear_cache() -> None:
    with _deps_lock:
        _deps_cache.clear()
    load_usage.cache_clear()


def _names(usage: Dict[str, Any], deps: Dict[str, List[str]], extra: Iterable[str]) -> Set[str]:
    names = set(usage.get("tables", {})) | set(extra)
    for v, ts in deps.items():
        names.add(v)
        names.update(ts)
    return names


def consumers_for(spec: DatasetSpec, deps: Optional[Dict[str, List[str]]] = None,
                  usage: Optional[Dict[str, Any]] = None,
                  existing: Iterable[str] = ()) -> Dict[str, Any]:
    """Code and views that read the dataset's tables.

    ``code``: one row per (path, table) with ``via`` sql/orm, or
    ``view:<name>`` when the code reads a view over the dataset.
    """
    from app.catalog.live import resolve_tables

    usage = usage if usage is not None else load_usage()
    deps = deps or {}
    names = _names(usage, deps, existing)
    tables = [t for t in resolve_tables(spec, {n for n in names if "." not in n} | set(spec.tables))]
    tset = set(tables)
    views = sorted({v for v, ts in deps.items() if tset & set(ts) and v not in tset})
    code: List[Dict[str, Any]] = []
    for t in tables:
        for e in usage.get("tables", {}).get(t, []):
            code.append({"path": e["path"], "kind": e["kind"], "table": t, "via": list(e["via"])})
    for v in views:
        for e in usage.get("tables", {}).get(v, []):
            code.append({"path": e["path"], "kind": e["kind"], "table": v, "via": [f"view:{v}"]})
    code.sort(key=lambda e: (e["path"], e["table"]))
    return {
        "tables": tables,
        "views": [{"view": v, "reads": sorted(tset & set(deps[v]))} for v in views],
        "code": code,
        "count": len({e["path"] for e in code}) + len(views),
    }


def undeclared_view_inputs(specs: Iterable[DatasetSpec], deps: Dict[str, List[str]],
                           base: Iterable[str]) -> List[Dict[str, Any]]:
    """Base tables read by views over catalog data that no spec declares
    (e.g. ``sec_company_metadata`` under ``public_company_financials``)."""
    from app.catalog.live import claimed_tables, resolve_tables

    specs = tuple(specs)
    base = set(base)
    claimed = claimed_tables(specs)
    covered: Set[str] = set()
    for s in specs:
        covered.update(resolve_tables(s, base, claimed))
    out: Dict[str, Set[str]] = {}
    for v, ts in deps.items():
        if v in covered or not (set(ts) & covered):
            continue
        for t in ts:
            if t in base and t not in covered:
                out.setdefault(t, set()).add(v)
    return [{"table": t, "views": sorted(vs)} for t, vs in sorted(out.items())]


def main(argv: Optional[List[str]] = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    rendered = render(build_usage())
    if "--check" in argv:
        current = USAGE_PATH.read_text(encoding="utf-8") if USAGE_PATH.exists() else ""
        if current != rendered:
            print(f"{USAGE_PATH} is stale: run python -m app.catalog.usage_build")
            return 1
        print(f"{USAGE_PATH} is current")
        return 0
    USAGE_PATH.write_text(rendered, encoding="utf-8", newline="\n")
    print(f"wrote {USAGE_PATH} ({len(json.loads(rendered)['tables'])} tables)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
