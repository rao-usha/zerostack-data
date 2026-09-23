"""
Where tables are declared in code, and how catalog table patterns map to
them (SPEC_123).

A table counts as *declared* when the code says it exists before any data
does: a SQLAlchemy model, a ``CREATE TABLE`` in ``app/`` or an Alembic
revision, a ``table_name`` / ``*_TABLE_NAME`` literal handed to a generic
ingestor, or a bulk loader's ``ddl()``. Names are normalised to
``name`` for the public schema and ``schema.name`` otherwise.

Used by the coverage test; nothing here touches the database.
"""

from __future__ import annotations

import fnmatch
import importlib
import re
from functools import lru_cache
from pathlib import Path
from typing import FrozenSet, Iterable, List, Optional, Set, Tuple

REPO_ROOT = Path(__file__).resolve().parents[2]

_CREATE_RE = re.compile(
    r"CREATE\s+(?:UNLOGGED\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?"
    r"(?:\"?([a-z_][a-z0-9_]*)\"?\.)?\"?([a-z_][a-z0-9_]*)\"?",
    re.I,
)
_TABLE_NAME_LITERAL_RE = re.compile(
    r"(?i)[\"']?[a-z_]*table_name[\"']?\s*[=:]\s*[\"']([a-z_][a-z0-9_]*)[\"']"
)
_FSTRING_RE = re.compile(r"\bf[\"']((?:[a-z0-9_]|\{[^{}\"']+\})+)[\"']")
_LITERAL_RE = re.compile(r"[\"']([a-z_][a-z0-9_]*)[\"']")

MODEL_MODULES = (
    "app.core.models",
    "app.core.models_site_intel",
    "app.core.pe_models",
    "app.core.people_models",
    "app.core.family_office_models",
    "app.core.macro_models",
    "app.core.models_queue",
    "app.core.models_watchdog",
    "app.core.convergence_models",
    "app.core.eval_models",
    "app.core.probability_models",
    "app.sources.sec.models",
)


def normalize(schema: Optional[str], name: str) -> str:
    schema = (schema or "public").lower()
    name = name.lower()
    return name if schema == "public" else f"{schema}.{name}"


def split(table: str) -> tuple:
    """'core.entity' -> ('core', 'entity'); 'pe_firms' -> ('public', 'pe_firms')."""
    if "." in table:
        schema, name = table.split(".", 1)
        return schema, name
    return "public", table


def _scan_sources() -> Set[str]:
    found: Set[str] = set()
    files = list((REPO_ROOT / "app").rglob("*.py")) + list((REPO_ROOT / "alembic").rglob("*.py"))
    for path in files:
        try:
            text = path.read_text(encoding="utf-8", errors="ignore")
        except OSError:
            continue
        for schema, name in _CREATE_RE.findall(text):
            found.add(normalize(schema, name))
        for name in _TABLE_NAME_LITERAL_RE.findall(text):
            found.add(normalize(None, name))
    return found


def _model_tables() -> Set[str]:
    from app.core.models import Base

    for mod in MODEL_MODULES:
        importlib.import_module(mod)
    return {normalize(t.schema, t.name) for t in Base.metadata.tables.values()}


def _bulk_ddl_tables() -> Set[str]:
    from app.ingest.bulk.registry import BULK_SOURCES, load_all

    load_all()
    out: Set[str] = set()
    for cls in BULK_SOURCES.values():
        out |= bulk_tables(cls)
    return out


def bulk_tables(cls) -> Set[str]:
    """Tables a BulkSource's ddl() creates."""
    out: Set[str] = set()
    for stmt in cls().ddl():
        for schema, name in _CREATE_RE.findall(stmt):
            out.add(normalize(schema, name))
    return out


@lru_cache(maxsize=1)
def declared_tables() -> FrozenSet[str]:
    return frozenset(_scan_sources() | _model_tables() | _bulk_ddl_tables())


# ---------------------------------------------------------------------------
# Patterns
# ---------------------------------------------------------------------------


def pattern_matches(pattern: str, table: str) -> bool:
    return fnmatch.fnmatchcase(table, pattern)


def pattern_like(pattern: str) -> str:
    """Glob `fred_*` -> SQL LIKE `fred\\_%` (escape `_`, which LIKE treats as a wildcard)."""
    return pattern.replace("\\", "\\\\").replace("_", "\\_").replace("%", "\\%").replace("*", "%")


def expand(patterns: Iterable[str], tables: Iterable[str]) -> List[str]:
    """Existing tables (public schema) matching any pattern, sorted."""
    pats = list(patterns)
    return sorted(t for t in set(tables) if "." not in t and any(pattern_matches(p, t) for p in pats))


def _template_regex(template: str) -> Tuple[re.Pattern, bool]:
    """f-string body -> (regex of the names it can produce, has literal letters)."""
    parts = re.split(r"(\{[^{}]+\})", template)
    # f"{survey}_{year}_{table}" has only structural literals
    literal_letters = any(re.search(r"[a-z]", p) for p in parts if not p.startswith("{"))
    rx = "".join("[a-z0-9_]+" if p.startswith("{") else re.escape(p) for p in parts)
    return re.compile(f"^{rx}$"), literal_letters


def package_source(module: str) -> str:
    """All .py text of the package that holds ``module``."""
    mod_path = REPO_ROOT / Path(*module.split("."))
    pkg = mod_path if mod_path.is_dir() else mod_path.parent
    return "".join(p.read_text(encoding="utf-8", errors="ignore") for p in sorted(pkg.glob("*.py")))


def pattern_generated_by(pattern: str, source_text: str) -> bool:
    """True when some f-string table-name template in ``source_text`` can
    produce a name matching ``pattern`` (a `<prefix>*` glob).

    A template made only of placeholders and underscores
    (``f"{survey}_{year}_{table_id}"``) counts only when the pattern's first
    token also appears as a literal (``"acs5"``) in the same package.
    """
    prefix = pattern.rstrip("*")
    first_token = prefix.split("_")[0]
    literals = set(_LITERAL_RE.findall(source_text))
    candidates = [prefix + s for s in ("x", "x_x", "x_x_x", "2023_b01001")]
    candidates += [prefix.rstrip("_")] if prefix.endswith("_") else []
    for template in _FSTRING_RE.findall(source_text):
        rx, has_letters = _template_regex(template)
        if not has_letters and first_token not in literals:
            continue
        if any(rx.match(c) for c in candidates):
            return True
    # a literal table name in the package that the pattern covers
    return any(pattern_matches(pattern, lit) for lit in literals if len(lit) > len(prefix))


def producer_module(producer: str) -> Optional[str]:
    """Python module that implements a producer (for the pattern check)."""
    kind, _, rest = producer.partition(":")
    name = rest.split("#", 1)[0]
    if kind == "dispatch":
        if name == "census":
            return "app.sources.census.ingest"
        if name == "public_lp_strategies":
            return "app.sources.public_lp_strategies.ingest"
        from app.api.v1.jobs import SOURCE_DISPATCH

        entry = SOURCE_DISPATCH.get(name)
        return entry[0] if entry else None
    if kind == "bulk":
        from app.ingest.bulk.registry import BULK_SOURCES, load_all

        load_all()
        cls = BULK_SOURCES.get(name)
        return cls.__module__ if cls else None
    if kind == "api":
        return f"app.api.v1.{name}"
    return None
