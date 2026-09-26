"""
Generate the column dictionary (SPEC_137).

    python -m app.catalog.dictionary_build              # rewrite columns.generated.json
    python -m app.catalog.dictionary_build --check      # exit 1 when the file is stale
    python -m app.catalog.dictionary_build --coverage   # description coverage per dataset
    python -m app.catalog.dictionary_build --sync-comments [--apply]   # COMMENT ON COLUMN

Offline and deterministic: nothing here touches a database, so CI regenerates
the file and fails on drift. Sources, highest precedence first:

1. curated / upstream  ``columns_curated.CURATED``
2. bulk                ``COMMENT ON COLUMN`` inside a ``BulkSource.ddl()``
3. metadata            ``app/sources/<pkg>/metadata.py`` description dicts and
                       ``-- comments`` in static ``CREATE TABLE`` DDL
4. model               ``Column(comment=)`` then the inline ``# comment``
5. glossary            name rules in ``columns.GLOSSARY``

(census labels and live PG comments are merged at request time by
``app.catalog.dictionary``.) Types come from the model, else the bulk DDL,
else static DDL. ``semantic_type`` / ``pii`` come from the curated row, else
the glossary.
"""

from __future__ import annotations

import argparse
import ast
import hashlib
import importlib
import io
import json
import re
import sys
import tokenize
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

from app.catalog.columns import SOURCE_CONFIDENCE, ColumnSpec, classify
from app.catalog.tables import MODEL_MODULES, REPO_ROOT, declared_tables, expand, normalize

OUT_PATH = Path(__file__).resolve().parent / "columns.generated.json"
FORMAT_VERSION = 1

# ---------------------------------------------------------------------------
# DDL parsing (bulk ddl() and static CREATE TABLE literals)
# ---------------------------------------------------------------------------

_CREATE_HEAD = re.compile(
    r"CREATE\s+(?:UNLOGGED\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?"
    r"(?:\"?([a-z_][a-z0-9_]*)\"?\.)?\"?([a-z_][a-z0-9_]*)\"?\s*\(",
    re.I,
)
_COMMENT_ON = re.compile(
    r"COMMENT\s+ON\s+COLUMN\s+(?:\"?([a-z_][a-z0-9_]*)\"?\.)?\"?([a-z_][a-z0-9_]*)\"?\.\"?([a-z_][a-z0-9_]*)\"?"
    r"\s+IS\s+'((?:[^']|'')*)'",
    re.I,
)
_CONSTRAINT_WORDS = ("primary", "unique", "constraint", "foreign", "check", "exclude", "like")
_TYPE_STOP = re.compile(
    r"\s+(not\s+null|null|default|primary\s+key|references|unique|check|generated|collate|constraint)\b.*$",
    re.I | re.S)


def _balanced_body(sql: str, start: int) -> Optional[str]:
    """Text between the '(' at ``start - 1`` and its matching ')'."""
    depth, i = 1, start
    in_str = False
    while i < len(sql):
        c = sql[i]
        if c == "'":
            in_str = not in_str
        elif not in_str:
            if c == "(":
                depth += 1
            elif c == ")":
                depth -= 1
                if depth == 0:
                    return sql[start:i]
        i += 1
    return None


def _split_top(body: str) -> List[str]:
    out, depth, cur = [], 0, []
    for c in body:
        if c == "(":
            depth += 1
        elif c == ")":
            depth -= 1
        if c == "," and depth == 0:
            out.append("".join(cur))
            cur = []
        else:
            cur.append(c)
    out.append("".join(cur))
    return [p.strip() for p in out if p.strip()]


def parse_create_tables(sql: str) -> Dict[str, Dict[str, Dict[str, Any]]]:
    """``CREATE TABLE`` statements in ``sql`` -> {table: {column: {pg_type, nullable, comment}}}."""
    out: Dict[str, Dict[str, Dict[str, Any]]] = {}
    for m in _CREATE_HEAD.finditer(sql):
        body = _balanced_body(sql, m.end())
        if body is None or "{" in body:
            continue
        table = normalize(m.group(1), m.group(2))
        comments: Dict[str, str] = {}
        lines = []
        for line in body.splitlines():
            code, sep, comment = line.partition("--")
            lines.append(code)
            if sep:
                cm = re.match(r"\s*\"?([a-z_][a-z0-9_]*)\"?\s", code)
                if cm and comment.strip():
                    comments[cm.group(1).lower()] = comment.strip()
        cols: Dict[str, Dict[str, Any]] = {}
        pk_cols: Set[str] = set()
        for item in _split_top("\n".join(lines)):
            first = item.split(None, 1)[0].strip('"').lower()
            if first in _CONSTRAINT_WORDS:
                pk = re.match(r"primary\s+key\s*\(([^)]*)\)", item, re.I)
                if pk:
                    pk_cols |= {c.strip().strip('"').lower() for c in pk.group(1).split(",")}
                continue
            parts = item.split(None, 1)
            if len(parts) < 2 or not re.match(r"^\"?[a-z_][a-z0-9_]*\"?$", parts[0], re.I):
                continue
            pg_type = _TYPE_STOP.sub("", parts[1]).strip()
            pg_type = re.sub(r"\s+", " ", pg_type).lower()
            upper = item.upper()
            cols[first] = {
                "pg_type": pg_type or None,
                "nullable": not ("NOT NULL" in upper or "PRIMARY KEY" in upper),
                "comment": comments.get(first),
            }
        for c in pk_cols:
            if c in cols:
                cols[c]["nullable"] = False
        if cols:
            # a table defined twice (legacy + current DDL): keep the union, first definition wins
            merged = out.setdefault(table, {})
            for c, v in cols.items():
                merged.setdefault(c, v)
    return out


def parse_column_comments(sql: str) -> Dict[Tuple[str, str], str]:
    return {(normalize(s, t), c.lower()): txt.replace("''", "'")
            for s, t, c, txt in _COMMENT_ON.findall(sql)}


# ---------------------------------------------------------------------------
# Harvesters
# ---------------------------------------------------------------------------


def _pg_type(col) -> Optional[str]:
    from sqlalchemy.dialects import postgresql

    try:
        return col.type.compile(dialect=postgresql.dialect()).lower()
    except Exception:
        return type(col.type).__name__.lower()


def _model_comments(module: str) -> Dict[Tuple[str, str], Dict[str, str]]:
    """(tablename, column) -> {"comment"|"example": text} from inline ``#`` comments."""
    path = REPO_ROOT / Path(*module.split(".")).with_suffix(".py")
    if not path.exists():
        return {}
    src = path.read_text(encoding="utf-8", errors="ignore")
    try:
        tree = ast.parse(src)
    except SyntaxError:
        return {}
    line_comments: Dict[int, str] = {}
    for tok in tokenize.generate_tokens(io.StringIO(src).readline):
        if tok.type == tokenize.COMMENT:
            line_comments.setdefault(tok.start[0], tok.string.lstrip("#").strip())
    out: Dict[Tuple[str, str], Dict[str, str]] = {}
    for cls in (n for n in ast.walk(tree) if isinstance(n, ast.ClassDef)):
        tablename = None
        for stmt in cls.body:
            if (isinstance(stmt, ast.Assign) and any(getattr(t, "id", None) == "__tablename__"
                                                      for t in stmt.targets)
                    and isinstance(stmt.value, ast.Constant)):
                tablename = str(stmt.value.value)
        if not tablename:
            continue
        for stmt in cls.body:
            target, value = None, None
            if isinstance(stmt, ast.Assign) and len(stmt.targets) == 1 and isinstance(stmt.targets[0], ast.Name):
                target, value = stmt.targets[0].id, stmt.value
            elif isinstance(stmt, ast.AnnAssign) and isinstance(stmt.target, ast.Name):
                target, value = stmt.target.id, stmt.value
            if not (isinstance(value, ast.Call) and getattr(value.func, "id", getattr(value.func, "attr", None))
                    in ("Column", "mapped_column")):
                continue
            colname = target
            if value.args and isinstance(value.args[0], ast.Constant) and isinstance(value.args[0].value, str):
                colname = value.args[0].value
            text = None
            for ln in range(stmt.lineno, (stmt.end_lineno or stmt.lineno) + 1):
                if ln in line_comments and line_comments[ln]:
                    text = line_comments[ln]
                    break
            if not text or text.lower().startswith(("noqa", "type:", "pragma")):
                continue
            key = "example" if re.match(r"(?i)^e\.?g\.?[,:]?\s", text) else "comment"
            val = re.sub(r"(?i)^e\.?g\.?[,:]?\s*", "", text) if key == "example" else text
            out[(tablename, colname.lower())] = {key: val}
    return out


def harvest_models() -> Dict[str, Dict[str, Dict[str, Any]]]:
    from app.core.models import Base

    comments: Dict[Tuple[str, str], Dict[str, str]] = {}
    for mod in MODEL_MODULES:
        importlib.import_module(mod)
        comments.update(_model_comments(mod))
    out: Dict[str, Dict[str, Dict[str, Any]]] = {}
    for t in Base.metadata.tables.values():
        table = normalize(t.schema, t.name)
        cols = {}
        for c in t.columns:
            extra = comments.get((t.name, c.name.lower()), {})
            cols[c.name.lower()] = {
                "pg_type": _pg_type(c),
                "nullable": bool(c.nullable) and not c.primary_key,
                "comment": (c.comment or extra.get("comment") or None),
                "example": extra.get("example"),
            }
        out[table] = cols
    return out


def harvest_bulk() -> Tuple[Dict[str, Dict[str, Dict[str, Any]]], Dict[Tuple[str, str], str]]:
    from app.ingest.bulk.registry import BULK_SOURCES, load_all

    load_all()
    tables: Dict[str, Dict[str, Dict[str, Any]]] = {}
    comments: Dict[Tuple[str, str], str] = {}
    for name in sorted(BULK_SOURCES):
        for stmt in BULK_SOURCES[name]().ddl():
            for t, cols in parse_create_tables(stmt).items():
                tables.setdefault(t, cols)
            comments.update(parse_column_comments(stmt))
    return tables, comments


def _string_constants(path: Path) -> Iterable[str]:
    try:
        tree = ast.parse(path.read_text(encoding="utf-8", errors="ignore"))
    except (SyntaxError, ValueError):
        return []
    return [n.value for n in ast.walk(tree)
            if isinstance(n, ast.Constant) and isinstance(n.value, str) and "CREATE" in n.value.upper()
            and "TABLE" in n.value.upper()]


def harvest_static_ddl() -> Dict[str, Dict[str, Dict[str, Any]]]:
    out: Dict[str, Dict[str, Dict[str, Any]]] = {}
    files = sorted(list((REPO_ROOT / "app").rglob("*.py")) + list((REPO_ROOT / "alembic").rglob("*.py")))
    for path in files:
        for s in _string_constants(path):
            for t, cols in parse_create_tables(s).items():
                merged = out.setdefault(t, {})
                for c, v in cols.items():
                    merged.setdefault(c, v)
    return out


# keys a column-definition dict may carry besides "description"; a dict with any other key
# (table_name, display_name, series, category, refresh_frequency ...) describes a dataset, a
# series or a vendor, not a column, and must not lend its text to a same-named column
_COLUMN_DEF_KEYS = frozenset((
    "description", "type", "sql_type", "pg_type", "nullable", "unit", "units", "example", "format",
    "primary_key", "index", "unique", "default", "length", "precision", "scale", "required",
))


def _is_column_def(v: Any) -> bool:
    return (isinstance(v, dict) and isinstance(v.get("description"), str) and set(v) <= _COLUMN_DEF_KEYS)


def _description_dicts(obj: Any, found: Dict[str, str], depth: int = 0) -> None:
    """Collect ``{column: {"description": text, "type": ...}}`` maps anywhere inside ``obj``."""
    if depth > 4:
        return
    if isinstance(obj, dict):
        cands = {k: v for k, v in obj.items()
                 if isinstance(k, str) and isinstance(v, dict) and isinstance(v.get("description"), str)
                 and re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", k)}
        # every entry must look like a column definition, else the whole map is not a column map
        hits = ({k: v["description"] for k, v in cands.items()}
                if cands and all(_is_column_def(v) for v in cands.values()) else {})
        if len(hits) >= 2:
            for k, d in hits.items():
                if d.strip():
                    found.setdefault(k.lower(), d.strip())
        for v in obj.values():
            if isinstance(v, (dict, list, tuple)):
                _description_dicts(v, found, depth + 1)
    elif isinstance(obj, (list, tuple)):
        for v in obj:
            if isinstance(v, (dict, list, tuple)):
                _description_dicts(v, found, depth + 1)


_DDL_COMMENT_LINE = re.compile(r"^[ \t]*\"?([a-z_][a-z0-9_]*)\"?[ \t]+[A-Z][A-Za-z0-9_(), ]*?,?[ \t]*--[ \t]*(\S.*)$")


def harvest_metadata() -> Dict[str, Dict[str, str]]:
    """package -> {column: description} from ``app/sources/<pkg>/metadata.py``."""
    out: Dict[str, Dict[str, str]] = {}
    for path in sorted((REPO_ROOT / "app" / "sources").glob("*/metadata.py")):
        pkg = path.parent.name
        found: Dict[str, str] = {}
        try:
            mod = importlib.import_module(f"app.sources.{pkg}.metadata")
        except Exception:
            continue
        for name in sorted(vars(mod)):
            if name.startswith("__"):
                continue
            val = getattr(mod, name)
            if isinstance(val, (dict, list, tuple)):
                _description_dicts(val, found)
        # `col TYPE, -- comment` lines in DDL templates (f-strings included). One physical line
        # only: a comment on a line of its own is a section header, not the previous column's text.
        text = path.read_text(encoding="utf-8", errors="ignore")
        for line in text.splitlines():
            m = _DDL_COMMENT_LINE.match(line)
            if m:
                found.setdefault(m.group(1), m.group(2).strip())
        if found:
            out[pkg] = dict(sorted(found.items()))
    return out


# ---------------------------------------------------------------------------
# Catalog tables and packages
# ---------------------------------------------------------------------------


def spec_packages(spec) -> List[str]:
    """``app/sources`` packages whose metadata describes this dataset's columns."""
    return list(_packages(spec.source, spec.producer))


@lru_cache(maxsize=None)
def _packages(source: str, producer: str) -> Tuple[str, ...]:
    pkgs = []
    if (REPO_ROOT / "app" / "sources" / source).is_dir():
        pkgs.append(source)
    from app.catalog.tables import producer_module

    try:
        mod = producer_module(producer)
    except Exception:
        mod = None
    if mod and mod.startswith("app.sources."):
        pkg = mod.split(".")[2]
        if pkg not in pkgs:
            pkgs.append(pkg)
    return tuple(pkgs)


def catalog_tables_offline(specs, known: Iterable[str]) -> Dict[str, List[str]]:
    """table -> dataset keys, from concrete tables and patterns expanded over ``known`` tables."""
    known = set(known) | set(declared_tables())
    out: Dict[str, List[str]] = {}
    for s in specs:
        for t in list(s.tables) + expand(s.table_patterns, known):
            if s.key not in out.setdefault(t, []):
                out[t].append(s.key)
    return out


# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------


def _merge_column(table: str, name: str, facts: Dict[str, Any], curated: Optional[Dict[str, Any]],
                  bulk_comment: Optional[str], meta_desc: Optional[str]) -> ColumnSpec:
    g = classify(name) or {}
    desc = src = None
    if curated and curated.get("description"):
        desc, src = curated["description"], curated.get("source", "curated")
    elif bulk_comment:
        desc, src = bulk_comment, "bulk"
    elif facts.get("ddl_comment"):
        desc, src = facts["ddl_comment"], facts.get("ddl_comment_source", "metadata")
    elif meta_desc:
        desc, src = meta_desc, "metadata"
    elif facts.get("model_comment"):
        desc, src = facts["model_comment"], "model"
    elif g.get("description"):
        desc, src = g["description"], "glossary"
    cur = curated or {}
    return ColumnSpec(
        table=table,
        name=name,
        pg_type=facts.get("pg_type"),
        nullable=facts.get("nullable"),
        description=desc,
        unit=cur.get("unit", g.get("unit")),
        example=cur.get("example", facts.get("example")),
        semantic_type=cur["semantic_type"] if "semantic_type" in cur else g.get("semantic_type"),
        pii=cur.get("pii", g.get("pii", "none")),
        source=src,
        confidence=SOURCE_CONFIDENCE.get(src) if src else None,
        upstream_url=cur.get("upstream_url"),
    )


def build(specs=None) -> Dict[str, Any]:
    from app.catalog.columns_curated import CURATED
    from app.catalog.registry import get_catalog

    specs = list(specs if specs is not None else get_catalog())
    models = harvest_models()
    bulk, bulk_comments = harvest_bulk()
    static = harvest_static_ddl()
    meta = harvest_metadata()

    tables = catalog_tables_offline(specs, set(models) | set(bulk) | set(static))
    by_key = {s.key: s for s in specs}
    curated_by_table: Dict[str, Dict[str, Dict[str, Any]]] = {}
    for key, row in CURATED.items():
        t, _, c = key.rpartition(".")
        curated_by_table.setdefault(t, {})[c] = row

    out_tables: Dict[str, Any] = {}
    for table in sorted(tables):
        origin, facts = "none", {}
        for name, src in (("model", models), ("bulk", bulk), ("ddl", static)):
            if table in src:
                origin, facts = name, src[table]
                break
        pkgs: List[str] = []
        for k in tables[table]:
            for p in spec_packages(by_key[k]):
                if p not in pkgs:
                    pkgs.append(p)
        names = list(facts) + [c for c in curated_by_table.get(table, {}) if c not in facts]
        cols = []
        for name in names:
            f = dict(facts.get(name, {}))
            f["model_comment"] = f.get("comment") if origin == "model" else None
            f["ddl_comment"] = f.get("comment") if origin in ("bulk", "ddl") else None
            f["ddl_comment_source"] = "bulk" if origin == "bulk" else "metadata"
            meta_desc = next((meta[p][name] for p in pkgs if name in meta.get(p, {})), None)
            spec = _merge_column(table, name, f, curated_by_table.get(table, {}).get(name),
                                 bulk_comments.get((table, name)), meta_desc)
            cols.append(spec.to_dict())
        out_tables[table] = {
            "datasets": sorted(tables[table]),
            "origin": origin,
            "packages": pkgs,
            "columns": sorted(cols, key=lambda c: c["name"]),
        }
    body = {
        "format_version": FORMAT_VERSION,
        "tables": out_tables,
        "package_columns": {p: meta[p] for p in sorted(meta)},
    }
    body["dictionary_hash"] = dictionary_hash(body)
    return body


def dictionary_hash(body: Dict[str, Any]) -> str:
    payload = {k: v for k, v in body.items() if k != "dictionary_hash"}
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()[:16]


def render(body: Dict[str, Any]) -> str:
    return json.dumps(body, indent=1, sort_keys=True, ensure_ascii=False) + "\n"


def write(body: Dict[str, Any], path: Path = OUT_PATH) -> None:
    with open(path, "w", encoding="utf-8", newline="\n") as f:
        f.write(render(body))


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--check", action="store_true", help="exit 1 when columns.generated.json is stale")
    ap.add_argument("--coverage", action="store_true", help="print description coverage per dataset")
    ap.add_argument("--sync-comments", action="store_true", help="COMMENT ON COLUMN sync (dry run unless --apply)")
    ap.add_argument("--apply", action="store_true", help="with --sync-comments: write the comments")
    args = ap.parse_args(argv)

    if args.sync_comments:
        from app.catalog.mirror import sync_column_comments
        from app.core.database import get_engine

        print(json.dumps(sync_column_comments(get_engine(), dry_run=not args.apply), indent=1, default=str))
        return 0
    body = build()
    if args.check:
        current = OUT_PATH.read_text(encoding="utf-8") if OUT_PATH.exists() else ""
        if current != render(body):
            print(f"{OUT_PATH.name} is stale: run python -m app.catalog.dictionary_build", file=sys.stderr)
            return 1
        print(f"{OUT_PATH.name} is up to date ({body['dictionary_hash']})")
        return 0
    if args.coverage:
        from app.catalog.dictionary import coverage_report

        print(json.dumps(coverage_report(body), indent=1))
        return 0
    write(body)
    print(f"wrote {OUT_PATH} ({len(body['tables'])} tables, hash {body['dictionary_hash']})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
