"""
Computed lineage graph for the dataset catalog (SPEC_143, PLAN_088 §3 "SPEC_136").

Nothing is recorded: the graph is computed on read from what the code and the
database already say, and cached for ``CACHE_TTL_S``. It replaces the unused
``lineage_service`` (its tables stay in the database, unread).

Nodes (``id`` is ``<type>:<name>``):

- ``dataset:<key>``     a catalog DatasetSpec
- ``producer:<p>``      a producer string (``bulk:sec_form_d``, ``job:pe_mart_build#funds``)
- ``source:<source>``   the upstream publisher family of a dataset built from no other dataset
- ``table:<name>``      a physical table a dataset declares (``pattern: true`` for a glob)
- ``view:<name>``       a view over catalog tables
- ``consumer:<path>``   code that reads a table (``app/catalog/usage.json``; opt-in)

Edges (``type``):

- ``declared``    input dataset -> dataset (``DatasetSpec.inputs``)
- ``sql``         input dataset -> dataset, from the FROM/JOIN tables in the SQL the
                  producer runs (``PRODUCER_MODULES``), with the tables read
- ``stage_input`` bulk input -> mart stage dataset, from ``app.marts.inputs`` stage maps
                  (what the build asserts before it runs)
- ``observed``    input dataset -> mart dataset, from the latest successful
                  ``core.mart_build`` row (the release keys it consumed)
- ``produces``    producer -> dataset (``producer`` and ``also_produced_by``)
- ``publishes``   source -> dataset
- ``stores``      dataset -> table
- ``view``        table (or view) -> view, from the CREATE VIEW code (``VIEW_MODULES``)
                  and, live, ``pg_depend``
- ``consumes``    table -> consumer (opt-in)

Dataset-to-dataset walks (``walk``) follow ``declared``, ``sql``, ``stage_input``
and ``observed`` edges. ``drift`` reconciles the four declarations of a mart's
inputs (spec, SQL, stage map, ledger) and lists tables that views or producers
read but no spec declares.
"""

from __future__ import annotations

import ast
import json
import logging
import re
import threading
import time
from collections import deque
from datetime import datetime
from functools import lru_cache
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Set, Tuple

from sqlalchemy import text

from app.catalog.spec import DatasetSpec
from app.catalog.tables import REPO_ROOT, normalize, pattern_matches

logger = logging.getLogger(__name__)

CACHE_TTL_S = 60
MAX_DEPTH = 10
DEFAULT_DEPTH = 3

EDGE_TYPES = ("declared", "sql", "stage_input", "observed", "produces", "publishes",
              "stores", "view", "consumes")
DATASET_EDGE_TYPES = ("declared", "sql", "stage_input", "observed")
DIRECTIONS = ("up", "down", "both")
DERIVED_KINDS = ("derived_mart", "entity")

# The modules whose SQL each derived producer runs. The SQL-reference test
# requires the spec's inputs to equal the datasets these modules read.
PRODUCER_MODULES: Dict[str, Tuple[str, ...]] = {
    "job:pe_mart_build#adv_private_funds": ("app/marts/adv_private_funds.py",),
    "job:pe_mart_build#firms": ("app/marts/pe_firms_sec.py",),
    "job:pe_mart_build#funds": ("app/marts/pe_funds_sec.py", "app/marts/links.py"),
    "job:pe_mart_build#people": ("app/marts/pe_people_sec.py",),
    "job:entity_resolve#feeds": ("app/entities/feeds.py",),
    "job:entity_resolve#bridge": ("app/entities/cik_crd_bridge.py",),
    "job:entity_resolve#resolve": ("app/entities/resolve.py", "app/entities/resolve_core.py"),
}

# derived_mart / entity specs whose inputs the SQL scan cannot check, and why.
# Their declared inputs stand (verified by SPEC_141) and still form graph edges.
SQL_UNCHECKED: Dict[str, str] = {
    "medspa_prospects": "discovery calls the Yelp API and enriches from nppes_providers in "
                        "Python; the IRS SOI / Yelp inputs are upstream sources, not SQL reads",
    "vertical_prospects": "discovery calls the Yelp API and scores with IRS SOI in Python",
    "rollup_market_scores": "the collector reads the Census CBP API and IRS SOI in Python",
}

# entity specs collected from outside the catalog (agents, websites, filings
# fetched live): no catalog dataset is an input, so none is declared.
NO_CATALOG_INPUTS: Dict[str, str] = {
    "people_org_charts": "LLM / website collection of leadership pages (job:people)",
    "pe_collection": "LLM / website collection of PE firms, funds and deals (job:pe)",
    "lp_collection": "public LP websites and documents (job:lp)",
    "family_offices": "family office websites and filings fetched live (job:fo)",
}

# Views over catalog tables whose CREATE VIEW lives in code. Live, pg_depend
# adds every view in the database (workbench.v_* ...).
VIEW_MODULES: Dict[str, Tuple[str, ...]] = {
    "public_company_financials": ("app/sources/sec/views.py",),
    "fred_observations": ("app/sources/fred/views.py",),
    "lp_strategy_quarterly_view": ("app/sources/public_lp_strategies/analytics_view.py",),
}


# =============================================================================
# Static SQL scan
# =============================================================================

_SQL_REF = re.compile(
    r"\b(?:from|join)\s+((?:\"?[a-z_][a-z0-9_]*\"?\.)?\"?[a-z_][a-z0-9_]*\"?)", re.I)
_PLACEHOLDER = re.compile(r"\{([A-Za-z_][A-Za-z0-9_]*)\}")
_NAME = re.compile(r"^(?:[a-z_][a-z0-9_]*\.)?[a-z_][a-z0-9_]*$")


def _norm_ref(ref: str) -> str:
    ref = ref.replace('"', "").lower()
    schema, _, name = ref.rpartition(".")
    return normalize(schema or None, name)


def _literal(node: ast.AST):
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, (ast.List, ast.Tuple, ast.Set)):
        vals = [e.value for e in node.elts if isinstance(e, ast.Constant) and isinstance(e.value, str)]
        return tuple(vals) if vals and len(vals) == len(node.elts) else None
    return None


def _module_constants(tree: ast.Module) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for node in tree.body:
        if isinstance(node, ast.Assign):
            names = [t.id for t in node.targets if isinstance(t, ast.Name)]
            value = node.value
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name) and node.value:
            names, value = [node.target.id], node.value
        else:
            continue
        lit = _literal(value)
        if lit is not None:
            for n in names:
                out[n] = lit
    return out


def _render(node: ast.JoinedStr, consts: Mapping[str, Any]) -> str:
    parts = []
    for v in node.values:
        if isinstance(v, ast.Constant):
            parts.append(str(v.value))
        elif (isinstance(v, ast.FormattedValue) and isinstance(v.value, ast.Name)
              and isinstance(consts.get(v.value.id), str)):
            parts.append(consts[v.value.id])
        else:
            parts.append("{?}")
    return "".join(parts)


def _fill(body: str, consts: Mapping[str, Any]) -> str:
    """``{fund_filings}`` in a ``str.format`` template -> the module constant
    ``FUND_FILINGS`` (or ``fund_filings``) when it is a string."""
    def sub(m: re.Match) -> str:
        name = m.group(1)
        for cand in (name, name.upper()):
            if isinstance(consts.get(cand), str):
                return consts[cand]
        return m.group(0)

    return _PLACEHOLDER.sub(sub, body)


def scan_source(source: str) -> Set[str]:
    """Relation names a module's SQL can reference: FROM/JOIN targets in its
    string literals (f-string and ``.format`` placeholders filled from module
    constants), plus module-level constants that hold a relation name (or a
    list of them), e.g. ``FILINGS = "public.sec_adv_filings"``.

    Unfiltered: callers keep only names the catalog knows (CTE aliases and
    English prose drop out there)."""
    tree = ast.parse(source)
    consts = _module_constants(tree)
    texts: List[str] = []
    fstring_parts: Set[int] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.JoinedStr):
            texts.append(_render(node, consts))
            fstring_parts.update(id(v) for v in node.values)
    for node in ast.walk(tree):
        if (isinstance(node, ast.Constant) and isinstance(node.value, str)
                and id(node) not in fstring_parts):
            texts.append(node.value)
    refs: Set[str] = set()
    for body in texts:
        for m in _SQL_REF.finditer(_fill(body, consts)):
            refs.add(_norm_ref(m.group(1)))
    for value in consts.values():
        for v in (value,) if isinstance(value, str) else value:
            if _NAME.match(v.lower()) and v == v.lower():
                refs.add(_norm_ref(v))
    return refs


@lru_cache(maxsize=64)
def scan_paths(paths: Tuple[str, ...]) -> frozenset:
    out: Set[str] = set()
    for p in paths:
        out |= scan_source((REPO_ROOT / p).read_text(encoding="utf-8"))
    return frozenset(out)


# =============================================================================
# Table ownership
# =============================================================================


def table_owners(specs: Sequence[DatasetSpec]) -> Tuple[Dict[str, Set[str]], List[Tuple[str, str]]]:
    """(declared table -> spec keys, [(pattern, spec key)])."""
    claims: Dict[str, Set[str]] = {}
    patterns: List[Tuple[str, str]] = []
    for s in specs:
        for t in s.tables:
            claims.setdefault(t, set()).add(s.key)
        for p in s.table_patterns:
            patterns.append((p, s.key))
    return claims, patterns


def owners_of(table: str, claims: Mapping[str, Set[str]],
              patterns: Sequence[Tuple[str, str]]) -> Set[str]:
    """Specs that own ``table``: declared claims first; a pattern only when
    no spec declares the table concretely (as ``live.resolve_tables``)."""
    if table in claims:
        return set(claims[table])
    if "." in table:
        return set()
    return {k for p, k in patterns if pattern_matches(p, table)}


def producer_modules(spec: DatasetSpec) -> Tuple[str, ...]:
    out: List[str] = []
    for p in spec.producers:
        for m in PRODUCER_MODULES.get(p, ()):
            if m not in out:
                out.append(m)
    return tuple(out)


def sql_inputs(spec: DatasetSpec, specs: Sequence[DatasetSpec]) -> Dict[str, Any]:
    """What the producer's SQL reads, as datasets.

    ``inputs``: input key -> tables read; ``self``: own tables read;
    ``ambiguous``: tables several other specs own (none declared as an input),
    table -> owners. Tables no spec owns are dropped (CTE names, staging,
    system catalogs)."""
    claims, patterns = table_owners(specs)
    refs = scan_paths(producer_modules(spec))
    inputs: Dict[str, List[str]] = {}
    own: List[str] = []
    ambiguous: Dict[str, List[str]] = {}
    for t in sorted(refs):
        owners = owners_of(t, claims, patterns)
        if not owners:
            continue
        if spec.key in owners:
            own.append(t)
            continue
        declared = owners & set(spec.inputs)
        pick = declared or owners
        if len(pick) > 1 and not declared:
            ambiguous[t] = sorted(owners)
            continue
        for k in sorted(pick):
            inputs.setdefault(k, []).append(t)
    return {"inputs": inputs, "self": own, "ambiguous": ambiguous}


# =============================================================================
# Stage maps (app/marts/inputs.py is derived from these)
# =============================================================================


def _mart_for_job(job_type: str) -> Optional[str]:
    from app.catalog.job_keys import MART_JOB_TYPES

    for mart, jt in MART_JOB_TYPES.items():
        if jt == job_type:
            return mart
    return None


def stage_inputs(job_type: str, specs: Optional[Sequence[DatasetSpec]] = None
                 ) -> Tuple[Dict[str, List[str]], Dict[str, List[str]]]:
    """(stage -> bulk sources, stage -> upstream marts) for ``job:<job_type>#<stage>``.

    From the catalog: a stage's bulk inputs are its spec's inputs produced by
    ``bulk:<name>``; its upstream marts are inputs built by another mart job
    (``core.mart_build.mart``). Inputs built by the same job are stages of the
    same build, so nothing is asserted for them."""
    if specs is None:
        from app.catalog.registry import get_catalog

        specs = get_catalog()
    by_key = {s.key: s for s in specs}
    prefix = f"job:{job_type}#"
    bulk: Dict[str, List[str]] = {}
    upstream: Dict[str, List[str]] = {}
    for s in specs:
        if not s.producer.startswith(prefix):
            continue
        stage = s.producer[len(prefix):]
        b: Set[str] = set()
        u: Set[str] = set()
        for key in s.inputs:
            src = by_key[key].producer
            if src.startswith("bulk:"):
                b.add(src[len("bulk:"):])
            elif src.startswith("job:"):
                other = src[len("job:"):].split("#", 1)[0]
                if other != job_type:
                    mart = _mart_for_job(other)
                    if mart:
                        u.add(mart)
        bulk[stage] = sorted(b)
        if u:
            upstream[stage] = sorted(u)
    return bulk, upstream


def _stage_datasets(job_type: str, specs: Sequence[DatasetSpec]) -> Dict[str, str]:
    prefix = f"job:{job_type}#"
    return {s.producer[len(prefix):]: s.key for s in specs if s.producer.startswith(prefix)}


# =============================================================================
# The graph
# =============================================================================


class _Builder:
    def __init__(self) -> None:
        self.nodes: Dict[str, Dict[str, Any]] = {}
        self.edges: Dict[Tuple[str, str, str], Dict[str, Any]] = {}

    def node(self, nid: str, **attrs) -> str:
        cur = self.nodes.setdefault(nid, {"id": nid, "type": nid.split(":", 1)[0],
                                          "name": nid.split(":", 1)[1]})
        for k, v in attrs.items():
            if v is not None:
                cur[k] = v
        return nid

    def edge(self, src: str, dst: str, etype: str, **attrs) -> None:
        key = (src, dst, etype)
        cur = self.edges.setdefault(key, {"from": src, "to": dst, "type": etype})
        for k, v in attrs.items():
            if v is None:
                continue
            if isinstance(v, list) and isinstance(cur.get(k), list):
                cur[k] = sorted(set(cur[k]) | set(v))
            else:
                cur[k] = v

    def result(self) -> Dict[str, Any]:
        return {
            "nodes": [self.nodes[k] for k in sorted(self.nodes)],
            "edges": sorted(self.edges.values(), key=lambda e: (e["type"], e["from"], e["to"])),
        }


def _ds(key: str) -> str:
    return f"dataset:{key}"


def _dataset_node(b: _Builder, s: DatasetSpec) -> None:
    b.node(_ds(s.key), label=s.display_name, kind=s.kind, source=s.source,
           status_public=s.status_public, data_state=s.data_state,
           redistribution=s.redistribution)


def _add_view_edges(b: _Builder, deps: Mapping[str, Iterable[str]], origin: str,
                    claims, patterns) -> None:
    for view, reads in deps.items():
        vid = b.node(f"view:{view}")
        for t in reads:
            if t in deps:
                src = b.node(f"view:{t}")
            else:
                owners = owners_of(t, claims, patterns)
                src = b.node(f"table:{t}", owned_by=sorted(owners) or None,
                             undeclared=True if not owners else None)
            b.edge(src, vid, "view", origin=[origin])


def static_view_dependencies(specs: Sequence[DatasetSpec]) -> Dict[str, List[str]]:
    """View -> catalog tables its CREATE VIEW code reads (``VIEW_MODULES``).

    Names the code reads that no spec declares are kept when they are tables
    the code declares (``sec_company_metadata``), so undeclared inputs show."""
    from app.catalog.tables import declared_tables

    claims, patterns = table_owners(specs)
    known = set(declared_tables())
    out: Dict[str, List[str]] = {}
    for view, paths in VIEW_MODULES.items():
        refs = scan_paths(paths)
        out[view] = sorted(t for t in refs if t != view and (
            owners_of(t, claims, patterns) or t in known))
    return out


def build_static(specs: Sequence[DatasetSpec]) -> Dict[str, Any]:
    """Everything the code says: specs, producers, tables, SQL reads, stage
    maps and CREATE VIEW definitions. No database."""
    from app.marts import inputs as mart_inputs

    b = _Builder()
    claims, patterns = table_owners(specs)
    by_key = {s.key: s for s in specs}
    drift: List[Dict[str, Any]] = []

    for s in specs:
        _dataset_node(b, s)
        for p in s.producers:
            b.edge(b.node(f"producer:{p}"), _ds(s.key), "produces",
                   primary=True if p == s.producer else None)
        for t in s.tables:
            b.edge(_ds(s.key), b.node(f"table:{t}"), "stores")
        for p in s.table_patterns:
            b.edge(_ds(s.key), b.node(f"table:{p}", pattern=True), "stores")
        if not s.inputs and s.kind not in DERIVED_KINDS:
            src = b.node(f"source:{s.source}")
            if s.upstream_url:
                urls = set(b.nodes[src].get("upstream_urls", [])) | {s.upstream_url}
                b.nodes[src]["upstream_urls"] = sorted(urls)
            b.edge(src, _ds(s.key), "publishes")
        for i in s.inputs:
            b.edge(_ds(i), _ds(s.key), "declared")

    # SQL the producers run
    for s in specs:
        if not producer_modules(s):
            continue
        found = sql_inputs(s, specs)
        for key, tables in found["inputs"].items():
            b.edge(_ds(key), _ds(s.key), "sql", tables=list(tables))
        sql_keys = set(found["inputs"])
        declared = set(s.inputs)
        for key in sorted(declared - sql_keys):
            drift.append({"dataset": s.key, "kind": "declared_not_read",
                          "input": key, "detail": "declared input the producer's SQL never reads"})
        for key in sorted(sql_keys - declared):
            drift.append({"dataset": s.key, "kind": "read_not_declared", "input": key,
                          "tables": found["inputs"][key],
                          "detail": "the producer's SQL reads this dataset; not declared"})
        for t, owners in found["ambiguous"].items():
            drift.append({"dataset": s.key, "kind": "ambiguous_table", "table": t,
                          "owners": owners, "detail": "table owned by several specs"})

    # stage maps the builds assert (app/marts/inputs.py)
    maps = (("pe_mart_build", mart_inputs.PE_MART_STAGE_INPUTS, mart_inputs.PE_MART_STAGE_UPSTREAM),
            ("entity_resolve", mart_inputs.ENTITY_STAGE_INPUTS,
             getattr(mart_inputs, "ENTITY_STAGE_UPSTREAM", {})))
    for job_type, bulk_map, up_map in maps:
        stage_ds = _stage_datasets(job_type, specs)
        want_bulk, want_up = stage_inputs(job_type, specs)
        for stage, sources in bulk_map.items():
            target = stage_ds.get(stage)
            if target is None:
                drift.append({"dataset": None, "kind": "stage_without_dataset",
                              "stage": f"{job_type}#{stage}", "detail": "no spec has this producer"})
                continue
            for src in sources:
                key = next((k for k, s in by_key.items() if s.producer == f"bulk:{src}"), src)
                b.edge(_ds(key), _ds(target), "stage_input", stage=f"{job_type}#{stage}")
        for stage in sorted(set(bulk_map) | set(want_bulk)):
            got, want = set(bulk_map.get(stage, ())), set(want_bulk.get(stage, ()))
            got_u, want_u = set(up_map.get(stage, ())), set(want_up.get(stage, ()))
            if got != want or got_u != want_u:
                drift.append({"dataset": stage_ds.get(stage), "kind": "stage_map_mismatch",
                              "stage": f"{job_type}#{stage}",
                              "asserted": sorted(got) + [f"mart:{m}" for m in sorted(got_u)],
                              "declared": sorted(want) + [f"mart:{m}" for m in sorted(want_u)],
                              "detail": "the build asserts different inputs than the spec declares"})

    # views defined in code
    deps = static_view_dependencies(specs)
    _add_view_edges(b, deps, "code", claims, patterns)

    graph = b.result()
    graph["drift"] = drift
    return graph


@lru_cache(maxsize=1)
def _static_default() -> Dict[str, Any]:
    from app.catalog.registry import get_catalog

    return build_static(get_catalog())


# =============================================================================
# Live parts: pg_depend and the mart ledger
# =============================================================================

_LATEST_BUILDS_SQL = """
    SELECT DISTINCT ON (mart) id, mart, finished_at, inputs
    FROM core.mart_build
    WHERE status = 'success' AND NOT dry_run
    ORDER BY mart, id DESC
"""


def latest_builds(engine) -> Dict[str, Any]:
    """Latest successful real build per mart in ``core.mart_build``."""
    out: Dict[str, Any] = {"available": False, "builds": [], "note": None}
    if getattr(getattr(engine, "dialect", None), "name", "") != "postgresql":
        out["note"] = "the ledger needs PostgreSQL"
        return out
    try:
        with engine.connect() as conn:
            with conn.begin():
                conn.execute(text("SET LOCAL statement_timeout = 5000"))
                if conn.execute(text("SELECT to_regclass('core.mart_build')")).scalar() is None:
                    out["note"] = "core.mart_build does not exist"
                    return out
                rows = conn.execute(text(_LATEST_BUILDS_SQL)).mappings().all()
    except Exception as e:
        logger.info(f"[catalog.lineage] mart ledger unavailable: {type(e).__name__}")
        out["note"] = "core.mart_build unreadable"
        return out
    out["available"] = True
    for r in rows:
        inputs = r["inputs"]
        if isinstance(inputs, str):
            try:
                inputs = json.loads(inputs)
            except ValueError:
                inputs = []
        out["builds"].append({
            "id": r["id"], "mart": r["mart"],
            "finished_at": r["finished_at"].isoformat() if r["finished_at"] else None,
            "inputs": inputs if isinstance(inputs, list) else [],
        })
    if not rows:
        out["note"] = ("no successful non-dry-run build recorded yet: every pe_mart_build / "
                       "entity_resolve run so far predates the SPEC_126a ledger")
    return out


def observed_edges(builds: Sequence[Mapping[str, Any]], specs: Sequence[DatasetSpec]
                   ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """(observed edges, drift) from the latest builds' input records."""
    from app.catalog.job_keys import MART_JOB_TYPES, ProducerMap

    pm = ProducerMap(specs)
    by_key = {s.key: s for s in specs}
    edges: List[Dict[str, Any]] = []
    drift: List[Dict[str, Any]] = []
    for build in builds:
        job_type = MART_JOB_TYPES.get(build["mart"])
        if not job_type:
            continue
        mart_keys = pm.datasets_for(f"job:{job_type}")
        for rec in build["inputs"]:
            if not isinstance(rec, Mapping) or not rec.get("source"):
                continue
            src = str(rec["source"])
            if rec.get("kind") == "mart":
                jt = MART_JOB_TYPES.get(src)
                in_keys = set(pm.datasets_for(f"job:{jt}")) if jt else set()
            else:
                in_keys = set(pm.datasets_for(f"bulk:{src}"))
            matched = False
            for mk in mart_keys:
                for ik in sorted(in_keys & set(by_key[mk].inputs)):
                    matched = True
                    edges.append({"from": _ds(ik), "to": _ds(mk), "type": "observed",
                                  "build_id": build["id"], "mart": build["mart"],
                                  "release_keys": list(rec.get("release_keys") or [])[:10]})
            if not matched:
                drift.append({"dataset": None, "kind": "observed_not_declared",
                              "mart": build["mart"], "input": src,
                              "detail": "the build asserted an input no dataset of the mart declares"})
    return edges, drift


_cache: Dict[str, Tuple[float, Dict[str, Any]]] = {}
_lock = threading.Lock()


def clear_cache() -> None:
    with _lock:
        _cache.clear()
    _static_default.cache_clear()
    scan_paths.cache_clear()


def full_graph(engine=None, live: bool = True, consumers: bool = False,
               refresh: bool = False) -> Dict[str, Any]:
    """The whole graph. ``live`` adds pg_depend view edges and the mart ledger
    (both degrade to nothing on error); ``consumers`` adds usage.json code
    readers."""
    from app.catalog.registry import get_catalog

    ck = f"{id(engine) if (engine is not None and live) else 'static'}:{int(consumers)}"
    if not refresh:
        with _lock:
            hit = _cache.get(ck)
        if hit and time.monotonic() - hit[0] < CACHE_TTL_S:
            return hit[1]

    specs = get_catalog()
    static = _static_default()
    b = _Builder()
    for n in static["nodes"]:
        b.nodes[n["id"]] = dict(n)
    for e in static["edges"]:
        b.edges[(e["from"], e["to"], e["type"])] = dict(e)
    drift = [dict(d) for d in static["drift"]]
    claims, patterns = table_owners(specs)
    observed: Dict[str, Any] = {"available": False, "builds": [], "note": "live=false"}
    views_live = False

    if engine is not None and live:
        from app.catalog.usage_build import view_dependencies

        deps = view_dependencies(engine)
        if deps:
            views_live = True
            _add_view_edges(b, deps, "pg_depend", claims, patterns)
        observed = latest_builds(engine)
        o_edges, o_drift = observed_edges(observed["builds"], specs)
        for e in o_edges:
            b.node(e["from"])
            b.edge(e["from"], e["to"], "observed", build_id=e["build_id"], mart=e["mart"],
                   release_keys=e["release_keys"])
        drift += o_drift

    if consumers:
        from app.catalog.usage_build import load_usage

        for t, entries in load_usage().get("tables", {}).items():
            if f"table:{t}" not in b.nodes and f"view:{t}" not in b.nodes:
                continue
            src = f"table:{t}" if f"table:{t}" in b.nodes else f"view:{t}"
            for e in entries:
                cid = b.node(f"consumer:{e['path']}", consumer_kind=e.get("kind"))
                b.edge(src, cid, "consumes", via=list(e.get("via") or []))

    # tables views read that no spec declares
    for n in b.nodes.values():
        if n["type"] == "table" and n.get("undeclared"):
            readers = sorted(e["to"] for e in b.edges.values()
                             if e["from"] == n["id"] and e["type"] == "view")
            drift.append({"dataset": None, "kind": "undeclared_view_input", "table": n["name"],
                          "views": [r.split(":", 1)[1] for r in readers],
                          "detail": "a view over catalog data reads a table no spec declares"})

    graph = b.result()
    graph["drift"] = drift
    graph["observed"] = {"available": observed["available"], "note": observed.get("note"),
                         "builds": [{k: v for k, v in bd.items() if k != "inputs"}
                                    for bd in observed["builds"]]}
    graph["views_source"] = "pg_depend+code" if views_live else "code"
    graph["counts"] = {
        "nodes": len(graph["nodes"]), "edges": len(graph["edges"]),
        "datasets": sum(1 for n in graph["nodes"] if n["type"] == "dataset"),
        "drift": len(drift),
        "by_edge_type": {t: sum(1 for e in graph["edges"] if e["type"] == t) for t in EDGE_TYPES},
    }
    graph["generated_at"] = datetime.utcnow().isoformat() + "Z"
    with _lock:
        _cache[ck] = (time.monotonic(), graph)
    return graph


# =============================================================================
# Walks
# =============================================================================


def dataset_adjacency(edges: Iterable[Mapping[str, Any]]
                      ) -> Tuple[Dict[str, Dict[str, Set[str]]], Dict[str, Dict[str, Set[str]]]]:
    """(downstream, upstream): dataset key -> neighbour key -> edge types."""
    down: Dict[str, Dict[str, Set[str]]] = {}
    up: Dict[str, Dict[str, Set[str]]] = {}
    for e in edges:
        if e["type"] not in DATASET_EDGE_TYPES:
            continue
        a, z = e["from"].split(":", 1)[1], e["to"].split(":", 1)[1]
        down.setdefault(a, {}).setdefault(z, set()).add(e["type"])
        up.setdefault(z, {}).setdefault(a, set()).add(e["type"])
    return down, up


def _bfs(start: str, adj: Mapping[str, Mapping[str, Set[str]]], depth: int) -> List[Dict[str, Any]]:
    seen = {start}
    out: List[Dict[str, Any]] = []
    q = deque([(start, 0)])
    while q:
        cur, d = q.popleft()
        if d >= depth:
            continue
        for nxt in sorted(adj.get(cur, {})):
            if nxt in seen:
                continue
            seen.add(nxt)
            out.append({"key": nxt, "depth": d + 1, "via": sorted(adj[cur][nxt]), "from": cur})
            q.append((nxt, d + 1))
    return out


def find_cycles(edges: Iterable[Mapping[str, Any]], types: Sequence[str] = DATASET_EDGE_TYPES
                ) -> List[List[str]]:
    """Dataset-level cycles (each as a key list); [] for a DAG."""
    adj: Dict[str, Set[str]] = {}
    for e in edges:
        if e["type"] in types:
            adj.setdefault(e["from"], set()).add(e["to"])
    cycles: List[List[str]] = []
    state: Dict[str, int] = {}
    stack: List[str] = []

    def visit(n: str) -> None:
        state[n] = 1
        stack.append(n)
        for m in sorted(adj.get(n, ())):
            if state.get(m) == 1:
                cycles.append(stack[stack.index(m):] + [m])
            elif m not in state:
                visit(m)
        stack.pop()
        state[n] = 2

    for n in sorted(adj):
        if n not in state:
            visit(n)
    return cycles


def walk(graph: Mapping[str, Any], key: str, direction: str = "both",
         depth: int = DEFAULT_DEPTH) -> Dict[str, Any]:
    """Upstream / downstream datasets of ``key`` to ``depth`` hops, plus the
    dataset's own producers, tables and the views over them, as a subgraph."""
    if direction not in DIRECTIONS:
        raise ValueError(f"direction must be one of {DIRECTIONS}")
    depth = max(1, min(int(depth), MAX_DEPTH))
    down, up = dataset_adjacency(graph["edges"])
    upstream = _bfs(key, up, depth) if direction in ("up", "both") else []
    downstream = _bfs(key, down, depth) if direction in ("down", "both") else []
    keep = {_ds(key)} | {_ds(r["key"]) for r in upstream + downstream}

    focal = _ds(key)
    local: Set[str] = set()
    for e in graph["edges"]:
        if e["type"] in ("produces", "publishes") and e["to"] == focal:
            local.add(e["from"])
        elif e["type"] == "stores" and e["from"] == focal:
            local.add(e["to"])
    # views over the dataset's tables, and views over those views
    views: Set[str] = set()
    frontier = set(local)
    while frontier:
        more = {e["to"] for e in graph["edges"]
                if e["type"] == "view" and e["from"] in frontier and e["to"] not in views}
        views |= more
        frontier = more
    ids = keep | local | views

    edges = [e for e in graph["edges"]
             if e["from"] in ids and e["to"] in ids
             and (e["type"] in DATASET_EDGE_TYPES
                  or e["from"] == focal or e["to"] == focal or e["type"] == "view")]
    nodes = [n for n in graph["nodes"] if n["id"] in ids]
    drift = [d for d in graph.get("drift", []) if d.get("dataset") in {key} | {
        r["key"] for r in upstream + downstream}]
    return {
        "key": key, "direction": direction, "depth": depth,
        "upstream": upstream, "downstream": downstream,
        "producers": sorted(n.split(":", 1)[1] for n in local if n.startswith("producer:")),
        "tables": sorted(n.split(":", 1)[1] for n in local if n.startswith("table:")),
        "views": sorted(v.split(":", 1)[1] for v in views),
        "nodes": nodes, "edges": edges, "drift": drift,
        "observed": graph.get("observed"),
    }
