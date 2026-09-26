"""
DatasetSpec — one declared dataset (SPEC_123).

A dataset is what a customer would name: "SEC Form D offerings", "FRED
interest rates", "PE firms (SEC-derived)". It is produced by exactly one
primary producer, may be written by a few wrapper producers
(``also_produced_by``), lives in one or more tables, and carries the rights
block that decides whether it may ever leave the building.

The vocabularies are closed on purpose: a typo in ``redistribution`` must
fail at import, not publish a restricted dataset.

SPEC_141 (catalog truth pass) added the honesty fields: ``data_state`` and
``limitations`` say what is really loaded, ``missing_tables`` names declared
tables that do not exist, ``row_filters`` scope a dataset's rows inside a
shared table, and ``coverage_basis`` says what ``coverage_sql`` measures.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from typing import Optional, Tuple

KINDS = ("reference", "filings", "timeseries", "holdings", "derived_mart", "entity", "geo", "other")
RERUN_POLICIES = ("idempotent", "append_only", "destructive", "currency_only")
REDISTRIBUTION = ("internal_only", "attribution", "open", "restricted")
PII_CLASSES = ("none", "business_contact", "personal")
# curated: hand-compiled or seed lists (FTZ seed data, incentive programs ...)
ORIGINS = ("official", "derived", "llm_extracted", "synthetic", "scraped", "curated")
STATUS_PUBLIC = ("ga", "beta", "internal", "archival", "retired")
# bulk:<BulkSource.name> | dispatch:<SOURCE_DISPATCH key> | collector:<SiteIntelSource>
# | job:<QueueJobType>[#stage] | api:<app/api/v1 module> (in-process router work)
# | script:<scripts/*.py name> (a checked-in script is the only writer)
PRODUCER_KINDS = ("bulk", "dispatch", "collector", "job", "api", "script")
# What coverage_sql measures: the end of the period covered, an as-of / load
# date (snapshot data with no period column), a fixed vintage, or the end of a
# rolling window (older rows are dropped).
COVERAGE_BASES = ("period", "as_of", "fixed_vintage", "rolling")
# What the last verification found in the tables: the worst finding, with the
# detail in ``limitations``.
DATA_STATES = ("ok", "stale", "empty", "missing_tables", "key_columns_null", "seeded",
               "fabricated", "sample_mixed", "placeholder", "demo")
KEYWORDS = ("pe", "entity", "filings", "macro", "energy", "real_estate", "labor", "health",
            "trade", "geo_risk", "finance", "government", "logistics", "infrastructure",
            "demographics", "markets", "company", "people", "synthetic")

_KEY_RE = re.compile(r"^[a-z][a-z0-9_]*$")
_TABLE_RE = re.compile(r"^(?:[a-z_][a-z0-9_]*\.)?[a-z_][a-z0-9_]*$")
_PATTERN_RE = re.compile(r"^[a-z_][a-z0-9_]*\*$")
_PRODUCER_RE = re.compile(r"^(bulk|dispatch|collector|job|api|script):[a-z0-9_:.]+(#[a-z0-9_]+)?$")
# ':name' is a bind parameter to SQLAlchemy text(); '::type' casts are fine
_BIND_RE = re.compile(r"(?<![:\w]):[A-Za-z_]\w*")
_SPATIAL_RE = re.compile(r"^(US|global)(:[A-Za-z_]+)*$")
MIN_DESCRIPTION = 50
MAX_SUBTITLE = 100
_WRITE_SQL = re.compile(
    r"\b(insert|update|delete|drop|alter|create|truncate|grant|revoke|copy|vacuum|call|do|set)\b",
    re.I,
)


def _check(cond: bool, key: str, msg: str) -> None:
    if not cond:
        raise ValueError(f"DatasetSpec {key!r}: {msg}")


def _read_only_sql(sql: str) -> bool:
    return ";" not in sql and not _WRITE_SQL.search(sql) and not _BIND_RE.search(sql)


def _iso_date(value: str) -> bool:
    try:
        date.fromisoformat(value)
    except (TypeError, ValueError):
        return False
    return len(value) == 10


@dataclass(frozen=True)
class DatasetSpec:
    key: str
    source: str
    display_name: str
    description: str                 # customer-facing, one or two sentences
    kind: str
    grain: str                       # "one row per filing per private fund"
    producer: str
    cadence: str                     # expected refresh cadence ("daily", "monthly", "quarterly", "ad_hoc")
    rerun: str
    # rights block — explicit, from rights.SourceRights unless overridden
    license: str
    redistribution: str
    pii_class: str
    origin: str
    status_public: str
    attribution: Optional[str] = None
    reviewed: bool = False           # rights reviewed by a human; gates effective_redistribution
    tables: Tuple[str, ...] = ()
    table_patterns: Tuple[str, ...] = ()   # glob (`fred_*`) for generated table names
    primary_key: Tuple[str, ...] = ()      # of tables[0], when known
    also_produced_by: Tuple[str, ...] = ()
    inputs: Tuple[str, ...] = ()           # dataset keys this one is built from
    coverage_sql: Optional[str] = None     # SELECT returning one date/timestamp: max period covered
    coverage_from: Optional[str] = None    # ISO date the history starts, when fixed
    owner: str = "data-platform"
    slo_lag_hours: Optional[int] = None    # max hours from upstream publish to loaded
    upstream_url: Optional[str] = None
    notes: Optional[str] = None
    # -- SPEC_141 truth pass ------------------------------------------------
    coverage_basis: Optional[str] = None   # COVERAGE_BASES; required with coverage_sql
    subtitle: Optional[str] = None         # <= 100 chars
    keywords: Tuple[str, ...] = ()         # closed KEYWORDS
    spatial_coverage: Optional[str] = None  # "US:state", "US:county", "global:country"
    limitations: Tuple[str, ...] = ()      # known defects / gaps, with bug ids
    row_filters: Tuple[Tuple[str, str], ...] = ()  # (table, read-only predicate) in a shared table
    data_state: Optional[str] = None       # DATA_STATES, as last verified
    missing_tables: Tuple[str, ...] = ()   # declared tables / patterns verified absent
    verified_at: Optional[str] = None      # ISO date of that verification

    def __post_init__(self) -> None:
        k = self.key
        _check(bool(_KEY_RE.match(k or "")), k, "key must be a lower-case slug")
        _check(bool(self.source) and bool(_KEY_RE.match(self.source)), k, "source must be a slug")
        _check(bool(self.display_name.strip()), k, "display_name is required")
        _check(len(self.description.strip()) >= MIN_DESCRIPTION, k,
               f"description must be customer-readable (>= {MIN_DESCRIPTION} chars)")
        _check(bool(self.grain.strip()), k, "grain is required")
        _check(self.kind in KINDS, k, f"kind {self.kind!r} not in {KINDS}")
        _check(self.rerun in RERUN_POLICIES, k, f"rerun {self.rerun!r} not in {RERUN_POLICIES}")
        _check(self.redistribution in REDISTRIBUTION, k,
               f"redistribution {self.redistribution!r} not in {REDISTRIBUTION}")
        _check(self.pii_class in PII_CLASSES, k, f"pii_class {self.pii_class!r} not in {PII_CLASSES}")
        _check(self.origin in ORIGINS, k, f"origin {self.origin!r} not in {ORIGINS}")
        _check(self.status_public in STATUS_PUBLIC, k,
               f"status_public {self.status_public!r} not in {STATUS_PUBLIC}")
        _check(bool(self.license.strip()), k, "license is required (say 'unknown' explicitly)")
        for p in (self.producer,) + tuple(self.also_produced_by):
            _check(bool(_PRODUCER_RE.match(p or "")), k, f"producer {p!r} must be <kind>:<name>[#stage]")
        _check(self.producer not in self.also_produced_by, k, "producer repeated in also_produced_by")
        _check(bool(self.tables or self.table_patterns), k, "declare tables or table_patterns")
        for t in self.tables:
            _check(bool(_TABLE_RE.match(t)), k, f"table {t!r} is not a plain [schema.]name")
        for p in self.table_patterns:
            _check(bool(_PATTERN_RE.match(p)), k, f"table pattern {p!r} must be <prefix>*")
        _check(len(set(self.tables)) == len(self.tables), k, "duplicate table")
        for i in self.inputs:
            _check(bool(_KEY_RE.match(i)), k, f"input {i!r} is not a dataset key")
        _check(k not in self.inputs, k, "a dataset cannot be its own input")
        if self.coverage_sql is not None:
            sql = self.coverage_sql.strip().rstrip(";")
            _check(sql.lower().startswith(("select", "with")), k, "coverage_sql must be a SELECT")
            _check(";" not in sql, k, "coverage_sql must be a single statement")
            _check(not _WRITE_SQL.search(sql), k, "coverage_sql must be read-only")
            _check(not _BIND_RE.search(sql), k, "coverage_sql must not contain :bind parameters")
            _check(self.coverage_basis is not None, k, "coverage_sql needs a coverage_basis")
        if self.coverage_basis is not None:
            _check(self.coverage_basis in COVERAGE_BASES, k,
                   f"coverage_basis {self.coverage_basis!r} not in {COVERAGE_BASES}")
        if self.coverage_from is not None:
            _check(_iso_date(self.coverage_from), k, "coverage_from must be an ISO date (YYYY-MM-DD)")
        if self.verified_at is not None:
            _check(_iso_date(self.verified_at), k, "verified_at must be an ISO date (YYYY-MM-DD)")
        if self.subtitle is not None:
            _check(0 < len(self.subtitle.strip()) <= MAX_SUBTITLE, k,
                   f"subtitle must be 1-{MAX_SUBTITLE} chars")
        for w in self.keywords:
            _check(w in KEYWORDS, k, f"keyword {w!r} not in KEYWORDS")
        _check(len(set(self.keywords)) == len(self.keywords), k, "duplicate keyword")
        if self.spatial_coverage is not None:
            _check(bool(_SPATIAL_RE.match(self.spatial_coverage)), k,
                   f"spatial_coverage {self.spatial_coverage!r} must look like US:state")
        for lim in self.limitations:
            _check(isinstance(lim, str) and bool(lim.strip()), k, "empty limitation")
        seen = set()
        for rf in self.row_filters:
            _check(isinstance(rf, tuple) and len(rf) == 2, k, "row_filter must be (table, predicate)")
            table, pred = rf
            _check(table in self.tables, k, f"row_filter table {table!r} is not a declared table")
            _check(table not in seen, k, f"two row_filters for {table!r}")
            seen.add(table)
            _check(bool(pred.strip()) and _read_only_sql(pred), k,
                   f"row_filter predicate for {table!r} must be read-only, one clause, no binds")
        if self.data_state is not None:
            _check(self.data_state in DATA_STATES, k,
                   f"data_state {self.data_state!r} not in {DATA_STATES}")
        for t in self.missing_tables:
            _check(t in self.tables or t in self.table_patterns, k,
                   f"missing_tables entry {t!r} is not a declared table or pattern")
        _check(bool(self.missing_tables) == (self.data_state == "missing_tables"), k,
               "missing_tables is set exactly when data_state is 'missing_tables'")
        if self.status_public in ("ga", "beta"):
            _check(self.reviewed, k, "a ga/beta dataset needs a reviewed rights block")
            _check(self.origin != "synthetic", k, "synthetic data is never published as ga/beta")
            _check(self.data_state == "ok", k, "a ga/beta dataset needs data_state 'ok'")
        if self.slo_lag_hours is not None:
            _check(self.slo_lag_hours > 0, k, "slo_lag_hours must be positive")

    # -- derived -----------------------------------------------------------

    @property
    def producer_kind(self) -> str:
        return self.producer.split(":", 1)[0]

    @property
    def producers(self) -> Tuple[str, ...]:
        return (self.producer,) + tuple(self.also_produced_by)

    @property
    def effective_redistribution(self) -> str:
        """What export and the consumer API may do today: nothing leaves until reviewed."""
        return self.redistribution if self.reviewed else "internal_only"

    def to_dict(self) -> dict:
        return {
            "key": self.key,
            "source": self.source,
            "display_name": self.display_name,
            "description": self.description,
            "kind": self.kind,
            "grain": self.grain,
            "tables": list(self.tables),
            "table_patterns": list(self.table_patterns),
            "primary_key": list(self.primary_key),
            "producer": self.producer,
            "also_produced_by": list(self.also_produced_by),
            "inputs": list(self.inputs),
            "cadence": self.cadence,
            "coverage_from": self.coverage_from,
            "has_coverage_sql": self.coverage_sql is not None,
            "rerun": self.rerun,
            "owner": self.owner,
            "slo_lag_hours": self.slo_lag_hours,
            "rights": {
                "license": self.license,
                "redistribution": self.redistribution,
                "effective_redistribution": self.effective_redistribution,
                "attribution": self.attribution,
                "reviewed": self.reviewed,
            },
            "pii_class": self.pii_class,
            "origin": self.origin,
            "status_public": self.status_public,
            "upstream_url": self.upstream_url,
            "notes": self.notes,
            "subtitle": self.subtitle,
            "keywords": list(self.keywords),
            "spatial_coverage": self.spatial_coverage,
            "coverage_basis": self.coverage_basis,
            "data_state": self.data_state,
            "limitations": list(self.limitations),
            "missing_tables": list(self.missing_tables),
            "row_filters": [{"table": t, "predicate": p} for t, p in self.row_filters],
            "verified_at": self.verified_at,
        }
