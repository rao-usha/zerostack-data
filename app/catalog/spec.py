"""
DatasetSpec — one declared dataset (SPEC_123).

A dataset is what a customer would name: "SEC Form D offerings", "FRED
interest rates", "PE firms (SEC-derived)". It is produced by exactly one
primary producer, may be written by a few wrapper producers
(``also_produced_by``), lives in one or more tables, and carries the rights
block that decides whether it may ever leave the building.

The vocabularies are closed on purpose: a typo in ``redistribution`` must
fail at import, not publish a restricted dataset.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional, Tuple

KINDS = ("reference", "filings", "timeseries", "holdings", "derived_mart", "entity", "geo", "other")
RERUN_POLICIES = ("idempotent", "append_only", "destructive", "currency_only")
REDISTRIBUTION = ("internal_only", "attribution", "open", "restricted")
PII_CLASSES = ("none", "business_contact", "personal")
ORIGINS = ("official", "derived", "llm_extracted", "synthetic", "scraped")
STATUS_PUBLIC = ("ga", "beta", "internal", "archival", "retired")
# bulk:<BulkSource.name> | dispatch:<SOURCE_DISPATCH key> | collector:<SiteIntelSource>
# | job:<QueueJobType>[#stage] | api:<app/api/v1 module> (in-process router work)
PRODUCER_KINDS = ("bulk", "dispatch", "collector", "job", "api")

_KEY_RE = re.compile(r"^[a-z][a-z0-9_]*$")
_TABLE_RE = re.compile(r"^(?:[a-z_][a-z0-9_]*\.)?[a-z_][a-z0-9_]*$")
_PATTERN_RE = re.compile(r"^[a-z_][a-z0-9_]*\*$")
_PRODUCER_RE = re.compile(r"^(bulk|dispatch|collector|job|api):[a-z0-9_:.]+(#[a-z0-9_]+)?$")
_WRITE_SQL = re.compile(
    r"\b(insert|update|delete|drop|alter|create|truncate|grant|revoke|copy|vacuum|call|do|set)\b",
    re.I,
)


def _check(cond: bool, key: str, msg: str) -> None:
    if not cond:
        raise ValueError(f"DatasetSpec {key!r}: {msg}")


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

    def __post_init__(self) -> None:
        k = self.key
        _check(bool(_KEY_RE.match(k or "")), k, "key must be a lower-case slug")
        _check(bool(self.source) and bool(_KEY_RE.match(self.source)), k, "source must be a slug")
        _check(bool(self.display_name.strip()), k, "display_name is required")
        _check(len(self.description.strip()) >= 20, k, "description must be customer-readable (>= 20 chars)")
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
        if self.status_public in ("ga", "beta"):
            _check(self.reviewed, k, "a ga/beta dataset needs a reviewed rights block")
            _check(self.origin != "synthetic", k, "synthetic data is never published as ga/beta")
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
        }
