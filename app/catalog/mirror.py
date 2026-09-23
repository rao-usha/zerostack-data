"""
dataset_registry as a generated mirror of the catalog (SPEC_123).

``sync_dataset_registry`` writes one ``dataset_registry`` row per catalog
table that exists in the database (pattern tables are expanded against the
live schema). It is keyed on ``table_name`` — the column's unique key — and:

- inserts missing rows with ``source = spec.source``, ``dataset_id = spec.key``
  and ``last_updated_at = CATALOG_ONLY_TS`` (1970-01-01): a *catalog-only*
  row. DQ rules, profiling, quality snapshots and the post-job quality gate
  filter on ``DatasetRegistry.ingested()`` and skip it until an ingestor
  touches the row (which stamps a real ``last_updated_at``);
- on existing rows adds a ``source_metadata["catalog"]`` block and fills an
  empty display_name / description, but keeps ``source``, ``dataset_id`` and
  ``last_updated_at`` (``_update_dataset_registry`` and the DQ services read
  those; ``last_updated_at`` means "an ingestor touched it", not "synced");
- never deletes: rows for tables outside the catalog get
  ``catalog.in_catalog = false``;
- is idempotent: the catalog block carries no timestamp, and a row whose
  block and names already match is not written.

A table written by several datasets (``pe_firms``: the SEC mart and the
LLM-collected PE rows; ``job_postings``: scraped and synthetic rows) gets a
*merged* rights block: the most restrictive redistribution and PII class of
all its writers, and every origin, so a table-level consumer never sees less
restriction than the table's contents carry. The owner (``key``) is the
first-declared writer.

Ingestors replace ``source_metadata`` wholesale; a ``before_update`` listener
on ``DatasetRegistry`` (app/core/models.py) and ``_update_dataset_registry``
carry the catalog block across those writes.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Sequence

from sqlalchemy import insert, select, update
from sqlalchemy.engine import Engine

from app.catalog.live import claimed_tables, existing_tables, resolve_tables
from app.catalog.registry import get_catalog
from app.catalog.spec import DatasetSpec
from app.catalog.tables import pattern_matches, split
from app.core.models import CATALOG_ONLY_TS, DatasetRegistry

logger = logging.getLogger(__name__)

_T = DatasetRegistry.__table__

# most restrictive last
REDISTRIBUTION_RANK = ("open", "attribution", "internal_only", "restricted")
PII_RANK = ("none", "business_contact", "personal")
# origins a consumer must be warned about, most severe first
ORIGIN_SEVERITY = ("synthetic", "llm_extracted", "scraped", "derived", "official")
_PUBLISHED = ("ga", "beta")


def _most(values: Iterable[str], rank: Sequence[str]) -> str:
    return max(values, key=rank.index)


def merge_rights(specs: Sequence[DatasetSpec]) -> Dict[str, Any]:
    """The rights of a table written by ``specs``: never less restrictive than any writer."""
    origins = sorted({s.origin for s in specs}, key=ORIGIN_SEVERITY.index)
    statuses = [s.status_public for s in specs]
    if all(st == "ga" for st in statuses):
        status = "ga"
    elif all(st in _PUBLISHED for st in statuses):
        status = "beta"
    else:
        # the table is only as public as its least public writer
        status = next((st for st in statuses if st not in _PUBLISHED), "internal")
    return {
        "status_public": status,
        "redistribution": _most((s.redistribution for s in specs), REDISTRIBUTION_RANK),
        "effective_redistribution": _most((s.effective_redistribution for s in specs),
                                          REDISTRIBUTION_RANK),
        "pii_class": _most((s.pii_class for s in specs), PII_RANK),
        "origin": origins[0],
        "origins": origins,
    }


def catalog_block(spec: Optional[DatasetSpec],
                  writers: Optional[Sequence[DatasetSpec]] = None) -> Dict[str, Any]:
    """``spec`` owns the row; ``writers`` (incl. ``spec``) all write the table."""
    if spec is None:
        return {"in_catalog": False}
    writers = list(writers) if writers else [spec]
    block = {
        "in_catalog": True,
        "managed_by": "catalog",
        "key": spec.key,
        "kind": spec.kind,
        **merge_rights(writers),
    }
    if len(writers) > 1:
        block["datasets"] = [w.key for w in writers]
    return block


def table_writers(specs: Iterable[DatasetSpec], existing: set) -> Dict[str, List[DatasetSpec]]:
    """table_name -> the specs that write it, in declaration order.

    Concrete declarations come first; a pattern only claims tables no other
    spec declares concretely (see ``resolve_tables``).
    """
    specs = list(specs)
    claimed = claimed_tables(specs)
    writers: Dict[str, List[DatasetSpec]] = {}
    for spec in specs:
        for t in spec.tables:
            if t in existing:
                writers.setdefault(t, []).append(spec)
    for spec in specs:
        for t in resolve_tables(spec, existing, claimed):
            if t in existing and spec not in writers.setdefault(t, []):
                writers[t].append(spec)
    return writers


def desired_rows(specs: Iterable[DatasetSpec], existing: set) -> Dict[str, DatasetSpec]:
    """table_name -> owning spec: the first spec that lists it, concrete before patterns."""
    return {t: ws[0] for t, ws in table_writers(specs, existing).items()}


def _writers_of(table: str, specs) -> List[DatasetSpec]:
    out = [s for s in specs if table in s.tables]
    if out:
        return out
    return [s for s in specs if any(pattern_matches(p, table) for p in s.table_patterns)]


def sync_dataset_registry(engine: Engine, specs: Optional[Iterable[DatasetSpec]] = None) -> Dict[str, int]:
    specs = list(specs if specs is not None else get_catalog())
    schemas = {split(t)[0] for s in specs for t in s.tables}
    existing = existing_tables(engine, schemas)
    writers = table_writers(specs, existing)

    stats = {"inserted": 0, "updated": 0, "unchanged": 0, "marked_not_in_catalog": 0}
    with engine.begin() as conn:
        rows = {r["table_name"]: dict(r) for r in conn.execute(select(_T)).mappings()}
        now = datetime.utcnow()

        for table, ws in writers.items():
            spec = ws[0]
            block = catalog_block(spec, ws)
            row = rows.get(table)
            if row is None:
                conn.execute(insert(_T).values(
                    source=spec.source[:50],
                    dataset_id=spec.key,
                    table_name=table,
                    display_name=spec.display_name,
                    description=spec.description,
                    source_metadata={"catalog": block},
                    created_at=now,
                    last_updated_at=CATALOG_ONLY_TS,  # catalog-only until an ingestor touches it
                ))
                stats["inserted"] += 1
                continue
            meta = row.get("source_metadata")
            meta = dict(meta) if isinstance(meta, dict) else {}
            values: Dict[str, Any] = {}
            if meta.get("catalog") != block:
                meta["catalog"] = block
                values["source_metadata"] = meta
            if not row.get("display_name"):
                values["display_name"] = spec.display_name
            if not row.get("description"):
                values["description"] = spec.description
            if values:
                # explicit last_updated_at suppresses the column's onupdate
                values["last_updated_at"] = _T.c.last_updated_at
                conn.execute(update(_T).where(_T.c.id == row["id"]).values(**values))
                stats["updated"] += 1
            else:
                stats["unchanged"] += 1

        for table, row in rows.items():
            if table in writers:
                continue
            meta = row.get("source_metadata")
            meta = dict(meta) if isinstance(meta, dict) else {}
            # a catalog table whose physical table is gone still belongs to its dataset
            ws = _writers_of(table, specs)
            block = catalog_block(ws[0] if ws else None, ws)
            if meta.get("catalog") == block:
                continue
            meta["catalog"] = block
            conn.execute(update(_T).where(_T.c.id == row["id"]).values(
                source_metadata=meta, last_updated_at=_T.c.last_updated_at))
            stats["updated" if block["in_catalog"] else "marked_not_in_catalog"] += 1

    logger.info(f"[catalog] dataset_registry mirror: {stats}")
    return stats
