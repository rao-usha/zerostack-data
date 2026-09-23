"""
dataset_registry as a generated mirror of the catalog (SPEC_123).

``sync_dataset_registry`` writes one ``dataset_registry`` row per catalog
table that exists in the database (pattern tables are expanded against the
live schema). It is keyed on ``table_name`` — the column's unique key — and:

- inserts missing rows with ``source = spec.source``, ``dataset_id = spec.key``;
- on existing rows adds a ``source_metadata["catalog"]`` block and fills an
  empty display_name / description, but keeps ``source``, ``dataset_id`` and
  ``last_updated_at`` (``_update_dataset_registry`` and the DQ services read
  those; ``last_updated_at`` means "an ingestor touched it", not "synced");
- never deletes: rows for tables outside the catalog get
  ``catalog.in_catalog = false``;
- is idempotent: the catalog block carries no timestamp, and a row whose
  block and names already match is not written.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, Iterable, Optional

from sqlalchemy import insert, select, update
from sqlalchemy.engine import Engine

from app.catalog.live import existing_tables, resolve_tables
from app.catalog.registry import get_catalog
from app.catalog.spec import DatasetSpec
from app.catalog.tables import pattern_matches, split
from app.core.models import DatasetRegistry

logger = logging.getLogger(__name__)

_T = DatasetRegistry.__table__


def catalog_block(spec: Optional[DatasetSpec]) -> Dict[str, Any]:
    if spec is None:
        return {"in_catalog": False}
    return {
        "in_catalog": True,
        "managed_by": "catalog",
        "key": spec.key,
        "kind": spec.kind,
        "status_public": spec.status_public,
        "redistribution": spec.redistribution,
        "effective_redistribution": spec.effective_redistribution,
        "pii_class": spec.pii_class,
        "origin": spec.origin,
    }


def desired_rows(specs: Iterable[DatasetSpec], existing: set) -> Dict[str, DatasetSpec]:
    """table_name -> owning spec: the first spec that lists it, concrete before patterns."""
    specs = list(specs)
    owners: Dict[str, DatasetSpec] = {}
    for spec in specs:
        for t in spec.tables:
            if t in existing:
                owners.setdefault(t, spec)
    for spec in specs:
        for t in resolve_tables(spec, existing):
            if t in existing:
                owners.setdefault(t, spec)
    return owners


def _owner_of(table: str, specs) -> Optional[DatasetSpec]:
    for spec in specs:
        if table in spec.tables:
            return spec
    for spec in specs:
        if any(pattern_matches(p, table) for p in spec.table_patterns):
            return spec
    return None


def sync_dataset_registry(engine: Engine, specs: Optional[Iterable[DatasetSpec]] = None) -> Dict[str, int]:
    specs = list(specs if specs is not None else get_catalog())
    schemas = {split(t)[0] for s in specs for t in s.tables}
    existing = existing_tables(engine, schemas)
    owners = desired_rows(specs, existing)

    stats = {"inserted": 0, "updated": 0, "unchanged": 0, "marked_not_in_catalog": 0}
    with engine.begin() as conn:
        rows = {r["table_name"]: dict(r) for r in conn.execute(select(_T)).mappings()}
        now = datetime.utcnow()

        for table, spec in owners.items():
            block = catalog_block(spec)
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
                    last_updated_at=now,
                ))
                stats["inserted"] += 1
                continue
            meta = dict(row.get("source_metadata") or {})
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
            if table in owners:
                continue
            meta = dict(row.get("source_metadata") or {})
            # a catalog table whose physical table is gone still belongs to its dataset
            block = catalog_block(_owner_of(table, specs))
            if meta.get("catalog") == block:
                continue
            meta["catalog"] = block
            conn.execute(update(_T).where(_T.c.id == row["id"]).values(
                source_metadata=meta, last_updated_at=_T.c.last_updated_at))
            stats["updated" if block["in_catalog"] else "marked_not_in_catalog"] += 1

    logger.info(f"[catalog] dataset_registry mirror: {stats}")
    return stats
