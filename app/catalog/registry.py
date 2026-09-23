"""
Catalog access (SPEC_123): lookups, filters and the producer index.

Built once at import. Duplicate dataset keys or a producer claimed by two
datasets raise immediately — the whole point of the catalog is that each
producer maps to exactly one dataset.
"""

from __future__ import annotations

from typing import Dict, List, Optional, Tuple

from app.catalog.datasets import CATALOG
from app.catalog.spec import DatasetSpec


def _index(specs: Tuple[DatasetSpec, ...]) -> Tuple[Dict[str, DatasetSpec], Dict[str, str]]:
    by_key: Dict[str, DatasetSpec] = {}
    by_producer: Dict[str, str] = {}
    for spec in specs:
        if spec.key in by_key:
            raise ValueError(f"duplicate dataset key {spec.key!r}")
        by_key[spec.key] = spec
        for producer in spec.producers:
            if producer in by_producer:
                raise ValueError(
                    f"producer {producer!r} claimed by {by_producer[producer]!r} and {spec.key!r}"
                )
            by_producer[producer] = spec.key
    for spec in specs:
        missing = [i for i in spec.inputs if i not in by_key]
        if missing:
            raise ValueError(f"dataset {spec.key!r} has unknown inputs {missing}")
    return by_key, by_producer


_BY_KEY, _BY_PRODUCER = _index(CATALOG)


def get_catalog() -> Tuple[DatasetSpec, ...]:
    return CATALOG


def get_spec(key: str) -> Optional[DatasetSpec]:
    return _BY_KEY.get(key)


def producer_index() -> Dict[str, str]:
    """producer string -> dataset key (primary and also_produced_by)."""
    return dict(_BY_PRODUCER)


def dataset_for_producer(producer: str) -> Optional[DatasetSpec]:
    key = _BY_PRODUCER.get(producer)
    return _BY_KEY.get(key) if key else None


def filter_specs(
    kind: Optional[str] = None,
    source: Optional[str] = None,
    status_public: Optional[str] = None,
    redistribution: Optional[str] = None,
    q: Optional[str] = None,
) -> List[DatasetSpec]:
    out = []
    needle = (q or "").strip().lower()
    for spec in CATALOG:
        if kind and spec.kind != kind:
            continue
        if source and spec.source != source:
            continue
        if status_public and spec.status_public != status_public:
            continue
        if redistribution and spec.redistribution != redistribution:
            continue
        if needle and needle not in f"{spec.key} {spec.display_name} {spec.description}".lower():
            continue
        out.append(spec)
    return out
