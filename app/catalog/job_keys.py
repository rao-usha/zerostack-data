"""
From run history to catalog datasets (SPEC_124).

Every store that records a run names its producer differently:

| Store | Names it as | Producer |
|---|---|---|
| ``ingestion_jobs`` | ``source`` (+ ``config["dataset"]``) | ``bulk:<name>`` / ``job:<type>`` / ``dispatch:<key>`` |
| ``job_queue`` | ``job_type`` + payload | ``bulk:<payload.bulk_source>``, ``dispatch:<...>``, ``collector:<payload.sources[]>``, ``job:<type>`` |
| ``raw.source_release`` | ``source`` | ``bulk:<source>`` |
| ``core.mart_build`` | ``mart`` | ``job:pe_mart_build`` / ``job:entity_resolve`` |
| ``site_intel_collection_job`` | ``source`` | ``collector:<source>`` |

A dispatch key resolves the way ``jobs._run_dispatched_job`` resolves it:
``<base>:<config.dataset>`` when that is a key, else ``<base>`` (split jobs
``<key>:split_<n>`` strip their suffix first).

A ``job:<type>`` producer is the base of every stage producer
(``job:pe_mart_build#firms``, ``#funds`` ...): one run rebuilds all of them.
"""

from __future__ import annotations

from functools import lru_cache
from typing import Any, Dict, Iterable, List, Optional, Tuple

from app.catalog.spec import DatasetSpec

# core.mart_build.mart -> the worker job type that builds it (SPEC_126a MART names)
MART_JOB_TYPES = {"pe_marts": "pe_mart_build", "entity_resolve": "entity_resolve"}


def base_producer(producer: str) -> str:
    """'job:pe_mart_build#firms' -> 'job:pe_mart_build'."""
    return producer.split("#", 1)[0]


class ProducerMap:
    """Producer strings -> dataset keys, for one set of specs."""

    def __init__(self, specs: Iterable[DatasetSpec]):
        self.by_base: Dict[str, List[str]] = {}
        self.dispatch_keys = set()
        for spec in specs:
            for p in spec.producers:
                keys = self.by_base.setdefault(base_producer(p), [])
                if spec.key not in keys:
                    keys.append(spec.key)
                if p.startswith("dispatch:"):
                    self.dispatch_keys.add(base_producer(p)[len("dispatch:"):])

    def datasets_for(self, producer: Optional[str]) -> Tuple[str, ...]:
        if not producer:
            return ()
        return tuple(self.by_base.get(base_producer(producer), ()))

    def dispatch_key_for(self, source: Optional[str], dataset: Optional[str]) -> Optional[str]:
        if not source:
            return None
        base = source.split(":split_")[0] if ":split_" in source else source
        if dataset and f"{base}:{dataset}" in self.dispatch_keys:
            return f"{base}:{dataset}"
        if base in self.dispatch_keys:
            return base
        return None

    def producer_for_job(self, source: Optional[str], config: Any) -> Optional[str]:
        """The producer an ``ingestion_jobs`` row (or ingestion payload) ran."""
        if not source:
            return None
        if source.startswith(("bulk:", "job:")):
            return source
        dataset = config.get("dataset") if isinstance(config, dict) else None
        key = self.dispatch_key_for(source, dataset if isinstance(dataset, str) else None)
        return f"dispatch:{key}" if key else None

    def producers_for_queue(self, job_type: Optional[str], payload: Any) -> List[str]:
        payload = payload if isinstance(payload, dict) else {}
        if job_type == "bulk_ingest":
            name = payload.get("bulk_source")
            return [f"bulk:{name}"] if name else []
        if job_type == "ingestion":
            p = self.producer_for_job(payload.get("source"), payload.get("config") or {})
            return [p] if p else []
        if job_type == "site_intel":
            sources = payload.get("sources") or []
            return [f"collector:{s}" for s in sources if isinstance(s, str)]
        if job_type:
            return [f"job:{job_type}"]
        return []

    def dataset_key_for_job(self, source: Optional[str], config: Any) -> Optional[str]:
        keys = self.datasets_for(self.producer_for_job(source, config))
        return keys[0] if len(keys) == 1 else None


@lru_cache(maxsize=1)
def default_map() -> ProducerMap:
    from app.catalog.registry import get_catalog

    return ProducerMap(get_catalog())


def producer_for_job(source: Optional[str], config: Any) -> Optional[str]:
    return default_map().producer_for_job(source, config)


def producers_for_queue(job_type: Optional[str], payload: Any) -> List[str]:
    return default_map().producers_for_queue(job_type, payload)


def dataset_key_for_job(source: Optional[str], config: Any) -> Optional[str]:
    """The one dataset an IngestionJob produces; None when zero or several."""
    return default_map().dataset_key_for_job(source, config)


def producer_for_mart(mart: Optional[str]) -> Optional[str]:
    job_type = MART_JOB_TYPES.get(mart or "")
    return f"job:{job_type}" if job_type else None


def producer_for_release(source: Optional[str]) -> Optional[str]:
    return f"bulk:{source}" if source else None


def producer_for_collector_job(source: Optional[str]) -> Optional[str]:
    return f"collector:{source}" if source else None
