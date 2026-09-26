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

SPEC_143 aliases, for ``ingestion_jobs.source`` values that are neither a
producer nor a dispatch key (the 322 rows SPEC_144's backfill left unresolved):

- ``api:<source>``: a router that records its in-process run under its own
  name (``form_d``, ``app_rankings``, ``census_cbp`` ...) is that producer;
- ``JOB_SOURCE_DATASETS``: legacy or per-router source names whose rows land
  in one dataset (``job_postings``, ``international_econ_oecd`` ...);
- ``config["tables"]``: a backfilled row that names its tables resolves to the
  one dataset that declares all of them (shared tables stay unresolved).

A producer that maps to several datasets (``job:pe_mart_build``) stays
unresolved: the aliases never override it.
"""

from __future__ import annotations

from functools import lru_cache
from typing import Any, Dict, Iterable, List, Optional, Tuple

from app.catalog.spec import DatasetSpec

# core.mart_build.mart -> the worker job type that builds it (SPEC_126a MART names)
MART_JOB_TYPES = {"pe_marts": "pe_mart_build", "entity_resolve": "entity_resolve"}

# ingestion_jobs.source -> dataset key, for sources that name no producer (SPEC_143).
JOB_SOURCE_DATASETS: Dict[str, str] = {
    # /job-postings router runs (company / all / discover) record the bare source
    "job_postings": "job_postings",
    # the skills backfill rewrites requirements on existing job_postings rows
    "job_postings_skills": "job_postings",
    # /usda router runs without config.dataset (incremental / all)
    "usda": "usda_nass",
    # app.sources.international_econ records f"international_econ_{source}"
    "international_econ_oecd": "intl_oecd",
    "international_econ_worldbank": "intl_worldbank",
    "international_econ_bis": "intl_bis",
    "international_econ_imf": "intl_imf",
}


def base_producer(producer: str) -> str:
    """'job:pe_mart_build#firms' -> 'job:pe_mart_build'."""
    return producer.split("#", 1)[0]


class ProducerMap:
    """Producer strings -> dataset keys, for one set of specs."""

    def __init__(self, specs: Iterable[DatasetSpec]):
        specs = tuple(specs)
        self.by_base: Dict[str, List[str]] = {}
        self.dispatch_keys = set()
        self.keys = {s.key for s in specs}
        self.table_owners: Dict[str, set] = {}
        for spec in specs:
            for t in spec.tables:
                self.table_owners.setdefault(t, set()).add(spec.key)
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
        if key:
            return f"dispatch:{key}"
        # a router's in-process run, recorded under the router's name (SPEC_143)
        api = f"api:{self._base(source)}"
        return api if api in self.by_base else None

    @staticmethod
    def _base(source: str) -> str:
        return source.split(":split_")[0] if ":split_" in source else source

    def dataset_for_tables(self, tables: Any) -> Optional[str]:
        """The one dataset that declares every table in ``tables``."""
        if not isinstance(tables, (list, tuple)) or not tables:
            return None
        owners = None
        for t in tables:
            if not isinstance(t, str):
                return None
            got = self.table_owners.get(t, set())
            owners = set(got) if owners is None else owners & got
        return next(iter(owners)) if owners and len(owners) == 1 else None

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
        producer = self.producer_for_job(source, config)
        keys = self.datasets_for(producer)
        if producer and keys:
            # several datasets (job:pe_mart_build): ambiguous, never aliased
            return keys[0] if len(keys) == 1 else None
        if not source:
            return None
        alias = JOB_SOURCE_DATASETS.get(self._base(source))
        if alias in self.keys:
            return alias
        if isinstance(config, dict):
            return self.dataset_for_tables(config.get("tables"))
        return None


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
