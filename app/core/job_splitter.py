"""
Job splitter for distributed parallel collection.

Splits geographic data sources into N parallel jobs (one per state-group),
each claimed by different workers via SELECT FOR UPDATE SKIP LOCKED.

The distributed rate limiter (rate_limit_bucket table) coordinates API
rate limits across all workers hitting the same domain.
"""

import logging
import math
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

# All 50 states + DC
ALL_STATES = [
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "DC", "FL",
    "GA", "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME",
    "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH",
    "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI",
    "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI",
    "WY",
]


@dataclass
class SplitConfig:
    """Configuration for splitting a source into parallel jobs."""

    source_key: str
    split_by: str  # "state" for geographic splitting
    partition_size: int  # number of states per partition
    rate_limit_domain: str  # API domain for distributed rate limiting


# Registry of sources that can be split into parallel jobs.
# partition_size controls how many states each worker handles.
# Smaller partition = more parallelism but more overhead.
SPLIT_REGISTRY: Dict[str, Tuple[str, int, str]] = {
    # Site intel collectors (slow, state-iterable)
    "nrel_resource":    ("state", 5,  "developer.nrel.gov"),
    "usgs_3dep":        ("state", 5,  "epqs.nationalmap.gov"),
    "epa_sdwis":        ("state", 5,  "data.epa.gov"),
    "bls_qcew":         ("state", 8,  "data.bls.gov"),
    "epa_acres":        ("state", 7,  "data.epa.gov"),
    "fema_nfhl":        ("state", 7,  "hazards.fema.gov"),
    "fcc":              ("state", 7,  "opendata.fcc.gov"),
    "epa_envirofacts":  ("state", 7,  "data.epa.gov"),
    "usfws_nwi":        ("state", 7,  "fwsprimary.wim.usgs.gov"),
    "eia":              ("state", 13, "api.eia.gov"),
    # Ingest-based sources (large, state-filterable)
    "census":           ("state", 10, "api.census.gov"),
    "cms":              ("state", 10, "data.cms.gov"),
    "nppes":            ("state", 5,  "npiregistry.cms.hhs.gov"),
    "epa_echo":         ("state", 5,  "echodata.epa.gov"),
}


def get_split_config(source_key: str) -> Optional[SplitConfig]:
    """
    Look up the split configuration for a source.

    Returns None if the source is not splittable.
    """
    entry = SPLIT_REGISTRY.get(source_key)
    if entry is None:
        return None
    split_by, partition_size, domain = entry
    return SplitConfig(
        source_key=source_key,
        split_by=split_by,
        partition_size=partition_size,
        rate_limit_domain=domain,
    )


def split_into_state_groups(
    source_key: str,
    states: Optional[List[str]] = None,
) -> List[List[str]]:
    """
    Partition states into groups for parallel collection.

    Args:
        source_key: The source key to look up partition size.
        states: Optional explicit state list. Defaults to ALL_STATES.

    Returns:
        List of state groups, e.g. [["AL","AK","AZ","AR","CA"], ["CO","CT",...], ...]
    """
    config = get_split_config(source_key)
    if config is None:
        # Not splittable — return all states as a single group
        return [states or ALL_STATES]

    target_states = states or ALL_STATES
    size = config.partition_size

    groups = []
    for i in range(0, len(target_states), size):
        groups.append(target_states[i : i + size])

    return groups


def create_split_jobs(
    db: Session,
    source_key: str,
    job_type: str,
    base_payload: Dict,
    priority: int = 0,
    queue_status=None,
    states: Optional[List[str]] = None,
) -> List[int]:
    """
    Create N parallel job_queue entries for a splittable source.

    Each job gets a subset of states in its payload. All share the same
    batch_run_id (if present in base_payload) for tracking.

    Args:
        db: Database session
        source_key: Source key (must be in SPLIT_REGISTRY)
        job_type: Queue job type (e.g. "site_intel", "ingestion")
        base_payload: Base payload dict (will be copied per split)
        priority: Job priority
        queue_status: Optional initial status (e.g. BLOCKED for tier 2+)
        states: Optional explicit state list to split

    Returns:
        List of created job_queue IDs
    """
    from app.core.job_queue_service import submit_job
    from app.core.models import IngestionJob, JobStatus

    config = get_split_config(source_key)
    if config is None:
        raise ValueError(f"Source '{source_key}' is not in SPLIT_REGISTRY")

    state_groups = split_into_state_groups(source_key, states)
    job_ids = []

    for group_idx, state_group in enumerate(state_groups):
        # Build per-split payload
        split_payload = {**base_payload}
        split_payload["states"] = state_group
        split_payload["split_group"] = group_idx
        split_payload["split_total"] = len(state_groups)
        split_payload["split_source"] = source_key

        # For site_intel jobs, sources must be a list
        if job_type == "site_intel" and "sources" not in split_payload:
            split_payload["sources"] = [source_key]

        # Create ingestion job record if batch metadata present
        ing_job_id = base_payload.get("ingestion_job_id")
        if "batch_id" in base_payload and not ing_job_id:
            is_blocked = queue_status is not None
            ing_status = JobStatus.BLOCKED if is_blocked else JobStatus.PENDING
            ing_job = IngestionJob(
                source=f"{source_key}:split_{group_idx}",
                status=ing_status,
                config={"states": state_group},
                batch_run_id=base_payload.get("batch_id"),
                trigger="batch",
                tier=base_payload.get("tier"),
            )
            db.add(ing_job)
            db.flush()
            split_payload["ingestion_job_id"] = ing_job.id
            ing_job_id = ing_job.id

        result = submit_job(
            db=db,
            job_type=job_type,
            payload=split_payload,
            priority=priority,
            job_table_id=ing_job_id,
            status=queue_status,
        )

        qid = result.get("job_queue_id")
        if qid:
            job_ids.append(qid)

    logger.info(
        f"Split {source_key} into {len(state_groups)} parallel jobs "
        f"({config.partition_size} states each): {job_ids}"
    )

    return job_ids
