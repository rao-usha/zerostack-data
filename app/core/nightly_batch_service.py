"""
DEPRECATED — import from app.core.batch_service instead.

This module re-exports everything from batch_service for backwards compatibility.
It will be removed in a future release.
"""

from app.core.batch_service import *  # noqa: F401,F403
from app.core.batch_service import (  # explicit re-exports for patch targets
    WORKER_MODE,
    Tier,
    TIER_1,
    TIER_2,
    TIER_3,
    TIER_4,
    TIERS,
    TIER_BY_LEVEL,
    resolve_effective_tiers,
    launch_batch_collection,
    get_batch_run_status,
    list_batch_runs,
    cancel_batch_run,
    rerun_failed_in_batch,
    check_and_notify_batch_completion,
    get_batch_status,
    list_batches,
    scheduled_batch_collection,
    scheduled_nightly_batch,
    launch_nightly_batch,
    submit_job,
)
