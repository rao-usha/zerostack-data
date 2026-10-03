"""Run ONE entity_resolve job through the real job path in a one-off worker container.

The shared workers run the image's code until they are restarted; a PENDING row could be claimed
by one of them. So the row is inserted already CLAIMED by this process (worker_id 'oneoff-<tag>')
and executed here with the worker's own `execute_job` (heartbeat, ledger, mart_build guard).

    docker-compose run --rm --no-deps -e PYTHONPATH=/app -v "$PWD/scripts:/app/scripts" worker \
        python scripts/run_entity_resolve_oneoff.py --dry-run --tag spec154
    ... --tag spec154            (LIVE)

Prints the job id, final status and the resolve summary keys the caller needs.
"""

import argparse
import asyncio
import json
from datetime import datetime

from app.core.database import get_session_factory
from app.core.models_queue import JobQueue, QueueJobStatus, QueueJobType
from app.worker import main as worker


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--tag", default="oneoff")
    a = ap.parse_args()
    db = get_session_factory()()
    payload = {"dry_run": bool(a.dry_run)}
    now = datetime.utcnow()
    job = JobQueue(job_type=QueueJobType.ENTITY_RESOLVE, status=QueueJobStatus.CLAIMED,
                   worker_id=f"oneoff-{a.tag}", claimed_at=now, heartbeat_at=now, payload=payload,
                   priority=5)
    db.add(job)
    db.commit()
    print(f"job_queue id {job.id} payload {payload}", flush=True)
    worker._load_executors()
    asyncio.run(worker.execute_job(job, db))
    db.refresh(job)
    print(f"job {job.id} status {job.status} error {job.error_message}", flush=True)
    print(json.dumps({"id": job.id, "status": str(job.status), "progress": job.progress_message},
                     default=str), flush=True)
    return 0 if str(job.status).endswith("success") else 1


if __name__ == "__main__":
    raise SystemExit(main())
