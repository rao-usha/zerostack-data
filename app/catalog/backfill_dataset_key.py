"""
Backfill ``ingestion_jobs.dataset_key`` for historical rows (SPEC_144).

Alembic 0014 added the column and a ``before_insert`` listener that fills it
for new jobs; older rows stayed NULL and are resolved at read time. PLAN_088
open decision 10 approved a backfill. There is no migration: this runs as

    python -m app.catalog.backfill_dataset_key           # dry run: the count report
    python -m app.catalog.backfill_dataset_key --apply   # write

or ``POST /api/v1/catalog/admin/backfill-dataset-key?apply=true`` (admin).

Resolution is ``ProducerMap.datasets_for_job`` (``dataset_key_for_job`` when it
is one dataset) -- the same one the insert listener uses, SPEC_143 aliases
included (``api:<source>``, ``JOB_SOURCE_DATASETS``, ``config["tables"]``). Only NULL rows are touched and a value is never
overwritten, so a rerun is a no-op for rows already filled and picks up rows
that a later alias (SPEC_143) makes resolvable. Rows whose producer maps to
several datasets (``job:pe_mart_build``) stay NULL. Updates go in batches
under a short ``lock_timeout`` and are retried, like 0014's ALTER.
"""

from __future__ import annotations

import json
import logging
import sys
import time
from collections import Counter
from typing import Any, Dict, List, Optional

from sqlalchemy import bindparam, text

logger = logging.getLogger(__name__)

BATCH_SIZE = 500
READ_CHUNK = 5000
LOCK_TIMEOUT = "2s"
LOCK_ATTEMPTS = 5
RETRY_SLEEP_S = 1.0


def _parse_config(config: Any) -> Any:
    if isinstance(config, str):
        try:
            return json.loads(config)
        except ValueError:
            return None
    return config


def _is_pg(engine) -> bool:
    return getattr(getattr(engine, "dialect", None), "name", "") == "postgresql"


def plan(engine, pmap=None) -> Dict[str, Any]:
    """Resolve every NULL row in memory. Returns the report and the updates
    (``{dataset_key: [ids]}``)."""
    from app.catalog.job_keys import default_map

    pmap = pmap or default_map()
    resolved: Dict[str, List[int]] = {}
    ambiguous: Counter = Counter()
    unresolved: Counter = Counter()
    total = 0
    last_id = 0
    while True:
        with engine.connect() as conn:
            rows = conn.execute(text(
                "SELECT id, source, config FROM ingestion_jobs "
                "WHERE dataset_key IS NULL AND id > :last ORDER BY id LIMIT :n"
            ), {"last": last_id, "n": READ_CHUNK}).fetchall()
        if not rows:
            break
        for job_id, source, config in rows:
            total += 1
            # the insert listener's resolver (SPEC_143 aliases included)
            keys = pmap.datasets_for_job(source, _parse_config(config))
            if len(keys) == 1:
                resolved.setdefault(keys[0], []).append(int(job_id))
            elif keys:
                ambiguous[source or ""] += 1
            else:
                unresolved[source or ""] += 1
        last_id = int(rows[-1][0])
    with engine.connect() as conn:
        rows_total = int(conn.execute(text("SELECT count(*) FROM ingestion_jobs")).scalar() or 0)
    n_resolved = sum(len(v) for v in resolved.values())
    report = {
        "rows_total": rows_total,
        "null_before": total,
        "resolvable": n_resolved,
        "ambiguous": sum(ambiguous.values()),
        "unresolved": sum(unresolved.values()),
        "by_dataset": {k: len(v) for k, v in sorted(resolved.items(), key=lambda kv: (-len(kv[1]), kv[0]))},
        "ambiguous_sources": dict(ambiguous.most_common()),
        "unresolved_sources": dict(unresolved.most_common()),
    }
    return {"report": report, "updates": resolved}


def _update_batch(engine, key: str, ids: List[int]) -> int:
    stmt = text(
        "UPDATE ingestion_jobs SET dataset_key = :k WHERE id IN :ids AND dataset_key IS NULL"
    ).bindparams(bindparam("ids", expanding=True))
    for attempt in range(1, LOCK_ATTEMPTS + 1):
        try:
            with engine.begin() as conn:
                if _is_pg(engine):
                    conn.execute(text(f"SET LOCAL lock_timeout = '{LOCK_TIMEOUT}'"))
                return int(conn.execute(stmt, {"k": key, "ids": ids}).rowcount or 0)
        except Exception as e:
            if attempt == LOCK_ATTEMPTS or "lock" not in str(e).lower():
                raise
            logger.info(f"[backfill_dataset_key] lock wait timed out (attempt {attempt}), retrying")
            time.sleep(RETRY_SLEEP_S * attempt)
    return 0


def backfill(engine, apply: bool = False, batch_size: int = BATCH_SIZE, pmap=None) -> Dict[str, Any]:
    """The count report; with ``apply`` also writes and reports ``updated``."""
    result = plan(engine, pmap)
    report = dict(result["report"], applied=bool(apply), updated=0)
    if apply:
        for key, ids in result["updates"].items():
            for i in range(0, len(ids), batch_size):
                report["updated"] += _update_batch(engine, key, ids[i:i + batch_size])
        with engine.connect() as conn:
            report["null_after"] = int(conn.execute(text(
                "SELECT count(*) FROM ingestion_jobs WHERE dataset_key IS NULL")).scalar() or 0)
    logger.info(f"[backfill_dataset_key] {'applied' if apply else 'dry run'}: "
                f"{report['resolvable']}/{report['null_before']} resolvable, updated {report['updated']}")
    return report


def main(argv: Optional[List[str]] = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    from app.core.database import get_engine

    report = backfill(get_engine(), apply="--apply" in argv)
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
