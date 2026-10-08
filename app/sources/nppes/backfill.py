"""
NPI gap backfill: utilization NPIs missing from nppes_providers (SPEC_162).

    python -m app.sources.nppes.backfill                 # dry run: gap report + 3-NPI sample fetch
    python -m app.sources.nppes.backfill --sample 10     # dry run with a bigger sample
    python -m app.sources.nppes.backfill --apply         # fetch every missing NPI and upsert
    python -m app.sources.nppes.backfill --apply --limit 500 --rps 1.5

PLAN_100 §7.1: only 233 of 3,216 cms_medicare_utilization NPIs were in nppes_providers. Owner
decision D4: close the gap through the public NPPES Registry API
(https://npiregistry.cms.hhs.gov/api/?version=2.1, one ``number=`` lookup per NPI), not the
V.2 bulk file.

- Requests go through ``NPPESClient`` (bounded concurrency 1, NexdataResearch User-Agent,
  retries with backoff). Pacing is local (``--rps``, default 1.0, capped at 2.0: the project's
  1-2 req/s rule) plus, under WORKER_MODE=1, the shared ``npiregistry.cms.hhs.gov`` bucket in
  ``rate_limit_bucket`` (2 req/s across every process).
- The dry run (default) never writes: it counts the gap and fetches ``--sample`` NPIs to
  prove the parse.
- ``--apply`` upserts with null-preserving semantics: ``COALESCE(EXCLUDED.col, existing.col)``,
  so a sparse Registry answer can never blank a column we already hold. Commits every
  ``--batch`` rows; a re-run skips NPIs already present, so it is resumable.
- NPIs the Registry does not return (deactivated or retired: the API serves active NPIs
  only) are counted as ``not_found`` and never inserted.

70 min worst case for ~3k NPIs at 1 req/s (about 50 min measured pacing plus retries).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import re
import sys
import time
from typing import Any, Dict, List, Optional, Sequence

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.sources.nppes import metadata
from app.sources.nppes.client import NPPESClient

logger = logging.getLogger(__name__)

TABLE = "nppes_providers"
SOURCE_TABLE = "cms_medicare_utilization"
MAX_RPS = 2.0
DEFAULT_RPS = 1.0
_NPI_RE = re.compile(r"^\d{10}$")

GAP_SQL = (
    "SELECT DISTINCT u.rndrng_npi FROM cms_medicare_utilization u "
    "WHERE u.rndrng_npi IS NOT NULL "
    "AND NOT EXISTS (SELECT 1 FROM nppes_providers p WHERE p.npi = u.rndrng_npi) "
    "ORDER BY u.rndrng_npi"
)
UTILIZATION_NPIS_SQL = "SELECT count(DISTINCT rndrng_npi) FROM cms_medicare_utilization"


def find_missing_npis(db: Session) -> List[str]:
    """Distinct utilization NPIs absent from nppes_providers (valid 10-digit values only)."""
    rows = db.execute(text(GAP_SQL)).fetchall()
    return [r[0].strip() for r in rows if r[0] and _NPI_RE.match(r[0].strip())]


def upsert_sql(columns: Sequence[str] = tuple(metadata.COLUMN_NAMES)) -> str:
    """INSERT ... ON CONFLICT (npi) DO UPDATE with COALESCE(EXCLUDED.col, existing.col)."""
    cols = list(columns)
    updates = ", ".join(f"{c} = COALESCE(EXCLUDED.{c}, {TABLE}.{c})" for c in cols if c != "npi")
    return (
        f"INSERT INTO {TABLE} ({', '.join(cols)}) VALUES ({', '.join(':' + c for c in cols)}) "
        f"ON CONFLICT (npi) DO UPDATE SET {updates}, ingestion_timestamp = CURRENT_TIMESTAMP"
    )


def null_preserving_upsert(db: Session, rows: List[Dict[str, Any]]) -> int:
    if not rows:
        return 0
    cols = list(metadata.COLUMN_NAMES)
    params = []
    for row in rows:
        rec = {c: row.get(c) for c in cols}  # identical keys for every row
        params.append(rec)
    db.execute(text(upsert_sql(cols)), params)
    return len(params)


def _ensure_table(db: Session) -> None:
    db.execute(text(metadata.CREATE_TABLE_SQL))


async def _fetch(client: NPPESClient, npi: str) -> Optional[Dict[str, Any]]:
    result = await client.lookup_npi(npi)
    if not result:
        return None
    row = metadata.parse_provider_record(result)
    if row.get("npi") != npi:  # the Registry answered for another number: never store it
        logger.warning(f"[nppes backfill] asked {npi}, got {row.get('npi')}; skipped")
        return None
    return row


async def run_backfill(
    db: Session,
    *,
    apply: bool = False,
    sample: int = 3,
    limit: Optional[int] = None,
    batch: int = 100,
    rps: float = DEFAULT_RPS,
    client: Optional[NPPESClient] = None,
) -> Dict[str, Any]:
    """Find the gap and (``apply``) fill it. Returns a JSON-able report."""
    rps = max(0.1, min(float(rps), MAX_RPS))
    started = time.monotonic()
    missing = find_missing_npis(db)
    total_util = db.execute(text(UTILIZATION_NPIS_SQL)).scalar() or 0
    todo = missing if apply else missing[: max(0, sample)]
    if apply and limit is not None:
        todo = todo[:limit]
    report: Dict[str, Any] = {
        "mode": "apply" if apply else "dry_run",
        "utilization_npis": int(total_util),
        "missing_before": len(missing),
        "requested": len(todo),
        "fetched": 0,
        "not_found": 0,
        "errors": 0,
        "upserted": 0,
        "rps": rps,
        "not_found_npis": [],
        "error_npis": [],
        "sample": [],
    }

    own = client is None
    if own:
        client = NPPESClient(max_concurrency=1, max_retries=3, backoff_factor=2.0)
    client.rate_limit_interval = 1.0 / rps
    pending: List[Dict[str, Any]] = []
    try:
        if apply and todo:
            _ensure_table(db)
            db.commit()
        for npi in todo:
            try:
                row = await _fetch(client, npi)
            except Exception as e:  # one bad NPI never stops the run
                report["errors"] += 1
                report["error_npis"].append(npi)
                logger.warning(f"[nppes backfill] {npi}: {e}")
                continue
            if row is None:
                report["not_found"] += 1
                report["not_found_npis"].append(npi)
                continue
            report["fetched"] += 1
            if not apply:
                report["sample"].append({k: row.get(k) for k in (
                    "npi", "entity_type", "taxonomy_code", "practice_state", "status")})
                continue
            pending.append(row)
            if len(pending) >= batch:
                report["upserted"] += null_preserving_upsert(db, pending)
                db.commit()
                pending = []
                logger.info(f"[nppes backfill] {report['upserted']}/{len(todo)} upserted")
        if apply and pending:
            report["upserted"] += null_preserving_upsert(db, pending)
            db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        if own:
            await client.close()

    report["missing_after"] = len(find_missing_npis(db)) if apply else len(missing)
    report["seconds"] = round(time.monotonic() - started, 1)
    return report


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    mode = ap.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true", default=True,
                      help="report the gap and fetch --sample NPIs; write nothing (default)")
    mode.add_argument("--apply", action="store_true", help="fetch every missing NPI and upsert")
    ap.add_argument("--sample", type=int, default=3, help="dry run: NPIs to fetch (default 3)")
    ap.add_argument("--limit", type=int, default=None, help="apply: at most this many NPIs")
    ap.add_argument("--batch", type=int, default=100, help="apply: rows per commit (default 100)")
    ap.add_argument("--rps", type=float, default=DEFAULT_RPS,
                    help=f"requests per second, capped at {MAX_RPS} (default {DEFAULT_RPS})")
    return ap


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    from app.core.database import get_session_factory

    db = get_session_factory()()
    try:
        report = asyncio.run(run_backfill(
            db, apply=bool(args.apply), sample=args.sample, limit=args.limit,
            batch=args.batch, rps=args.rps))
    finally:
        db.close()
    print(json.dumps(report, indent=1, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
