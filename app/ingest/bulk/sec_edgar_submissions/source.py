"""
EDGAR submissions bulk loader (SPEC_111).

One nightly rolling snapshot (``submissions.zip``, ~1.5GB+, one JSON per filer)
is loaded into:

- ``public.sec_filers``              key cik (10-digit TEXT)
- ``public.sec_filer_former_names``  key (cik, name, from_date)
- ``public.sec_8k_index``            key accession_number; 8-K / 8-K/A filed in
                                     the last 3 years only (from filings.recent)

The zip is read one member at a time. Filer rows stream straight into COPY.
Former-name and 8-K rows are spooled to temp files on disk during that same
pass and copied afterwards, so memory stays bounded to one filer document.
"""

from __future__ import annotations

import json
import logging
import tempfile
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Dict, Iterator, List, Optional

from sqlalchemy import text

from app.core.copy_loader import copy_rows, create_staging, drop_staging, merge_staging
from app.ingest.bulk.base import BulkSource, Release
from app.ingest.bulk.registry import register_bulk_source
from app.ingest.bulk.sec_edgar_submissions import parse

logger = logging.getLogger(__name__)

SUBMISSIONS_URL = "https://www.sec.gov/Archives/edgar/daily-index/bulkdata/submissions.zip"

STG_FILERS = "sec_edgar_filers"
STG_FORMER = "sec_edgar_former_names"
STG_8K = "sec_edgar_8k_index"


def _table_ddl(table: str, columns: List[str], types: Dict[str, str], pk: List[str]) -> str:
    cols = []
    for c in columns:
        t = types[c]
        if c == "loaded_at":
            t = "TIMESTAMP DEFAULT NOW()"
        elif c in pk:
            t = f"{t} NOT NULL"
        cols.append(f"{c} {t}")
    return f"CREATE TABLE IF NOT EXISTS {table} ({', '.join(cols)}, PRIMARY KEY ({', '.join(pk)}))"


def _snapshot_date(release: Release) -> date:
    key = release.release_key
    if key.startswith("snapshot:"):
        try:
            return date.fromisoformat(key.split(":", 1)[1])
        except ValueError:
            pass
    return datetime.now(timezone.utc).date()


@register_bulk_source
class EdgarSubmissionsBulk(BulkSource):
    name = "sec_edgar_submissions"
    parser_version = "1"

    def discover(self, http, since: Optional[date] = None) -> List[Release]:
        """The file is a rolling snapshot: one release per UTC day, no index page to read."""
        today = datetime.now(timezone.utc).date()
        return [Release(f"snapshot:{today.isoformat()}", SUBMISSIONS_URL, {"snapshot_date": today.isoformat()})]

    def ddl(self) -> List[str]:
        return [
            _table_ddl("public.sec_filers", parse.FILER_COLUMNS, parse.FILER_TYPES, ["cik"]),
            "CREATE INDEX IF NOT EXISTS ix_sec_filers_ein ON public.sec_filers (ein)",
            "CREATE INDEX IF NOT EXISTS ix_sec_filers_sic ON public.sec_filers (sic)",
            "CREATE INDEX IF NOT EXISTS ix_sec_filers_lower_name ON public.sec_filers (lower(name))",
            "CREATE INDEX IF NOT EXISTS ix_sec_filers_tickers ON public.sec_filers USING GIN (tickers)",
            _table_ddl("public.sec_filer_former_names", parse.FORMER_NAME_COLUMNS,
                       parse.FORMER_NAME_TYPES, ["cik", "name", "from_date"]),
            "CREATE INDEX IF NOT EXISTS ix_sec_filer_former_names_lower_name "
            "ON public.sec_filer_former_names (lower(name))",
            _table_ddl("public.sec_8k_index", parse.EIGHT_K_COLUMNS, parse.EIGHT_K_TYPES,
                       ["accession_number"]),
            "CREATE INDEX IF NOT EXISTS ix_sec_8k_index_cik_filing_date ON public.sec_8k_index (cik, filing_date)",
            "CREATE INDEX IF NOT EXISTS ix_sec_8k_index_filing_date ON public.sec_8k_index (filing_date)",
        ]

    def load(self, conn, release: Release, path: Path) -> Dict[str, int]:
        self.ensure_ddl(conn)
        snapshot = _snapshot_date(release)
        cutoff = parse.cutoff_date(snapshot)
        key = release.release_key
        loaded_at = datetime.utcnow().isoformat(sep=" ")
        stats = parse.MemberStats()

        specs = [
            (STG_FILERS, "public.sec_filers", parse.FILER_COLUMNS, parse.FILER_TYPES, ["cik"]),
            (STG_FORMER, "public.sec_filer_former_names", parse.FORMER_NAME_COLUMNS,
             parse.FORMER_NAME_TYPES, ["cik", "name", "from_date"]),
            (STG_8K, "public.sec_8k_index", parse.EIGHT_K_COLUMNS, parse.EIGHT_K_TYPES, ["accession_number"]),
        ]
        for stg, _target, cols, types, _keys in specs:
            create_staging(conn, stg, [(c, types[c]) for c in cols])

        with tempfile.TemporaryFile("w+", encoding="utf-8") as fn_spool, \
                tempfile.TemporaryFile("w+", encoding="utf-8") as k_spool:

            def filer_rows() -> Iterator[tuple]:
                for _member, doc in parse.iter_filer_docs(Path(path), stats):
                    row = parse.filer_row(doc, key, loaded_at)
                    if row is None:
                        continue
                    for r in parse.former_name_rows(doc, key, loaded_at):
                        fn_spool.write(json.dumps(r) + "\n")
                    for r in parse.eight_k_rows(doc, cutoff, key, loaded_at):
                        k_spool.write(json.dumps(r) + "\n")
                    yield row

            def spooled(fh) -> Iterator[tuple]:
                fh.flush()
                fh.seek(0)
                for line in fh:
                    yield tuple(json.loads(line))

            n_filers = copy_rows(conn, STG_FILERS, parse.FILER_COLUMNS, filer_rows())
            n_former = copy_rows(conn, STG_FORMER, parse.FORMER_NAME_COLUMNS, spooled(fn_spool))
            n_8k = copy_rows(conn, STG_8K, parse.EIGHT_K_COLUMNS, spooled(k_spool))

        logger.info(
            f"[bulk:{self.name}] {key}: staged filers={n_filers} former_names={n_former} 8k={n_8k} "
            f"(overflow_skipped={stats.overflow_skipped} bad_json={stats.bad_json} "
            f"other_skipped={stats.other_skipped})"
        )

        result: Dict[str, int] = {}
        for stg, target, cols, _types, keys in specs:
            inserted, updated = merge_staging(conn, stg, target, cols, keys)
            result[target.split(".", 1)[1]] = inserted + updated
            drop_staging(conn, stg)

        pruned = conn.execute(
            text("DELETE FROM public.sec_8k_index WHERE filing_date < :cutoff"), {"cutoff": cutoff}
        ).rowcount
        if pruned:
            logger.info(f"[bulk:{self.name}] {key}: pruned {pruned} 8-K rows filed before {cutoff}")
        return result
