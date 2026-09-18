"""
SEC Insider Transactions Data Sets (Forms 3/4/5) bulk source (SPEC_110).

Quarterly ``YYYYqN_form345.zip`` files from
https://www.sec.gov/data-research/sec-markets-data/insider-transactions-data-sets
load into ``public.sec_insider_filings`` / ``_owners`` / ``_transactions`` /
``_footnotes``. The people pipeline's ``public.insider_transactions`` is not touched.
"""

from __future__ import annotations

import logging
import os
import re
import zipfile
from datetime import date
from pathlib import Path
from typing import Callable, Dict, Iterator, List, Optional, Sequence, Tuple

from sqlalchemy import text

from app.core.copy_loader import copy_rows, create_staging, drop_staging, merge_staging
from app.core.safe_sql import qi
from app.ingest.bulk.base import BulkSource, Release
from app.ingest.bulk.registry import register_bulk_source
from app.ingest.bulk.sec_insider import parse as p

logger = logging.getLogger(__name__)

INDEX_URL = "https://www.sec.gov/data-research/sec-markets-data/insider-transactions-data-sets"

# (target table, columns, key columns, row iterator, is_child_of_filing)
_Target = Tuple[str, List[Tuple[str, str]], List[str], Callable[[zipfile.ZipFile, str], Iterator[tuple]], bool]
TARGETS: List[_Target] = [
    ("sec_insider_filings", p.FILING_COLUMNS, ["accession_number"], p.iter_filings, False),
    ("sec_insider_owners", p.OWNER_COLUMNS, ["accession_number", "rptowner_cik"], p.iter_owners, True),
    ("sec_insider_transactions", p.TRANSACTION_COLUMNS, ["accession_number", "table_type", "trans_sk"],
     p.iter_transactions, True),
    ("sec_insider_footnotes", p.FOOTNOTE_COLUMNS, ["accession_number", "footnote_id"], p.iter_footnotes, True),
]

_INDEXES = [
    ("sec_insider_filings", "issuer_cik"),
    ("sec_insider_filings", "issuer_trading_symbol"),
    ("sec_insider_filings", "filing_date"),
    ("sec_insider_owners", "rptowner_cik"),
    ("sec_insider_transactions", "trans_date"),
    ("sec_insider_transactions", "trans_code"),
]


def _create_table_sql(table: str, columns: Sequence[Tuple[str, str]], keys: Sequence[str]) -> str:
    cols = [f"{name} {typ}" + (" NOT NULL" if name in keys else "") for name, typ in columns]
    cols.append("loaded_at TIMESTAMP DEFAULT NOW()")
    return (f"CREATE TABLE IF NOT EXISTS public.{table} (\n    " + ",\n    ".join(cols)
            + f",\n    PRIMARY KEY ({', '.join(keys)})\n)")


def _safe_suffix(release_key: str) -> str:
    return re.sub(r"[^a-z0-9_]", "_", release_key.lower())[:24]


def _delete_stale_children_sql(stg: str, table: str, keys: Sequence[str]) -> str:
    match = " AND ".join(f"k.{qi(c)} = t.{qi(c)}" for c in keys)
    return (
        f"DELETE FROM public.{qi(table)} t "
        f"USING (SELECT DISTINCT accession_number FROM stg.{qi(stg)}) s "
        f"WHERE t.accession_number = s.accession_number "
        f"AND NOT EXISTS (SELECT 1 FROM stg.{qi(stg)} k WHERE {match})"
    )


@register_bulk_source
class SecInsiderDataSets(BulkSource):
    name = "sec_insider"
    parser_version = "1"
    default_quarters = p.DEFAULT_QUARTERS

    def discover(self, http, since: Optional[date] = None) -> List[Release]:
        html = http.get_text(INDEX_URL)
        pairs = p.parse_index(html, INDEX_URL)
        if not pairs:
            raise RuntimeError(f"[sec_insider] no *_form345.zip links found on {INDEX_URL}")
        chosen = p.select_releases(pairs, since, self.default_quarters)
        return [Release(key, url, {"quarter_end": p.quarter_end(key).isoformat()}) for key, url in chosen]

    def ddl(self) -> List[str]:
        stmts = [_create_table_sql(t, cols, keys) for t, cols, keys, _, _ in TARGETS]
        stmts += [f"CREATE INDEX IF NOT EXISTS ix_{t}_{c} ON public.{t} ({c})" for t, c in _INDEXES]
        return stmts

    def load(self, conn, release: Release, path: Path) -> Dict[str, int]:
        self.ensure_ddl(conn)
        suffix = _safe_suffix(release.release_key)
        rows: Dict[str, int] = {}
        load_footnotes = os.getenv("BULK_INSIDER_LOAD_FOOTNOTES", "0") == "1"
        with zipfile.ZipFile(path) as zf:
            for table, columns, keys, iterator, is_child in TARGETS:
                if table == "sec_insider_footnotes" and not load_footnotes:
                    continue  # ~1M rows per 12 quarters; opt-in (Lean disk budget)
                stg = f"{table}_{suffix}"
                names = [c for c, _ in columns]
                create_staging(conn, stg, columns)
                try:
                    copied = copy_rows(conn, stg, names, iterator(zf, release.release_key))
                    conn.execute(text(f"ANALYZE stg.{qi(stg)}"))
                    deleted = 0
                    if is_child:
                        deleted = conn.execute(text(_delete_stale_children_sql(stg, table, keys))).rowcount or 0
                    inserted, updated = merge_staging(conn, stg, f"public.{table}", names, keys)
                finally:
                    drop_staging(conn, stg)
                rows[table] = inserted + updated
                logger.info(f"[bulk:sec_insider] {release.release_key} {table}: copied={copied} "
                            f"inserted={inserted} updated={updated} stale_deleted={deleted}")
        return rows
