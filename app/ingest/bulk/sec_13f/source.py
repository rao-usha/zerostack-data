"""
SEC Form 13F data sets bulk source (SPEC_109).

Index: https://www.sec.gov/data-research/sec-markets-data/form-13f-data-sets
Each quarterly zip (``01jun2026-31aug2026_form13f.zip`` / ``2023q4_form13f.zip``)
is one release.

Disk budget:
- filings / cover / summary / signature / other managers: data sets whose
  end date falls in the last 3 years (default window);
- holdings (INFOTABLE, ~3.8M rows per data set): only the newest
  ``HOLDINGS_RELEASES`` data sets listed (env ``BULK_13F_HOLDINGS_RELEASES``).
  Loading a holdings release prunes holdings of releases no longer in the
  newest N (disable with ``BULK_13F_PRUNE_HOLDINGS=0``).
"""

from __future__ import annotations

import logging
import os
import zipfile
from datetime import date, datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence

from sqlalchemy import text

from app.core.copy_loader import copy_rows, create_staging, drop_staging, merge_staging
from app.ingest.bulk.base import BulkSource, Release
from app.ingest.bulk.registry import register_bulk_source
from app.ingest.bulk.sec_13f.parse import (
    FILING_COLUMNS,
    HOLDING_COLUMNS,
    INDEX_URL,
    OTHER_MANAGER_COLUMNS,
    DataSet,
    filing_dates,
    iter_filings,
    iter_holdings,
    iter_other_managers,
    parse_index,
)

logger = logging.getLogger(__name__)

HOLDINGS_RELEASES = 1  # Lean disk budget (PLAN_082): ~1 GB per data set
DEFAULT_WINDOW_YEARS = 3

META_COLUMNS = [("source_release_key", "TEXT"), ("loaded_at", "TIMESTAMP")]

FILING_TYPES = {
    "accession_number": "TEXT", "cik": "TEXT", "submission_type": "TEXT", "filing_date": "DATE",
    "period_of_report": "DATE", "report_calendar_or_quarter": "DATE", "is_amendment": "BOOLEAN",
    "amendment_no": "INTEGER", "amendment_type": "TEXT", "conf_denied_expired": "BOOLEAN",
    "date_denied_expired": "DATE", "date_reported": "DATE", "reason_for_non_confidentiality": "TEXT",
    "filing_manager_name": "TEXT", "filing_manager_street1": "TEXT", "filing_manager_street2": "TEXT",
    "filing_manager_city": "TEXT", "filing_manager_state_or_country": "TEXT",
    "filing_manager_zipcode": "TEXT", "report_type": "TEXT", "form13f_file_number": "TEXT",
    "crd_number": "TEXT", "sec_file_number": "TEXT", "provide_info_for_instruction5": "BOOLEAN",
    "additional_information": "TEXT", "other_included_managers_count": "INTEGER",
    "table_entry_total": "INTEGER", "table_value_total": "NUMERIC(20,2)",
    "is_confidential_omitted": "BOOLEAN", "signature_name": "TEXT", "signature_title": "TEXT",
    "signature_phone": "TEXT", "signature": "TEXT", "signature_city": "TEXT",
    "signature_state_or_country": "TEXT", "signature_date": "DATE", "value_multiplier": "INTEGER",
}
HOLDING_TYPES = {
    "accession_number": "TEXT", "infotable_sk": "BIGINT", "name_of_issuer": "TEXT",
    "title_of_class": "TEXT", "cusip": "TEXT", "figi": "TEXT", "value": "NUMERIC(20,2)",
    "ssh_prnamt": "BIGINT", "ssh_prnamt_type": "TEXT", "put_call": "TEXT",
    "investment_discretion": "TEXT", "other_manager": "TEXT", "voting_auth_sole": "BIGINT",
    "voting_auth_shared": "BIGINT", "voting_auth_none": "BIGINT",
}
OTHER_MANAGER_TYPES = {
    "accession_number": "TEXT", "list_type": "TEXT", "sequence_number": "BIGINT", "cik": "TEXT",
    "form13f_file_number": "TEXT", "crd_number": "TEXT", "sec_file_number": "TEXT", "name": "TEXT",
}

FILINGS_TABLE = "public.sec_13f_filings"
HOLDINGS_TABLE = "public.sec_13f_holdings"
OTHER_MANAGERS_TABLE = "public.sec_13f_other_managers"


def _create_table_sql(table: str, columns: Sequence[str], types: Dict[str, str],
                      pk: Sequence[str]) -> str:
    cols = [f"{c} {types[c]}{' NOT NULL' if c in pk else ''}" for c in columns]
    cols += ["source_release_key TEXT", "loaded_at TIMESTAMP DEFAULT NOW()", f"PRIMARY KEY ({', '.join(pk)})"]
    return f"CREATE TABLE IF NOT EXISTS {table} (\n    " + ",\n    ".join(cols) + "\n)"


DDL: List[str] = [
    _create_table_sql(FILINGS_TABLE, FILING_COLUMNS, FILING_TYPES, ["accession_number"]),
    "CREATE INDEX IF NOT EXISTS ix_sec_13f_filings_cik ON public.sec_13f_filings (cik)",
    "CREATE INDEX IF NOT EXISTS ix_sec_13f_filings_period ON public.sec_13f_filings (period_of_report)",
    "CREATE INDEX IF NOT EXISTS ix_sec_13f_filings_crd ON public.sec_13f_filings (crd_number)",
    "CREATE INDEX IF NOT EXISTS ix_sec_13f_filings_release ON public.sec_13f_filings (source_release_key)",
    "COMMENT ON COLUMN public.sec_13f_filings.table_value_total IS "
    "'US dollars. Filings before 2023-01-03 reported thousands; multiplied by value_multiplier.'",
    _create_table_sql(HOLDINGS_TABLE, HOLDING_COLUMNS, HOLDING_TYPES, ["accession_number", "infotable_sk"]),
    "CREATE INDEX IF NOT EXISTS ix_sec_13f_holdings_cusip ON public.sec_13f_holdings (cusip)",
    "CREATE INDEX IF NOT EXISTS ix_sec_13f_holdings_release ON public.sec_13f_holdings (source_release_key)",
    "COMMENT ON COLUMN public.sec_13f_holdings.value IS "
    "'US dollars. Filings before 2023-01-03 reported thousands and are multiplied by 1000.'",
    _create_table_sql(OTHER_MANAGERS_TABLE, OTHER_MANAGER_COLUMNS, OTHER_MANAGER_TYPES,
                      ["accession_number", "list_type", "sequence_number"]),
    "CREATE INDEX IF NOT EXISTS ix_sec_13f_other_managers_cik ON public.sec_13f_other_managers (cik)",
    "CREATE INDEX IF NOT EXISTS ix_sec_13f_other_managers_crd ON public.sec_13f_other_managers (crd_number)",
]


def _env_int(name: str, default: int) -> int:
    try:
        return max(0, int(os.environ.get(name, "")))
    except ValueError:
        return default


def _years_ago(today: date, years: int) -> date:
    try:
        return today.replace(year=today.year - years)
    except ValueError:  # Feb 29
        return today.replace(year=today.year - years, day=28)


def select_releases(datasets: List[DataSet], since: Optional[date] = None, today: Optional[date] = None,
                    holdings_n: Optional[int] = None) -> List[Release]:
    """Window + holdings flag. ``datasets`` must be oldest first (parse_index order).

    The holdings flag goes to the newest N of ALL listed data sets, independent
    of the window, so a wide ``since`` never loads holdings for old quarters.
    """
    if holdings_n is None:
        holdings_n = _env_int("BULK_13F_HOLDINGS_RELEASES", HOLDINGS_RELEASES)
    today = today or date.today()
    cutoff = since if since is not None else _years_ago(today, DEFAULT_WINDOW_YEARS)
    ordered = sorted(datasets, key=lambda d: (d.end_date, d.release_key))
    keep = [d.release_key for d in ordered[-holdings_n:]] if holdings_n > 0 else []
    releases = []
    for d in ordered:
        if d.end_date < cutoff:
            continue
        releases.append(Release(d.release_key, d.url, {
            "start_date": d.start_date,
            "end_date": d.end_date,
            "load_holdings": d.release_key in keep,
            "holdings_keep_keys": list(keep),
        }))
    return releases


def _tuples(rows: Iterable[Dict], columns: Sequence[str], release_key: str, loaded_at: datetime):
    for r in rows:
        yield tuple(r[c] for c in columns) + (release_key, loaded_at)


@register_bulk_source
class Sec13FDataSets(BulkSource):
    """SEC Form 13F data sets (quarterly zips of 13F-HR / 13F-NT filings)."""

    name = "sec_13f"
    parser_version = "1"

    def discover(self, http, since: Optional[date] = None) -> List[Release]:
        html = http.get_text(INDEX_URL)
        datasets = parse_index(html)
        if not datasets:
            raise RuntimeError(f"no *_form13f.zip hrefs found on {INDEX_URL}; index layout drifted")
        return select_releases(datasets, since=since)

    def ddl(self) -> List[str]:
        return list(DDL)

    def _stage_and_merge(self, conn, stg: str, target: str, columns: Sequence[str],
                         types: Dict[str, str], keys: Sequence[str], rows: Iterable[Dict],
                         release_key: str, loaded_at: datetime) -> int:
        all_cols = list(columns) + [c for c, _ in META_COLUMNS]
        create_staging(conn, stg, [(c, types[c]) for c in columns] + META_COLUMNS)
        try:
            copy_rows(conn, stg, all_cols, _tuples(rows, columns, release_key, loaded_at))
            inserted, updated = merge_staging(conn, stg, target, all_cols, keys)
        finally:
            drop_staging(conn, stg)
        return inserted + updated

    def load(self, conn, release: Release, path: Path) -> Dict[str, int]:
        self.ensure_ddl(conn)
        meta = release.meta or {}
        fallback = meta.get("end_date")
        if isinstance(fallback, str):
            fallback = date.fromisoformat(fallback)
        loaded_at = datetime.utcnow()
        key = release.release_key
        out: Dict[str, int] = {}

        with zipfile.ZipFile(path) as zf:
            out["sec_13f_filings"] = self._stage_and_merge(
                conn, "sec_13f_filings", FILINGS_TABLE, FILING_COLUMNS, FILING_TYPES,
                ["accession_number"], iter_filings(zf, fallback), key, loaded_at)
            out["sec_13f_other_managers"] = self._stage_and_merge(
                conn, "sec_13f_other_managers", OTHER_MANAGERS_TABLE, OTHER_MANAGER_COLUMNS,
                OTHER_MANAGER_TYPES, ["accession_number", "list_type", "sequence_number"],
                iter_other_managers(zf), key, loaded_at)

            if meta.get("load_holdings"):
                dates = filing_dates(zf)
                out["sec_13f_holdings"] = self._stage_and_merge(
                    conn, "sec_13f_holdings", HOLDINGS_TABLE, HOLDING_COLUMNS, HOLDING_TYPES,
                    ["accession_number", "infotable_sk"], iter_holdings(zf, dates, fallback), key, loaded_at)
                keep = list(meta.get("holdings_keep_keys") or [])
                if key not in keep:
                    keep.append(key)
                if os.environ.get("BULK_13F_PRUNE_HOLDINGS", "1") != "0":
                    pruned = conn.execute(
                        text("DELETE FROM public.sec_13f_holdings "
                             "WHERE source_release_key IS NULL OR NOT (source_release_key = ANY(:keep))"),
                        {"keep": keep},
                    ).rowcount
                    if pruned:
                        logger.info(f"[bulk:sec_13f] pruned {pruned} holdings rows outside {keep}")
            else:
                logger.info(f"[bulk:sec_13f] {key}: holdings skipped (not in newest data sets)")
        return out
