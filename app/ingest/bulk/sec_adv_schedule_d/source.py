"""
``sec_adv_schedule_d``: Form ADV Schedule D 7.B.(1) private funds (SPEC_118).

One release per monthly ``ADV_Filing_Data_YYYYMMDD_YYYYMMDD.zip`` listed in
the adviserinfo FOIA manifest. Each zip holds only that month's filings, so
coverage comes from loading the whole window; the tables are at filing grain
so the releases may load in any order.

The release key carries ``uploadedOn`` because the SEC restates months in
place (all of 2025 was re-uploaded on 2026-05-04) -- a re-upload of an
already-loaded month has to look like new work, not a duplicate.

Filings load before funds, and a fund row whose FilingID is not in
``sec_adv_filings`` aborts the release: the CRD lives only on the filing, so
such a fund could never be attributed to an adviser.
"""

from __future__ import annotations

import json
import logging
import re
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Set, Tuple

from sqlalchemy import text

from app.core.copy_loader import copy_rows, create_staging, drop_staging, merge_staging
from app.core.safe_sql import qi
from app.ingest.bulk.base import BulkSource, Release
from app.ingest.bulk.registry import register_bulk_source
from app.ingest.bulk.sec_adv_schedule_d.parse import (
    DOWNLOAD_BASE,
    FILING_COLUMNS,
    FUND_COLUMNS,
    MANIFEST_URL,
    iter_filing_rows,
    iter_fund_rows,
)

logger = logging.getLogger(__name__)

TARGET_FILINGS = "public.sec_adv_filings"
TARGET_FUNDS = "public.sec_adv_private_fund_filings"
FILING_KEYS = ["filing_id"]
FUND_KEYS = ["filing_id", "private_fund_id"]
# stg is one shared schema and create_staging drops first, so the names are
# prefixed rather than suffixed per release.
STG_FILINGS = "sec_adv_sd_filings"
STG_FUNDS = "sec_adv_sd_funds"

FILENAME_RE = re.compile(r"(\d{8})_(\d{8})\.zip$", re.I)


def _create_table_sql(table: str, columns: Sequence[Tuple[str, str]], keys: Sequence[str],
                      not_null: Sequence[str]) -> str:
    cols = [f"{qi(c)} {t}" + (" NOT NULL" if c in not_null else "") for c, t in columns]
    cols.append("loaded_at TIMESTAMP DEFAULT NOW()")
    return (f"CREATE TABLE IF NOT EXISTS {table} (\n    " + ",\n    ".join(cols)
            + f",\n    PRIMARY KEY ({', '.join(qi(k) for k in keys)})\n)")


def _compact(value) -> str:
    """'2026-05-04' / '05/04/2026 09:00' -> digits+letters only, for the release key."""
    return re.sub(r"[^0-9A-Za-z]", "", str(value or ""))


def _period(file_name: str) -> Optional[Tuple[date, date]]:
    m = FILENAME_RE.search(file_name)
    if not m:
        return None
    try:
        return (datetime.strptime(m.group(1), "%Y%m%d").date(),
                datetime.strptime(m.group(2), "%Y%m%d").date())
    except ValueError:
        return None


@register_bulk_source
class SecAdvScheduleD(BulkSource):
    name = "sec_adv_schedule_d"
    parser_version = "1"

    def discover(self, http, since: Optional[date] = None) -> List[Release]:
        raw = http.get_text(MANIFEST_URL)
        try:
            manifest = json.loads(raw)
        except ValueError as e:
            raise RuntimeError(f"ADV FOIA manifest is not JSON: {e}") from e
        by_year = manifest.get("advFilingData") if isinstance(manifest, dict) else None
        if not isinstance(by_year, dict) or not by_year:
            keys = sorted(manifest) if isinstance(manifest, dict) else type(manifest).__name__
            raise RuntimeError(
                f"ADV FOIA manifest has no advFilingData entries: expected "
                f"{{'advFilingData': {{'<year>': {{'files': [...]}}}}}}, got {keys}")

        releases: List[Release] = []
        unparsed: List[str] = []
        parsed = 0
        for year, entry in by_year.items():
            files = entry.get("files") if isinstance(entry, dict) else None
            for f in files or []:
                file_name = f.get("fileName") if isinstance(f, dict) else None
                period = _period(file_name or "")
                if period is None:
                    unparsed.append(str(file_name))
                    continue
                start, end = period
                parsed += 1
                if since is not None and start < since:
                    continue
                uploaded = f.get("uploadedOn")
                releases.append(Release(
                    release_key=f"adv1:{start:%Y-%m}:{_compact(uploaded)}",
                    url=f"{DOWNLOAD_BASE}{f.get('year', year)}/{file_name}",
                    meta={"period_start": start.isoformat(), "period_end": end.isoformat(),
                          "year": str(f.get("year", year)), "file_name": file_name,
                          "uploaded_on": uploaded},
                ))
        if unparsed:
            logger.info(f"[bulk:{self.name}] skipped {len(unparsed)} manifest files without a "
                        f"YYYYMMDD_YYYYMMDD.zip name: {unparsed[:5]}")
        if not parsed:
            raise RuntimeError(
                f"ADV FOIA manifest listed no ADV_Filing_Data_*.zip files; saw {unparsed[:10]}")
        releases.sort(key=lambda r: (r.meta["period_start"], r.release_key))
        return releases

    def ddl(self) -> List[str]:
        return [
            _create_table_sql(TARGET_FILINGS, FILING_COLUMNS, FILING_KEYS,
                              ("crd_number", "adviser_type")),
            "CREATE INDEX IF NOT EXISTS ix_sec_adv_filings_crd_filed "
            "ON public.sec_adv_filings (crd_number, filed_at DESC)",
            _create_table_sql(TARGET_FUNDS, FUND_COLUMNS, FUND_KEYS,
                              ("filing_id", "private_fund_id", "adviser_type")),
            "CREATE INDEX IF NOT EXISTS ix_sec_adv_pff_fund_id "
            "ON public.sec_adv_private_fund_filings (private_fund_id)",
            "CREATE INDEX IF NOT EXISTS ix_sec_adv_pff_name_core "
            "ON public.sec_adv_private_fund_filings (fund_name_core) WHERE fund_name_core IS NOT NULL",
        ]

    def load(self, conn, release: Release, path: Path) -> Dict[str, int]:
        self.ensure_ddl(conn)
        key = release.release_key

        filing_names = [c for c, _ in FILING_COLUMNS]
        create_staging(conn, STG_FILINGS, FILING_COLUMNS)
        refused_filings: Set[str] = set()
        copied_filings = copy_rows(
            conn, STG_FILINGS, filing_names, iter_filing_rows(path, key, refused_filings)
        )
        if copied_filings == 0:
            raise RuntimeError(f"{key}: ADV base parsed 0 filings from {Path(path).name}")
        self._drop_superseded(conn, STG_FILINGS, TARGET_FILINGS, FILING_KEYS, key)
        ins_f, upd_f = merge_staging(conn, STG_FILINGS, TARGET_FILINGS, filing_names, FILING_KEYS)

        fund_names = [c for c, _ in FUND_COLUMNS]
        create_staging(conn, STG_FUNDS, FUND_COLUMNS)
        copied_funds = copy_rows(conn, STG_FUNDS, fund_names, iter_fund_rows(path, key))
        if copied_funds == 0:
            raise RuntimeError(f"{key}: Schedule D 7B1 parsed 0 funds from {Path(path).name}")

        # Funds of a refused filing carry no CRD, so they are dropped here and
        # counted. Anything still unattributable afterwards is a filing the
        # file never contained, which is a real inconsistency worth failing on.
        dropped_funds = 0
        if refused_filings:
            dropped_funds = conn.execute(
                text(f"DELETE FROM stg.{qi(STG_FUNDS)} WHERE filing_id = ANY(:ids)"),
                {"ids": sorted(refused_filings)},
            ).rowcount or 0
            logger.warning(
                f"[bulk:{self.name}] {key}: dropped {dropped_funds} fund rows from "
                f"{len(refused_filings)} filings refused for a missing/non-numeric CRD"
            )
        self._assert_attributable(conn, key)
        self._drop_superseded(conn, STG_FUNDS, TARGET_FUNDS, FUND_KEYS, key)
        ins_p, upd_p = merge_staging(conn, STG_FUNDS, TARGET_FUNDS, fund_names, FUND_KEYS)

        drop_staging(conn, STG_FILINGS)
        drop_staging(conn, STG_FUNDS)
        logger.info(f"[bulk:{self.name}] {key}: filings copied={copied_filings} "
                    f"inserted={ins_f} updated={upd_f}; funds copied={copied_funds} "
                    f"inserted={ins_p} updated={upd_p}; refused_filings={len(refused_filings)} "
                    f"dropped_funds={dropped_funds}")
        return {
            "sec_adv_filings": ins_f + upd_f,
            "sec_adv_private_fund_filings": ins_p + upd_p,
            "refused_filings": len(refused_filings),
            "dropped_fund_rows": dropped_funds,
        }

    @staticmethod
    def _drop_superseded(conn, staging: str, target: str, keys: Sequence[str], key: str) -> int:
        """Never let an older release overwrite rows a newer one already wrote.

        The SEC restates months (all of 2025 was re-uploaded on 2026-05-04), and
        `merge_staging` overwrites unconditionally, so without this a backfill
        run after a restatement would reinstate the superseded values — the
        result would depend on load order. Release keys sort by period, so the
        comparison is a plain string compare.
        """
        on = " AND ".join(f"t.{qi(k)} = s.{qi(k)}" for k in keys)
        dropped = conn.execute(
            text(
                f"DELETE FROM stg.{qi(staging)} s USING {target} t "
                f"WHERE {on} AND t.source_release_key > :key"
            ),
            {"key": key},
        ).rowcount or 0
        if dropped:
            logger.info(
                f"[bulk:sec_adv_schedule_d] {key}: skipped {dropped} {target} rows already "
                "held by a newer release"
            )
        return dropped

    @staticmethod
    def _assert_attributable(conn, key: str) -> None:
        """A fund whose filing is unknown has no CRD anywhere: abort, never write it."""
        rows = conn.execute(
            text(f"SELECT DISTINCT s.filing_id FROM stg.{qi(STG_FUNDS)} s "
                 f"WHERE NOT EXISTS (SELECT 1 FROM {TARGET_FILINGS} f WHERE f.filing_id = s.filing_id) "
                 "ORDER BY s.filing_id LIMIT 11")
        ).scalars().all()
        if rows:
            raise RuntimeError(
                f"{key}: {len(rows)}{'+' if len(rows) > 10 else ''} fund rows reference filing_ids "
                f"absent from {TARGET_FILINGS} (no CRD is derivable for them): {rows[:10]}")
