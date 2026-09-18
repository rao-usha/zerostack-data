"""
SEC XBRL companyfacts bulk source (SPEC_113).

https://www.sec.gov/Archives/edgar/daily-index/xbrl/companyfacts.zip is a
rolling nightly snapshot (~1.2GB) of every filer's companyfacts JSON
(``CIK##########.json``). One release per UTC day: ``snapshot:YYYY-MM-DD``.

load():
- streams zip members one company at a time through the fixed
  ``xbrl_parser.build_financial_statements`` (own-period grouping),
- keeps only periods ending within the last 3 years of the snapshot date,
- COPYs batches into ``stg.sec_companyfacts_<table>`` and merges them into
  sec_income_statement / sec_balance_sheet / sec_cash_flow_statement on the
  SPEC_113 keys.

Memory is bounded by one company's JSON plus one batch of rows.
"""

from __future__ import annotations

import json
import logging
import re
import zipfile
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

from sqlalchemy import Numeric, String, text
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateIndex, CreateTable

from app.core.copy_loader import copy_rows, create_staging, drop_staging, merge_staging
from app.ingest.bulk.base import BulkSource, Release
from app.ingest.bulk.registry import register_bulk_source
from app.sources.sec.ingest_xbrl import STATEMENT_CONFLICT_COLUMNS
from app.sources.sec.models import SECBalanceSheet, SECCashFlowStatement, SECIncomeStatement
from app.sources.sec.xbrl_parser import build_financial_statements, three_year_cutoff

logger = logging.getLogger(__name__)

COMPANYFACTS_URL = "https://www.sec.gov/Archives/edgar/daily-index/xbrl/companyfacts.zip"
MEMBER_RE = re.compile(r"^CIK(\d{10})\.json$", re.IGNORECASE)
STAGING_PREFIX = "sec_companyfacts_"
# Columns never written by the bulk load (ticker isn't in companyfacts; don't clobber it)
SKIP_COLUMNS = {"id", "ticker"}

_PG = postgresql.dialect()


@dataclass
class TableSpec:
    model: Any
    parsed_key: str  # key in build_financial_statements() output

    @property
    def table(self) -> str:
        return self.model.__tablename__

    @property
    def _cols(self):
        return [c for c in self.model.__table__.columns if c.name not in SKIP_COLUMNS]

    @property
    def columns(self) -> List[str]:
        return [c.name for c in self._cols]

    @property
    def column_types(self) -> List[Tuple[str, str]]:
        return [(c.name, c.type.compile(dialect=_PG)) for c in self._cols]

    @property
    def key_columns(self) -> List[str]:
        return STATEMENT_CONFLICT_COLUMNS[self.model]

    @property
    def unique_constraint(self) -> str:
        from sqlalchemy import UniqueConstraint

        return next(c.name for c in self.model.__table__.constraints if isinstance(c, UniqueConstraint))

    def to_tuple(self, row: Dict[str, Any]) -> Tuple:
        """Row dict -> COPY tuple. Values that don't fit the column become NULL
        (bad filer data must not fail the whole release)."""
        out = []
        for col in self._cols:
            v = row.get(col.name)
            if v is not None:
                t = col.type
                if isinstance(t, Numeric) and isinstance(v, Decimal) and t.precision is not None:
                    limit = Decimal(10) ** (t.precision - (t.scale or 0))
                    if not v.is_finite() or abs(v) >= limit:
                        v = None
                elif isinstance(t, String) and t.length is not None and len(str(v)) > t.length:
                    v = None
            out.append(v)
        return tuple(out)


TABLES: Dict[str, TableSpec] = {
    spec.table: spec
    for spec in (
        TableSpec(SECIncomeStatement, "income_statement"),
        TableSpec(SECBalanceSheet, "balance_sheet"),
        TableSpec(SECCashFlowStatement, "cash_flow"),
    )
}


def snapshot_date(release: Release) -> date:
    meta = (release.meta or {}).get("snapshot_date")
    if meta:
        return meta if isinstance(meta, date) else date.fromisoformat(str(meta))
    key = release.release_key
    if key.startswith("snapshot:"):
        return date.fromisoformat(key.split(":", 1)[1])
    return datetime.now(timezone.utc).date()


@register_bulk_source
class SecCompanyFactsSource(BulkSource):
    name = "sec_companyfacts"
    parser_version = "1"

    def __init__(self, batch_companies: int = 500, batch_rows: int = 200_000):
        self.batch_companies = max(1, batch_companies)
        self.batch_rows = max(1, batch_rows)

    def discover(self, http, since: Optional[date] = None) -> List[Release]:
        today = datetime.now(timezone.utc).date()
        return [Release(f"snapshot:{today.isoformat()}", COMPANYFACTS_URL, {"snapshot_date": today.isoformat()})]

    def ddl(self) -> List[str]:
        stmts: List[str] = []
        for spec in TABLES.values():
            table = spec.model.__table__
            stmts.append(str(CreateTable(table, if_not_exists=True).compile(dialect=_PG)))
            for idx in table.indexes:
                stmts.append(str(CreateIndex(idx, if_not_exists=True).compile(dialect=_PG)))
        return stmts

    def _check_keys(self, conn) -> None:
        wanted = {spec.unique_constraint: spec.table for spec in TABLES.values()}
        found = set(
            conn.execute(
                text("SELECT conname FROM pg_constraint WHERE contype = 'u' AND conname = ANY(:names)"),
                {"names": list(wanted)},
            ).scalars()
        )
        missing = sorted(set(wanted) - found)
        if missing:
            raise RuntimeError(
                f"SEC statement tables lack the SPEC_113 period keys {missing}; "
                "run alembic migration 0006_xbrl_period_keys first"
            )

    def _flush(self, conn, buffers: Dict[str, List[Tuple]], totals: Dict[str, int]) -> None:
        for table, rows in buffers.items():
            if not rows:
                continue
            spec = TABLES[table]
            stg = STAGING_PREFIX + table
            create_staging(conn, stg, spec.column_types)
            copy_rows(conn, stg, spec.columns, rows)
            inserted, updated = merge_staging(conn, stg, f"public.{table}", spec.columns, spec.key_columns)
            totals[table] += inserted + updated
            rows.clear()

    def load(self, conn, release: Release, path: Path) -> Dict[str, int]:
        self.ensure_ddl(conn)
        self._check_keys(conn)
        cutoff = three_year_cutoff(snapshot_date(release))
        totals = {t: 0 for t in TABLES}
        buffers: Dict[str, List[Tuple]] = {t: [] for t in TABLES}
        companies = skipped = pending = 0

        with zipfile.ZipFile(path) as zf:
            for info in zf.infolist():
                m = MEMBER_RE.match(Path(info.filename).name)
                if not m or info.is_dir():
                    continue
                cik = m.group(1)
                try:
                    with zf.open(info) as fh:
                        data = json.load(fh)
                    parsed = build_financial_statements(data, cik, min_period_end=cutoff)
                except Exception as e:  # one bad company must not fail the snapshot
                    skipped += 1
                    logger.warning(f"[bulk:{self.name}] skipping {info.filename}: {type(e).__name__}: {e}")
                    continue
                finally:
                    data = None
                for table, spec in TABLES.items():
                    buffers[table].extend(spec.to_tuple(r) for r in parsed[spec.parsed_key])
                parsed = None
                companies += 1
                pending += 1
                if pending >= self.batch_companies or sum(map(len, buffers.values())) >= self.batch_rows:
                    self._flush(conn, buffers, totals)
                    pending = 0
                if companies % 2000 == 0:
                    logger.info(f"[bulk:{self.name}] {companies} companies processed: {totals}")

        self._flush(conn, buffers, totals)
        for table in TABLES:
            drop_staging(conn, STAGING_PREFIX + table)
        logger.info(
            f"[bulk:{self.name}] {release.release_key}: {companies} companies, {skipped} skipped, "
            f"cutoff {cutoff}: {totals}"
        )
        return totals


__all__: Sequence[str] = ["SecCompanyFactsSource", "COMPANYFACTS_URL", "TABLES", "snapshot_date"]
