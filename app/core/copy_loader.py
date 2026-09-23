"""
COPY-based staging + set-based merge (SPEC_107).

Replaces row-by-row inserts for bulk loads:

    create_staging(conn, "form_d_issuers", [("accession_number", "TEXT"), ...])
    copy_rows(conn, "form_d_issuers", columns, row_iter)       # psycopg2 COPY FROM STDIN
    merge_staging(conn, "form_d_issuers", "public.form_d_filings", columns, ["accession_number"])
    drop_staging(conn, "form_d_issuers")

Staging tables are UNLOGGED in schema ``stg`` and carry ``_row_num`` so the
merge keeps the LAST row per key within a batch.
"""

from __future__ import annotations

import contextlib
import contextvars
import io
import logging
import os
from typing import Iterable, Iterator, List, Optional, Sequence, Tuple

from sqlalchemy import text
from sqlalchemy.engine import Connection

from app.core.safe_sql import qi

logger = logging.getLogger(__name__)

STAGING_SCHEMA = "stg"
_ALLOWED_TYPE_CHARS = set("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789 (),_[]")
_CSV_SPECIAL = (",", '"', "\n", "\r")


def _qualified(name: str) -> str:
    """'schema.table' or 'table' -> quoted identifier(s)."""
    parts = name.split(".")
    if len(parts) > 2:
        raise ValueError(f"Invalid table name: {name!r}")
    return ".".join(qi(p) for p in parts)


def _stg(name: str) -> str:
    return f"{qi(STAGING_SCHEMA)}.{qi(name)}"


def _check_type(pg_type: str) -> str:
    if not pg_type or not set(pg_type) <= _ALLOWED_TYPE_CHARS:
        raise ValueError(f"Invalid column type: {pg_type!r}")
    return pg_type


def create_staging(conn: Connection, name: str, columns: Sequence[Tuple[str, str]]) -> None:
    """Create (or recreate) UNLOGGED stg.<name> with the given (column, type) pairs."""
    cols = ", ".join(f"{qi(c)} {_check_type(t)}" for c, t in columns)
    conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {qi(STAGING_SCHEMA)}"))
    conn.execute(text(f"DROP TABLE IF EXISTS {_stg(name)}"))
    conn.execute(text(f"CREATE UNLOGGED TABLE {_stg(name)} (_row_num BIGSERIAL, {cols})"))


def drop_staging(conn: Connection, name: str) -> None:
    conn.execute(text(f"DROP TABLE IF EXISTS {_stg(name)}"))


def _csv_field(value) -> str:
    """CSV field for COPY ... (FORMAT csv, NULL ''): None -> NULL, '' -> quoted empty string."""
    if value is None:
        return ""
    s = value if isinstance(value, str) else str(value)
    if s == "" or any(ch in s for ch in _CSV_SPECIAL) or s[0].isspace() or s[-1].isspace():
        return '"' + s.replace('"', '""') + '"'
    return s


class _CsvStream(io.RawIOBase):
    """File-like object producing CSV bytes from a row iterator (for COPY FROM STDIN)."""

    def __init__(self, rows: Iterable[Sequence]):
        self._rows: Iterator[Sequence] = iter(rows)
        self._buf = b""
        self.count = 0

    def readable(self) -> bool:
        return True

    def readinto(self, b) -> int:
        while len(self._buf) < len(b):
            try:
                row = next(self._rows)
            except StopIteration:
                break
            line = ",".join(_csv_field(v) for v in row) + "\n"
            self._buf += line.encode("utf-8")
            self.count += 1
        n = min(len(b), len(self._buf))
        b[:n] = self._buf[:n]
        self._buf = self._buf[n:]
        return n


def copy_rows(conn: Connection, name: str, columns: Sequence[str], rows: Iterable[Sequence]) -> int:
    """Stream rows into stg.<name> via COPY. Returns the number of rows copied."""
    stream = _CsvStream(rows)
    col_sql = ", ".join(qi(c) for c in columns)
    sql = f"COPY {_stg(name)} ({col_sql}) FROM STDIN WITH (FORMAT csv, NULL '')"
    dbapi_conn = conn.connection.dbapi_connection
    with dbapi_conn.cursor() as cur:
        cur.copy_expert(sql, io.BufferedReader(stream, buffer_size=1024 * 1024))
    return stream.count


# Bookkeeping columns: rewriting them alone is not a real change
NON_COMPARED_COLUMNS = ("loaded_at", "ingested_at", "updated_at", "source_release_key")


def build_merge_sql(
    stg_name: str,
    target: str,
    columns: Sequence[str],
    key_columns: Sequence[str],
    update_columns: Optional[Sequence[str]] = None,
    skip_unchanged: bool = True,
    compare_columns: Optional[Sequence[str]] = None,
    conflict_where: Optional[str] = None,
) -> str:
    cols = ", ".join(qi(c) for c in columns)
    keys = ", ".join(qi(c) for c in key_columns)
    # a partial unique index only matches ON CONFLICT when its predicate is repeated
    conflict_target = f"({keys}) WHERE {conflict_where}" if conflict_where else f"({keys})"
    if update_columns is None:
        update_columns = [c for c in columns if c not in key_columns]
    if update_columns:
        sets = ", ".join(f"{qi(c)} = EXCLUDED.{qi(c)}" for c in update_columns)
        conflict = f"ON CONFLICT {conflict_target} DO UPDATE SET {sets}"
        if skip_unchanged:
            compared = (
                list(compare_columns)
                if compare_columns is not None
                else [c for c in update_columns if c not in NON_COMPARED_COLUMNS] or list(update_columns)
            )
            alias = qi(target.split(".")[-1])
            current = ", ".join(f"{alias}.{qi(c)}" for c in compared)
            incoming = ", ".join(f"EXCLUDED.{qi(c)}" for c in compared)
            # Leave identical rows completely untouched: no dead tuples, no disk churn
            conflict += f" WHERE ({current}) IS DISTINCT FROM ({incoming})"
    else:
        conflict = f"ON CONFLICT {conflict_target} DO NOTHING"
    return f"""
        WITH src AS (
            SELECT DISTINCT ON ({keys}) {cols}
            FROM {_stg(stg_name)}
            ORDER BY {keys}, _row_num DESC
        ),
        upserted AS (
            INSERT INTO {_qualified(target)} ({cols})
            SELECT {cols} FROM src
            {conflict}
            RETURNING (xmax = 0) AS inserted
        )
        SELECT COUNT(*) FILTER (WHERE inserted), COUNT(*) FILTER (WHERE NOT inserted)
        FROM upserted
    """


def merge_staging(
    conn: Connection,
    stg_name: str,
    target: str,
    columns: Sequence[str],
    key_columns: Sequence[str],
    update_columns: Optional[Sequence[str]] = None,
    skip_unchanged: bool = True,
    compare_columns: Optional[Sequence[str]] = None,
    conflict_where: Optional[str] = None,
) -> Tuple[int, int]:
    """Upsert stg.<stg_name> into target. Returns (inserted, updated).

    Rows whose non-key columns already match are skipped, so ``updated`` counts
    real changes only.
    """
    row = conn.execute(
        text(
            build_merge_sql(
                stg_name, target, columns, key_columns, update_columns, skip_unchanged,
                compare_columns, conflict_where
            )
        )
    ).one()
    return int(row[0] or 0), int(row[1] or 0)


def staging_count(conn: Connection, name: str) -> int:
    return conn.execute(text(f"SELECT COUNT(*) FROM {_stg(name)}")).scalar() or 0


# ---------------------------------------------------------------------------
# Publish guard (SPEC_129)
# ---------------------------------------------------------------------------

PUBLISH_GUARD_OVERRIDE_ENV = "BULK_PUBLISH_GUARD_OVERRIDE"
DEFAULT_MAX_DROP = 0.5


class PublishGuardError(RuntimeError):
    """A destructive replace would publish an empty or much smaller table."""


# Per-job override: a job payload's ``publish_guard_override``, set by the
# worker executors around one run. contextvars are copied into
# asyncio.to_thread, so the value reaches that job's loader thread and no
# other job running on the same worker.
_job_override: contextvars.ContextVar[Tuple[str, ...]] = contextvars.ContextVar(
    "publish_guard_override", default=()
)
_ALL = ("1", "true", "yes", "all", "*")


def _normalize_override(value) -> Tuple[str, ...]:
    if value is None or value is False:
        return ()
    if value is True:
        return ("*",)
    if isinstance(value, str):
        value = value.split(",")
    return tuple(str(v).strip().lower() for v in value if str(v).strip())


@contextlib.contextmanager
def publish_guard_override(value):
    """Accept guard trips for the named tables (or ``True``/``"all"``) inside the block.

    The bulk_ingest and pe_mart_build executors wrap one job in this with the
    payload's ``publish_guard_override``, so an operator can accept one
    legitimately smaller release by re-queuing a job, without changing any
    container's environment.
    """
    token = _job_override.set(_normalize_override(value))
    try:
        yield
    finally:
        _job_override.reset(token)


def _matches(wanted: Sequence[str], target: str) -> bool:
    if any(w in _ALL for w in wanted):
        return True
    short = target.lower().split(".")[-1]
    return short in wanted or target.lower() in wanted


def _guard_overridden(target: str) -> bool:
    job = _job_override.get()
    if job and _matches(job, target):
        return True
    env = _normalize_override(os.environ.get(PUBLISH_GUARD_OVERRIDE_ENV, ""))
    return bool(env) and _matches(env, target)


def check_publish(
    target: str,
    new_rows: int,
    current_rows: int,
    max_drop: float = DEFAULT_MAX_DROP,
    override: Optional[bool] = None,
    allow_empty: bool = False,
) -> None:
    """Refuse to publish a replacement that is empty or shrinks ``target`` too far.

    Call this BEFORE deleting published rows (a prune, or a delete of rows the
    new batch no longer carries), inside the same transaction, so raising rolls
    the whole load back and the bulk runner marks the release ``failed``.

    - ``new_rows``: rows that will be published once the replace completes.
    - ``current_rows``: rows published right now (before this load).
    - Raises when ``new_rows == 0``, or when ``current_rows > 0`` and
      ``new_rows < current_rows * (1 - max_drop)``.

    - ``allow_empty``: an empty replacement of an EMPTY target is a no-op
      rather than an error. Use it for derived marts whose sources can
      legitimately be empty (a fresh database before the first Schedule D
      load): a delete on an empty target destroys nothing. Raw loads such as
      13F holdings keep the default, because an empty release is never real.

    ``override`` defaults to the job payload's ``publish_guard_override`` (see
    :func:`publish_guard_override`), then env ``BULK_PUBLISH_GUARD_OVERRIDE``:
    ``1``/``all`` for every target, or a comma list of table names
    (``sec_13f_holdings``). It is an operator decision, never a code default.
    """
    if override is None:
        override = _guard_overridden(target)
    new_rows, current_rows = int(new_rows or 0), int(current_rows or 0)
    if allow_empty and new_rows == 0 and current_rows == 0:
        return
    problem = None
    if new_rows == 0:
        problem = f"replacement for {target} has 0 rows (currently {current_rows})"
    elif current_rows > 0 and new_rows < current_rows * (1 - max_drop):
        drop = 1 - new_rows / current_rows
        problem = (
            f"replacement for {target} would drop from {current_rows} to {new_rows} rows "
            f"({drop:.0%} > {max_drop:.0%} tolerance)"
        )
    if problem is None:
        return
    if override:
        logger.warning(f"[publish-guard] OVERRIDDEN: {problem}")
        return
    raise PublishGuardError(
        f"{problem}. Refusing to publish; to accept it, re-queue the job with payload "
        f"publish_guard_override=[\"{target.split('.')[-1]}\"]."
    )


def table_count(conn: Connection, table: str, where: str = "", params: Optional[dict] = None) -> int:
    """COUNT(*) of a (possibly schema-qualified) table; 0 when it does not exist."""
    if not conn.execute(text("SELECT to_regclass(:t)"), {"t": table}).scalar():
        return 0
    sql = f"SELECT COUNT(*) FROM {_qualified(table)}"
    if where:
        sql += f" WHERE {where}"
    return int(conn.execute(text(sql), params or {}).scalar() or 0)


__all__: List[str] = [
    "PublishGuardError",
    "check_publish",
    "publish_guard_override",
    "table_count",
    "create_staging",
    "copy_rows",
    "merge_staging",
    "build_merge_sql",
    "drop_staging",
    "staging_count",
]
