"""
BulkSource contract and runner (SPEC_107).

A bulk source publishes files (quarterly ZIPs, a nightly archive, ...). Each
file is a *release*. ``run_source`` walks every release through
``raw.source_release``:

    discovered -> fetched -> loaded
                        \\-> failed   (retried on the next run)

- ``discover`` is the only step that reads index pages.
- ``fetch`` streams the file to ``<raw_root>/<source>/<release_key>/``; a file
  already on disk whose sha256 matches the manifest is reused (no re-download).
- ``load`` stages (COPY) and merges inside ONE transaction per release.
"""

from __future__ import annotations

import json
import logging
import re
import traceback
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional

from sqlalchemy import text

from app.core.sec_http import FetchedFile, SecHttp, sha256_file

logger = logging.getLogger(__name__)


@dataclass
class Release:
    release_key: str
    url: str
    meta: Dict[str, Any] = field(default_factory=dict)


class BulkSource(ABC):
    """One publisher bulk dataset (e.g. SEC Form D quarterly data sets)."""

    name: str = ""
    parser_version: str = "1"

    @abstractmethod
    def discover(self, http: SecHttp, since: Optional[date] = None) -> List[Release]:
        """Releases available from the publisher, oldest first, filtered to ``since``."""

    def fetch(self, http: SecHttp, release: Release, dest_dir: Path) -> Path:
        """Download the release file. Default: stream ``release.url`` into dest_dir."""
        filename = release.url.rstrip("/").rsplit("/", 1)[-1] or f"{release.release_key}.bin"
        result: FetchedFile = http.stream_to_file(release.url, Path(dest_dir) / filename)
        self._last_fetch = result
        return result.path

    @abstractmethod
    def load(self, conn, release: Release, path: Path) -> Dict[str, int]:
        """Stage + merge the file. Returns rows loaded per target table."""

    def ddl(self) -> List[str]:
        """CREATE TABLE IF NOT EXISTS ... statements for this source's target tables."""
        return []

    def ensure_ddl(self, conn) -> None:
        """Apply ddl() (idempotent). Call at the start of load()."""
        for stmt in self.ddl():
            conn.execute(text(stmt))


def _parse_since(since) -> Optional[date]:
    if since is None or isinstance(since, date):
        return since
    return datetime.strptime(str(since), "%Y-%m-%d").date()


def _upsert_discovered(conn, source: BulkSource, releases: Iterable[Release]) -> None:
    for rel in releases:
        conn.execute(
            text(
                """
                INSERT INTO raw.source_release (source, release_key, url, status, parser_version)
                VALUES (:source, :key, :url, 'discovered', :pv)
                ON CONFLICT (source, release_key) DO UPDATE SET url = EXCLUDED.url, updated_at = NOW()
                """
            ),
            {"source": source.name, "key": rel.release_key, "url": rel.url, "pv": source.parser_version},
        )


def _get_state(conn, source: str, key: str) -> Dict[str, Any]:
    row = conn.execute(
        text(
            "SELECT status, local_path, sha256 FROM raw.source_release "
            "WHERE source = :s AND release_key = :k"
        ),
        {"s": source, "k": key},
    ).mappings().one()
    return dict(row)


def _set(engine, source: str, key: str, **fields) -> None:
    if not fields:
        return
    fields["updated_at"] = datetime.utcnow()
    allowed = {
        "status", "local_path", "bytes", "sha256", "etag", "last_modified", "rows_loaded",
        "parser_version", "error", "fetched_at", "loaded_at", "updated_at",
    }
    assert set(fields) <= allowed, set(fields) - allowed
    sets = ", ".join(
        f"{k} = CAST(:{k} AS JSONB)" if k == "rows_loaded" else f"{k} = :{k}" for k in fields
    )
    params = dict(fields)
    if "rows_loaded" in params and not isinstance(params["rows_loaded"], str):
        params["rows_loaded"] = json.dumps(params["rows_loaded"])
    params.update({"s": source, "k": key})
    with engine.begin() as conn:
        conn.execute(
            text(f"UPDATE raw.source_release SET {sets} WHERE source = :s AND release_key = :k"),
            params,
        )


def run_source(
    source: BulkSource,
    engine=None,
    http: Optional[SecHttp] = None,
    raw_root: Optional[Path | str] = None,
    since=None,
    max_releases: Optional[int] = None,
    release_keys: Optional[List[str]] = None,
    progress: Optional[Callable[[str, float], None]] = None,
) -> Dict[str, Any]:
    """Discover, fetch and load releases for ``source``. Never raises for per-release errors."""
    from app.core.config import get_settings

    if engine is None:
        from app.core.database import get_engine

        engine = get_engine()
    raw_root = Path(raw_root or get_settings().bulk_raw_dir)
    owns_http = http is None
    if http is None:
        from app.core.database import get_session_factory

        http = SecHttp(session_factory=get_session_factory())

    summary: Dict[str, Any] = {"source": source.name, "loaded": 0, "skipped": 0, "failed": 0,
                               "rows": 0, "errors": [], "releases": []}
    try:
        releases = source.discover(http, _parse_since(since))
        if release_keys:
            wanted = set(release_keys)
            releases = [r for r in releases if r.release_key in wanted]
        with engine.begin() as conn:
            _upsert_discovered(conn, source, releases)

        todo = []
        for rel in releases:
            with engine.connect() as conn:
                state = _get_state(conn, source.name, rel.release_key)
            if state["status"] == "loaded":
                summary["skipped"] += 1
                continue
            todo.append((rel, state))
        if max_releases is not None:
            todo = todo[:max_releases]

        for i, (rel, state) in enumerate(todo):
            if progress:
                progress(f"{source.name} {rel.release_key} ({i + 1}/{len(todo)})", 100.0 * i / max(1, len(todo)))
            try:
                path = _ensure_file(source, http, rel, state, raw_root, engine)
                with engine.begin() as conn:
                    rows = source.load(conn, rel, path)
                total = int(sum(rows.values())) if rows else 0
                _set(engine, source.name, rel.release_key, status="loaded", rows_loaded=rows or {},
                     loaded_at=datetime.utcnow(), error=None, parser_version=source.parser_version)
                summary["loaded"] += 1
                summary["rows"] += total
                summary["releases"].append({"release": rel.release_key, "rows": rows})
                logger.info(f"[bulk:{source.name}] loaded {rel.release_key}: {rows}")
            except Exception as e:  # per-release failure: record and continue
                msg = f"{type(e).__name__}: {e}"
                logger.error(f"[bulk:{source.name}] {rel.release_key} failed: {msg}\n{traceback.format_exc()}")
                _set(engine, source.name, rel.release_key, status="failed", error=msg[:4000])
                summary["failed"] += 1
                summary["errors"].append(f"{rel.release_key}: {msg[:300]}")
        return summary
    finally:
        if owns_http:
            http.close()


def _ensure_file(source: BulkSource, http: SecHttp, rel: Release, state: Dict[str, Any],
                 raw_root: Path, engine) -> Path:
    """Reuse the downloaded file when its sha256 matches the manifest; otherwise fetch."""
    local = state.get("local_path")
    if local and state.get("sha256") and Path(local).exists():
        if sha256_file(local) == state["sha256"]:
            return Path(local)
        logger.warning(f"[bulk:{source.name}] {rel.release_key}: sha256 mismatch, re-downloading")

    # release keys may contain ':' etc.; raw_root is often a Windows bind mount
    safe_key = re.sub(r"[^A-Za-z0-9._-]+", "_", rel.release_key)
    dest_dir = raw_root / source.name / safe_key
    dest_dir.mkdir(parents=True, exist_ok=True)
    source._last_fetch = None
    path = Path(source.fetch(http, rel, dest_dir))
    fetched = getattr(source, "_last_fetch", None)
    _set(
        engine, source.name, rel.release_key,
        status="fetched",
        local_path=str(path),
        bytes=path.stat().st_size,
        sha256=fetched.sha256 if fetched else sha256_file(path),
        etag=fetched.etag if fetched else None,
        last_modified=fetched.last_modified if fetched else None,
        fetched_at=datetime.utcnow(),
        error=None,
    )
    return path
