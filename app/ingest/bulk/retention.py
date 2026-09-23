"""
Raw-file retention for bulk sources (SPEC_122).

Files live at ``<raw_root>/<source>/<safe release key>/<file>`` and are
referenced by ``raw.source_release.local_path``. ``apply_retention`` keeps the
newest ``keep`` *loaded* files per source and deletes older loaded ones.

Never deleted:
- the file of a release that is not ``loaded`` (``discovered``/``fetched``/
  ``staged``/``failed`` -- a later run resumes from it), unless the release is
  marked superseded;
- the newest loaded file (``keep`` is clamped to >= 1);
- anything outside ``<raw_root>/<source>/``;
- files no release row references (reported as ``orphans``; one may be an
  in-flight ``.part`` download).

Superseded: a snapshot source (``BulkSource.snapshot``) only ever discovers
today's key, so an older non-loaded snapshot will never be loaded -- merging it
after a newer one would regress data. ``supersede_stale`` gives such rows an
honest terminal state: ``status='failed'`` with ``error`` starting
``SUPERSEDED_PREFIX`` (the status CHECK constraint has no ``superseded``).

One-time cleanup (dry-run unless ``--apply``)::

    python -m app.ingest.bulk.retention [--source NAME ...] [--keep N] [--apply]
"""

from __future__ import annotations

import argparse
import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from sqlalchemy import text

from app.ingest.bulk.registry import get_source, list_sources

logger = logging.getLogger(__name__)

SUPERSEDED_PREFIX = "superseded:"


def _is_superseded(row: Dict[str, Any]) -> bool:
    return row["status"] != "loaded" and (row.get("error") or "").startswith(SUPERSEDED_PREFIX)


def _inside(path: Path, root: Path) -> bool:
    try:
        path.resolve().relative_to(root.resolve())
        return True
    except (ValueError, OSError):
        return False


def _size(path: Path) -> int:
    try:
        return path.stat().st_size
    except OSError:
        return 0


def supersede_stale(engine, source: str, current_key: str, dry_run: bool = False) -> List[str]:
    """Mark non-loaded releases older than ``current_key`` superseded. Returns their keys.

    Only for snapshot sources, whose keys sort by date (``snapshot:YYYY-MM-DD``).
    """
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT release_key, status, error FROM raw.source_release "
                "WHERE source = :s AND status <> 'loaded' AND release_key < :cur "
                "ORDER BY release_key"
            ),
            {"s": source, "cur": current_key},
        ).mappings().all()
    stale = [dict(r) for r in rows if not _is_superseded(dict(r))]
    if dry_run or not stale:
        return [r["release_key"] for r in stale]
    with engine.begin() as conn:
        for r in stale:
            note = f"{SUPERSEDED_PREFIX} newer snapshot {current_key} discovered; was {r['status']}"
            if r.get("error"):
                note += f" ({r['error']})"
            conn.execute(
                text(
                    "UPDATE raw.source_release SET status = 'failed', error = :e, updated_at = NOW() "
                    "WHERE source = :s AND release_key = :k AND status <> 'loaded'"
                ),
                {"e": note[:4000], "s": source, "k": r["release_key"]},
            )
    keys = [r["release_key"] for r in stale]
    logger.info(f"[bulk:{source}] superseded stale releases {keys}")
    return keys


def apply_retention(
    engine,
    source: str,
    raw_root: Optional[Path | str] = None,
    keep: Optional[int] = None,
    dry_run: bool = False,
    assume_superseded: Iterable[str] = (),
) -> Dict[str, Any]:
    """Delete raw files beyond the newest ``keep`` loaded ones for ``source``.

    ``assume_superseded``: keys to treat as superseded (dry-run of a cleanup
    that would supersede them first).
    """
    from app.core.config import get_settings

    settings = get_settings()
    if keep is None:
        keep = settings.bulk_raw_retention
    keep = max(1, int(keep))
    raw_root = Path(raw_root or settings.bulk_raw_dir)
    src_dir = raw_root / source
    assume = set(assume_superseded)

    with engine.connect() as conn:
        rows = [
            dict(r)
            for r in conn.execute(
                text(
                    "SELECT release_key, status, local_path, error, loaded_at "
                    "FROM raw.source_release WHERE source = :s AND local_path IS NOT NULL"
                ),
                {"s": source},
            ).mappings()
        ]

    result: Dict[str, Any] = {
        "source": source, "keep": keep, "dry_run": dry_run, "kept": [], "protected": [],
        "deleted": [], "orphans": [], "bytes_to_free": 0, "bytes_freed": 0, "errors": [],
    }

    def entry(row, path):
        return {"release_key": row["release_key"], "status": row["status"],
                "path": str(path), "bytes": _size(path)}

    referenced = set()
    on_disk = []
    for row in rows:
        path = Path(row["local_path"])
        referenced.add(str(path.resolve()))
        if path.is_file() and _inside(path, src_dir):
            on_disk.append((row, path))

    loaded = sorted(
        (rp for rp in on_disk if rp[0]["status"] == "loaded"),
        key=lambda rp: (rp[0]["loaded_at"] or datetime.min, rp[0]["release_key"]),
        reverse=True,
    )
    keep_paths = set()
    doomed = []
    for i, (row, path) in enumerate(loaded):
        if i < keep:
            keep_paths.add(str(path.resolve()))
            result["kept"].append(entry(row, path))
        else:
            doomed.append((row, path))
    for row, path in on_disk:
        if row["status"] == "loaded":
            continue
        if _is_superseded(row) or row["release_key"] in assume:
            doomed.append((row, path))
        else:
            keep_paths.add(str(path.resolve()))
            result["protected"].append(entry(row, path))

    seen = set()
    for row, path in doomed:
        resolved = str(path.resolve())
        if resolved in keep_paths or resolved in seen:  # shared with a kept/protected row
            continue
        seen.add(resolved)
        item = entry(row, path)
        result["deleted"].append(item)
        result["bytes_to_free"] += item["bytes"]
        if dry_run:
            continue
        try:
            path.unlink()
            result["bytes_freed"] += item["bytes"]
            parent = path.parent
            src_resolved = src_dir.resolve()
            while (parent.resolve() != src_resolved and _inside(parent, src_dir)
                   and not any(parent.iterdir())):
                parent.rmdir()
                parent = parent.parent
        except OSError as e:
            result["errors"].append(f"{path}: {e}")

    if src_dir.is_dir():
        for dirpath, _dirs, files in os.walk(src_dir):
            for name in files:
                p = Path(dirpath) / name
                if str(p.resolve()) not in referenced:
                    result["orphans"].append({"path": str(p), "bytes": _size(p)})

    if result["deleted"]:
        verb = "would free" if dry_run else "freed"
        logger.info(
            f"[bulk:{source}] retention keep={keep}: {len(result['deleted'])} file(s), "
            f"{verb} {result['bytes_to_free'] if dry_run else result['bytes_freed']} bytes"
        )
    return result


def snapshot_sources() -> List[str]:
    return [n for n in list_sources() if getattr(get_source(n), "snapshot", False)]


def cleanup(
    engine=None,
    sources: Optional[List[str]] = None,
    raw_root: Optional[Path | str] = None,
    keep: Optional[int] = None,
    dry_run: bool = True,
    current_key: Optional[str] = None,
) -> Dict[str, Any]:
    """One-time / on-demand cleanup: supersede stale snapshots, then apply retention.

    ``sources`` defaults to the snapshot sources. Dry-run changes nothing.
    """
    if engine is None:
        from app.core.database import get_engine

        engine = get_engine()
    names = sources or snapshot_sources()
    out: Dict[str, Any] = {"dry_run": dry_run, "sources": {}, "bytes_to_free": 0, "bytes_freed": 0}
    for name in names:
        src = get_source(name)
        superseded: List[str] = []
        if getattr(src, "snapshot", False):
            cur = current_key
            if cur is None:
                keys = [r.release_key for r in src.discover(None)]  # snapshot discover is offline
                cur = max(keys) if keys else None
            if cur:
                superseded = supersede_stale(engine, name, cur, dry_run=dry_run)
        res = apply_retention(engine, name, raw_root, keep=keep, dry_run=dry_run,
                              assume_superseded=superseded if dry_run else ())
        res["superseded"] = superseded
        out["sources"][name] = res
        out["bytes_to_free"] += res["bytes_to_free"]
        out["bytes_freed"] += res["bytes_freed"]
    return out


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Prune raw bulk files (SPEC_122). Dry-run by default.")
    parser.add_argument("--source", action="append", help="Bulk source (repeatable). Default: snapshot sources")
    parser.add_argument("--keep", type=int, default=None, help="Loaded files to keep (default BULK_RAW_RETENTION)")
    parser.add_argument("--raw-root", default=None, help="Raw root (default settings.bulk_raw_dir)")
    parser.add_argument("--apply", action="store_true", help="Actually delete (otherwise dry-run)")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO)
    out = cleanup(sources=args.source, raw_root=args.raw_root, keep=args.keep, dry_run=not args.apply)
    print(json.dumps(out, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
