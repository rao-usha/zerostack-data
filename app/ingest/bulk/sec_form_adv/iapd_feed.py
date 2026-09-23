"""
``sec_iapd_feed``: the IAPD SEC firm compilation feed (SPEC_112).

``CompilationReports.manifest.json`` names the current
``IA_FIRM_SEC_Feed_MM_DD_YYYY.xml.gz``. That is ~7.3 MB gz, ~82 MB XML, and
~24k firms, both registered advisers and ERAs. The manifest lists only the
latest edition, so discover returns a single release, ``edition:YYYY-MM-DD``.

Ported from wildcard-workbench ``iapd_compilation_feed.py``. The tracked
universes and the finalize differ were removed, so every firm is loaded.
"""

from __future__ import annotations

import logging
from datetime import date
from pathlib import Path
from typing import Dict, List, Optional

from sqlalchemy import text

from app.core.copy_loader import (
    check_publish,
    copy_rows,
    create_staging,
    drop_staging,
    merge_staging,
    table_count,
)
from app.core.safe_sql import qi
from app.ingest.bulk.base import BulkSource, Release
from app.ingest.bulk.registry import register_bulk_source
from app.ingest.bulk.sec_form_adv.parse import (
    FEED_BASE,
    FEED_COLUMNS,
    MANIFEST_URL,
    iter_feed_rows,
    parse_manifest,
)

logger = logging.getLogger(__name__)

TARGET = "public.sec_adv_feed_firm_state"
KEY_COLUMNS = ["crd_number", "edition_date"]
STAGING = "sec_adv_feed"

FEED_DDL = [
    "CREATE TABLE IF NOT EXISTS public.sec_adv_feed_firm_state (\n    "
    + ",\n    ".join(f"{qi(c)} {t}" for c, t in FEED_COLUMNS)
    + ",\n    loaded_at TIMESTAMP DEFAULT NOW(),\n    PRIMARY KEY (crd_number, edition_date)\n)",
    "CREATE INDEX IF NOT EXISTS idx_sec_adv_feed_crd ON public.sec_adv_feed_firm_state (crd_number)",
    "CREATE INDEX IF NOT EXISTS idx_sec_adv_feed_edition ON public.sec_adv_feed_firm_state (edition_date)",
]


@register_bulk_source
class SecIapdFeed(BulkSource):
    name = "sec_iapd_feed"
    parser_version = "1"

    def discover(self, http, since: Optional[date] = None) -> List[Release]:
        filename, edition = parse_manifest(http.get_text(MANIFEST_URL))
        if since is not None and edition < since:
            return []
        return [Release(
            release_key=f"edition:{edition.isoformat()}",
            url=FEED_BASE + filename,
            meta={"edition_date": edition.isoformat(), "filename": filename},
        )]

    def ddl(self) -> List[str]:
        return list(FEED_DDL)

    def load(self, conn, release: Release, path: Path) -> Dict[str, int]:
        self.ensure_ddl(conn)
        edition_s = release.meta.get("edition_date") or release.release_key.partition(":")[2]
        edition = date.fromisoformat(edition_s)
        names = [c for c, _ in FEED_COLUMNS]
        create_staging(conn, STAGING, FEED_COLUMNS)
        copied = copy_rows(conn, STAGING, names, iter_feed_rows(path, edition, release.release_key))
        if copied == 0:
            raise RuntimeError(f"{release.release_key}: feed parsed 0 firms from {Path(path).name}")
        published_before = table_count(conn, TARGET)
        inserted, updated = merge_staging(conn, STAGING, TARGET, names, KEY_COLUMNS)
        drop_staging(conn, STAGING)
        # The prune below keeps only this edition: a truncated feed must not
        # replace a full one (SPEC_129).
        check_publish(TARGET, table_count(conn, TARGET, "edition_date >= :edition", {"edition": edition}),
                      published_before)
        # Keep only the latest edition (~40 MB each; daily editions ~14 GB/yr)
        pruned = conn.execute(
            text(f"DELETE FROM {TARGET} WHERE edition_date < :edition"), {"edition": edition}
        ).rowcount or 0
        if pruned:
            logger.info(f"[bulk:{self.name}] pruned {pruned} rows from older editions")
        logger.info(f"[bulk:{self.name}] {release.release_key}: copied {copied}, inserted {inserted}, updated {updated}")
        return {"sec_adv_feed_firm_state": inserted + updated}
