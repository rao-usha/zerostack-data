"""
Entity resolution I/O (SPEC_116) — the write path around `resolve_core`.

`resolve_core` decides everything (pure, self-tested). This module only loads
its inputs from `core.*` and writes the results back, keeping the workbench's
two guarantees:

- **Idempotent.** Every write is guarded by `IS DISTINCT FROM`, so a second run
  over an unchanged corpus reports 0 entities and 0 memberships changed.
- **Nothing is dropped silently.** Refusals (vetoed keys, fan-out caps,
  oversized components) are counted into the run ledger.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import text

from app.core.copy_loader import copy_rows, create_staging, drop_staging, merge_staging
from app.entities.resolve_core import (
    BRIDGE_TIERS_ACCEPTED,
    RESOLVER_VERSION,
    assign_ids,
    canonical_row,
    plan,
)

logger = logging.getLogger(__name__)

# entity ids pre-allocated for this run (module-level so _build_rows can pop)
_RESERVED: List[int] = []

RECORD_COLUMNS = (
    "record_key, source, native_id, legal_name, name_norm, name_norm_version, "
    "ein, cik, crd, lei, uei, state_entity_id, state, zip5, domain"
)

# core.entity columns written from canonical_row()
_CANON_COLS = (
    "canonical_name", "ein", "cik", "crd", "lei", "uei", "state_entity_id",
    "canonical_state", "canonical_zip5", "canonical_domain",
)


def _now() -> datetime:
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# load
# ---------------------------------------------------------------------------

def _load_records(conn) -> List[dict]:
    rows = conn.execute(
        text(f"SELECT {RECORD_COLUMNS} FROM core.source_record ORDER BY record_key")
    ).mappings()
    return [dict(r) for r in rows]


def _load_vetoes(conn) -> Dict[Tuple[str, str, str], str]:
    rows = conn.execute(
        text(
            "SELECT key_type, key_value, record_key, reason FROM core.key_veto "
            "WHERE revoked_at IS NULL"
        )
    ).mappings()
    return {(r["key_type"], r["key_value"], r["record_key"]): r["reason"] for r in rows}


def _load_bridge(conn) -> List[Tuple[str, str, str, str]]:
    """Identifier-tier bridge edges only, and only where the CRD maps to one CIK.

    `name_state` is excluded on purpose: it is a name match, and letting it in
    would smuggle name-based merging into an identifier-only resolver.
    """
    rows = conn.execute(
        text(
            "SELECT cik, crd, tier, matched_on FROM core.cik_crd_bridge "
            "WHERE tier = ANY(:tiers) AND crd_cik_count = 1"
        ),
        {"tiers": list(BRIDGE_TIERS_ACCEPTED)},
    ).mappings()
    return [(r["cik"], r["crd"], r["tier"], r["matched_on"]) for r in rows]


def _load_prior(conn) -> Tuple[Dict[str, int], Dict[str, int]]:
    """(live membership, dissolved-entity fallback) — record_key -> entity_id."""
    live = {
        r["record_key"]: r["entity_id"]
        for r in conn.execute(text("SELECT record_key, entity_id FROM core.membership")).mappings()
    }
    dissolved: Dict[str, int] = {}
    rows = conn.execute(
        text(
            "SELECT entity_id, last_members FROM core.entity "
            "WHERE dissolved_at IS NOT NULL AND last_members IS NOT NULL "
            "ORDER BY entity_id"
        )
    ).mappings()
    for r in rows:
        for rk in r["last_members"] or []:
            dissolved.setdefault(rk, r["entity_id"])
    return live, dissolved


# ---------------------------------------------------------------------------
# write
# ---------------------------------------------------------------------------

ENTITY_COLUMNS = [
    ("entity_id", "BIGINT"),
    ("canonical_name", "TEXT"),
    ("ein", "TEXT"),
    ("cik", "TEXT"),
    ("crd", "TEXT"),
    ("lei", "TEXT"),
    ("uei", "TEXT"),
    ("state_entity_id", "TEXT"),
    ("canonical_state", "VARCHAR(2)"),
    ("canonical_zip5", "VARCHAR(5)"),
    ("canonical_domain", "TEXT"),
    ("member_count", "INTEGER"),
    ("strong_key_count", "INTEGER"),
    ("match_tier", "VARCHAR(16)"),
    ("field_conflicts", "JSONB"),
    ("last_members", "JSONB"),
    ("resolver_version", "VARCHAR(16)"),
    ("dissolved_at", "TIMESTAMP"),
    ("superseded_by", "BIGINT"),
    ("updated_at", "TIMESTAMP"),
]
MEMBERSHIP_COLUMNS = [
    ("record_key", "TEXT"),
    ("entity_id", "BIGINT"),
    ("match_tier", "VARCHAR(16)"),
    ("match_method", "VARCHAR(32)"),
    ("features", "JSONB"),
    ("updated_at", "TIMESTAMP"),
]
IDENTIFIER_COLUMNS = [
    ("id_type", "VARCHAR(16)"),
    ("id_value", "TEXT"),
    ("entity_id", "BIGINT"),
    ("sources", "TEXT[]"),
    ("last_seen", "TIMESTAMP"),
]
ALIAS_COLUMNS = [
    ("entity_id", "BIGINT"),
    ("name_norm", "TEXT"),
    ("name", "TEXT"),
    ("source", "VARCHAR(32)"),
]

STG = {
    "entity": "core_entity",
    "membership": "core_membership",
    "identifier": "core_identifier",
    "alias": "core_alias",
}


def _pg_array(values) -> str:
    """TEXT[] literal for COPY (sources are short alnum tokens)."""
    return "{" + ",".join(sorted({str(v) for v in values})) + "}"


def _reserve_entity_ids(conn, count: int) -> List[int]:
    """Pre-allocate ids so every row can be written in one COPY."""
    if count <= 0:
        return []
    return [
        r[0]
        for r in conn.execute(
            text("SELECT nextval('core.entity_entity_id_seq') FROM generate_series(1, :n)"),
            {"n": count},
        )
    ]


def _build_rows(comps, ids, by_key, rec_keys, now):
    """Components -> (entity rows, membership rows, identifier rows, alias rows)."""
    reserved = _RESERVED
    entity_rows, membership_rows, identifier_rows, alias_rows = [], [], [], []
    for idx, comp in enumerate(comps):
        cols, conflicts = canonical_row(comp["members"], by_key)
        entity_id = ids["assigned"].get(idx) or reserved.pop()
        entity_rows.append(
            (
                entity_id,
                cols.get("canonical_name"), cols.get("ein"), cols.get("cik"), cols.get("crd"),
                cols.get("lei"), cols.get("uei"), cols.get("state_entity_id"),
                cols.get("canonical_state"), cols.get("canonical_zip5"), cols.get("canonical_domain"),
                len(comp["members"]), len(comp["keys"]), comp["tier"],
                json.dumps(conflicts), json.dumps(comp["members"]), RESOLVER_VERSION,
                None, None, now,
            )
        )
        sources = sorted({m.split(":", 1)[0] for m in comp["members"]})
        for rk in comp["members"]:
            features = json.dumps(
                {"keys": [{"type": t, "value": v} for t, v in rec_keys.get(rk, [])]}
            )
            membership_rows.append((rk, entity_id, comp["tier"], "strong_key", features, now))
            rec = by_key[rk]
            if rec.get("name_norm"):
                alias_rows.append(
                    (entity_id, rec["name_norm"], rec.get("legal_name"), rec.get("source") or "unknown")
                )
        for key_type, key_value in comp["keys"]:
            identifier_rows.append((key_type, key_value, entity_id, _pg_array(sources), now))
    return entity_rows, membership_rows, identifier_rows, alias_rows


def _stage_and_merge(conn, name, columns, rows, target, keys, update_columns=None):
    """COPY rows into stg.<name>, merge into target, keep the staging table."""
    names = [c for c, _ in columns]
    create_staging(conn, name, columns)
    copy_rows(conn, name, names, rows)
    inserted, updated = merge_staging(conn, name, target, names, keys, update_columns)
    return inserted, updated


def _ledger(conn, metrics: Dict[str, Any], started: datetime, dry_run: bool) -> None:
    conn.execute(
        text(
            """
            INSERT INTO core.resolve_run (run_at, resolver_version, dry_run, duration_seconds, metrics)
            VALUES (:run_at, :version, :dry_run, :secs, CAST(:metrics AS JSONB))
            ON CONFLICT (run_at) DO NOTHING
            """
        ),
        {
            "run_at": started.replace(tzinfo=None),
            "version": RESOLVER_VERSION,
            "dry_run": dry_run,
            "secs": round((_now() - started).total_seconds(), 2),
            "metrics": json.dumps(metrics),
        },
    )


# ---------------------------------------------------------------------------
# entry point
# ---------------------------------------------------------------------------

def resolve(conn, dry_run: bool = False) -> Dict[str, Any]:
    """Re-derive core.entity / membership / identifier from core.source_record."""
    started = _now()
    records = _load_records(conn)
    vetoes = _load_vetoes(conn)
    bridge = _load_bridge(conn)
    by_key = {r["record_key"]: r for r in records}

    result = plan(records, bridge, vetoes)
    comps = result["components"]
    metrics = dict(result["metrics"])
    metrics["refusals"] = result["refusals"]
    metrics["refused_detail"] = result["refused_detail"]

    live, dissolved = _load_prior(conn)
    ids = assign_ids(comps, live, dissolved)
    metrics.update(
        {
            "entities_new": len(ids["new_entities"]),
            "entities_reused": len(ids["assigned"]),
            "entities_merged": len(ids["merges"]),
            "entities_split": len(ids["splits"]),
            "entities_reclaimed": ids["reclaimed"],
            "dry_run": dry_run,
        }
    )

    if dry_run:
        _ledger(conn, metrics, started, True)
        return metrics

    now = started.replace(tzinfo=None)
    global _RESERVED
    _RESERVED = _reserve_entity_ids(conn, len(ids["new_entities"]))
    entity_rows, membership_rows, identifier_rows, alias_rows = _build_rows(
        comps, ids, by_key, result["record_keys"], now
    )

    # Set-based writes: four COPY + merge passes instead of ~1M statements.
    # The merge skips unchanged rows, so a re-run reports 0 written.
    entities_written, entities_updated = _stage_and_merge(
        conn, STG["entity"], ENTITY_COLUMNS, entity_rows, "core.entity", ["entity_id"]
    )
    memberships_written, memberships_updated = _stage_and_merge(
        conn, STG["membership"], MEMBERSHIP_COLUMNS, membership_rows, "core.membership", ["record_key"]
    )
    identifiers_written, identifiers_updated = _stage_and_merge(
        conn, STG["identifier"], IDENTIFIER_COLUMNS, identifier_rows, "core.identifier",
        ["id_type", "id_value"]
    )
    _stage_and_merge(
        conn, STG["alias"], ALIAS_COLUMNS, alias_rows, "core.alias",
        ["entity_id", "name_norm", "source"], ["name"]
    )

    # rows that no longer belong to any component
    removed_memberships = conn.execute(
        text(
            f"DELETE FROM core.membership m WHERE NOT EXISTS "
            f"(SELECT 1 FROM stg.{STG['membership']} s WHERE s.record_key = m.record_key)"
        )
    ).rowcount or 0
    conn.execute(
        text(
            f"DELETE FROM core.identifier i WHERE NOT EXISTS "
            f"(SELECT 1 FROM stg.{STG['identifier']} s "
            f" WHERE s.id_type = i.id_type AND s.id_value = i.id_value)"
        )
    )
    for stg_name in STG.values():
        drop_staging(conn, stg_name)

    metrics["memberships_removed"] = removed_memberships
    metrics["entities_updated"] = entities_updated
    metrics["memberships_updated"] = memberships_updated
    metrics["identifiers_written"] = identifiers_written + identifiers_updated

    for absorbed, survivor, cause in ids["merges"]:
        conn.execute(
            text(
                "INSERT INTO core.entity_merge (absorbed_entity_id, survivor_entity_id, cause) "
                "VALUES (:a, :s, CAST(:cause AS JSONB))"
            ),
            {"a": absorbed, "s": survivor, "cause": json.dumps(cause)},
        )
        _dissolve(conn, absorbed, survivor)

    # entities that lost every member
    orphaned = conn.execute(
        text(
            """
            UPDATE core.entity e SET dissolved_at = NOW(), updated_at = NOW()
            WHERE e.dissolved_at IS NULL
              AND NOT EXISTS (SELECT 1 FROM core.membership m WHERE m.entity_id = e.entity_id)
            """
        )
    ).rowcount or 0

    metrics.update(
        {
            "entities_written": entities_written,
            "memberships_written": memberships_written,
            "entities_dissolved": orphaned,
        }
    )
    _ledger(conn, metrics, started, False)
    logger.info(f"[entities:resolve] {metrics}")
    return metrics
