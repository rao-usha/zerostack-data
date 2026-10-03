"""
Entity resolution I/O (SPEC_116) — the write path around `resolve_core`.

`resolve_core` decides everything (pure, self-tested). This module only loads
its inputs from `core.*` and writes the results back, keeping the workbench's
two guarantees:

- **Idempotent.** Every write is guarded by `IS DISTINCT FROM`, so a second run
  over an unchanged corpus reports 0 entities and 0 memberships changed.
- **Nothing is dropped silently.** Refusals (vetoed keys, fan-out caps,
  oversized components) are counted into the run ledger.

SPEC_148: records of `ATTACH_ONLY_SOURCES` never reach `plan()` (nor the weak
name+state tier); `domain_links()` sees every record and decides each domain ->
subject link. Strong entity links become `core.identifier` rows
(`id_type = 'domain'`) and the entity's `canonical_domain`; every link, with its
claims and status, is written to `core.domain_link`.

SPEC_150: an EIN / CRD shared by two CIKs joins them only when the gate
corroborates it. `_load_profiles` reads the EDGAR filer profile of exactly the CIKs
that need one (`resolve_core.gated_ciks`); the write path records why: `gate` and
`split_from` in `field_conflicts`, and `dissolved_by_gate` on an entity whose
records all became single-record pieces.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple

from sqlalchemy import text

from app.core.copy_loader import copy_rows, create_staging, drop_staging, merge_staging
from app.entities.resolve_core import (
    ATTACH_ONLY_SOURCES,
    WEAK_LEI_SOURCES,
    BRIDGE_TIERS_ACCEPTED,
    RESOLVER_VERSION,
    assign_ids,
    canonical_row,
    domain_links,
    gated_ciks,
    plan,
    source_metrics,
    weak_name_state,
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


def _load_crd_hints(conn) -> List[Tuple[str, str]]:
    """SPEC_150 review hints: identifier-tier bridge edges on a CRD that maps to several
    CIKs (never unioned). `plan()` reads them only to flag a split EIN-sharing pair."""
    rows = conn.execute(
        text(
            "SELECT cik, crd FROM core.cik_crd_bridge "
            "WHERE tier = ANY(:tiers) AND crd_cik_count > 1"
        ),
        {"tiers": list(BRIDGE_TIERS_ACCEPTED)},
    ).mappings()
    return [(r["cik"], r["crd"]) for r in rows]


PROBE_TABLE = "core.domain_probe"


def _load_probes(conn) -> Dict[str, Dict[str, Any]]:
    """Latest probe per domain -> {redirect_to, names_found, evidence} (SPEC_148).
    Only a cross-domain redirect or a fetched page is evidence; refusals are not."""
    if not conn.execute(text("SELECT to_regclass(:t) IS NOT NULL"), {"t": PROBE_TABLE}).scalar():
        return {}
    rows = conn.execute(
        text(
            f"""
            SELECT DISTINCT ON (domain) domain, probed_at, url, outcome, http_status, redirect_to,
                   final_url, hops, names_found, user_agent, terms_citation
            FROM {PROBE_TABLE}
            ORDER BY domain, probed_at DESC
            """
        )
    ).mappings()
    out: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        if r["outcome"] not in ("redirect_offsite", "fetched"):
            continue
        evidence = {
            "probed_at": r["probed_at"].isoformat() if r["probed_at"] else None,
            "url": r["url"], "outcome": r["outcome"], "http_status": r["http_status"],
            "redirect_to": r["redirect_to"], "final_url": r["final_url"], "hops": r["hops"],
            "names_found": r["names_found"], "user_agent": r["user_agent"],
            "terms_citation": r["terms_citation"],
        }
        out[r["domain"]] = {
            "redirect_to": r["redirect_to"] if r["outcome"] == "redirect_offsite" else None,
            "names_found": list(r["names_found"] or []) if r["outcome"] == "fetched" else [],
            "evidence": evidence,
        }
    return out


# SPEC_150: the EDGAR filer profile the gate reads. A table or column a database
# lacks is tolerated -- a missing one is a NULL feature, never an error.
PROFILE_BATCH = 5000


def _columns(conn, table: str) -> set:
    rows = conn.execute(
        text("SELECT column_name FROM information_schema.columns "
             "WHERE table_schema = 'public' AND table_name = :t"),
        {"t": table},
    ).fetchall()
    return {r[0] for r in rows}


def _profile_rows(conn, sql: str, ciks: List[str]) -> List[dict]:
    """Run one profile query over CIKs in batches, inside a savepoint so a bad
    source table costs that feature, not the resolve."""
    out: List[dict] = []
    try:
        with conn.begin_nested():
            for i in range(0, len(ciks), PROFILE_BATCH):
                batch = ciks[i:i + PROFILE_BATCH]
                forms = sorted(set(batch) | {c.zfill(10) for c in batch})
                out.extend(dict(r) for r in conn.execute(text(sql), {"ciks": forms}).mappings())
    except Exception as exc:  # noqa: BLE001 -- reported; the feature is dropped
        logger.warning(f"[entities:resolve] filer profile query skipped: {type(exc).__name__}: {exc}")
        return []
    return out


def _load_profiles(conn, ciks) -> Dict[str, Dict[str, Any]]:
    """canonical CIK -> EDGAR filer profile (SPEC_150), for exactly `ciks`."""
    ciks = sorted(ciks)
    if not ciks:
        return {}

    def canon(v):
        return str(v or "").lstrip("0")

    prof: Dict[str, Dict[str, Any]] = {}
    have = _columns(conn, "sec_filers")
    if "cik" in have:
        want = {"name": "name", "sic": "sic", "latest_filing_date": "last_filed",
                "earliest_recent_filing_date": "first_filed", "latest_form": "latest_form",
                "tickers": "tickers", "insider_transaction_for_owner_exists": "insider_owner",
                "insider_transaction_for_issuer_exists": "insider_issuer",
                "recent_filing_count": "recent_filing_count"}
        cols = ", ".join(f"{c} AS {a}" if c in have else f"NULL AS {a}" for c, a in want.items())
        for r in _profile_rows(conn, f"SELECT cik, {cols} FROM sec_filers WHERE cik = ANY(:ciks)", ciks):
            prof[canon(r.pop("cik"))] = {k: (v.isoformat() if hasattr(v, "isoformat") else v)
                                         for k, v in r.items()}

    def add(table, need, sql, apply):
        if not need <= _columns(conn, table):
            return
        for r in _profile_rows(conn, sql, ciks):
            apply(prof.setdefault(canon(r["cik"]), {}), r)

    # dated former names: when a CIK was renamed INTO its name (SPEC_150 V4), and the
    # earliest name date of a heavy filer whose recent-filing list is truncated
    fcols = _columns(conn, "sec_filer_former_names")
    dated = ", ".join(c if c in fcols else f"NULL::date AS {c}" for c in ("from_date", "to_date"))

    def _iso(v):
        return v.isoformat() if hasattr(v, "isoformat") else v

    add("sec_filer_former_names", {"cik", "name"},
        f"SELECT DISTINCT cik, name, {dated} FROM sec_filer_former_names "
        "WHERE cik = ANY(:ciks) ORDER BY 1, 2, 3, 4",
        lambda p, r: p.setdefault("former_names", []).append(
            {"name": r["name"], "from_date": _iso(r["from_date"]), "to_date": _iso(r["to_date"])}))
    add("sec_insider_owners", {"rptowner_cik"},
        "SELECT rptowner_cik AS cik, COUNT(*) AS n FROM sec_insider_owners "
        "WHERE rptowner_cik = ANY(:ciks) GROUP BY 1",
        lambda p, r: p.__setitem__("owner_filings", int(r["n"])))
    add("form_d_filings", {"cik", "is_pooled_investment_fund"},
        "SELECT cik, COUNT(*) AS n, bool_or(is_pooled_investment_fund) AS pooled "
        "FROM form_d_filings WHERE cik = ANY(:ciks) GROUP BY 1",
        lambda p, r: p.update({"formd_filings": int(r["n"]), "formd_pooled": bool(r["pooled"])}))
    add("sec_13f_filings", {"cik"},
        "SELECT cik, COUNT(*) AS n FROM sec_13f_filings WHERE cik = ANY(:ciks) GROUP BY 1",
        lambda p, r: p.__setitem__("f13_filings", int(r["n"])))
    return prof


def entity_domain_columns(strong: List[str]) -> Tuple[Any, Dict[str, Any]]:
    """(canonical_domain, extra field_conflicts) from an entity's STRONG domains.
    One strong domain is the canonical one; none or several leave it NULL."""
    strong = sorted(set(strong or []))
    if len(strong) == 1:
        return strong[0], {}
    if not strong:
        return None, {}
    return None, {"domain_strong": strong[:12]}


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

# SPEC_147: flagged name+state candidates (evidence, never a merge)
WEAK_TABLE = "core.weak_match"
WEAK_COLUMNS = [
    ("record_key", "TEXT"),
    ("candidate_record_key", "TEXT"),
    ("candidate_entity_id", "BIGINT"),
    ("tier", "VARCHAR(16)"),
    ("status", "VARCHAR(16)"),
    ("conflicts", "JSONB"),
    ("matched_on", "JSONB"),
    ("resolver_version", "VARCHAR(16)"),
    ("updated_at", "TIMESTAMP"),
]
WEAK_STG = "core_weak_match"

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


def _build_rows(comps, ids, by_key, rec_keys, now, domain_plan=None, prior_split_from=None):
    """Components -> (entity rows, membership rows, identifier rows, alias rows).

    `prior_split_from` (entity_id -> ids) keeps a piece's SPEC_150 `split_from`
    note on later runs, when no split happens any more (idempotent)."""
    split_from = {s["component_first_member"]: sorted(s["lost_entity_ids"])
                  for s in ids.get("splits", [])}
    prior_split_from = prior_split_from or {}
    reserved = _RESERVED
    entity_rows, membership_rows, identifier_rows, alias_rows = [], [], [], []
    strong = (domain_plan or {}).get("entity_strong", {})
    strong_sources = {
        (r["candidate_comp"], r["domain"]): r["sources"]
        for r in (domain_plan or {}).get("rows", [])
        if r["status"] == "strong" and r["candidate_comp"] is not None
    }
    for idx, comp in enumerate(comps):
        cols, conflicts = canonical_row(comp["members"], by_key, comp.get("withheld"))
        # SPEC_150 provenance: why the gate joined / refused, where a piece came from
        if comp.get("gate"):
            conflicts["gate"] = comp["gate"]
        # SPEC_148: canonical_domain is a STRONG link or nothing
        cols["canonical_domain"], extra = entity_domain_columns(strong.get(idx, []))
        conflicts.update(extra)
        entity_id = ids["assigned"].get(idx) or reserved.pop()
        if comp["members"][0] in split_from:
            conflicts["split_from"] = split_from[comp["members"][0]]
        elif entity_id in prior_split_from:
            conflicts["split_from"] = prior_split_from[entity_id]
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
        for d in strong.get(idx, []):
            identifier_rows.append(("domain", d, entity_id, _pg_array(strong_sources[(idx, d)]), now))
    return entity_rows, membership_rows, identifier_rows, alias_rows


def _stage_and_merge(conn, name, columns, rows, target, keys, update_columns=None):
    """COPY rows into stg.<name>, merge into target, keep the staging table."""
    names = [c for c, _ in columns]
    create_staging(conn, name, columns)
    copy_rows(conn, name, names, rows)
    inserted, updated = merge_staging(conn, name, target, names, keys, update_columns)
    return inserted, updated


def _flat(d: Dict[str, Any], prefix: str = "") -> Dict[str, Any]:
    """Nested metrics -> one level of scalars ('a.b': 1, lists joined by ','), so
    the job ledger's compact_summary keeps them (it drops nested blocks)."""
    out: Dict[str, Any] = {}
    for k, v in d.items():
        key = f"{prefix}{k}"
        if isinstance(v, dict):
            out.update(_flat(v, key + "."))
        elif isinstance(v, (list, tuple)):
            out[key] = ",".join(str(x) for x in v)
        else:
            out[key] = v
    return out


def dissolved_by_gate(comps, live, ids, rec_keys, refused_by_cik) -> Dict[int, Dict[str, Any]]:
    """Live entities this run leaves with no component, where the gate refused a
    link among their records -> {entity_id: evidence}. PURE (SPEC_150)."""
    from app.entities.gate import GATE_VERSION
    from app.entities.resolve_core import DETAIL_CAP

    kept = set(ids["assigned"].values()) | {
        e for s in ids.get("splits", []) for e in s["lost_entity_ids"]}
    in_comp = {rk for c in comps for rk in c["members"]}
    by_entity: Dict[int, List[str]] = {}
    for rk, eid in live.items():
        by_entity.setdefault(eid, []).append(rk)
    out: Dict[int, Dict[str, Any]] = {}
    for eid, rks in by_entity.items():
        if eid in kept or any(rk in in_comp for rk in rks):
            continue
        ciks = sorted({v for rk in rks for t, v in rec_keys.get(rk, ()) if t == "cik"})
        refused = [e for c in ciks for e in refused_by_cik.get(c, ())]
        if not refused:
            continue
        refused.sort(key=lambda e: (e["cik"], e["other_cik"]))
        ev = {"gate_version": GATE_VERSION, "records": sorted(rks)[:DETAIL_CAP],
              "refused": refused[:DETAIL_CAP]}
        if len(refused) > DETAIL_CAP:
            ev["refused_count"] = len(refused)
        out[eid] = ev
    return out


def _dissolve(conn, absorbed: int, survivor: int) -> None:
    """Forward an entity absorbed by a merge to its survivor (SPEC_147 T17).

    Called on the merge path since SPEC_116 but never defined: no merge had
    happened, so the NameError was latent. Setting superseded_by here, before
    the orphan sweep, is what keeps `ent:<absorbed>` resolvable."""
    conn.execute(
        text(
            "UPDATE core.entity SET dissolved_at = NOW(), superseded_by = :s, updated_at = NOW() "
            "WHERE entity_id = :a AND dissolved_at IS NULL"
        ),
        {"a": absorbed, "s": survivor},
    )


def _write_weak(conn, rows, comp_entity, now) -> Dict[str, Any]:
    """Merge the weak-tier rows into core.weak_match; drop rows no longer derived."""
    if not conn.execute(text("SELECT to_regclass(:t) IS NOT NULL"), {"t": WEAK_TABLE}).scalar():
        # migration 0016 not applied: report it in the ledger rather than fail the resolve
        logger.warning(f"[entities:resolve] {WEAK_TABLE} missing: weak tier not written")
        return {"table_missing": True, "rows_written": 0, "rows_removed": 0}
    staged = [
        (
            r["record_key"], r["candidate_record_key"],
            comp_entity[r["candidate_comp"]] if r["candidate_comp"] is not None else None,
            r["tier"], r["status"], json.dumps(r["conflicts"]), json.dumps(r["matched_on"]),
            RESOLVER_VERSION, now,
        )
        for r in rows
    ]
    inserted, updated = _stage_and_merge(
        conn, WEAK_STG, WEAK_COLUMNS, staged, WEAK_TABLE, ["record_key", "candidate_record_key"]
    )
    removed = conn.execute(
        text(
            f"DELETE FROM {WEAK_TABLE} w WHERE NOT EXISTS "
            f"(SELECT 1 FROM stg.{WEAK_STG} s WHERE s.record_key = w.record_key "
            f" AND s.candidate_record_key = w.candidate_record_key)"
        )
    ).rowcount or 0
    drop_staging(conn, WEAK_STG)
    return {"rows_total": len(staged), "rows_written": inserted + updated, "rows_removed": removed}


DOMAIN_TABLE = "core.domain_link"
DOMAIN_STG = "core_domain_link"
DOMAIN_COLUMNS = [
    ("domain", "TEXT"),
    ("subject", "TEXT"),
    ("entity_id", "BIGINT"),
    ("record_key", "TEXT"),
    ("status", "VARCHAR(16)"),
    ("families", "TEXT[]"),
    ("sources", "TEXT[]"),
    ("claims", "JSONB"),
    ("claim_count", "INTEGER"),
    ("conflicts", "JSONB"),
    ("alias_of", "TEXT"),
    ("evidence", "JSONB"),
    ("resolver_version", "VARCHAR(16)"),
    ("updated_at", "TIMESTAMP"),
]


def _domain_subject(row, comp_entity) -> Tuple[str, Any]:
    if row["candidate_comp"] is not None:
        eid = comp_entity[row["candidate_comp"]]
        return f"ent:{eid}", eid
    return row["subject"], None


def _write_domains(conn, rows, comp_entity, now) -> Dict[str, Any]:
    """Merge the domain links into core.domain_link; drop links no longer derived."""
    if not conn.execute(text("SELECT to_regclass(:t) IS NOT NULL"), {"t": DOMAIN_TABLE}).scalar():
        logger.warning(f"[entities:resolve] {DOMAIN_TABLE} missing: domain links not written")
        return {"table_missing": True, "rows_written": 0, "rows_removed": 0}
    staged = []
    for r in rows:
        subject, eid = _domain_subject(r, comp_entity)
        staged.append((
            r["domain"], subject, eid, r["record_key"], r["status"],
            _pg_array(r["families"]), _pg_array(r["sources"]),
            json.dumps(r["claims"]), r["claim_count"], json.dumps(r["conflicts"]),
            r["alias_of"], json.dumps(r["evidence"]) if r["evidence"] is not None else None,
            RESOLVER_VERSION, now,
        ))
    inserted, updated = _stage_and_merge(
        conn, DOMAIN_STG, DOMAIN_COLUMNS, staged, DOMAIN_TABLE, ["domain", "subject"]
    )
    removed = conn.execute(
        text(
            f"DELETE FROM {DOMAIN_TABLE} d WHERE NOT EXISTS "
            f"(SELECT 1 FROM stg.{DOMAIN_STG} s WHERE s.domain = d.domain AND s.subject = d.subject)"
        )
    ).rowcount or 0
    drop_staging(conn, DOMAIN_STG)
    return {"rows_total": len(staged), "rows_written": inserted + updated, "rows_removed": removed}


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
    all_records = _load_records(conn)
    # SPEC_148: attach-only records carry domain claims; they never resolve
    records = [r for r in all_records if r.get("source") not in ATTACH_ONLY_SOURCES]
    vetoes = _load_vetoes(conn)
    bridge = _load_bridge(conn)
    by_key = {r["record_key"]: r for r in records}

    # SPEC_150: the gate's filer profiles, for exactly the CIKs that need one
    profiles = _load_profiles(conn, gated_ciks(records, bridge, vetoes))
    result = plan(records, bridge, vetoes, profiles=profiles, crd_hints=_load_crd_hints(conn))
    comps = result["components"]
    metrics = dict(result["metrics"])
    metrics["gate"] = _flat(metrics["gate"])
    metrics["refusals"] = result["refusals"]
    metrics["refused_detail"] = result["refused_detail"]

    live, dissolved = _load_prior(conn)
    ids = assign_ids(comps, live, dissolved)
    gate_dissolved = dissolved_by_gate(comps, live, ids, result["record_keys"],
                                       result["gate_refused_by_cik"])
    metrics["gate"]["entities_dissolved_by_gate"] = len(gate_dissolved)
    metrics["gate"]["pieces_split_off"] = len(ids["splits"])
    # SPEC_147: the weak tier reads the components, it never changes them
    weak = weak_name_state(records, result["record_keys"], comps)
    metrics["weak"] = _flat(weak["metrics"])
    metrics["dol5500"] = _flat(source_metrics(
        records, result["record_keys"], comps, weak, new_components=ids["new_entities"]
    ))
    # SPEC_154: GLEIF records get their own name+state call (lei_conflict); USAspending UEIs carry
    # no state, so they have no weak tier. Both report how they resolved.
    weak_lei = weak_name_state(records, result["record_keys"], comps, sources=WEAK_LEI_SOURCES)
    metrics["weak_gleif"] = _flat(weak_lei["metrics"])
    metrics["gleif"] = _flat(source_metrics(
        records, result["record_keys"], comps, weak_lei, new_components=ids["new_entities"],
        source="gleif", key_type="lei"))
    metrics["usasp"] = _flat(source_metrics(
        records, result["record_keys"], comps, {"rows": [], "metrics": {"sponsors_by_status": {}}},
        new_components=ids["new_entities"], source="usasp", key_type="uei"))
    domain_plan = domain_links(all_records, result["record_keys"], comps, _load_probes(conn),
                               key_owner=result["key_owner"])
    metrics["domains"] = _flat(domain_plan["metrics"])
    metrics["domains"]["attach_only_records"] = len(all_records) - len(records)
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
    prior_split_from = {
        r[0]: r[1] for r in conn.execute(text(
            "SELECT entity_id, field_conflicts->'split_from' FROM core.entity "
            "WHERE dissolved_at IS NULL AND field_conflicts ? 'split_from'"))
    }
    entity_rows, membership_rows, identifier_rows, alias_rows = _build_rows(
        comps, ids, by_key, result["record_keys"], now, domain_plan, prior_split_from
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

    # component i was written as entity_rows[i] (same order as comps)
    comp_entity = [row[0] for row in entity_rows]
    metrics["weak"].update(_write_weak(conn, weak["rows"] + weak_lei["rows"], comp_entity, now))
    metrics["domains"].update(_write_domains(conn, domain_plan["rows"], comp_entity, now))

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

    # SPEC_150: an entity the gate dissolved says why (after the orphan sweep)
    for eid, ev in sorted(gate_dissolved.items()):
        conn.execute(
            text(
                "UPDATE core.entity SET field_conflicts = COALESCE(field_conflicts, '{}'::jsonb) "
                "|| CAST(:ev AS JSONB) WHERE entity_id = :e AND dissolved_at IS NOT NULL"
            ),
            {"e": eid, "ev": json.dumps({"dissolved_by_gate": ev})},
        )

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
