"""
Rights review workflow and report (SPEC_142, PLAN_088 §1.7 / §3 "SPEC_135").

**Who decides ``reviewed``.** Code truth plus a committed hash (PLAN_088
decision 8, the plan's default):

- ``rights.py`` holds the declared rights block of every dataset, with the
  citation it rests on. Cited tightenings are applied there; loosenings are
  ``RightsProposal``s that are shown and never applied.
- An admin records a decision here (``record_review``): ``confirm_current``,
  ``accept_proposal`` or ``reject``, pinned to the ``rights_hash`` of the
  block they saw. The row goes to the append-only ``catalog_rights_review``
  table (Alembic 0015) with reviewer and timestamp. **Recording a decision
  never makes anything reviewed.**
- ``python -m app.catalog.rights_review --emit --out app/catalog/rights_reviewed.py`` writes
  ``rights_reviewed.py`` from the latest ``confirm_current`` per dataset whose
  hash still matches the code. A human reviews that diff and commits it; only
  then is ``reviewed`` True (``rights_reviewed.apply_review``). Any later
  rights change alters the hash and re-opens review.

The queue (``build_queue``) and the report (``build_report``) read the
catalog statically; live row counts of storage-forbidden holdings and the
review history are best-effort (``None`` / empty when the DB or the table is
unavailable), never an error.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import threading
import time
from collections import Counter
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from sqlalchemy import text
from sqlalchemy.engine import Engine

from app.catalog.spec import DatasetSpec, rights_hash_of

logger = logging.getLogger(__name__)

TABLE = "catalog_rights_review"
DECISIONS = ("confirm_current", "accept_proposal", "reject")
MIN_NOTE = 10
EVIDENCE_PATH = "app/catalog/evidence/rights_research_2026-09-25.json"

# ── PLAN_088 §1.7 candidates for reviewed=True (a human still signs off) ──────
CANDIDATE_KEYS = ("sec_13f", "sec_companyfacts", "sec_company_financials", "realestate_fhfa_hpi",
                  "realestate_hud_permits", "intl_worldbank", "intl_oecd")
CANDIDATE_SOURCES = ("treasury", "bls", "bea", "eia", "census", "us_trade", "bts", "cftc_cot", "usda",
                     "fdic", "fema", "irs_soi", "fbi_crime", "osha", "epa_echo", "usaspending", "fda",
                     "fcc_broadband", "uspto")
CANDIDATE_COLLECTORS = ("epoch_dc", "openei_urdb", "fcc")
# public domain but personal data: they wait on the PII-policy decision, not a rights review
PII_POLICY_KEYS = ("nppes_providers", "sec_insider", "sec_form_d", "sec_edgar_submissions",
                   "sam_gov_entities", "pe_people_sec")

# ── what SPEC_142 tightened (for the report; the values live in rights.py) ────
TIGHTENINGS: Dict[str, str] = {
    "cms_medicare_utilization": "open -> restricted, commercial_use agreement_required (CPT © AMA)",
    "intl_imf": "attribution -> restricted, commercial_use agreement_required (IMF permission first)",
    "intl_bis": "commercial_use restricted (no surcharge to subscribers, no implied endorsement)",
    "courtlistener": "attribution -> restricted, commercial_use agreement_required (FLP agreement)",
    "yelp": "storage forbidden (no storage beyond 24 h, no listings database)",
    "medspa_discovery": "storage forbidden (derived from Yelp content)",
    "vertical_discovery": "storage forbidden (derived from Yelp content)",
    "fred": "storage forbidden as retrieved (no caching/archiving), mandated FRED notice",
    "kaggle": "storage forbidden, commercial_use forbidden (M5: non-commercial only)",
    "prediction_markets": "storage forbidden, commercial_use forbidden (Kalshi: no archived data sets)",
    "national_zoning_atlas": "storage forbidden, commercial_use forbidden (no host/store without permission)",
    "peeringdb": "storage forbidden, commercial_use forbidden (no commercial application)",
    "loopnet": "storage forbidden, commercial_use forbidden (scraping / database creation prohibited)",
    "foot_traffic": "storage time_limited 30 days (Google Places lat/lng)",
    "afdc": "licence: NREL open data, free with credit (not §105); open -> attribution",
    "nrel_resource": "licence: NREL open data, free with credit (not §105); open -> attribution",
    "freightos": "commercial_use restricted (no resale, no derived indexes, no AI training)",
    "si_utility_rates": "attribution adds EIA (about 36% of rows are EIA-sourced)",
    "si_zoning_districts": "note: 164 nj_sussex_county_gis rows outside the NZA terms",
}

_HOLDINGS_TTL_S = 300
_HOLDINGS_DEADLINE_S = 30.0
_holdings_cache: Dict[str, Tuple[float, Dict[str, Any]]] = {}
_holdings_lock = threading.Lock()


class ReviewError(Exception):
    """A refused review request; ``status`` is the HTTP status the API returns."""

    def __init__(self, status: int, detail: str):
        super().__init__(detail)
        self.status = status
        self.detail = detail


# ---------------------------------------------------------------------------
# Static helpers
# ---------------------------------------------------------------------------


def _catalog() -> Sequence[DatasetSpec]:
    from app.catalog.registry import get_catalog

    return get_catalog()


def collector_of(spec: DatasetSpec) -> Optional[str]:
    return spec.producer.split(":", 1)[1] if spec.producer.startswith("collector:") else None


def proposal_hash(spec: DatasetSpec) -> Optional[str]:
    block = spec.proposed_block()
    return rights_hash_of(block) if block is not None else None


def rights_diff(current: Dict[str, Any], proposed: Optional[Dict[str, Any]]) -> Dict[str, List[Any]]:
    if proposed is None:
        return {}
    return {f: [current.get(f), v] for f, v in proposed.items() if current.get(f) != v}


def is_candidate(spec: DatasetSpec) -> bool:
    """A §1.7 candidate for reviewed=True: public domain / CC with a citation-backed family,
    no gate, not personal data (those wait on the PII policy)."""
    listed = (spec.key in CANDIDATE_KEYS or spec.source in CANDIDATE_SOURCES
              or collector_of(spec) in CANDIDATE_COLLECTORS)
    return (listed and spec.key not in PII_POLICY_KEYS and spec.pii_class != "personal"
            and spec.redistribution in ("open", "attribution") and not spec.rights_gate)


def is_pii_blocked(spec: DatasetSpec) -> bool:
    listed = (spec.key in CANDIDATE_KEYS or spec.source in CANDIDATE_SOURCES
              or collector_of(spec) in CANDIDATE_COLLECTORS or spec.key in PII_POLICY_KEYS)
    return listed and (spec.key in PII_POLICY_KEYS or spec.pii_class == "personal") \
        and spec.redistribution in ("open", "attribution") and not spec.rights_gate


def is_flagged(spec: DatasetSpec) -> bool:
    return bool(spec.rights_gate) or spec.storage == "time_limited"


def _pe_first(specs: Iterable[DatasetSpec]) -> List[DatasetSpec]:
    from app.catalog.dictionary import PE_ENTITY_PACK

    rank = {k: i for i, k in enumerate(PE_ENTITY_PACK)}
    return sorted(specs, key=lambda s: (rank.get(s.key, len(rank)), s.source, s.key))


# ---------------------------------------------------------------------------
# Review store (catalog_rights_review, Alembic 0015)
# ---------------------------------------------------------------------------


def table_ready(engine: Engine) -> bool:
    try:
        with engine.connect() as conn:
            return bool(conn.execute(text("SELECT to_regclass(:t) IS NOT NULL"), {"t": TABLE}).scalar())
    except Exception:
        return False


def _row(r: Any) -> Dict[str, Any]:
    d = dict(r)
    if d.get("reviewed_at") is not None:
        d["reviewed_at"] = d["reviewed_at"].isoformat() + "Z"
    for k in ("rights_hash", "proposal_hash"):
        if d.get(k) is not None:
            d[k] = d[k].strip()
    for k in ("rights_snapshot", "proposal_snapshot"):
        if isinstance(d.get(k), str):
            d[k] = json.loads(d[k])
    return d


def review_history(engine: Engine, key: str, limit: int = 50) -> List[Dict[str, Any]]:
    if not table_ready(engine):
        return []
    with engine.connect() as conn:
        rows = conn.execute(text(
            f"SELECT * FROM {TABLE} WHERE dataset_key = :k ORDER BY reviewed_at DESC, id DESC LIMIT :n"),
            {"k": key, "n": int(limit)}).mappings().all()
    return [_row(r) for r in rows]


def latest_reviews(engine: Optional[Engine]) -> Dict[str, Dict[str, Any]]:
    """dataset_key -> latest decision row. Empty when the DB or the table is unavailable."""
    if engine is None or not table_ready(engine):
        return {}
    try:
        with engine.connect() as conn:
            rows = conn.execute(text(
                f"SELECT DISTINCT ON (dataset_key) id, dataset_key, decision, rights_hash, proposal_hash, "
                f"reviewer, note, reviewed_at FROM {TABLE} "
                f"ORDER BY dataset_key, reviewed_at DESC, id DESC")).mappings().all()
    except Exception as e:
        logger.info(f"[rights] review history unavailable: {type(e).__name__}")
        return {}
    return {r["dataset_key"]: _row(r) for r in rows}


def review_state(spec: DatasetSpec, latest: Optional[Dict[str, Any]]) -> str:
    """none | in_force | awaiting_commit | proposal_accepted | rejected | stale."""
    from app.catalog.rights_reviewed import REVIEWED

    if latest is None:
        return "in_force" if spec.reviewed else "none"
    if latest["rights_hash"] != spec.rights_hash:
        return "stale"
    if latest["decision"] == "confirm_current":
        entry = REVIEWED.get(spec.key)
        return "in_force" if entry and entry[0] == spec.rights_hash else "awaiting_commit"
    if latest["decision"] == "accept_proposal":
        return "proposal_accepted"
    return "rejected"


def reviewer_of(principal: Dict[str, Any]) -> str:
    if principal.get("local_dev"):
        raise ReviewError(403, "a rights sign-off needs a named reviewer: the REQUIRE_AUTH=false "
                               "local-dev principal cannot sign off")
    who = principal.get("email") or principal.get("name")
    if not who:
        raise ReviewError(403, "a rights sign-off needs a named reviewer (no email or name on the principal)")
    return str(who)[:255]


def record_review(engine: Engine, spec: DatasetSpec, decision: str, rights_hash: str, note: str,
                  principal: Dict[str, Any]) -> Dict[str, Any]:
    """Append one decision. Refuses a hash that is not the current block's (409): the reviewer
    saw a different block. Never changes ``reviewed``."""
    if decision not in DECISIONS:
        raise ReviewError(422, f"decision must be one of {list(DECISIONS)}")
    note = (note or "").strip()
    if len(note) < MIN_NOTE:
        raise ReviewError(422, f"a note of at least {MIN_NOTE} characters is required (what was checked)")
    reviewer = reviewer_of(principal)
    if (rights_hash or "").strip() != spec.rights_hash:
        raise ReviewError(409, f"rights_hash does not match the current rights block of {spec.key!r} "
                               f"(current {spec.rights_hash}): reload and review the current block")
    p_hash = p_snap = None
    if decision == "accept_proposal":
        if spec.proposed_rights is None:
            raise ReviewError(422, f"{spec.key!r} has no rights proposal to accept")
        p_hash = proposal_hash(spec)
        p_snap = json.dumps({**spec.proposed_block(), **spec.proposed_rights.to_dict()}, sort_keys=True)
    if not table_ready(engine):
        raise ReviewError(503, f"{TABLE} does not exist: run the Alembic migrations (0015)")
    snapshot = {**spec.rights_block(), "citation_url": spec.citation_url,
                "citation_quote": spec.citation_quote, "confidence": spec.rights_confidence,
                "notes": spec.rights_notes}
    params = {
        "k": spec.key, "d": decision, "h": spec.rights_hash, "snap": json.dumps(snapshot, sort_keys=True),
        "ph": p_hash, "psnap": p_snap, "who": reviewer, "uid": principal.get("user_id"),
        "kid": principal.get("api_key_id"), "note": note,
    }
    with engine.begin() as conn:
        row = conn.execute(text(
            f"INSERT INTO {TABLE} (dataset_key, decision, rights_hash, rights_snapshot, proposal_hash, "
            f"proposal_snapshot, reviewer, reviewer_user_id, api_key_id, note) VALUES "
            f"(:k, :d, :h, CAST(:snap AS jsonb), :ph, CAST(:psnap AS jsonb), :who, :uid, :kid, :note) "
            f"RETURNING *"), params).mappings().one()
    out = _row(row)
    out["review_state"] = review_state(spec, out)
    out["reviewed_now"] = spec.reviewed  # unchanged by this call, by design
    return out


# ---------------------------------------------------------------------------
# Live holdings (storage-forbidden / time-limited / agreement-required)
# ---------------------------------------------------------------------------


def holdings(engine: Optional[Engine], spec: DatasetSpec, deadline: Optional[float] = None) -> Dict[str, Any]:
    """Exact row counts of the dataset's tables (row filters applied), cached 5 min."""
    empty = {"tables": [], "rows_total": None, "rows_exact": False, "measured": False}
    if engine is None or (deadline is not None and time.monotonic() > deadline):
        return empty
    with _holdings_lock:
        hit = _holdings_cache.get(spec.key)
    if hit and time.monotonic() - hit[0] < _HOLDINGS_TTL_S:
        return hit[1]
    from app.catalog.live import count_rows, existing_tables, resolve_tables
    from app.catalog.tables import split

    try:
        existing = existing_tables(engine, {split(t)[0] for t in spec.tables})
        filters = dict(spec.row_filters)
        tables = [count_rows(engine, t, t in existing, where=filters.get(t))
                  for t in resolve_tables(spec, existing)]
    except Exception as e:
        logger.info(f"[rights] holdings of {spec.key} unavailable: {type(e).__name__}")
        return empty
    out = {
        "tables": [{"table": t["table"], "exists": t["exists"], "rows": t["rows"],
                    "rows_exact": t["rows_exact"]} for t in tables],
        "rows_total": sum(t["rows"] or 0 for t in tables),
        "rows_exact": all(t["rows_exact"] for t in tables if t["exists"]),
        "measured": True,
    }
    with _holdings_lock:
        _holdings_cache[spec.key] = (time.monotonic(), out)
    return out


def clear_holdings_cache() -> None:
    with _holdings_lock:
        _holdings_cache.clear()


def _gate_effect(spec: DatasetSpec) -> Dict[str, Any]:
    from app.core.rights_guard import ENFORCED_PATHS

    gated = bool(spec.rights_gate)
    return {
        "sample_non_admin": "refused (403)" if gated or spec.redistribution == "restricted" else "served, masked",
        "sample_admin": "served, flagged (rights_gate + X-Dataset-Rights-Gate)" if gated else "served",
        "schema_examples": ("withheld from everyone" if "storage_forbidden" in spec.rights_gate
                            or "commercial_use_forbidden" in spec.rights_gate
                            else "withheld from non-admins" if gated else "per redistribution"),
        "export": ("admin-only router; export_allowed refuses non-admins; admin jobs and the table "
                   "list / preview carry rights_gate") if gated else "admin-only (unchanged)",
        "source_api": ("non-admins refused (403), admins flagged (X-Dataset-Rights-Gate) on: "
                       + ", ".join(ENFORCED_PATHS[spec.key])) if gated and spec.key in ENFORCED_PATHS
        else ("no source read API guarded; see serving_paths.unenforced" if gated else "unchanged"),
        "time_limited": (f"rows may be held {spec.storage_max_age_days} days; age of held rows is not "
                         "measured and nothing expires them (flag only)")
        if spec.storage == "time_limited" else None,
        "effective_redistribution": spec.effective_redistribution,
    }


def _serving_paths() -> Dict[str, Any]:
    """Where the rights gate is enforced, and the known paths where it is not."""
    from app.core.rights_guard import ENFORCED_PATHS, UNENFORCED_PATHS

    return {
        "enforced": {"catalog": ["/api/v1/catalog/{key}/sample", "/api/v1/catalog/{key}/schema (examples)",
                                 "/api/v1/export/* (admin-only, flagged)"],
                     **ENFORCED_PATHS},
        "unenforced": UNENFORCED_PATHS,
    }


# ---------------------------------------------------------------------------
# Queue
# ---------------------------------------------------------------------------


def _item(spec: DatasetSpec, latest: Optional[Dict[str, Any]], why: str) -> Dict[str, Any]:
    current = spec.rights_block()
    proposed = spec.proposed_block()
    return {
        "key": spec.key,
        "display_name": spec.display_name,
        "source": spec.source,
        "collector": collector_of(spec),
        "status_public": spec.status_public,
        "data_state": spec.data_state,
        "why": why,
        "rights": spec.rights_dict(),
        "rights_hash": spec.rights_hash,
        "proposed_block": proposed,
        "proposal_hash": proposal_hash(spec),
        "diff": rights_diff(current, proposed),
        "latest_review": latest,
        "review_state": review_state(spec, latest),
    }


def build_queue(engine: Optional[Engine], live: bool = True,
                specs: Optional[Sequence[DatasetSpec]] = None) -> Dict[str, Any]:
    """The review queue, sectioned. Nothing here changes any state."""
    specs = list(specs if specs is not None else _catalog())
    latest = latest_reviews(engine)
    deadline = time.monotonic() + _HOLDINGS_DEADLINE_S
    pending = [s for s in specs if review_state(s, latest.get(s.key)) not in ("in_force",)]

    proposals = [_item(s, latest.get(s.key), f"{s.proposed_rights.change}: {s.proposed_rights.reason}")
                 for s in _pe_first(pending) if s.proposed_rights is not None]
    candidates = [_item(s, latest.get(s.key), "PLAN_088 §1.7 candidate for reviewed=True")
                  for s in _pe_first(pending) if is_candidate(s)]
    pii_blocked = [_item(s, latest.get(s.key), "public domain but personal data: waits on the PII policy")
                   for s in _pe_first(pending) if is_pii_blocked(s)]
    flags = []
    for s in _pe_first(specs):
        if not is_flagged(s):
            continue
        it = _item(s, latest.get(s.key), "the source's terms limit holding or using this data "
                                         "(flag only, PLAN_088 decision 3)")
        it["gate"] = list(s.rights_gate)
        it["holdings"] = holdings(engine if live else None, s, deadline)
        it["gating_effect"] = _gate_effect(s)
        flags.append(it)
    by_state: Dict[str, List[Dict[str, Any]]] = {"awaiting_commit": [], "stale": []}
    for s in specs:
        st = review_state(s, latest.get(s.key))
        if st in by_state:
            by_state[st].append(_item(s, latest.get(s.key), st))
    return {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "review_table": engine is not None and table_ready(engine),
        "policy": ("reviewed = committed rights_reviewed.REVIEWED hash matches the current block; "
                   "a recorded decision never flips anything (SPEC_142, PLAN_088 decision 8)"),
        "counts": {"proposals": len(proposals), "candidates": len(candidates), "storage_flags": len(flags),
                   "pii_blocked": len(pii_blocked), "awaiting_commit": len(by_state["awaiting_commit"]),
                   "stale": len(by_state["stale"])},
        "proposals": proposals,
        "candidates": candidates,
        "storage_flags": flags,
        "pii_blocked": pii_blocked,
        **by_state,
    }


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------


def build_report(engine: Optional[Engine], live: bool = True,
                 specs: Optional[Sequence[DatasetSpec]] = None) -> Dict[str, Any]:
    specs = list(specs if specs is not None else _catalog())
    latest = latest_reviews(engine)
    deadline = time.monotonic() + _HOLDINGS_DEADLINE_S
    families: Dict[str, List[Dict[str, Any]]] = {}
    for s in sorted(specs, key=lambda x: (x.source, x.key)):
        fam = s.source if s.source != "site_intel" else f"site_intel:{collector_of(s)}"
        families.setdefault(fam, []).append({
            "key": s.key,
            "current": {**s.rights_block(), "effective_redistribution": s.effective_redistribution},
            "proposed": s.proposed_rights.to_dict() if s.proposed_rights else None,
            "diff": rights_diff(s.rights_block(), s.proposed_block()),
            "citation_url": s.citation_url,
            "citation_quote": s.citation_quote,
            "confidence": s.rights_confidence,
            "notes": s.rights_notes,
            "gate": list(s.rights_gate),
            "reviewed": s.reviewed,
            "review_state": review_state(s, latest.get(s.key)),
            "candidate": is_candidate(s),
        })
    holdings_rows = []
    for s in _pe_first(specs):
        if not is_flagged(s):
            continue
        h = holdings(engine if live else None, s, deadline)
        holdings_rows.append({
            "key": s.key, "source": s.source, "collector": collector_of(s), "storage": s.storage,
            "storage_max_age_days": s.storage_max_age_days, "commercial_use": s.commercial_use,
            "gate": list(s.rights_gate), "citation_url": s.citation_url, "rows_total": h["rows_total"],
            "rows_exact": h["rows_exact"], "measured": h["measured"], "tables": h["tables"],
            "gating_effect": _gate_effect(s),
        })

    def count(attr: str) -> Dict[str, int]:
        return dict(sorted(Counter(str(getattr(s, attr)) for s in specs).items()))

    return {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "evidence": EVIDENCE_PATH,
        "live": bool(live and engine is not None),
        "review_table": engine is not None and table_ready(engine),
        "summary": {
            "datasets": len(specs),
            "reviewed": sum(1 for s in specs if s.reviewed),
            "by_redistribution": count("redistribution"),
            "by_effective_redistribution": count("effective_redistribution"),
            "by_storage": count("storage"),
            "by_commercial_use": count("commercial_use"),
            "with_citation": sum(1 for s in specs if s.citation_url),
            "proposals": sum(1 for s in specs if s.proposed_rights),
            "gated": sum(1 for s in specs if s.rights_gate),
            "candidates": sum(1 for s in specs if is_candidate(s)),
            "pii_blocked": sum(1 for s in specs if is_pii_blocked(s)),
            "review_states": dict(Counter(review_state(s, latest.get(s.key)) for s in specs)),
        },
        "tightenings": TIGHTENINGS,
        "serving_paths": _serving_paths(),
        "storage_holdings": holdings_rows,
        "families": families,
    }


def _md_cell(v: Any) -> str:
    s = "" if v is None else str(v)
    return s.replace("|", "\\|").replace("\n", " ")


def render_markdown(report: Dict[str, Any]) -> str:
    sm = report["summary"]
    out = [
        "# Catalog rights report (SPEC_142)",
        "",
        f"Generated {report['generated_at']}. Evidence: `{report['evidence']}`. "
        f"Live counts: {'yes' if report['live'] else 'no'}.",
        "",
        "Rule: tightenings with a citation are applied; loosenings are proposals only; "
        "`reviewed` comes only from a committed sign-off hash.",
        "",
        "## Summary",
        "",
        f"- Datasets: {sm['datasets']}; reviewed: {sm['reviewed']}; with a citation: {sm['with_citation']}",
        f"- Redistribution: {sm['by_redistribution']}",
        f"- Effective: {sm['by_effective_redistribution']}",
        f"- Storage: {sm['by_storage']}",
        f"- Commercial use: {sm['by_commercial_use']}",
        f"- Gated (samples refused to non-admins): {sm['gated']}; proposals: {sm['proposals']}; "
        f"review candidates: {sm['candidates']}; waiting on PII policy: {sm['pii_blocked']}",
        f"- Review states: {sm['review_states']}",
        "",
        "## Tightenings applied (SPEC_142)",
        "",
        "| Entry | Change |",
        "|---|---|",
    ]
    out += [f"| `{k}` | {_md_cell(v)} |" for k, v in report["tightenings"].items()]
    out += ["", "## Holdings the source terms limit (flagged, not deleted)", "",
            "| Dataset | Storage | Commercial use | Gate | Rows | Export / sample effect | Citation |",
            "|---|---|---|---|---|---|---|"]
    for h in report["storage_holdings"]:
        storage = h["storage"] if h["storage"] != "time_limited" else f"time_limited ({h['storage_max_age_days']} d)"
        rows = "n/a" if not h["measured"] else f"{h['rows_total']:,}" + ("" if h["rows_exact"] else " (est.)")
        eff = h["gating_effect"]
        out.append(f"| `{h['key']}` | {_md_cell(storage)} | {_md_cell(h['commercial_use'])} | "
                   f"{_md_cell(', '.join(h['gate']) or '-')} | {rows} | "
                   f"sample: {_md_cell(eff['sample_non_admin'])} / admin {_md_cell(eff['sample_admin'])}; "
                   f"export: {_md_cell(eff['export'])} | {_md_cell(h['citation_url'])} |")
    sp = report.get("serving_paths") or {}
    out += ["", "## Where the gate is enforced", ""]
    for k, paths in (sp.get("enforced") or {}).items():
        out.append(f"- `{k}`: {_md_cell(', '.join(paths))}")
    out += ["", "Known paths that still read gated tables for any signed-in user (not guarded):", ""]
    for u in sp.get("unenforced") or []:
        out.append(f"- {_md_cell(u['paths'])}: {_md_cell(u['reads'])}")
    out += ["", "## Proposals (not applied)", ""]
    for fam, rows in report["families"].items():
        for r in rows:
            if r["proposed"]:
                p = r["proposed"]
                out.append(f"- `{r['key']}` ({p['change']}, confidence {p['confidence']}): {p['reason']} "
                           f"Changes: {json.dumps(r['diff'], ensure_ascii=False)}. Citation: {p['citation_url']} "
                           f"— \"{p['citation_quote']}\"")
    out += ["", "## Per source: current block and citation", "",
            "| Family | Dataset | Redistribution | Storage | Commercial use | Licence | Citation (confidence) | Review |",
            "|---|---|---|---|---|---|---|---|"]
    for fam, rows in report["families"].items():
        for r in rows:
            c = r["current"]
            cite = f"{r['citation_url']} ({r['confidence']})" if r["citation_url"] else "-"
            out.append(f"| {_md_cell(fam)} | `{r['key']}` | {_md_cell(c['redistribution'])} | "
                       f"{_md_cell(c['storage'] or '-')} | {_md_cell(c['commercial_use'] or '-')} | "
                       f"{_md_cell(c['license'])} | {_md_cell(cite)} | {_md_cell(r['review_state'])} |")
    return "\n".join(out) + "\n"


# ---------------------------------------------------------------------------
# --emit: the committed sign-off file
# ---------------------------------------------------------------------------


def reviewed_entries(engine: Engine, specs: Optional[Sequence[DatasetSpec]] = None) -> Dict[str, Tuple[str, int]]:
    """key -> (rights_hash, review_id) for the latest decision per key when it is a
    confirm_current whose hash matches the current code."""
    by_key = {s.key: s for s in (specs if specs is not None else _catalog())}
    out: Dict[str, Tuple[str, int]] = {}
    for key, row in latest_reviews(engine).items():
        spec = by_key.get(key)
        if spec is not None and row["decision"] == "confirm_current" and row["rights_hash"] == spec.rights_hash:
            out[key] = (spec.rights_hash, int(row["id"]))
    return dict(sorted(out.items()))


def render_reviewed_module(entries: Dict[str, Tuple[str, int]]) -> str:
    from app.catalog import rights_reviewed

    doc = (rights_reviewed.__doc__ or "").strip("\n")
    lines = ['"""', doc, '"""', "", "from __future__ import annotations", "",
             "from dataclasses import replace", "from typing import Dict, Tuple", ""]
    if entries:
        lines.append("REVIEWED: Dict[str, Tuple[str, int]] = {")
        lines += [f"    {k!r}: ({h!r}, {rid})," for k, (h, rid) in entries.items()]
        lines.append("}")
    else:
        lines.append("REVIEWED: Dict[str, Tuple[str, int]] = {}")
    lines += ["", "", "def apply_review(spec):",
              '    """``spec`` with ``reviewed`` set from the committed sign-offs (hash must match)."""',
              "    entry = REVIEWED.get(spec.key)",
              "    reviewed = bool(entry) and entry[0] == spec.rights_hash",
              "    return spec if spec.reviewed == reviewed else replace(spec, reviewed=reviewed)", ""]
    return "\n".join(lines)


def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Catalog rights review (SPEC_142)")
    ap.add_argument("--emit", action="store_true",
                    help="print rights_reviewed.py from the recorded confirmations (review, then commit)")
    ap.add_argument("--out", help="with --emit: write here instead of stdout (rendered before the "
                                  "file is opened, so it can be app/catalog/rights_reviewed.py itself)")
    ap.add_argument("--report", choices=("md", "json"), help="print the rights report")
    ap.add_argument("--no-live", action="store_true", help="report without live row counts")
    args = ap.parse_args(argv)
    from app.core.database import get_engine

    engine = get_engine()
    if args.emit:
        body = render_reviewed_module(reviewed_entries(engine))
        if args.out:
            with open(args.out, "w", encoding="utf-8", newline="\n") as f:
                f.write(body)
        else:
            sys.stdout.write(body)
        return 0
    if args.report:
        rep = build_report(engine, live=not args.no_live)
        sys.stdout.write(render_markdown(rep) if args.report == "md" else json.dumps(rep, indent=1, default=str))
        return 0
    ap.print_help()
    return 2


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
