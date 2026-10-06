"""
ATS board discovery over the PE targets universe (SPEC_155).

For each kept target (``workbench.targets_universe``, ``thesis_excluded = false``) try a few
name-derived board tokens on Greenhouse and Lever, verify every board found with rule R7
(``match.py``), and:

- VERIFIED  -> ``ats_board`` row ``status='active'``, ``verification='verified'``, linked to the
  firm (``core_entity_id``, ``cik``, ``target_id``), postings stored (SPEC_151 merge), one
  ``ats_board_fetch`` row; the SPEC_153 weekly refresh picks it up from then on;
- CANDIDATE -> ``ats_board`` row ``status='candidate'`` with NO firm link (the proposed firm is in
  ``discovery_evidence.proposed``): a review queue, never refreshed, never on a firm page;
- REJECTED  -> the attempt ledger only.

Every (target, site, token) asked is a row of ``ats_discovery_attempt``: a re-run skips final
pairs (fetched / not_found / a 4xx) and retries only transient ones (timeouts, 5xx, Retry-After),
so a stopped run resumes where it left off and never skips anything. One thread per host, each
with its own ``open_web.OpenWebFetcher`` (terms registry, robots.txt on every hop, the honest
NexdataResearch user agent, Retry-After, >= max(Crawl-delay, 2 s) between requests to the host)
and its own DB session. A host stops for the run on any 429 / Retry-After, after 3 consecutive
transient errors, on any refusal (robots / terms), at its request budget, or when the stop file
appears. Progress (targets done, requests, outcomes, verdicts, reasons, postings, pay) is written
to ``ats_discovery_run`` every 25 targets.

    python -m app.sources.ats_boards.targets --limit 200                     # DRY RUN, writes nothing
    python -m app.sources.ats_boards.targets --target-ids-file ids.txt --json out.json
    python -m app.sources.ats_boards.targets --apply --link-workbench        # all kept targets
    python -m app.sources.ats_boards.targets --apply --resume-run 3          # continue run 3

SPEC_156 (rule R8, after the 2026-10-06 review of run 1):

    python -m app.sources.ats_boards.targets --retract --board-ids 178,465 --reason review_false_link [--apply]
    python -m app.sources.ats_boards.targets --reverify [--board-ids ...] [--apply]   # re-fetch + R8 every link
    python -m app.sources.ats_boards.targets --reparse-pay [--apply]

A retracted board is ``status='rejected'`` with its previous link and the reason kept under
``discovery_evidence.retracted``; its postings become ``retracted`` (kept, never read as roles).
"""

from __future__ import annotations

import argparse
import json
import os
import threading
import time
from collections import Counter, OrderedDict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Sequence, Set, Tuple

from sqlalchemy import text

from app.core import open_web
from app.sources.ats_boards import adapters, collect, match
from app.sources.ats_boards.match import Target

SITES = ("greenhouse", "lever")
CHUNK = 25                              # progress flush / checkpoint every 25 targets
MAX_CONSECUTIVE_ERRORS = 3
BOARD_CACHE = 64                        # fetched boards kept per site for re-use within a run
STOP_FILE = "/tmp/ats_discovery.stop"
STOP_OUTCOMES = ("retry_after", "host_backed_off")
TRANSIENT_OUTCOMES = ("error", "retry_after", "host_backed_off", "dns_error", "too_many_redirects",
                      "truncated", "not_json")


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def is_final(outcome: str, http_status: Optional[int]) -> bool:
    """A ledger pair that is never asked again."""
    if outcome in ("fetched", "not_found"):
        return True
    return outcome == "http_error" and http_status is not None and http_status < 500


def _transient(outcome: str, http_status: Optional[int]) -> bool:
    return outcome in TRANSIENT_OUTCOMES or (outcome == "http_error" and (http_status or 500) >= 500)


@dataclass
class Prior:
    final_pairs: Set[Tuple[int, str, str]] = field(default_factory=set)
    not_found: Dict[str, Set[str]] = field(default_factory=lambda: {s: set() for s in SITES})
    verified_sites: Set[Tuple[int, str]] = field(default_factory=set)     # (target_id, ats) already verified


def load_prior(db) -> Prior:
    p = Prior()
    for tid, ats, token, outcome, status, verdict in db.execute(text(
            "SELECT target_id, ats_type, board_token, outcome, http_status, verdict FROM ats_discovery_attempt")):
        if verdict == "verified":
            p.verified_sites.add((int(tid), ats))
        if is_final(outcome, status):
            p.final_pairs.add((int(tid), ats, token))
        if outcome == "not_found":
            p.not_found.setdefault(ats, set()).add(token)
    return p


# ---------------------------------------------------------------------------
# targets
# ---------------------------------------------------------------------------

_TARGETS_SQL = ("SELECT target_id, cik, ein, core_entity_id, name, city, state, archetype "
                "FROM workbench.targets_universe WHERE NOT thesis_excluded")


def load_targets(db, target_ids: Optional[Sequence[int]] = None, limit: Optional[int] = None,
                 after: Optional[int] = None, skip_linked: bool = True,
                 kept_only: bool = True) -> Tuple[List[Target], Dict[str, int]]:
    """Kept targets in target_id order with every name NexData holds for them (``kept_only=False``:
    thesis-excluded ones too, for re-verifying links made while they were kept)."""
    sql, params = _TARGETS_SQL, {}
    if not kept_only:
        sql = sql.replace(" WHERE NOT thesis_excluded", " WHERE TRUE")
    if target_ids is not None:
        sql += " AND target_id = ANY(:ids)"
        params["ids"] = [int(x) for x in target_ids]
    if after is not None:
        sql += " AND target_id > :after"
        params["after"] = int(after)
    sql += " ORDER BY target_id"
    if limit is not None:
        sql += " LIMIT :limit"
        params["limit"] = int(limit)
    rows = db.execute(text(sql), params).fetchall()
    stats = {"selected": len(rows), "skipped_already_linked": 0}
    linked_e, linked_c, linked_t = set(), set(), set()
    if skip_linked:
        # boards linked BEFORE target discovery (SPEC_151 / 152 seeds): a target this discovery verified
        # carries target_id and is not skipped, so a resumed run still tries its other site (the ledger
        # and Prior.verified_sites keep it from re-asking anything)
        for e, c, t in db.execute(text("SELECT core_entity_id, cik, target_id FROM ats_board "
                                       "WHERE status = 'active' AND target_id IS NULL")):
            if e:
                linked_e.add(int(e))
            if c and str(c).isdigit():
                linked_c.add(str(int(c)))
            if t:
                linked_t.add(int(t))
    out: List[Target] = []
    for tid, cik, ein, eid, name, city, state, arch in rows:
        cik_s = str(int(cik)) if cik is not None else None
        if (eid and int(eid) in linked_e) or (cik_s and cik_s in linked_c) or int(tid) in linked_t:
            stats["skipped_already_linked"] += 1
            continue
        out.append(Target(target_id=int(tid), name=name, city=city, state=state, cik=cik_s, ein=ein,
                          core_entity_id=int(eid) if eid else None, archetype=arch))
    _attach_names(db, out)
    stats["name_df_tokens"] = attach_name_df(db, out)
    stats["targets"] = len(out)
    return out, stats


def name_corpus_df(db, tokens: Set[str]) -> Optional[Dict[str, int]]:
    """Document frequency of each one-word name in NexData's own name corpus: distinct normalized
    (``match.mnorm``) names among core.entity canonical names and Form D issuer names that contain the
    token as a word. None when the corpus tables are absent (R8 then treats every one-word name as
    ambiguous)."""
    if not db.execute(text("SELECT to_regclass('core.entity') IS NOT NULL")).scalar():
        return None
    sqls = ["SELECT DISTINCT canonical_name FROM core.entity WHERE canonical_name IS NOT NULL"]
    if db.execute(text("SELECT to_regclass('public.form_d_issuers') IS NOT NULL")).scalar():
        sqls.append("SELECT DISTINCT entity_name FROM form_d_issuers WHERE entity_name IS NOT NULL")
    names: Set[str] = set()
    for q in sqls:
        for (nm,) in db.execute(text(q)):
            n = match.mnorm(nm)
            if n:
                names.add(n)
    df = Counter()
    for n in names:
        for tok in set(n.split()) & tokens:
            df[tok] += 1
    return {t: int(df.get(t, 0)) for t in tokens}


def attach_name_df(db, targets: Sequence[Target]) -> int:
    """Fill ``Target.name_df`` for the targets' one-word names (rule R8). Returns the token count."""
    need: Set[str] = set()
    for t in targets:
        need |= match.name_tokens(t)
    if not need:
        for t in targets:
            t.name_df = {}
        return 0
    df = name_corpus_df(db, need)
    if df is None:
        return 0
    for t in targets:
        t.name_df = {k: df.get(k, 0) for k in match.name_tokens(t)}
    return len(need)


def _attach_names(db, targets: List[Target]) -> None:
    by_e: Dict[int, List[Target]] = {}
    by_c: Dict[str, List[Target]] = {}
    for t in targets:
        if t.core_entity_id:
            by_e.setdefault(t.core_entity_id, []).append(t)
        if t.cik:
            by_c.setdefault(t.cik, []).append(t)
    if by_e:
        ids = list(by_e)
        for eid, nm in db.execute(text("SELECT entity_id, canonical_name FROM core.entity WHERE entity_id = ANY(:ids)"),
                                  {"ids": ids}):
            for t in by_e.get(int(eid), []):
                if nm and nm != t.name and nm not in t.aliases:
                    t.aliases.append(nm)
        for eid, nm in db.execute(text("SELECT DISTINCT entity_id, name FROM core.alias "
                                       "WHERE entity_id = ANY(:ids) AND name IS NOT NULL ORDER BY 1, 2"), {"ids": ids}):
            for t in by_e.get(int(eid), []):
                if nm != t.name and nm not in t.aliases:
                    t.aliases.append(nm)
    if by_c:
        ks = list(by_c)
        # form_d_issuers.cik is zero-padded text: joins need ltrim (measured 2026-10-05)
        for k, pn, en in db.execute(text(
                "SELECT ltrim(cik, '0'), issuer_previous_names, edgar_previous_names FROM form_d_issuers "
                "WHERE ltrim(cik, '0') = ANY(:ks)"), {"ks": ks}):
            for t in by_c.get(k, []):
                for v in list(pn or []) + list(en or []):
                    if v and v not in t.previous_names:
                        t.previous_names.append(v)
        for k, fn, ln in db.execute(text(
                "SELECT DISTINCT ltrim(i.cik, '0'), rp.first_name, rp.last_name FROM form_d_related_persons rp "
                "JOIN form_d_issuers i ON i.accession_number = rp.accession_number "
                "WHERE ltrim(i.cik, '0') = ANY(:ks)"), {"ks": ks}):
            nm = " ".join(x for x in (fn, ln) if x)
            for t in by_c.get(k, []):
                if nm and len(t.persons) < 40 and nm not in t.persons:
                    t.persons.append(nm)


# ---------------------------------------------------------------------------
# storage
# ---------------------------------------------------------------------------

_ATTEMPT_UPSERT = text("""
    INSERT INTO ats_discovery_attempt (run_id, target_id, core_entity_id, ats_type, board_token, variant, outcome,
                                       http_status, requests, verdict, reason, score, board_id, postings, evidence,
                                       error, attempted_at)
    VALUES (:run_id, :target_id, :eid, :ats, :token, :variant, :outcome, :http_status, :requests, :verdict, :reason,
            :score, :board_id, :postings, CAST(:evidence AS JSONB), :error, :at)
    ON CONFLICT (target_id, ats_type, board_token) DO UPDATE SET
        run_id = EXCLUDED.run_id, variant = EXCLUDED.variant, outcome = EXCLUDED.outcome,
        http_status = EXCLUDED.http_status, requests = ats_discovery_attempt.requests + EXCLUDED.requests,
        verdict = EXCLUDED.verdict, reason = EXCLUDED.reason, score = EXCLUDED.score, board_id = EXCLUDED.board_id,
        postings = EXCLUDED.postings, evidence = EXCLUDED.evidence, error = EXCLUDED.error,
        attempted_at = EXCLUDED.attempted_at
""")

_BOARD_ROW = text("SELECT id, status, core_entity_id, cik, target_id, industrial_company_id, discovery_evidence, "
                  "company_name FROM ats_board WHERE ats_type = :ats AND board_token = :token FOR UPDATE")

_VERIFIED_UPSERT = text("""
    INSERT INTO ats_board (ats_type, board_token, company_name, core_entity_id, cik, target_id, ein, discovery_basis,
                           discovery_evidence, status, verification, verification_score, verified_at, careers_domain,
                           careers_domain_evidence, terms_citation, last_fetched_at, last_outcome, updated_at)
    VALUES (:ats, :token, :name, :eid, :cik, :tid, :ein, 'name_slug', CAST(:evidence AS JSONB), 'active', 'verified',
            :score, :at, :dom, CAST(:dom_ev AS JSONB), :citation, :at, 'fetched', NOW())
    ON CONFLICT (ats_type, board_token) DO UPDATE SET
        company_name = EXCLUDED.company_name, core_entity_id = EXCLUDED.core_entity_id, cik = EXCLUDED.cik,
        target_id = EXCLUDED.target_id, ein = EXCLUDED.ein, discovery_basis = EXCLUDED.discovery_basis,
        discovery_evidence = EXCLUDED.discovery_evidence, status = 'active', verification = 'verified',
        verification_score = EXCLUDED.verification_score, verified_at = EXCLUDED.verified_at,
        careers_domain = EXCLUDED.careers_domain, careers_domain_evidence = EXCLUDED.careers_domain_evidence,
        terms_citation = EXCLUDED.terms_citation, last_fetched_at = EXCLUDED.last_fetched_at,
        last_outcome = 'fetched', updated_at = NOW()
    RETURNING id
""")

# a board the lane already holds as active for THIS firm (e.g. a SPEC_151 seed): add the target link and
# the verification, keep its basis / evidence
_VERIFIED_SAME_FIRM = text("""
    UPDATE ats_board SET target_id = COALESCE(target_id, :tid), core_entity_id = COALESCE(core_entity_id, :eid),
        cik = COALESCE(cik, :cik), ein = COALESCE(ein, :ein), verification = COALESCE(verification, 'verified'),
        verification_score = COALESCE(verification_score, :score), verified_at = COALESCE(verified_at, :at),
        careers_domain = COALESCE(careers_domain, :dom),
        careers_domain_evidence = COALESCE(careers_domain_evidence, CAST(:dom_ev AS JSONB)), updated_at = NOW()
    WHERE id = :id
""")

_CANDIDATE_INSERT = text("""
    INSERT INTO ats_board (ats_type, board_token, company_name, discovery_basis, discovery_evidence, status,
                           verification, verification_score, terms_citation, last_fetched_at, last_outcome, updated_at)
    VALUES (:ats, :token, :name, 'name_slug', CAST(:evidence AS JSONB), 'candidate', 'candidate', :score, :citation,
            :at, 'fetched', NOW())
    RETURNING id
""")

_CANDIDATE_UPDATE = text("""
    UPDATE ats_board SET company_name = :name, core_entity_id = NULL, cik = NULL, target_id = NULL,
        industrial_company_id = NULL, discovery_basis = 'name_slug', discovery_evidence = CAST(:evidence AS JSONB),
        status = 'candidate', verification = 'candidate',
        verification_score = GREATEST(COALESCE(verification_score, 0), :score),
        terms_citation = COALESCE(:citation, terms_citation), last_fetched_at = :at, last_outcome = 'fetched',
        updated_at = NOW()
    WHERE id = :id
""")


def _same_firm(row, t: Target) -> bool:
    _id, _st, eid, cik, tid, _iid, _ev, name = row[:8]
    if (t.core_entity_id and eid and int(eid) == t.core_entity_id) or (t.cik and cik and str(cik) == t.cik) \
            or (tid and int(tid) == t.target_id):
        return True
    # SPEC_156 (Cockroach Labs, board 18): an active seed that carries NO firm key at all belongs to the firm
    # whose name it was seeded under -- the verified firm claims it when that name is one of the firm's names
    # (an industrial_companies id is a separate dataset's link, not a firm key: board 18 carries id 190
    # "Cockroach Labs")
    if not (eid or cik or tid) and name:
        return match.mnorm(name) in set(match.firm_names(t)[0])
    return False


def link_verified(db, ats: str, token: str, t: Target, v: match.Verdict, board: Dict[str, Any],
                  domain: Optional[Dict[str, Any]], variant: str, run_id: Optional[int]) -> Tuple[str, str, Optional[int]]:
    """-> (verdict, reason, board_id). Never relinks a board active for another firm."""
    row = db.execute(_BOARD_ROW, {"ats": ats, "token": token}).fetchone()
    dom_ev = json.dumps(domain, default=str) if domain else None
    common = {"tid": t.target_id, "eid": t.core_entity_id, "cik": t.cik, "ein": t.ein, "score": v.score,
              "at": board["fetched_at"], "dom": domain["domain"] if domain else None, "dom_ev": dom_ev}
    if row is not None and row[1] == "rejected":
        return "candidate", "board_rejected", row[0]      # SPEC_156: a retracted board is never relinked
    if row is not None and row[1] in ("active", "refused"):
        if row[1] == "refused" or not _same_firm(row, t):
            return "candidate", ("board_refused" if row[1] == "refused" else "board_linked_to_other_firm"), row[0]
        db.execute(_VERIFIED_SAME_FIRM, {**common, "id": row[0]})
        return "verified", v.reason, row[0]
    evidence = {**v.evidence, "variant": variant, "target_id": t.target_id, "run_id": run_id,
                "board_url": board["url"]}
    board_id = db.execute(_VERIFIED_UPSERT, {
        **common, "ats": ats, "token": token, "name": t.name, "evidence": json.dumps(evidence, default=str),
        "citation": board.get("terms_citation")}).scalar()
    return "verified", v.reason, board_id


def record_candidate(db, ats: str, token: str, t: Target, v: match.Verdict, reason: str, board: Dict[str, Any],
                     variant: str, run_id: Optional[int]) -> Optional[int]:
    """A board that names the firm weakly: kept for review with NO firm link."""
    row = db.execute(_BOARD_ROW, {"ats": ats, "token": token}).fetchone()
    if row is not None and row[1] in ("active", "refused", "rejected"):
        return row[0]                       # never downgrade a live, refused or retracted board
    proposal = {"target_id": t.target_id, "name": t.name, "core_entity_id": t.core_entity_id, "cik": t.cik,
                "reason": reason, "score": v.score, "variant": variant, "run_id": run_id,
                "name_share": v.evidence.get("name_share"), "canon_share": v.evidence.get("canon_share"),
                "location": v.evidence.get("location")}
    name = (board.get("meta") or {}).get("name") or token
    params = {"name": name, "score": v.score, "citation": board.get("terms_citation"), "at": board["fetched_at"]}
    if row is None:
        ev = {"rule_version": match.RULE_VERSION, "proposed": [proposal], "board_url": board["url"]}
        return db.execute(_CANDIDATE_INSERT, {**params, "ats": ats, "token": token,
                                              "evidence": json.dumps(ev, default=str)}).scalar()
    old = row[6] if (row[1] == "candidate" and isinstance(row[6], dict)) else {}
    props = [p for p in old.get("proposed") or [] if p.get("target_id") != t.target_id] + [proposal]
    ev = {"rule_version": match.RULE_VERSION, "proposed": props[-20:], "board_url": board["url"]}
    db.execute(_CANDIDATE_UPDATE, {**params, "id": row[0], "evidence": json.dumps(ev, default=str)})
    return row[0]


def readjudicate(db, apply: bool = False) -> Dict[str, Any]:
    """Re-decide every discovery-verified link from its STORED evidence under the current rule
    (``match.decide``; ``canon_dropped_words`` recomputed from the firm name). A link the current rule
    no longer verifies becomes an unlinked ``candidate`` (its attempt row too); nothing is fetched.
    Dry run (default) only lists what would change."""
    rows = db.execute(text(
        "SELECT id, ats_type, board_token, company_name, target_id, core_entity_id, cik, discovery_evidence, "
        "verification_score FROM ats_board WHERE verification = 'verified' AND status = 'active' "
        "AND discovery_evidence ? 'rule_version' ORDER BY id")).fetchall()
    demoted = []
    for bid, ats, token, name, tid, eid, cik, ev, sc in rows:
        f = dict(ev or {})
        if "canon_dropped_words" not in f:
            f["canon_dropped_words"] = match.canon_dropped(name)
        verdict, reason, _rule = match.decide(f)
        if verdict == "verified":
            continue
        demoted.append({"board_id": bid, "token": token, "ats": ats, "name": name,
                        "target_id": tid, "reason": reason, "old_rule": f.get("rule"),
                        "old_rule_version": f.get("rule_version")})
        if not apply:
            continue
        proposal = {"target_id": tid, "name": name, "core_entity_id": eid, "cik": cik, "reason": reason,
                    "score": float(sc) if sc is not None else None, "variant": f.get("variant"),
                    "demoted_from": f.get("rule"), "demoted_rule_version": match.RULE_VERSION}
        new_ev = {"rule_version": match.RULE_VERSION, "proposed": [proposal], "board_url": f.get("board_url"),
                  "previous_evidence": f}
        db.execute(_CANDIDATE_UPDATE, {"id": bid, "name": name, "evidence": json.dumps(new_ev, default=str),
                                       "score": float(sc or 0), "citation": None, "at": _now()})
        db.execute(text("UPDATE ats_board SET last_outcome = 'demoted' WHERE id = :i"), {"i": bid})
        db.execute(text("UPDATE ats_discovery_attempt SET verdict = 'candidate', reason = :r "
                        "WHERE ats_type = :a AND board_token = :t AND target_id = :tid AND verdict = 'verified'"),
                   {"r": reason, "a": ats, "t": token, "tid": tid})
        if tid is not None and db.execute(text("SELECT to_regclass('workbench.targets_universe') IS NOT NULL")).scalar():
            db.execute(text("UPDATE workbench.targets_universe SET ats_board_id = NULL "
                            "WHERE target_id = :tid AND ats_board_id = :b"), {"tid": tid, "b": bid})
    if apply:
        db.commit()
    return {"checked": len(rows), "demoted": demoted, "apply": apply, "rule_version": match.RULE_VERSION}


# ---------------------------------------------------------------------------
# SPEC_156: retraction and re-verification
# ---------------------------------------------------------------------------

_RETRACT_BOARD = text("""
    UPDATE ats_board SET status = 'rejected', verification = 'rejected', core_entity_id = NULL, cik = NULL,
        target_id = NULL, ein = NULL, industrial_company_id = NULL, careers_domain = NULL,
        careers_domain_evidence = NULL, last_outcome = 'retracted', updated_at = NOW(),
        discovery_evidence = COALESCE(discovery_evidence, '{}'::jsonb)
                             || jsonb_build_object('retracted', CAST(:r AS JSONB))
    WHERE id = :id AND status <> 'rejected'
""")


def _workbench_present(db) -> bool:
    return bool(db.execute(text("SELECT to_regclass('workbench.targets_universe') IS NOT NULL")).scalar())


def retract(db, board_ids: Sequence[int], reason: str, apply: bool = False,
            detail: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Take boards out of every firm view: ``status='rejected'`` with the reason and the previous link
    kept under ``discovery_evidence.retracted``; firm keys and careers domain cleared; postings ->
    ``retracted`` (rows kept); verified attempts -> ``rejected``; the workbench link cleared. Dry run
    (default) only lists. Idempotent: a board already rejected keeps its first retraction."""
    reason = (reason or "retracted")[:40]
    out = []
    n_post = 0
    wb = _workbench_present(db)
    for bid in [int(x) for x in board_ids]:
        row = db.execute(text(
            "SELECT id, ats_type, board_token, status, verification, core_entity_id, cik, target_id, ein, "
            "industrial_company_id, careers_domain, careers_domain_evidence, verification_score, company_name "
            "FROM ats_board WHERE id = :i FOR UPDATE"), {"i": bid}).mappings().fetchone()
        if row is None:
            out.append({"board_id": bid, "action": "missing"})
            continue
        counts = dict(db.execute(text("SELECT status, count(*) FROM ats_posting WHERE board_id = :b "
                                      "AND status IN ('open', 'closed') GROUP BY 1"), {"b": bid}).fetchall())
        n_post += sum(counts.values())
        out.append({"board_id": bid, "token": f"{row['ats_type']}:{row['board_token']}", "name": row["company_name"],
                    "status_before": row["status"], "target_id": row["target_id"],
                    "careers_domain": row["careers_domain"], "postings": counts,
                    "action": "already_rejected" if row["status"] == "rejected" else "retract"})
        if not apply:
            continue
        prev = {k: row[k] for k in ("status", "verification", "core_entity_id", "cik", "target_id", "ein",
                                    "industrial_company_id", "careers_domain", "careers_domain_evidence")}
        prev["verification_score"] = float(row["verification_score"]) if row["verification_score"] is not None else None
        rec = {"reason": reason, "at": _now().isoformat(), "rule_version": match.RULE_VERSION, "previous": prev}
        if detail:
            rec["detail"] = detail
        db.execute(_RETRACT_BOARD, {"id": bid, "r": json.dumps(rec, default=str)})
        db.execute(text("UPDATE ats_posting SET status = 'retracted' WHERE board_id = :b "
                        "AND status IN ('open', 'closed')"), {"b": bid})
        db.execute(text("UPDATE ats_discovery_attempt SET verdict = 'rejected', reason = :r "
                        "WHERE ats_type = :a AND board_token = :t AND verdict = 'verified'"),
                   {"r": reason, "a": row["ats_type"], "t": row["board_token"]})
        if wb:
            db.execute(text("UPDATE workbench.targets_universe SET ats_board_id = NULL WHERE ats_board_id = :b"),
                       {"b": bid})
    if apply:
        db.commit()
    return {"apply": apply, "reason": reason, "boards": out, "postings_retracted": n_post}


def retract_candidate_postings(db, apply: bool = False) -> Dict[str, Any]:
    """A candidate board has no firm link and is never refreshed: postings stored on it (a board demoted
    after it was verified, Luminate) can only go stale. They become ``retracted``."""
    rows = db.execute(text(
        "SELECT p.board_id, count(*) FROM ats_posting p JOIN ats_board b ON b.id = p.board_id "
        "WHERE b.status = 'candidate' AND p.status IN ('open', 'closed') GROUP BY 1 ORDER BY 1")).fetchall()
    if apply and rows:
        db.execute(text("UPDATE ats_posting p SET status = 'retracted' FROM ats_board b WHERE b.id = p.board_id "
                        "AND b.status = 'candidate' AND p.status IN ('open', 'closed')"))
        db.commit()
    return {"apply": apply, "boards": {int(b): int(n) for b, n in rows}, "postings": sum(int(n) for _b, n in rows)}


def fetch_board(bf: "collect.BoardFetcher", ats: str, token: str) -> Dict[str, Any]:
    """meta (Greenhouse) + postings for one token through the gate."""
    meta = None
    meta_url = adapters.board_meta_url(ats, token)
    if meta_url:
        outcome, meta, info = bf.fetch_json(meta_url)
        if outcome != "fetched":
            return {"outcome": outcome, "info": info}
    outcome, payload, info = bf.fetch_json(adapters.board_jobs_url(ats, token))
    if outcome != "fetched":
        return {"outcome": outcome, "info": info}
    postings = []
    for j in adapters.jobs_from_payload(ats, payload):
        rec = adapters.normalize(ats, j)
        if rec["external_id"] and rec["title"]:
            postings.append(rec)
    return {"outcome": "fetched", "info": info, "meta": meta if isinstance(meta, dict) else {},
            "postings": postings, "url": info["url"], "http_status": info["http_status"],
            "bytes": info["bytes"], "sha256": info["sha256"], "terms_citation": info["terms_citation"],
            "fetched_at": _now()}


def _board_result(ats: str, token: str, t: Target, board: Dict[str, Any]) -> "collect.BoardResult":
    return collect.BoardResult(
        ats=ats, token=token, company=collect.Company(name=t.name), basis="name_slug", evidence={},
        status="active", outcome="fetched", url=board["url"], http_status=board["http_status"],
        fetched_at=board["fetched_at"], postings=board["postings"], bytes=board["bytes"], sha256=board["sha256"],
        terms_citation=board["terms_citation"])


_REVERIFIED = text("""
    UPDATE ats_board SET discovery_evidence = CAST(:ev AS JSONB), verification_score = :score,
        careers_domain = :dom, careers_domain_evidence = CAST(:dom_ev AS JSONB), ein = COALESCE(ein, :ein),
        terms_citation = COALESCE(:citation, terms_citation), last_fetched_at = :at, last_outcome = 'fetched',
        updated_at = NOW()
    WHERE id = :id AND status = 'active'
""")


class _SiteGate:
    """One fetcher per host for a re-verification pass, with the discovery run's stop conditions."""

    def __init__(self, fetcher_factory, stop_file: Optional[str]):
        self.factory, self.stop_file = fetcher_factory, stop_file
        self.fetchers: Dict[str, open_web.OpenWebFetcher] = {}
        self.bfs: Dict[str, collect.BoardFetcher] = {}
        self.consecutive: Counter = Counter()
        self.stopped: Dict[str, str] = {}

    def fetch(self, ats: str, token: str) -> Optional[Dict[str, Any]]:
        if self.stop_file and os.path.exists(self.stop_file):
            self.stopped.setdefault(ats, "stop_file")
        if ats in self.stopped:
            return None
        if ats not in self.bfs:
            self.fetchers[ats] = self.factory(ats)
            self.bfs[ats] = collect.BoardFetcher(self.fetchers[ats])
        board = fetch_board(self.bfs[ats], ats, token)
        outcome, status = board["outcome"], board["info"].get("http_status")
        if outcome in STOP_OUTCOMES:
            self.stopped[ats] = outcome
        elif outcome in collect.REFUSED:
            self.stopped[ats] = f"refused:{outcome}"
        elif _transient(outcome, status):
            self.consecutive[ats] += 1
            if self.consecutive[ats] >= MAX_CONSECUTIVE_ERRORS:
                self.stopped[ats] = "consecutive_errors"
        else:
            self.consecutive[ats] = 0
        return board

    def requests(self) -> Dict[str, int]:
        return {a: f.requests for a, f in self.fetchers.items()}

    def close(self) -> None:
        for f in self.fetchers.values():
            f.close()


def _default_loader(db, ids: Sequence[int]) -> List[Target]:
    return load_targets(db, target_ids=ids, skip_linked=False, kept_only=False)[0]


def reverify(db_factory, apply: bool = False, fetcher_factory=None, board_ids: Optional[Sequence[int]] = None,
             target_loader: Optional[Callable[[Any, Sequence[int]], List[Target]]] = None,
             retry_other_firm: bool = True, stop_file: Optional[str] = None) -> Dict[str, Any]:
    """Re-fetch every discovery-verified active board (or ``board_ids``) through the gate and re-decide
    it under the current rule for its linked target. Verified -> evidence / score / careers domain
    refreshed and postings merged (one fetch row). Not verified -> retracted (reason ``R8:<reason>``).
    A failed fetch changes nothing. Then (``retry_other_firm``) the attempts that ended
    ``board_linked_to_other_firm`` are re-tried with the fixed same-firm check, and candidate boards'
    stale postings are retracted. Dry run (default) writes nothing."""
    fetcher_factory = fetcher_factory or _default_fetcher
    loader = target_loader or _default_loader
    gate = _SiteGate(fetcher_factory, stop_file)
    db = db_factory()
    report: Dict[str, Any] = {"apply": apply, "rule_version": match.RULE_VERSION, "boards": [], "other_firm": []}
    try:
        sql = ("SELECT id, ats_type, board_token, target_id, discovery_evidence FROM ats_board "
               "WHERE status = 'active' AND verification = 'verified' AND target_id IS NOT NULL")
        params: Dict[str, Any] = {}
        if board_ids is not None:
            sql += " AND id = ANY(:ids)"
            params["ids"] = [int(x) for x in board_ids]
        rows = db.execute(text(sql + " ORDER BY id"), params).fetchall()
        others = []
        if retry_other_firm:
            others = db.execute(text(
                "SELECT target_id, ats_type, board_token, variant FROM ats_discovery_attempt "
                "WHERE verdict = 'candidate' AND reason = 'board_linked_to_other_firm' ORDER BY target_id")).fetchall()
        tids = sorted({int(r[3]) for r in rows} | {int(o[0]) for o in others})
        tg = {t.target_id: t for t in loader(db, tids)} if tids else {}
        for bid, ats, token, tid, ev in rows:
            item = {"board_id": bid, "ats": ats, "token": token, "target_id": tid}
            t = tg.get(int(tid))
            board = gate.fetch(ats, token) if t is not None else None
            if t is None:
                item["action"] = "unchanged_no_target"
            elif board is None:
                item["action"] = "skipped_stopped"
            elif board["outcome"] != "fetched":
                item.update(action="unchanged_fetch_failed", outcome=board["outcome"],
                            http_status=board["info"].get("http_status"))
            else:
                v = match.verdict(ats, t, board["meta"], board["postings"])
                item.update(name=t.name, verdict=v.verdict, reason=v.reason, score=v.score,
                            postings=len(board["postings"]), old_rule=(ev or {}).get("rule"),
                            location=v.evidence.get("location"), name_df=v.evidence.get("name_df"))
                if v.verdict == "verified":
                    dom = match.careers_domain(t, board["postings"], board["meta"])
                    item.update(action="keep", careers_domain=dom["domain"] if dom else None)
                    if apply:
                        keep = {k: (ev or {}).get(k) for k in ("variant", "target_id", "run_id", "board_url")}
                        new_ev = {**keep, **v.evidence, "reverified_at": _now().isoformat()}
                        db.execute(_REVERIFIED, {
                            "id": bid, "ev": json.dumps(new_ev, default=str), "score": v.score,
                            "dom": dom["domain"] if dom else None,
                            "dom_ev": json.dumps(dom, default=str) if dom else None, "ein": t.ein,
                            "citation": board.get("terms_citation"), "at": board["fetched_at"]})
                        collect.store_postings(db, bid, _board_result(ats, token, t, board))
                        db.commit()
                elif v.reason == "empty_board":
                    # no openings today says nothing against a link verified on postings: keep it; the
                    # fetch closes its open postings exactly as the weekly refresh would
                    item["action"] = "keep_empty"
                    if apply:
                        db.execute(text("UPDATE ats_board SET last_fetched_at = :at, last_outcome = 'fetched', "
                                        "updated_at = NOW() WHERE id = :id"), {"at": board["fetched_at"], "id": bid})
                        collect.store_postings(db, bid, _board_result(ats, token, t, board))
                        db.commit()
                else:
                    item["action"] = "retract"
                    if apply:
                        retract(db, [bid], f"R8:{v.reason}", apply=True,
                                detail={k: v.evidence.get(k) for k in (
                                    "verdict", "reason", "score", "board_name", "full_hits", "canon_hits", "name_df",
                                    "location", "city_share", "state_share", "name_share", "canon_share",
                                    "postings", "rule_version")})
            report["boards"].append(item)
        for tid, ats, token, variant in others:
            item = {"target_id": int(tid), "ats": ats, "token": token}
            t = tg.get(int(tid))
            board = gate.fetch(ats, token) if t is not None else None
            if t is None or board is None or board["outcome"] != "fetched":
                item["action"] = "unchanged"
                report["other_firm"].append(item)
                continue
            v = match.verdict(ats, t, board["meta"], board["postings"])
            item.update(name=t.name, verdict=v.verdict, reason=v.reason)
            if v.verdict != "verified":
                item["action"] = "not_verified"
            elif not apply:
                row = db.execute(_BOARD_ROW, {"ats": ats, "token": token}).fetchone()
                item["action"] = "would_link" if row is None or (row[1] == "active" and _same_firm(row, t)) \
                    else "still_other_firm"
                db.rollback()
            else:
                dom = match.careers_domain(t, board["postings"], board["meta"])
                verdict, reason, bid = link_verified(db, ats, token, t, v, board, dom, variant or "joined", None)
                item.update(action="linked" if verdict == "verified" else "still_other_firm", board_id=bid)
                if verdict == "verified":
                    collect.store_postings(db, bid, _board_result(ats, token, t, board))
                    db.execute(text("UPDATE ats_discovery_attempt SET verdict = 'verified', reason = :r, board_id = :b, "
                                    "score = :s WHERE target_id = :t AND ats_type = :a AND board_token = :k"),
                               {"r": reason, "b": bid, "s": v.score, "t": int(tid), "a": ats, "k": token})
                db.commit()
            report["other_firm"].append(item)
        report["candidate_postings"] = retract_candidate_postings(db, apply=apply)
        if apply:
            report["workbench"] = link_workbench(db)
    finally:
        gate.close()
        db.close()
    report["counts"] = dict(Counter(b["action"] for b in report["boards"]))
    report["other_firm_counts"] = dict(Counter(b["action"] for b in report["other_firm"]))
    report["requests"] = gate.requests()
    report["stopped"] = dict(gate.stopped)
    return report


def link_workbench(db) -> Dict[str, Any]:
    """Fill workbench.targets_universe.ats_board_id for verified targets that have none (the busiest
    verified board when a firm has two). Never overwrites a link the workbench already holds."""
    if not db.execute(text("SELECT to_regclass('workbench.targets_universe') IS NOT NULL")).scalar():
        return {"skipped": "workbench.targets_universe absent"}
    n = db.execute(text("""
        UPDATE workbench.targets_universe t SET ats_board_id = x.id FROM (
            SELECT DISTINCT ON (b.target_id) b.target_id, b.id
            FROM ats_board b
            LEFT JOIN LATERAL (SELECT count(*) AS n FROM ats_posting p
                               WHERE p.board_id = b.id AND p.status = 'open') c ON TRUE
            WHERE b.status = 'active' AND b.verification = 'verified' AND b.target_id IS NOT NULL
            ORDER BY b.target_id, c.n DESC, b.id) x
        WHERE t.target_id = x.target_id AND t.ats_board_id IS NULL
    """)).rowcount
    db.commit()
    return {"targets_linked": n}


# ---------------------------------------------------------------------------
# the run
# ---------------------------------------------------------------------------


class _Run:
    """Shared state of one run: the report, the run row, the stop file."""

    def __init__(self, apply: bool, run_id: Optional[int], stop_file: Optional[str], db_factory):
        self.apply, self.run_id, self.stop_file, self.db_factory = apply, run_id, stop_file, db_factory
        self.lock = threading.Lock()
        self.attempts: List[Dict[str, Any]] = []
        self.hits: List[Dict[str, Any]] = []
        self.sites: Dict[str, Dict[str, Any]] = {}

    def stop_requested(self) -> bool:
        return bool(self.stop_file) and os.path.exists(self.stop_file)


class SiteRunner:
    def __init__(self, run: _Run, ats: str, targets: Sequence[Target], fetcher: open_web.OpenWebFetcher,
                 prior: Prior, max_requests: Optional[int]):
        self.run, self.ats, self.targets = run, ats, list(targets)
        self.fetcher = fetcher
        self.bf = collect.BoardFetcher(fetcher)
        self.prior = prior
        self.not_found: Set[str] = set(prior.not_found.get(ats, set()))
        self.cache: "OrderedDict[str, Dict[str, Any]]" = OrderedDict()
        self.max_requests = max_requests
        self.db = None
        self.consecutive = 0
        self.retry: List[Tuple[Target, str, str]] = []
        self.stored_boards: Set[int] = set()
        self.m: Dict[str, Any] = {
            "targets_total": len(self.targets), "targets_done": 0, "attempts": 0, "requests": 0,
            "outcomes": Counter(), "verdicts": Counter(), "reasons": Counter(), "timeouts": 0, "cache_hits": 0,
            "verified_boards": 0, "postings_stored": 0, "postings_with_pay": 0, "careers_domains": 0,
            "stopped": None, "last_target_id": None, "started_at": _now().isoformat(), "elapsed_s": 0.0,
        }
        self._t0 = time.monotonic()

    # -- control ---------------------------------------------------------------

    def _check_stop(self) -> bool:
        if self.m["stopped"]:
            return True
        if self.run.stop_requested():
            self.m["stopped"] = "stop_file"
        elif self.max_requests is not None and self.fetcher.requests >= self.max_requests:
            self.m["stopped"] = "budget_exhausted"
        return bool(self.m["stopped"])

    def execute(self) -> None:
        if self.run.apply:
            self.db = self.run.db_factory()
        try:
            for i, t in enumerate(self.targets, 1):
                if self._check_stop():
                    break
                self._target(t)
                if self.m["stopped"]:
                    break
                self.m["targets_done"] = i
                self.m["last_target_id"] = t.target_id
                if i % CHUNK == 0:
                    self._flush()
            if not self.m["stopped"] and self.retry:
                pending, self.retry = self.retry, []
                for t, token, kind in pending:          # one retry pass for transient outcomes
                    if self._check_stop():
                        break
                    self._attempt(t, token, kind, retry=True)
        except Exception as exc:                        # recorded, the run row says failed
            self.m["stopped"] = f"crashed: {type(exc).__name__}: {exc}"[:300]
            if self.db is not None:
                self.db.rollback()
        finally:
            self._flush(final=True)
            if self.db is not None:
                self.db.close()

    def _flush(self, final: bool = False) -> None:
        self.m["requests"] = self.fetcher.requests
        self.m["elapsed_s"] = round(time.monotonic() - self._t0, 1)
        snap = json.loads(json.dumps(self.m, default=str))
        with self.run.lock:
            self.run.sites[self.ats] = snap
        print(f"[ats-discovery:{self.ats}] {snap['targets_done']}/{snap['targets_total']} targets, "
              f"{snap['requests']} requests, verdicts {dict(snap['verdicts'])}, last target {snap['last_target_id']}, "
              f"{snap['elapsed_s']:.0f}s" + (f", stopped: {snap['stopped']}" if snap["stopped"] else ""), flush=True)
        if self.run.apply and self.run.run_id is not None and self.db is not None:
            self.db.execute(text(
                "UPDATE ats_discovery_run SET heartbeat_at = NOW(), "
                "metrics = jsonb_set(metrics, CAST(:path AS TEXT[]), CAST(:m AS JSONB), TRUE), "
                "checkpoint = CASE WHEN CAST(:last AS BIGINT) IS NULL THEN checkpoint ELSE "
                "jsonb_set(checkpoint, CAST(:cp AS TEXT[]), to_jsonb(CAST(:last AS BIGINT)), TRUE) END "
                "WHERE run_id = :r"),
                {"path": "{sites," + self.ats + "}", "m": json.dumps(snap), "cp": "{" + self.ats + "}",
                 "last": snap["last_target_id"], "r": self.run.run_id})
            self.db.commit()

    # -- one firm ----------------------------------------------------------------

    def _target(self, t: Target) -> None:
        if (t.target_id, self.ats) in self.prior.verified_sites:
            return                                      # verified on this site by an earlier run
        for token, kind in match.variants(t, self.ats):
            if (t.target_id, self.ats, token) in self.prior.final_pairs:
                continue
            if self._check_stop():
                return
            if self._attempt(t, token, kind) == "verified":
                return                                  # one verified board per firm per site

    def _fetch(self, token: str) -> Dict[str, Any]:
        """meta (Greenhouse) + postings for one token through the gate."""
        return fetch_board(self.bf, self.ats, token)

    def _attempt(self, t: Target, token: str, kind: str, retry: bool = False) -> Optional[str]:
        self.m["attempts"] += 1
        r0 = self.fetcher.requests
        cached = False
        if token in self.not_found:
            board = {"outcome": "not_found", "info": {"http_status": 404, "error": None}}
            cached = True
        elif token in self.cache:
            board = self.cache[token]
            self.cache.move_to_end(token)
            cached = True
            self.m["cache_hits"] += 1
        else:
            board = self._fetch(token)
            if board["outcome"] == "fetched":
                self.cache[token] = board
                while len(self.cache) > BOARD_CACHE:
                    self.cache.popitem(last=False)
            elif board["outcome"] == "not_found":
                self.not_found.add(token)
        reqs = self.fetcher.requests - r0
        outcome = board["outcome"]
        info = board["info"]
        status = info.get("http_status")
        self.m["outcomes"][outcome] += 1
        rec = {"target_id": t.target_id, "ats": self.ats, "token": token, "variant": kind, "outcome": outcome,
               "http_status": status, "requests": reqs, "verdict": None, "reason": None, "score": None,
               "board_id": None, "postings": None, "error": info.get("error"), "retry": retry,
               "evidence": {"cached": True} if cached else None}
        if outcome != "fetched":
            self._record(t, rec)
            if outcome in STOP_OUTCOMES:
                self.m["stopped"] = outcome
            elif outcome in collect.REFUSED:
                self.m["stopped"] = f"refused:{outcome}"
            elif _transient(outcome, status):
                self.consecutive += 1
                if "Timeout" in (info.get("error") or ""):
                    self.m["timeouts"] += 1
                if not retry:
                    self.retry.append((t, token, kind))
                if self.consecutive >= MAX_CONSECUTIVE_ERRORS:
                    self.m["stopped"] = "consecutive_errors"
            else:
                self.consecutive = 0
            return None
        self.consecutive = 0
        postings = board["postings"]
        v = match.verdict(self.ats, t, board["meta"], postings)
        verdict, reason = v.verdict, v.reason
        domain = match.careers_domain(t, postings, board["meta"]) if verdict == "verified" else None
        board_id = None
        if self.run.apply:
            if verdict == "verified":
                verdict, reason, board_id = link_verified(self.db, self.ats, token, t, v, board, domain, kind,
                                                          self.run.run_id)
                if verdict == "verified" and board_id not in self.stored_boards:
                    counts = collect.store_postings(self.db, board_id, _board_result(self.ats, token, t, board))
                    self.stored_boards.add(board_id)
                    self.m["postings_stored"] += counts.get("open") or 0
                    self.m["postings_with_pay"] += sum(1 for p in postings if p.get("pay_min") is not None)
                elif verdict == "candidate":
                    domain = None
            if verdict == "candidate":
                cid = record_candidate(self.db, self.ats, token, t, v, reason, board, kind, self.run.run_id)
                board_id = board_id or cid
        pay_n = sum(1 for p in postings if p.get("pay_min") is not None)
        if verdict == "verified":
            self.m["verified_boards"] += 1
            if domain:
                self.m["careers_domains"] += 1
            if not self.run.apply:
                self.m["postings_stored"] += len(postings)
                self.m["postings_with_pay"] += pay_n
        self.m["verdicts"][verdict] += 1
        if verdict != "verified":
            self.m["reasons"][reason] += 1
        ev = dict(v.evidence, verdict=verdict, reason=reason, careers_domain=domain, variant=kind,
                  board_reused=cached)
        rec.update(verdict=verdict, reason=reason, score=v.score, board_id=board_id, postings=len(postings), evidence=ev)
        self._record(t, rec)
        hit = {"target_id": t.target_id, "name": t.name, "city": t.city, "state": t.state,
               "core_entity_id": t.core_entity_id, "cik": t.cik, "ats": self.ats, "token": token, "variant": kind,
               "verdict": verdict, "reason": reason, "score": v.score, "board_id": board_id,
               "board_name": (board["meta"] or {}).get("name"), "postings": len(postings), "pay": pay_n,
               "careers_domain": domain, "evidence": ev,
               "sample": [{"title": p.get("title"), "location": p.get("location"), "url": p.get("source_url"),
                           "text": (p.get("description_text") or "")[:300]} for p in postings[:3]]}
        with self.run.lock:
            self.run.hits.append(hit)
        return verdict

    def _record(self, t: Target, rec: Dict[str, Any]) -> None:
        with self.run.lock:
            self.run.attempts.append({k: v for k, v in rec.items() if k != "evidence"} | (
                {"cached": True} if rec.get("evidence") == {"cached": True} else {}))
        if not self.run.apply:
            return
        self.db.execute(_ATTEMPT_UPSERT, {
            "run_id": self.run.run_id, "target_id": t.target_id, "eid": t.core_entity_id, "ats": self.ats,
            "token": rec["token"], "variant": rec["variant"], "outcome": rec["outcome"],
            "http_status": rec["http_status"], "requests": rec["requests"], "verdict": rec["verdict"],
            "reason": rec["reason"], "score": rec["score"], "board_id": rec["board_id"], "postings": rec["postings"],
            "evidence": json.dumps(rec["evidence"], default=str) if rec["evidence"] is not None else None,
            "error": (rec["error"] or None) and str(rec["error"])[:500], "at": _now()})
        self.db.commit()


def _default_fetcher(ats: str) -> open_web.OpenWebFetcher:
    return open_web.OpenWebFetcher(open_web.load_terms(collect.open_web_terms_path()))


def _start_run_row(db, params: Dict[str, Any], resume_run: Optional[int]) -> int:
    if resume_run is not None:
        rid = db.execute(text("UPDATE ats_discovery_run SET status = 'running', finished_at = NULL, "
                              "heartbeat_at = NOW() WHERE run_id = :r RETURNING run_id"), {"r": resume_run}).scalar()
        if rid is None:
            raise ValueError(f"no ats_discovery_run {resume_run} to resume")
    else:
        rid = db.execute(text("INSERT INTO ats_discovery_run (status, params, metrics, heartbeat_at) "
                              "VALUES ('running', CAST(:p AS JSONB), CAST(:m AS JSONB), NOW()) RETURNING run_id"),
                         {"p": json.dumps(params, default=str), "m": json.dumps({"sites": {}})}).scalar()
    db.commit()
    return int(rid)


def _final_status(sites: Dict[str, Dict[str, Any]]) -> str:
    stops = [s.get("stopped") for s in sites.values() if s.get("stopped")]
    if not stops:
        return "done"
    if any(str(x).startswith("crashed") for x in stops):
        return "failed"
    if all(x == "budget_exhausted" for x in stops):
        return "budget_exhausted"
    return "paused"


def run(db_factory: Optional[Callable[[], Any]], targets: Sequence[Target], apply: bool = False,
        fetcher_factory: Callable[[str], open_web.OpenWebFetcher] = _default_fetcher,
        sites: Sequence[str] = SITES, max_requests: Optional[int] = None, prior: Optional[Prior] = None,
        stop_file: Optional[str] = None, resume_run: Optional[int] = None,
        params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Discover boards for ``targets`` on ``sites`` (one thread per host). Dry run (default) writes nothing."""
    if apply and db_factory is None:
        raise ValueError("--apply needs a database")
    for s in sites:
        if s not in SITES:
            raise ValueError(f"site {s!r} is not discoverable (Ashby: robots.txt 401, owner decision: blocked)")
    if prior is None:
        prior = Prior()
        if db_factory is not None:
            db = db_factory()
            try:
                prior = load_prior(db)
            finally:
                db.close()
    run_id = None
    if apply:
        db = db_factory()
        try:
            run_id = _start_run_row(db, {**(params or {}), "targets": len(targets), "sites": list(sites),
                                         "max_requests_per_site": max_requests, "rule": match.RULE_VERSION},
                                    resume_run)
        finally:
            db.close()
    r = _Run(apply, run_id, stop_file, db_factory)
    started = time.monotonic()
    runners, fetchers = [], []
    for s in sites:
        f = fetcher_factory(s)
        fetchers.append(f)
        runners.append(SiteRunner(r, s, targets, f, prior, max_requests))
    threads = [threading.Thread(target=x.execute, name=f"ats-{x.ats}") for x in runners]
    try:
        for th in threads:
            th.start()
        for th in threads:
            th.join()
    finally:
        for f in fetchers:
            f.close()
    status = _final_status(r.sites)
    if apply:
        db = db_factory()
        try:
            db.execute(text("UPDATE ats_discovery_run SET status = :s, finished_at = NOW(), heartbeat_at = NOW() "
                            "WHERE run_id = :r"), {"s": status, "r": run_id})
            db.commit()
        finally:
            db.close()
    return {"apply": bool(apply), "run_id": run_id, "status": status, "targets": len(targets),
            "hours": round((time.monotonic() - started) / 3600, 3), "sites": r.sites,
            "requests": sum(s.get("requests") or 0 for s in r.sites.values()),
            "attempts": r.attempts, "hits": r.hits}


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--limit", type=int, help="at most this many kept targets (target_id order)")
    ap.add_argument("--target-ids-file", help="file of target ids (one per line) to restrict the run to")
    ap.add_argument("--after-target-id", type=int, help="only targets with a larger target_id")
    ap.add_argument("--apply", action="store_true", help="write (default: dry run, writes nothing)")
    ap.add_argument("--max-requests", type=int, help="request budget per site for this run")
    ap.add_argument("--resume-run", type=int, help="continue this ats_discovery_run (the ledger skips finished pairs)")
    ap.add_argument("--link-workbench", action="store_true",
                    help="after an --apply run, fill workbench.targets_universe.ats_board_id where NULL")
    ap.add_argument("--sites", default=",".join(SITES), help="comma-separated: greenhouse,lever")
    ap.add_argument("--stop-file", default=STOP_FILE, help="the run stops cleanly when this file exists")
    ap.add_argument("--json", help="write the full report (every hit with its evidence) to this path")
    ap.add_argument("--readjudicate", action="store_true",
                    help="only re-decide stored verified links under the current rule (with --apply: demote)")
    # SPEC_156
    ap.add_argument("--reverify", action="store_true",
                    help="re-fetch every verified link through the gate and re-decide it (with --apply: retract "
                         "the ones the current rule rejects, refresh the rest, retry board_linked_to_other_firm)")
    ap.add_argument("--retract", action="store_true",
                    help="retract --board-ids with --reason (with --apply: status 'rejected', postings retracted)")
    ap.add_argument("--board-ids", help="comma-separated ats_board ids for --reverify / --retract")
    ap.add_argument("--reason", help="why the boards are retracted (stored with them; <= 40 chars)")
    ap.add_argument("--no-retry-other-firm", action="store_true",
                    help="--reverify: skip re-trying board_linked_to_other_firm attempts")
    ap.add_argument("--reparse-pay", action="store_true",
                    help="re-parse stored pay with the current parser (with --apply: write)")
    return ap


def main(argv: Optional[Sequence[str]] = None) -> int:
    a = build_parser().parse_args(argv)
    from app.core.database import get_session_factory

    factory = get_session_factory()
    ids = [int(x) for x in a.board_ids.split(",") if x.strip()] if a.board_ids else None
    if a.retract:
        if not ids or not a.reason:
            raise SystemExit("--retract needs --board-ids and --reason")
        db = factory()
        try:
            rep = retract(db, ids, a.reason, apply=a.apply)
        finally:
            db.close()
        print(json.dumps(rep, indent=1, default=str), flush=True)
        return 0
    if a.reverify:
        rep = reverify(factory, apply=a.apply, board_ids=ids, retry_other_firm=not a.no_retry_other_firm,
                       stop_file=a.stop_file)
        if a.json:
            with open(a.json, "w", encoding="utf-8") as fh:
                json.dump(rep, fh, indent=1, default=str)
        print(json.dumps({k: rep[k] for k in ("apply", "rule_version", "counts", "other_firm_counts", "requests",
                                              "stopped")}, default=str), flush=True)
        for b in rep["boards"]:
            if b["action"] != "keep":
                print(f"  {b['action']:24} {b['board_id']:5} {b['ats']}:{b['token']:22} {str(b.get('name'))[:36]:36} "
                      f"{b.get('reason')} df={b.get('name_df')}", flush=True)
        for o in rep["other_firm"]:
            print(f"  other_firm {o['action']:16} {o['ats']}:{o['token']} -> {o['target_id']} {o.get('name')}", flush=True)
        print(f"  candidate postings: {rep['candidate_postings']}", flush=True)
        return 0 if not rep["stopped"] else 1
    if a.reparse_pay:
        db = factory()
        try:
            rep = collect.reparse_pay(db, apply=a.apply)
        finally:
            db.close()
        print(json.dumps(rep, indent=1, default=str), flush=True)
        return 0
    if a.readjudicate:
        db = factory()
        try:
            rep = readjudicate(db, apply=a.apply)
        finally:
            db.close()
        print(json.dumps(rep, indent=1, default=str), flush=True)
        return 0
    ids = None
    if a.target_ids_file:
        with open(a.target_ids_file, encoding="utf-8") as fh:
            ids = [int(x) for x in fh.read().split() if x.strip()]
    db = factory()
    try:
        targets, stats = load_targets(db, target_ids=ids, limit=a.limit, after=a.after_target_id)
    finally:
        db.close()
    print(f"targets: {stats}", flush=True)
    rep = run(factory, targets, apply=a.apply, sites=[s.strip() for s in a.sites.split(",") if s.strip()],
              max_requests=a.max_requests, stop_file=a.stop_file, resume_run=a.resume_run,
              params={"argv": list(argv) if argv is not None else None, "load": stats})
    rep["load"] = stats
    if a.apply and a.link_workbench:
        db = factory()
        try:
            rep["workbench"] = link_workbench(db)
        finally:
            db.close()
    if a.json:
        with open(a.json, "w", encoding="utf-8") as fh:
            json.dump(rep, fh, indent=1, default=str)
    v = [h for h in rep["hits"] if h["verdict"] == "verified"]
    print(f"{'APPLIED' if a.apply else 'DRY RUN'} run {rep['run_id']}: status {rep['status']}, {rep['targets']} targets, "
          f"{rep['requests']} requests, {rep['hours']} h, verified {len(v)}, "
          f"candidates {sum(1 for h in rep['hits'] if h['verdict'] == 'candidate')}, "
          f"rejected {sum(1 for h in rep['hits'] if h['verdict'] == 'rejected')}", flush=True)
    for h in sorted(v, key=lambda x: x["score"])[:30]:
        print(f"  {h['score']:.3f} {h['ats']}:{h['token']:24} {h['name'][:40]:40} {h['reason']} "
              f"postings={h['postings']} domain={(h['careers_domain'] or {}).get('domain')}")
    return 0 if rep["status"] in ("done", "paused", "budget_exhausted") else 1


if __name__ == "__main__":
    raise SystemExit(main())
