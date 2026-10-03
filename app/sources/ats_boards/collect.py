"""
ATS board collector (SPEC_151): Greenhouse / Lever / Ashby public board JSON -> NexData.

Every request -- board name, postings, robots.txt -- goes through
``app.core.open_web.OpenWebFetcher``: a recorded terms review for the host's registrable
domain (``app/entities/data/site_terms.json``), robots.txt per origin, the honest
NexdataResearch user agent, Retry-After honoured (the host is not asked again in the run),
>= max(Crawl-delay, 2 s) between requests to one host, public addresses only, nothing retried.
A body that hits the size cap is ``truncated`` and never parsed.

Storage (``--apply`` only; the default is a dry run that writes nothing):
- ``ats_board``: one row per board (ats, token) with the company link (core entity id,
  CIK, industrial company id), how the token was found and the evidence it was verified on.
- ``ats_posting``: one row per posting with first_seen_at / last_seen_at / closed_at, so
  hiring velocity (new first_seen per period) and team growth (open roles by department over
  time) are computable; pay columns carry their source, raw snippet, confidence and parser.
- ``ats_board_fetch``: one row per fetch (outcome, open / new / closed / reopened counts,
  bytes, sha256, user agent, terms citation) -- the lane's clock and its hiring time series.
A failed fetch closes nothing.

    python -m app.sources.ats_boards.collect --preset pilot            # DRY RUN
    python -m app.sources.ats_boards.collect --preset pilot --apply
    python -m app.sources.ats_boards.collect --ciks 1617078,1686840 --apply
    python -m app.sources.ats_boards.collect --preset migrated [--apply]   # SPEC_152: the retired
        job_postings:all run's verified Greenhouse / Lever boards (seeded token only, no slugs);
        with --apply the blocked Ashby seeds are recorded as status 'refused' (no request)
    python -m app.sources.ats_boards.collect --preset active [--apply]     # SPEC_153: the weekly
        refresh -- every active Greenhouse / Lever board, stored token only, chunks of <= 25
"""

from __future__ import annotations

import argparse
import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple

from sqlalchemy import text

from app.core import open_web
from app.sources.ats_boards import adapters, discover

MAX_BYTES = 16_000_000          # Tecovas' Greenhouse board is ~1 MB with content=true
TRANSIENT = ("retry_after", "host_backed_off", "error", "dns_error", "too_many_redirects")
REFUSED = ("terms_unreviewed", "terms_refused", "robots_disallowed", "robots_crawl_delay_too_long",
           "non_public_address", "bad_url", "redirect_offsite")


@dataclass
class Company:
    name: str
    core_entity_id: Optional[int] = None
    cik: Optional[str] = None
    industrial_company_id: Optional[int] = None
    domain: Optional[str] = None
    aliases: List[str] = field(default_factory=list)
    # (ats, token, basis, evidence) from held data or a cited seed
    tokens: List[Tuple[str, str, str, Dict[str, Any]]] = field(default_factory=list)


@dataclass
class BoardResult:
    ats: str
    token: str
    company: Company
    basis: str
    evidence: Dict[str, Any]
    status: str                      # active | unverified | not_found | refused | error
    outcome: str
    url: str
    http_status: Optional[int]
    fetched_at: datetime
    postings: Optional[List[Dict[str, Any]]]
    bytes: Optional[int] = None
    sha256: Optional[str] = None
    terms_citation: Optional[str] = None
    error: Optional[str] = None


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _status(outcome: str) -> str:
    if outcome == "fetched":
        return "active"
    if outcome == "unverified":
        return "unverified"
    if outcome == "not_found":
        return "not_found"
    if outcome in REFUSED:
        return "refused"
    return "error"


class BoardFetcher:
    """JSON over the open-web gate; never raises for a refusal or an HTTP error."""

    def __init__(self, fetcher: open_web.OpenWebFetcher, max_bytes: int = MAX_BYTES):
        self.fetcher = fetcher
        self.max_bytes = max_bytes
        fetcher.max_bytes = max_bytes

    def fetch_json(self, url: str) -> Tuple[str, Any, Dict[str, Any]]:
        res = self.fetcher.get(url)
        info = {"url": url, "http_status": res.status, "terms_citation": res.terms_citation,
                "error": res.error, "bytes": None, "sha256": None}
        if res.outcome == "http_error":
            return ("not_found" if res.status == 404 else "http_error"), None, info
        if res.outcome != "fetched":
            return res.outcome, None, info
        raw = (res.body or "").encode("utf-8")
        info["bytes"] = len(raw)
        info["sha256"] = hashlib.sha256(raw).hexdigest()
        if len(raw) >= self.max_bytes - 8:
            return "truncated", None, info
        try:
            return "fetched", json.loads(res.body), info
        except ValueError:
            return "not_json", None, info


def _candidates(company: Company, slugs: bool, ats_order: Sequence[str]) -> List[Tuple[str, str, str, Dict]]:
    out: List[Tuple[str, str, str, Dict]] = []
    seen = set()
    for ats, token, basis, ev in company.tokens:
        if ats in adapters.ATS_TYPES and (ats, token.lower()) not in seen:
            seen.add((ats, token.lower()))
            out.append((ats, token, basis, ev))
    if slugs:
        dom_label = company.domain.split(".")[0] if company.domain else None
        for slug in discover.slug_candidates(company.name, company.domain):
            basis = "domain_slug" if slug == dom_label else "name_slug"
            for ats in ats_order:
                if (ats, slug) not in seen:
                    seen.add((ats, slug))
                    out.append((ats, slug, basis, {}))
    return out


def try_board(bf: BoardFetcher, company: Company, ats: str, token: str, basis: str,
              evidence: Dict[str, Any]) -> BoardResult:
    names = [company.name] + list(company.aliases)
    meta = None
    jobs_url = adapters.board_jobs_url(ats, token)
    meta_url = adapters.board_meta_url(ats, token)

    def result(outcome, info, postings=None, ev=None):
        return BoardResult(ats=ats, token=token, company=company, basis=basis,
                           evidence={**evidence, **(ev or {})}, status=_status(outcome), outcome=outcome,
                           url=info.get("url") or jobs_url, http_status=info.get("http_status"),
                           fetched_at=_now(), postings=postings, bytes=info.get("bytes"),
                           sha256=info.get("sha256"), terms_citation=info.get("terms_citation"),
                           error=info.get("error"))

    if meta_url:
        outcome, meta, info = bf.fetch_json(meta_url)
        if outcome != "fetched":
            return result(outcome, info)
        ok, ev = discover.verify(ats, names, meta, [])
        if not ok:
            return result("unverified", info, ev=ev)
        evidence = {**evidence, **ev}
    outcome, payload, info = bf.fetch_json(jobs_url)
    if outcome != "fetched":
        return result(outcome, info)
    raw_jobs = adapters.jobs_from_payload(ats, payload)
    if not meta_url:
        ok, ev = discover.verify(ats, names, None, raw_jobs)
        if not ok:
            return result("unverified", info, ev=ev)
        evidence = {**evidence, **ev}
    postings = []
    for j in raw_jobs:
        rec = adapters.normalize(ats, j)
        if rec["external_id"] and rec["title"]:
            postings.append(rec)
    return result("fetched", info, postings=postings)


def run(db, companies: Sequence[Company], apply: bool = False, fetcher: Optional[open_web.OpenWebFetcher] = None,
        slugs: bool = True, ats_order: Sequence[str] = adapters.ATS_TYPES, max_bytes: int = MAX_BYTES,
        terms_path=None) -> Dict[str, Any]:
    if len(companies) > discover.MAX_COMPANIES:
        raise ValueError(f"{len(companies)} companies: at most {discover.MAX_COMPANIES} per run "
                         "(board discovery is never run at scale)")
    own = fetcher is None
    if own:
        fetcher = open_web.OpenWebFetcher(open_web.load_terms(terms_path or open_web_terms_path()))
    bf = BoardFetcher(fetcher, max_bytes=max_bytes)
    attempts: List[Dict[str, Any]] = []
    boards: List[Dict[str, Any]] = []
    try:
        for co in companies:
            held = {(a, t.lower()) for a, t, _b, _e in co.tokens}
            for ats, token, basis, ev in _candidates(co, slugs, ats_order):
                br = try_board(bf, co, ats, token, basis, ev)
                attempts.append({"company": co.name, "ats": ats, "token": token, "basis": basis,
                                 "outcome": br.outcome, "http_status": br.http_status,
                                 "evidence": br.evidence})
                keep = br.outcome == "fetched" or (ats, token.lower()) in held
                if not keep:
                    continue
                summary = {"company": co.name, "core_entity_id": co.core_entity_id, "cik": co.cik,
                           "industrial_company_id": co.industrial_company_id, "ats": ats, "token": token,
                           "basis": basis, "status": br.status, "outcome": br.outcome,
                           "postings": len(br.postings) if br.postings is not None else None,
                           "pay": sum(1 for p in br.postings or [] if p.get("pay_min") is not None),
                           "pay_text": sum(1 for p in br.postings or [] if p.get("pay_source") == "text"),
                           "pay_structured": sum(1 for p in br.postings or [] if p.get("pay_source") == "structured")}
                if apply and db is not None:
                    summary.update(store(db, br))
                    db.commit()
                boards.append(summary)
                if br.outcome == "fetched":
                    break                    # one verified board per company
    finally:
        if own:
            fetcher.close()
    return {"apply": apply, "companies": len(companies), "attempts": attempts, "boards": boards,
            "boards_fetched": sum(1 for b in boards if b["outcome"] == "fetched"),
            "requests": getattr(fetcher, "requests", None)}


def open_web_terms_path():
    from app.entities.domain_probe import TERMS_PATH

    return TERMS_PATH


# ---------------------------------------------------------------------------
# storage
# ---------------------------------------------------------------------------

_BOARD_UPSERT = text("""
    INSERT INTO ats_board (ats_type, board_token, company_name, core_entity_id, cik, industrial_company_id,
                           discovery_basis, discovery_evidence, status, terms_citation, last_fetched_at,
                           last_outcome, updated_at)
    VALUES (:ats, :token, :name, :eid, :cik, :iid, :basis, CAST(:evidence AS JSONB), :status, :citation,
            :fetched_at, :outcome, NOW())
    ON CONFLICT (ats_type, board_token) DO UPDATE SET
        company_name = EXCLUDED.company_name,
        core_entity_id = COALESCE(EXCLUDED.core_entity_id, ats_board.core_entity_id),
        cik = COALESCE(EXCLUDED.cik, ats_board.cik),
        industrial_company_id = COALESCE(EXCLUDED.industrial_company_id, ats_board.industrial_company_id),
        discovery_basis = EXCLUDED.discovery_basis,
        discovery_evidence = EXCLUDED.discovery_evidence,
        status = CASE WHEN EXCLUDED.status = 'error' THEN ats_board.status ELSE EXCLUDED.status END,
        terms_citation = COALESCE(EXCLUDED.terms_citation, ats_board.terms_citation),
        last_fetched_at = EXCLUDED.last_fetched_at,
        last_outcome = EXCLUDED.last_outcome,
        updated_at = NOW()
    RETURNING id
""")

POSTING_COLUMNS = ("title", "department", "team", "location", "employment_type", "workplace_type",
                   "posted_at", "source_updated_at", "source_url", "description_text", "description_sha256",
                   "pay_min", "pay_max", "pay_currency", "pay_interval", "pay_kind", "pay_source", "pay_snippet",
                   "pay_confidence", "pay_parser")

_POSTING_UPSERT = text(
    "INSERT INTO ats_posting (board_id, external_id, locations_all, status, first_seen_at, last_seen_at, "
    "closed_at, " + ", ".join(POSTING_COLUMNS) + ") VALUES (:board_id, :external_id, "
    "CAST(:locations_all AS JSONB), 'open', :t, :t, NULL, " + ", ".join(f":{c}" for c in POSTING_COLUMNS) + ") "
    "ON CONFLICT (board_id, external_id) DO UPDATE SET locations_all = EXCLUDED.locations_all, "
    + ", ".join(f"{c} = EXCLUDED.{c}" for c in POSTING_COLUMNS)
    + ", last_seen_at = EXCLUDED.last_seen_at, status = 'open', closed_at = NULL"
)

_FETCH_INSERT = text("""
    INSERT INTO ats_board_fetch (board_id, fetched_at, url, outcome, http_status, bytes, body_sha256,
                                 postings_seen, open_count, new_count, closed_count, reopened_count,
                                 pay_count, user_agent, terms_citation, error)
    VALUES (:board_id, :fetched_at, :url, :outcome, :http_status, :bytes, :sha256, :seen, :open, :new,
            :closed, :reopened, :pay, :ua, :citation, :error)
""")


def store(db, br: BoardResult) -> Dict[str, Optional[int]]:
    """Write one board result. Returns new / seen / closed / reopened / open counts."""
    co = br.company
    board_id = db.execute(_BOARD_UPSERT, {
        "ats": br.ats, "token": br.token, "name": co.name, "eid": co.core_entity_id, "cik": co.cik,
        "iid": co.industrial_company_id, "basis": br.basis, "evidence": json.dumps(br.evidence, default=str),
        "status": br.status, "citation": br.terms_citation, "fetched_at": br.fetched_at, "outcome": br.outcome,
    }).scalar()
    counts: Dict[str, Optional[int]] = {"new": None, "seen": None, "closed": None, "reopened": None, "open": None}
    pay_n = None
    if br.postings is not None:
        by_id: Dict[str, Dict[str, Any]] = {}
        for p in br.postings:
            if p.get("external_id"):
                by_id[p["external_id"]] = p
        existing = {r[0]: r[1] for r in db.execute(
            text("SELECT external_id, status FROM ats_posting WHERE board_id = :b"), {"b": board_id})}
        new = seen = reopened = 0
        for eid, p in by_id.items():
            prev = existing.get(eid)
            if prev is None:
                new += 1
            elif prev == "closed":
                reopened += 1
            else:
                seen += 1
            params = {c: p.get(c) for c in POSTING_COLUMNS}
            params.update({"board_id": board_id, "external_id": eid, "t": br.fetched_at,
                           "locations_all": json.dumps(p.get("locations_all") or [])})
            db.execute(_POSTING_UPSERT, params)
        closed = db.execute(text(
            "UPDATE ats_posting SET status = 'closed', closed_at = :t WHERE board_id = :b AND status = 'open' "
            "AND NOT (external_id = ANY(CAST(:ids AS TEXT[])))"),
            {"t": br.fetched_at, "b": board_id, "ids": list(by_id)}).rowcount
        open_n = db.execute(text("SELECT count(*) FROM ats_posting WHERE board_id = :b AND status = 'open'"),
                            {"b": board_id}).scalar()
        pay_n = sum(1 for p in by_id.values() if p.get("pay_min") is not None)
        counts = {"new": new, "seen": seen, "closed": closed, "reopened": reopened, "open": open_n}
    db.execute(_FETCH_INSERT, {
        "board_id": board_id, "fetched_at": br.fetched_at, "url": br.url, "outcome": br.outcome,
        "http_status": br.http_status, "bytes": br.bytes, "sha256": br.sha256,
        "seen": len(br.postings) if br.postings is not None else None, "open": counts["open"],
        "new": counts["new"], "closed": counts["closed"], "reopened": counts["reopened"], "pay": pay_n,
        "ua": open_web.USER_AGENT, "citation": br.terms_citation, "error": br.error,
    })
    return {**counts, "board_id": board_id}


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _session():
    from app.core.database import get_session_factory

    return get_session_factory()()


def main(argv: Optional[Sequence[str]] = None) -> int:
    from app.sources.ats_boards import ingest

    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--preset", choices=("pilot", "migrated", "active"))
    ap.add_argument("--ciks", help="comma-separated CIKs (resolved to core entities)")
    ap.add_argument("--industrial-ids", help="comma-separated industrial_companies ids")
    ap.add_argument("--apply", action="store_true", help="write (default: dry run, writes nothing)")
    ap.add_argument("--json", help="write the full report to this path")
    a = ap.parse_args(argv)
    db = _session()
    try:
        if a.preset == "active":
            rep = ingest.run_chunked(db, ingest.active_companies(db), apply=a.apply, slugs=False)
        else:
            companies = ingest._companies(db, preset=a.preset, ciks=a.ciks, industrial_ids=a.industrial_ids)
            rep = run(db, companies, apply=a.apply, slugs=a.preset != "migrated")
        if a.preset == "migrated":
            rep["blocked_recorded"] = ingest.record_blocked(db, apply=a.apply)
    finally:
        db.close()
    if a.json:
        with open(a.json, "w", encoding="utf-8") as fh:
            json.dump(rep, fh, indent=2, default=str)
    print(f"{'APPLIED' if a.apply else 'DRY RUN'}: {rep['companies']} companies, {len(rep['attempts'])} attempts, "
          f"{rep['boards_fetched']} boards fetched, {rep['requests']} HTTP requests"
          + (f", {rep['blocked_recorded']} blocked seeds {'recorded' if a.apply else 'listed'}"
             if "blocked_recorded" in rep else ""))
    for at in rep["attempts"]:
        print(f"  {at['company'][:34]:34} {at['ats']:10} {at['token']:24} {at['basis']:12} {at['outcome']}")
    for b in rep["boards"]:
        print(f"  BOARD {b['company'][:30]:30} {b['ats']}:{b['token']} {b['status']} postings={b['postings']} "
              f"pay={b['pay']} (text {b['pay_text']}, structured {b['pay_structured']})"
              + (f" new={b.get('new')} seen={b.get('seen')} closed={b.get('closed')}" if a.apply else ""))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
