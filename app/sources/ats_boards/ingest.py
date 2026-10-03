"""
ATS boards -- job-path entry point and company loading (SPEC_151).

Dispatch key ``ats_boards`` (app/api/v1/jobs.py). Config: ``preset`` ("pilot" | "migrated" | "active"), ``ciks``,
``industrial_ids`` (comma-separated or lists), ``apply`` (default False: a dry run that
writes nothing). The job FAILS (raises) when no board could be fetched at all, so a lane
that collected nothing never reports success.

Preset ``migrated`` (SPEC_152): the retired ``job_postings:all`` run's Greenhouse / Lever companies
whose board this lane verified (seeds keyed by ``industrial_company_id``); the seeded token only,
no slug discovery. Seeds with ``blocked`` (Ashby: robots.txt 401) are never offered to the
fetcher; ``record_blocked`` records them as ``ats_board.status = 'refused'`` without a request.

Preset ``active`` (SPEC_153, the WEEKLY schedule): every board the lane already verified
(``ats_board.status = 'active'``, Greenhouse / Lever only), its stored token only, no slug
discovery, in chunks of at most ``discover.MAX_COMPANIES`` companies (the per-run cap is
unchanged). Ashby rows (robots.txt 401) and refused / not_found / unverified rows are never read.
``collect.run`` is blocking HTTP, so it runs in a worker thread: the worker heartbeat keeps ticking.
"""

from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Union

from sqlalchemy import text

from app.entities import domains
from app.sources.ats_boards import collect, discover
from app.sources.ats_boards.collect import Company

logger = logging.getLogger(__name__)

SEEDS_PATH = Path(__file__).resolve().parent / "data" / "board_seeds.json"

# PE-TARGETS-DEPTH-PLAN pilot (2026-09-29): GlossGenius/Genius, Whatnot, Bombas, Tecovas,
# Carbon Arc, Rockfish Data, Talent Source Solutions by CIK; Kimball Midwest (no CIK) by its
# industrial_companies id.
PILOT_CIKS = ("1988828", "1844768", "1617078", "1686840", "2036345", "2041965", "2135289")
PILOT_INDUSTRIAL_IDS = (78,)


def _list(v: Union[None, str, Sequence[Any]]) -> List[str]:
    if v is None:
        return []
    if isinstance(v, str):
        return [x.strip() for x in v.split(",") if x.strip()]
    return [str(x).strip() for x in v if str(x).strip()]


def migrated_industrial_ids(path: Path = SEEDS_PATH) -> List[int]:
    """SPEC_152: industrial companies moved over from the retired job_postings:all run."""
    return sorted({int(s["industrial_company_id"]) for s in load_seeds(path)
                   if s.get("industrial_company_id") and not s.get("blocked")})


def _blocked_keys(seeds: Sequence[Dict[str, Any]]) -> set:
    return {(s["ats"], s["token"].lower()) for s in seeds if s.get("blocked")}


def record_blocked(db, apply: bool = False, path: Path = SEEDS_PATH) -> int:
    """SPEC_152: write each blocked seed as an ``ats_board`` row with status ``refused`` (no request is
    made; the reason and its measurement are the evidence). Dry run writes nothing. Returns the count."""
    blocked = [s for s in load_seeds(path) if s.get("blocked")]
    if not apply:
        return len(blocked)
    for s in blocked:
        db.execute(collect._BOARD_UPSERT, {
            "ats": s["ats"], "token": s["token"], "name": s.get("company_name") or s["token"],
            "eid": None, "cik": s.get("cik"), "iid": s.get("industrial_company_id"), "basis": "seed",
            "evidence": json.dumps({"blocked": s["blocked"], "citation": s["citation"]}),
            "status": "refused", "citation": None, "fetched_at": None, "outcome": "robots_disallowed",
        })
    db.commit()
    return len(blocked)


def load_seeds(path: Path = SEEDS_PATH) -> List[Dict[str, Any]]:
    seeds = json.loads(Path(path).read_text(encoding="utf-8"))
    for s in seeds:
        if not (s.get("cik") or s.get("industrial_company_id")) or not s.get("ats") or not s.get("token") \
                or not s.get("citation"):
            raise ValueError(f"board seed needs a company key, ats, token and citation: {s}")
    return seeds


def _ats_config_tokens(db, industrial_id: int) -> List[tuple]:
    row = db.execute(text(
        "SELECT ats_type, board_token FROM company_ats_config WHERE company_id = :i"), {"i": industrial_id}).fetchone()
    if row and row[0] in ("greenhouse", "lever", "ashby") and row[1]:
        return [(row[0], row[1], "ats_config", {"company_ats_config.company_id": industrial_id})]
    return []


def _entity_by_cik(db, cik: str) -> Optional[tuple]:
    return db.execute(text(
        "SELECT e.entity_id, e.canonical_name, e.canonical_domain FROM core.identifier i "
        "JOIN core.entity e ON e.entity_id = i.entity_id "
        "WHERE i.id_type = 'cik' AND i.id_value = :c AND e.dissolved_at IS NULL"), {"c": str(int(cik))}).fetchone()


def _aliases(db, entity_id: int) -> List[str]:
    return [r[0] for r in db.execute(text(
        "SELECT DISTINCT name FROM core.alias WHERE entity_id = :e AND name IS NOT NULL"), {"e": entity_id})]


# SPEC_153: the weekly refresh reads only boards already verified on Greenhouse / Lever. A constant
# statement: no parameter, nothing string-built.
ACTIVE_BOARDS_SQL = text(
    "SELECT id, ats_type, board_token, company_name, core_entity_id, cik, industrial_company_id "
    "FROM ats_board WHERE status = 'active' AND ats_type IN ('greenhouse', 'lever') "
    "ORDER BY id")


def active_companies(db) -> List[Company]:
    """SPEC_153: one Company per ACTIVE BOARD, its stored token only.

    Per board, not per company: ``collect.run`` stops at a company's first fetched board, so a
    company holding two active boards (a Lever -> Greenhouse move) had the second one never
    re-read and its postings never closed (review fix T11)."""
    seeds = load_seeds()
    blocked = _blocked_keys(seeds)
    by_key: Dict[tuple, Company] = {}
    for bid, ats, token, name, eid, cik, iid in db.execute(ACTIVE_BOARDS_SQL).fetchall():
        if (ats, token.lower()) in blocked:
            continue
        key = bid
        co = by_key.get(key)
        if co is None:
            co = Company(name=name, core_entity_id=eid, cik=cik, industrial_company_id=iid)
            if eid:
                co.aliases = [a for a in _aliases(db, eid) if a != name]
            for s in seeds:
                if not s.get("blocked") and (
                        (s.get("cik") and cik and s["cik"] == cik)
                        or (s.get("industrial_company_id") and s["industrial_company_id"] == iid)):
                    co.aliases += [a for a in s.get("aliases") or [] if a not in co.aliases]
            by_key[key] = co
        co.tokens.append((ats, token, "refresh", {"ats_board.id": bid}))
    return list(by_key.values())


def run_chunked(db, companies: Sequence[Company], apply: bool = False, slugs: bool = True) -> Dict[str, Any]:
    """``collect.run`` over at most ``discover.MAX_COMPANIES`` companies at a time; one merged report."""
    merged: Dict[str, Any] = {"apply": bool(apply), "companies": 0, "attempts": [], "boards": [],
                              "boards_fetched": 0, "requests": 0, "chunks": 0}
    for i in range(0, len(companies), discover.MAX_COMPANIES):
        rep = collect.run(db, companies[i:i + discover.MAX_COMPANIES], apply=bool(apply), slugs=slugs)
        merged["chunks"] += 1
        merged["companies"] += rep.get("companies") or 0
        merged["attempts"] += rep.get("attempts") or []
        merged["boards"] += rep.get("boards") or []
        merged["boards_fetched"] += rep.get("boards_fetched") or 0
        merged["requests"] += rep.get("requests") or 0
    return merged


def _companies(db, preset: Optional[str] = None, ciks=None, industrial_ids=None) -> List[Company]:
    cik_list = _list(ciks)
    ind_list = [int(x) for x in _list(industrial_ids)]
    seeds = load_seeds()
    if preset == "pilot":
        cik_list += [c for c in PILOT_CIKS if c not in cik_list]
        ind_list += [i for i in PILOT_INDUSTRIAL_IDS if i not in ind_list]
    elif preset == "migrated":
        ind_list += [i for i in migrated_industrial_ids() if i not in ind_list]
    elif preset == "active":
        out = active_companies(db)
        if cik_list or ind_list:
            raise ValueError("preset 'active' takes no ciks / industrial_ids")
        return out
    elif preset:
        raise ValueError(f"unknown preset {preset!r}")
    blocked = _blocked_keys(seeds)
    out: List[Company] = []
    for cik in cik_list:
        row = _entity_by_cik(db, cik)
        if not row:
            logger.warning(f"ats_boards: CIK {cik} has no live core entity; skipped")
            continue
        eid, name, dom = row
        co = Company(name=name, core_entity_id=eid, cik=str(int(cik)), domain=dom,
                     aliases=[a for a in _aliases(db, eid) if a != name])
        out.append(co)
    for iid in ind_list:
        row = db.execute(text("SELECT id, name, website, cik FROM industrial_companies WHERE id = :i"),
                         {"i": iid}).fetchone()
        if not row:
            logger.warning(f"ats_boards: industrial company {iid} not found; skipped")
            continue
        host = (row[2] or "").split("//")[-1].split("/")[0].lower()
        co = Company(name=row[1], industrial_company_id=row[0], domain=domains.registrable(host) if host else None,
                     cik=str(int(row[3])) if row[3] else None, tokens=_ats_config_tokens(db, row[0]))
        if co.cik:
            ent = _entity_by_cik(db, co.cik)
            co.core_entity_id = ent[0] if ent else None
        out.append(co)
    for co in out:
        for s in seeds:
            if s.get("blocked"):
                continue
            if (s.get("cik") and s["cik"] == co.cik) or (
                    s.get("industrial_company_id") and s["industrial_company_id"] == co.industrial_company_id):
                ev = {"citation": s["citation"]}
                if s.get("verified"):
                    ev["verified"] = s["verified"]
                co.tokens.append((s["ats"], s["token"], "seed", ev))
                co.aliases += [a for a in s.get("aliases") or [] if a not in co.aliases]
        # SPEC_152: a blocked board (Ashby, robots 401) is never offered to the fetcher
        co.tokens = [t for t in co.tokens if (t[0], t[1].lower()) not in blocked]
    return out


async def ingest_ats_boards(db, job_id: Optional[int] = None, preset: Optional[str] = None, ciks=None,
                            industrial_ids=None, apply: bool = False, **config) -> Dict[str, Any]:
    if preset == "active":
        companies = active_companies(db)
    else:
        companies = _companies(db, preset=preset, ciks=ciks, industrial_ids=industrial_ids)
    slugs = preset not in ("migrated", "active")
    # blocking HTTP: off the event loop so the worker heartbeat keeps ticking (SPEC_153)
    if preset == "active":
        rep = await asyncio.to_thread(run_chunked, db, companies, bool(apply), slugs)
    else:
        rep = await asyncio.to_thread(collect.run, db, companies, apply=bool(apply), slugs=slugs)
    logger.info(f"ats_boards job {job_id}: {rep['boards_fetched']} boards fetched of {len(companies)} companies, "
                f"{len(rep['attempts'])} attempts, apply={bool(apply)}")
    if rep["boards_fetched"] == 0:
        outcomes = sorted({a["outcome"] for a in rep["attempts"]})
        raise RuntimeError(f"ats_boards: no board fetched for {len(companies)} companies (outcomes: {outcomes})")
    return {"rows_inserted": sum((b.get("new") or 0) for b in rep["boards"]), "report": rep}
