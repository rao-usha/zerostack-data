"""
ATS boards -- job-path entry point and company loading (SPEC_151).

Dispatch key ``ats_boards`` (app/api/v1/jobs.py). Config: ``preset`` ("pilot"), ``ciks``,
``industrial_ids`` (comma-separated or lists), ``apply`` (default False: a dry run that
writes nothing). The job FAILS (raises) when no board could be fetched at all, so a lane
that collected nothing never reports success.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Union

from sqlalchemy import text

from app.entities import domains
from app.sources.ats_boards import collect
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


def _companies(db, preset: Optional[str] = None, ciks=None, industrial_ids=None) -> List[Company]:
    cik_list = _list(ciks)
    ind_list = [int(x) for x in _list(industrial_ids)]
    if preset == "pilot":
        cik_list += [c for c in PILOT_CIKS if c not in cik_list]
        ind_list += [i for i in PILOT_INDUSTRIAL_IDS if i not in ind_list]
    elif preset:
        raise ValueError(f"unknown preset {preset!r}")
    seeds = load_seeds()
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
            if (s.get("cik") and s["cik"] == co.cik) or (
                    s.get("industrial_company_id") and s["industrial_company_id"] == co.industrial_company_id):
                co.tokens.append((s["ats"], s["token"], "seed", {"citation": s["citation"]}))
                co.aliases += [a for a in s.get("aliases") or [] if a not in co.aliases]
    return out


async def ingest_ats_boards(db, job_id: Optional[int] = None, preset: Optional[str] = None, ciks=None,
                            industrial_ids=None, apply: bool = False, **config) -> Dict[str, Any]:
    companies = _companies(db, preset=preset, ciks=ciks, industrial_ids=industrial_ids)
    rep = collect.run(db, companies, apply=bool(apply))
    logger.info(f"ats_boards job {job_id}: {rep['boards_fetched']} boards fetched of {len(companies)} companies, "
                f"{len(rep['attempts'])} attempts, apply={bool(apply)}")
    if rep["boards_fetched"] == 0:
        outcomes = sorted({a["outcome"] for a in rep["attempts"]})
        raise RuntimeError(f"ats_boards: no board fetched for {len(companies)} companies (outcomes: {outcomes})")
    return {"rows_inserted": sum((b.get("new") or 0) for b in rep["boards"]), "report": rep}
