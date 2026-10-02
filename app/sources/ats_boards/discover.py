"""
Board-token discovery from data NexData already holds (SPEC_151). Pure: no I/O.

Where a token may come from, in order:
1. ``company_ats_config`` -- a non-generic token the old lane already recorded;
2. a cited seed (``data/board_seeds.json``), e.g. a token seen in a dated probe;
3. the company's held domain (first label of the registrable domain) and its legal name
   from ``core.entity`` -- at most ``MAX_SLUGS`` slugs per company.

Slugs are not guesses at scale: a run takes at most ``MAX_COMPANIES`` companies, every
request is gated, and a board is used ONLY when ``verify`` accepts it:
- Greenhouse: the board's own ``name`` equals one of the company's names after legal
  suffixes are stripped (``app.entities.norm.norm``);
- Lever / Ashby (no board name in the API): the company's name appears, token-bounded,
  in at least half the postings (and there is at least one posting).
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Sequence, Tuple

from app.entities import norm

MAX_SLUGS = 2
MAX_COMPANIES = 25
MIN_MENTION_SHARE = 0.5

# trailing words a company's board slug often drops ("Rockfish Data" -> "rockfish")
_GENERIC_TAIL = {"data", "ai", "labs", "lab", "technologies", "technology", "tech", "software", "solutions",
                 "systems", "group", "holdings", "hq", "app", "apps", "platform", "health", "studio", "studios"}


def slug_candidates(name: Optional[str], domain: Optional[str] = None) -> List[str]:
    out: List[str] = []

    def add(s: str) -> None:
        s = re.sub(r"[^a-z0-9-]", "", s.lower()).strip("-")
        if s and len(s) >= 3 and s not in out:
            out.append(s)

    if domain:
        add(domain.lower().split(".")[0])
    n = norm.norm(name) if name else None
    if n:
        toks = n.split()
        add("".join(toks))
        short = list(toks)
        while len(short) > 1 and short[-1] in _GENERIC_TAIL:
            short.pop()
        if short != toks:
            add("".join(short))
        if len(toks) > 1:
            add("-".join(toks))
    return out[:MAX_SLUGS]


def _names(names: Sequence[str]) -> List[str]:
    return sorted({x for x in (norm.norm(n) for n in names or []) if x})


def _mentions(names: List[str], text: str) -> bool:
    page = " " + norm._fold(text or "") + " "
    return any(len(n) >= 3 and f" {n} " in page for n in names)


def _job_text(job: Dict[str, Any]) -> str:
    from app.sources.ats_boards.pay import clean

    parts = [job.get("descriptionPlain"), job.get("additionalPlain"), clean(job.get("content")),
             clean(job.get("description")), job.get("company_name")]
    return " ".join(p for p in parts if p)


def verify(ats: str, company_names: Sequence[str], meta: Optional[Dict[str, Any]],
           jobs: Sequence[Dict[str, Any]]) -> Tuple[bool, Dict[str, Any]]:
    names = _names(company_names)
    if not names:
        return False, {"rule": "no_company_name"}
    if ats == "greenhouse":
        board_name = (meta or {}).get("name")
        ok = bool(board_name) and norm.norm(board_name) in names
        return ok, {"rule": "greenhouse_board_name", "board_name": board_name, "names": names}
    hits = sum(1 for j in jobs or [] if _mentions(names, _job_text(j)))
    total = len(jobs or [])
    ok = total > 0 and hits / total >= MIN_MENTION_SHARE
    return ok, {"rule": "name_in_postings", "mentions": hits, "postings": total, "names": names}
