"""
Domain probe collector (SPEC_148): redirect / rebrand evidence and own-site names.

WHAT IT DOES. For a SMALL sample of domains already linked in `core.domain_link`,
request the homepage `https://<domain>/` once, through `app.core.open_web`
(terms review required, robots.txt per origin, honest NexdataResearch UA,
Retry-After honoured, >= 2 s per host, public addresses only, nothing retried),
and record what happened in `core.domain_probe`:

- `redirect_offsite` + `redirect_to`: the site sends visitors to another
  registrable domain. The resolver records the old domain as an ALIAS of the new
  one for every subject that named it (rebrands: GlossGenius -> genius.ai). The
  target is never requested here.
- `fetched` + `names_found`: which of the subject's normalized legal names the
  homepage text contains (token-bounded, names shorter than `MIN_NAME_CHARS`
  never count). A match is the `own_site` source family: the company's own site
  naming the filer's legal name is the second independent source the two-source
  rule asks for.
- any refusal (`terms_unreviewed`, `robots_disallowed`, `retry_after`, ...) is
  recorded too, so a re-run can see what was not asked and why.

WHAT IT IS NOT. Not a crawler: one page per domain, no link following, no
content stored beyond the matched names and the hop list. Sites without a
recorded terms review (`app/entities/data/site_terms.json`) get ZERO requests.

    python -m app.entities.domain_probe                      # DRY RUN: the sample and its gates
    python -m app.entities.domain_probe --limit 10 --apply   # probe and write core.domain_probe
    python -m app.entities.domain_probe --domains a.com,b.com --apply

The resolver reads the latest probe per domain on its next run (entity_resolve).
"""

from __future__ import annotations

import argparse
import html
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

from sqlalchemy import text

from app.core import open_web
from app.entities import norm

TERMS_PATH = Path(__file__).resolve().parent / "data" / "site_terms.json"
MIN_NAME_CHARS = 4
SAMPLE_MAX = 25                 # small sample only: a hard ceiling per run
_TAG = re.compile(r"<(script|style)\b.*?</\1>|<[^>]+>", re.S | re.I)

PROBE_COLUMNS = ("domain", "probed_at", "url", "outcome", "http_status", "redirect_to", "final_url",
                 "hops", "names_checked", "names_found", "retry_after_seconds", "user_agent",
                 "terms_citation", "error")


def _page_text(body: Optional[str]) -> str:
    return " " + norm._fold(html.unescape(_TAG.sub(" ", body or ""))) + " "


def names_on_page(body: Optional[str], names: Sequence[str]) -> List[str]:
    page = _page_text(body)
    found = []
    for n in sorted({x for x in names if x}):
        folded = norm._fold(n)
        if len(folded) >= MIN_NAME_CHARS and f" {folded} " in page:
            found.append(n)
    return found


def probe_domain(fetcher: open_web.OpenWebFetcher, domain: str, names: Sequence[str]) -> Dict[str, Any]:
    """One homepage request (plus robots.txt) -> a core.domain_probe row."""
    url = f"https://{domain}/"
    res = fetcher.get(url)
    return {
        "domain": domain,
        "probed_at": datetime.now(timezone.utc).replace(tzinfo=None),
        "url": url,
        "outcome": res.outcome,
        "http_status": res.status,
        "redirect_to": res.redirect_to,
        "final_url": res.final_url,
        "hops": res.hops,
        "names_checked": sorted({n for n in names if n}),
        "names_found": names_on_page(res.body, names) if res.outcome == "fetched" else [],
        "retry_after_seconds": res.retry_after,
        "user_agent": fetcher.user_agent,
        "terms_citation": res.terms_citation,
        "error": res.error,
    }


def select_sample(conn, terms: Dict[str, open_web.Review], limit: int,
                  only: Optional[Sequence[str]] = None) -> List[Dict[str, Any]]:
    """Linked domains whose site has an 'allowed' terms review -> [{domain, names}].
    The names are the normalized names of the records that claimed the domain."""
    allowed = sorted(d for d, r in terms.items() if r.verdict == "allowed")
    if only:
        allowed = [d for d in allowed if d in set(only)]
    if not allowed:
        return []
    rows = conn.execute(
        text(
            """
            SELECT dl.domain, array_agg(DISTINCT sr.name_norm) FILTER (WHERE sr.name_norm IS NOT NULL) AS names
            FROM core.domain_link dl
            CROSS JOIN LATERAL jsonb_array_elements(dl.claims) c
            JOIN core.source_record sr ON sr.record_key = c->>'record_key'
            WHERE dl.domain = ANY(:domains) AND dl.status IN ('strong', 'weak', 'conflict')
            GROUP BY dl.domain ORDER BY dl.domain LIMIT :limit
            """
        ),
        {"domains": allowed, "limit": min(limit, SAMPLE_MAX)},
    ).mappings()
    return [{"domain": r["domain"], "names": list(r["names"] or [])} for r in rows]


def write_rows(conn, rows: Sequence[Dict[str, Any]]) -> int:
    for r in rows:
        params = dict(r)
        for k in ("hops", "names_checked", "names_found"):
            params[k] = json.dumps(params[k])
        conn.execute(
            text(
                f"INSERT INTO core.domain_probe ({', '.join(PROBE_COLUMNS)}) VALUES ("
                + ", ".join(f"CAST(:{c} AS JSONB)" if c in ("hops", "names_checked", "names_found") else f":{c}"
                            for c in PROBE_COLUMNS)
                + ") ON CONFLICT (domain, probed_at) DO NOTHING"
            ),
            params,
        )
    return len(rows)


def run(conn, fetcher: open_web.OpenWebFetcher, sample: Sequence[Dict[str, Any]], apply: bool) -> Dict[str, Any]:
    rows = [probe_domain(fetcher, s["domain"], s["names"]) for s in sample[:SAMPLE_MAX]]
    outcomes: Dict[str, int] = {}
    for r in rows:
        outcomes[r["outcome"]] = outcomes.get(r["outcome"], 0) + 1
    written = write_rows(conn, rows) if apply else 0
    return {"sampled": len(rows), "requests": fetcher.requests, "outcomes": outcomes,
            "written": written, "rows": rows}


def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--limit", type=int, default=10)
    ap.add_argument("--domains", default="", help="comma-separated: probe only these (still terms-gated)")
    ap.add_argument("--apply", action="store_true", help="send requests and write core.domain_probe")
    args = ap.parse_args(argv)

    from app.core.database import get_engine

    terms = open_web.load_terms(TERMS_PATH)
    only = [d.strip().lower() for d in args.domains.split(",") if d.strip()] or None
    with get_engine().connect() as conn:
        sample = select_sample(conn, terms, args.limit, only)
        print(f"terms reviews: {len(terms)} ({sum(r.verdict == 'allowed' for r in terms.values())} allowed); "
              f"sample: {len(sample)}")
        for s in sample:
            print(f"  {s['domain']}  names={s['names'][:5]}")
        if not args.apply:
            print("DRY RUN: no request sent, nothing written (--apply to probe)")
            return 0
        fetcher = open_web.OpenWebFetcher(terms)
        try:
            conn.rollback()                  # end the read-only autobegin of select_sample
            out = run(conn, fetcher, sample, apply=True)
            conn.commit()
        finally:
            fetcher.close()
        for r in out["rows"]:
            print(f"  {r['domain']}: {r['outcome']} status={r['http_status']} redirect_to={r['redirect_to']} "
                  f"names_found={r['names_found']} hops={len(r['hops'])}")
        print(json.dumps({k: v for k, v in out.items() if k != "rows"}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
