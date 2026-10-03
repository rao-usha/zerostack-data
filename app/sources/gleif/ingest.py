"""
GLEIF LEI records -> gleif_lei_record (SPEC_154). Dispatch key ``gleif``; CLI below.

    python -m app.sources.gleif.ingest                 # DRY RUN: fetch, parse, count; writes nothing
    python -m app.sources.gleif.ingest --max-pages 2   # a short dry run
    python -m app.sources.gleif.ingest --apply         # load (upsert per page) + ledger row

Every request goes through ``app.core.open_web.OpenWebFetcher``: the recorded terms review for
gleif.org (``app/entities/data/site_terms.json``), robots.txt (api.gleif.org allows all;
goldencopy.gleif.org answers 403 = disallow-all, so the bulk zips are not used), the honest
NexdataResearch User-Agent, >= 2 s between requests (GLEIF allows 60/min). A 429 / 503 with
Retry-After is waited out on the fetcher's own clock and the same page retried; transport errors
and 5xx back off exponentially with jitter. At most ``MAX_TRIES`` attempts per page.

Ledger: one ``gleif_fetch`` row per applied run (outcome complete | partial | refused | error).
The dataset's clock is the golden copy publish date of the newest ``complete`` run; a partial
run keeps the pages it loaded but never moves the clock.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import random
from typing import Any, Callable, Dict, List, Optional, Sequence

from sqlalchemy import text

from app.core import open_web
from app.sources.gleif import client

logger = logging.getLogger(__name__)

MAX_TRIES = 3
MAX_WAIT = 900.0             # longest Retry-After we wait out before giving up the page
HOST_BACKOFF_WAIT = 60.0
MAX_BYTES = 8_000_000        # one 200-record page is ~350 KB
TIMEOUT = 60.0
MIN_INTERVAL = 2.0

_REFUSALS = ("robots_disallowed", "robots_crawl_delay_too_long", "terms_unreviewed", "terms_refused",
             "non_public_address", "dns_error", "bad_url", "redirect_offsite", "too_many_redirects")


def _backoff(attempt: int, rand: Callable[[], float]) -> float:
    return min(60.0, 2.0 * (2 ** attempt)) + rand()


def _get_json(fetcher, url: str, sleep: Callable[[float], None], rand: Callable[[], float]):
    """-> (doc | None, error | None, refused: bool, terms_citation)."""
    err, citation = None, None
    for attempt in range(MAX_TRIES):
        res = fetcher.get(url)
        citation = res.terms_citation or citation
        if res.outcome == "fetched":
            try:
                return json.loads(res.body or ""), None, False, citation
            except ValueError:
                err = "invalid JSON body"
        elif res.outcome == "retry_after":
            err = f"Retry-After {res.retry_after}"
            wait = res.retry_after if res.retry_after is not None else open_web.DEFAULT_BACKOFF
            if wait > MAX_WAIT:
                return None, f"Retry-After {wait:.0f}s exceeds {MAX_WAIT:.0f}s", False, citation
            sleep(wait)
            continue
        elif res.outcome == "host_backed_off":
            err = res.error
            sleep(HOST_BACKOFF_WAIT)
            continue
        elif res.outcome == "http_error" and (res.status or 0) < 500:
            return None, f"HTTP {res.status}", False, citation
        elif res.outcome in _REFUSALS:
            return None, f"{res.outcome}: {res.error or ''}".strip(), True, citation
        else:                                   # transport error or 5xx
            err = res.error or f"HTTP {res.status}"
        sleep(_backoff(attempt, rand))
    return None, f"gave up after {MAX_TRIES} tries: {err}", False, citation


def collect(fetcher, *, country: str = "US", on_page: Callable[[List[Dict[str, Any]]], None],
            max_pages: Optional[int] = None, sleep: Optional[Callable[[float], None]] = None,
            rand: Callable[[], float] = random.random) -> Dict[str, Any]:
    """Page the API; hand each page's rows to ``on_page``. -> the run report.

    ``clock`` is the golden copy publish date only when every page was read (``complete``)."""
    sleep = sleep or fetcher.sleep
    url: Optional[str] = client.first_url(country)
    pages = records = bad = 0
    publish, total, error, citation = None, None, None, None
    publishes: List[str] = []
    by_status: Dict[str, int] = {}
    outcome = "complete"
    while url:
        if max_pages is not None and pages >= max_pages:
            outcome, error = "partial", f"stopped at --max-pages {max_pages}"
            break
        doc, err, refused, cite = _get_json(fetcher, url, sleep, rand)
        citation = cite or citation
        if doc is None:
            outcome = "refused" if refused and pages == 0 else ("partial" if pages else "error")
            error = err
            break
        rows, b, nxt, pub, tot = client.parse_page(doc)
        if pub and pub not in publishes:
            publishes.append(pub)
        publish = publish or pub
        total = total if total is not None else tot
        for r in rows:
            r["golden_copy_publish_date"] = pub
            st = r.get("registration_status") or "?"
            by_status[st] = by_status.get(st, 0) + 1
        on_page(rows)
        pages += 1
        records += len(rows)
        bad += b
        if not rows and not b:
            break                               # an empty page ends the walk, whatever `next` says
        url = client.next_url(nxt, country)
    return {"outcome": outcome, "country": country, "pages": pages, "records": records,
            "records_without_lei": bad, "api_total": total, "publish_date": publish,
            "publish_dates_seen": publishes,
            "clock": min(publishes) if outcome == "complete" and publishes else None,
            "by_registration_status": by_status, "error": error, "terms_citation": citation,
            "requests": getattr(fetcher, "requests", None)}


# ---------------------------------------------------------------------------
# storage
# ---------------------------------------------------------------------------

_STORE_COLS = client.ROW_COLUMNS + ("golden_copy_publish_date", "last_seen_run_id")
_UPDATE_SET = (", ".join(f"{c} = EXCLUDED.{c}" for c in _STORE_COLS if c != "lei")
               + ", loaded_at = NOW()")


def _upsert(db, rows: List[Dict[str, Any]], run_id: Optional[int]) -> None:
    """One multi-row INSERT per page (parameterized). A row-by-row executemany costs one
    round trip per row through the Cloud SQL proxy (~10 s per 200-row page, measured)."""
    rows = list({r["lei"]: r for r in rows}.values())   # ON CONFLICT cannot touch one row twice
    params: Dict[str, Any] = {}
    values = []
    for i, r in enumerate(rows):
        values.append("(" + ", ".join(f":{c}_{i}" for c in _STORE_COLS) + ", NOW())")
        for c in _STORE_COLS:
            params[f"{c}_{i}"] = run_id if c == "last_seen_run_id" else r.get(c)
    db.execute(text(f"INSERT INTO gleif_lei_record ({', '.join(_STORE_COLS)}, loaded_at) VALUES "
                    + ", ".join(values) + f" ON CONFLICT (lei) DO UPDATE SET {_UPDATE_SET}"), params)


_LEDGER_START = text(
    "INSERT INTO gleif_fetch (started_at, country, outcome, user_agent) "
    "VALUES (NOW(), :country, 'running', :ua) RETURNING id"
)
_LEDGER_END = text(
    "UPDATE gleif_fetch SET finished_at = NOW(), outcome = :outcome, pages = :pages, "
    "records = :records, requests = :requests, api_total = :api_total, "
    "golden_copy_publish_date = CAST(:publish AS TIMESTAMPTZ), terms_citation = :citation, "
    "error = :error, report = CAST(:report AS JSONB) WHERE id = :id"
)


def run(db, *, apply: bool = False, country: str = "US", max_pages: Optional[int] = None,
        fetcher=None, sleep: Optional[Callable[[float], None]] = None) -> Dict[str, Any]:
    """Fetch every LEI with a ``country`` legal address. Dry run (default) writes nothing."""
    own = fetcher is None
    if own:
        from app.entities.domain_probe import TERMS_PATH

        fetcher = open_web.OpenWebFetcher(open_web.load_terms(TERMS_PATH), max_bytes=MAX_BYTES,
                                          timeout=TIMEOUT, min_interval=MIN_INTERVAL)
    run_id = None
    if apply:
        run_id = db.execute(_LEDGER_START, {"country": country, "ua": fetcher.user_agent}).scalar()
        db.commit()

    def on_page(rows: List[Dict[str, Any]]) -> None:
        if apply and rows:
            _upsert(db, rows, run_id)
            db.commit()

    try:
        rep = collect(fetcher, country=country, on_page=on_page, max_pages=max_pages, sleep=sleep)
    except BaseException as exc:
        if apply:
            db.rollback()
            db.execute(_LEDGER_END, {"id": run_id, "outcome": "error", "pages": None, "records": None,
                                     "requests": fetcher.requests, "api_total": None, "publish": None,
                                     "citation": None, "error": f"{type(exc).__name__}: {exc}"[:2000],
                                     "report": json.dumps({})})
            db.commit()
        raise
    finally:
        if own:
            fetcher.close()
    rep.update({"apply": apply, "run_id": run_id})
    if apply:
        db.execute(_LEDGER_END, {"id": run_id, "outcome": rep["outcome"], "pages": rep["pages"],
                                 "records": rep["records"], "requests": rep["requests"],
                                 "api_total": rep["api_total"], "publish": rep["publish_date"],
                                 "citation": rep["terms_citation"], "error": rep["error"],
                                 "report": json.dumps(rep, default=str)})
        db.commit()
    return rep


async def ingest_gleif(db, job_id: Optional[int] = None, apply: bool = False, max_pages=None,
                       country: str = "US", **config) -> Dict[str, Any]:
    """Job entry (dispatch ``gleif``). Fails when nothing could be read (refused / error)."""
    rep = await asyncio.to_thread(run, db, apply=bool(apply), country=country or "US",
                                  max_pages=int(max_pages) if max_pages else None)
    logger.info(f"gleif job {job_id}: {rep['outcome']} {rep['records']} records / {rep['pages']} pages, "
                f"apply={bool(apply)}")
    if rep["outcome"] in ("refused", "error"):
        raise RuntimeError(f"gleif: {rep['outcome']}: {rep['error']}")
    return {"rows_inserted": rep["records"] if apply else 0, "report": rep}


def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--apply", action="store_true", help="write (default: dry run, writes nothing)")
    ap.add_argument("--max-pages", type=int, help="stop after N pages (the run is then 'partial')")
    ap.add_argument("--country", default="US", help="legal-address country (ISO 3166 alpha-2)")
    ap.add_argument("--json", help="write the report to this path")
    a = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO)
    from app.core.database import get_session_factory

    db = get_session_factory()()
    try:
        rep = run(db, apply=a.apply, country=a.country, max_pages=a.max_pages)
    finally:
        db.close()
    if a.json:
        with open(a.json, "w", encoding="utf-8") as fh:
            json.dump(rep, fh, indent=2, default=str)
    print(f"{'APPLIED' if a.apply else 'DRY RUN'}: {rep['outcome']}, {rep['records']} records in {rep['pages']} "
          f"pages, {rep['requests']} HTTP requests, api total {rep['api_total']}, golden copy "
          f"{rep['publish_date']}, run_id {rep['run_id']}")
    print(f"  by registration status: {rep['by_registration_status']}")
    if rep["error"]:
        print(f"  error: {rep['error']}")
    return 0 if rep["outcome"] in ("complete", "partial") else 1


if __name__ == "__main__":
    raise SystemExit(main())
