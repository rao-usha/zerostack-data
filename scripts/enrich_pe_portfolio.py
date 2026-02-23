#!/usr/bin/env python3
"""
Enrich PE portfolio companies with industry, sector, location, and other metadata.

Portfolio companies from 13F filings only have names and CUSIPs. This script
enriches them in two phases:

Phase 1 (yfinance): Resolve ticker via Yahoo Finance search, then fetch full
company profile — industry, sector, HQ location, employee count, website, description.

Phase 2 (SEC EDGAR): For companies where a ticker was resolved, fetch SIC codes
from SEC's EDGAR submissions data.

Usage:
    python scripts/enrich_pe_portfolio.py                    # All sparse companies
    python scripts/enrich_pe_portfolio.py --firm Blackstone  # Only Blackstone's
    python scripts/enrich_pe_portfolio.py --limit 10         # Test with 10
    python scripts/enrich_pe_portfolio.py --dry-run          # Preview only
    python scripts/enrich_pe_portfolio.py --skip-sec         # Skip SEC phase
"""

import argparse
import asyncio
import io
import logging
import re
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

sys.path.insert(0, str(Path(__file__).parent.parent))

import httpx

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Terminal colours
# ---------------------------------------------------------------------------

class C:
    G = "\033[92m"
    Y = "\033[93m"
    R = "\033[91m"
    B = "\033[94m"
    CY = "\033[96m"
    DIM = "\033[2m"
    BD = "\033[1m"
    E = "\033[0m"


def banner(text: str):
    print(f"\n{C.BD}{C.B}{'=' * 64}{C.E}")
    print(f"{C.BD}{C.B}  {text}{C.E}")
    print(f"{C.BD}{C.B}{'=' * 64}{C.E}\n")


def ok(text: str):
    print(f"  {C.G}[OK]{C.E}  {text}")


def fail(text: str):
    print(f"  {C.R}[FAIL]{C.E}  {text}")


def info(text: str):
    print(f"  {C.CY}[..]{C.E}  {text}")


def warn(text: str):
    print(f"  {C.Y}[!!]{C.E}  {text}")


def skip(text: str):
    print(f"  {C.DIM}[--]{C.E}  {C.DIM}{text}{C.E}")


# ---------------------------------------------------------------------------
# Name normalization — strip 13F suffixes for better Yahoo Finance matching
# ---------------------------------------------------------------------------

# Common 13F suffixes to strip (order matters — longer first)
STRIP_SUFFIXES = [
    "HOLDINGS INC", "HOLDING INC", "HOLDINGS CORP", "HOLDING CORP",
    "HOLDINGS CO", "HOLDING CO", "HOLDINGS LLC", "HOLDING LLC",
    "HOLDINGS LTD", "HOLDING LTD", "HOLDINGS PLC", "HOLDING PLC",
    "HOLDINGS LP", "HOLDING LP",
    "GROUP INC", "GROUP CORP", "GROUP CO", "GROUP LTD", "GROUP PLC", "GROUP LLC",
    "TECHNOLOGIES INC", "TECHNOLOGY INC", "TECHNOLOGIES CORP", "TECHNOLOGY CORP",
    "TECHNOLOGIES LTD", "TECHNOLOGY LTD",
    "INTERNATIONAL INC", "INTERNATIONAL CORP", "INTERNATIONAL LTD",
    "INTERNATIONAL PLC",
    "ENTERPRISES INC", "ENTERPRISE INC", "ENTERPRISES CORP",
    "SOLUTIONS INC", "SOLUTIONS CORP",
    "INDUSTRIES INC", "INDUSTRIES CORP",
    "SYSTEMS INC", "SYSTEMS CORP",
    "INC", "CORP", "CO", "LTD", "PLC", "LLC", "LP", "LLP",
    "CLASS A", "CLASS B", "CLASS C", "CL A", "CL B", "CL C",
    "SER A", "SER B", "SERIES A", "SERIES B",
    "COM", "COMMON", "ORD", "ORDINARY",
    "ADR", "ADS", "NEW", "DEL",
]


def normalize_13f_name(raw_name: str) -> str:
    """
    Normalize a 13F filing name for Yahoo Finance search.

    '13F names are ALL CAPS with entity suffixes: "APPLE INC", "ALPHABET INC CL A"
    We strip suffixes and convert to title case for better search matching.
    """
    name = raw_name.strip().upper()

    # Strip suffixes (longest first to avoid partial matches)
    for suffix in STRIP_SUFFIXES:
        pattern = r"\b" + re.escape(suffix) + r"\b"
        name = re.sub(pattern, "", name).strip()

    # Collapse whitespace
    name = re.sub(r"\s+", " ", name).strip()

    # Title case for search
    return name.title() if name else raw_name.strip().title()


# ---------------------------------------------------------------------------
# Yahoo Finance search & profile
# ---------------------------------------------------------------------------

# US exchanges in Yahoo Finance
US_EXCHANGES = {"NYQ", "NMS", "NGM", "NCM", "PCX", "ASE", "BTS", "NYS", "NAS"}

# 13F filings store security class descriptors in the ticker field.
# Valid tickers are 1-8 alphanumeric chars, optionally with a dot or hyphen.
_VALID_TICKER_RE = re.compile(r"^[A-Z]{1,5}([.-][A-Z]{1,3})?$")


def is_valid_ticker(ticker: Optional[str]) -> bool:
    """Check if a string looks like a real stock ticker (not a 13F class descriptor)."""
    if not ticker:
        return False
    return bool(_VALID_TICKER_RE.match(ticker.upper().strip()))


def _search_ticker_sync(company_name: str) -> Optional[str]:
    """
    Search Yahoo Finance for a US equity ticker by company name.

    Uses yfinance's built-in Search class which handles cookies/crumbs.
    Prefers US-listed equities (symbols without dots on NYSE/NASDAQ).
    """
    try:
        import yfinance as yf
    except ImportError:
        return None

    try:
        s = yf.Search(company_name, max_results=8)
        quotes = s.quotes or []
    except Exception as e:
        logger.debug(f"yfinance search failed for '{company_name}': {e}")
        return None

    # Filter to equities only
    equities = [q for q in quotes if q.get("quoteType") == "EQUITY"]
    if not equities:
        return None

    # Prefer US-listed: no dot in symbol (e.g. "ABT" not "ABT.TO")
    us_equities = [q for q in equities if "." not in q.get("symbol", ".")]
    if us_equities:
        return us_equities[0]["symbol"]

    # Fallback: prefer known US exchanges
    for q in equities:
        if q.get("exchange") in US_EXCHANGES:
            return q["symbol"]

    # Last resort: first equity
    return equities[0].get("symbol")


async def search_ticker(company_name: str) -> Optional[str]:
    """Async wrapper — runs yfinance search in thread pool."""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, _search_ticker_sync, company_name)


def fetch_yfinance_info(ticker: str) -> Optional[Dict[str, Any]]:
    """Fetch ticker info via yfinance (synchronous — called from thread pool)."""
    try:
        import yfinance as yf
    except ImportError:
        logger.error("yfinance package not installed — pip install yfinance")
        return None

    try:
        t = yf.Ticker(ticker)
        info = t.info
        if not info or not info.get("symbol"):
            return None
        return info
    except Exception as e:
        logger.debug(f"yfinance fetch failed for {ticker}: {e}")
        return None


def extract_profile(info: Dict[str, Any]) -> Dict[str, Any]:
    """Extract profile fields from yfinance info dict."""
    return {
        "industry": info.get("industry"),
        "sector": info.get("sector"),
        "description": info.get("longBusinessSummary"),
        "employee_count": info.get("fullTimeEmployees"),
        "headquarters_city": info.get("city"),
        "headquarters_state": info.get("state"),
        "headquarters_country": info.get("country"),
        "website": info.get("website"),
        "ticker": info.get("symbol"),
        "founded_year": None,  # yfinance doesn't provide this
    }


# ---------------------------------------------------------------------------
# SEC EDGAR — SIC code lookup
# ---------------------------------------------------------------------------

SEC_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
SEC_SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik}.json"
SEC_HEADERS = {
    "User-Agent": "NexdataResearch/1.0 (research@nexdata.com; respectful research bot)",
    "Accept-Encoding": "gzip, deflate",
}


async def fetch_sec_maps(
    client: httpx.AsyncClient,
) -> Tuple[Dict[str, str], Dict[str, str]]:
    """
    Fetch SEC company_tickers.json → two lookup maps:
      1. {TICKER: CIK} — lookup by stock ticker
      2. {NORMALIZED_NAME: CIK} — lookup by company title (for 13F name matching)
    """
    try:
        resp = await client.get(SEC_TICKERS_URL, headers=SEC_HEADERS, timeout=30)
        if resp.status_code != 200:
            warn(f"SEC tickers fetch failed: HTTP {resp.status_code}")
            return {}, {}

        data = resp.json()
        ticker_map = {}
        name_map = {}
        for entry in data.values():
            tick = entry.get("ticker", "").upper()
            cik = str(entry.get("cik_str", "")).zfill(10)
            title = entry.get("title", "").upper().strip()
            if tick:
                ticker_map[tick] = cik
            if title:
                name_map[title] = cik
        return ticker_map, name_map
    except Exception as e:
        warn(f"SEC tickers fetch error: {e}")
        return {}, {}


async def fetch_sec_sic(
    client: httpx.AsyncClient, cik: str
) -> Optional[Tuple[str, str]]:
    """Fetch SIC code and description from SEC submissions for a CIK."""
    url = SEC_SUBMISSIONS_URL.format(cik=cik)
    try:
        resp = await client.get(url, headers=SEC_HEADERS, timeout=10)
        if resp.status_code != 200:
            return None

        data = resp.json()
        sic = data.get("sic")
        sic_desc = data.get("sicDescription")
        if sic:
            return (str(sic), sic_desc or "")
    except Exception as e:
        logger.debug(f"SEC SIC fetch failed for CIK {cik}: {e}")

    return None


# ---------------------------------------------------------------------------
# Database operations
# ---------------------------------------------------------------------------

def get_sparse_companies(
    db, firm_name: Optional[str] = None, limit: Optional[int] = None
) -> List[Dict]:
    """
    Query portfolio companies that are missing industry/sector.

    Returns list of dicts with id, name, ticker, firm_name.
    """
    from sqlalchemy import text

    sql = """
        SELECT pc.id, pc.name, pc.ticker,
               pf.name AS firm_name
        FROM pe_portfolio_companies pc
        LEFT JOIN pe_fund_investments fi ON fi.company_id = pc.id
        LEFT JOIN pe_funds f ON f.id = fi.fund_id
        LEFT JOIN pe_firms pf ON pf.id = f.firm_id
        WHERE pc.industry IS NULL
          AND pc.sector IS NULL
    """
    params = {}

    if firm_name:
        sql += " AND pf.name ILIKE :firm_name"
        params["firm_name"] = f"%{firm_name}%"

    sql += " ORDER BY pc.name"

    if limit:
        sql += " LIMIT :limit"
        params["limit"] = limit

    rows = db.execute(text(sql), params).fetchall()
    return [
        {
            "id": r[0],
            "name": r[1],
            "ticker": r[2],
            "firm_name": r[3],
        }
        for r in rows
    ]


def get_companies_needing_sic(
    db, firm_name: Optional[str] = None, limit: Optional[int] = None
) -> List[Tuple[int, str, str]]:
    """
    Query portfolio companies that have industry but no SIC code.

    Returns list of (id, name, ticker) tuples for Phase 2.
    """
    from sqlalchemy import text

    sql = """
        SELECT DISTINCT pc.id, pc.name, pc.ticker
        FROM pe_portfolio_companies pc
        LEFT JOIN pe_fund_investments fi ON fi.company_id = pc.id
        LEFT JOIN pe_funds f ON f.id = fi.fund_id
        LEFT JOIN pe_firms pf ON pf.id = f.firm_id
        WHERE pc.industry IS NOT NULL
          AND pc.sic_code IS NULL
    """
    params = {}

    if firm_name:
        sql += " AND pf.name ILIKE :firm_name"
        params["firm_name"] = f"%{firm_name}%"

    sql += " ORDER BY pc.name"

    if limit:
        sql += " LIMIT :limit"
        params["limit"] = limit

    rows = db.execute(text(sql), params).fetchall()
    return [(r[0], r[1], r[2]) for r in rows]


def null_preserving_update(db, company_id: int, updates: Dict[str, Any]) -> int:
    """
    Update pe_portfolio_companies with COALESCE — only fills NULL columns.

    Returns 1 if row was updated, 0 otherwise.
    """
    from sqlalchemy import text

    # Filter out None values from updates
    fields = {k: v for k, v in updates.items() if v is not None}
    if not fields:
        return 0

    # Build COALESCE SET clauses: only overwrite if existing value is NULL
    set_clauses = []
    params = {"cid": company_id}
    for col, val in fields.items():
        set_clauses.append(f"{col} = COALESCE({col}, :{col})")
        params[col] = val

    # Always update updated_at
    set_clauses.append("updated_at = NOW()")

    sql = f"""
        UPDATE pe_portfolio_companies
        SET {', '.join(set_clauses)}
        WHERE id = :cid
    """

    result = db.execute(text(sql), params)
    return result.rowcount


# ---------------------------------------------------------------------------
# Main enrichment logic
# ---------------------------------------------------------------------------

async def enrich_phase1_yfinance(
    db,
    companies: List[Dict],
    dry_run: bool = False,
) -> Dict[str, Any]:
    """
    Phase 1: Enrich portfolio companies via yfinance.

    Returns stats dict and list of (company_id, ticker) for Phase 2.
    """
    banner("Phase 1: Yahoo Finance Enrichment")

    stats = {
        "total": len(companies),
        "enriched": 0,
        "no_ticker": 0,
        "no_profile": 0,
        "errors": 0,
    }
    ticker_map: List[Tuple[int, str]] = []  # (company_id, ticker) for Phase 2

    loop = asyncio.get_running_loop()

    for i, company in enumerate(companies, 1):
        cid = company["id"]
        raw_name = company["name"]
        existing_ticker = company.get("ticker")

        progress = f"[{i}/{len(companies)}]"
        search_name = normalize_13f_name(raw_name)

        try:
            # Step 1: Resolve ticker via search (13F ticker field is unreliable —
            # stores security types like "COM", "CL A", "SPONSORED ADS")
            ticker = await search_ticker(search_name)
            if not ticker:
                # Try raw name as fallback
                ticker = await search_ticker(raw_name)

            if not ticker:
                skip(f"{progress} {raw_name} → no ticker found")
                stats["no_ticker"] += 1
                await asyncio.sleep(0.5)
                continue

            # Step 2: Fetch yfinance profile (sync, in thread pool)
            yf_info = await loop.run_in_executor(None, fetch_yfinance_info, ticker)

            if not yf_info:
                skip(f"{progress} {raw_name} → {ticker} (no yfinance data)")
                stats["no_profile"] += 1
                # Still record ticker for SEC phase
                ticker_map.append((cid, ticker))
                await asyncio.sleep(1.0)
                continue

            # Step 3: Extract profile
            profile = extract_profile(yf_info)
            industry = profile.get("industry") or "-"
            city = profile.get("headquarters_city") or "-"
            state = profile.get("headquarters_state") or ""
            location = f"{city}, {state}".rstrip(", ")

            if dry_run:
                ok(
                    f"{progress} {raw_name} → {ticker} | "
                    f"{industry} | {location} {C.DIM}(dry run){C.E}"
                )
            else:
                # Null-preserving update
                updated = null_preserving_update(db, cid, profile)
                if updated:
                    ok(
                        f"{progress} {raw_name} → {ticker} | "
                        f"{industry} | {location}"
                    )
                else:
                    skip(f"{progress} {raw_name} → {ticker} (no new data)")

            stats["enriched"] += 1
            ticker_map.append((cid, ticker))

        except Exception as e:
            fail(f"{progress} {raw_name}: {e}")
            stats["errors"] += 1

        # Rate limit: 1s between yfinance calls
        await asyncio.sleep(1.0)

    # Summary
    info(
        f"Phase 1 complete: {stats['enriched']} enriched, "
        f"{stats['no_ticker']} no ticker, "
        f"{stats['no_profile']} no profile, "
        f"{stats['errors']} errors"
    )

    return {"stats": stats, "ticker_map": ticker_map}


async def enrich_phase2_sec(
    db,
    ticker_map: List[tuple],
    dry_run: bool = False,
) -> Dict[str, Any]:
    """
    Phase 2: Enrich SIC codes from SEC EDGAR.

    Accepts tuples of either:
      - (company_id, ticker) from Phase 1
      - (company_id, name, ticker) from --sec-only mode

    Looks up CIK by ticker first, then falls back to name matching.
    """
    banner("Phase 2: SEC EDGAR SIC Codes")

    if not ticker_map:
        info("No companies to look up — skipping SEC phase")
        return {"enriched": 0, "not_found": 0, "errors": 0}

    stats = {"enriched": 0, "not_found": 0, "errors": 0}

    async with httpx.AsyncClient() as client:
        # Fetch ticker → CIK and name → CIK maps
        info("Fetching SEC company_tickers.json...")
        sec_ticker_cik, sec_name_cik = await fetch_sec_maps(client)
        info(f"Loaded {len(sec_ticker_cik)} SEC tickers, {len(sec_name_cik)} company names")

        for i, entry in enumerate(ticker_map, 1):
            # Support both (id, ticker) and (id, name, ticker) tuples
            if len(entry) == 3:
                cid, name, ticker = entry
            else:
                cid, ticker = entry
                name = None

            progress = f"[{i}/{len(ticker_map)}]"
            display = name or ticker or str(cid)

            # Try ticker lookup first (if it's a valid ticker)
            cik = None
            if ticker and is_valid_ticker(ticker):
                cik = sec_ticker_cik.get(ticker.upper())

            # Fallback: match by company name
            if not cik and name:
                cik = sec_name_cik.get(name.upper().strip())

            if not cik:
                stats["not_found"] += 1
                continue

            try:
                result = await fetch_sec_sic(client, cik)
                if result:
                    sic_code, sic_desc = result
                    if dry_run:
                        ok(f"{progress} {display} → SIC {sic_code} ({sic_desc}) {C.DIM}(dry run){C.E}")
                    else:
                        from sqlalchemy import text
                        db.execute(
                            text("""
                                UPDATE pe_portfolio_companies
                                SET sic_code = COALESCE(sic_code, :sic),
                                    updated_at = NOW()
                                WHERE id = :cid
                            """),
                            {"cid": cid, "sic": sic_code},
                        )
                        ok(f"{progress} {display} → SIC {sic_code} ({sic_desc})")
                    stats["enriched"] += 1
                else:
                    stats["not_found"] += 1

            except Exception as e:
                fail(f"{progress} {display}: {e}")
                stats["errors"] += 1

            # SEC rate limit: 10 req/sec max
            await asyncio.sleep(0.1)

    info(
        f"Phase 2 complete: {stats['enriched']} SIC codes added, "
        f"{stats['not_found']} not found, "
        f"{stats['errors']} errors"
    )

    return stats


async def run(args):
    from app.core.database import get_session_factory

    SessionFactory = get_session_factory()
    db = SessionFactory()

    try:
        if args.sec_only:
            # SEC-only mode: skip Phase 1, query companies with ticker but no SIC
            info("Querying companies with ticker but no SIC code...")
            ticker_map = get_companies_needing_sic(
                db, firm_name=args.firm, limit=args.limit
            )
            if not ticker_map:
                warn("No companies need SIC codes!")
                return

            info(f"Found {len(ticker_map)} companies to enrich with SIC codes")
            if args.dry_run:
                warn("  DRY RUN — no database changes will be made")

            await enrich_phase2_sec(db, ticker_map, dry_run=args.dry_run)

            if not args.dry_run:
                db.commit()
                ok("All changes committed to database")
            else:
                db.rollback()
                info("Dry run complete — no changes committed")

            banner("Enrichment Summary")
            info(f"SEC-only mode: {len(ticker_map)} companies processed")
            return

        # Query sparse companies
        info("Querying portfolio companies missing industry/sector...")
        companies = get_sparse_companies(
            db,
            firm_name=args.firm,
            limit=args.limit,
        )

        if not companies:
            warn("No sparse portfolio companies found!")
            if args.firm:
                warn(f"  (filtered by firm: '{args.firm}')")
            return

        info(f"Found {len(companies)} companies to enrich")
        if args.firm:
            info(f"  Filtered by firm: '{args.firm}'")
        if args.dry_run:
            warn("  DRY RUN — no database changes will be made")

        # Phase 1: yfinance
        phase1 = await enrich_phase1_yfinance(db, companies, dry_run=args.dry_run)

        # Phase 2: SEC EDGAR
        if not args.skip_sec:
            await enrich_phase2_sec(
                db, phase1["ticker_map"], dry_run=args.dry_run
            )
        else:
            info("Skipping SEC EDGAR phase (--skip-sec)")

        # Commit all changes
        if not args.dry_run:
            db.commit()
            ok("All changes committed to database")
        else:
            db.rollback()
            info("Dry run complete — no changes committed")

        # Final summary
        banner("Enrichment Summary")
        p1 = phase1["stats"]
        info(f"Total companies:    {p1['total']}")
        info(f"Enriched (yfinance): {p1['enriched']}")
        info(f"No ticker found:    {p1['no_ticker']}")
        info(f"Tickers for SEC:    {len(phase1['ticker_map'])}")

    except Exception as e:
        fail(f"Fatal error: {e}")
        db.rollback()
        raise
    finally:
        db.close()


def main():
    parser = argparse.ArgumentParser(
        description="Enrich PE portfolio companies with industry, sector, location metadata"
    )
    parser.add_argument(
        "--firm", type=str, default=None,
        help="Filter by PE firm name (e.g. 'Blackstone')"
    )
    parser.add_argument(
        "--limit", type=int, default=None,
        help="Max number of companies to process"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Preview changes without writing to database"
    )
    parser.add_argument(
        "--skip-sec", action="store_true",
        help="Skip Phase 2 (SEC EDGAR SIC codes)"
    )
    parser.add_argument(
        "--sec-only", action="store_true",
        help="Only run Phase 2 (SEC SIC codes) for already-enriched companies"
    )
    args = parser.parse_args()

    print(f"\n{C.BD}{C.CY}{'=' * 64}{C.E}")
    print(f"{C.BD}{C.CY}  PE Portfolio Company Enrichment{C.E}")
    print(f"{C.BD}{C.CY}{'=' * 64}{C.E}")

    start = time.time()
    asyncio.run(run(args))
    elapsed = time.time() - start
    info(f"Completed in {elapsed:.1f}s")


if __name__ == "__main__":
    main()
