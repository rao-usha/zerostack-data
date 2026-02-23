#!/usr/bin/env python3
"""
PE Intelligence Demo Dataset Builder

Collects real data for 10 PE firms across tiers, exports CSVs, and generates
formatted reports — creating a ready-to-show proof of concept for mid-market
PE firm associates/VPs who currently spend 20+ hrs/week on manual research.

Usage:
    python scripts/build_demo_dataset.py
    python scripts/build_demo_dataset.py --quick        # Skip LLM-dependent phases
    python scripts/build_demo_dataset.py --api-url http://localhost:8001

Requires: API running at localhost:8001 (docker-compose up -d)
"""

import argparse
import asyncio
import io
import os
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional

# Fix Windows console encoding for Unicode characters
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

import httpx

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

API_BASE_URL = "http://localhost:8001"
OUTPUT_DIR = Path(__file__).parent.parent / "demo_output"
POLL_INTERVAL = 5  # seconds between status checks
COLLECTION_TIMEOUT = 300  # 5 min max wait per collection phase


# ---------------------------------------------------------------------------
# Terminal colours
# ---------------------------------------------------------------------------

class C:
    G = "\033[92m"   # green
    Y = "\033[93m"   # yellow
    R = "\033[91m"   # red
    B = "\033[94m"   # blue
    CY = "\033[96m"  # cyan
    BD = "\033[1m"   # bold
    E = "\033[0m"    # end


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


# ---------------------------------------------------------------------------
# Demo firms
# ---------------------------------------------------------------------------

DEMO_FIRMS: List[Dict] = [
    # Mega-Cap (3)
    {
        "name": "Blackstone",
        "website": "https://www.blackstone.com",
        "cik": "1393818",
        "firm_type": "PE",
        "primary_strategy": "Buyout",
        "aum_usd_millions": 1_000_000,
        "founded_year": 1985,
        "headquarters_city": "New York",
        "headquarters_state": "NY",
        "tier": "mega",
    },
    {
        "name": "KKR",
        "website": "https://www.kkr.com",
        "cik": "1404912",
        "firm_type": "PE",
        "primary_strategy": "Buyout",
        "aum_usd_millions": 553_000,
        "founded_year": 1976,
        "headquarters_city": "New York",
        "headquarters_state": "NY",
        "tier": "mega",
    },
    {
        "name": "Apollo Global Management",
        "website": "https://www.apollo.com",
        "cik": "1411494",
        "firm_type": "PE",
        "primary_strategy": "Credit/PE",
        "aum_usd_millions": 651_000,
        "founded_year": 1990,
        "headquarters_city": "New York",
        "headquarters_state": "NY",
        "tier": "mega",
    },
    # Mid-Market (4)
    {
        "name": "Thoma Bravo",
        "website": "https://www.thomabravo.com",
        "firm_type": "PE",
        "primary_strategy": "Software Buyout",
        "aum_usd_millions": 131_000,
        "founded_year": 2008,
        "headquarters_city": "Chicago",
        "headquarters_state": "IL",
        "tier": "mid",
    },
    {
        "name": "Vista Equity Partners",
        "website": "https://www.vistaequitypartners.com",
        "firm_type": "PE",
        "primary_strategy": "Enterprise Software",
        "aum_usd_millions": 101_000,
        "founded_year": 2000,
        "headquarters_city": "Austin",
        "headquarters_state": "TX",
        "tier": "mid",
    },
    {
        "name": "Genstar Capital",
        "website": "https://www.genstarcapital.com",
        "firm_type": "PE",
        "primary_strategy": "Buyout",
        "aum_usd_millions": 40_000,
        "founded_year": 1988,
        "headquarters_city": "San Francisco",
        "headquarters_state": "CA",
        "tier": "mid",
    },
    {
        "name": "GTCR",
        "website": "https://www.gtcr.com",
        "firm_type": "PE",
        "primary_strategy": "Buyout",
        "aum_usd_millions": 35_000,
        "founded_year": 1980,
        "headquarters_city": "Chicago",
        "headquarters_state": "IL",
        "tier": "mid",
    },
    # Sector-Focused (3)
    {
        "name": "Veritas Capital",
        "website": "https://www.veritascapital.com",
        "firm_type": "PE",
        "primary_strategy": "Government/Defense",
        "aum_usd_millions": 45_000,
        "founded_year": 1992,
        "headquarters_city": "New York",
        "headquarters_state": "NY",
        "tier": "sector",
    },
    {
        "name": "Welsh Carson Anderson & Stowe",
        "website": "https://www.wcas.com",
        "firm_type": "PE",
        "primary_strategy": "Healthcare/Tech",
        "aum_usd_millions": 35_000,
        "founded_year": 1979,
        "headquarters_city": "New York",
        "headquarters_state": "NY",
        "tier": "sector",
    },
    {
        "name": "Francisco Partners",
        "website": "https://www.franciscopartners.com",
        "firm_type": "PE",
        "primary_strategy": "Technology",
        "aum_usd_millions": 45_000,
        "founded_year": 1999,
        "headquarters_city": "San Francisco",
        "headquarters_state": "CA",
        "tier": "sector",
    },
]

# Firms with CIKs (eligible for SEC collection)
CIK_FIRMS = [f["name"] for f in DEMO_FIRMS if f.get("cik")]

# Mega-cap firms (for report generation)
MEGA_FIRMS = [f["name"] for f in DEMO_FIRMS if f["tier"] == "mega"]

# Export tables
EXPORT_TABLES = [
    "pe_firms",
    "pe_portfolio_companies",
    "pe_people",
    "pe_deals",
    "pe_firm_people",
    "pe_company_leadership",
    "pe_funds",
]

# Report templates — investor_profile for mega-cap firms
REPORT_FORMATS = ["html", "excel"]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def api_get(client: httpx.AsyncClient, path: str, **kwargs) -> Optional[dict]:
    """GET helper — returns JSON or None on error."""
    try:
        r = await client.get(f"{API_BASE_URL}{path}", timeout=30, **kwargs)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        fail(f"GET {path}: {e}")
        return None


async def api_post(client: httpx.AsyncClient, path: str, json: dict = None, **kwargs) -> Optional[dict]:
    """POST helper — returns JSON or None on error."""
    try:
        r = await client.post(f"{API_BASE_URL}{path}", json=json, timeout=30, **kwargs)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        fail(f"POST {path}: {e}")
        return None


async def poll_firm_enrichment(
    client: httpx.AsyncClient,
    firm_ids: Dict[str, int],
    field: str = "website",
    timeout: int = COLLECTION_TIMEOUT,
):
    """Poll firm profiles until enrichment data appears or timeout."""
    start = time.time()
    pending = set(firm_ids.keys())

    while pending and (time.time() - start) < timeout:
        await asyncio.sleep(POLL_INTERVAL)
        still_pending = set()
        for name in pending:
            fid = firm_ids[name]
            data = await api_get(client, f"/api/v1/pe/firms/{fid}")
            if data is None:
                still_pending.add(name)
                continue
            # Check if we got enrichment data
            has_data = False
            if field == "website":
                # Website collector populates social links, team, and portfolio
                has_data = bool(
                    data.get("social", {}).get("linkedin")
                    or data.get("team")
                    or (data.get("data_quality", {}).get("sources") or [])
                )
            elif field == "team":
                has_data = bool(data.get("team"))
            if not has_data:
                still_pending.add(name)
        done = pending - still_pending
        for name in done:
            ok(f"{name}: enrichment complete ({field})")
        pending = still_pending
        elapsed = int(time.time() - start)
        if pending:
            info(f"  Waiting... {len(pending)} remaining ({elapsed}s elapsed)")

    if pending:
        warn(f"Timeout ({timeout}s) — still pending: {', '.join(sorted(pending))}")


async def poll_export_job(
    client: httpx.AsyncClient,
    job_id: int,
    table_name: str,
    timeout: int = 120,
) -> bool:
    """Poll export job until complete. Returns True on success."""
    start = time.time()
    while (time.time() - start) < timeout:
        data = await api_get(client, f"/api/v1/export/jobs/{job_id}")
        if data is None:
            return False
        status = data.get("status", "")
        if status == "completed":
            return True
        if status == "failed":
            fail(f"Export {table_name}: {data.get('error_message', 'unknown error')}")
            return False
        await asyncio.sleep(2)
    warn(f"Export {table_name}: timeout after {timeout}s")
    return False


async def poll_report(
    client: httpx.AsyncClient,
    report_id: int,
    label: str,
    timeout: int = 120,
) -> bool:
    """Poll report generation until complete."""
    start = time.time()
    while (time.time() - start) < timeout:
        data = await api_get(client, f"/api/v1/reports/{report_id}")
        if data is None:
            return False
        status = data.get("status", "")
        if status == "complete":
            return True
        if status == "failed":
            fail(f"Report {label}: failed")
            return False
        await asyncio.sleep(2)
    warn(f"Report {label}: timeout after {timeout}s")
    return False


# ---------------------------------------------------------------------------
# Phase functions
# ---------------------------------------------------------------------------

async def phase0_health(client: httpx.AsyncClient) -> bool:
    """Phase 0: Health check and output directory setup."""
    banner("Phase 0: Health Check")
    data = await api_get(client, "/health")
    if data is None or data.get("status") != "healthy":
        fail("API is not healthy. Start with: docker-compose up -d")
        return False
    ok(f"API healthy  (db: {data.get('database', '?')})")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    ok(f"Output directory: {OUTPUT_DIR}")
    return True


async def phase1_seed(client: httpx.AsyncClient) -> Dict[str, int]:
    """Phase 1: Seed the 10 demo firms. Returns name -> id mapping."""
    banner("Phase 1: Seed Firms")
    firm_ids: Dict[str, int] = {}

    for firm in DEMO_FIRMS:
        payload = {k: v for k, v in firm.items() if k != "tier"}
        data = await api_post(client, "/api/v1/pe/firms/", json=payload)
        if data and "id" in data:
            firm_ids[firm["name"]] = data["id"]
            ok(f"{firm['name']}  (id={data['id']})")
        else:
            fail(f"{firm['name']}: could not create")

    info(f"Seeded {len(firm_ids)}/{len(DEMO_FIRMS)} firms")
    return firm_ids


async def phase2_collect(client: httpx.AsyncClient, firm_ids: Dict[str, int]):
    """Phase 2: Trigger SEC ADV + website collection."""
    banner("Phase 2: Collect Firm Data")

    # SEC ADV for CIK firms
    cik_ids = [firm_ids[n] for n in CIK_FIRMS if n in firm_ids]
    if cik_ids:
        info(f"SEC ADV collection for {len(cik_ids)} firms with CIKs...")
        resp = await api_post(client, "/api/v1/pe/collection/collect", json={
            "entity_type": "firm",
            "sources": ["sec_adv"],
            "firm_ids": cik_ids,
            "max_concurrent": 3,
            "rate_limit_delay": 2.0,
        })
        if resp:
            ok(f"SEC ADV collection started: {resp.get('message', '')}")
        else:
            warn("SEC ADV collection failed to start")

    # Website collection for all firms
    all_ids = list(firm_ids.values())
    info(f"Website collection for all {len(all_ids)} firms...")
    resp = await api_post(client, "/api/v1/pe/collection/collect", json={
        "entity_type": "firm",
        "sources": ["firm_website"],
        "firm_ids": all_ids,
        "max_concurrent": 5,
        "rate_limit_delay": 2.0,
    })
    if resp:
        ok(f"Website collection started: {resp.get('message', '')}")
    else:
        warn("Website collection failed to start")

    # Poll until enrichment appears
    info("Polling for enrichment data...")
    await poll_firm_enrichment(client, firm_ids, field="website", timeout=COLLECTION_TIMEOUT)


async def phase3_portfolio(client: httpx.AsyncClient, firm_ids: Dict[str, int]) -> Dict[str, List[int]]:
    """Phase 3: Check portfolio companies discovered."""
    banner("Phase 3: Portfolio Companies")
    portfolio_map: Dict[str, List[int]] = {}

    for name, fid in firm_ids.items():
        data = await api_get(client, f"/api/v1/pe/firms/{fid}/portfolio")
        if data:
            count = data.get("count", 0)
            ids = [co["id"] for co in data.get("portfolio", [])]
            portfolio_map[name] = ids
            ok(f"{name}: {count} portfolio companies")
        else:
            portfolio_map[name] = []

    # Total companies (use export tables for accurate count)
    tables = await api_get(client, "/api/v1/export/tables")
    if tables:
        for t in tables:
            if t.get("table_name") == "pe_portfolio_companies":
                info(f"Total portfolio companies in DB: {t.get('row_count', '?')}")
                break

    return portfolio_map


async def phase4_people(client: httpx.AsyncClient, firm_ids: Dict[str, int], quick: bool = False):
    """Phase 4: Bio extraction (LLM-dependent)."""
    banner("Phase 4: People & Leadership")

    if quick:
        warn("Skipping bio_extractor in --quick mode")
        return

    all_ids = list(firm_ids.values())
    info(f"Bio extraction for {len(all_ids)} firms (uses OpenAI)...")
    resp = await api_post(client, "/api/v1/pe/collection/collect", json={
        "entity_type": "firm",
        "sources": ["bio_extractor"],
        "firm_ids": all_ids,
        "max_concurrent": 3,
        "rate_limit_delay": 3.0,
    })
    if resp:
        ok(f"Bio extraction started: {resp.get('message', '')}")
    else:
        warn("Bio extraction failed to start")
        return

    # Poll team pages
    info("Polling for team member data...")
    await poll_firm_enrichment(client, firm_ids, field="team", timeout=COLLECTION_TIMEOUT)

    # Quick count via export tables
    tables = await api_get(client, "/api/v1/export/tables")
    if tables:
        for t in tables:
            if t.get("table_name") == "pe_people":
                info(f"Total people in DB: {t.get('row_count', '?')}")
                break


async def phase5_deals(client: httpx.AsyncClient, firm_ids: Dict[str, int]):
    """Phase 5: SEC Form D deal discovery for CIK firms."""
    banner("Phase 5: Deals via SEC Form D")

    cik_ids = [firm_ids[n] for n in CIK_FIRMS if n in firm_ids]
    if not cik_ids:
        warn("No CIK firms available — skipping")
        return

    info(f"SEC Form D collection for {len(cik_ids)} firms...")
    resp = await api_post(client, "/api/v1/pe/collection/collect", json={
        "entity_type": "firm",
        "sources": ["sec_form_d"],
        "firm_ids": cik_ids,
        "max_concurrent": 3,
        "rate_limit_delay": 2.0,
    })
    if resp:
        ok(f"Form D collection started: {resp.get('message', '')}")
    else:
        warn("Form D collection failed to start")
        return

    # Wait a bit then check deal count
    info("Waiting for Form D processing...")
    await asyncio.sleep(30)

    # Poll for deals to appear
    start = time.time()
    while (time.time() - start) < COLLECTION_TIMEOUT:
        data = await api_get(client, "/api/v1/pe/deals/", params={"limit": 1})
        if data:
            count = data.get("count", 0)
            if count and count > 0:
                ok(f"Deals discovered: {count}")
                break
        await asyncio.sleep(POLL_INTERVAL)
    else:
        warn("No deals found within timeout (Form D data may still be processing)")


async def phase6_export(client: httpx.AsyncClient):
    """Phase 6: Export tables to CSV."""
    banner("Phase 6: Export CSVs")
    exported = 0

    for table in EXPORT_TABLES:
        info(f"Exporting {table}...")
        resp = await api_post(client, "/api/v1/export/jobs", json={
            "table_name": table,
            "format": "csv",
        })
        if not resp or "id" not in resp:
            fail(f"{table}: could not create export job")
            continue

        job_id = resp["id"]
        success = await poll_export_job(client, job_id, table)
        if not success:
            continue

        # Download
        try:
            r = await client.get(
                f"{API_BASE_URL}/api/v1/export/jobs/{job_id}/download",
                timeout=60,
            )
            r.raise_for_status()
            out_path = OUTPUT_DIR / f"{table}.csv"
            out_path.write_bytes(r.content)
            size_kb = len(r.content) / 1024
            ok(f"{table}.csv  ({size_kb:.1f} KB)")
            exported += 1
        except Exception as e:
            fail(f"{table}: download error — {e}")

    info(f"Exported {exported}/{len(EXPORT_TABLES)} tables")


async def phase7_reports(client: httpx.AsyncClient, firm_ids: Dict[str, int]):
    """Phase 7: Generate reports for mega-cap firms."""
    banner("Phase 7: Generate Reports")
    generated = 0

    for name in MEGA_FIRMS:
        fid = firm_ids.get(name)
        if not fid:
            warn(f"{name}: no firm_id — skipping report")
            continue

        for fmt in REPORT_FORMATS:
            label = f"{name} ({fmt})"
            info(f"Generating {label}...")
            resp = await api_post(client, "/api/v1/reports/generate", json={
                "template": "investor_profile",
                "format": fmt,
                "params": {"investor_id": fid, "investor_type": "pe_firm"},
                "title": f"{name} — PE Intelligence Profile",
            })
            if not resp or "id" not in resp:
                fail(f"{label}: could not create report")
                continue

            report_id = resp["id"]
            success = await poll_report(client, report_id, label)
            if not success:
                continue

            # Download
            try:
                r = await client.get(
                    f"{API_BASE_URL}/api/v1/reports/{report_id}/download",
                    timeout=60,
                )
                r.raise_for_status()
                safe_name = name.lower().replace(" ", "_").replace("&", "and")
                ext = "html" if fmt == "html" else "xlsx"
                out_path = OUTPUT_DIR / f"report_{safe_name}.{ext}"
                out_path.write_bytes(r.content)
                size_kb = len(r.content) / 1024
                ok(f"report_{safe_name}.{ext}  ({size_kb:.1f} KB)")
                generated += 1
            except Exception as e:
                fail(f"{label}: download error — {e}")

    info(f"Generated {generated}/{len(MEGA_FIRMS) * len(REPORT_FORMATS)} reports")


async def phase8_summary(client: httpx.AsyncClient, firm_ids: Dict[str, int], elapsed: float):
    """Phase 8: Print final summary."""
    banner("Phase 8: Summary")

    # Fetch stats — use export tables for accurate row counts
    firms_stats = await api_get(client, "/api/v1/pe/firms/stats/overview")
    deals_stats = await api_get(client, "/api/v1/pe/deals/stats/overview")
    tables_data = await api_get(client, "/api/v1/export/tables")

    total_firms = firms_stats.get("total_firms", "?") if firms_stats else "?"
    total_aum = firms_stats.get("aum", {}).get("total_millions", 0) if firms_stats else 0
    total_deals = deals_stats.get("total_deals", "?") if deals_stats else "?"

    # Extract row counts from export tables
    table_counts = {}
    if tables_data:
        for t in tables_data:
            table_counts[t["table_name"]] = t.get("row_count", 0)
    total_companies = table_counts.get("pe_portfolio_companies", "?")
    total_people = table_counts.get("pe_people", "?")

    # Output files
    output_files = sorted(OUTPUT_DIR.glob("*")) if OUTPUT_DIR.exists() else []

    print(f"\n{C.BD}  Database Totals{C.E}")
    print(f"  {'Firms:':<24} {total_firms}")
    print(f"  {'Portfolio Companies:':<24} {total_companies}")
    print(f"  {'People:':<24} {total_people}")
    print(f"  {'Deals:':<24} {total_deals}")
    if isinstance(total_aum, (int, float)) and total_aum > 0:
        print(f"  {'AUM Tracked:':<24} ${total_aum / 1_000_000:.1f}T")

    print(f"\n{C.BD}  Output Files ({OUTPUT_DIR}){C.E}")
    for f in output_files:
        size_kb = f.stat().st_size / 1024
        print(f"    {f.name:<40} {size_kb:>8.1f} KB")

    minutes = int(elapsed // 60)
    seconds = int(elapsed % 60)
    print(f"\n{C.BD}  Elapsed: {minutes}m {seconds}s{C.E}")
    print()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def run(quick: bool = False):
    """Execute all phases."""
    t0 = time.time()

    async with httpx.AsyncClient() as client:
        # Phase 0
        if not await phase0_health(client):
            return

        # Phase 1
        firm_ids = await phase1_seed(client)
        if not firm_ids:
            fail("No firms seeded — aborting")
            return

        # Phase 2
        await phase2_collect(client, firm_ids)

        # Phase 3
        await phase3_portfolio(client, firm_ids)

        # Phase 4
        await phase4_people(client, firm_ids, quick=quick)

        # Phase 5
        await phase5_deals(client, firm_ids)

        # Phase 6
        await phase6_export(client)

        # Phase 7
        await phase7_reports(client, firm_ids)

        # Phase 8
        elapsed = time.time() - t0
        await phase8_summary(client, firm_ids, elapsed)


def main():
    global API_BASE_URL

    parser = argparse.ArgumentParser(
        description="Build PE Intelligence demo dataset"
    )
    parser.add_argument(
        "--quick",
        action="store_true",
        help="Skip LLM-dependent phases (bio_extractor) for faster iteration",
    )
    parser.add_argument(
        "--api-url",
        type=str,
        default=API_BASE_URL,
        help=f"API base URL (default: {API_BASE_URL})",
    )
    args = parser.parse_args()
    API_BASE_URL = args.api_url

    print(f"\n{C.BD}{C.CY}{'=' * 64}{C.E}")
    print(f"{C.BD}{C.CY}  PE Intelligence Demo Dataset Builder{C.E}")
    print(f"{C.BD}{C.CY}  10 firms | SEC + Website + Bio + Form D{C.E}")
    print(f"{C.BD}{C.CY}{'=' * 64}{C.E}")

    if args.quick:
        warn("Quick mode: skipping LLM-dependent phases")

    asyncio.run(run(quick=args.quick))


if __name__ == "__main__":
    main()
