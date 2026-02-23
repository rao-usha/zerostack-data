#!/usr/bin/env python3
"""
Seed well-known ATS companies into the database.

Pre-populates industrial_companies and company_ats_config with companies
whose ATS platform and board token are publicly known, enabling immediate
job posting collection without running ATS detection first.

Usage:
    python scripts/seed_ats_companies.py
    python scripts/seed_ats_companies.py --api-url http://localhost:8001
    python scripts/seed_ats_companies.py --collect   # also trigger collection after seeding
"""

import argparse
import asyncio
import io
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional

if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

import httpx

sys.path.insert(0, str(Path(__file__).parent.parent))

API_BASE_URL = "http://localhost:8001"

# ---------------------------------------------------------------------------
# Terminal colours
# ---------------------------------------------------------------------------

class C:
    G = "\033[92m"
    Y = "\033[93m"
    R = "\033[91m"
    B = "\033[94m"
    CY = "\033[96m"
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


# ---------------------------------------------------------------------------
# Well-known ATS company mappings
# ---------------------------------------------------------------------------
# board_token is the identifier used in the ATS public API URL.
# These are all publicly accessible job boards.

KNOWN_ATS_COMPANIES: List[Dict] = [
    # ===== GREENHOUSE (boards-api.greenhouse.io/v1/boards/{token}/jobs) =====
    {"name": "Stripe", "website": "https://stripe.com", "ats_type": "greenhouse", "board_token": "stripe"},
    {"name": "Spotify", "website": "https://www.spotify.com", "ats_type": "greenhouse", "board_token": "spotify"},
    {"name": "Cloudflare", "website": "https://www.cloudflare.com", "ats_type": "greenhouse", "board_token": "cloudflare"},
    {"name": "HubSpot", "website": "https://www.hubspot.com", "ats_type": "greenhouse", "board_token": "hubspot"},
    {"name": "DoorDash", "website": "https://www.doordash.com", "ats_type": "greenhouse", "board_token": "doordash"},
    {"name": "Twitch", "website": "https://www.twitch.tv", "ats_type": "greenhouse", "board_token": "twitch"},
    {"name": "Pinterest", "website": "https://www.pinterest.com", "ats_type": "greenhouse", "board_token": "pinterest"},
    {"name": "Lyft", "website": "https://www.lyft.com", "ats_type": "greenhouse", "board_token": "lyft"},
    {"name": "Datadog", "website": "https://www.datadoghq.com", "ats_type": "greenhouse", "board_token": "datadog"},
    {"name": "HashiCorp", "website": "https://www.hashicorp.com", "ats_type": "greenhouse", "board_token": "hashicorp"},
    {"name": "Snyk", "website": "https://snyk.io", "ats_type": "greenhouse", "board_token": "snyk"},
    {"name": "Cockroach Labs", "website": "https://www.cockroachlabs.com", "ats_type": "greenhouse", "board_token": "cockroachlabs"},
    {"name": "Canonical", "website": "https://canonical.com", "ats_type": "greenhouse", "board_token": "canonical"},
    {"name": "MongoDB", "website": "https://www.mongodb.com", "ats_type": "greenhouse", "board_token": "mongodb"},
    {"name": "Elastic", "website": "https://www.elastic.co", "ats_type": "greenhouse", "board_token": "elastic"},
    {"name": "GitLab", "website": "https://about.gitlab.com", "ats_type": "greenhouse", "board_token": "gitlab"},
    {"name": "Confluent", "website": "https://www.confluent.io", "ats_type": "greenhouse", "board_token": "confluent"},
    {"name": "New Relic", "website": "https://newrelic.com", "ats_type": "greenhouse", "board_token": "newrelic"},
    {"name": "Airtable", "website": "https://www.airtable.com", "ats_type": "greenhouse", "board_token": "airtable"},
    {"name": "Squarespace", "website": "https://www.squarespace.com", "ats_type": "greenhouse", "board_token": "squarespace"},
    {"name": "Gusto", "website": "https://gusto.com", "ats_type": "greenhouse", "board_token": "gusto"},
    {"name": "Plaid", "website": "https://plaid.com", "ats_type": "greenhouse", "board_token": "plaid"},
    {"name": "Affirm", "website": "https://www.affirm.com", "ats_type": "greenhouse", "board_token": "affirm"},
    {"name": "Zillow", "website": "https://www.zillow.com", "ats_type": "greenhouse", "board_token": "zillow"},
    {"name": "Reddit", "website": "https://www.reddit.com", "ats_type": "greenhouse", "board_token": "reddit"},

    # ===== LEVER (api.lever.co/v0/postings/{token}) =====
    {"name": "Netflix", "website": "https://www.netflix.com", "ats_type": "lever", "board_token": "netflix"},
    {"name": "Figma", "website": "https://www.figma.com", "ats_type": "lever", "board_token": "figma"},
    {"name": "Grammarly", "website": "https://www.grammarly.com", "ats_type": "lever", "board_token": "grammarly"},
    {"name": "Postman", "website": "https://www.postman.com", "ats_type": "lever", "board_token": "postman"},
    {"name": "Samsara", "website": "https://www.samsara.com", "ats_type": "lever", "board_token": "samsara"},
    {"name": "Scale AI", "website": "https://scale.com", "ats_type": "lever", "board_token": "scaleai"},
    {"name": "Anduril", "website": "https://www.anduril.com", "ats_type": "lever", "board_token": "anduril"},
    {"name": "Navan", "website": "https://navan.com", "ats_type": "lever", "board_token": "navan"},
    {"name": "Miro", "website": "https://miro.com", "ats_type": "lever", "board_token": "miro"},
    {"name": "Yelp", "website": "https://www.yelp.com", "ats_type": "lever", "board_token": "yelp"},
    {"name": "Webflow", "website": "https://webflow.com", "ats_type": "lever", "board_token": "webflow"},
    {"name": "Lucid Software", "website": "https://lucid.co", "ats_type": "lever", "board_token": "lucidsoftware"},
    {"name": "Drata", "website": "https://drata.com", "ats_type": "lever", "board_token": "drata"},
    {"name": "Verkada", "website": "https://www.verkada.com", "ats_type": "lever", "board_token": "verkada"},
    {"name": "Applied Intuition", "website": "https://www.appliedintuition.com", "ats_type": "lever", "board_token": "applied"},

    # ===== ASHBY (api.ashbyhq.com/posting-api/job-board/{token}) =====
    {"name": "OpenAI", "website": "https://openai.com", "ats_type": "ashby", "board_token": "openai"},
    {"name": "Anthropic", "website": "https://www.anthropic.com", "ats_type": "ashby", "board_token": "anthropic"},
    {"name": "Vercel", "website": "https://vercel.com", "ats_type": "ashby", "board_token": "vercel"},
    {"name": "Ramp", "website": "https://ramp.com", "ats_type": "ashby", "board_token": "ramp"},
    {"name": "Linear", "website": "https://linear.app", "ats_type": "ashby", "board_token": "linear"},
    {"name": "Retool", "website": "https://retool.com", "ats_type": "ashby", "board_token": "retool"},
    {"name": "Notion", "website": "https://www.notion.so", "ats_type": "ashby", "board_token": "notion"},
    {"name": "Supabase", "website": "https://supabase.com", "ats_type": "ashby", "board_token": "supabase"},
    {"name": "Wiz", "website": "https://www.wiz.io", "ats_type": "ashby", "board_token": "wiz"},
    {"name": "Rippling", "website": "https://www.rippling.com", "ats_type": "ashby", "board_token": "rippling"},
    {"name": "Vanta", "website": "https://www.vanta.com", "ats_type": "ashby", "board_token": "vanta"},
    {"name": "Mercury", "website": "https://mercury.com", "ats_type": "ashby", "board_token": "mercury"},
    {"name": "Weights & Biases", "website": "https://wandb.ai", "ats_type": "ashby", "board_token": "wandb"},
    {"name": "Cursor", "website": "https://cursor.com", "ats_type": "ashby", "board_token": "anysphere"},
    {"name": "Mistral AI", "website": "https://mistral.ai", "ats_type": "ashby", "board_token": "mistralai"},

    # ===== SMARTRECRUITERS (api.smartrecruiters.com/v1/companies/{token}/postings) =====
    {"name": "Visa", "website": "https://www.visa.com", "ats_type": "smartrecruiters", "board_token": "Visa"},
    {"name": "IKEA", "website": "https://www.ikea.com", "ats_type": "smartrecruiters", "board_token": "IKEA"},
    {"name": "Bosch", "website": "https://www.bosch.com", "ats_type": "smartrecruiters", "board_token": "BoschGroup"},
    {"name": "Sanofi", "website": "https://www.sanofi.com", "ats_type": "smartrecruiters", "board_token": "Sanofi"},
    {"name": "Equinix", "website": "https://www.equinix.com", "ats_type": "smartrecruiters", "board_token": "Equinix"},
    {"name": "Bayer", "website": "https://www.bayer.com", "ats_type": "smartrecruiters", "board_token": "Bayer"},
    {"name": "T-Mobile", "website": "https://www.t-mobile.com", "ats_type": "smartrecruiters", "board_token": "TMobile"},
    {"name": "McDonald's", "website": "https://www.mcdonalds.com", "ats_type": "smartrecruiters", "board_token": "McDonaldsCorporation"},
    {"name": "Adidas", "website": "https://www.adidas.com", "ats_type": "smartrecruiters", "board_token": "adidas"},
    {"name": "ABB", "website": "https://www.abb.com", "ats_type": "smartrecruiters", "board_token": "ABB"},
]

# Build API URLs from tokens
API_URL_TEMPLATES = {
    "greenhouse": "https://boards-api.greenhouse.io/v1/boards/{token}/jobs?content=true",
    "lever": "https://api.lever.co/v0/postings/{token}?mode=json",
    "ashby": "https://api.ashbyhq.com/posting-api/job-board/{token}",
    "smartrecruiters": "https://api.smartrecruiters.com/v1/companies/{token}/postings",
}


def get_api_url(ats_type: str, token: str) -> Optional[str]:
    template = API_URL_TEMPLATES.get(ats_type)
    if template:
        return template.format(token=token)
    return None


# ---------------------------------------------------------------------------
# Seeding functions
# ---------------------------------------------------------------------------

async def seed_via_api(client: httpx.AsyncClient, collect: bool = False) -> Dict:
    """Seed companies via the API endpoint."""
    banner("Seeding Well-Known ATS Companies")

    stats = {"seeded": 0, "skipped": 0, "errors": 0, "by_ats": {}}
    company_ids = []

    for entry in KNOWN_ATS_COMPANIES:
        ats = entry["ats_type"]
        stats["by_ats"].setdefault(ats, 0)

        try:
            # Step 1: Upsert into industrial_companies
            r = await client.post(
                f"{API_BASE_URL}/api/v1/job-postings/seed-ats-companies",
                json={"companies": [entry]},
                timeout=30,
            )
            if r.status_code == 200:
                data = r.json()
                stats["seeded"] += data.get("seeded", 0)
                stats["skipped"] += data.get("skipped", 0)
                stats["by_ats"][ats] += data.get("seeded", 0)
                for cid in data.get("company_ids", []):
                    company_ids.append(cid)
                ok(f"{entry['name']} ({ats}/{entry['board_token']})")
            else:
                fail(f"{entry['name']}: HTTP {r.status_code}")
                stats["errors"] += 1
        except Exception as e:
            fail(f"{entry['name']}: {e}")
            stats["errors"] += 1

    # Summary
    banner("Seed Summary")
    info(f"Seeded: {stats['seeded']}  |  Skipped: {stats['skipped']}  |  Errors: {stats['errors']}")
    for ats, count in sorted(stats["by_ats"].items()):
        info(f"  {ats}: {count}")

    if collect and company_ids:
        banner("Triggering Collection")
        info(f"Collecting job postings for {len(company_ids)} companies...")
        r = await client.post(
            f"{API_BASE_URL}/api/v1/job-postings/collect-all",
            json={"limit": len(company_ids)},
            timeout=30,
        )
        if r.status_code == 200:
            data = r.json()
            ok(f"Collection started: job_id={data.get('job_id')}")
        else:
            fail(f"Collection trigger failed: HTTP {r.status_code}")

    return stats


async def seed_direct(collect: bool = False) -> Dict:
    """Seed companies by calling the database directly (no API required).

    Falls back to this when the API isn't running the seed endpoint yet.
    Uses raw SQL via the DB connection.
    """
    banner("Seeding via Direct DB (Fallback)")

    # Import DB tools
    from app.core.database import get_session_factory
    from sqlalchemy import text

    SessionFactory = get_session_factory()
    db = SessionFactory()

    stats = {"seeded": 0, "skipped": 0, "errors": 0, "by_ats": {}}

    try:
        for entry in KNOWN_ATS_COMPANIES:
            ats = entry["ats_type"]
            stats["by_ats"].setdefault(ats, 0)

            try:
                # Check if company already exists (by name)
                existing = db.execute(
                    text("SELECT id FROM industrial_companies WHERE name = :name"),
                    {"name": entry["name"]},
                ).fetchone()

                if existing:
                    company_id = existing[0]
                else:
                    # Insert into industrial_companies
                    result = db.execute(
                        text("""
                            INSERT INTO industrial_companies (name, website)
                            VALUES (:name, :website)
                            ON CONFLICT (name) DO UPDATE SET website = COALESCE(EXCLUDED.website, industrial_companies.website)
                            RETURNING id
                        """),
                        {"name": entry["name"], "website": entry["website"]},
                    )
                    company_id = result.fetchone()[0]

                # Upsert company_ats_config
                api_url = get_api_url(ats, entry["board_token"])
                db.execute(
                    text("""
                        INSERT INTO company_ats_config (
                            company_id, ats_type, board_token, api_url,
                            crawl_status, updated_at
                        ) VALUES (
                            :cid, :ats_type, :token, :api_url,
                            'pending', NOW()
                        )
                        ON CONFLICT (company_id) DO UPDATE SET
                            ats_type = EXCLUDED.ats_type,
                            board_token = EXCLUDED.board_token,
                            api_url = EXCLUDED.api_url,
                            updated_at = NOW()
                    """),
                    {
                        "cid": company_id,
                        "ats_type": ats,
                        "token": entry["board_token"],
                        "api_url": api_url,
                    },
                )

                stats["seeded"] += 1
                stats["by_ats"][ats] += 1
                ok(f"{entry['name']} (id={company_id}, {ats}/{entry['board_token']})")

            except Exception as e:
                fail(f"{entry['name']}: {e}")
                stats["errors"] += 1

        db.commit()

    except Exception as e:
        fail(f"Database error: {e}")
        db.rollback()
    finally:
        db.close()

    # Summary
    banner("Seed Summary")
    info(f"Seeded: {stats['seeded']}  |  Errors: {stats['errors']}")
    for ats, count in sorted(stats["by_ats"].items()):
        info(f"  {ats}: {count}")

    return stats


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def run(collect: bool = False, direct: bool = False):
    if direct:
        await seed_direct(collect=collect)
    else:
        async with httpx.AsyncClient() as client:
            # Check if API is up
            try:
                r = await client.get(f"{API_BASE_URL}/health", timeout=5)
                if r.status_code != 200:
                    warn("API not healthy, falling back to direct DB seed")
                    await seed_direct(collect=collect)
                    return
            except Exception:
                warn("API not reachable, falling back to direct DB seed")
                await seed_direct(collect=collect)
                return

            await seed_via_api(client, collect=collect)


def main():
    global API_BASE_URL

    parser = argparse.ArgumentParser(description="Seed well-known ATS companies")
    parser.add_argument("--api-url", default=API_BASE_URL, help=f"API URL (default: {API_BASE_URL})")
    parser.add_argument("--collect", action="store_true", help="Trigger collection after seeding")
    parser.add_argument("--direct", action="store_true", help="Seed directly via DB (bypass API)")
    args = parser.parse_args()
    API_BASE_URL = args.api_url

    print(f"\n{C.BD}{C.CY}{'=' * 64}{C.E}")
    print(f"{C.BD}{C.CY}  ATS Company Seeder — {len(KNOWN_ATS_COMPANIES)} Companies{C.E}")
    print(f"{C.BD}{C.CY}{'=' * 64}{C.E}")

    ats_counts = {}
    for co in KNOWN_ATS_COMPANIES:
        ats_counts[co["ats_type"]] = ats_counts.get(co["ats_type"], 0) + 1
    for ats, count in sorted(ats_counts.items()):
        info(f"  {ats}: {count} companies")

    asyncio.run(run(collect=args.collect, direct=args.direct))


if __name__ == "__main__":
    main()
