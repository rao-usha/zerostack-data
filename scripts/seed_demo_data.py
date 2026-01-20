#!/usr/bin/env python3
"""
Seed Demo Data Script

Populates the database with demo data for showcasing Nexdata.
Run with: python scripts/seed_demo_data.py
"""

import os
import sys
import json
import requests
from datetime import datetime, timedelta
import random

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Base URL for API
BASE_URL = os.getenv("NEXDATA_URL", "http://localhost:8001")

def api_get(endpoint: str):
    """Make GET request to API."""
    try:
        resp = requests.get(f"{BASE_URL}{endpoint}", timeout=30)
        return resp.json() if resp.ok else None
    except Exception as e:
        print(f"  Error: {e}")
        return None

def api_post(endpoint: str, data: dict = None):
    """Make POST request to API."""
    try:
        resp = requests.post(f"{BASE_URL}{endpoint}", json=data or {}, timeout=60)
        return resp.json() if resp.ok else None
    except Exception as e:
        print(f"  Error: {e}")
        return None


# =============================================================================
# DEMO DATA
# =============================================================================

DEMO_COMPANIES = [
    {"name": "Stripe", "domain": "stripe.com", "sector": "fintech"},
    {"name": "OpenAI", "domain": "openai.com", "sector": "ai_ml"},
    {"name": "Anthropic", "domain": "anthropic.com", "sector": "ai_ml"},
    {"name": "Databricks", "domain": "databricks.com", "sector": "enterprise"},
    {"name": "Figma", "domain": "figma.com", "sector": "enterprise"},
    {"name": "Notion", "domain": "notion.so", "sector": "enterprise"},
    {"name": "Canva", "domain": "canva.com", "sector": "consumer"},
    {"name": "Discord", "domain": "discord.com", "sector": "consumer"},
    {"name": "Plaid", "domain": "plaid.com", "sector": "fintech"},
    {"name": "Ramp", "domain": "ramp.com", "sector": "fintech"},
]

DEMO_GITHUB_ORGS = [
    "stripe", "openai", "anthropics", "databricks", "figma",
    "notionhq", "canva", "discord", "plaid", "raboramp"
]

GLASSDOOR_SAMPLE_DATA = [
    {"company_name": "Stripe", "overall_rating": 4.2, "ceo_approval": 92, "recommend_percent": 88, "employee_count_estimate": 8000},
    {"company_name": "OpenAI", "overall_rating": 4.5, "ceo_approval": 95, "recommend_percent": 91, "employee_count_estimate": 1500},
    {"company_name": "Anthropic", "overall_rating": 4.6, "ceo_approval": 97, "recommend_percent": 93, "employee_count_estimate": 500},
    {"company_name": "Databricks", "overall_rating": 4.3, "ceo_approval": 90, "recommend_percent": 87, "employee_count_estimate": 5500},
    {"company_name": "Figma", "overall_rating": 4.4, "ceo_approval": 93, "recommend_percent": 90, "employee_count_estimate": 1200},
    {"company_name": "Notion", "overall_rating": 4.1, "ceo_approval": 88, "recommend_percent": 85, "employee_count_estimate": 800},
    {"company_name": "Canva", "overall_rating": 4.3, "ceo_approval": 91, "recommend_percent": 89, "employee_count_estimate": 3500},
    {"company_name": "Discord", "overall_rating": 3.9, "ceo_approval": 82, "recommend_percent": 78, "employee_count_estimate": 1000},
    {"company_name": "Plaid", "overall_rating": 4.0, "ceo_approval": 85, "recommend_percent": 82, "employee_count_estimate": 1200},
    {"company_name": "Ramp", "overall_rating": 4.4, "ceo_approval": 94, "recommend_percent": 91, "employee_count_estimate": 700},
]

SAMPLE_DEALS = [
    {"company_name": "TechCo AI", "stage": "reviewing", "sector": "ai_ml", "source": "inbound", "priority": 2},
    {"company_name": "HealthTech Solutions", "stage": "sourced", "sector": "healthcare", "source": "referral", "priority": 3},
    {"company_name": "FinanceFlow", "stage": "due_diligence", "sector": "fintech", "source": "conference", "priority": 1},
    {"company_name": "CloudSecure", "stage": "negotiation", "sector": "enterprise", "source": "outbound", "priority": 1},
    {"company_name": "GreenEnergy Co", "stage": "sourced", "sector": "climate", "source": "inbound", "priority": 2},
]


# =============================================================================
# SEED FUNCTIONS
# =============================================================================

def seed_glassdoor_data():
    """Seed Glassdoor company data."""
    print("\n📊 Seeding Glassdoor data...")

    for company in GLASSDOOR_SAMPLE_DATA:
        # Use the bulk import endpoint or direct insert
        result = api_post("/api/v1/glassdoor/company", company)
        if result:
            print(f"  ✅ {company['company_name']}: {company['overall_rating']} rating")
        else:
            # Try creating via different method
            print(f"  ⚠️  {company['company_name']}: may already exist or endpoint unavailable")


def seed_web_traffic():
    """Fetch web traffic data for demo domains."""
    print("\n🌐 Fetching web traffic data...")

    for company in DEMO_COMPANIES:
        domain = company["domain"]
        result = api_get(f"/api/v1/web-traffic/domain/{domain}")
        if result and result.get("tranco_rank"):
            print(f"  ✅ {domain}: Tranco rank #{result['tranco_rank']}")
        else:
            print(f"  ⚠️  {domain}: No Tranco data")


def seed_github_data():
    """Fetch GitHub data for demo orgs (requires GITHUB_TOKEN)."""
    print("\n🐙 Fetching GitHub data...")

    github_token = os.getenv("GITHUB_TOKEN")
    if not github_token:
        print("  ⚠️  GITHUB_TOKEN not set - skipping GitHub data")
        return

    for org in DEMO_GITHUB_ORGS[:5]:  # Limit to avoid rate limits
        result = api_post(f"/api/v1/github/org/{org}/fetch")
        if result:
            print(f"  ✅ {org}: fetched")
        else:
            print(f"  ⚠️  {org}: fetch failed")


def seed_app_store_data():
    """Search for apps from demo companies."""
    print("\n📱 Fetching App Store data...")

    search_terms = ["stripe", "notion", "discord", "canva", "figma"]

    for term in search_terms:
        result = api_get(f"/api/v1/apps/search?q={term}&limit=3")
        if result and result.get("results"):
            count = len(result["results"])
            print(f"  ✅ '{term}': found {count} apps")
        else:
            print(f"  ⚠️  '{term}': no results")


def seed_deals():
    """Create sample deals in the pipeline."""
    print("\n💼 Creating sample deals...")

    for deal in SAMPLE_DEALS:
        result = api_post("/api/v1/deals", deal)
        if result:
            print(f"  ✅ {deal['company_name']}: {deal['stage']}")
        else:
            print(f"  ⚠️  {deal['company_name']}: may already exist")


def seed_company_scores():
    """Generate company scores for demo companies."""
    print("\n📈 Generating company scores...")

    for company in DEMO_COMPANIES[:5]:
        result = api_get(f"/api/v1/scores/company/{company['name']}")
        if result and result.get("composite_score"):
            print(f"  ✅ {company['name']}: {result['composite_score']:.1f} ({result.get('tier', 'N/A')})")
        else:
            print(f"  ⚠️  {company['name']}: scoring unavailable")


def seed_company_research():
    """Run company research for a few demo companies."""
    print("\n🔬 Running company research (this may take a moment)...")

    for company in DEMO_COMPANIES[:3]:  # Just top 3 to save time
        result = api_post("/api/v1/agents/research/company", {"company_name": company["name"]})
        if result and result.get("job_id"):
            print(f"  ✅ {company['name']}: research started (job: {result['job_id'][:20]}...)")
        else:
            print(f"  ⚠️  {company['name']}: research failed to start")


def run_market_scan():
    """Trigger a market scan."""
    print("\n📊 Running market scan...")

    result = api_post("/api/v1/market/scan/trigger")
    if result and result.get("scan_id"):
        signals = result.get("total_signals", 0)
        print(f"  ✅ Scan complete: {signals} signals detected")
    else:
        print("  ⚠️  Market scan failed")


def seed_form_adv_sample():
    """Ingest sample Form ADV data."""
    print("\n📋 Seeding Form ADV sample data...")

    result = api_post("/api/v1/form-adv/ingest")
    if result:
        count = result.get("inserted", 0)
        print(f"  ✅ Inserted {count} sample advisers")
    else:
        print("  ⚠️  Form ADV ingestion unavailable")


def check_health():
    """Check if API is running."""
    print("🏥 Checking API health...")
    result = api_get("/health")
    if result and result.get("status") == "healthy":
        print("  ✅ API is healthy")
        return True
    else:
        print("  ❌ API is not responding. Make sure it's running on port 8001")
        return False


def show_summary():
    """Show summary of seeded data."""
    print("\n" + "="*60)
    print("📊 SEED SUMMARY")
    print("="*60)

    # Get stats from various endpoints
    stats = {}

    glassdoor = api_get("/api/v1/glassdoor/rankings?limit=1")
    stats["glassdoor_companies"] = glassdoor.get("total", 0) if glassdoor else 0

    deals = api_get("/api/v1/deals/pipeline")
    stats["deals"] = deals.get("total_deals", 0) if deals else 0

    market = api_get("/api/v1/market/stats")
    stats["market_scans"] = market.get("total_scans", 0) if market else 0
    stats["market_signals"] = market.get("total_signals", 0) if market else 0

    search = api_get("/api/v1/search/stats")
    stats["indexed_records"] = search.get("total_indexed", 0) if search else 0

    print(f"""
  Glassdoor Companies: {stats['glassdoor_companies']}
  Deals in Pipeline:   {stats['deals']}
  Market Scans:        {stats['market_scans']}
  Market Signals:      {stats['market_signals']}
  Search Index:        {stats['indexed_records']} records
    """)

    print("\n✅ Demo data seeding complete!")
    print("\nNext steps:")
    print("  1. Open http://localhost:8001/docs to explore the API")
    print("  2. Try: curl http://localhost:8001/api/v1/scores/company/Stripe")
    print("  3. Try: curl http://localhost:8001/api/v1/market/brief")


# =============================================================================
# MAIN
# =============================================================================

def main():
    print("""
╔═══════════════════════════════════════════════════════════════╗
║                    NEXDATA DEMO SEEDER                        ║
║        Populating database with sample data...                ║
╚═══════════════════════════════════════════════════════════════╝
    """)

    if not check_health():
        print("\nPlease start the API first:")
        print("  docker-compose up -d")
        sys.exit(1)

    # Run seeders
    seed_glassdoor_data()
    seed_web_traffic()
    seed_github_data()
    seed_app_store_data()
    seed_deals()
    seed_form_adv_sample()
    seed_company_scores()
    seed_company_research()
    run_market_scan()

    # Show summary
    show_summary()


if __name__ == "__main__":
    main()
