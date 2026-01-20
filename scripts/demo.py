#!/usr/bin/env python3
"""
Nexdata Interactive Demo Script

This script demonstrates the actual working features of Nexdata.
Run with: python scripts/demo.py
"""

import os
import sys
import json
import time
import requests
from datetime import datetime

# Fix Windows encoding
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

# Configuration
BASE_URL = os.getenv("NEXDATA_URL", "http://localhost:8001")
COLORS = {
    "header": "\033[95m",
    "blue": "\033[94m",
    "cyan": "\033[96m",
    "green": "\033[92m",
    "yellow": "\033[93m",
    "red": "\033[91m",
    "bold": "\033[1m",
    "end": "\033[0m"
}

def c(text, color):
    """Colorize text."""
    return f"{COLORS.get(color, '')}{text}{COLORS['end']}"

def header(text):
    """Print a header."""
    print("\n" + "=" * 70)
    print(c(f"  {text}", "header"))
    print("=" * 70)

def subheader(text):
    print(f"\n{c('▸ ' + text, 'cyan')}")

def success(text):
    print(c(f"  ✓ {text}", "green"))

def info(text):
    print(c(f"  ℹ {text}", "blue"))

def warn(text):
    print(c(f"  ⚠ {text}", "yellow"))

def api_get(endpoint):
    """Make GET request."""
    try:
        resp = requests.get(f"{BASE_URL}{endpoint}", timeout=30)
        return resp.json() if resp.ok else None
    except Exception as e:
        return None

def api_post(endpoint, data=None):
    """Make POST request."""
    try:
        resp = requests.post(f"{BASE_URL}{endpoint}", json=data or {}, timeout=60)
        return resp.json() if resp.ok else None
    except Exception as e:
        return None

def wait_for_input(prompt="Press Enter to continue..."):
    """Wait for user input."""
    input(f"\n{c(prompt, 'yellow')}")

def format_currency(value):
    """Format large numbers as currency."""
    if value is None:
        return "N/A"
    try:
        v = int(value)
        if v >= 1_000_000_000_000:
            return f"${v/1_000_000_000_000:.1f}T"
        elif v >= 1_000_000_000:
            return f"${v/1_000_000_000:.1f}B"
        elif v >= 1_000_000:
            return f"${v/1_000_000:.1f}M"
        else:
            return f"${v:,}"
    except:
        return str(value)


# =============================================================================
# DEMO SECTIONS
# =============================================================================

def demo_health_check():
    """Check API health."""
    header("NEXDATA DEMO - Investment Intelligence Platform")
    print(f"\n  Connecting to {BASE_URL}...")

    health = api_get("/health")
    if health and health.get("status") == "healthy":
        success(f"API is healthy - Database connected")
        return True
    else:
        print(c("  ✗ API is not responding. Start with: docker-compose up -d", "red"))
        return False


def demo_platform_overview():
    """Show platform overview."""
    header("1. PLATFORM OVERVIEW")

    stats = api_get("/api/v1/agentic/stats/overview")
    if stats:
        coverage = stats.get("coverage", {})
        portfolio = stats.get("portfolio_data", {})

        print(f"""
  {c('Investor Coverage:', 'bold')}
    • LPs with data:           {coverage.get('lps_with_data', 0)}
    • Family Offices with data: {coverage.get('family_offices_with_data', 0)}
    • Total investors:          {coverage.get('total_investors_covered', 0)}

  {c('Portfolio Data:', 'bold')}
    • Total holdings tracked:   {portfolio.get('total_portfolio_companies', 0):,}
    • From SEC 13F filings:     {portfolio.get('by_source', {}).get('sec_13f', 0):,}
    • From annual reports:      {portfolio.get('by_source', {}).get('annual_report', 0):,}
    • From websites:            {portfolio.get('by_source', {}).get('website', 0):,}
""")

    analytics = api_get("/api/v1/analytics/overview")
    if analytics:
        print(f"""  {c('Market Intelligence:', 'bold')}
    • Unique companies tracked: {analytics.get('unique_companies', 0):,}
    • Total market value:       {format_currency(analytics.get('total_market_value_usd', 0))}
    • Data coverage:            {analytics.get('coverage_percentage', 0):.1f}%
""")


def demo_investor_network():
    """Show investor network analysis."""
    header("2. INVESTOR NETWORK ANALYSIS")

    subheader("Most Connected Institutional Investors")
    print("  (Ranked by co-investment network centrality)\n")

    investors = api_get("/api/v1/network/central?limit=10")
    if investors:
        print(f"  {'Rank':<6}{'Investor':<25}{'Type':<18}{'Location':<12}{'Connections':<12}")
        print("  " + "-" * 70)

        for i, inv in enumerate(investors, 1):
            inv_type = (inv.get('subtype') or inv.get('type', '')).replace('_', ' ').title()
            print(f"  {i:<6}{inv['name']:<25}{inv_type:<18}{inv.get('location', 'N/A'):<12}{inv.get('degree', 0):<12}")

        # Deep dive into one investor
        wait_for_input("\nPress Enter to see Ontario Teachers' network...")

        subheader("Ontario Teachers' Pension Plan - Network Detail")
        network = api_get("/api/v1/network/investor/19?investor_type=lp")
        if network:
            center = network.get('center', {})
            nodes = network.get('nodes', [])[:10]

            print(f"\n  {c(center.get('name', 'Unknown'), 'bold')} is connected to {len(network.get('nodes', []))} other investors")
            print(f"\n  {c('Connected Investors:', 'cyan')}")

            for node in nodes:
                if node.get('id') != center.get('id'):
                    node_type = (node.get('subtype') or node.get('type', '')).replace('_', ' ').title()
                    print(f"    • {node['name']} ({node_type}) - {node.get('location', 'N/A')}")


def demo_portfolio_analysis():
    """Show portfolio analysis."""
    header("3. PORTFOLIO ANALYSIS (SEC 13F Data)")

    # Show STRS Ohio portfolio (largest)
    subheader("STRS Ohio - Portfolio Summary")
    summary = api_get("/api/v1/agentic/portfolio/4/summary?investor_type=lp")
    if summary:
        print(f"""
  {c('Investor:', 'bold')} {summary.get('investor_name', 'Unknown')}
  {c('Total Holdings:', 'bold')} {summary.get('total_companies', 0):,} companies
  {c('Data Source:', 'bold')} SEC 13F Filings
  {c('Last Updated:', 'bold')} {summary.get('last_updated', 'N/A')[:10]}
""")

    wait_for_input("Press Enter to see top holdings...")

    subheader("STRS Ohio - Top Holdings by Market Value")
    companies = api_get("/api/v1/agentic/portfolio/4/companies?investor_type=lp&limit=15")
    if companies and companies.get('companies'):
        holdings = companies['companies']

        # Filter and sort by market value
        valued_holdings = []
        for h in holdings:
            try:
                if h.get('market_value_usd'):
                    val = int(h['market_value_usd'])
                    if val > 0 and len(h.get('company_name', '')) < 40:
                        valued_holdings.append((h['company_name'], val))
            except:
                pass

        valued_holdings.sort(key=lambda x: x[1], reverse=True)

        print(f"\n  {'Company':<35}{'Market Value':<15}")
        print("  " + "-" * 50)

        for name, value in valued_holdings[:15]:
            print(f"  {name:<35}{format_currency(value):<15}")


def demo_company_research():
    """Show company research capabilities."""
    header("4. COMPANY RESEARCH AGENT")

    companies_to_research = ["Stripe", "OpenAI", "Databricks"]

    for company in companies_to_research:
        subheader(f"Researching: {company}")

        # Get research
        result = api_post("/api/v1/agents/research/company", {"company_name": company})

        if result and result.get('result', {}).get('profile'):
            profile = result['result']['profile']

            # Health Score
            health = profile.get('health_score', {})
            if health:
                tier = health.get('tier', 'N/A')
                composite = health.get('composite', 0)
                tier_color = 'green' if tier in ['A', 'B'] else 'yellow' if tier == 'C' else 'red'
                print(f"\n  {c('Health Score:', 'bold')} {composite:.0f}/100 (Tier {c(tier, tier_color)})")
                print(f"    • Growth:     {health.get('growth', 0):.0f}")
                print(f"    • Stability:  {health.get('stability', 0):.0f}")
                print(f"    • Market:     {health.get('market', 0):.0f}")
                print(f"    • Tech:       {health.get('tech', 0):.0f}")

            # Employer Brand
            employer = profile.get('employer_brand', {})
            if employer and employer.get('overall_rating'):
                print(f"\n  {c('Employer Brand (Glassdoor):', 'bold')}")
                print(f"    • Overall Rating:    {employer.get('overall_rating', 0):.1f}/5.0")
                print(f"    • CEO Approval:      {employer.get('ceo_approval', 0)*100:.0f}%")
                print(f"    • Recommend to Friend: {employer.get('recommend_to_friend', 0)*100:.0f}%")
                print(f"    • Compensation:      {employer.get('compensation_rating', 0):.1f}/5.0")

            # Mobile Presence
            mobile = profile.get('mobile_presence', {})
            if mobile and mobile.get('apps'):
                print(f"\n  {c('Mobile Presence:', 'bold')}")
                for app in mobile.get('apps', [])[:2]:
                    print(f"    • {app.get('app_name', 'Unknown')}: {app.get('rating', 0):.1f}★ ({app.get('rating_count', 0):,} ratings)")

            # Data Gaps
            gaps = profile.get('data_gaps', [])
            if gaps:
                print(f"\n  {c('Data Gaps:', 'yellow')} {', '.join(gaps[:4])}")

        if company != companies_to_research[-1]:
            wait_for_input(f"\nPress Enter to research next company...")


def demo_company_scoring():
    """Show ML-based company scoring."""
    header("5. COMPANY HEALTH SCORING")

    subheader("ML-Based Company Scores")
    print("  (Composite score based on growth, stability, market position, tech velocity)\n")

    companies = ["Stripe", "OpenAI", "Anthropic", "Databricks", "Figma"]

    print(f"  {'Company':<15}{'Score':<10}{'Tier':<8}{'Confidence':<12}{'Top Factor':<20}")
    print("  " + "-" * 65)

    for company in companies:
        score = api_get(f"/api/v1/scores/company/{company}")
        if score:
            tier = score.get('tier', 'N/A')
            tier_color = 'green' if tier in ['A', 'B'] else 'yellow' if tier == 'C' else 'red'

            # Find top category
            categories = score.get('category_scores', {})
            top_cat = max(categories.items(), key=lambda x: x[1])[0] if categories else "N/A"

            print(f"  {company:<15}{score.get('composite_score', 0):<10.1f}{c(tier, tier_color):<17}{score.get('confidence', 0)*100:<12.0f}%{top_cat:<20}")


def demo_web_traffic():
    """Show web traffic intelligence."""
    header("6. WEB TRAFFIC INTELLIGENCE")

    subheader("Tranco Rankings (Top 1M Websites)")
    print("  (Lower rank = more traffic)\n")

    domains = [
        ("stripe.com", "Stripe"),
        ("openai.com", "OpenAI"),
        ("anthropic.com", "Anthropic"),
        ("discord.com", "Discord"),
        ("notion.so", "Notion"),
        ("figma.com", "Figma"),
        ("canva.com", "Canva"),
        ("databricks.com", "Databricks"),
    ]

    results = []
    for domain, name in domains:
        data = api_get(f"/api/v1/web-traffic/domain/{domain}")
        if data and data.get('tranco_rank'):
            results.append((name, domain, data['tranco_rank']))

    results.sort(key=lambda x: x[2])

    print(f"  {'Company':<15}{'Domain':<20}{'Tranco Rank':<15}{'Tier':<10}")
    print("  " + "-" * 60)

    for name, domain, rank in results:
        tier = "Top 500" if rank < 500 else "Top 1K" if rank < 1000 else "Top 5K" if rank < 5000 else "Top 10K" if rank < 10000 else "10K+"
        tier_color = 'green' if rank < 1000 else 'yellow' if rank < 5000 else 'red'
        print(f"  {name:<15}{domain:<20}#{rank:<14}{c(tier, tier_color):<10}")


def demo_deal_pipeline():
    """Show deal pipeline."""
    header("7. DEAL PIPELINE")

    deals = api_get("/api/v1/deals?limit=10")
    if deals and deals.get('deals'):
        subheader("Active Deals in Pipeline")

        # Group by stage
        stages = {}
        for deal in deals['deals']:
            stage = deal.get('pipeline_stage', 'unknown')
            if stage not in stages:
                stages[stage] = []
            stages[stage].append(deal)

        stage_order = ['sourced', 'reviewing', 'due_diligence', 'negotiation', 'closed_won', 'closed_lost']

        for stage in stage_order:
            if stage in stages:
                print(f"\n  {c(stage.replace('_', ' ').upper(), 'cyan')} ({len(stages[stage])})")
                for deal in stages[stage][:3]:
                    sector = deal.get('company_sector') or 'N/A'
                    source = deal.get('source') or 'N/A'
                    print(f"    • {deal['company_name']} - {sector} (via {source})")


def demo_market_brief():
    """Show market intelligence."""
    header("8. MARKET INTELLIGENCE")

    brief = api_get("/api/v1/market/brief?period_type=weekly")
    if brief:
        subheader(f"Weekly Market Brief ({brief.get('period', {}).get('start', 'N/A')} to {brief.get('period', {}).get('end', 'N/A')})")

        print(f"\n  {brief.get('summary', 'No summary available')}")

        stats = brief.get('stats', {})
        if stats:
            print(f"""
  {c('Signal Summary:', 'bold')}
    • Total Signals:    {stats.get('total_signals', 0)}
    • Accelerating:     {stats.get('accelerating', 0)}
    • Decelerating:     {stats.get('decelerating', 0)}
""")


def demo_api_examples():
    """Show API usage examples."""
    header("9. API REFERENCE - Quick Examples")

    examples = [
        ("Get investor network", "GET /api/v1/network/central?limit=10"),
        ("Get investor portfolio", "GET /api/v1/agentic/portfolio/{id}/companies?investor_type=lp"),
        ("Research a company", "POST /api/v1/agents/research/company {company_name: 'Stripe'}"),
        ("Get company score", "GET /api/v1/scores/company/{name}"),
        ("Get web traffic", "GET /api/v1/web-traffic/domain/{domain}"),
        ("Get Glassdoor data", "GET /api/v1/glassdoor/company/{name}"),
        ("List deals", "GET /api/v1/deals"),
        ("Get market brief", "GET /api/v1/market/brief?period_type=weekly"),
    ]

    print("\n  Key Endpoints:\n")
    for desc, endpoint in examples:
        print(f"  {c(desc + ':', 'cyan')}")
        print(f"    {endpoint}\n")

    print(f"\n  {c('Full API Documentation:', 'bold')} {BASE_URL}/docs")
    print(f"  {c('OpenAPI Spec:', 'bold')} {BASE_URL}/openapi.json")


# =============================================================================
# MAIN
# =============================================================================

def main():
    """Run the demo."""
    print(c("""
    +---------------------------------------------------------------+
    |                                                               |
    |                        NEXDATA                                |
    |                                                               |
    |            Investment Intelligence Platform                   |
    |                    Interactive Demo                           |
    +---------------------------------------------------------------+
    """, "header"))

    if not demo_health_check():
        sys.exit(1)

    wait_for_input("\nPress Enter to start the demo...")

    # Run demo sections
    demo_platform_overview()
    wait_for_input()

    demo_investor_network()
    wait_for_input()

    demo_portfolio_analysis()
    wait_for_input()

    demo_company_research()
    wait_for_input()

    demo_company_scoring()
    wait_for_input()

    demo_web_traffic()
    wait_for_input()

    demo_deal_pipeline()
    wait_for_input()

    demo_market_brief()
    wait_for_input()

    demo_api_examples()

    header("DEMO COMPLETE")
    print(f"""
  {c('Next Steps:', 'bold')}

  1. Explore the API docs: {BASE_URL}/docs
  2. Try the frontend:     http://localhost:3001
  3. Add more data:        python scripts/seed_demo_data.py

  {c('Key Data Available:', 'cyan')}
  • 5,236 portfolio holdings from SEC 13F filings
  • 28 LPs and 24 Family Offices tracked
  • Investor co-investment network analysis
  • Company health scoring (ML-based)
  • Web traffic rankings for 10+ companies
  • Glassdoor employer brand data

  Thank you for exploring Nexdata!
""")


if __name__ == "__main__":
    main()
