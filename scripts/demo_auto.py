#!/usr/bin/env python3
"""
Nexdata Auto Demo Script (Non-Interactive)

This script runs through all demo features automatically.
Run with: python scripts/demo_auto.py
"""

import os
import sys
import json
import requests

# Fix Windows encoding
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

BASE_URL = os.getenv("NEXDATA_URL", "http://localhost:8001")

def header(text):
    print("\n" + "=" * 70)
    print(f"  {text}")
    print("=" * 70)

def subheader(text):
    print(f"\n>> {text}")

def api_get(endpoint):
    try:
        resp = requests.get(f"{BASE_URL}{endpoint}", timeout=30)
        return resp.json() if resp.ok else None
    except:
        return None

def api_post(endpoint, data=None):
    try:
        resp = requests.post(f"{BASE_URL}{endpoint}", json=data or {}, timeout=60)
        return resp.json() if resp.ok else None
    except:
        return None

def format_currency(value):
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


def main():
    print("""
    +---------------------------------------------------------------+
    |                        NEXDATA                                |
    |            Investment Intelligence Platform                   |
    |                  Automated Demo                               |
    +---------------------------------------------------------------+
    """)

    # Health Check
    header("HEALTH CHECK")
    health = api_get("/health")
    if health and health.get("status") == "healthy":
        print(f"  [OK] API is healthy - Database connected")
    else:
        print(f"  [ERROR] API not responding. Run: docker-compose up -d")
        sys.exit(1)

    # 1. Platform Overview
    header("1. PLATFORM OVERVIEW")
    stats = api_get("/api/v1/agentic/stats/overview")
    if stats:
        coverage = stats.get("coverage", {})
        portfolio = stats.get("portfolio_data", {})
        print(f"""
  Investor Coverage:
    - LPs with data:           {coverage.get('lps_with_data', 0)}
    - Family Offices with data: {coverage.get('family_offices_with_data', 0)}
    - Total investors:          {coverage.get('total_investors_covered', 0)}

  Portfolio Data:
    - Total holdings tracked:   {portfolio.get('total_portfolio_companies', 0):,}
    - From SEC 13F filings:     {portfolio.get('by_source', {}).get('sec_13f', 0):,}
    - From annual reports:      {portfolio.get('by_source', {}).get('annual_report', 0):,}
    - From websites:            {portfolio.get('by_source', {}).get('website', 0):,}
""")

    # 2. Investor Network
    header("2. INVESTOR NETWORK - Most Connected Institutional Investors")
    investors = api_get("/api/v1/network/central?limit=10")
    if investors:
        print(f"\n  {'Rank':<6}{'Investor':<25}{'Type':<18}{'Location':<12}{'Connections':<12}")
        print("  " + "-" * 70)
        for i, inv in enumerate(investors, 1):
            inv_type = (inv.get('subtype') or inv.get('type', '')).replace('_', ' ').title()
            print(f"  {i:<6}{inv['name']:<25}{inv_type:<18}{inv.get('location', 'N/A'):<12}{inv.get('degree', 0):<12}")

    # 3. Portfolio Analysis
    header("3. PORTFOLIO ANALYSIS - STRS Ohio (Largest Portfolio)")
    summary = api_get("/api/v1/agentic/portfolio/4/summary?investor_type=lp")
    if summary:
        print(f"""
  Investor: {summary.get('investor_name', 'Unknown')}
  Total Holdings: {summary.get('total_companies', 0):,} companies
  Data Source: SEC 13F Filings
  Last Updated: {summary.get('last_updated', 'N/A')[:10]}
""")

    subheader("Top Holdings by Market Value")
    companies = api_get("/api/v1/agentic/portfolio/4/companies?investor_type=lp&limit=50")
    if companies and companies.get('companies'):
        holdings = []
        for h in companies['companies']:
            try:
                if h.get('market_value_usd'):
                    val = int(h['market_value_usd'])
                    if val > 0 and len(h.get('company_name', '')) < 40 and '+' not in h.get('company_name', ''):
                        holdings.append((h['company_name'], val))
            except:
                pass
        holdings.sort(key=lambda x: x[1], reverse=True)
        print(f"\n  {'Company':<35}{'Market Value':<15}")
        print("  " + "-" * 50)
        for name, value in holdings[:15]:
            print(f"  {name:<35}{format_currency(value):<15}")

    # 4. Company Research
    header("4. COMPANY RESEARCH - Stripe")
    result = api_post("/api/v1/agents/research/company", {"company_name": "Stripe"})
    if result and result.get('result', {}).get('profile'):
        profile = result['result']['profile']
        health = profile.get('health_score', {})
        if health:
            print(f"""
  Health Score: {health.get('composite', 0):.0f}/100 (Tier {health.get('tier', 'N/A')})
    - Growth:     {health.get('growth', 0):.0f}
    - Stability:  {health.get('stability', 0):.0f}
    - Market:     {health.get('market', 0):.0f}
    - Tech:       {health.get('tech', 0):.0f}
""")
        employer = profile.get('employer_brand', {})
        if employer and employer.get('overall_rating'):
            print(f"""  Employer Brand (Glassdoor):
    - Overall Rating:    {employer.get('overall_rating', 0):.1f}/5.0
    - CEO Approval:      {employer.get('ceo_approval', 0)*100:.0f}%
    - Compensation:      {employer.get('compensation_rating', 0):.1f}/5.0
""")

    # 5. Company Scoring
    header("5. COMPANY HEALTH SCORES")
    print(f"\n  {'Company':<15}{'Score':<10}{'Tier':<8}{'Confidence':<12}")
    print("  " + "-" * 45)
    for company in ["Stripe", "OpenAI", "Anthropic", "Databricks", "Figma"]:
        score = api_get(f"/api/v1/scores/company/{company}")
        if score:
            print(f"  {company:<15}{score.get('composite_score', 0):<10.1f}{score.get('tier', 'N/A'):<8}{score.get('confidence', 0)*100:<12.0f}%")

    # 6. Web Traffic
    header("6. WEB TRAFFIC RANKINGS")
    print(f"\n  {'Company':<15}{'Domain':<20}{'Tranco Rank':<15}")
    print("  " + "-" * 50)
    domains = [("stripe.com", "Stripe"), ("openai.com", "OpenAI"), ("discord.com", "Discord"),
               ("notion.so", "Notion"), ("figma.com", "Figma"), ("canva.com", "Canva")]
    for domain, name in domains:
        data = api_get(f"/api/v1/web-traffic/domain/{domain}")
        if data and data.get('tranco_rank'):
            print(f"  {name:<15}{domain:<20}#{data['tranco_rank']:<14}")

    # 7. Deal Pipeline
    header("7. DEAL PIPELINE")
    deals = api_get("/api/v1/deals?limit=10")
    if deals and deals.get('deals'):
        stages = {}
        for deal in deals['deals']:
            stage = deal.get('pipeline_stage', 'unknown')
            if stage not in stages:
                stages[stage] = []
            stages[stage].append(deal)

        for stage in ['sourced', 'reviewing', 'due_diligence', 'negotiation']:
            if stage in stages:
                print(f"\n  {stage.replace('_', ' ').upper()} ({len(stages[stage])})")
                for deal in stages[stage][:3]:
                    print(f"    - {deal['company_name']}")

    # Summary
    header("DEMO COMPLETE")
    print(f"""
  Key Data Available:
  - 5,236 portfolio holdings from SEC 13F filings
  - 28 LPs and 24 Family Offices tracked
  - Investor co-investment network analysis
  - Company health scoring (ML-based)
  - Web traffic rankings
  - Glassdoor employer brand data

  API Documentation: {BASE_URL}/docs
  Frontend:          http://localhost:3001

  To run interactive demo: python scripts/demo.py
""")


if __name__ == "__main__":
    main()
