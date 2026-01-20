#!/usr/bin/env python3
"""
Nexdata FULL Demo Script

Comprehensive demo showcasing ALL data sources and AI agents.
Run with: python scripts/demo_full.py

Or inside Docker:
  docker-compose exec api python scripts/demo_full.py
"""

import os
import sys
import json
import requests
from datetime import datetime, timedelta

# Fix Windows encoding
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

# Configuration
BASE_URL = os.getenv("NEXDATA_URL", "http://localhost:8001")
DB_URL = os.getenv("DATABASE_URL", "postgresql://nexdata:nexdata_dev_password@localhost:5433/nexdata")

# Try to import sqlalchemy for direct DB queries
try:
    from sqlalchemy import create_engine, text
    HAS_DB = True
except ImportError:
    HAS_DB = False


def header(text):
    print("\n" + "=" * 80)
    print(f"  {text}")
    print("=" * 80)

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

def db_query(query, params=None):
    """Direct database query."""
    if not HAS_DB:
        return None
    try:
        engine = create_engine(DB_URL)
        with engine.connect() as conn:
            result = conn.execute(text(query), params or {})
            return [dict(row._mapping) for row in result]
    except Exception as e:
        print(f"  [DB Error: {e}]")
        return None

def format_number(n):
    if n is None:
        return "N/A"
    try:
        n = float(n)
        if n >= 1e12:
            return f"${n/1e12:.1f}T"
        elif n >= 1e9:
            return f"${n/1e9:.1f}B"
        elif n >= 1e6:
            return f"${n/1e6:.1f}M"
        elif n >= 1e3:
            return f"${n/1e3:.1f}K"
        else:
            return f"${n:,.0f}"
    except:
        return str(n)


def main():
    print("""
    +========================================================================+
    |                                                                        |
    |                           NEXDATA FULL DEMO                            |
    |                                                                        |
    |              Investment Intelligence Platform - Complete Tour          |
    |                                                                        |
    +========================================================================+
    """)

    # =========================================================================
    # SECTION 1: PLATFORM OVERVIEW
    # =========================================================================
    header("1. PLATFORM OVERVIEW - Data Assets")

    if HAS_DB:
        subheader("Database Statistics")
        stats = db_query("""
            SELECT
                (SELECT COUNT(*) FROM sec_financial_facts) as sec_facts,
                (SELECT COUNT(*) FROM portfolio_companies) as portfolio,
                (SELECT COUNT(*) FROM treasury_daily_balance) as treasury,
                (SELECT COUNT(*) FROM us_trade_exports_state) as trade,
                (SELECT COUNT(*) FROM irs_soi_zip_income) as irs,
                (SELECT COUNT(*) FROM fred_interest_rates) as fred,
                (SELECT COUNT(*) FROM sec_10k) as sec_10k,
                (SELECT COUNT(*) FROM sec_income_statement) as income_stmt,
                (SELECT COUNT(*) FROM news_items) as news
        """)
        if stats:
            s = stats[0]
            print(f"""
  +----------------------------------+----------------+
  | Data Source                      | Records        |
  +----------------------------------+----------------+
  | SEC Financial Facts (XBRL)       | {s['sec_facts']:>14,} |
  | SEC Income Statements            | {s['income_stmt']:>14,} |
  | SEC 10-K Annual Reports          | {s['sec_10k']:>14,} |
  | US Treasury Daily Balances       | {s['treasury']:>14,} |
  | US Trade Export Data             | {s['trade']:>14,} |
  | IRS Income by ZIP Code           | {s['irs']:>14,} |
  | FRED Interest Rates              | {s['fred']:>14,} |
  | Portfolio Company Holdings       | {s['portfolio']:>14,} |
  | News Articles                    | {s['news']:>14,} |
  +----------------------------------+----------------+
  | TOTAL                            | {sum(s.values()):>14,} |
  +----------------------------------+----------------+
""")

    # =========================================================================
    # SECTION 2: SEC FINANCIAL DATA
    # =========================================================================
    header("2. SEC FINANCIAL DATA - Real Company Financials")

    if HAS_DB:
        subheader("Companies with Most Financial Data")
        companies = db_query("""
            SELECT company_name, COUNT(*) as facts
            FROM sec_financial_facts
            GROUP BY company_name
            ORDER BY facts DESC
            LIMIT 15
        """)
        if companies:
            print(f"\n  {'Company':<45} {'Facts':>10}")
            print("  " + "-" * 60)
            for c in companies:
                print(f"  {c['company_name']:<45} {c['facts']:>10,}")

        subheader("Apple Inc. - Key Financial Metrics (Latest)")
        apple = db_query("""
            SELECT fact_name, fact_label, value, unit, period_end_date, fiscal_year
            FROM sec_financial_facts
            WHERE cik = '0000320193'
              AND fact_name IN ('Revenues', 'RevenueFromContractWithCustomerExcludingAssessedTax',
                               'NetIncomeLoss', 'Assets', 'StockholdersEquity',
                               'CashAndCashEquivalentsAtCarryingValue')
              AND period_end_date = (SELECT MAX(period_end_date) FROM sec_financial_facts WHERE cik = '0000320193')
            ORDER BY fact_name
        """)
        if apple:
            print(f"\n  Apple Inc. Financials (as of {apple[0]['period_end_date']}):")
            print("  " + "-" * 50)
            seen = set()
            for row in apple:
                if row['fact_name'] not in seen:
                    seen.add(row['fact_name'])
                    label = row['fact_label'] or row['fact_name']
                    if len(label) > 35:
                        label = label[:32] + "..."
                    print(f"  {label:<35} {format_number(row['value']):>12}")

        subheader("Microsoft Corp. - Revenue Trend")
        msft = db_query("""
            SELECT fiscal_year, MAX(value) as revenue
            FROM sec_financial_facts
            WHERE cik = '0000789019'
              AND fact_name IN ('Revenues', 'RevenueFromContractWithCustomerExcludingAssessedTax')
              AND fiscal_year >= 2020
            GROUP BY fiscal_year
            ORDER BY fiscal_year DESC
            LIMIT 5
        """)
        if msft:
            print(f"\n  {'Fiscal Year':<15} {'Revenue':>15}")
            print("  " + "-" * 35)
            for row in msft:
                print(f"  {row['fiscal_year']:<15} {format_number(row['revenue']):>15}")

    # =========================================================================
    # SECTION 3: TREASURY & ECONOMIC DATA
    # =========================================================================
    header("3. US TREASURY & ECONOMIC DATA")

    if HAS_DB:
        subheader("Treasury Daily Operating Cash Balance")
        treasury = db_query("""
            SELECT record_date, open_today_bal, close_today_bal, transaction_today_amt
            FROM treasury_daily_balance
            WHERE account_type = 'Public Debt'
            ORDER BY record_date DESC
            LIMIT 10
        """)
        if treasury:
            print(f"\n  {'Date':<12} {'Opening':>18} {'Closing':>18} {'Daily Change':>18}")
            print("  " + "-" * 70)
            for row in treasury:
                print(f"  {str(row['record_date']):<12} {format_number(row['open_today_bal']):>18} {format_number(row['close_today_bal']):>18} {format_number(row['transaction_today_amt']):>18}")

        subheader("FRED Interest Rates - Latest")
        fred = db_query("""
            SELECT series_id, date, value
            FROM fred_interest_rates
            WHERE date = (SELECT MAX(date) FROM fred_interest_rates)
            ORDER BY series_id
        """)
        if fred:
            print(f"\n  {'Rate':<20} {'Value':>10}")
            print("  " + "-" * 35)
            for row in fred:
                rate_name = {
                    'DFF': 'Fed Funds Rate',
                    'DGS10': '10Y Treasury',
                    'DGS30': '30Y Treasury',
                    'DGS2': '2Y Treasury',
                    'DGS5': '5Y Treasury',
                    'DPRIME': 'Prime Rate'
                }.get(row['series_id'], row['series_id'])
                print(f"  {rate_name:<20} {row['value']:>10.2f}%")

    # =========================================================================
    # SECTION 4: TRADE DATA
    # =========================================================================
    header("4. US TRADE DATA")

    if HAS_DB:
        subheader("Top Exporting States (Latest Year)")
        trade = db_query("""
            SELECT state_name, SUM(value_ytd) as total_exports
            FROM us_trade_exports_state
            WHERE year = (SELECT MAX(year) FROM us_trade_exports_state)
            GROUP BY state_name
            ORDER BY total_exports DESC
            LIMIT 10
        """)
        if trade:
            print(f"\n  {'State':<25} {'Total Exports':>18}")
            print("  " + "-" * 47)
            for row in trade:
                state = row['state_name'] or 'Unknown'
                exports = row['total_exports'] or 0
                print(f"  {state:<25} {format_number(exports):>18}")

    # =========================================================================
    # SECTION 5: IRS TAX DATA
    # =========================================================================
    header("5. IRS INCOME TAX DATA")

    if HAS_DB:
        subheader("Average Income by State (from ZIP data)")
        irs = db_query("""
            SELECT state_abbr,
                   AVG(avg_agi) as avg_agi,
                   SUM(num_returns) as total_returns
            FROM irs_soi_zip_income
            WHERE tax_year = (SELECT MAX(tax_year) FROM irs_soi_zip_income)
            GROUP BY state_abbr
            ORDER BY avg_agi DESC
            LIMIT 10
        """)
        if irs:
            print(f"\n  {'State':<10} {'Avg AGI':>15} {'Returns':>15}")
            print("  " + "-" * 45)
            for row in irs:
                print(f"  {row['state_abbr']:<10} {format_number(row['avg_agi']):>15} {int(row['total_returns']):>15,}")

    # =========================================================================
    # SECTION 6: INVESTOR NETWORK
    # =========================================================================
    header("6. INVESTOR NETWORK ANALYSIS")

    subheader("Most Connected Institutional Investors")
    investors = api_get("/api/v1/network/central?limit=10")
    if investors:
        print(f"\n  {'Rank':<6}{'Investor':<28}{'Type':<18}{'Location':<12}{'Connections':<10}")
        print("  " + "-" * 75)
        for i, inv in enumerate(investors, 1):
            inv_type = (inv.get('subtype') or inv.get('type', '')).replace('_', ' ').title()
            print(f"  {i:<6}{inv['name']:<28}{inv_type:<18}{inv.get('location', 'N/A'):<12}{inv.get('degree', 0):<10}")

    subheader("Portfolio Holdings by Source")
    stats = api_get("/api/v1/agentic/stats/overview")
    if stats:
        portfolio = stats.get('portfolio_data', {})
        by_source = portfolio.get('by_source', {})
        print(f"""
  Total Holdings: {portfolio.get('total_portfolio_companies', 0):,}

  By Source:
    - SEC 13F Filings:  {by_source.get('sec_13f', 0):,}
    - Annual Reports:   {by_source.get('annual_report', 0):,}
    - Websites:         {by_source.get('website', 0):,}
""")

    # =========================================================================
    # SECTION 7: AI AGENTS
    # =========================================================================
    header("7. AI AGENTS - Automated Intelligence")

    subheader("Available Data Sources for Research")
    sources = api_get("/api/v1/agents/sources")
    if sources:
        print(f"\n  {'Source':<20}{'Description':<45}{'Weight':>8}")
        print("  " + "-" * 75)
        for src in sources.get('sources', []):
            print(f"  {src['name']:<20}{src['description']:<45}{src['weight']:>8.0%}")

    subheader("Due Diligence Agent - Risk Assessment")
    dd = api_get("/api/v1/diligence/company/Stripe")
    if dd:
        print(f"""
  Company: Stripe
  Risk Score: {dd.get('risk_score', 0):.1f}/100
  Risk Level: {dd.get('risk_level', 'unknown').upper()}

  Recommendation: {dd.get('memo', {}).get('recommendation', 'N/A')}

  Category Scores:
""")
        for cat, data in dd.get('memo', {}).get('category_scores', {}).items():
            print(f"    - {cat.title()}: {data.get('score', 0)}/100 - {data.get('summary', '')}")

    subheader("AI Report Generation")
    templates = api_get("/api/v1/ai-reports/templates")
    if templates:
        print(f"\n  Available Report Templates:")
        for t in templates:
            print(f"    - {t['name']}: {t['description']}")

    # Generate a report
    report = api_post("/api/v1/ai-reports/generate", {
        "report_type": "company",
        "entity_name": "Apple",
        "template_name": "executive_brief"
    })
    if report and report.get('report_id'):
        report_data = api_get(f"/api/v1/ai-reports/{report['report_id']}")
        if report_data:
            print(f"\n  Generated Report: {report_data.get('title', 'N/A')}")
            print(f"  Status: {report_data.get('status', 'N/A')}")
            print(f"  Confidence: {report_data.get('confidence', 0)*100:.0f}%")

    # =========================================================================
    # SECTION 8: COMPANY RESEARCH
    # =========================================================================
    header("8. COMPANY RESEARCH & SCORING")

    companies = ["Stripe", "OpenAI", "Databricks"]
    for company in companies:
        subheader(f"Research: {company}")

        # Get score
        score = api_get(f"/api/v1/scores/company/{company}")
        if score:
            print(f"""
  Health Score: {score.get('composite_score', 0):.0f}/100 (Tier {score.get('tier', 'N/A')})
  Confidence: {score.get('confidence', 0)*100:.0f}%

  Breakdown:
    - Growth:     {score.get('category_scores', {}).get('growth', 0):.0f}
    - Stability:  {score.get('category_scores', {}).get('stability', 0):.0f}
    - Market:     {score.get('category_scores', {}).get('market_position', 0):.0f}
    - Technology: {score.get('category_scores', {}).get('tech_velocity', 0):.0f}
""")

    # =========================================================================
    # SECTION 9: MARKET INTELLIGENCE
    # =========================================================================
    header("9. MARKET INTELLIGENCE")

    subheader("Weekly Market Brief")
    brief = api_get("/api/v1/market/brief?period_type=weekly")
    if brief:
        print(f"""
  Period: {brief.get('period', {}).get('start', 'N/A')} to {brief.get('period', {}).get('end', 'N/A')}

  Summary: {brief.get('summary', 'N/A')}

  Signal Stats:
    - Total Signals:  {brief.get('stats', {}).get('total_signals', 0)}
    - Accelerating:   {brief.get('stats', {}).get('accelerating', 0)}
    - Decelerating:   {brief.get('stats', {}).get('decelerating', 0)}
""")

    # =========================================================================
    # SECTION 10: WEB TRAFFIC & ALT DATA
    # =========================================================================
    header("10. ALTERNATIVE DATA")

    subheader("Web Traffic Rankings (Tranco)")
    domains = [
        ("stripe.com", "Stripe"),
        ("openai.com", "OpenAI"),
        ("discord.com", "Discord"),
        ("notion.so", "Notion"),
        ("figma.com", "Figma"),
        ("canva.com", "Canva"),
        ("anthropic.com", "Anthropic"),
    ]
    print(f"\n  {'Company':<15}{'Domain':<20}{'Rank':>10}")
    print("  " + "-" * 50)
    for domain, name in domains:
        data = api_get(f"/api/v1/web-traffic/domain/{domain}")
        if data and data.get('tranco_rank'):
            print(f"  {name:<15}{domain:<20}#{data['tranco_rank']:>9}")

    subheader("Glassdoor Employer Data")
    glassdoor = api_get("/api/v1/glassdoor/company/Stripe")
    if glassdoor:
        ratings = glassdoor.get('ratings', {})
        print(f"""
  Company: Stripe
  Overall Rating: {ratings.get('overall', 0):.1f}/5.0
  Work-Life Balance: {ratings.get('work_life_balance', 0):.1f}/5.0
  Compensation: {ratings.get('compensation_benefits', 0):.1f}/5.0
  CEO Approval: {glassdoor.get('sentiment', {}).get('ceo_approval', 0)*100:.0f}%
""")

    # =========================================================================
    # SUMMARY
    # =========================================================================
    header("DEMO COMPLETE - Summary")

    print("""
  NEXDATA provides:

  1. SEC FINANCIAL DATA
     - 2.97M financial facts for major public companies
     - Income statements, balance sheets, cash flows
     - Historical data from 10-K and 10-Q filings

  2. ECONOMIC DATA
     - US Treasury daily operating balances
     - FRED interest rates and economic indicators
     - IRS income tax statistics by geography
     - US trade data by state and commodity

  3. INVESTOR INTELLIGENCE
     - Portfolio holdings from SEC 13F filings
     - Co-investment network analysis
     - LP and Family Office tracking

  4. AI AGENTS
     - Company Researcher: Multi-source company profiles
     - Due Diligence: Automated risk assessment
     - Report Writer: Generate investment memos
     - Market Scanner: Detect market signals
     - Anomaly Detector: Find data anomalies

  5. ALTERNATIVE DATA
     - Web traffic rankings
     - Glassdoor employer metrics
     - App store presence
     - News monitoring

  API Documentation: {BASE_URL}/docs

""")


if __name__ == "__main__":
    main()
