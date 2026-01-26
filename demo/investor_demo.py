#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Nexdata Investor Demo Script
============================

A comprehensive demonstration of Nexdata's alternative data platform.
Run this script to showcase the platform's capabilities to investors.

Usage:
    python demo/investor_demo.py [--quick] [--section SECTION]

Options:
    --quick         Run quick demo (skip data collection)
    --section       Run specific section only (overview, web, github, markets, research, lp, competitive)
"""

import argparse
import io
import json
import sys
import time

# Fix Windows console encoding
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')
from datetime import datetime
from typing import Any, Dict, List, Optional

import requests

# Configuration
API_BASE = "http://localhost:8001/api/v1"
COLORS = {
    "header": "\033[95m",
    "blue": "\033[94m",
    "cyan": "\033[96m",
    "green": "\033[92m",
    "yellow": "\033[93m",
    "red": "\033[91m",
    "bold": "\033[1m",
    "underline": "\033[4m",
    "end": "\033[0m",
}


def color(text: str, c: str) -> str:
    """Apply color to text."""
    return f"{COLORS.get(c, '')}{text}{COLORS['end']}"


def header(title: str) -> None:
    """Print a section header."""
    print("\n" + "=" * 70)
    print(color(f"  {title}", "header"))
    print("=" * 70 + "\n")


def subheader(title: str) -> None:
    """Print a subsection header."""
    print(f"\n{color('>>>', 'cyan')} {color(title, 'bold')}\n")


def success(msg: str) -> None:
    """Print success message."""
    print(f"  {color('✓', 'green')} {msg}")


def info(msg: str) -> None:
    """Print info message."""
    print(f"  {color('•', 'blue')} {msg}")


def highlight(msg: str) -> None:
    """Print highlighted message."""
    print(f"  {color('★', 'yellow')} {color(msg, 'bold')}")


def api_call(endpoint: str, method: str = "GET", data: Dict = None) -> Optional[Dict]:
    """Make API call and return JSON response."""
    url = f"{API_BASE}{endpoint}"
    try:
        if method == "GET":
            resp = requests.get(url, timeout=30)
        else:
            resp = requests.post(url, json=data, timeout=60)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        print(f"  {color('✗', 'red')} API Error: {e}")
        return None


def format_number(n: float) -> str:
    """Format large numbers with K/M/B suffixes."""
    if n is None:
        return "N/A"
    if n >= 1_000_000_000:
        return f"{n/1_000_000_000:.1f}B"
    if n >= 1_000_000:
        return f"{n/1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n/1_000:.1f}K"
    return str(int(n))


def format_currency(n: float) -> str:
    """Format currency with $ and suffixes."""
    if n is None:
        return "N/A"
    return f"${format_number(n)}"


def pause(msg: str = "Press Enter to continue...") -> None:
    """Pause for user input."""
    input(f"\n  {color(msg, 'cyan')}")


# =============================================================================
# DEMO SECTIONS
# =============================================================================


def demo_intro() -> None:
    """Show introduction and platform overview."""
    print("\n" + "=" * 70)
    print(color("             N E X D A T A", "cyan"))
    print(color("    Alternative Data Intelligence Platform", "bold"))
    print("=" * 70)

    print(f"""
    {color('What is Nexdata?', 'yellow')}

    Nexdata is an AI-powered alternative data platform that aggregates
    intelligence from 25+ public data sources to provide:

    • {color('Institutional Investor Intelligence', 'green')} - LP & Family Office data
    • {color('Company Research', 'green')} - Multi-source company profiles
    • {color('Market Signals', 'green')} - Prediction markets, news, trends
    • {color('Developer Activity', 'green')} - GitHub analytics & velocity
    • {color('Web Intelligence', 'green')} - Traffic rankings & comparisons

    {color('400+ API Endpoints', 'bold')} | {color('10+ Agentic Data Sources', 'bold')} | {color('Real-time Collection', 'bold')}
    """)


def demo_overview() -> None:
    """Show platform overview and data coverage."""
    header("PLATFORM OVERVIEW - Data Coverage")

    # Get analytics overview
    subheader("Core Data Statistics")
    data = api_call("/analytics/overview")

    if data:
        print(f"  ┌{'─' * 40}┐")
        print(f"  │ {color('Institutional Investors (LPs)', 'bold'):48} │")
        print(f"  │   Total LPs:              {color(str(data.get('total_lps', 0)), 'green'):>18} │")
        print(f"  │   LPs with Portfolio Data: {color(str(data.get('lps_with_portfolio_data', 0)), 'green'):>17} │")
        print(f"  │                                          │")
        print(f"  │ {color('Family Offices', 'bold'):48} │")
        print(f"  │   Total Family Offices:   {color(str(data.get('total_family_offices', 0)), 'green'):>18} │")
        print(f"  │   FOs with Data:          {color(str(data.get('fos_with_portfolio_data', 0)), 'green'):>18} │")
        print(f"  │                                          │")
        print(f"  │ {color('Portfolio Intelligence', 'bold'):48} │")
        print(f"  │   Portfolio Companies:    {color(str(data.get('total_portfolio_companies', 0)), 'green'):>18} │")
        print(f"  │   Unique Companies:       {color(str(data.get('unique_companies', 0)), 'green'):>18} │")
        print(f"  └{'─' * 40}┘")

        # Data sources breakdown
        sources = data.get("companies_by_source", {})
        if sources:
            subheader("Data Sources Breakdown")
            for source, count in sources.items():
                bar_len = min(int(count / 100), 40)
                bar = "█" * bar_len
                print(f"  {source:15} {color(bar, 'green')} {count:,}")

    # Get LP coverage
    subheader("LP Coverage by Type")
    coverage = api_call("/lp-collection/coverage")

    if coverage and "coverage_by_type" in coverage:
        for lp_type, stats in coverage["coverage_by_type"].items():
            total = stats.get("total", 0)
            collected = stats.get("collected", 0)
            pct = stats.get("coverage_pct", 0)
            bar_len = int(pct / 2.5)
            bar = "█" * bar_len + "░" * (40 - bar_len)
            print(f"  {lp_type:18} [{color(bar, 'blue')}] {pct:5.1f}% ({collected}/{total})")

    # Get FO coverage
    subheader("Family Office Coverage by Region")
    fo_coverage = api_call("/fo-collection/coverage")

    if fo_coverage and "by_region" in fo_coverage:
        regions = fo_coverage["by_region"]
        for region, count in sorted(regions.items(), key=lambda x: -x[1])[:6]:
            bar_len = min(int(count / 3), 30)
            bar = "█" * bar_len
            print(f"  {region:15} {color(bar, 'cyan')} {count}")


def demo_web_traffic() -> None:
    """Demonstrate web traffic intelligence."""
    header("WEB TRAFFIC INTELLIGENCE")

    subheader("Top Global Domains (Tranco Rankings)")
    info("Real-time rankings from 1M+ domain database, updated daily")
    print()

    rankings = api_call("/web-traffic/rankings?limit=10")
    if rankings and "rankings" in rankings:
        print(f"  {'Rank':<6} {'Domain':<30} ")
        print(f"  {'-'*6} {'-'*30}")
        for item in rankings["rankings"]:
            rank = item["rank"]
            domain = item["domain"]
            rank_color = "green" if rank <= 3 else "blue" if rank <= 10 else "end"
            print(f"  {color(f'#{rank:<5}', rank_color)} {domain:<30}")

    subheader("Fintech Companies Comparison")
    info("Comparing payment platforms by global web traffic")
    print()

    comparison = api_call("/web-traffic/compare?domains=stripe.com&domains=paypal.com&domains=square.com&domains=shopify.com")
    if comparison and "comparison" in comparison:
        print(f"  {'Company':<15} {'Tranco Rank':<15} {'Market Position'}")
        print(f"  {'-'*15} {'-'*15} {'-'*20}")
        for i, item in enumerate(comparison["comparison"], 1):
            domain = item["domain"].replace(".com", "").title()
            rank = item.get("tranco_rank", "N/A")
            position = "Market Leader" if i == 1 else f"#{i} Competitor"
            rank_str = f"#{rank:,}" if isinstance(rank, int) else rank
            print(f"  {domain:<15} {rank_str:<15} {position}")

    subheader("AI Companies Traffic Analysis")
    info("Tracking AI industry leaders by web presence")
    print()

    ai_comparison = api_call("/web-traffic/compare?domains=openai.com&domains=anthropic.com&domains=google.com&domains=meta.com")
    if ai_comparison and "comparison" in ai_comparison:
        for item in ai_comparison["comparison"]:
            domain = item["domain"]
            rank = item.get("tranco_rank", 0)
            bar_len = max(1, 50 - int(rank / 100)) if rank else 1
            bar = "█" * bar_len
            print(f"  {domain:20} #{rank:<8,} {color(bar, 'green')}")


def demo_github() -> None:
    """Demonstrate GitHub intelligence."""
    header("GITHUB DEVELOPER INTELLIGENCE")

    subheader("AI Company Developer Activity")
    info("Real-time GitHub metrics showing developer velocity & community engagement")
    print()

    companies = ["openai", "anthropics", "stripe"]

    print(f"  {'Company':<12} {'Repos':<8} {'Stars':<10} {'Forks':<10} {'Followers':<12} {'Velocity'}")
    print(f"  {'-'*12} {'-'*8} {'-'*10} {'-'*10} {'-'*12} {'-'*10}")

    for company in companies:
        data = api_call(f"/github/org/{company}")
        if data:
            name = data.get("name", company)[:11]
            repos = data.get("public_repos", 0)
            metrics = data.get("metrics", {})
            stars = metrics.get("total_stars", 0)
            forks = metrics.get("total_forks", 0)
            followers = data.get("followers", 0)
            velocity = data.get("velocity_score", 0)

            velocity_bar = "█" * (velocity // 10) + "░" * (10 - velocity // 10)
            velocity_color = "green" if velocity >= 80 else "yellow" if velocity >= 60 else "red"

            print(f"  {name:<12} {repos:<8} {format_number(stars):<10} {format_number(forks):<10} {format_number(followers):<12} {color(velocity_bar, velocity_color)} {velocity}")

    # Show top repos for OpenAI
    subheader("OpenAI Top Repositories")
    openai = api_call("/github/org/openai")
    if openai and "metrics" in openai:
        top_repos = openai["metrics"].get("top_repos", [])[:5]
        languages = openai["metrics"].get("primary_languages", [])[:5]

        print(f"  Top Repos: {', '.join(top_repos)}")
        print(f"  Languages: {', '.join(languages)}")

        highlight(f"OpenAI has {format_number(openai['metrics'].get('total_stars', 0))} total GitHub stars!")


def demo_prediction_markets() -> None:
    """Demonstrate prediction markets intelligence."""
    header("PREDICTION MARKETS INTELLIGENCE")

    subheader("Live Market Dashboard")
    info("Real-time data from Polymarket & Kalshi prediction markets")
    print()

    # First, refresh data
    info("Refreshing market data...")
    api_call("/prediction-markets/monitor/polymarket", method="POST")

    dashboard = api_call("/prediction-markets/dashboard")
    if dashboard:
        total = dashboard.get("total_markets", 0)
        print(f"  Total Markets Tracked: {color(str(total), 'green')}")
        print()

        # Show high priority markets
        high_priority = dashboard.get("high_priority_markets", [])[:5]
        if high_priority:
            subheader("High-Volume Markets")

            for market in high_priority:
                question = market.get("question", "")[:60]
                prob = market.get("yes_probability", 0) * 100
                volume = market.get("volume_usd", 0)
                category = market.get("category", "other")

                # Color based on probability
                if prob > 80:
                    prob_color = "green"
                elif prob < 20:
                    prob_color = "red"
                else:
                    prob_color = "yellow"

                print(f"  {color('Q:', 'cyan')} {question}")
                print(f"     Probability: {color(f'{prob:.1f}%', prob_color)}  |  Volume: {color(format_currency(volume), 'green')}  |  Category: {category}")
                print()

    # Show specific Fed rate markets
    subheader("Federal Reserve Rate Decision Markets")
    info("Market-implied probabilities for January 2026 FOMC meeting")
    print()

    markets = api_call("/prediction-markets/markets/top?limit=20")
    if markets:
        fed_markets = [m for m in markets if "fed" in m.get("question", "").lower() or "rate" in m.get("question", "").lower()][:4]

        if fed_markets:
            print(f"  {'Outcome':<50} {'Prob':<10} {'Volume'}")
            print(f"  {'-'*50} {'-'*10} {'-'*15}")

            for market in fed_markets:
                question = market.get("question", "")[:48]
                prob = market.get("yes_probability", 0) * 100
                volume = market.get("volume_usd", 0)

                print(f"  {question:<50} {prob:>6.1f}%   {format_currency(volume)}")

            print()
            total_volume = sum(m.get("volume_usd", 0) for m in fed_markets)
            highlight(f"Total Fed Rate Market Volume: {format_currency(total_volume)}")


def demo_company_research() -> None:
    """Demonstrate multi-source company research."""
    header("AGENTIC COMPANY RESEARCH")

    subheader("Multi-Source Intelligence")
    info("Our AI agent automatically collects data from 10+ sources")
    print()

    sources = api_call("/agents/sources")
    if sources and "sources" in sources:
        print(f"  {'Source':<20} {'Description':<45} {'Weight'}")
        print(f"  {'-'*20} {'-'*45} {'-'*8}")
        for src in sources["sources"]:
            name = src.get("name", "")[:19]
            desc = src.get("description", "")[:44]
            weight = src.get("weight", 0)
            bar = "█" * int(weight * 50)
            print(f"  {name:<20} {desc:<45} {color(bar, 'blue')}")

    # Show Stripe research
    subheader("Company Profile: Stripe")
    info("Pre-cached research from multiple sources")

    profile = api_call("/agents/research/company/Stripe")
    if profile and "result" in profile:
        result = profile.get("result", {}).get("profile", {})

        # News
        news = result.get("news", {})
        if news:
            print(f"\n  {color('News Intelligence:', 'yellow')}")
            print(f"  {news.get('content_summary', 'No summary')[:200]}...")

            articles = news.get("recent_articles", [])[:3]
            for article in articles:
                title = article.get("title", "")[:60]
                event = article.get("event_type", "news")
                print(f"    • [{event}] {title}")

    # Show Glassdoor data
    subheader("Employee Intelligence: Stripe")

    glassdoor = api_call("/glassdoor/company/Stripe")
    if glassdoor:
        ratings = glassdoor.get("ratings", {})
        sentiment = glassdoor.get("sentiment", {})
        company_info = glassdoor.get("company_info", {})

        overall = ratings.get("overall", 0)
        wlb = ratings.get("work_life_balance", 0)
        comp = ratings.get("compensation_benefits", 0)
        culture = ratings.get("culture_values", 0)
        ceo_approval = sentiment.get("ceo_approval", 0) * 100
        recommend = sentiment.get("recommend_to_friend", 0) * 100
        industry = company_info.get("industry", "N/A")
        size = company_info.get("size", "N/A")
        founded = company_info.get("founded", "N/A")

        overall_str = color(f"{overall:.1f}", "green")
        ceo_str = color(f"{ceo_approval:.0f}%", "green")
        rec_str = color(f"{recommend:.0f}%", "green")

        print(f"  ┌{'─' * 45}┐")
        print(f"  │ {color('Glassdoor Ratings', 'bold'):53} │")
        print(f"  │   Overall Rating:     {overall_str}/5.0              │")
        print(f"  │   Work-Life Balance:  {wlb:.1f}/5.0              │")
        print(f"  │   Compensation:       {comp:.1f}/5.0              │")
        print(f"  │   Culture & Values:   {culture:.1f}/5.0              │")
        print(f"  │                                             │")
        print(f"  │ {color('Employee Sentiment', 'bold'):53} │")
        print(f"  │   CEO Approval:       {ceo_str:>20}      │")
        print(f"  │   Recommend to Friend: {rec_str:>19}      │")
        print(f"  │                                             │")
        print(f"  │ {color('Company Info', 'bold'):53} │")
        print(f"  │   Industry: {industry:>32} │")
        print(f"  │   Size: {size:>36} │")
        print(f"  │   Founded: {str(founded):>33} │")
        print(f"  └{'─' * 45}┘")


def demo_competitive() -> None:
    """Demonstrate competitive intelligence."""
    header("COMPETITIVE INTELLIGENCE")

    subheader("Company Scoring & Rankings")
    info("ML-powered health scores across multiple dimensions")
    print()

    rankings = api_call("/scores/rankings?limit=10")
    if rankings and "rankings" in rankings:
        print(f"  {'Rank':<6} {'Company':<20} {'Score':<8} {'Tier':<6} {'Confidence'}")
        print(f"  {'-'*6} {'-'*20} {'-'*8} {'-'*6} {'-'*12}")

        for item in rankings["rankings"]:
            rank = item.get("rank", 0)
            company = item.get("company_name", "")[:19]
            score = item.get("score", 0)
            tier = item.get("tier", "?")
            confidence = item.get("confidence", 0)

            tier_color = "green" if tier == "A" else "yellow" if tier == "B" else "red"
            conf_bar = "█" * int(confidence * 10) + "░" * (10 - int(confidence * 10))

            print(f"  #{rank:<5} {company:<20} {score:>5.1f}   {color(tier, tier_color):<5}  {conf_bar} {confidence:.0%}")

    subheader("Due Diligence Capabilities")

    stats = api_call("/diligence/stats")
    if stats:
        jobs = stats.get("jobs", {})
        risk = stats.get("risk_analysis", {})

        print(f"  Total Diligence Jobs: {jobs.get('total', 0)}")
        print(f"  Completed: {jobs.get('completed', 0)}")
        print(f"  Average Risk Score: {risk.get('avg_risk_score', 0):.1f}")
        print(f"  Templates Available: {stats.get('templates', 0)}")


def demo_trends() -> None:
    """Show investment trends."""
    header("INVESTMENT TRENDS")

    subheader("Portfolio Holdings Snapshot")

    snapshot = api_call("/trends/snapshot")
    if snapshot:
        total_holdings = snapshot.get("total_holdings", 0)
        total_investors = snapshot.get("total_investors", 0)
        holdings_str = color(f"{total_holdings:,}", "green")
        print(f"  Total Holdings: {holdings_str}")
        print(f"  Total Investors: {total_investors:,}")
        print()

        # By LP type
        by_type = snapshot.get("by_lp_type", [])
        if by_type:
            subheader("Holdings by Investor Type")
            for item in by_type[:5]:
                lp_type = item.get("lp_type", "unknown")
                count = item.get("count", 0)
                pct = item.get("pct", 0)
                bar_len = int(pct / 2.5)
                bar = "█" * bar_len
                print(f"  {lp_type:20} {color(bar, 'blue')} {pct:.1f}% ({count:,})")


def demo_api_showcase() -> None:
    """Show API capabilities."""
    header("API CAPABILITIES")

    print(f"""
    {color('400+ REST API Endpoints', 'yellow')}

    {color('Data Sources:', 'bold')}
    • SEC (13F, Form D, Form ADV)
    • FRED (Federal Reserve Economic Data)
    • Census Bureau, BLS, BEA
    • USPTO Patents
    • GitHub, Glassdoor, App Stores
    • Prediction Markets (Polymarket, Kalshi)
    • News & Corporate Registries

    {color('Key Capabilities:', 'bold')}
    • Agentic multi-source company research
    • Real-time data collection & monitoring
    • Automated anomaly detection
    • Investment trend analysis
    • Competitive intelligence
    • Due diligence automation

    {color('Integration:', 'bold')}
    • RESTful JSON APIs
    • GraphQL support
    • Webhook notifications
    • Scheduled data collection
    • Export to CSV/Excel/JSON

    {color('Example Endpoints:', 'cyan')}
    GET  /api/v1/web-traffic/domain/{{domain}}
    GET  /api/v1/github/org/{{org}}
    POST /api/v1/agents/research/company
    GET  /api/v1/prediction-markets/dashboard
    GET  /api/v1/analytics/overview
    """)


def demo_closing() -> None:
    """Show closing summary."""
    header("DEMO SUMMARY")

    print(f"""
    {color('What You Saw Today:', 'yellow')}

    ✓ {color('564 LPs', 'green')} + {color('308 Family Offices', 'green')} tracked
    ✓ {color('5,236 Portfolio Companies', 'green')} with holdings data
    ✓ {color('10+ Alternative Data Sources', 'green')} aggregated
    ✓ {color('Real-time Prediction Markets', 'green')} intelligence
    ✓ {color('GitHub Developer Activity', 'green')} tracking
    ✓ {color('Web Traffic Rankings', 'green')} for 1M+ domains
    ✓ {color('AI-Powered Company Research', 'green')} automation

    {color('Why Nexdata?', 'yellow')}

    • {color('Alternative Data at Scale', 'bold')} - Aggregate 25+ public data sources
    • {color('Real-Time Intelligence', 'bold')} - Continuous monitoring & alerts
    • {color('Agentic Automation', 'bold')} - AI agents that research for you
    • {color('400+ API Endpoints', 'bold')} - Build any data product

    {color('Contact:', 'cyan')}
    API Docs: http://localhost:8001/docs
    """)


# =============================================================================
# MAIN
# =============================================================================


def run_full_demo() -> None:
    """Run the complete demo."""
    demo_intro()
    pause()

    demo_overview()
    pause()

    demo_web_traffic()
    pause()

    demo_github()
    pause()

    demo_prediction_markets()
    pause()

    demo_company_research()
    pause()

    demo_competitive()
    pause()

    demo_trends()
    pause()

    demo_api_showcase()
    pause()

    demo_closing()


def run_quick_demo() -> None:
    """Run a quick demo without pauses."""
    demo_intro()
    demo_overview()
    demo_web_traffic()
    demo_github()
    demo_prediction_markets()
    demo_closing()


def main():
    parser = argparse.ArgumentParser(description="Nexdata Investor Demo")
    parser.add_argument("--quick", action="store_true", help="Run quick demo without pauses")
    parser.add_argument("--section", type=str, help="Run specific section")
    args = parser.parse_args()

    # Check API is running
    try:
        resp = requests.get(f"{API_BASE.replace('/api/v1', '')}/health", timeout=5)
        if resp.status_code != 200:
            print(f"{color('Error:', 'red')} API is not responding. Start with: docker-compose up -d")
            sys.exit(1)
    except Exception:
        print(f"{color('Error:', 'red')} Cannot connect to API at {API_BASE}")
        print("Start the API with: docker-compose up -d")
        sys.exit(1)

    sections = {
        "overview": demo_overview,
        "web": demo_web_traffic,
        "github": demo_github,
        "markets": demo_prediction_markets,
        "research": demo_company_research,
        "competitive": demo_competitive,
        "trends": demo_trends,
        "api": demo_api_showcase,
    }

    if args.section:
        if args.section in sections:
            demo_intro()
            sections[args.section]()
        else:
            print(f"Unknown section: {args.section}")
            print(f"Available: {', '.join(sections.keys())}")
            sys.exit(1)
    elif args.quick:
        run_quick_demo()
    else:
        run_full_demo()

    print(f"\n{color('Demo Complete!', 'green')}\n")


if __name__ == "__main__":
    main()
