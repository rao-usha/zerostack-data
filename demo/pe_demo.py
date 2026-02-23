#!/usr/bin/env python3
"""
Nexdata PE Demo — Acquisition & Disposition Walkthrough
========================================================

Two demo stories for PE firm prospects:

  Story 1: "Finding and Winning a Deal" (Acquisition)
  Story 2: "Preparing and Executing an Exit" (Disposition)

Prerequisites:
  - API running on localhost:8001
  - Run scripts/build_demo_dataset.py first (seeds PE firms)
  - Run scripts/seed_demo_data.py (seeds enrichment data)
  - Run scripts/seed_ats_companies.py (seeds job posting data)

Usage:
    python demo/pe_demo.py                     # Full interactive demo
    python demo/pe_demo.py --story acquisition # Acquisition story only
    python demo/pe_demo.py --story disposition # Disposition story only
    python demo/pe_demo.py --quick             # Skip pauses
    python demo/pe_demo.py --section market    # Single section
"""

import argparse
import io
import json
import sys
import time
from typing import Any, Dict, List, Optional

if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

import requests

API_BASE = "http://localhost:8001/api/v1"
QUICK_MODE = False

# ---------------------------------------------------------------------------
# Terminal formatting
# ---------------------------------------------------------------------------

class C:
    H = "\033[95m"   # header/magenta
    B = "\033[94m"   # blue
    CY = "\033[96m"  # cyan
    G = "\033[92m"   # green
    Y = "\033[93m"   # yellow
    R = "\033[91m"   # red
    BD = "\033[1m"   # bold
    UL = "\033[4m"   # underline
    DIM = "\033[2m"  # dim
    E = "\033[0m"    # end


def banner(title: str, subtitle: str = ""):
    w = 66
    print(f"\n{C.BD}{C.B}{'=' * w}{C.E}")
    print(f"{C.BD}{C.B}  {title}{C.E}")
    if subtitle:
        print(f"{C.DIM}  {subtitle}{C.E}")
    print(f"{C.BD}{C.B}{'=' * w}{C.E}\n")


def section(title: str):
    print(f"\n{C.BD}{C.CY}--- {title} ---{C.E}\n")


def metric(label: str, value: str, color: str = "G"):
    c = getattr(C, color, C.G)
    print(f"  {label:<30} {c}{C.BD}{value}{C.E}")


def row(cols: list, widths: list, colors: list = None):
    parts = []
    for i, (col, w) in enumerate(zip(cols, widths)):
        s = str(col)[:w].ljust(w)
        if colors and i < len(colors) and colors[i]:
            c = getattr(C, colors[i], C.E)
            parts.append(f"{c}{s}{C.E}")
        else:
            parts.append(s)
    print(f"  {'  '.join(parts)}")


def ok(msg: str):
    print(f"  {C.G}[OK]{C.E}  {msg}")


def info(msg: str):
    print(f"  {C.CY}[..]{C.E}  {msg}")


def warn(msg: str):
    print(f"  {C.Y}[!!]{C.E}  {msg}")


def narrate(text: str):
    """Narrator voice — the 'so what' for the PE audience."""
    print(f"\n  {C.Y}{C.BD}> {text}{C.E}\n")


def pause(msg: str = "Press Enter to continue..."):
    if not QUICK_MODE:
        input(f"  {C.DIM}{msg}{C.E}")


def fmt_num(n) -> str:
    if n is None:
        return "N/A"
    if isinstance(n, str):
        return n
    if abs(n) >= 1_000_000_000:
        return f"${n/1e9:.1f}B"
    if abs(n) >= 1_000_000:
        return f"${n/1e6:.1f}M"
    if abs(n) >= 1_000:
        return f"${n/1e3:.0f}K"
    return f"${n:,.0f}"


def fmt_pct(n) -> str:
    if n is None:
        return "N/A"
    return f"{n:+.1f}%"


def tier_color(tier: str) -> str:
    return {"A": "G", "B": "G", "C": "Y", "D": "R", "E": "R", "F": "R"}.get(tier, "E")


# ---------------------------------------------------------------------------
# API helpers
# ---------------------------------------------------------------------------

def api(method: str, endpoint: str, json_data: dict = None, params: dict = None) -> Optional[dict]:
    url = f"{API_BASE}{endpoint}"
    try:
        if method == "GET":
            r = requests.get(url, params=params, timeout=30)
        else:
            r = requests.post(url, json=json_data or {}, timeout=60)
        if r.ok:
            return r.json()
        return None
    except Exception:
        return None


def get(endpoint: str, **params) -> Optional[dict]:
    return api("GET", endpoint, params=params)


def post(endpoint: str, data: dict = None) -> Optional[dict]:
    return api("POST", endpoint, json_data=data)


# ---------------------------------------------------------------------------
# STORY 1: ACQUISITION
# ---------------------------------------------------------------------------

def acq_intro():
    banner(
        "STORY 1: Finding and Winning a Deal",
        "You're a deal team associate at a $2B mid-market PE firm."
    )
    print(f"""
  {C.BD}The problem:{C.E}
  Your MD wants to deploy capital in enterprise software. You need to:
    1. Understand the market landscape
    2. Find high-quality targets
    3. Run preliminary diligence on the top candidate
    4. Add it to your pipeline with a win probability estimate

  {C.BD}Without Nexdata:{C.E} 2-3 weeks of PitchBook searches, manual research,
  analyst calls, and spreadsheet wrangling.

  {C.BD}With Nexdata:{C.E} 15 minutes. Let's go.
""")
    pause()


def acq_market_intel():
    section("Step 1: Market Intelligence Brief")
    narrate("Every Monday, your team gets an automated market brief. Let's see what's hot.")

    brief = get("/market/brief", period_type="weekly")
    if brief:
        summary = brief.get("summary", "")
        if summary:
            # Truncate for display
            lines = summary.split("\n")[:8]
            for line in lines:
                if line.strip():
                    print(f"  {line.strip()}")

        stats = brief.get("stats", {})
        if stats:
            print()
            metric("Signals detected", str(stats.get("total_signals", 0)))
            metric("Sectors analyzed", str(stats.get("sectors_covered", 0)))
    else:
        # Fall back to triggering a scan
        info("Triggering market scan...")
        scan = post("/market/scan/trigger", {"scan_type": "manual"})
        if scan:
            metric("Signals found", str(scan.get("total_signals", 0)))

    # Show opportunities
    opps = get("/market/opportunities")
    if opps and opps.get("opportunities"):
        print()
        section("Investment Opportunities Detected")
        for opp in opps["opportunities"][:5]:
            name = opp.get("sector", opp.get("name", "Unknown"))
            thesis = opp.get("thesis", opp.get("description", ""))[:80]
            opp_type = opp.get("type", "opportunity")
            print(f"  {C.G}>{C.E} {C.BD}{name}{C.E} ({opp_type})")
            if thesis:
                print(f"    {C.DIM}{thesis}{C.E}")

    narrate("The scanner flagged enterprise software momentum. Let's dig in.")
    pause()


def acq_target_discovery():
    section("Step 2: Target Discovery")
    narrate("Let's find companies in the space and rank them by health score.")

    # Search for companies
    results = get("/search", q="software", types="company", page_size=10)
    if results and results.get("results"):
        row(["Company", "Type", "Industry", "Score"], [25, 12, 20, 8])
        row(["---", "---", "---", "---"], [25, 12, 20, 8])
        for r in results["results"][:8]:
            meta = r.get("metadata", {})
            row(
                [r.get("name", "?"), r.get("type", "?"), meta.get("industry", "?"), meta.get("score", "?")],
                [25, 12, 20, 8],
            )

    # Show rankings
    rankings = get("/scores/rankings", order="top", limit=10)
    if rankings and rankings.get("rankings"):
        print()
        section("Health Score Rankings (Top Companies)")
        row(["#", "Company", "Score", "Tier", "Confidence"], [4, 22, 8, 6, 10])
        row(["--", "---", "---", "---", "---"], [4, 22, 8, 6, 10])
        for item in rankings["rankings"][:10]:
            tc = tier_color(item.get("tier", "?"))
            row(
                [
                    f"#{item.get('rank', '?')}",
                    item.get("company_name", "?"),
                    f"{item.get('score', 0):.0f}",
                    item.get("tier", "?"),
                    f"{item.get('confidence', 0):.0%}",
                ],
                [4, 22, 8, 6, 10],
                [None, None, "G", tc, None],
            )

    # Job posting trends — hiring velocity
    jp_trends = get("/job-postings/trends/market", days=30, limit=5)
    if jp_trends:
        growing = jp_trends.get("top_growing", [])
        if growing:
            print()
            section("Fastest-Hiring Companies (Job Posting Velocity)")
            narrate("Hiring velocity is a leading indicator. Who's scaling fastest?")
            row(["Company", "Open Roles", "Growth"], [25, 12, 12])
            row(["---", "---", "---"], [25, 12, 12])
            for co in growing[:7]:
                growth_pct = co.get("growth_pct", co.get("change_pct", 0))
                row(
                    [co.get("company_name", "?"), str(co.get("current_total", co.get("total_open", "?"))), fmt_pct(growth_pct)],
                    [25, 12, 12],
                    [None, "CY", "G" if (growth_pct or 0) > 0 else "R"],
                )

    narrate("Stripe looks strong — high health score and aggressive hiring. Let's deep-dive.")
    pause()


def acq_deep_dive():
    section("Step 3: Deep Dive on Target — Stripe")
    target = "Stripe"

    # Health score
    score = get(f"/scores/company/{target}")
    if score:
        section(f"Health Score: {target}")
        composite = score.get("composite_score", 0)
        tier = score.get("tier", "?")
        tc = tier_color(tier)

        metric("Composite Score", f"{composite:.0f}/100", tc)
        metric("Tier", tier, tc)
        metric("Confidence", f"{score.get('confidence', 0):.0%}")

        cats = score.get("category_scores", score.get("breakdown", {}))
        if cats:
            print()
            for cat, val in cats.items():
                label = cat.replace("_", " ").title()
                bar_len = int((val or 0) / 2.5)
                bar = "█" * bar_len + "░" * (40 - bar_len)
                color = "G" if (val or 0) >= 60 else "Y" if (val or 0) >= 40 else "R"
                c = getattr(C, color)
                print(f"  {label:<20} {c}{bar}{C.E} {val:.0f}")

    # Competitive landscape
    competitive = get(f"/competitive/{target}")
    if competitive:
        print()
        section(f"Competitive Landscape: {target}")
        moat = competitive.get("moat_assessment", {})
        if moat:
            metric("Overall Moat", moat.get("overall_moat", "?"))
            metric("Moat Score", f"{moat.get('overall_score', 0):.0f}/100")
            scores = moat.get("scores", {})
            for dim, val in scores.items():
                label = dim.replace("_", " ").title()
                metric(f"  {label}", f"{val:.0f}/100")

        competitors = competitive.get("competitors", [])
        if competitors:
            print()
            row(["Competitor", "Similarity", "Type"], [22, 12, 16])
            row(["---", "---", "---"], [22, 12, 16])
            for comp in competitors[:6]:
                row(
                    [
                        comp.get("name", "?"),
                        f"{comp.get('similarity_score', 0):.0%}",
                        comp.get("relationship", "?"),
                    ],
                    [22, 12, 16],
                )

    # Job posting detail
    jp_stats = get("/job-postings/stats")
    if jp_stats:
        print()
        section("Job Posting Intelligence")
        metric("Total Tracked Postings", str(jp_stats.get("total_postings", 0)))
        metric("Companies Tracked", str(jp_stats.get("companies_with_postings", 0)))

        top_hiring = jp_stats.get("top_hiring_companies", [])
        if top_hiring:
            print()
            for co in top_hiring[:5]:
                name = co.get("company_name", co.get("name", "?"))
                count = co.get("total", co.get("open_postings", "?"))
                print(f"    {name:<25} {C.CY}{count} open roles{C.E}")

    # Skills demand
    skills = get("/job-postings/skills-stats")
    if skills and skills.get("top_skills"):
        print()
        section("Market Skills Demand (from Job Postings)")
        for skill in skills["top_skills"][:10]:
            name = skill.get("skill", skill.get("name", "?"))
            count = skill.get("count", 0)
            bar = "█" * min(count // 5, 30) if isinstance(count, int) else ""
            print(f"    {name:<20} {C.G}{bar}{C.E} {count}")

    narrate("Strong moat, aggressive hiring, in-demand skill set. Time for diligence.")
    pause()


def acq_due_diligence():
    section("Step 4: Automated Due Diligence")
    narrate("What takes your analysts a week, Nexdata does in under 60 seconds.")

    target = "Stripe"

    # Check for existing DD
    cached = get(f"/diligence/company/{target}")
    if cached and cached.get("status") == "completed":
        dd = cached
    else:
        info(f"Starting due diligence on {target}...")
        start = post("/diligence/start", {
            "company_name": target,
            "template": "standard",
        })
        if start and start.get("job_id"):
            job_id = start["job_id"]
            # Poll for completion
            for _ in range(20):
                time.sleep(3)
                dd = get(f"/diligence/{job_id}")
                if dd and dd.get("status") == "completed":
                    break
                info("Analyzing...")
            else:
                dd = get(f"/diligence/{job_id}")
        else:
            dd = None

    if dd:
        result = dd.get("result", dd)
        risk_score = result.get("risk_score", 0)
        risk_level = result.get("risk_level", "?")
        recommendation = result.get("recommendation", "")

        risk_color = "G" if risk_score < 40 else "Y" if risk_score < 70 else "R"
        metric("Risk Score", f"{risk_score:.0f}/100", risk_color)
        metric("Risk Level", risk_level)

        if recommendation:
            print()
            print(f"  {C.BD}Recommendation:{C.E} {recommendation[:120]}")

        # Red flags
        flags = result.get("red_flags", [])
        if flags:
            print(f"\n  {C.R}{C.BD}Red Flags:{C.E}")
            for flag in flags[:5]:
                if isinstance(flag, dict):
                    print(f"    {C.R}!{C.E} {flag.get('description', flag.get('title', str(flag)))}")
                else:
                    print(f"    {C.R}!{C.E} {flag}")

        # Strengths
        strengths = result.get("strengths", [])
        if strengths:
            print(f"\n  {C.G}{C.BD}Strengths:{C.E}")
            for s in strengths[:5]:
                if isinstance(s, dict):
                    print(f"    {C.G}+{C.E} {s.get('description', s.get('title', str(s)))}")
                else:
                    print(f"    {C.G}+{C.E} {s}")

        # Category scores
        categories = result.get("categories", result.get("sections", []))
        if categories:
            print(f"\n  {C.BD}Category Breakdown:{C.E}")
            for cat in categories[:6]:
                if isinstance(cat, dict):
                    name = cat.get("name", cat.get("category", "?"))
                    cat_score = cat.get("score", 0)
                    cc = "G" if cat_score < 40 else "Y" if cat_score < 70 else "R"
                    metric(f"  {name}", f"{cat_score:.0f}/100", cc)
    else:
        warn("DD not available — run scripts/seed_demo_data.py first")

    narrate("IC-ready preliminary memo in 60 seconds. Let's add this to the pipeline.")
    pause()


def acq_pipeline():
    section("Step 5: Deal Pipeline & Win Probability")

    # Check existing pipeline
    pipeline = get("/deals/pipeline")
    if pipeline:
        metric("Total Deals", str(pipeline.get("total_deals", 0)))
        metric("Active Deals", str(pipeline.get("active_deals", 0)))

    # Show deals
    deals = get("/deals", limit=10)
    if deals:
        deal_list = deals if isinstance(deals, list) else deals.get("deals", deals.get("results", []))
        if deal_list and isinstance(deal_list, list):
            print()
            row(["Company", "Stage", "Priority", "Sector"], [22, 16, 10, 15])
            row(["---", "---", "---", "---"], [22, 16, 10, 15])
            for d in deal_list[:8]:
                stage = d.get("pipeline_stage", d.get("stage", "?"))
                prio = d.get("priority", "?")
                row(
                    [
                        d.get("company_name", "?"),
                        stage,
                        f"P{prio}" if isinstance(prio, int) else str(prio),
                        d.get("company_sector", d.get("sector", "?")),
                    ],
                    [22, 16, 10, 15],
                    [None, "CY", "Y" if prio == 1 else None, None],
                )

    # Pipeline predictions
    predictions = get("/predictions/pipeline")
    if predictions:
        deals_pred = predictions.get("deals", [])
        summary = predictions.get("summary", {})

        if summary:
            print()
            section("AI Pipeline Scoring")
            metric("Avg Win Probability", f"{summary.get('avg_probability', 0):.0%}")
            metric("Expected Wins", f"{summary.get('expected_wins', 0):.1f}")
            metric("High Confidence Deals", str(summary.get("high_confidence_count", 0)))

        if deals_pred:
            print()
            row(["Company", "Win Prob", "Tier", "Confidence", "Next Action"], [18, 10, 6, 12, 24])
            row(["---", "---", "---", "---", "---"], [18, 10, 6, 12, 24])
            for dp in deals_pred[:8]:
                tc = tier_color(dp.get("tier", "?"))
                row(
                    [
                        dp.get("company_name", "?"),
                        f"{dp.get('win_probability', 0):.0%}",
                        dp.get("tier", "?"),
                        dp.get("confidence", "?"),
                        (dp.get("next_action", "") or "")[:24],
                    ],
                    [18, 10, 6, 12, 24],
                    [None, "G", tc, None, "CY"],
                )

    # Pipeline insights
    insights = get("/predictions/insights")
    if insights:
        health = insights.get("pipeline_health", {})
        if health:
            print()
            section("Pipeline Health")
            metric("Total Pipeline Value", fmt_num(health.get("total_pipeline_value_millions", 0) * 1e6))
            metric("Expected Value", fmt_num(health.get("expected_value_millions", 0) * 1e6))

        risk_alerts = insights.get("risk_alerts", [])
        if risk_alerts:
            print(f"\n  {C.Y}{C.BD}Risk Alerts:{C.E}")
            for alert in risk_alerts[:3]:
                if isinstance(alert, dict):
                    print(f"    {C.Y}!{C.E} {alert.get('message', alert.get('description', str(alert)))}")
                else:
                    print(f"    {C.Y}!{C.E} {alert}")

    narrate("Every deal scored, ranked, and monitored. Your MD gets this dashboard daily.")
    pause()


def acq_monitoring():
    section("Step 6: Ongoing Monitoring & Alerts")
    narrate("Once a company is in your pipeline, we watch it 24/7.")

    # Job posting alerts
    alerts = get("/job-postings/alerts", limit=5)
    if alerts and alerts.get("alerts"):
        section("Hiring Alerts (Last 30 Days)")
        for alert in alerts["alerts"][:5]:
            severity = alert.get("severity", "?")
            sc = "R" if severity == "high" else "Y" if severity == "medium" else "G"
            c = getattr(C, sc)
            print(f"  {c}[{severity.upper()}]{C.E} {alert.get('company_name', '?')}: {alert.get('alert_type', '?')}")
            detail = alert.get("details", "")
            if detail:
                print(f"    {C.DIM}{str(detail)[:80]}{C.E}")
    else:
        info("No hiring alerts yet — alerts trigger after nightly collection runs")

    # Generate a report
    section("Auto-Generated Reports")
    narrate("Need a memo for the IC meeting? One click.")

    report = post("/ai-reports/generate", {
        "report_type": "company_profile",
        "entity_name": "Stripe",
        "template": "executive_brief",
    })
    if report and report.get("report_id"):
        rid = report["report_id"]
        time.sleep(2)
        full = get(f"/ai-reports/{rid}")
        if full:
            content = full.get("content", {})
            md = content.get("markdown", "") if isinstance(content, dict) else ""
            if md:
                # Show first ~500 chars
                preview = md[:500]
                print(f"\n  {C.BD}Report Preview:{C.E}")
                for line in preview.split("\n"):
                    print(f"  {C.DIM}{line}{C.E}")
                print(f"  {C.DIM}...{C.E}")
            metric("Word Count", str(full.get("word_count", "?")))
            metric("Report ID", str(rid))
    else:
        info("Report generation in progress — check /ai-reports/list")


def acq_closing():
    banner(
        "Acquisition Story Complete",
        "From market scan to IC-ready memo in 15 minutes."
    )
    print(f"""
  {C.BD}What you saw:{C.E}
    {C.G}1.{C.E} Automated weekly market intelligence brief
    {C.G}2.{C.E} Target discovery via health scores + hiring velocity
    {C.G}3.{C.E} Deep dive: competitive moat, job trends, skills demand
    {C.G}4.{C.E} Automated due diligence with risk scoring
    {C.G}5.{C.E} Deal pipeline with AI win probability
    {C.G}6.{C.E} 24/7 monitoring + one-click reports

  {C.BD}Time saved:{C.E} {C.G}2-3 weeks of analyst work → 15 minutes{C.E}
""")


# ---------------------------------------------------------------------------
# STORY 2: DISPOSITION
# ---------------------------------------------------------------------------

def disp_intro():
    banner(
        "STORY 2: Preparing and Executing an Exit",
        "You're a portfolio ops partner preparing a company for sale."
    )
    print(f"""
  {C.BD}The situation:{C.E}
  Your fund acquired a B2B SaaS company 4 years ago. The board wants
  to explore a sale. You need to:
    1. Assess portfolio health — which companies are exit-ready?
    2. Build the valuation case with benchmarks
    3. Identify potential buyers
    4. Prepare data room materials

  {C.BD}Without Nexdata:{C.E} 4-6 months of McKinsey decks, manual benchmarking,
  and frantic LP calls.

  {C.BD}With Nexdata:{C.E} Continuous readiness monitoring + instant data room.
""")
    pause()


def disp_portfolio_health():
    section("Step 1: Portfolio Health Dashboard")
    narrate("At a glance: which portfolio companies are exit-ready?")

    # PE firms overview
    overview = get("/pe/firms/stats/overview")
    if overview:
        metric("PE Firms Tracked", str(overview.get("total_firms", 0)))
        aum = overview.get("aum", {})
        if aum.get("total_millions"):
            metric("Total AUM", fmt_num(aum["total_millions"] * 1e6))
        funds = overview.get("funds", {})
        if funds:
            metric("Funds", str(funds.get("total", 0)))

    # List PE firms
    firms = get("/pe/firms/", limit=10)
    if firms and firms.get("firms"):
        print()
        section("Your Firms")
        row(["Firm", "Type", "Strategy", "AUM"], [28, 8, 22, 14])
        row(["---", "---", "---", "---"], [28, 8, 22, 14])
        for f in firms["firms"][:10]:
            aum_val = f.get("aum_usd_millions")
            aum_str = fmt_num(aum_val * 1e6) if aum_val else "N/A"
            row(
                [f.get("name", "?"), f.get("firm_type", "?"), f.get("primary_strategy", "?")[:22], aum_str],
                [28, 8, 22, 14],
                [None, "CY", None, "G"],
            )

    # Portfolio companies — show companies with PE ownership data
    companies = get("/pe/companies/", limit=100, ownership_status="PE-Backed")
    if not companies or not companies.get("companies"):
        companies = get("/pe/companies/", limit=15)
    if companies and companies.get("companies"):
        # Filter to companies with meaningful data
        display_cos = [
            co for co in companies["companies"]
            if co.get("industry") or co.get("current_pe_owner")
        ][:12]
        if display_cos:
            print()
            section("Portfolio Companies (PE-Backed)")
            row(["Company", "Industry", "PE Owner", "Status"], [24, 18, 18, 12])
            row(["---", "---", "---", "---"], [24, 18, 18, 12])
            for co in display_cos:
                status = co.get("ownership_status", co.get("status", "?"))
                sc = "G" if status == "PE-Backed" or status == "Active" else "Y" if status == "Exited" else None
                row(
                    [
                        co.get("name", "?"),
                        (co.get("industry", "") or "?")[:18],
                        (co.get("current_pe_owner", "") or "?")[:18],
                        status,
                    ],
                    [24, 18, 18, 12],
                    [None, None, "CY", sc],
                )

    # Health scores for top companies
    rankings = get("/scores/rankings", order="top", limit=8)
    if rankings and rankings.get("rankings"):
        print()
        section("Company Health Rankings")
        narrate("A-tier companies with strong scores are your exit candidates.")
        row(["Company", "Score", "Tier"], [28, 8, 6])
        row(["---", "---", "---"], [28, 8, 6])
        for item in rankings["rankings"][:8]:
            tc = tier_color(item.get("tier", "?"))
            row(
                [item.get("company_name", "?"), f"{item.get('score', 0):.0f}", item.get("tier", "?")],
                [28, 8, 6],
                [None, "G", tc],
            )

    narrate("Stripe and OpenAI scoring highest. Let's look at exit readiness signals.")
    pause()


def disp_exit_signals():
    section("Step 2: Exit Readiness Assessment")
    narrate("One score that answers: 'Is this company ready to sell?'")

    # Use PE portfolio companies with financial data
    demo_companies = [
        (274, "SERVICETITAN INC"),
        (308, "SAILPOINT INC"),
        (1341, "Coupa Software Inc"),
        (1349, "Qualtrics International Inc."),
        (355, "N-ABLE INC"),
    ]

    # Show exit readiness for multiple companies
    section("Exit Readiness Scores — Portfolio Comparison")
    row(["Company", "Score", "Tier", "Timing"], [28, 8, 6, 34])
    row(["---", "---", "---", "---"], [28, 8, 6, 34])

    best_company_id = None
    best_score = 0

    for cid, cname in demo_companies:
        er = get(f"/pe/companies/{cid}/exit-readiness")
        if er and er.get("exit_readiness_score"):
            score_val = er["exit_readiness_score"]
            tier = er.get("tier", "?")
            tc = tier_color(tier)
            timing = (er.get("timing_recommendation", "") or "")[:34]
            row(
                [cname[:28], f"{score_val:.0f}", tier, timing],
                [28, 8, 6, 34],
                [None, "G" if score_val >= 70 else "Y", tc, "CY"],
            )
            if score_val > best_score:
                best_score = score_val
                best_company_id = cid

    # Deep dive on top exit candidate
    if best_company_id:
        print()
        er = get(f"/pe/companies/{best_company_id}/exit-readiness")
        if er:
            cname = er.get("company_name", "?")
            section(f"Exit Readiness Deep Dive: {cname}")
            narrate(f"Score: {er['exit_readiness_score']:.0f}/100 — Tier {er.get('tier', '?')}")

            # Category breakdown with visual bars
            cats = er.get("category_scores", {})
            if cats:
                for cat_name, cat_data in cats.items():
                    label = cat_name.replace("_", " ").title()
                    val = cat_data.get("score", 0) if isinstance(cat_data, dict) else cat_data
                    weight = cat_data.get("weight", "") if isinstance(cat_data, dict) else ""
                    bar_len = int(val / 2.5)
                    bar = "█" * bar_len + "░" * (40 - bar_len)
                    color = "G" if val >= 70 else "Y" if val >= 50 else "R"
                    c = getattr(C, color)
                    print(f"  {label:<22} {c}{bar}{C.E} {val:.0f} {C.DIM}({weight}){C.E}")

            # Strengths
            strengths_list = er.get("strengths", [])
            if strengths_list:
                print(f"\n  {C.G}{C.BD}Strengths:{C.E}")
                for s in strengths_list[:6]:
                    print(f"    {C.G}+{C.E} {s}")

            # Risks
            risks_list = er.get("risks", [])
            if risks_list:
                print(f"\n  {C.Y}{C.BD}Risks:{C.E}")
                for r_item in risks_list[:4]:
                    print(f"    {C.Y}!{C.E} {r_item}")

            print(f"\n  {C.BD}Recommendation:{C.E} {C.CY}{er.get('timing_recommendation', '')}{C.E}")

    # Also show benchmark comparison for the top company
    if best_company_id:
        print()
        section("Financial Benchmark vs. Peers")
        narrate("How does the exit candidate stack up against competitors?")
        bm = get(f"/pe/companies/{best_company_id}/benchmark")
        if bm and bm.get("company_metrics"):
            cm = bm["company_metrics"]
            ps = bm.get("peer_statistics", {})
            ranks = bm.get("company_percentile_rank", {})

            row(["Metric", "Company", "Peer Median", "Percentile"], [22, 14, 14, 12])
            row(["---", "---", "---", "---"], [22, 14, 14, 12])

            def fmt_metric(key, label, fmt_fn=lambda x: f"{x:.1f}%"):
                comp_val = cm.get(key)
                peer_med = ps.get(key, {}).get("median")
                pctile = ranks.get(key)
                comp_str = fmt_fn(comp_val) if comp_val is not None else "N/A"
                med_str = fmt_fn(peer_med) if peer_med is not None else "N/A"
                pct_str = f"P{pctile:.0f}" if pctile is not None else "N/A"
                pct_color = "G" if (pctile or 0) >= 60 else "Y" if (pctile or 0) >= 40 else "R"
                row([label, comp_str, med_str, pct_str], [22, 14, 14, 12],
                    [None, "CY", None, pct_color])

            fmt_metric("revenue_growth_pct", "Revenue Growth")
            fmt_metric("gross_margin_pct", "Gross Margin")
            fmt_metric("ebitda_margin_pct", "EBITDA Margin")
            fmt_metric("debt_to_ebitda", "Debt/EBITDA", lambda x: f"{x:.1f}x")

            # Valuation multiples
            vm = bm.get("valuation_multiples", {})
            co_mult = vm.get("company", {})
            peer_mult = vm.get("peer_stats", {})
            if co_mult.get("ev_revenue"):
                print()
                metric("Company EV/Revenue", f"{co_mult['ev_revenue']:.1f}x")
                med = peer_mult.get("ev_revenue", {}).get("median")
                if med:
                    metric("Peer Median EV/Revenue", f"{med:.1f}x")

            # Assessment
            assessment = bm.get("assessment", [])
            if assessment:
                print()
                for a in assessment:
                    icon = "+" if "outpace" in a.lower() or "above" in a.lower() or "strong" in a.lower() else ">"
                    color_a = C.G if icon == "+" else C.Y
                    print(f"    {color_a}{icon}{C.E} {a}")

    narrate("Exit readiness quantified with data-driven scoring. Time to map buyers.")
    pause()


def disp_buyer_universe():
    section("Step 3: Buyer Universe Mapping")
    target = "Stripe"
    narrate("Who would buy this company? Let's map strategic and financial buyers.")

    # Strategic buyers = competitors
    competitive = get(f"/competitive/{target}")
    if competitive:
        competitors = competitive.get("competitors", [])
        if competitors:
            section("Strategic Buyers (Competitive Landscape)")
            row(["Company", "Similarity", "Relationship", "Strengths"], [20, 12, 14, 24])
            row(["---", "---", "---", "---"], [20, 12, 14, 24])
            for comp in competitors[:8]:
                strengths = comp.get("strengths", [])
                strength_str = ", ".join(strengths[:2]) if strengths else ""
                row(
                    [
                        comp.get("name", "?"),
                        f"{comp.get('similarity_score', 0):.0%}",
                        comp.get("relationship", "?"),
                        strength_str[:24],
                    ],
                    [20, 12, 14, 24],
                )

    # Financial buyers = PE firms in similar sectors
    pe_firms = get("/pe/firms/", limit=10, strategy="Software")
    if not pe_firms or not pe_firms.get("firms"):
        pe_firms = get("/pe/firms/", limit=10)

    if pe_firms and pe_firms.get("firms"):
        print()
        section("Financial Buyers (PE Firms)")
        row(["Firm", "Strategy", "AUM", "Check Size"], [24, 22, 14, 14])
        row(["---", "---", "---", "---"], [24, 22, 14, 14])
        for f in pe_firms["firms"][:8]:
            aum_val = f.get("aum_usd_millions")
            aum_str = fmt_num(aum_val * 1e6) if aum_val else "N/A"
            row(
                [f.get("name", "?"), (f.get("primary_strategy", "") or "?")[:22], aum_str, "N/A"],
                [24, 22, 14, 14],
                [None, None, "G", None],
            )

    # Recent M&A activity
    recent = get("/pe/deals/activity/recent", days=90, limit=5)
    if recent and recent.get("deals"):
        print()
        section("Recent M&A Activity (Last 90 Days)")
        narrate("Recent comparable transactions support your valuation.")
        for deal in recent["deals"][:5]:
            ev = deal.get("enterprise_value_usd")
            ev_str = fmt_num(ev) if ev else "Undisclosed"
            print(f"  {C.BD}{deal.get('deal_name', deal.get('company_name', '?'))}{C.E}")
            print(f"    Type: {deal.get('deal_type', '?')}  |  EV: {ev_str}  |  Buyer: {deal.get('buyer', '?')}")

    narrate("Strategic and financial buyer universe mapped. Now let's build the data room.")
    pause()


def disp_data_room():
    section("Step 4: Data Room Preparation")
    narrate("One-click data room foundation. What used to take weeks.")

    # Use a PE portfolio company with rich data
    sample_company_id = 308  # SailPoint
    sample_name = "SAILPOINT INC"

    # Financial time series
    financials = get(f"/pe/companies/{sample_company_id}/financials", limit=5)
    if financials and financials.get("financials"):
        section(f"Financial Time Series: {sample_name}")
        row(["Year", "Revenue", "Growth", "EBITDA", "Margin", "FCF"], [6, 12, 8, 12, 8, 12])
        row(["---", "---", "---", "---", "---", "---"], [6, 12, 8, 12, 8, 12])
        for fin in reversed(financials["financials"][:5]):
            period = fin.get("period", {})
            inc = fin.get("income_statement", {})
            cf = fin.get("cash_flow", {})
            growth = inc.get("revenue_growth_pct")
            margin = inc.get("ebitda_margin_pct")
            gc = "G" if (growth or 0) > 15 else "Y" if (growth or 0) > 5 else "R"
            mc = "G" if (margin or 0) > 20 else "Y" if (margin or 0) > 10 else "R"
            row(
                [
                    str(period.get("fiscal_year", "?")),
                    fmt_num(inc.get("revenue_usd")),
                    fmt_pct(growth),
                    fmt_num(inc.get("ebitda_usd")),
                    f"{margin:.0f}%" if margin else "N/A",
                    fmt_num(cf.get("free_cash_flow_usd")),
                ],
                [6, 12, 8, 12, 8, 12],
                [None, "CY", gc, "CY", mc, "G"],
            )

    # Leadership
    leaders = get(f"/pe/companies/{sample_company_id}/leadership")
    if leaders and leaders.get("leadership"):
        print()
        section(f"Leadership Team: {sample_name}")
        for leader in leaders["leadership"][:8]:
            title = leader.get("title", "?")
            flags = leader.get("flags", {})
            pe_rel = leader.get("pe_relationship", {})
            flag_str = ""
            if flags.get("is_ceo"):
                flag_str = f" {C.G}[CEO]{C.E}"
            elif flags.get("is_cfo"):
                flag_str = f" {C.CY}[CFO]{C.E}"
            elif flags.get("is_board_member"):
                flag_str = f" {C.B}[Board]{C.E}"
            if pe_rel.get("appointed_by_pe"):
                flag_str += f" {C.Y}[PE-Appointed]{C.E}"
            if pe_rel.get("firm_affiliation"):
                flag_str += f" {C.DIM}({pe_rel['firm_affiliation']}){C.E}"
            print(f"    {leader.get('name', '?'):<28} {title}{flag_str}")

    # Valuations
    vals = get(f"/pe/companies/{sample_company_id}/valuations", limit=5)
    if vals and vals.get("valuations"):
        print()
        section(f"Valuation History: {sample_name}")
        for v in vals["valuations"][:5]:
            values = v.get("values", {})
            multiples = v.get("multiples", {})
            context = v.get("context", {})
            ev = values.get("enterprise_value_usd")
            print(f"  {C.BD}{v.get('date', '?')}{C.E} ({context.get('event', context.get('type', '?'))})")
            if ev:
                metric("  Enterprise Value", fmt_num(ev))
            if multiples.get("ev_ebitda"):
                metric("  EV/EBITDA", f"{multiples['ev_ebitda']:.1f}x")
            if multiples.get("ev_revenue"):
                metric("  EV/Revenue", f"{multiples['ev_revenue']:.1f}x")

    # Competitors
    comps = get(f"/pe/companies/{sample_company_id}/competitors")
    if comps and comps.get("competitors"):
        print()
        section(f"Competitive Landscape: {sample_name}")
        row(["Competitor", "Type", "Position", "Public", "PE-Backed"], [20, 10, 12, 8, 12])
        row(["---", "---", "---", "---", "---"], [20, 10, 12, 8, 12])
        for comp in comps["competitors"][:6]:
            pub = comp.get("public_info", {})
            pe = comp.get("pe_info", {})
            pos = comp.get("competitive_position", {})
            pub_str = pub.get("ticker", "No") if pub.get("is_public") else "No"
            pe_str = (pe.get("pe_owner") or "No")[:12] if pe.get("is_pe_backed") else "No"
            row(
                [comp.get("name", "?"), pos.get("type", "?"), pos.get("market_position", "?"), pub_str, pe_str],
                [20, 10, 12, 8, 12],
            )

    # Reports & exports
    print()
    section("Export & Report Capabilities")
    narrate("Everything exportable for your virtual data room.")

    exports = [
        ("PE Company Financials", "CSV/JSON/Parquet"),
        ("Leadership Roster", "CSV/JSON/Parquet"),
        ("Valuation History", "CSV/JSON/Parquet"),
        ("Deal History", "CSV/JSON/Parquet"),
        ("Competitor Analysis", "Markdown/HTML/JSON"),
        ("Company Profile Report", "Markdown/HTML"),
        ("Due Diligence Memo", "Markdown/HTML"),
        ("Competitive Landscape", "Markdown/HTML"),
    ]
    for name, fmt in exports:
        print(f"    {C.G}>{C.E} {name:<30} {C.DIM}({fmt}){C.E}")

    narrate("All via API — automate data room assembly with a single script.")
    pause()


def disp_lp_reporting():
    section("Step 5: LP Communication")
    narrate("When the deal closes, LP communications are pre-drafted.")

    # Show PE firm fund data
    firms = get("/pe/firms/", limit=3)
    if firms and firms.get("firms"):
        for firm in firms["firms"][:2]:
            fid = firm.get("id")
            if not fid:
                continue
            funds = get(f"/pe/firms/{fid}/funds")
            if funds and funds.get("funds"):
                section(f"Fund Performance: {firm.get('name', '?')}")
                for fund in funds["funds"][:3]:
                    print(f"  {C.BD}{fund.get('name', '?')}{C.E}")
                    metric("  Vintage", str(fund.get("vintage_year", "?")))
                    size = fund.get("final_close_usd_millions", fund.get("target_size_usd_millions"))
                    if size and isinstance(size, (int, float)):
                        metric("  Fund Size", fmt_num(size * 1e6))
                    terms = fund.get("terms", {})
                    if terms:
                        if terms.get("mgmt_fee"):
                            metric("  Mgmt Fee", f"{terms['mgmt_fee']}%")
                        if terms.get("carry"):
                            metric("  Carry", f"{terms['carry']}%")

    # Show report templates
    templates = get("/ai-reports/templates")
    if templates:
        print()
        section("Available Report Templates")
        for t in templates[:6]:
            if isinstance(t, dict):
                name = t.get("name", "?")
                desc = (t.get("description", "") or "")[:50]
                print(f"    {C.CY}>{C.E} {name:<20} {C.DIM}{desc}{C.E}")

    narrate("Investor memo, portfolio summary, and exit announcement — all auto-generated.")


def disp_closing():
    banner(
        "Disposition Story Complete",
        "From portfolio health check to data room in 15 minutes."
    )
    print(f"""
  {C.BD}What you saw:{C.E}
    {C.G}1.{C.E} Portfolio health dashboard with PE firms, companies & health rankings
    {C.G}2.{C.E} Exit readiness scoring: 6-signal composite (0-100) with tier & timing
    {C.G}3.{C.E} Financial benchmarking vs. peers (percentile ranks, multiples)
    {C.G}4.{C.E} Buyer universe: strategic competitors + financial PE firms + recent M&A
    {C.G}5.{C.E} Data room: financial time series, leadership, valuations, competitors
    {C.G}6.{C.E} LP reporting: fund performance + auto-generated memos

  {C.BD}Key differentiator:{C.E}
    {C.CY}Exit Readiness Score{C.E} — nobody else has this.
    6 signals (financials, trajectory, leadership, valuation, market, hold period)
    scored nightly. Your board knows when it's time to sell.

  {C.BD}Time saved:{C.E} {C.G}4-6 months of exit prep → continuous readiness + instant export{C.E}
""")


# ---------------------------------------------------------------------------
# COMBINED DEMO
# ---------------------------------------------------------------------------

def demo_intro():
    print(f"""
{C.BD}{C.CY}
    ╔══════════════════════════════════════════════════════════════╗
    ║                                                              ║
    ║     N E X D A T A                                            ║
    ║     Private Markets Intelligence Platform                    ║
    ║                                                              ║
    ║     "Bloomberg Terminal for PE — with an AI analyst team"    ║
    ║                                                              ║
    ╚══════════════════════════════════════════════════════════════╝
{C.E}
    {C.BD}28+ data sources{C.E}  |  {C.BD}10 AI agents{C.E}  |  {C.BD}500+ API endpoints{C.E}

    Today we'll show two stories:

    {C.G}Story 1:{C.E} Finding and Winning a Deal (Acquisition)
    {C.G}Story 2:{C.E} Preparing and Executing an Exit (Disposition)
""")
    pause()


def demo_closing():
    banner("Demo Complete", "Thank you for your time.")
    print(f"""
  {C.BD}Nexdata replaces:{C.E}
    PitchBook ($25K/seat)    → Target discovery + health scoring
    Capital IQ ($20K/seat)   → Financial benchmarking + comps
    DealCloud ($15K/seat)    → Deal pipeline + AI scoring
    McKinsey ($500K/project) → Automated DD + competitive intel

  {C.BD}Starting at $2K/month.{C.E}

  {C.CY}API Docs:  http://localhost:8001/docs
  Dashboard: http://localhost:8080 (serve demo/index.html){C.E}
""")


# ---------------------------------------------------------------------------
# SECTION MAP
# ---------------------------------------------------------------------------

ACQUISITION_SECTIONS = [
    ("intro", acq_intro),
    ("market", acq_market_intel),
    ("discovery", acq_target_discovery),
    ("deepdive", acq_deep_dive),
    ("diligence", acq_due_diligence),
    ("pipeline", acq_pipeline),
    ("monitoring", acq_monitoring),
    ("closing", acq_closing),
]

DISPOSITION_SECTIONS = [
    ("intro", disp_intro),
    ("portfolio", disp_portfolio_health),
    ("exit_readiness", disp_exit_signals),
    ("buyers", disp_buyer_universe),
    ("dataroom", disp_data_room),
    ("lp", disp_lp_reporting),
    ("closing", disp_closing),
]

ALL_SECTIONS = {name: fn for name, fn in ACQUISITION_SECTIONS + DISPOSITION_SECTIONS}


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    global QUICK_MODE, API_BASE

    parser = argparse.ArgumentParser(description="Nexdata PE Demo")
    parser.add_argument("--story", choices=["acquisition", "disposition"], help="Run one story only")
    parser.add_argument("--section", type=str, help=f"Run single section: {', '.join(ALL_SECTIONS.keys())}")
    parser.add_argument("--quick", action="store_true", help="Skip pauses")
    parser.add_argument("--api-url", default="http://localhost:8001", help="API base URL")
    args = parser.parse_args()

    QUICK_MODE = args.quick
    API_BASE = f"{args.api_url}/api/v1"

    # Health check
    try:
        r = requests.get(f"{args.api_url}/health", timeout=5)
        if r.status_code != 200:
            print(f"{C.R}API not healthy. Run: docker-compose up -d{C.E}")
            sys.exit(1)
    except Exception:
        print(f"{C.R}Cannot connect to API at {args.api_url}{C.E}")
        sys.exit(1)

    # Single section
    if args.section:
        if args.section in ALL_SECTIONS:
            ALL_SECTIONS[args.section]()
        else:
            print(f"Unknown section: {args.section}")
            print(f"Available: {', '.join(ALL_SECTIONS.keys())}")
            sys.exit(1)
        return

    # Full demo
    demo_intro()

    if args.story != "disposition":
        for _, fn in ACQUISITION_SECTIONS:
            fn()

    if args.story != "acquisition":
        for _, fn in DISPOSITION_SECTIONS:
            fn()

    demo_closing()

    print(f"\n{C.G}Demo Complete!{C.E}\n")


if __name__ == "__main__":
    main()
