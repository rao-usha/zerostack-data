"""
Deal Radar — AI Deal Memo Generator.

Gathers real signal data from 5 source tables for a convergence region,
uses Claude to synthesize a full investment memo with 6 sections, and
renders it as a styled HTML document using the Nexdata design system.
"""

import json
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.core.convergence_models import ConvergenceRegion
from app.services.convergence_engine import REGION_DEFINITIONS

logger = logging.getLogger(__name__)

MEMO_SECTIONS = [
    "executive_summary",
    "market_opportunity",
    "signal_analysis",
    "comparable_deals",
    "target_profile",
    "risk_factors",
    "recommended_action",
    "lp_considerations",
]

MEMO_SYSTEM_PROMPT = """You are a senior private equity investment analyst writing a deal memo for an Investment Committee.

Given data about a US region showing convergent public signals, write a professional investment memo with these 6 sections. Each section should be 3-5 sentences of sharp, actionable analysis.

Output as JSON with this exact format:
{
  "executive_summary": "...",
  "market_opportunity": "...",
  "signal_analysis": "...",
  "comparable_deals": "...",
  "target_profile": "...",
  "risk_factors": "...",
  "recommended_action": "...",
  "lp_considerations": "..."
}

Section guidelines:
- executive_summary: Lead with the thesis. What's the opportunity and why now?
- market_opportunity: Size the addressable market. What macro trends create tailwinds?
- signal_analysis: Why are these signals converging here? What does the data pattern mean?
- comparable_deals: Reference the comparable PE deals provided. What do multiples and deal structures tell us? What's the implied valuation range?
- target_profile: What kind of companies should we acquire? Revenue range, characteristics, fragmentation.
- risk_factors: What could go wrong? Regulatory, market, execution risks.
- recommended_action: Specific next steps. Entry timing, deal structure, diligence priorities.
- lp_considerations: Why should LPs care? Fund-level thesis, return expectations, portfolio fit, co-invest potential, ESG angles.

Write in crisp, professional PE language. No hedging. Be specific with numbers from the data provided.
Return ONLY valid JSON, no markdown, no backticks."""


@dataclass
class MemoResult:
    region_id: str
    title: str
    html: str
    sections: List[str]
    generated_at: str
    data_summary: Dict[str, Any]


class DealRadarMemoGenerator:
    """AI-powered investment memo generator for convergence clusters."""

    def __init__(self, db: Session):
        self.db = db

    async def generate(self, region_id: str) -> Optional[MemoResult]:
        """Generate a full investment memo for a convergence cluster."""
        defn = REGION_DEFINITIONS.get(region_id)
        if not defn:
            return None

        # 1. Gather all signal data
        data = self._gather_data(region_id, defn)
        if not data.get("region"):
            return None

        # 2. Generate AI analysis sections
        analysis = await self._generate_analysis(data)

        # 3. Render HTML memo
        html = self._render_memo(data, analysis)

        return MemoResult(
            region_id=region_id,
            title=f"{defn['label']} — Convergence Investment Memo",
            html=html,
            sections=MEMO_SECTIONS,
            generated_at=datetime.utcnow().isoformat(),
            data_summary={
                "convergence_score": data["region"].get("convergence_score", 0),
                "cluster_status": data["region"].get("cluster_status", "LOW"),
                "active_signals": data["region"].get("active_signals", []),
                "states": defn["states"],
            },
        )

    def _gather_data(self, region_id: str, defn: Dict) -> Dict[str, Any]:
        """Pull all available data for the region from source tables."""
        states = defn["states"]
        data = {"region_id": region_id, "label": defn["label"], "states": states}

        # Convergence scores
        region = self.db.query(ConvergenceRegion).filter(
            ConvergenceRegion.region_id == region_id
        ).first()
        if region:
            data["region"] = region.to_dict()
        else:
            data["region"] = {}

        # EPA details
        data["epa"] = self._gather_epa(states)
        # IRS migration
        data["migration"] = self._gather_migration(states)
        # Trade
        data["trade"] = self._gather_trade(states)
        # Water systems
        data["water"] = self._gather_water(states)
        # Income/Macro
        data["income"] = self._gather_income(states)
        # Comparable PE deals in region
        data["deals"] = self._gather_deals(states)
        # PE portfolio companies in region
        data["companies"] = self._gather_pe_companies(states)

        return data

    def _gather_epa(self, states: List[str]) -> Dict:
        try:
            row = self.db.execute(text("""
                SELECT COUNT(*) as facilities,
                       COALESCE(SUM(violation_count), 0) as violations,
                       COALESCE(SUM(penalty_amount), 0) as penalties,
                       COALESCE(AVG(violation_count), 0) as avg_violations
                FROM epa_echo_facilities WHERE state = ANY(:s)
            """), {"s": states}).mappings().first()
            return dict(row) if row else {}
        except Exception as e:
            logger.warning("Memo: EPA gather failed: %s", e)
            return {}

    def _gather_migration(self, states: List[str]) -> Dict:
        try:
            inflow = self.db.execute(text("""
                SELECT COALESCE(SUM(num_returns), 0) as returns,
                       COALESCE(SUM(total_agi), 0) as agi
                FROM irs_soi_migration
                WHERE orig_state_abbr = ANY(:s) AND flow_type = 'inflow'
            """), {"s": states}).mappings().first()
            outflow = self.db.execute(text("""
                SELECT COALESCE(SUM(num_returns), 0) as returns,
                       COALESCE(SUM(total_agi), 0) as agi
                FROM irs_soi_migration
                WHERE orig_state_abbr = ANY(:s) AND flow_type = 'outflow'
            """), {"s": states}).mappings().first()
            return {
                "inflow_returns": int(inflow["returns"]) if inflow else 0,
                "inflow_agi": int(inflow["agi"]) if inflow else 0,
                "outflow_returns": int(outflow["returns"]) if outflow else 0,
                "outflow_agi": int(outflow["agi"]) if outflow else 0,
                "net_returns": int((inflow["returns"] if inflow else 0) - (outflow["returns"] if outflow else 0)),
                "net_agi": int((inflow["agi"] if inflow else 0) - (outflow["agi"] if outflow else 0)),
            }
        except Exception as e:
            logger.warning("Memo: Migration gather failed: %s", e)
            return {}

    def _gather_trade(self, states: List[str]) -> Dict:
        try:
            row = self.db.execute(text("""
                SELECT COALESCE(SUM(value_monthly), 0) as total_exports,
                       COUNT(DISTINCT country_code) as countries,
                       COUNT(DISTINCT hs_code) as commodities
                FROM us_trade_exports_state WHERE state_code = ANY(:s)
            """), {"s": states}).mappings().first()
            return dict(row) if row else {}
        except Exception as e:
            logger.warning("Memo: Trade gather failed: %s", e)
            return {}

    def _gather_water(self, states: List[str]) -> Dict:
        try:
            row = self.db.execute(text("""
                SELECT COUNT(*) as systems,
                       COALESCE(SUM(population_served), 0) as population
                FROM public_water_system WHERE state = ANY(:s)
            """), {"s": states}).mappings().first()
            return dict(row) if row else {}
        except Exception as e:
            logger.warning("Memo: Water gather failed: %s", e)
            return {}

    def _gather_income(self, states: List[str]) -> Dict:
        try:
            row = self.db.execute(text("""
                SELECT COALESCE(SUM(num_returns), 0) as total_returns,
                       COALESCE(SUM(total_agi), 0) as total_agi,
                       COALESCE(SUM(total_capital_gains), 0) as capgains,
                       COALESCE(SUM(total_business_income), 0) as biz_income,
                       CASE WHEN SUM(num_returns) > 0
                            THEN SUM(total_agi) * 1000.0 / SUM(num_returns)
                            ELSE 0 END as avg_agi
                FROM irs_soi_zip_income WHERE state_abbr = ANY(:s)
            """), {"s": states}).mappings().first()
            return {
                "total_returns": int(row["total_returns"]) if row else 0,
                "total_agi_thousands": int(row["total_agi"]) if row else 0,
                "capital_gains_thousands": int(row["capgains"]) if row else 0,
                "business_income_thousands": int(row["biz_income"]) if row else 0,
                "avg_agi": round(float(row["avg_agi"]), 0) if row else 0,
            }
        except Exception as e:
            logger.warning("Memo: Income gather failed: %s", e)
            return {}

    def _gather_deals(self, states: List[str]) -> List[Dict]:
        """Get comparable PE deals in the region states."""
        try:
            rows = self.db.execute(text("""
                SELECT d.deal_name, d.deal_type, d.enterprise_value_usd,
                       d.ev_ebitda_multiple, d.ev_revenue_multiple,
                       d.ltm_revenue_usd, d.ltm_ebitda_usd,
                       d.buyer_name, d.seller_name, d.closed_date,
                       d.status, c.company_name, c.industry, c.headquarters_state
                FROM pe_deals d
                JOIN pe_portfolio_companies c ON d.company_id = c.id
                WHERE c.headquarters_state = ANY(:states)
                ORDER BY d.enterprise_value_usd DESC NULLS LAST
                LIMIT 15
            """), {"states": states}).mappings().fetchall()
            return [dict(r) for r in rows]
        except Exception as e:
            logger.warning("Memo: deals gather failed: %s", e)
            return []

    def _gather_pe_companies(self, states: List[str]) -> List[Dict]:
        """Get PE portfolio companies headquartered in region states."""
        try:
            rows = self.db.execute(text("""
                SELECT company_name, industry, sub_industry,
                       headquarters_city, headquarters_state,
                       revenue_usd, ebitda_usd, employee_count,
                       year_founded, ownership_status
                FROM pe_portfolio_companies
                WHERE headquarters_state = ANY(:states)
                ORDER BY revenue_usd DESC NULLS LAST
                LIMIT 15
            """), {"states": states}).mappings().fetchall()
            return [dict(r) for r in rows]
        except Exception as e:
            logger.warning("Memo: PE companies gather failed: %s", e)
            return []

    async def _generate_analysis(self, data: Dict) -> Dict[str, str]:
        """Use Claude to generate 6 memo sections."""
        region = data.get("region", {})
        label = data.get("label", "Unknown")
        states = data.get("states", [])

        context = f"""Region: {label} ({', '.join(states)})
Convergence Score: {region.get('convergence_score', 0)}/100
Cluster Status: {region.get('cluster_status', 'LOW')}
Active Signals: {', '.join(region.get('active_signals', []))}

Signal Scores:
- EPA Environmental: {region.get('epa_score', 0)}/100
- IRS Migration: {region.get('irs_migration_score', 0)}/100
- Trade & Commerce: {region.get('trade_score', 0)}/100
- Water Systems: {region.get('water_score', 0)}/100
- Macro / Income: {region.get('macro_score', 0)}/100

EPA Data:
- {data.get('epa', {}).get('facilities', 0):,} facilities tracked
- {data.get('epa', {}).get('violations', 0):,} total violations
- ${data.get('epa', {}).get('penalties', 0):,.0f} in penalties

Migration Data:
- {data.get('migration', {}).get('inflow_returns', 0):,} inflow returns
- {data.get('migration', {}).get('outflow_returns', 0):,} outflow returns
- Net migration: {data.get('migration', {}).get('net_returns', 0):,} returns
- Net AGI movement: ${data.get('migration', {}).get('net_agi', 0):,}K

Trade Data:
- ${data.get('trade', {}).get('total_exports', 0):,} in exports
- {data.get('trade', {}).get('countries', 0)} trading partners
- {data.get('trade', {}).get('commodities', 0)} commodity categories

Water Infrastructure:
- {data.get('water', {}).get('systems', 0):,} water systems
- {data.get('water', {}).get('population', 0):,} population served

Income Profile:
- {data.get('income', {}).get('total_returns', 0):,} tax returns
- Average AGI: ${data.get('income', {}).get('avg_agi', 0):,.0f}
- Capital gains: ${data.get('income', {}).get('capital_gains_thousands', 0):,}K
- Business income: ${data.get('income', {}).get('business_income_thousands', 0):,}K"""

        # Add comparable deals
        deals = data.get("deals", [])
        if deals:
            context += "\n\nComparable PE Deals in Region:"
            for d in deals[:10]:
                ev = d.get("enterprise_value_usd")
                ev_str = f"${float(ev):,.0f}" if ev else "undisclosed"
                mult = d.get("ev_ebitda_multiple")
                mult_str = f"{float(mult):.1f}x" if mult else "n/a"
                context += f"\n- {d.get('deal_name', 'Unknown')}: {d.get('deal_type', '')} | EV: {ev_str} | EV/EBITDA: {mult_str} | Buyer: {d.get('buyer_name', 'n/a')} | Industry: {d.get('industry', 'n/a')} | State: {d.get('headquarters_state', '')}"
        else:
            context += "\n\nComparable PE Deals: None found in database for this region."

        # Add PE portfolio companies
        companies = data.get("companies", [])
        if companies:
            context += "\n\nPE Portfolio Companies in Region:"
            for c in companies[:10]:
                rev = c.get("revenue_usd")
                rev_str = f"${float(rev):,.0f}" if rev else "undisclosed"
                context += f"\n- {c.get('company_name', 'Unknown')}: {c.get('industry', 'n/a')} | Revenue: {rev_str} | Employees: {c.get('employee_count', 'n/a')} | {c.get('headquarters_city', '')}, {c.get('headquarters_state', '')}"
        else:
            context += "\n\nPE Portfolio Companies: None found in database for this region."

        try:
            from app.agentic.llm_client import LLMClient

            llm = LLMClient(
                provider="anthropic",
                model="claude-3-5-haiku-20241022",
                max_tokens=2000,
                temperature=0.3,
            )

            response = await llm.complete(
                prompt=context,
                system_prompt=MEMO_SYSTEM_PROMPT,
            )

            parsed = response.parse_json()
            if parsed:
                return parsed

            try:
                return json.loads(response.content.strip())
            except json.JSONDecodeError:
                pass

        except Exception as e:
            logger.warning("Memo: LLM generation failed: %s", e)

        # Fallback: data-only sections
        return self._fallback_sections(data)

    def _fallback_sections(self, data: Dict) -> Dict[str, str]:
        """Generate data-only sections when LLM is unavailable."""
        label = data.get("label", "Region")
        region = data.get("region", {})
        score = region.get("convergence_score", 0)
        signals = ", ".join(region.get("active_signals", []))

        deals = data.get("deals", [])
        deal_count = len(deals)
        deal_text = f"{deal_count} comparable PE transactions found in {', '.join(data.get('states', []))}."
        if deals:
            top = deals[0]
            ev = top.get("enterprise_value_usd")
            deal_text += f" Largest: {top.get('deal_name', 'n/a')} at ${float(ev):,.0f} EV." if ev else ""

        companies = data.get("companies", [])
        co_text = f"{len(companies)} PE-backed companies headquartered in the region."
        if companies:
            industries = list(set(c.get("industry", "") for c in companies if c.get("industry")))[:5]
            co_text += f" Key sectors: {', '.join(industries)}." if industries else ""

        return {
            "executive_summary": f"{label} shows a convergence score of {score}/100 with active signals in {signals}. Multiple public data sources indicate aligned investment conditions in this geography.",
            "market_opportunity": f"The region encompasses {', '.join(data.get('states', []))} with {data.get('income', {}).get('total_returns', 0):,} tax filers and average AGI of ${data.get('income', {}).get('avg_agi', 0):,.0f}.",
            "signal_analysis": f"EPA score: {region.get('epa_score', 0)}, IRS migration: {region.get('irs_migration_score', 0)}, Trade: {region.get('trade_score', 0)}, Water: {region.get('water_score', 0)}, Macro: {region.get('macro_score', 0)}.",
            "comparable_deals": deal_text,
            "target_profile": co_text + " Connect an API key to generate AI-powered target profiles.",
            "risk_factors": "Regulatory and environmental risk elevated given EPA signal strength. Connect an API key for full risk analysis.",
            "recommended_action": "Run a convergence scan to populate the latest data, then generate an AI-powered investment thesis.",
            "lp_considerations": f"Region convergence score of {score}/100 across {len(region.get('active_signals', []))} signals suggests a differentiated sourcing angle. The multi-signal thesis provides LP-ready narrative for capital deployment in {label}.",
        }

    def _render_memo(self, data: Dict, analysis: Dict[str, str]) -> str:
        """Render the memo as a self-contained HTML doc with D3 visualizations."""
        import html as html_mod

        region = data.get("region", {})
        label = data.get("label", "Region")
        score = region.get("convergence_score", 0)
        status = region.get("cluster_status", "LOW")
        signals = region.get("active_signals", [])
        states = data.get("states", [])
        date_str = datetime.utcnow().strftime("%B %d, %Y")

        epa = region.get("epa_score", 0)
        irs = region.get("irs_migration_score", 0)
        trade = region.get("trade_score", 0)
        water = region.get("water_score", 0)
        macro = region.get("macro_score", 0)

        status_color = "#e24b4a" if status == "HOT" else "#ba7517" if status == "ACTIVE" else "#7f77dd"

        # Build section HTML
        section_map = {
            "executive_summary": ("Executive Summary", "01"),
            "market_opportunity": ("Market Opportunity", "02"),
            "signal_analysis": ("Signal Analysis", "03"),
            "comparable_deals": ("Comparable Deals", "04"),
            "target_profile": ("Target Profile", "05"),
            "risk_factors": ("Risk Factors", "06"),
            "recommended_action": ("Recommended Action", "07"),
            "lp_considerations": ("LP Considerations", "08"),
        }

        sections_html = ""
        for key in MEMO_SECTIONS:
            title, num = section_map.get(key, (key.replace("_", " ").title(), ""))
            text = html_mod.escape(analysis.get(key, ""))
            extra = ""

            if key == "comparable_deals":
                deals = data.get("deals", [])
                if deals:
                    rows = ""
                    for d in deals[:8]:
                        ev = d.get("enterprise_value_usd")
                        ev_s = f"${float(ev)/1e6:,.0f}M" if ev and float(ev) > 0 else "—"
                        mult = d.get("ev_ebitda_multiple")
                        mult_s = f"{float(mult):.1f}x" if mult else "—"
                        rows += f'<tr><td>{html_mod.escape(str(d.get("deal_name","—")))}</td><td>{html_mod.escape(str(d.get("deal_type","")))}</td><td class="num">{ev_s}</td><td class="num">{mult_s}</td><td>{html_mod.escape(str(d.get("buyer_name","—")))}</td></tr>'
                    extra = f'<table class="dtable"><thead><tr><th>Deal</th><th>Type</th><th>EV</th><th>EV/EBITDA</th><th>Buyer</th></tr></thead><tbody>{rows}</tbody></table>'
                else:
                    extra = '<div class="empty-note">No comparable PE deals found in region database. Run PE collection to populate.</div>'

            if key == "target_profile":
                companies = data.get("companies", [])
                if companies:
                    rows = ""
                    for c in companies[:8]:
                        rev = c.get("revenue_usd")
                        rev_s = f"${float(rev)/1e6:,.0f}M" if rev and float(rev) > 0 else "—"
                        rows += f'<tr><td>{html_mod.escape(str(c.get("company_name","—")))}</td><td>{html_mod.escape(str(c.get("industry","—")))}</td><td class="num">{rev_s}</td><td>{html_mod.escape(str(c.get("headquarters_state","")))}</td></tr>'
                    extra = f'<table class="dtable"><thead><tr><th>Company</th><th>Industry</th><th>Revenue</th><th>State</th></tr></thead><tbody>{rows}</tbody></table>'

            sections_html += f'''
            <div class="memo-section">
              <div class="section-num">{num}</div>
              <h2>{title}</h2>
              <p>{text}</p>
              {extra}
            </div>'''

        # Data for signal bars
        epa_fac = data.get("epa", {}).get("facilities", 0)
        epa_viol = data.get("epa", {}).get("violations", 0)
        mig_net = data.get("migration", {}).get("net_returns", 0)
        mig_agi = data.get("migration", {}).get("net_agi", 0)
        trade_exp = data.get("trade", {}).get("total_exports", 0)
        trade_ctry = data.get("trade", {}).get("countries", 0)
        water_sys = data.get("water", {}).get("systems", 0)
        water_pop = data.get("water", {}).get("population", 0)
        inc_agi = data.get("income", {}).get("avg_agi", 0)
        inc_ret = data.get("income", {}).get("total_returns", 0)

        return f'''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{html_mod.escape(label)} — Investment Memo</title>
<script src="https://cdn.jsdelivr.net/npm/d3@7/dist/d3.min.js"></script>
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap');
*{{box-sizing:border-box;margin:0;padding:0}}
body{{background:#0c0c10;color:#d4d0c8;font-family:'Inter',system-ui,sans-serif;font-size:14px;line-height:1.6;-webkit-font-smoothing:antialiased}}
.wrap{{max-width:1100px;margin:0 auto;padding:40px 32px 60px}}

/* Hero */
.hero{{position:relative;padding:48px 0 36px;border-bottom:1px solid rgba(255,255,255,.06);margin-bottom:36px}}
.hero-badge{{display:inline-flex;align-items:center;gap:6px;font-size:11px;font-weight:600;padding:4px 12px;border-radius:20px;background:{status_color}18;color:{status_color};border:1px solid {status_color}30;text-transform:uppercase;letter-spacing:.08em;margin-bottom:16px}}
.hero-badge .dot{{width:6px;height:6px;border-radius:50%;background:{status_color};animation:pulse 1.5s ease infinite}}
@keyframes pulse{{0%,100%{{opacity:1}}50%{{opacity:.3}}}}
.hero h1{{font-size:32px;font-weight:800;color:#f0ede6;letter-spacing:-.03em;margin-bottom:6px}}
.hero .subtitle{{font-size:13px;color:#5a574f;font-weight:400}}
.hero .date{{font-size:12px;color:#444;margin-top:2px}}

/* KPI row */
.kpi-row{{display:grid;grid-template-columns:repeat(4,1fr);gap:16px;margin-bottom:40px}}
.kpi{{background:#141418;border:1px solid rgba(255,255,255,.05);border-radius:12px;padding:20px;text-align:center}}
.kpi .lbl{{font-size:10px;font-weight:600;text-transform:uppercase;letter-spacing:.1em;color:#555;margin-bottom:8px}}
.kpi .val{{font-size:28px;font-weight:800;color:#f0ede6;letter-spacing:-.02em}}
.kpi .val.hot{{color:{status_color}}}

/* Charts grid */
.charts-grid{{display:grid;grid-template-columns:1fr 1fr;gap:20px;margin-bottom:40px}}
.chart-card{{background:#141418;border:1px solid rgba(255,255,255,.05);border-radius:12px;padding:24px;position:relative}}
.chart-card h3{{font-size:11px;font-weight:600;text-transform:uppercase;letter-spacing:.1em;color:#555;margin-bottom:16px}}
.chart-card svg{{display:block;margin:0 auto}}

/* Signal bars */
.signal-bars{{display:flex;flex-direction:column;gap:14px}}
.sig-row{{display:flex;align-items:center;gap:12px}}
.sig-label{{width:100px;font-size:12px;font-weight:500;color:#888;text-align:right;flex-shrink:0}}
.sig-track{{flex:1;height:8px;background:rgba(255,255,255,.04);border-radius:4px;overflow:hidden;position:relative}}
.sig-fill{{height:100%;border-radius:4px;transition:width 1s ease}}
.sig-val{{width:36px;font-size:13px;font-weight:700;color:#d4d0c8;text-align:right;flex-shrink:0}}

/* Sections */
.memo-section{{background:#141418;border:1px solid rgba(255,255,255,.05);border-radius:12px;padding:28px 28px 24px;margin-bottom:20px;position:relative}}
.memo-section .section-num{{position:absolute;top:20px;right:24px;font-size:48px;font-weight:800;color:rgba(255,255,255,.03);line-height:1}}
.memo-section h2{{font-size:16px;font-weight:700;color:#e8e5dd;margin-bottom:12px;letter-spacing:-.01em}}
.memo-section p{{font-size:14px;line-height:1.8;color:#999;margin-bottom:16px}}
.memo-section .empty-note{{font-size:12px;color:#444;font-style:italic;padding:12px 0}}

/* Tables */
.dtable{{width:100%;border-collapse:collapse;font-size:12px;margin-top:12px}}
.dtable th{{text-align:left;padding:8px 12px;font-weight:600;font-size:10px;text-transform:uppercase;letter-spacing:.08em;color:#555;border-bottom:1px solid rgba(255,255,255,.08)}}
.dtable td{{padding:8px 12px;border-bottom:1px solid rgba(255,255,255,.03);color:#aaa}}
.dtable td.num{{font-variant-numeric:tabular-nums;text-align:right;color:#d4d0c8;font-weight:600}}
.dtable tr:hover td{{background:rgba(255,255,255,.02)}}

/* Data summary */
.data-grid{{display:grid;grid-template-columns:repeat(5,1fr);gap:12px;margin-bottom:40px}}
.data-card{{background:#141418;border:1px solid rgba(255,255,255,.04);border-radius:10px;padding:16px;text-align:center}}
.data-card .dc-icon{{font-size:20px;margin-bottom:6px;opacity:.6}}
.data-card .dc-val{{font-size:16px;font-weight:700;color:#e8e5dd;margin-bottom:2px}}
.data-card .dc-lbl{{font-size:10px;color:#555;text-transform:uppercase;letter-spacing:.06em}}

/* Disclaimer */
.disclaimer{{padding:16px 20px;background:rgba(186,117,23,.06);border:1px solid rgba(186,117,23,.15);border-radius:8px;font-size:11px;color:#8a7440;line-height:1.7;margin-top:20px}}

@media(max-width:700px){{
  .kpi-row,.charts-grid{{grid-template-columns:1fr 1fr}}
  .data-grid{{grid-template-columns:repeat(3,1fr)}}
  .hero h1{{font-size:24px}}
}}
@media print{{body{{background:white;color:#222}}.memo-section,.chart-card,.kpi{{border-color:#ddd;background:#fafafa}}}}
</style>
</head>
<body>
<div class="wrap">
  <!-- Hero -->
  <div class="hero">
    <div class="hero-badge"><span class="dot"></span>{status}</div>
    <h1>{html_mod.escape(label)} — Convergence Memo</h1>
    <div class="subtitle">Deal Radar Intelligence &middot; Convergence Score {score:.0f}/100 &middot; {len(signals)} Active Signals</div>
    <div class="date">Generated {date_str} &middot; {', '.join(states)}</div>
  </div>

  <!-- KPIs -->
  <div class="kpi-row">
    <div class="kpi"><div class="lbl">Convergence</div><div class="val hot">{score:.0f}</div></div>
    <div class="kpi"><div class="lbl">Active Signals</div><div class="val">{len(signals)}/5</div></div>
    <div class="kpi"><div class="lbl">Tax Filers</div><div class="val">{inc_ret:,.0f}</div></div>
    <div class="kpi"><div class="lbl">Avg AGI</div><div class="val">${inc_agi:,.0f}</div></div>
  </div>

  <!-- Charts: Radar + Signal Bars -->
  <div class="charts-grid">
    <div class="chart-card">
      <h3>Signal Radar</h3>
      <svg id="radar" width="280" height="280"></svg>
    </div>
    <div class="chart-card">
      <h3>Signal Strength</h3>
      <div class="signal-bars">
        <div class="sig-row"><div class="sig-label">EPA</div><div class="sig-track"><div class="sig-fill" style="width:{epa}%;background:#e24b4a"></div></div><div class="sig-val">{epa:.0f}</div></div>
        <div class="sig-row"><div class="sig-label">Migration</div><div class="sig-track"><div class="sig-fill" style="width:{irs}%;background:#7f77dd"></div></div><div class="sig-val">{irs:.0f}</div></div>
        <div class="sig-row"><div class="sig-label">Trade</div><div class="sig-track"><div class="sig-fill" style="width:{trade}%;background:#1d9e75"></div></div><div class="sig-val">{trade:.0f}</div></div>
        <div class="sig-row"><div class="sig-label">Water</div><div class="sig-track"><div class="sig-fill" style="width:{water}%;background:#ba7517"></div></div><div class="sig-val">{water:.0f}</div></div>
        <div class="sig-row"><div class="sig-label">Macro</div><div class="sig-track"><div class="sig-fill" style="width:{macro}%;background:#378add"></div></div><div class="sig-val">{macro:.0f}</div></div>
      </div>
    </div>
  </div>

  <!-- Data cards -->
  <div class="data-grid">
    <div class="data-card"><div class="dc-icon">🏭</div><div class="dc-val">{epa_fac:,}</div><div class="dc-lbl">EPA Facilities</div></div>
    <div class="data-card"><div class="dc-icon">📊</div><div class="dc-val">{epa_viol:,}</div><div class="dc-lbl">Violations</div></div>
    <div class="data-card"><div class="dc-icon">🔄</div><div class="dc-val">{mig_net:+,}</div><div class="dc-lbl">Net Migration</div></div>
    <div class="data-card"><div class="dc-icon">🚢</div><div class="dc-val">{trade_ctry}</div><div class="dc-lbl">Trade Partners</div></div>
    <div class="data-card"><div class="dc-icon">💧</div><div class="dc-val">{water_sys:,}</div><div class="dc-lbl">Water Systems</div></div>
  </div>

  <!-- Memo sections -->
  {sections_html}

  <!-- Disclaimer -->
  <div class="disclaimer">
    This memo was generated by AI using publicly available data from EPA ECHO, IRS SOI, US Census Trade,
    EPA SDWIS, and IRS income statistics. All signals derived from government data sources. This is not investment advice.
    Generated by Deal Radar &middot; Nexdata Intelligence Platform.
  </div>
</div>

<script>
// D3 Radar Chart
(function() {{
  const data = [
    {{axis:"EPA",value:{epa/100}}},
    {{axis:"Migration",value:{irs/100}}},
    {{axis:"Trade",value:{trade/100}}},
    {{axis:"Water",value:{water/100}}},
    {{axis:"Macro",value:{macro/100}}}
  ];
  const W=280, H=280, cx=W/2, cy=H/2, R=100;
  const colors = ["#e24b4a","#7f77dd","#1d9e75","#ba7517","#378add"];
  const svg = d3.select("#radar");
  const n = data.length;
  const angleSlice = Math.PI*2/n;

  // Grid circles
  [.2,.4,.6,.8,1].forEach(function(d){{
    svg.append("circle").attr("cx",cx).attr("cy",cy).attr("r",R*d)
      .attr("fill","none").attr("stroke","rgba(255,255,255,.06)").attr("stroke-width",0.5);
  }});

  // Axis lines + labels
  data.forEach(function(d,i){{
    const a = angleSlice*i - Math.PI/2;
    const x2 = cx + R*Math.cos(a);
    const y2 = cy + R*Math.sin(a);
    svg.append("line").attr("x1",cx).attr("y1",cy).attr("x2",x2).attr("y2",y2)
      .attr("stroke","rgba(255,255,255,.06)").attr("stroke-width",0.5);
    const lx = cx + (R+18)*Math.cos(a);
    const ly = cy + (R+18)*Math.sin(a);
    svg.append("text").attr("x",lx).attr("y",ly+4)
      .attr("text-anchor","middle").attr("font-size",10).attr("fill","#666")
      .attr("font-family","Inter,sans-serif").text(d.axis);
  }});

  // Data polygon
  const pts = data.map(function(d,i){{
    const a = angleSlice*i - Math.PI/2;
    return [cx + R*d.value*Math.cos(a), cy + R*d.value*Math.sin(a)];
  }});
  const line = pts.map(function(p){{return p.join(",")}}).join(" ");

  svg.append("polygon").attr("points",line)
    .attr("fill","rgba(127,119,221,.12)").attr("stroke","#7f77dd").attr("stroke-width",1.5);

  // Data dots
  pts.forEach(function(p,i){{
    svg.append("circle").attr("cx",p[0]).attr("cy",p[1]).attr("r",4)
      .attr("fill",colors[i]).attr("stroke","#0c0c10").attr("stroke-width",2);
  }});

  // Center score
  svg.append("text").attr("x",cx).attr("y",cy+2).attr("text-anchor","middle")
    .attr("font-size",28).attr("font-weight",800).attr("fill","#e8e5dd")
    .attr("font-family","Inter,sans-serif").text("{score:.0f}");
  svg.append("text").attr("x",cx).attr("y",cy+16).attr("text-anchor","middle")
    .attr("font-size",9).attr("font-weight",500).attr("fill","#555")
    .attr("font-family","Inter,sans-serif").attr("text-transform","uppercase")
    .attr("letter-spacing","0.1em").text("CONVERGENCE");
}})();
</script>
</body>
</html>'''
