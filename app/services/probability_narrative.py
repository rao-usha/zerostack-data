"""
Deal Probability Engine — Narrative Generator (SPEC 047, PLAN_059 Phase 3).

LLM-powered explainers that translate the numerical signal chain into
deal-sourcer-friendly prose. Three output shapes:

- `generate_narrative(company_id)`  — 3-5 sentence company explainer
- `generate_memo(company_id)`       — 6-section deal memo (JSON + HTML)
- `generate_sector_briefing(sector)` — sector-level top movers summary

All generation uses `LLMClient` (Anthropic). When the LLM is unavailable
or errors out, each method falls back to a deterministic template built
from the persisted signal chain. The engine never crashes the caller.

Memo sections:
  1. executive_summary
  2. signal_analysis
  3. comparable_transactions
  4. risk_factors
  5. recommended_action
  6. timing_thesis
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from sqlalchemy import func
from sqlalchemy.orm import Session

from app.core.probability_models import (
    TxnProbCompany,
    TxnProbScore,
)
from app.services.probability_convergence import ConvergenceDetector

logger = logging.getLogger(__name__)


MEMO_SECTIONS = [
    "executive_summary",
    "signal_analysis",
    "comparable_transactions",
    "risk_factors",
    "recommended_action",
    "timing_thesis",
]


NARRATIVE_SYSTEM_PROMPT = """You are a senior PE associate writing a 3-5 sentence analyst note explaining WHY a specific company scored its transaction probability.

Focus on the 3-5 strongest contributing signals (highest contribution values). Explicitly name them and the value they add.

Style:
- Crisp, professional, no hype
- No bullets, no headings, just prose
- Reference the signal_type names verbatim (e.g., "exec_transition", "founder_risk")
- If the probability is low, explain what's MISSING, not just what's present
- End with a clear "next step" recommendation

Return ONLY the prose — no markdown, no preamble, no quotation marks."""


MEMO_SYSTEM_PROMPT = """You are a senior PE associate writing an internal deal memo.

Input: JSON with a company's transaction probability data (name, sector, probability, signal chain).
Output: JSON with EXACTLY these keys and concise prose for each (2-4 sentences):
- executive_summary: The headline — why this company, why now
- signal_analysis: Which signals drove the score, in plain English
- comparable_transactions: Reference any comparable deals in the same sector (if data available, else acknowledge the gap)
- risk_factors: What could make this a false positive
- recommended_action: Specific next step (e.g., "schedule intro call with CEO", "monitor for next 30 days")
- timing_thesis: Why now vs 6 months from now

Return ONLY valid JSON, no markdown, no backticks."""


SECTOR_BRIEFING_SYSTEM_PROMPT = """You are a PE market analyst writing a 4-6 sentence sector briefing.

Given a list of top companies in a sector ranked by transaction probability, write a briefing that:
- Identifies the sector's overall signal temperature (hot / warm / cold)
- Highlights the top 2-3 named movers and their dominant signals
- Calls out any pattern (e.g., multiple companies with high founder_risk)
- Provides one actionable recommendation

No bullets, no headings, just prose. Return ONLY the prose."""


# ---------------------------------------------------------------------------
# Generator
# ---------------------------------------------------------------------------


class ProbabilityNarrativeGenerator:
    """Async LLM-backed narrative / memo / briefing generator."""

    def __init__(self, db: Session):
        self.db = db

    # -------------------------------------------------------------------
    # Public API
    # -------------------------------------------------------------------

    async def generate_narrative(self, company_id: int) -> Dict[str, Any]:
        """Return {narrative: str, source: 'llm' | 'fallback'}."""
        context = self._gather_company_context(company_id)
        if context is None:
            return {
                "narrative": "",
                "source": "error",
                "error": f"Company {company_id} has no latest score",
            }

        prompt = json.dumps(context, indent=2)
        llm_result = await self._call_llm(
            system_prompt=NARRATIVE_SYSTEM_PROMPT,
            user_prompt=prompt,
            max_tokens=400,
            json_output=False,
        )
        if llm_result is not None:
            return {"narrative": llm_result.strip(), "source": "llm", "context": context}

        # Fallback
        return {
            "narrative": self._fallback_narrative(context),
            "source": "fallback",
            "context": context,
        }

    async def generate_memo(self, company_id: int) -> Dict[str, Any]:
        """Return {sections: {...}, html: str, source: 'llm' | 'fallback'}."""
        context = self._gather_company_context(company_id)
        if context is None:
            return {
                "sections": {},
                "html": "",
                "source": "error",
                "error": f"Company {company_id} has no latest score",
            }
        # Enrich with comparable sector deals for the memo
        context["comparable_sector_deals"] = self._gather_sector_deals(
            context.get("sector")
        )

        prompt = json.dumps(context, indent=2)
        llm_result = await self._call_llm(
            system_prompt=MEMO_SYSTEM_PROMPT,
            user_prompt=prompt,
            max_tokens=1500,
            json_output=True,
        )

        if isinstance(llm_result, dict):
            sections = {k: str(llm_result.get(k, "")) for k in MEMO_SECTIONS}
            source = "llm"
        else:
            sections = self._fallback_memo_sections(context)
            source = "fallback"

        html = self._render_memo_html(context, sections)
        return {
            "company_id": company_id,
            "sections": sections,
            "html": html,
            "source": source,
        }

    async def generate_sector_briefing(
        self, sector: str, top_n: int = 5
    ) -> Dict[str, Any]:
        """Return {briefing: str, top_companies: [...], source}."""
        top = self._top_sector_companies(sector, top_n)
        if not top:
            return {
                "briefing": f"No scored companies found in sector '{sector}'.",
                "top_companies": [],
                "source": "error",
            }
        prompt = json.dumps({"sector": sector, "top_companies": top}, indent=2)
        llm_result = await self._call_llm(
            system_prompt=SECTOR_BRIEFING_SYSTEM_PROMPT,
            user_prompt=prompt,
            max_tokens=500,
            json_output=False,
        )
        if llm_result:
            return {
                "briefing": llm_result.strip(),
                "top_companies": top,
                "source": "llm",
            }
        return {
            "briefing": self._fallback_sector_briefing(sector, top),
            "top_companies": top,
            "source": "fallback",
        }

    # -------------------------------------------------------------------
    # Context gathering
    # -------------------------------------------------------------------

    def _gather_company_context(self, company_id: int) -> Optional[Dict]:
        company = self.db.query(TxnProbCompany).filter_by(id=company_id).first()
        if not company:
            return None
        score = (
            self.db.query(TxnProbScore)
            .filter_by(company_id=company_id)
            .order_by(TxnProbScore.scored_at.desc())
            .first()
        )
        if not score:
            return None

        convergences = ConvergenceDetector(self.db).detect_company(company_id)

        return {
            "company_id": company.id,
            "company_name": company.company_name,
            "sector": company.sector,
            "hq_state": company.hq_state,
            "ownership_status": company.ownership_status,
            "founded_year": company.founded_year,
            "employee_count_est": company.employee_count_est,
            "probability": round(score.probability, 4),
            "raw_composite_score": round(score.raw_composite_score, 2),
            "grade": score.grade,
            "active_signal_count": score.active_signal_count,
            "top_signals": score.top_signals,
            "signal_chain": score.signal_chain,
            "convergences": convergences,
        }

    def _gather_sector_deals(self, sector: Optional[str]) -> List[Dict]:
        """Best-effort fetch of recent deals in the same sector."""
        if not sector:
            return []
        from sqlalchemy import text as sa_text

        try:
            rows = (
                self.db.execute(
                    sa_text(
                        """
                        SELECT d.announced_date, d.deal_value_usd, p.name AS target, d.deal_type
                        FROM pe_deals d
                        JOIN pe_portfolio_companies p ON d.company_id = p.id
                        WHERE p.sector = :sector
                          AND d.announced_date >= NOW() - INTERVAL '12 months'
                        ORDER BY d.announced_date DESC
                        LIMIT 5
                        """
                    ),
                    {"sector": sector},
                )
                .mappings()
                .all()
            )
            return [
                {
                    "target": r.get("target"),
                    "deal_type": r.get("deal_type"),
                    "deal_value_usd": (
                        float(r["deal_value_usd"])
                        if r.get("deal_value_usd") is not None
                        else None
                    ),
                    "announced_date": (
                        r["announced_date"].isoformat()
                        if r.get("announced_date")
                        else None
                    ),
                }
                for r in rows
            ]
        except Exception as exc:
            self.db.rollback()
            logger.debug("sector deals fetch failed: %s", exc)
            return []

    def _top_sector_companies(self, sector: str, n: int) -> List[Dict]:
        latest_subq = (
            self.db.query(
                TxnProbScore.company_id,
                func.max(TxnProbScore.scored_at).label("latest_at"),
            )
            .group_by(TxnProbScore.company_id)
            .subquery()
        )
        rows = (
            self.db.query(TxnProbScore, TxnProbCompany)
            .join(
                latest_subq,
                (TxnProbScore.company_id == latest_subq.c.company_id)
                & (TxnProbScore.scored_at == latest_subq.c.latest_at),
            )
            .join(TxnProbCompany, TxnProbCompany.id == TxnProbScore.company_id)
            .filter(TxnProbCompany.sector == sector)
            .order_by(TxnProbScore.probability.desc())
            .limit(n)
            .all()
        )
        return [
            {
                "company_name": c.company_name,
                "probability": s.probability,
                "grade": s.grade,
                "top_signals": (s.top_signals or [])[:3],
            }
            for s, c in rows
        ]

    # -------------------------------------------------------------------
    # LLM
    # -------------------------------------------------------------------

    async def _call_llm(
        self,
        system_prompt: str,
        user_prompt: str,
        max_tokens: int,
        json_output: bool,
    ):
        """Return str (or parsed dict if json_output) on success, None otherwise."""
        try:
            from app.agentic.llm_client import LLMClient

            llm = LLMClient(
                provider="anthropic",
                model="claude-3-5-haiku-20241022",
                max_tokens=max_tokens,
                temperature=0.3,
            )
            response = await llm.complete(
                prompt=user_prompt, system_prompt=system_prompt
            )
            if json_output:
                parsed = response.parse_json()
                if parsed is not None:
                    return parsed
                try:
                    return json.loads(response.content.strip())
                except Exception:
                    return None
            return response.content
        except Exception as exc:
            logger.debug("LLM call failed (%s): %s", type(exc).__name__, exc)
            return None

    # -------------------------------------------------------------------
    # Fallbacks (deterministic templates)
    # -------------------------------------------------------------------

    @staticmethod
    def _fallback_narrative(ctx: Dict) -> str:
        top = (ctx.get("top_signals") or [])[:3]
        driver_text = ", ".join(
            f"{s['signal_type']} ({s['score']:.0f})" for s in top if "signal_type" in s
        )
        if not driver_text:
            driver_text = "insufficient signal data"
        cvg_text = ""
        if ctx.get("convergences"):
            labels = ", ".join(c["label"] for c in ctx["convergences"])
            cvg_text = f" Named convergence pattern(s) matched: {labels}."
        return (
            f"{ctx['company_name']} scored P = {ctx['probability']:.1%} (grade {ctx['grade']}), "
            f"driven primarily by {driver_text}.{cvg_text} "
            f"With {ctx['active_signal_count']} of 12 signals above the 60 threshold, "
            f"this company warrants a {_recommendation_from_prob(ctx['probability'])}."
        )

    @staticmethod
    def _fallback_memo_sections(ctx: Dict) -> Dict[str, str]:
        top = (ctx.get("top_signals") or [])[:5]
        drivers = ", ".join(s.get("signal_type", "?") for s in top)
        prob_pct = ctx["probability"]

        deals = ctx.get("comparable_sector_deals") or []
        if deals:
            deal_text = (
                f"Recent comparable activity in {ctx.get('sector')}: "
                + "; ".join(
                    f"{d.get('target')} ({d.get('deal_type') or 'deal'})"
                    for d in deals[:3]
                )
                + "."
            )
        else:
            deal_text = (
                f"No comparable deals found in {ctx.get('sector') or 'this sector'} "
                "within the tracked 12-month window."
            )

        return {
            "executive_summary": (
                f"{ctx['company_name']} ({ctx.get('sector') or 'unspecified sector'}) scored "
                f"P = {prob_pct:.1%} (grade {ctx['grade']}). "
                f"Primary drivers: {drivers or 'none above baseline'}."
            ),
            "signal_analysis": (
                f"Active signals: {ctx['active_signal_count']} of 12 above the 60 threshold. "
                f"Top contributors are {drivers or 'n/a'}."
            ),
            "comparable_transactions": deal_text,
            "risk_factors": (
                "Signal confidence varies; some scores reflect graceful-defaults where "
                "source data was missing. Verify via direct diligence before committing time."
            ),
            "recommended_action": _recommendation_from_prob(prob_pct),
            "timing_thesis": (
                "Probability reflects a 6-12 month window; re-score after major signal "
                "events (executive change, Form D filing, sector momentum shift)."
            ),
        }

    @staticmethod
    def _fallback_sector_briefing(sector: str, top: List[Dict]) -> str:
        if not top:
            return f"No scored companies in {sector}."
        avg_prob = sum(t["probability"] for t in top) / len(top)
        temp = (
            "hot" if avg_prob >= 0.6 else "warm" if avg_prob >= 0.4 else "cold"
        )
        leaders = ", ".join(
            f"{t['company_name']} ({t['probability']:.1%})" for t in top[:3]
        )
        return (
            f"{sector} sector temperature: {temp} (avg P = {avg_prob:.1%} among top {len(top)}). "
            f"Leaders: {leaders}. Review top_signals on each for convergence confirmation."
        )

    # -------------------------------------------------------------------
    # HTML rendering (self-contained, no external assets)
    # -------------------------------------------------------------------

    @staticmethod
    def _render_memo_html(ctx: Dict, sections: Dict[str, str]) -> str:
        """Render a minimal, self-contained HTML memo."""
        top_signals = ctx.get("top_signals") or []
        rows = "".join(
            f"<tr><td>{s.get('signal_type', '')}</td>"
            f"<td>{s.get('score', 0):.1f}</td>"
            f"<td>{s.get('weight', 0):.3f}</td>"
            f"<td>{s.get('contribution', 0):.2f}</td></tr>"
            for s in top_signals
        )

        cvg_html = ""
        if ctx.get("convergences"):
            items = "".join(
                f"<li><b>{c['label']}</b> ({c['severity']}): {c['description']}</li>"
                for c in ctx["convergences"]
            )
            cvg_html = f"<h3>Convergence Patterns</h3><ul>{items}</ul>"

        sections_html = "".join(
            f"<h3>{k.replace('_', ' ').title()}</h3><p>{v}</p>"
            for k, v in sections.items()
        )

        return f"""<!doctype html>
<html><head><meta charset="utf-8"><title>Deal Memo — {ctx['company_name']}</title>
<style>
body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Helvetica, Arial, sans-serif;
       max-width: 900px; margin: 2rem auto; padding: 0 1rem; color: #1c1e21; line-height: 1.5; }}
h1 {{ border-bottom: 2px solid #0b63ce; padding-bottom: 0.3rem; }}
h3 {{ color: #0b63ce; margin-top: 1.5rem; }}
table {{ width: 100%; border-collapse: collapse; margin: 1rem 0; }}
th, td {{ padding: 0.5rem 0.7rem; border-bottom: 1px solid #e2e4e9; text-align: left; }}
th {{ background: #f5f7fa; }}
.kpi {{ display: inline-block; margin-right: 2rem; }}
.kpi-label {{ color: #606770; font-size: 0.85rem; text-transform: uppercase; letter-spacing: 0.5px; }}
.kpi-value {{ font-size: 1.7rem; font-weight: 600; color: #0b63ce; }}
</style></head>
<body>
<h1>{ctx['company_name']}</h1>
<p><em>{ctx.get('sector') or 'Unspecified sector'} · {ctx.get('hq_state') or 'Unknown state'} · {ctx.get('ownership_status') or 'Private'}</em></p>
<div>
  <div class="kpi"><div class="kpi-label">Probability</div><div class="kpi-value">{ctx['probability']:.1%}</div></div>
  <div class="kpi"><div class="kpi-label">Grade</div><div class="kpi-value">{ctx['grade']}</div></div>
  <div class="kpi"><div class="kpi-label">Active Signals</div><div class="kpi-value">{ctx['active_signal_count']} / 12</div></div>
  <div class="kpi"><div class="kpi-label">Raw Composite</div><div class="kpi-value">{ctx['raw_composite_score']:.1f}</div></div>
</div>
{cvg_html}
<h3>Top Signals</h3>
<table><thead><tr><th>Signal</th><th>Score</th><th>Weight</th><th>Contribution</th></tr></thead><tbody>{rows}</tbody></table>
{sections_html}
<hr><p style="color:#606770;font-size:0.85rem">Generated by the Deal Probability Engine. Fallback renderer used when LLM unavailable.</p>
</body></html>"""


def _recommendation_from_prob(p: float) -> str:
    if p >= 0.7:
        return "priority intro and fast-track diligence"
    if p >= 0.5:
        return "schedule an introductory call and monitor closely"
    if p >= 0.3:
        return "add to watchlist and re-score monthly"
    return "low-priority monitoring"
