"""
Portfolio Macro Stress Scorer — Chain 6 of PLAN_052.

Applies Chain 1 macro data (FRED, BLS, BEA, EIA) to individual PE portfolio
holdings based on their sector classification and leverage profile.
Produces per-holding stress scores (0-100, higher = more stressed).

3 stress components:
  Rate Stress (40%) — leverage × rate sensitivity
  Margin Stress (35%) — input costs vs. margin buffer
  Sector Headwind (25%) — macro environment for the sector (inverted Chain 1 score)
"""
from __future__ import annotations
import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional
from sqlalchemy.orm import Session
from sqlalchemy import text

from app.services.deal_environment_scorer import (
    DealEnvironmentScorer,
    SECTOR_CONFIGS,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class StressComponent:
    component: str
    stress: int          # 0-100 (higher = more stressed)
    weight: float
    reading: str
    driver: str          # which macro input drives this


@dataclass
class HoldingStressScore:
    company_id: int
    company_name: str
    industry: str
    sector_slug: str
    stress_score: int    # 0-100 composite (higher = worse)
    stress_grade: str    # A (low stress) to D (critical stress)
    components: List[StressComponent] = field(default_factory=list)
    financials: Dict = field(default_factory=dict)
    macro_context: Dict = field(default_factory=dict)


@dataclass
class PortfolioStressReport:
    firm_id: int
    firm_name: str
    portfolio_stress: int          # weighted avg across holdings
    holdings_scored: int
    holdings_critical: int         # stress >= 75
    holdings_elevated: int         # stress 50-74
    holdings_moderate: int         # stress 25-49
    holdings_low: int              # stress < 25
    holdings: List[HoldingStressScore] = field(default_factory=list)
    macro_summary: Dict = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Industry → sector mapping (text-based, since NAICS mostly null)
# ---------------------------------------------------------------------------

INDUSTRY_KEYWORDS = {
    "healthcare": ["biotech", "healthcare", "health", "medical", "pharma",
                   "therapeutics", "diagnostics", "clinical", "hospital",
                   "aesthetics", "dental", "veterinary"],
    "technology": ["software", "saas", "technology", "tech", "data", "cloud",
                   "cyber", "ai", "analytics", "platform", "digital", "internet"],
    "energy": ["oil", "gas", "energy", "petroleum", "midstream", "upstream",
               "e&p", "pipeline", "refin", "solar", "wind", "power", "utility"],
    "industrials": ["industrial", "manufacturing", "aerospace", "defense",
                    "chemical", "material", "metal", "machinery", "equipment"],
    "consumer": ["consumer", "retail", "food", "beverage", "restaurant",
                 "hospitality", "apparel", "leisure", "entertainment"],
    "financial": ["financial", "credit", "insurance", "banking", "fintech",
                  "payment", "lending", "capital market", "asset management"],
    "real_estate": ["real estate", "reit", "property", "housing", "mortgage"],
    "logistics": ["logistics", "transport", "freight", "shipping", "supply chain",
                  "warehouse", "distribution"],
    "auto_service": ["auto", "automotive", "vehicle", "tire", "collision"],
}


def _map_industry_to_sector(industry: str, naics_code: Optional[str] = None) -> str:
    """Map company industry text to one of 9 SECTOR_CONFIGS slugs."""
    if not industry:
        return "industrials"  # safe default

    lower = industry.lower()

    # Try keyword matching — longer keywords first to avoid substring false positives
    # (e.g., "ai" matching inside "retail")
    for sector_slug, keywords in INDUSTRY_KEYWORDS.items():
        # Sort keywords longest-first so "biotech" matches before "tech"
        for kw in sorted(keywords, key=len, reverse=True):
            # Word-boundary check: keyword must be a whole word or prefix/suffix
            idx = lower.find(kw)
            if idx == -1:
                continue
            # Check word boundary: keyword must start at a word boundary
            # (prefix matching OK: "biotech" matches "biotechnology")
            # but reject mid-word matches ("ai" in "retail")
            before_ok = (idx == 0 or not lower[idx - 1].isalpha())
            if before_ok:
                return sector_slug

    return "industrials"  # fallback


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

STRESS_GRADES = [(75, "D"), (50, "C"), (25, "B"), (0, "A")]


def _safe_query(db: Session, sql: str, params: dict):
    try:
        result = db.execute(text(sql), params)
        return result.fetchall()
    except Exception as exc:
        try:
            db.rollback()
        except Exception:
            pass
        logger.debug("Stress query failed: %s", exc)
        return []


# ---------------------------------------------------------------------------
# Core scorer
# ---------------------------------------------------------------------------

class PortfolioStressScorer:

    def __init__(self, db: Session):
        self.db = db
        self._deal_scorer = DealEnvironmentScorer(db)
        self._sector_scores: Dict[str, int] = {}

    def _ensure_sector_scores(self):
        """Load all sector deal environment scores once."""
        if self._sector_scores:
            return
        results = self._deal_scorer.score_all_sectors()
        for s in results:
            self._sector_scores[s.sector] = s.score
            # Also cache macro_inputs from any result
            if not hasattr(self, '_macro') and s.macro_inputs:
                self._macro = s.macro_inputs

    def score_portfolio(
        self,
        firm_id: int,
        macro_overrides: Optional[Dict] = None,
    ) -> PortfolioStressReport:
        """Score all active holdings for a PE firm.

        Args:
            firm_id: PE firm ID.
            macro_overrides: Optional dict of macro values to use instead of
                live FRED data. Keys: fed_funds_rate, cpi_yoy_pct,
                energy_cost_yoy_pct, oil_price, consumer_sentiment.
                Enables scenario-based stress testing via synthetic macro data.
        """
        self._ensure_sector_scores()

        # Apply scenario overrides if provided
        if macro_overrides:
            if not hasattr(self, '_macro'):
                self._macro = {}
            self._macro.update(macro_overrides)

        # Get firm name
        firm_rows = _safe_query(self.db, "SELECT name FROM pe_firms WHERE id = :fid", {"fid": firm_id})
        firm_name = firm_rows[0][0] if firm_rows else f"Firm #{firm_id}"

        # Get active portfolio companies via fund investments
        holdings = _safe_query(self.db, """
            SELECT DISTINCT pc.id, pc.name, pc.industry, pc.naics_code, pc.headquarters_state
            FROM pe_portfolio_companies pc
            JOIN pe_fund_investments fi ON fi.company_id = pc.id
            JOIN pe_funds f ON f.id = fi.fund_id
            WHERE f.firm_id = :fid AND fi.status = 'Active'
            ORDER BY pc.name
        """, {"fid": firm_id})

        # If no active investments found, try all companies linked to firm
        if not holdings:
            holdings = _safe_query(self.db, """
                SELECT DISTINCT pc.id, pc.name, pc.industry, pc.naics_code, pc.headquarters_state
                FROM pe_portfolio_companies pc
                JOIN pe_fund_investments fi ON fi.company_id = pc.id
                JOIN pe_funds f ON f.id = fi.fund_id
                WHERE f.firm_id = :fid
                ORDER BY pc.name
                LIMIT 50
            """, {"fid": firm_id})

        scored = []
        for h in holdings:
            score = self._score_single(
                company_id=h[0], company_name=h[1],
                industry=h[2], naics_code=h[3], state=h[4],
            )
            scored.append(score)

        # Sort by stress (highest first)
        scored.sort(key=lambda s: s.stress_score, reverse=True)

        # Portfolio summary
        n = len(scored)
        avg_stress = int(sum(s.stress_score for s in scored) / n) if n > 0 else 0
        critical = sum(1 for s in scored if s.stress_score >= 75)
        elevated = sum(1 for s in scored if 50 <= s.stress_score < 75)
        moderate = sum(1 for s in scored if 25 <= s.stress_score < 50)
        low = sum(1 for s in scored if s.stress_score < 25)

        macro = getattr(self, '_macro', {})

        return PortfolioStressReport(
            firm_id=firm_id, firm_name=firm_name,
            portfolio_stress=avg_stress,
            holdings_scored=n,
            holdings_critical=critical,
            holdings_elevated=elevated,
            holdings_moderate=moderate,
            holdings_low=low,
            holdings=scored,
            macro_summary={
                "fed_funds_rate": macro.get("fed_funds_rate"),
                "cpi_yoy_pct": macro.get("cpi_yoy_pct"),
                "energy_cost_yoy_pct": macro.get("energy_cost_yoy_pct"),
                "oil_price": macro.get("oil_price"),
                "consumer_sentiment": macro.get("consumer_sentiment"),
            },
        )

    def score_holding(self, company_id: int) -> HoldingStressScore:
        """Score a single holding with full detail."""
        self._ensure_sector_scores()

        rows = _safe_query(self.db, """
            SELECT id, name, industry, naics_code, headquarters_state
            FROM pe_portfolio_companies WHERE id = :cid
        """, {"cid": company_id})
        if not rows:
            return HoldingStressScore(
                company_id=company_id, company_name=f"Company #{company_id}",
                industry="Unknown", sector_slug="industrials",
                stress_score=50, stress_grade="C",
            )
        r = rows[0]
        return self._score_single(r[0], r[1], r[2], r[3], r[4])

    def _score_single(
        self, company_id: int, company_name: str,
        industry: Optional[str], naics_code: Optional[str],
        state: Optional[str],
    ) -> HoldingStressScore:
        """Score a single company against current macro conditions."""
        sector_slug = _map_industry_to_sector(industry or "", naics_code)
        config = SECTOR_CONFIGS.get(sector_slug, SECTOR_CONFIGS["industrials"])
        macro = getattr(self, '_macro', {})

        # Get latest financials
        fin_rows = _safe_query(self.db, """
            SELECT revenue_usd, ebitda_margin_pct, debt_to_ebitda,
                   interest_coverage, free_cash_flow_usd
            FROM pe_company_financials
            WHERE company_id = :cid AND revenue_usd IS NOT NULL
            ORDER BY period_end_date DESC NULLS LAST, fiscal_year DESC
            LIMIT 1
        """, {"cid": company_id})

        revenue = None
        ebitda_margin = None
        debt_ebitda = None
        interest_cov = None
        fcf = None
        has_financials = False

        if fin_rows:
            has_financials = True
            revenue = float(fin_rows[0][0]) if fin_rows[0][0] else None
            ebitda_margin = float(fin_rows[0][1]) if fin_rows[0][1] else None
            debt_ebitda = float(fin_rows[0][2]) if fin_rows[0][2] else None
            interest_cov = float(fin_rows[0][3]) if fin_rows[0][3] else None
            fcf = float(fin_rows[0][4]) if fin_rows[0][4] else None

        components = []
        ffr = macro.get("fed_funds_rate") or 4.0
        cpi_yoy = macro.get("cpi_yoy_pct") or 2.5
        energy_yoy = macro.get("energy_cost_yoy_pct") or 0
        deal_score = self._sector_scores.get(sector_slug, 70)

        # --- Component 1: Rate Stress (40%) ---
        rate_mult = {"very_high": 1.5, "high": 1.25, "medium": 1.0, "low": 0.5}.get(
            config["sensitivity"].get("rates", "medium"), 1.0
        )

        if debt_ebitda is not None and debt_ebitda > 0:
            # High leverage + high rates = stress
            rate_stress = min(100, int(debt_ebitda * (ffr / 3.0) * rate_mult * 3))
            reading = f"{debt_ebitda:.1f}x leverage at {ffr:.1f}% FFR"
            if interest_cov is not None and interest_cov < 1.5:
                rate_stress = min(100, rate_stress + 15)
                reading += f", coverage {interest_cov:.1f}x (thin)"
        else:
            # No leverage data — use sector default
            rate_stress = int(30 * rate_mult)  # moderate default
            reading = f"No leverage data; sector rate sensitivity: {config['sensitivity'].get('rates', 'medium')}"

        components.append(StressComponent(
            "Rate stress", rate_stress, 0.40, reading, f"FFR {ffr:.1f}%"
        ))

        # --- Component 2: Margin Stress (35%) ---
        energy_mult = {"very_high": 1.5, "high": 1.25, "medium": 1.0, "low": 0.5}.get(
            config["sensitivity"].get("energy", "low"), 0.5
        )

        if ebitda_margin is not None and ebitda_margin > 0:
            # Thin margins + high CPI/energy = stress
            cost_pressure = abs(cpi_yoy) * 3 + abs(max(0, energy_yoy)) * energy_mult
            margin_stress = min(100, int(cost_pressure * (15 / max(ebitda_margin, 3))))
            reading = f"{ebitda_margin:.1f}% EBITDA margin vs CPI {cpi_yoy:.1f}%"
            if energy_yoy > 10:
                reading += f", energy +{energy_yoy:.0f}%"
        else:
            margin_stress = int(25 * energy_mult + 10)  # moderate default
            reading = f"No margin data; CPI {cpi_yoy:.1f}%, energy {energy_yoy:+.0f}%"

        components.append(StressComponent(
            "Margin stress", margin_stress, 0.35, reading,
            f"CPI {cpi_yoy:.1f}%, energy {energy_yoy:+.0f}%"
        ))

        # --- Component 3: Sector Headwind (25%) ---
        sector_stress = max(0, 100 - deal_score)
        reading = f"{config['label']} deal score {deal_score}/100"
        if deal_score >= 80:
            reading += " — favorable macro tailwind"
        elif deal_score >= 60:
            reading += " — neutral macro environment"
        else:
            reading += " — macro headwinds present"

        components.append(StressComponent(
            "Sector headwind", sector_stress, 0.25, reading,
            f"Deal Environment Score: {deal_score}"
        ))

        # --- Composite ---
        total_w = sum(c.weight for c in components)
        composite = sum(c.stress * c.weight for c in components) / total_w if total_w > 0 else 50
        stress_score = max(0, min(100, int(round(composite))))
        stress_grade = next((g for threshold, g in STRESS_GRADES if stress_score >= threshold), "A")

        return HoldingStressScore(
            company_id=company_id,
            company_name=company_name or f"Company #{company_id}",
            industry=industry or "Unknown",
            sector_slug=sector_slug,
            stress_score=stress_score,
            stress_grade=stress_grade,
            components=components,
            financials={
                "revenue_usd": revenue,
                "ebitda_margin_pct": ebitda_margin,
                "debt_to_ebitda": debt_ebitda,
                "interest_coverage": interest_cov,
                "free_cash_flow_usd": fcf,
                "has_financials": has_financials,
            },
            macro_context={
                "sector": sector_slug,
                "sector_label": config["label"],
                "deal_environment_score": deal_score,
                "fed_funds_rate": ffr,
                "cpi_yoy_pct": cpi_yoy,
                "energy_cost_yoy_pct": energy_yoy,
            },
        )
