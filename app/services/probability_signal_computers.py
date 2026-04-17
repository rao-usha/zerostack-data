"""
Deal Probability Engine — Signal Computers (SPEC 046, PLAN_059 Phase 2).

6 new signal computers for signals not already covered by existing scorers.
Each computer has `.compute(company: TxnProbCompany) -> SignalResult` returning
a score (0-100), confidence (0-1), and details dict for explainability.

Graceful degradation: if source data is missing or empty, returns a
neutral score (50) with low confidence (0.0-0.3) rather than raising.

Computers:
- InsiderActivityComputer     — net buy/sell over trailing 90 days (Form 4)
- HiringVelocityComputer      — senior hiring intensity + corp dev postings
- DealActivitySignalComputer  — Form D raises + corp dev job titles
- FounderRiskComputer         — founder age + co-founder departures + succession
- InnovationVelocityComputer  — patent filing rate (USPTO)
- MacroTailwindComputer       — HQ state convergence + sector momentum
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.core.probability_models import TxnProbCompany

logger = logging.getLogger(__name__)


NEUTRAL_SCORE = 50.0


@dataclass
class SignalResult:
    """Output from a signal computer or existing-scorer wrapper."""

    signal_type: str
    score: float  # 0-100
    confidence: float  # 0-1
    details: Dict = field(default_factory=dict)
    data_sources: List[str] = field(default_factory=list)


def _clamp(v: float, lo: float = 0.0, hi: float = 100.0) -> float:
    return max(lo, min(hi, v))


def _safe_fetchall(db: Session, sql: str, params: Optional[Dict] = None) -> List:
    """Run a query with graceful handling of missing tables."""
    try:
        return db.execute(text(sql), params or {}).mappings().all()
    except Exception as exc:
        db.rollback()
        logger.debug("query failed (%s): %s", exc.__class__.__name__, str(exc)[:120])
        return []


# ---------------------------------------------------------------------------
# Base class
# ---------------------------------------------------------------------------


class BaseSignalComputer:
    """Shared helpers; subclasses must set signal_type and implement compute()."""

    signal_type: str = ""

    def __init__(self, db: Session):
        self.db = db

    def compute(self, company: TxnProbCompany) -> SignalResult:  # pragma: no cover
        raise NotImplementedError

    def _neutral(self, reason: str, sources: Optional[List[str]] = None) -> SignalResult:
        return SignalResult(
            signal_type=self.signal_type,
            score=NEUTRAL_SCORE,
            confidence=0.0,
            details={"reason": reason},
            data_sources=sources or [],
        )


# ---------------------------------------------------------------------------
# 1. InsiderActivityComputer
# ---------------------------------------------------------------------------


class InsiderActivityComputer(BaseSignalComputer):
    """
    Score insider buying/selling activity over trailing 90 days.

    High score = strong net buying (bullish signal preceding transactions).
    Low score = strong net selling (either insider exits OR imminent deal
    sell-side pressure — both can precede transactions).
    Neutral = no activity or balanced.

    Uses insider_transactions table (Form 4 data).
    """

    signal_type = "insider_activity"
    LOOKBACK_DAYS = 90

    def compute(self, company: TxnProbCompany) -> SignalResult:
        rows = _safe_fetchall(
            self.db,
            """
            SELECT
                transaction_type,
                COALESCE(total_value_usd, 0) AS value_usd,
                COALESCE(shares, 0) AS shares
            FROM insider_transactions
            WHERE (company_id = :company_id OR LOWER(company_name) = LOWER(:name))
              AND transaction_date >= NOW() - make_interval(days => :lookback)
            """,
            {
                "company_id": company.canonical_company_id,
                "name": company.company_name,
                "lookback": self.LOOKBACK_DAYS,
            },
        )
        if not rows:
            return self._neutral("no insider transactions in window", ["insider_transactions"])

        buy_value = 0.0
        sell_value = 0.0
        for r in rows:
            ttype = (r.get("transaction_type") or "").lower()
            val = float(r.get("value_usd") or 0)
            if "buy" in ttype or "purchase" in ttype or "p " in ttype or ttype == "p":
                buy_value += val
            elif "sell" in ttype or "sale" in ttype or "s " in ttype or ttype == "s":
                sell_value += val

        total = buy_value + sell_value
        if total == 0:
            return self._neutral(
                "no buy/sell values parsable", ["insider_transactions"]
            )

        # Net buy ratio in [-1, 1]. Strong buying → high score;
        # strong selling is ALSO informative (pre-deal disposal) → moderate-high score.
        # Neutral/balanced → 50.
        net_ratio = (buy_value - sell_value) / total
        # Map |net_ratio| to distance from neutral; sign biases direction.
        # Buying (net>0): 50 + 50*net_ratio
        # Selling (net<0): 50 + 30*|net_ratio|  (less lift than buying — weaker signal)
        if net_ratio >= 0:
            score = 50 + 50 * net_ratio
        else:
            score = 50 + 30 * abs(net_ratio)

        score = _clamp(score)
        confidence = min(1.0, len(rows) / 10.0)
        return SignalResult(
            signal_type=self.signal_type,
            score=score,
            confidence=confidence,
            details={
                "buy_value_usd": buy_value,
                "sell_value_usd": sell_value,
                "net_ratio": round(net_ratio, 3),
                "transaction_count": len(rows),
                "lookback_days": self.LOOKBACK_DAYS,
            },
            data_sources=["insider_transactions"],
        )


# ---------------------------------------------------------------------------
# 2. HiringVelocityComputer
# ---------------------------------------------------------------------------


class HiringVelocityComputer(BaseSignalComputer):
    """
    Score hiring intensity: senior roles + corp dev postings.

    High score = aggressive senior hiring → management scale-up → often
    precedes capital event. Uses job_postings table.
    """

    signal_type = "hiring_velocity"
    LOOKBACK_DAYS = 90
    DEAL_KEYWORDS = (
        "corporate development",
        "corp dev",
        "m&a",
        "mergers",
        "business development director",
        "vp business development",
        "strategic partnerships",
    )

    def compute(self, company: TxnProbCompany) -> SignalResult:
        rows = _safe_fetchall(
            self.db,
            """
            SELECT
                COALESCE(LOWER(seniority_level), '') AS seniority,
                COALESCE(LOWER(title), '') AS title,
                first_seen
            FROM job_postings
            WHERE (company_id = :cid OR LOWER(company) = LOWER(:name))
              AND (status = 'open' OR status IS NULL)
              AND first_seen >= NOW() - make_interval(days => :lookback)
            """,
            {
                "cid": company.canonical_company_id,
                "name": company.company_name,
                "lookback": self.LOOKBACK_DAYS,
            },
        )
        if not rows:
            return self._neutral("no open job postings in window", ["job_postings"])

        senior_count = 0
        deal_title_count = 0
        total = len(rows)

        for r in rows:
            sen = r.get("seniority") or ""
            title = r.get("title") or ""
            if sen in ("c_suite", "c-suite", "vp", "director", "svp"):
                senior_count += 1
            if any(kw in title for kw in self.DEAL_KEYWORDS):
                deal_title_count += 1

        senior_ratio = senior_count / total if total else 0
        # Score = 40% senior ratio weight + 40% deal-title weight + 20% volume lift
        # Benchmarks: 15% senior ratio = high; 3+ deal titles = high
        senior_component = min(100, senior_ratio * 100 * 2.5)  # 15% → 37.5 → scale
        deal_component = min(100, deal_title_count * 25)  # 4+ titles → 100
        volume_component = min(100, total * 2)  # 50 postings → 100

        score = 0.4 * senior_component + 0.4 * deal_component + 0.2 * volume_component
        score = _clamp(score)
        confidence = min(1.0, total / 20.0)

        return SignalResult(
            signal_type=self.signal_type,
            score=score,
            confidence=confidence,
            details={
                "total_postings": total,
                "senior_count": senior_count,
                "senior_ratio": round(senior_ratio, 3),
                "deal_title_count": deal_title_count,
                "lookback_days": self.LOOKBACK_DAYS,
            },
            data_sources=["job_postings"],
        )


# ---------------------------------------------------------------------------
# 3. DealActivitySignalComputer
# ---------------------------------------------------------------------------


class DealActivitySignalComputer(BaseSignalComputer):
    """
    Score direct deal-activity signals.

    Combines:
    - Form D capital raise in last 12 months (transactional intent)
    - Corp dev / M&A job titles posted (deal team buildout)
    - Deal flow in same sector (market heat)
    """

    signal_type = "deal_activity_signals"
    FORM_D_LOOKBACK_DAYS = 365

    def compute(self, company: TxnProbCompany) -> SignalResult:
        form_d_count = 0
        form_d_value = 0.0
        form_d_rows = _safe_fetchall(
            self.db,
            """
            SELECT filing_date, COALESCE(CAST(total_amount_sold AS FLOAT), 0) AS amount
            FROM form_d_filings
            WHERE LOWER(issuer_name) = LOWER(:name)
              AND filing_date >= NOW() - make_interval(days => :lookback)
            """,
            {"name": company.company_name, "lookback": self.FORM_D_LOOKBACK_DAYS},
        )
        if form_d_rows:
            form_d_count = len(form_d_rows)
            form_d_value = sum(float(r.get("amount") or 0) for r in form_d_rows)

        # Sector deal flow (last 90 days of pe_deals)
        sector_deal_count = 0
        if company.sector:
            sector_rows = _safe_fetchall(
                self.db,
                """
                SELECT COUNT(*) AS c
                FROM pe_deals d
                JOIN pe_portfolio_companies p ON d.company_id = p.id
                WHERE p.sector = :sector
                  AND d.announced_date >= NOW() - make_interval(days => 90)
                """,
                {"sector": company.sector},
            )
            if sector_rows:
                sector_deal_count = int(sector_rows[0].get("c") or 0)

        if form_d_count == 0 and sector_deal_count == 0:
            return self._neutral(
                "no form_d filings or sector deals",
                ["form_d_filings", "pe_deals"],
            )

        # Form D: 1 filing = 50, 2+ filings = 80, value >$10M = bonus 10
        if form_d_count >= 2:
            form_d_component = 80
        elif form_d_count == 1:
            form_d_component = 50
        else:
            form_d_component = 30
        if form_d_value >= 10_000_000:
            form_d_component = min(100, form_d_component + 10)

        # Sector component: 5+ deals = 80; 2-4 = 60; 1 = 50; 0 = 40
        if sector_deal_count >= 5:
            sector_component = 80
        elif sector_deal_count >= 2:
            sector_component = 60
        elif sector_deal_count == 1:
            sector_component = 50
        else:
            sector_component = 40

        score = 0.6 * form_d_component + 0.4 * sector_component
        score = _clamp(score)

        # Confidence: higher if we have Form D data specifically
        confidence = min(1.0, 0.3 + 0.4 * min(form_d_count, 3) / 3 + 0.3 * min(sector_deal_count, 10) / 10)

        return SignalResult(
            signal_type=self.signal_type,
            score=score,
            confidence=confidence,
            details={
                "form_d_count": form_d_count,
                "form_d_value_usd": form_d_value,
                "sector_deal_count_90d": sector_deal_count,
                "sector": company.sector,
            },
            data_sources=["form_d_filings", "pe_deals"],
        )


# ---------------------------------------------------------------------------
# 4. FounderRiskComputer
# ---------------------------------------------------------------------------


class FounderRiskComputer(BaseSignalComputer):
    """
    Score founder-related risk that may precipitate a liquidity event.

    High score = high founder risk (aging founder without clear succession,
    co-founder departures). These companies often pursue exits / recaps.
    Low score = stable, non-founder-led, with clear succession.
    """

    signal_type = "founder_risk"
    CURRENT_YEAR = datetime.utcnow().year

    def compute(self, company: TxnProbCompany) -> SignalResult:
        sources = []
        risk_components = []

        # Component 1: company age (older companies → higher founder transition probability)
        if company.founded_year:
            age = self.CURRENT_YEAR - company.founded_year
            if age >= 40:
                age_component = 85
            elif age >= 25:
                age_component = 70
            elif age >= 15:
                age_component = 55
            elif age >= 8:
                age_component = 40
            else:
                age_component = 25
            risk_components.append(age_component)
            sources.append("company.founded_year")

        # Component 2: recent co-founder / CEO departures
        if company.canonical_company_id:
            departure_rows = _safe_fetchall(
                self.db,
                """
                SELECT change_type, old_title, COALESCE(significance_score, 5) AS sig
                FROM leadership_changes
                WHERE company_id = :cid
                  AND change_type IN ('departure', 'retirement')
                  AND effective_date >= NOW() - make_interval(days => 730)
                  AND (is_c_suite = true OR LOWER(old_title) LIKE '%founder%' OR LOWER(old_title) LIKE '%ceo%')
                """,
                {"cid": company.canonical_company_id},
            )
            if departure_rows:
                # Significance-weighted: 1 CEO departure = 80, 2+ = 90
                max_sig = max(int(r.get("sig") or 5) for r in departure_rows)
                dep_component = 50 + min(40, max_sig * 4)
                if len(departure_rows) >= 2:
                    dep_component = min(100, dep_component + 10)
                risk_components.append(dep_component)
                sources.append("leadership_changes")

        if not risk_components:
            return self._neutral(
                "no founded_year and no leadership change data",
                ["company.founded_year", "leadership_changes"],
            )

        score = sum(risk_components) / len(risk_components)
        score = _clamp(score)
        confidence = min(1.0, len(risk_components) / 2.0)

        return SignalResult(
            signal_type=self.signal_type,
            score=score,
            confidence=confidence,
            details={
                "company_age": (
                    self.CURRENT_YEAR - company.founded_year
                    if company.founded_year
                    else None
                ),
                "components_computed": len(risk_components),
            },
            data_sources=sources,
        )


# ---------------------------------------------------------------------------
# 5. InnovationVelocityComputer
# ---------------------------------------------------------------------------


class InnovationVelocityComputer(BaseSignalComputer):
    """
    Score R&D/innovation output via USPTO patent activity.

    High score = strong patent portfolio recently active → valuable IP →
    attractive acquisition target. Low score = no patents (non-IP business
    model OR stagnant innovation).
    """

    signal_type = "innovation_velocity"

    def compute(self, company: TxnProbCompany) -> SignalResult:
        rows = _safe_fetchall(
            self.db,
            """
            SELECT
                COALESCE(assignee_total_num_patents, 0) AS total_patents,
                assignee_last_seen_date
            FROM uspto_assignees
            WHERE LOWER(assignee_organization) ILIKE LOWER(:pat)
               OR LOWER(assignee_name) ILIKE LOWER(:pat)
            ORDER BY assignee_total_num_patents DESC
            LIMIT 1
            """,
            {"pat": f"%{company.company_name}%"},
        )
        if not rows:
            return self._neutral("no USPTO assignee match", ["uspto_assignees"])

        row = rows[0]
        total_patents = int(row.get("total_patents") or 0)
        last_seen = row.get("assignee_last_seen_date")

        # Patent count component
        if total_patents >= 500:
            count_component = 90
        elif total_patents >= 100:
            count_component = 75
        elif total_patents >= 20:
            count_component = 60
        elif total_patents >= 5:
            count_component = 45
        else:
            count_component = 30

        # Recency component: active within 2 years = 80, 2-5 years = 50, >5 years = 20
        recency_component = 30
        if last_seen:
            try:
                if isinstance(last_seen, str):
                    last_seen = datetime.fromisoformat(last_seen).date()
                days_ago = (date.today() - last_seen).days
                if days_ago <= 730:
                    recency_component = 85
                elif days_ago <= 1825:
                    recency_component = 55
                else:
                    recency_component = 25
            except Exception:
                pass

        score = 0.6 * count_component + 0.4 * recency_component
        score = _clamp(score)
        # Confidence based on match likelihood — exact matches ideal; fuzzy matches reduce confidence
        confidence = 0.6 if total_patents > 0 else 0.3

        return SignalResult(
            signal_type=self.signal_type,
            score=score,
            confidence=confidence,
            details={
                "total_patents": total_patents,
                "last_seen_date": str(last_seen) if last_seen else None,
            },
            data_sources=["uspto_assignees"],
        )


# ---------------------------------------------------------------------------
# 6. MacroTailwindComputer
# ---------------------------------------------------------------------------


class MacroTailwindComputer(BaseSignalComputer):
    """
    Score macro/geographic tailwinds for the company.

    Combines:
    - Convergence region score for the company's HQ state
    - Sector momentum from pe_market_signals
    """

    signal_type = "macro_tailwind"

    def compute(self, company: TxnProbCompany) -> SignalResult:
        region_score = None
        sector_score = None
        sources = []

        # HQ state → convergence region score
        if company.hq_state:
            region_rows = _safe_fetchall(
                self.db,
                """
                SELECT convergence_score
                FROM convergence_regions
                WHERE :state = ANY(SELECT jsonb_array_elements_text(states::jsonb))
                ORDER BY scored_at DESC
                LIMIT 1
                """,
                {"state": company.hq_state},
            )
            if region_rows:
                region_score = float(region_rows[0].get("convergence_score") or 0)
                sources.append("convergence_regions")

        # Sector momentum
        if company.sector:
            sector_rows = _safe_fetchall(
                self.db,
                """
                SELECT momentum_score
                FROM pe_market_signals
                WHERE sector = :sector
                ORDER BY scanned_at DESC
                LIMIT 1
                """,
                {"sector": company.sector},
            )
            if sector_rows:
                sector_score = float(sector_rows[0].get("momentum_score") or 0)
                sources.append("pe_market_signals")

        components = [c for c in (region_score, sector_score) if c is not None]
        if not components:
            return self._neutral(
                "no convergence region or sector momentum data",
                ["convergence_regions", "pe_market_signals"],
            )

        score = sum(components) / len(components)
        score = _clamp(score)
        confidence = min(1.0, len(components) / 2.0)

        return SignalResult(
            signal_type=self.signal_type,
            score=score,
            confidence=confidence,
            details={
                "region_score": region_score,
                "sector_momentum": sector_score,
                "hq_state": company.hq_state,
                "sector": company.sector,
            },
            data_sources=sources,
        )


# ---------------------------------------------------------------------------
# Registry — mapping signal_type → computer class
# ---------------------------------------------------------------------------

NEW_COMPUTERS: Dict[str, type] = {
    "insider_activity": InsiderActivityComputer,
    "hiring_velocity": HiringVelocityComputer,
    "deal_activity_signals": DealActivitySignalComputer,
    "founder_risk": FounderRiskComputer,
    "innovation_velocity": InnovationVelocityComputer,
    "macro_tailwind": MacroTailwindComputer,
}
