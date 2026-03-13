"""
PE Market Scanner & Intelligence Brief Service.

Aggregates deal flow, valuation trends, and competitive dynamics across
sectors. Produces sector overviews, intelligence briefs, and momentum
signals for PE deal teams.
"""

import logging
import statistics
from datetime import date, timedelta
from typing import Any, Dict, List, Optional

from sqlalchemy import select, and_, func, extract
from sqlalchemy.orm import Session

from app.core.pe_models import PECompanyNews, PEDeal, PEPortfolioCompany

logger = logging.getLogger(__name__)


class MarketScannerService:
    """Scan markets for deal flow, multiples, and momentum signals."""

    def __init__(self, db: Session):
        self.db = db

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get_sector_overview(
        self, industry: str, years_back: int = 3,
    ) -> Dict[str, Any]:
        """Full sector overview: deal stats, multiples, top buyers."""
        deals = self._fetch_sector_deals(industry, years_back)
        overview = self._compute_sector_overview(industry, deals)

        # Add YoY changes
        prior_deals = self._fetch_sector_deals(industry, years_back, offset_years=years_back)
        current_count = overview["deal_count"]
        prior_count = len(prior_deals)

        current_median = overview.get("median_ev_ebitda")
        prior_multiples = [
            float(d.ev_ebitda_multiple) for d in prior_deals
            if d.ev_ebitda_multiple
        ]
        prior_median = round(statistics.median(prior_multiples), 2) if prior_multiples else None

        overview["yoy_deal_count_change"] = (
            round((current_count - prior_count) / prior_count, 2)
            if prior_count > 0 else None
        )
        overview["yoy_multiple_change"] = (
            round((current_median - prior_median) / prior_median, 2)
            if current_median and prior_median else None
        )

        return overview

    def get_intelligence_brief(
        self, industry: str, years_back: int = 3,
    ) -> Dict[str, Any]:
        """Generate an intelligence brief for the sector."""
        overview = self.get_sector_overview(industry, years_back)
        brief = self._generate_intelligence_brief(overview)
        return brief

    def get_market_signals(self) -> List[Dict[str, Any]]:
        """Cross-sector momentum signals with 0-100 momentum score."""
        industries = self._get_active_industries()
        signals = []

        for industry in industries:
            recent = self._fetch_sector_deals(industry, years_back=2)
            prior = self._fetch_sector_deals(industry, years_back=2, offset_years=2)

            recent_multiples = [
                float(d.ev_ebitda_multiple) for d in recent if d.ev_ebitda_multiple
            ]
            prior_multiples = [
                float(d.ev_ebitda_multiple) for d in prior if d.ev_ebitda_multiple
            ]

            current_median = (
                round(statistics.median(recent_multiples), 2) if recent_multiples else None
            )
            prior_median = (
                round(statistics.median(prior_multiples), 2) if prior_multiples else None
            )

            # News sentiment for sector
            sentiment = self._get_sector_sentiment(industry)

            momentum = self._compute_momentum(
                current_deals=len(recent),
                prior_deals=len(prior),
                current_median=current_median,
                prior_median=prior_median,
                sentiment_score=sentiment.get("avg_sentiment"),
            )

            signals.append({
                "industry": industry,
                "recent_deal_count": len(recent),
                "prior_deal_count": len(prior),
                "current_median_ev_ebitda": current_median,
                "prior_median_ev_ebitda": prior_median,
                "news_sentiment": sentiment,
                **momentum,
            })

        # Sort by momentum_score descending
        signals.sort(
            key=lambda s: s.get("momentum_score", 50), reverse=True
        )
        return signals

    def get_sectors_ranked(self) -> List[Dict[str, Any]]:
        """Return sectors ranked by momentum score (0-100)."""
        signals = self.get_market_signals()
        return [
            {
                "rank": i + 1,
                "industry": s["industry"],
                "momentum_score": s["momentum_score"],
                "momentum": s["momentum"],
                "recent_deal_count": s["recent_deal_count"],
                "current_median_ev_ebitda": s["current_median_ev_ebitda"],
                "news_sentiment": s.get("news_sentiment", {}),
            }
            for i, s in enumerate(signals)
        ]

    # ------------------------------------------------------------------
    # Internal: data fetching
    # ------------------------------------------------------------------

    def _fetch_sector_deals(
        self, industry: str, years_back: int, offset_years: int = 0,
    ) -> List[PEDeal]:
        """Fetch closed deals in an industry within a time window."""
        end_date = date(date.today().year - offset_years, 12, 31)
        start_date = date(end_date.year - years_back, 1, 1)

        stmt = (
            select(PEDeal)
            .join(PEPortfolioCompany, PEDeal.company_id == PEPortfolioCompany.id)
            .where(
                PEPortfolioCompany.industry == industry,
                PEDeal.status == "Closed",
                PEDeal.enterprise_value_usd.isnot(None),
                PEDeal.closed_date >= start_date,
                PEDeal.closed_date <= end_date,
            )
            .order_by(PEDeal.closed_date.desc().nullslast())
        )
        return list(self.db.execute(stmt).scalars().all())

    def _get_sector_sentiment(self, industry: str) -> Dict[str, Any]:
        """Aggregate news sentiment for companies in an industry."""
        stmt = (
            select(
                func.count(PECompanyNews.id),
                func.avg(PECompanyNews.sentiment_score),
            )
            .join(PEPortfolioCompany, PECompanyNews.company_id == PEPortfolioCompany.id)
            .where(PEPortfolioCompany.industry == industry)
        )
        row = self.db.execute(stmt).one()
        count = row[0] or 0
        avg_score = round(float(row[1]), 3) if row[1] is not None else None

        # Sentiment breakdown
        pos_count = neg_count = neutral_count = 0
        if count > 0:
            for sentiment_val, label in [("Positive", "pos"), ("Negative", "neg"), ("Neutral", "neutral")]:
                c = self.db.execute(
                    select(func.count(PECompanyNews.id))
                    .join(PEPortfolioCompany, PECompanyNews.company_id == PEPortfolioCompany.id)
                    .where(
                        PEPortfolioCompany.industry == industry,
                        PECompanyNews.sentiment == sentiment_val,
                    )
                ).scalar() or 0
                if label == "pos":
                    pos_count = c
                elif label == "neg":
                    neg_count = c
                else:
                    neutral_count = c

        return {
            "total_articles": count,
            "avg_sentiment": avg_score,
            "positive": pos_count,
            "negative": neg_count,
            "neutral": neutral_count,
        }

    def _get_active_industries(self) -> List[str]:
        """Get industries that have at least one closed deal."""
        stmt = (
            select(PEPortfolioCompany.industry)
            .join(PEDeal, PEDeal.company_id == PEPortfolioCompany.id)
            .where(
                PEDeal.status == "Closed",
                PEPortfolioCompany.industry.isnot(None),
            )
            .group_by(PEPortfolioCompany.industry)
            .order_by(func.count(PEDeal.id).desc())
        )
        return [row[0] for row in self.db.execute(stmt).all()]

    # ------------------------------------------------------------------
    # Internal: computation (static for testability)
    # ------------------------------------------------------------------

    @staticmethod
    def _compute_sector_overview(
        industry: str, deals: list,
    ) -> Dict[str, Any]:
        """Compute sector overview from a list of deals."""
        overview: Dict[str, Any] = {
            "industry": industry,
            "deal_count": len(deals),
        }

        if not deals:
            overview.update({
                "total_deal_value_usd": 0,
                "median_ev_ebitda": None,
                "median_ev_revenue": None,
                "ev_ebitda_range": None,
                "deal_type_breakdown": {},
                "seller_type_breakdown": {},
                "top_buyers": [],
                "yoy_deal_count_change": None,
                "yoy_multiple_change": None,
            })
            return overview

        # Total deal value
        total_value = sum(
            float(d.enterprise_value_usd) for d in deals
            if d.enterprise_value_usd
        )
        overview["total_deal_value_usd"] = total_value

        # Multiples
        ev_ebitda = [float(d.ev_ebitda_multiple) for d in deals if d.ev_ebitda_multiple]
        ev_revenue = [float(d.ev_revenue_multiple) for d in deals if d.ev_revenue_multiple]

        if ev_ebitda:
            sorted_m = sorted(ev_ebitda)
            overview["median_ev_ebitda"] = round(statistics.median(sorted_m), 2)
            overview["ev_ebitda_range"] = {
                "min": round(sorted_m[0], 2),
                "max": round(sorted_m[-1], 2),
            }
        else:
            overview["median_ev_ebitda"] = None
            overview["ev_ebitda_range"] = None

        overview["median_ev_revenue"] = (
            round(statistics.median(ev_revenue), 2) if ev_revenue else None
        )

        # Deal type breakdown
        type_counts: Dict[str, int] = {}
        for d in deals:
            dt = d.deal_sub_type or d.status or "Unknown"
            type_counts[dt] = type_counts.get(dt, 0) + 1
        overview["deal_type_breakdown"] = type_counts

        # Seller type breakdown
        seller_counts: Dict[str, int] = {}
        for d in deals:
            st = d.seller_type or "Unknown"
            seller_counts[st] = seller_counts.get(st, 0) + 1
        overview["seller_type_breakdown"] = seller_counts

        # Top buyers
        buyer_counts: Dict[str, int] = {}
        for d in deals:
            if d.buyer_name:
                buyer_counts[d.buyer_name] = buyer_counts.get(d.buyer_name, 0) + 1
        top_buyers = sorted(
            [{"buyer_name": k, "deal_count": v} for k, v in buyer_counts.items()],
            key=lambda x: x["deal_count"],
            reverse=True,
        )[:10]
        overview["top_buyers"] = top_buyers

        return overview

    @staticmethod
    def _generate_intelligence_brief(
        overview: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Generate a narrative intelligence brief from sector overview."""
        industry = overview["industry"]
        deal_count = overview["deal_count"]
        total_value = overview.get("total_deal_value_usd", 0)
        median_ebitda = overview.get("median_ev_ebitda")
        yoy_deal = overview.get("yoy_deal_count_change")
        yoy_mult = overview.get("yoy_multiple_change")

        # Headline
        total_b = total_value / 1_000_000_000 if total_value else 0
        headline = (
            f"{industry}: {deal_count} transactions totaling "
            f"${total_b:.1f}B in deal value"
        )

        # Key findings
        findings = []
        if median_ebitda:
            findings.append(
                f"Median EV/EBITDA multiple of {median_ebitda:.1f}x "
                f"across {deal_count} closed deals"
            )
        if overview.get("ev_ebitda_range"):
            r = overview["ev_ebitda_range"]
            findings.append(
                f"Multiple range: {r['min']:.1f}x to {r['max']:.1f}x"
            )
        if yoy_deal is not None:
            direction = "up" if yoy_deal > 0 else "down"
            findings.append(
                f"Deal volume {direction} {abs(yoy_deal)*100:.0f}% year-over-year"
            )
        if yoy_mult is not None:
            direction = "expanding" if yoy_mult > 0 else "compressing"
            findings.append(
                f"Multiples {direction} {abs(yoy_mult)*100:.0f}% year-over-year"
            )

        # Deal type mix
        type_breakdown = overview.get("deal_type_breakdown", {})
        if type_breakdown:
            top_type = max(type_breakdown, key=type_breakdown.get)
            findings.append(
                f"Dominant deal type: {top_type} "
                f"({type_breakdown[top_type]}/{deal_count} deals)"
            )

        # Top buyers
        top_buyers = overview.get("top_buyers", [])
        if top_buyers:
            names = ", ".join(b["buyer_name"] for b in top_buyers[:3])
            findings.append(f"Most active buyers: {names}")

        # Recommendations
        recs = []
        if median_ebitda and median_ebitda > 12:
            recs.append(
                "Seller-favorable market — consider accelerating exit timelines"
            )
        elif median_ebitda and median_ebitda < 8:
            recs.append(
                "Buyer-favorable market — focus on operational improvements before exit"
            )
        if yoy_deal is not None and yoy_deal > 0.2:
            recs.append(
                "Increasing deal flow suggests competitive dynamics — move quickly on targets"
            )
        if yoy_mult is not None and yoy_mult < -0.1:
            recs.append(
                "Declining multiples — consider delaying non-urgent exits"
            )
        if not recs:
            recs.append("Market conditions stable — standard diligence approach appropriate")

        return {
            "industry": industry,
            "headline": headline,
            "key_findings": findings,
            "recommendations": recs,
            "deal_count": deal_count,
            "total_deal_value_usd": total_value,
            "median_ev_ebitda": median_ebitda,
        }

    @staticmethod
    def _compute_momentum(
        current_deals: int,
        prior_deals: int,
        current_median: Optional[float],
        prior_median: Optional[float],
        sentiment_score: Optional[float] = None,
    ) -> Dict[str, Any]:
        """Compute momentum signal and 0-100 score from current vs prior period.

        Score components (total 100):
        - Deal flow change: 40 pts (positive change = higher score)
        - Multiple change: 35 pts (expanding multiples = higher)
        - Sentiment: 25 pts (positive sentiment = higher)
        """
        deal_change = (
            round((current_deals - prior_deals) / prior_deals, 2)
            if prior_deals > 0 else None
        )
        mult_change = (
            round((current_median - prior_median) / prior_median, 2)
            if current_median and prior_median else None
        )

        # Momentum score 0-100
        score = 50.0  # baseline

        # Deal flow component (40 pts: -20 to +20 from baseline)
        if deal_change is not None:
            # Clamp to [-1, 1] range, scale to [-20, +20]
            clamped = max(-1.0, min(1.0, deal_change))
            score += clamped * 20.0

        # Multiple change component (35 pts: -17.5 to +17.5)
        if mult_change is not None:
            clamped = max(-1.0, min(1.0, mult_change * 2))  # amplify since changes are small
            score += clamped * 17.5

        # Sentiment component (25 pts: -12.5 to +12.5)
        if sentiment_score is not None:
            # sentiment_score is -1.0 to 1.0
            score += sentiment_score * 12.5

        # Clamp to 0-100
        score = round(max(0, min(100, score)))

        # Determine momentum label
        if score >= 65:
            momentum = "bullish"
        elif score <= 35:
            momentum = "bearish"
        else:
            momentum = "neutral"

        return {
            "momentum": momentum,
            "momentum_score": score,
            "deal_flow_change_pct": deal_change,
            "multiple_change_pct": mult_change,
        }
