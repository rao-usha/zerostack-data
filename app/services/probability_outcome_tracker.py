"""
Deal Probability Engine — Outcome Tracker (SPEC 048, PLAN_059 Phase 4).

Closes the learning loop by scanning the real world for transactions that
happened on universe companies, then backfilling the probability scores
we had recorded 6 and 12 months prior.

Data sources for outcomes:
- `pe_deals` — primary source: confirmed PE transactions
- `pe_portfolio_companies.ownership_status` changes (future enhancement)
- SEC 8-K filings (future enhancement)
- News events (future enhancement)

Output: populated `txn_prob_outcomes` rows, usable as labels for
`ProbabilityCalibrator.fit_*` and `SignalWeightOptimizer.optimize_weights`.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.core.probability_models import (
    TxnProbCompany,
    TxnProbOutcome,
    TxnProbScore,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DEFAULT_LOOKBACK_DAYS = 365 * 2  # Scan 2 years of deals
DEAL_TYPE_TO_OUTCOME = {
    "acquisition": "acquired",
    "buyout": "acquired",
    "ipo": "ipo",
    "secondary": "secondary_sale",
    "recap": "recap",
    "recapitalization": "recap",
    "spac": "spac_merger",
    "merger": "acquired",
}


# ---------------------------------------------------------------------------
# Outcome tracker
# ---------------------------------------------------------------------------


class OutcomeTracker:
    """
    Populate and maintain `txn_prob_outcomes`.

    Usage:
        tracker = OutcomeTracker(db)
        scan_stats  = tracker.scan_for_outcomes()
        backfill    = tracker.backfill_predictions()
        df          = tracker.get_labeled_dataset()
    """

    def __init__(self, db: Session):
        self.db = db

    # -------------------------------------------------------------------
    # Scan: find real transactions and persist as outcomes
    # -------------------------------------------------------------------

    def scan_for_outcomes(self, lookback_days: int = DEFAULT_LOOKBACK_DAYS) -> Dict:
        """
        Scan pe_deals for transactions on universe companies.
        Returns {scanned, inserted, skipped_existing, errors}.
        """
        stats = {"scanned": 0, "inserted": 0, "skipped_existing": 0, "errors": 0}

        try:
            rows = self.db.execute(
                text(
                    """
                    SELECT
                        d.id AS pe_deal_id,
                        d.company_id AS pe_portfolio_company_id,
                        d.announced_date,
                        d.closed_date,
                        COALESCE(d.enterprise_value_usd, d.equity_value_usd) AS deal_value_usd,
                        d.deal_type,
                        d.buyer_name,
                        p.name AS company_name
                    FROM pe_deals d
                    JOIN pe_portfolio_companies p ON d.company_id = p.id
                    WHERE d.announced_date IS NOT NULL
                      AND d.announced_date >= NOW() - make_interval(days => :lookback)
                    """
                ),
                {"lookback": lookback_days},
            ).mappings().all()
        except Exception as exc:
            self.db.rollback()
            logger.debug("scan_for_outcomes query failed: %s", exc)
            return stats

        for r in rows:
            stats["scanned"] += 1
            try:
                outcome = self._upsert_outcome(r)
                if outcome == "inserted":
                    stats["inserted"] += 1
                else:
                    stats["skipped_existing"] += 1
            except Exception as exc:
                logger.debug("outcome upsert failed for deal %s: %s", r.get("pe_deal_id"), exc)
                self.db.rollback()
                stats["errors"] += 1

        self.db.commit()
        return stats

    def _upsert_outcome(self, deal_row: Dict) -> str:
        """
        Insert a TxnProbOutcome row if we have a matching universe company.
        Dedups by (company_id, announced_date, outcome_type).
        """
        pe_pc_id = deal_row.get("pe_portfolio_company_id")
        if not pe_pc_id:
            return "skipped"

        # Find the universe company for this PE portfolio company
        universe_company = (
            self.db.query(TxnProbCompany)
            .filter_by(canonical_company_id=pe_pc_id)
            .first()
        )
        if not universe_company:
            return "skipped"

        outcome_type = DEAL_TYPE_TO_OUTCOME.get(
            (deal_row.get("deal_type") or "").lower(), "acquired"
        )
        announced_date = deal_row.get("announced_date")

        # Dedup
        existing = (
            self.db.query(TxnProbOutcome)
            .filter_by(
                company_id=universe_company.id,
                announced_date=announced_date,
                outcome_type=outcome_type,
            )
            .first()
        )
        if existing:
            return "skipped"

        savepoint = self.db.begin_nested()
        try:
            row = TxnProbOutcome(
                company_id=universe_company.id,
                outcome_type=outcome_type,
                announced_date=announced_date,
                closed_date=deal_row.get("closed_date"),
                deal_value_usd=deal_row.get("deal_value_usd"),
                buyer_name=deal_row.get("buyer_name"),
                deal_source="pe_deals",
                pe_deal_id=deal_row.get("pe_deal_id"),
            )
            self.db.add(row)
            self.db.flush()
            savepoint.commit()
            return "inserted"
        except Exception as exc:
            savepoint.rollback()
            logger.debug("outcome insert rolled back: %s", exc)
            return "skipped"

    # -------------------------------------------------------------------
    # Backfill: look up historical predictions at announcement/6mo/12mo
    # -------------------------------------------------------------------

    def backfill_predictions(self) -> Dict:
        """
        For each outcome without backfilled predictions, look up the
        probability we had recorded at that company at the time of
        announcement, 6 months prior, and 12 months prior.
        """
        stats = {"evaluated": 0, "filled": 0, "no_history": 0}

        outcomes = (
            self.db.query(TxnProbOutcome)
            .filter(
                (TxnProbOutcome.prediction_at_announcement.is_(None))
                | (TxnProbOutcome.prediction_6mo_prior.is_(None))
                | (TxnProbOutcome.prediction_12mo_prior.is_(None))
            )
            .all()
        )

        for outcome in outcomes:
            stats["evaluated"] += 1
            ad = outcome.announced_date
            if not ad:
                stats["no_history"] += 1
                continue

            ad_dt = (
                datetime.combine(ad, datetime.min.time())
                if isinstance(ad, date) and not isinstance(ad, datetime)
                else ad
            )

            p_at = self._prediction_closest_before(outcome.company_id, ad_dt)
            p_6 = self._prediction_closest_before(
                outcome.company_id, ad_dt - timedelta(days=183)
            )
            p_12 = self._prediction_closest_before(
                outcome.company_id, ad_dt - timedelta(days=365)
            )

            if p_at is None and p_6 is None and p_12 is None:
                stats["no_history"] += 1
                continue

            if p_at is not None:
                outcome.prediction_at_announcement = p_at
            if p_6 is not None:
                outcome.prediction_6mo_prior = p_6
            if p_12 is not None:
                outcome.prediction_12mo_prior = p_12
            stats["filled"] += 1

        self.db.commit()
        return stats

    def _prediction_closest_before(
        self, company_id: int, target_dt: datetime
    ) -> Optional[float]:
        """Return the probability from the closest score at-or-before target_dt."""
        row = (
            self.db.query(TxnProbScore)
            .filter(TxnProbScore.company_id == company_id)
            .filter(TxnProbScore.scored_at <= target_dt)
            .order_by(TxnProbScore.scored_at.desc())
            .first()
        )
        return row.probability if row else None

    # -------------------------------------------------------------------
    # Build labeled dataset for calibration / ML training
    # -------------------------------------------------------------------

    def get_labeled_dataset(self, min_history_months: int = 0):
        """
        Build a pandas DataFrame with 12 signal columns + binary label.

        Each row is one (company, scored_at) snapshot:
          - features: one column per signal_type (the score 0-100)
          - label `outcome_within_12mo`: 1 if a transaction happened in
            the 12 months after `scored_at`, else 0
        """
        import pandas as pd

        # Map signal_chain (JSON) to per-signal feature rows
        score_rows = self.db.query(TxnProbScore).all()
        feature_records = []
        for s in score_rows:
            chain = s.signal_chain or []
            record = {
                "company_id": s.company_id,
                "scored_at": s.scored_at,
                "probability": s.probability,
                "raw_composite_score": s.raw_composite_score,
            }
            for entry in chain:
                sig = entry.get("signal_type")
                if sig:
                    record[sig] = entry.get("score")
            feature_records.append(record)

        if not feature_records:
            return pd.DataFrame()

        df = pd.DataFrame(feature_records)

        # Attach labels — did an outcome happen in the 12 months after scored_at?
        outcomes = self.db.query(TxnProbOutcome).all()
        outcome_map: Dict[int, List[datetime]] = {}
        for o in outcomes:
            if o.outcome_type == "no_transaction":
                continue
            if o.announced_date is None:
                continue
            ad = o.announced_date
            ad_dt = (
                datetime.combine(ad, datetime.min.time())
                if isinstance(ad, date) and not isinstance(ad, datetime)
                else ad
            )
            outcome_map.setdefault(o.company_id, []).append(ad_dt)

        def has_outcome_in_window(cid, scored_at):
            if cid not in outcome_map:
                return 0
            scored = (
                datetime.combine(scored_at, datetime.min.time())
                if isinstance(scored_at, date) and not isinstance(scored_at, datetime)
                else scored_at
            )
            window_end = scored + timedelta(days=365)
            return int(
                any(scored < ad <= window_end for ad in outcome_map[cid])
            )

        df["outcome_within_12mo"] = df.apply(
            lambda r: has_outcome_in_window(r["company_id"], r["scored_at"]),
            axis=1,
        )

        return df

    # -------------------------------------------------------------------
    # Metrics
    # -------------------------------------------------------------------

    def compute_calibration_metrics(self) -> Dict:
        """Summary counts for the dashboard."""
        total = self.db.query(TxnProbOutcome).count()
        labeled = (
            self.db.query(TxnProbOutcome)
            .filter(TxnProbOutcome.prediction_at_announcement.isnot(None))
            .count()
        )
        by_type_rows = self.db.execute(
            text(
                """
                SELECT outcome_type, COUNT(*) AS c
                FROM txn_prob_outcomes
                GROUP BY outcome_type
                """
            )
        ).mappings().all()
        return {
            "total_outcomes": total,
            "with_backfilled_predictions": labeled,
            "by_type": {r["outcome_type"]: r["c"] for r in by_type_rows},
        }
