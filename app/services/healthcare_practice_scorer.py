"""
Healthcare Practice Profile Scorer — Chain 7 of PLAN_052.

Unified acquisition scoring for healthcare practices (med-spas, clinics).
Combines existing medspa_prospects + zip_medspa_scores + NPPES data into
a single 5-factor profile for PE roll-up targeting.

5 factors:
  Market Attractiveness (25%) — ZIP affluence + market size
  Clinical Credibility (20%) — physician oversight + NPPES provider count
  Competitive Position (20%) — Yelp rating + review volume + saturation
  Revenue Potential (20%) — estimated revenue + growth signals
  Multi-Unit Potential (15%) — location count + chain indicators
"""
from __future__ import annotations
import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional
from sqlalchemy.orm import Session
from sqlalchemy import text

logger = logging.getLogger(__name__)


@dataclass
class PracticeFactor:
    factor: str
    score: int       # 0-100
    weight: float
    reading: str
    impact: str


@dataclass
class HealthcarePracticeProfile:
    prospect_id: int
    name: str
    city: str
    state: str
    zip_code: str
    acquisition_score: int     # 0-100 composite
    grade: str
    factors: List[PracticeFactor] = field(default_factory=list)
    details: Dict = field(default_factory=dict)


GRADE_THRESHOLDS = [(85, "A"), (70, "B"), (55, "C"), (40, "D"), (0, "F")]


def _safe_query(db: Session, sql: str, params: dict):
    try:
        result = db.execute(text(sql), params)
        return result.fetchall()
    except Exception as exc:
        try:
            db.rollback()
        except Exception:
            pass
        logger.debug("Healthcare query failed: %s", exc)
        return []


class HealthcarePracticeScorer:

    def __init__(self, db: Session):
        self.db = db

    def screen(self, state: Optional[str] = None, min_score: int = 0,
               limit: int = 50) -> List[HealthcarePracticeProfile]:
        """Screen medspa prospects for acquisition, ranked by composite score."""
        params: dict = {"limit": limit, "min_score": min_score}
        state_clause = ""
        if state:
            state_clause = "AND m.state = :state"
            params["state"] = state.upper()

        rows = _safe_query(self.db, f"""
            SELECT m.id, m.name, m.city, m.state, m.zip_code,
                   m.rating, m.review_count, m.acquisition_score, m.acquisition_grade,
                   m.zip_overall_score, m.zip_affluence_density,
                   m.has_physician_oversight, m.nppes_provider_count,
                   m.estimated_annual_revenue, m.location_count, m.ownership_type,
                   m.competitor_count_in_zip, m.market_saturation_index,
                   m.review_velocity_30d, m.rating_trend,
                   m.has_botox, m.has_fillers, m.has_laser, m.has_coolsculpting
            FROM medspa_prospects m
            WHERE m.acquisition_score IS NOT NULL
              AND m.acquisition_score >= :min_score
              {state_clause}
            ORDER BY m.acquisition_score DESC
            LIMIT :limit
        """, params)

        results = []
        for r in rows:
            profile = self._build_profile(r)
            results.append(profile)
        return results

    def score_prospect(self, prospect_id: int) -> Optional[HealthcarePracticeProfile]:
        """Score a single prospect with full factor breakdown."""
        rows = _safe_query(self.db, """
            SELECT m.id, m.name, m.city, m.state, m.zip_code,
                   m.rating, m.review_count, m.acquisition_score, m.acquisition_grade,
                   m.zip_overall_score, m.zip_affluence_density,
                   m.has_physician_oversight, m.nppes_provider_count,
                   m.estimated_annual_revenue, m.location_count, m.ownership_type,
                   m.competitor_count_in_zip, m.market_saturation_index,
                   m.review_velocity_30d, m.rating_trend,
                   m.has_botox, m.has_fillers, m.has_laser, m.has_coolsculpting
            FROM medspa_prospects m
            WHERE m.id = :pid
        """, {"pid": prospect_id})

        if not rows:
            return None
        return self._build_profile(rows[0])

    def _build_profile(self, r) -> HealthcarePracticeProfile:
        (pid, name, city, state, zip_code,
         rating, review_count, acq_score, acq_grade,
         zip_score, zip_affluence,
         has_physician, nppes_count,
         est_revenue, location_count, ownership_type,
         competitor_count, saturation_index,
         review_velocity, rating_trend,
         has_botox, has_fillers, has_laser, has_coolsculpting) = r

        factors = []

        # --- Factor 1: Market Attractiveness (25%) ---
        zip_s = float(zip_score or 0)
        if zip_s >= 80:
            mkt_score = 95
            reading = f"ZIP score {zip_s:.0f}/100 — premium market"
            impact = "positive"
        elif zip_s >= 60:
            mkt_score = 75
            reading = f"ZIP score {zip_s:.0f}/100 — strong market"
            impact = "positive"
        elif zip_s >= 40:
            mkt_score = 55
            reading = f"ZIP score {zip_s:.0f}/100 — moderate market"
            impact = "neutral"
        else:
            mkt_score = 30
            reading = f"ZIP score {zip_s:.0f}/100 — weak market"
            impact = "warning"
        factors.append(PracticeFactor("Market attractiveness", mkt_score, 0.25, reading, impact))

        # --- Factor 2: Clinical Credibility (20%) ---
        nppes = int(nppes_count or 0)
        physician = bool(has_physician)
        if physician and nppes >= 3:
            clin_score = 95
            reading = f"Physician oversight confirmed, {nppes} NPPES providers — high credibility"
            impact = "positive"
        elif physician or nppes >= 2:
            clin_score = 75
            reading = f"{'Physician oversight' if physician else f'{nppes} NPPES providers'} — good credibility"
            impact = "positive"
        elif nppes >= 1:
            clin_score = 55
            reading = f"{nppes} NPPES provider — basic clinical coverage"
            impact = "neutral"
        else:
            clin_score = 25
            reading = "No physician oversight or NPPES match — credibility risk"
            impact = "warning"
        factors.append(PracticeFactor("Clinical credibility", clin_score, 0.20, reading, impact))

        # --- Factor 3: Competitive Position (20%) ---
        r_val = float(rating or 0)
        rev_ct = int(review_count or 0)
        try:
            sat = float(saturation_index) if saturation_index else 0
        except (ValueError, TypeError):
            sat = 0.5  # default moderate saturation if non-numeric

        rating_pts = min(r_val * 20, 100) if r_val > 0 else 50
        review_pts = min(rev_ct / 5, 100)  # 500 reviews = 100
        sat_pts = max(0, 100 - sat * 100) if sat else 50  # low saturation = good

        comp_score = int(rating_pts * 0.4 + review_pts * 0.3 + sat_pts * 0.3)
        comp_score = max(0, min(100, comp_score))

        if comp_score >= 75:
            reading = f"{r_val}★, {rev_ct} reviews — strong competitive position"
            impact = "positive"
        elif comp_score >= 50:
            reading = f"{r_val}★, {rev_ct} reviews — adequate position"
            impact = "neutral"
        else:
            reading = f"{r_val}★, {rev_ct} reviews — weak competitive position"
            impact = "warning"
        factors.append(PracticeFactor("Competitive position", comp_score, 0.20, reading, impact))

        # --- Factor 4: Revenue Potential (20%) ---
        rev = float(est_revenue) if est_revenue else None
        if rev and rev > 3_000_000:
            rev_score = 95
            reading = f"${rev / 1e6:.1f}M estimated revenue — high-value target"
            impact = "positive"
        elif rev and rev > 1_000_000:
            rev_score = 75
            reading = f"${rev / 1e6:.1f}M estimated revenue — solid practice"
            impact = "positive"
        elif rev and rev > 500_000:
            rev_score = 55
            reading = f"${rev / 1e3:.0f}K estimated revenue — mid-size"
            impact = "neutral"
        elif rev:
            rev_score = 35
            reading = f"${rev / 1e3:.0f}K estimated revenue — small practice"
            impact = "neutral"
        else:
            rev_score = 50
            reading = "No revenue estimate available"
            impact = "neutral"
        factors.append(PracticeFactor("Revenue potential", rev_score, 0.20, reading, impact))

        # --- Factor 5: Multi-Unit Potential (15%) ---
        locs = int(location_count or 1)
        own_type = ownership_type or "Unknown"

        if locs >= 5:
            multi_score = 95
            reading = f"{locs} locations ({own_type}) — established multi-unit platform"
            impact = "positive"
        elif locs >= 2:
            multi_score = 80
            reading = f"{locs} locations ({own_type}) — proven expansion capability"
            impact = "positive"
        elif own_type == "Independent":
            multi_score = 60
            reading = f"Single location, independent — acquisition candidate"
            impact = "neutral"
        else:
            multi_score = 40
            reading = f"Single location ({own_type})"
            impact = "neutral"
        factors.append(PracticeFactor("Multi-unit potential", multi_score, 0.15, reading, impact))

        # --- Composite ---
        total_w = sum(f.weight for f in factors)
        composite = sum(f.score * f.weight for f in factors) / total_w if total_w > 0 else 0
        score = max(0, min(100, int(round(composite))))
        grade = next((g for threshold, g in GRADE_THRESHOLDS if score >= threshold), "F")

        # Count services offered
        services = []
        if has_botox: services.append("Botox")
        if has_fillers: services.append("Fillers")
        if has_laser: services.append("Laser")
        if has_coolsculpting: services.append("CoolSculpting")

        return HealthcarePracticeProfile(
            prospect_id=pid, name=name or "", city=city or "", state=state or "",
            zip_code=zip_code or "",
            acquisition_score=score, grade=grade,
            factors=factors,
            details={
                "yelp_rating": float(rating) if rating else None,
                "review_count": int(review_count) if review_count else None,
                "zip_market_score": float(zip_score) if zip_score else None,
                "has_physician_oversight": physician,
                "nppes_provider_count": nppes,
                "estimated_revenue": rev,
                "location_count": locs,
                "ownership_type": own_type,
                "competitor_count_in_zip": int(competitor_count) if competitor_count else None,
                "services_offered": services,
                "review_velocity_30d": float(review_velocity) if review_velocity else None,
                "original_acquisition_score": float(acq_score) if acq_score else None,
            },
        )
