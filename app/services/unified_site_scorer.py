"""
Unified Site Scorer — Chain 5 of PLAN_052.

Point-based (lat/lng) site scoring across 5 factors using Haversine
radius queries against the existing 63-table site intel infrastructure.
Supports configurable use-case weights (datacenter, manufacturing, warehouse, general).

Factors:
  Power Access (nearby MW, substations, electricity price)
  Climate Risk (NRI risk score, flood zones, seismic hazard)
  Workforce (unemployment, wages, employment density)
  Connectivity (broadband, IX proximity, DC cluster density)
  Regulatory/Incentives (OZ, FTZ, incentive programs, utility rates)
"""
from __future__ import annotations
import logging
import math
from dataclasses import dataclass, field
from typing import Dict, List, Optional
from sqlalchemy.orm import Session
from sqlalchemy import text

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class SiteFactor:
    factor: str
    score: int          # 0-100
    weight: float
    reading: str
    impact: str         # positive, neutral, warning, negative
    details: Dict = field(default_factory=dict)


@dataclass
class UnifiedSiteScore:
    latitude: float
    longitude: float
    score: int          # 0-100 composite
    grade: str
    signal: str
    use_case: str
    factors: List[SiteFactor] = field(default_factory=list)
    raw_metrics: Dict = field(default_factory=dict)
    coverage: float = 0.0   # % of factors with data


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

USE_CASE_WEIGHTS = {
    "general":       {"power": 0.20, "climate": 0.20, "workforce": 0.20, "connectivity": 0.20, "regulatory": 0.20},
    "datacenter":    {"power": 0.35, "climate": 0.15, "workforce": 0.15, "connectivity": 0.25, "regulatory": 0.10},
    "manufacturing": {"power": 0.25, "climate": 0.15, "workforce": 0.25, "connectivity": 0.10, "regulatory": 0.25},
    "warehouse":     {"power": 0.10, "climate": 0.15, "workforce": 0.20, "connectivity": 0.15, "regulatory": 0.40},
}

GRADE_THRESHOLDS = [(80, "A"), (65, "B"), (50, "C"), (35, "D"), (0, "F")]
SIGNAL_MAP = [(70, "green"), (50, "yellow"), (0, "red")]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_query(db: Session, sql: str, params: dict):
    try:
        result = db.execute(text(sql), params)
        return result.fetchall()
    except Exception as exc:
        try:
            db.rollback()
        except Exception:
            pass
        logger.debug("Site score query failed: %s", exc)
        return []


def _bbox_params(lat: float, lng: float, radius_miles: float) -> dict:
    """Compute bounding box for radius pre-filter."""
    lat_range = radius_miles / 69.0
    lng_range = radius_miles / (69.0 * max(math.cos(math.radians(lat)), 0.01))
    return {
        "lat": lat, "lng": lng,
        "lat_min": lat - lat_range, "lat_max": lat + lat_range,
        "lng_min": lng - lng_range, "lng_max": lng + lng_range,
        "radius": radius_miles,
    }


def _reverse_geocode_state(db: Session, lat: float, lng: float) -> Optional[str]:
    """Find nearest state from labor_market_area or NRI data."""
    # Use NRI county data — find nearest county by FIPS/state
    rows = _safe_query(db, """
        SELECT DISTINCT state FROM electricity_price
        WHERE geography_type = 'state' AND geography_id IS NOT NULL
        LIMIT 60
    """, {})
    # Fallback: derive state from nearest power plant
    plant_rows = _safe_query(db, """
        SELECT state FROM power_plant
        WHERE latitude BETWEEN :lat_min AND :lat_max
          AND longitude BETWEEN :lng_min AND :lng_max
        LIMIT 1
    """, _bbox_params(lat, lng, 100))
    if plant_rows:
        return plant_rows[0][0]
    return None


def _find_county_fips(db: Session, lat: float, lng: float) -> Optional[str]:
    """Find county FIPS by matching nearest NRI county (crude centroid match)."""
    # Use NRI table — find closest county (already has county_fips)
    # Since NRI doesn't have lat/lng, use flood_zone state+county to approximate
    state = _reverse_geocode_state(db, lat, lng)
    if not state:
        return None
    # Get all counties in state from NRI, return first with risk_score
    rows = _safe_query(db, """
        SELECT county_fips FROM national_risk_index
        WHERE state = :state AND risk_score IS NOT NULL
        ORDER BY county_fips
        LIMIT 1
    """, {"state": state})
    return rows[0][0] if rows else None


# ---------------------------------------------------------------------------
# Factor scorers
# ---------------------------------------------------------------------------

def _score_power(db: Session, lat: float, lng: float, radius: float) -> tuple[int, str, str, dict]:
    """Power Access: nearby MW capacity + substations + electricity price."""
    params = _bbox_params(lat, lng, radius)

    # Nearby power plants
    plants = _safe_query(db, """
        SELECT COALESCE(SUM(nameplate_capacity_mw), 0) as total_mw,
               COUNT(*) as plant_count
        FROM power_plant
        WHERE latitude BETWEEN :lat_min AND :lat_max
          AND longitude BETWEEN :lng_min AND :lng_max
    """, params)
    total_mw = float(plants[0][0]) if plants else 0
    plant_count = int(plants[0][1]) if plants else 0

    # Nearby substations
    subs = _safe_query(db, """
        SELECT COUNT(*) FROM substation
        WHERE latitude BETWEEN :lat_min AND :lat_max
          AND longitude BETWEEN :lng_min AND :lng_max
    """, params)
    sub_count = int(subs[0][0]) if subs else 0

    # State electricity price (industrial)
    state = _reverse_geocode_state(db, lat, lng)
    price = None
    if state:
        price_rows = _safe_query(db, """
            SELECT avg_price_cents_kwh FROM electricity_price
            WHERE geography_type = 'state' AND geography_id = :state
              AND sector = 'industrial'
            ORDER BY period_year DESC, period_month DESC NULLS LAST
            LIMIT 1
        """, {"state": state})
        if price_rows and price_rows[0][0]:
            price = float(price_rows[0][0])

    # Score
    mw_score = min(total_mw / 50, 100)   # 5000 MW = 100
    sub_score = min(sub_count * 5, 100)   # 20 subs = 100
    price_score = max(0, 100 - (price - 5) * 10) if price else 50  # 5¢=100, 15¢=0

    score = int(mw_score * 0.4 + sub_score * 0.3 + price_score * 0.3)
    score = max(0, min(100, score))

    details = {
        "total_mw_nearby": round(total_mw),
        "plant_count": plant_count,
        "substation_count": sub_count,
        "electricity_price_cents_kwh": price,
        "state": state,
    }

    if score >= 75:
        reading = f"{total_mw:.0f} MW, {sub_count} substations within {radius}mi"
        if price:
            reading += f", {price:.1f}¢/kWh"
        reading += " — strong power access"
        impact = "positive"
    elif score >= 50:
        reading = f"{total_mw:.0f} MW, {sub_count} substations — adequate power"
        impact = "neutral"
    else:
        reading = f"{total_mw:.0f} MW, {sub_count} substations — limited power infrastructure"
        impact = "warning"

    return score, reading, impact, details


def _score_climate(db: Session, lat: float, lng: float, state: Optional[str]) -> tuple[int, str, str, dict]:
    """Climate Risk: NRI score + flood zones + seismic (inverted — low risk = high score)."""
    nri_score = None
    nri_rating = None
    flood_high_risk = 0

    # NRI county risk — use state median (not worst county)
    if state:
        nri_rows = _safe_query(db, """
            SELECT AVG(risk_score), MODE() WITHIN GROUP (ORDER BY risk_rating)
            FROM national_risk_index
            WHERE state = :state AND risk_score IS NOT NULL
        """, {"state": state})
        if nri_rows:
            nri_score = float(nri_rows[0][0])
            nri_rating = nri_rows[0][1]

    # Flood zones in state
    if state:
        flood_rows = _safe_query(db, """
            SELECT COUNT(*) FROM flood_zone
            WHERE state = :state AND is_high_risk = true
        """, {"state": state})
        flood_high_risk = int(flood_rows[0][0]) if flood_rows else 0

    # Seismic: nearest hazard
    seismic_rows = _safe_query(db, """
        SELECT hazard_level FROM seismic_hazard
        WHERE latitude BETWEEN :lat_min AND :lat_max
          AND longitude BETWEEN :lng_min AND :lng_max
        ORDER BY hazard_level ASC
        LIMIT 1
    """, _bbox_params(lat, lng, 200))
    seismic_level = seismic_rows[0][0] if seismic_rows else None

    details = {
        "nri_risk_score": nri_score,
        "nri_risk_rating": nri_rating,
        "flood_high_risk_zones": flood_high_risk,
        "seismic_hazard_level": seismic_level,
    }

    # Invert: low risk = high score
    # NRI scale is ~0-100 where higher = MORE risk
    if nri_score is not None:
        nri_inverted = max(0, 100 - nri_score)  # low NRI = high score
    else:
        nri_inverted = 50  # neutral if no data

    flood_penalty = min(flood_high_risk // 5, 20)  # up to -20, scaled by state flood zone density
    seismic_penalty = 0
    if seismic_level and str(seismic_level).isdigit():
        seismic_penalty = min(int(seismic_level) * 5, 20)

    score = max(0, min(100, int(nri_inverted - flood_penalty - seismic_penalty)))

    if score >= 75:
        reading = f"Low risk profile"
        if nri_rating:
            reading += f" (NRI: {nri_rating})"
        reading += " — favorable for development"
        impact = "positive"
    elif score >= 50:
        reading = f"Moderate risk"
        if nri_rating:
            reading += f" (NRI: {nri_rating})"
        if flood_high_risk > 0:
            reading += f", {flood_high_risk} flood risk zones"
        impact = "neutral"
    else:
        reading = f"Elevated risk"
        if nri_rating:
            reading += f" (NRI: {nri_rating})"
        if flood_high_risk > 5:
            reading += f", {flood_high_risk} flood zones"
        impact = "warning"

    return score, reading, impact, details


def _score_workforce(db: Session, state: Optional[str]) -> tuple[int, str, str, dict]:
    """Workforce: unemployment + wages + employment density."""
    unemployment = None
    employment = None
    labor_force = None

    if state:
        labor_rows = _safe_query(db, """
            SELECT unemployment_rate, employment, labor_force
            FROM labor_market_area
            WHERE state = :state AND area_type = 'state'
            LIMIT 1
        """, {"state": state})
        if labor_rows:
            unemployment = float(labor_rows[0][0]) if labor_rows[0][0] else None
            employment = int(labor_rows[0][1]) if labor_rows[0][1] else None
            labor_force = int(labor_rows[0][2]) if labor_rows[0][2] else None

    # Wage data
    median_wage = None
    if state:
        wage_rows = _safe_query(db, """
            SELECT AVG(median_annual_wage) FROM occupational_wage
            WHERE area_code = :area_code AND median_annual_wage > 0
        """, {"area_code": f"ST{state}" if len(state) == 2 else state})
        if not wage_rows or wage_rows[0][0] is None:
            # Try state abbreviation format
            wage_rows = _safe_query(db, """
                SELECT AVG(median_annual_wage) FROM occupational_wage
                WHERE area_code LIKE :pattern AND median_annual_wage > 0
            """, {"pattern": f"%{state}%"})
        if wage_rows and wage_rows[0][0]:
            median_wage = float(wage_rows[0][0])

    details = {
        "unemployment_rate": unemployment,
        "employment": employment,
        "labor_force": labor_force,
        "median_wage": median_wage,
        "state": state,
    }

    # Score components
    unemp_score = 50  # default
    if unemployment is not None:
        if unemployment < 3.0:
            unemp_score = 90  # tight labor = strong economy
        elif unemployment < 4.5:
            unemp_score = 80
        elif unemployment < 6.0:
            unemp_score = 60
        else:
            unemp_score = 35  # high unemployment = weak

    emp_score = 50
    if employment:
        if employment > 5_000_000:
            emp_score = 95
        elif employment > 1_000_000:
            emp_score = 80
        elif employment > 500_000:
            emp_score = 65
        else:
            emp_score = 45

    wage_score = 50
    if median_wage:
        if median_wage > 60000:
            wage_score = 70  # high wages = skilled but costly
        elif median_wage > 45000:
            wage_score = 85  # sweet spot
        elif median_wage > 30000:
            wage_score = 65
        else:
            wage_score = 40

    score = int(unemp_score * 0.4 + emp_score * 0.3 + wage_score * 0.3)
    score = max(0, min(100, score))

    if score >= 75:
        parts = []
        if unemployment is not None:
            parts.append(f"{unemployment:.1f}% unemployment")
        if median_wage:
            parts.append(f"${median_wage / 1000:.0f}K median wage")
        reading = ", ".join(parts) + " — strong labor market" if parts else "Strong labor market"
        impact = "positive"
    elif score >= 50:
        reading = f"Adequate labor market"
        if unemployment is not None:
            reading += f" ({unemployment:.1f}% unemployment)"
        impact = "neutral"
    else:
        reading = "Weak labor market indicators"
        if unemployment is not None:
            reading += f" ({unemployment:.1f}% unemployment)"
        impact = "warning"

    return score, reading, impact, details


def _score_connectivity(db: Session, lat: float, lng: float, radius: float) -> tuple[int, str, str, dict]:
    """Connectivity: broadband + IX proximity + DC cluster."""
    params = _bbox_params(lat, lng, radius)

    # Broadband coverage
    bb_rows = _safe_query(db, """
        SELECT COUNT(DISTINCT provider_name) as providers,
               MAX(max_download_mbps) as max_speed,
               SUM(CASE WHEN technology = 'Fiber' OR max_download_mbps >= 1000 THEN 1 ELSE 0 END) as fiber_count
        FROM broadband_availability
        WHERE latitude BETWEEN :lat_min AND :lat_max
          AND longitude BETWEEN :lng_min AND :lng_max
    """, params)
    providers = int(bb_rows[0][0]) if bb_rows and bb_rows[0][0] else 0
    max_speed = float(bb_rows[0][1]) if bb_rows and bb_rows[0][1] else 0
    has_fiber = int(bb_rows[0][2] or 0) > 0 if bb_rows else False

    # Internet exchanges nearby
    ix_rows = _safe_query(db, """
        SELECT COUNT(*), MAX(network_count)
        FROM internet_exchange
        WHERE latitude BETWEEN :lat_min AND :lat_max
          AND longitude BETWEEN :lng_min AND :lng_max
    """, params)
    ix_count = int(ix_rows[0][0]) if ix_rows else 0
    ix_networks = int(ix_rows[0][1] or 0) if ix_rows else 0

    # Data center cluster
    dc_rows = _safe_query(db, """
        SELECT COUNT(*), SUM(COALESCE(power_mw, 0))
        FROM data_center_facility
        WHERE latitude BETWEEN :lat_min AND :lat_max
          AND longitude BETWEEN :lng_min AND :lng_max
    """, params)
    dc_count = int(dc_rows[0][0]) if dc_rows else 0
    dc_power = float(dc_rows[0][1] or 0) if dc_rows else 0

    details = {
        "broadband_providers": providers,
        "max_download_mbps": max_speed,
        "has_fiber": has_fiber,
        "ix_count": ix_count,
        "ix_max_networks": ix_networks,
        "dc_count": dc_count,
        "dc_total_power_mw": round(dc_power),
    }

    # Score
    bb_score = min(providers * 15, 40) + (30 if has_fiber else 0) + min(max_speed / 100, 30)
    ix_score = min(ix_count * 30, 100)
    dc_score = min(dc_count * 10, 100)

    score = int(bb_score * 0.4 + ix_score * 0.3 + dc_score * 0.3)
    score = max(0, min(100, score))

    if score >= 75:
        reading = f"{providers} providers, {ix_count} IX, {dc_count} DCs within {radius}mi"
        if has_fiber:
            reading += " (fiber available)"
        reading += " — excellent connectivity"
        impact = "positive"
    elif score >= 50:
        reading = f"{providers} providers, {dc_count} DCs — good connectivity"
        impact = "neutral"
    else:
        reading = f"{providers} providers — limited connectivity infrastructure"
        impact = "warning"

    return score, reading, impact, details


def _score_regulatory(db: Session, lat: float, lng: float, radius: float, state: Optional[str]) -> tuple[int, str, str, dict]:
    """Regulatory/Incentives: OZ, FTZ, incentive programs, utility rates."""
    params = _bbox_params(lat, lng, radius)

    # Opportunity zones nearby (state-level since OZ lacks lat/lng)
    oz_count = 0
    if state:
        oz_rows = _safe_query(db, """
            SELECT COUNT(*) FROM opportunity_zone WHERE state = :state
        """, {"state": state})
        oz_count = int(oz_rows[0][0]) if oz_rows else 0

    # Foreign trade zones nearby
    ftz_rows = _safe_query(db, """
        SELECT COUNT(*) FROM foreign_trade_zone
        WHERE latitude BETWEEN :lat_min AND :lat_max
          AND longitude BETWEEN :lng_min AND :lng_max
    """, params)
    ftz_count = int(ftz_rows[0][0]) if ftz_rows else 0

    # Incentive programs (state-level)
    incentive_count = 0
    if state:
        inc_rows = _safe_query(db, """
            SELECT COUNT(*) FROM incentive_program WHERE state = :state
        """, {"state": state})
        incentive_count = int(inc_rows[0][0]) if inc_rows else 0

    # Industrial utility rate
    ind_rate = None
    if state:
        rate_rows = _safe_query(db, """
            SELECT avg_price_cents_kwh FROM utility_rate
            WHERE state_fips = :state AND sector = 'industrial'
            ORDER BY effective_date DESC NULLS LAST
            LIMIT 1
        """, {"state": state})
        if rate_rows and rate_rows[0][0]:
            ind_rate = float(rate_rows[0][0])

    details = {
        "opportunity_zones_in_state": oz_count,
        "ftz_nearby": ftz_count,
        "incentive_programs": incentive_count,
        "industrial_utility_rate": ind_rate,
        "state": state,
    }

    # Score
    oz_score = min(oz_count, 50)  # cap at 50 — many OZs = good
    ftz_score = min(ftz_count * 25, 50)  # 2 FTZ = 50
    inc_score = min(incentive_count * 10, 30)  # 3 programs = 30
    rate_score = max(0, 50 - (ind_rate - 5) * 5) if ind_rate else 25  # 5¢=50, 15¢=0

    score = int(oz_score * 0.25 + ftz_score * 0.25 + inc_score * 0.25 + rate_score * 0.25)
    score = max(0, min(100, score))

    parts = []
    if ftz_count > 0:
        parts.append(f"{ftz_count} FTZ nearby")
    if incentive_count > 0:
        parts.append(f"{incentive_count} incentive programs")
    if oz_count > 0:
        parts.append(f"{oz_count} opportunity zones in state")

    if score >= 70:
        reading = ", ".join(parts) + " — favorable regulatory environment" if parts else "Favorable environment"
        impact = "positive"
    elif score >= 40:
        reading = ", ".join(parts) + " — moderate incentives" if parts else "Moderate incentive landscape"
        impact = "neutral"
    else:
        reading = "Limited incentives and regulatory support"
        impact = "warning"

    return score, reading, impact, details


# ---------------------------------------------------------------------------
# Core scorer
# ---------------------------------------------------------------------------

class UnifiedSiteScorer:

    def __init__(self, db: Session):
        self.db = db

    def score_location(
        self,
        lat: float,
        lng: float,
        radius_miles: float = 50,
        use_case: str = "general",
    ) -> UnifiedSiteScore:
        """Score a location across 5 factors."""

        weights = USE_CASE_WEIGHTS.get(use_case, USE_CASE_WEIGHTS["general"])
        state = _reverse_geocode_state(self.db, lat, lng)
        factors = []
        all_metrics = {"state_detected": state}
        factors_with_data = 0

        # --- Power Access ---
        p_score, p_reading, p_impact, p_details = _score_power(self.db, lat, lng, radius_miles)
        factors.append(SiteFactor("Power access", p_score, weights["power"], p_reading, p_impact, p_details))
        all_metrics.update({f"power_{k}": v for k, v in p_details.items()})
        if p_details.get("plant_count", 0) > 0 or p_details.get("substation_count", 0) > 0:
            factors_with_data += 1

        # --- Climate Risk ---
        c_score, c_reading, c_impact, c_details = _score_climate(self.db, lat, lng, state)
        factors.append(SiteFactor("Climate risk", c_score, weights["climate"], c_reading, c_impact, c_details))
        all_metrics.update({f"climate_{k}": v for k, v in c_details.items()})
        if c_details.get("nri_risk_score") is not None:
            factors_with_data += 1

        # --- Workforce ---
        w_score, w_reading, w_impact, w_details = _score_workforce(self.db, state)
        factors.append(SiteFactor("Workforce", w_score, weights["workforce"], w_reading, w_impact, w_details))
        all_metrics.update({f"workforce_{k}": v for k, v in w_details.items()})
        if w_details.get("unemployment_rate") is not None:
            factors_with_data += 1

        # --- Connectivity ---
        n_score, n_reading, n_impact, n_details = _score_connectivity(self.db, lat, lng, radius_miles)
        factors.append(SiteFactor("Connectivity", n_score, weights["connectivity"], n_reading, n_impact, n_details))
        all_metrics.update({f"connectivity_{k}": v for k, v in n_details.items()})
        if n_details.get("broadband_providers", 0) > 0 or n_details.get("dc_count", 0) > 0:
            factors_with_data += 1

        # --- Regulatory / Incentives ---
        r_score, r_reading, r_impact, r_details = _score_regulatory(self.db, lat, lng, radius_miles, state)
        factors.append(SiteFactor("Regulatory & incentives", r_score, weights["regulatory"], r_reading, r_impact, r_details))
        all_metrics.update({f"regulatory_{k}": v for k, v in r_details.items()})
        if r_details.get("ftz_nearby", 0) > 0 or r_details.get("incentive_programs", 0) > 0:
            factors_with_data += 1

        # --- Composite ---
        total_weight = sum(f.weight for f in factors)
        composite = sum(f.score * f.weight for f in factors) / total_weight if total_weight > 0 else 0
        score = max(0, min(100, int(round(composite))))
        coverage = factors_with_data / 5

        grade = next((g for threshold, g in GRADE_THRESHOLDS if score >= threshold), "F")
        signal = next((s for threshold, s in SIGNAL_MAP if score >= threshold), "red")

        return UnifiedSiteScore(
            latitude=lat, longitude=lng,
            score=score, grade=grade, signal=signal,
            use_case=use_case, factors=factors,
            raw_metrics=all_metrics, coverage=round(coverage, 2),
        )

    def compare_locations(
        self,
        locations: List[Dict],
        radius_miles: float = 50,
        use_case: str = "general",
    ) -> List[UnifiedSiteScore]:
        """Score and rank multiple locations."""
        results = []
        for loc in locations:
            result = self.score_location(
                lat=loc["lat"], lng=loc["lng"],
                radius_miles=radius_miles, use_case=use_case,
            )
            results.append(result)
        return sorted(results, key=lambda r: r.score, reverse=True)
