"""
Deal Environment Scorer — Product 1 of PLAN_048 Macro PE Data Products.

Computes a 0-100 deal attractiveness score per PE sector based on live
FRED and BLS macro signals. Updated on every data refresh.
"""
from __future__ import annotations
import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple
from sqlalchemy.orm import Session
from sqlalchemy import text

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class ScoreFactor:
    factor: str
    reading: str
    score_contribution: int   # positive or negative
    impact: str               # "positive", "negative", "neutral"
    data_source: str


@dataclass
class DealEnvironmentScore:
    sector: str
    sector_label: str
    score: int                # 0-100
    grade: str                # A, B, C, D
    signal: str               # "green", "yellow", "red"
    recommendation: str
    factors: List[ScoreFactor] = field(default_factory=list)
    macro_inputs: Dict = field(default_factory=dict)
    updated_at: Optional[str] = None


# ---------------------------------------------------------------------------
# Sector definitions — maps sector slug to relevant series
# ---------------------------------------------------------------------------

SECTOR_CONFIGS = {
    "industrials": {
        "label": "Industrials",
        "fred_series": ["INDPRO", "TCU"],       # Industrial production, capacity util
        "bls_series": ["CES3000000001"],          # Manufacturing employment
        "sensitivity": {"rates": "medium", "labor": "high", "consumer": "low", "energy": "high", "fx_export": "medium"},
    },
    "consumer": {
        "label": "Consumer",
        "fred_series": ["UMCSENT", "RSXFS"],     # Consumer sentiment, retail sales
        "bls_series": ["CES4200000001"],          # Retail employment
        "sensitivity": {"rates": "medium", "labor": "medium", "consumer": "high", "energy": "low", "fx_export": "low"},
    },
    "healthcare": {
        "label": "Healthcare",
        "fred_series": ["UNRATE"],
        "bls_series": ["CES6500000001"],          # Education & health employment
        "sensitivity": {"rates": "low", "labor": "high", "consumer": "low", "energy": "low", "fx_export": "low"},
    },
    "technology": {
        "label": "Technology",
        "fred_series": ["DGS10", "DFF"],
        "bls_series": ["CES6000000001"],          # Professional services
        "sensitivity": {"rates": "high", "labor": "high", "consumer": "low", "energy": "low", "fx_export": "medium"},
    },
    "real_estate": {
        "label": "Real Estate",
        "fred_series": ["MORTGAGE30US", "HOUST", "CSUSHPINSA"],
        "bls_series": ["CES2000000001"],          # Construction
        "sensitivity": {"rates": "very_high", "labor": "medium", "consumer": "low", "energy": "low", "fx_export": "low"},
    },
    "energy": {
        "label": "Energy",
        "fred_series": ["DCOILWTICO", "DHHNGSP"],
        "bls_series": [],
        "sensitivity": {"rates": "medium", "labor": "low", "consumer": "low", "energy": "very_high", "fx_export": "high"},
    },
    "financial": {
        "label": "Financial Services",
        "fred_series": ["DFF", "DGS10", "DGS2"],
        "bls_series": ["CES5500000001"],
        "sensitivity": {"rates": "very_high", "labor": "low", "consumer": "medium", "energy": "low", "fx_export": "low"},
    },
    "auto_service": {
        "label": "Auto Service",
        "fred_series": ["TOTALSA", "UMCSENT"],
        "bls_series": [],
        "sensitivity": {"rates": "medium", "labor": "medium", "consumer": "high", "energy": "medium", "fx_export": "low"},
    },
    "logistics": {
        "label": "Logistics & Transportation",
        "fred_series": ["INDPRO", "DCOILWTICO"],
        "bls_series": ["CES4300000001"],
        "sensitivity": {"rates": "medium", "labor": "high", "consumer": "medium", "energy": "very_high", "fx_export": "medium"},
    },
}

# ---------------------------------------------------------------------------
# BEA GDP by Industry → PE sector mapping
# BEA Table 1 (Value Added) industry_id codes
# ---------------------------------------------------------------------------

SECTOR_BEA_INDUSTRY_MAP = {
    "industrials": "31G",       # Manufacturing
    "consumer": "44RT",         # Retail trade
    "healthcare": "62",         # Health care and social assistance
    "technology": "51",         # Information
    "real_estate": "53",        # Real estate and rental and leasing
    "energy": "21",             # Mining (includes oil & gas extraction)
    "financial": "52",          # Finance and insurance
    "logistics": "48TW",       # Transportation and warehousing
    "auto_service": "3361MV",   # Motor vehicles, bodies and trailers, and parts
}

GRADE_THRESHOLDS = [(80, "A"), (65, "B"), (50, "C"), (0, "D")]
SIGNAL_THRESHOLDS = [(70, "green"), (50, "yellow"), (0, "red")]

SECTOR_RECOMMENDATIONS = {
    "A": "Compelling deployment window. Macro conditions support deal financing and exit visibility.",
    "B": "Selective deployment. Favorable conditions with manageable headwinds — focus on quality assets.",
    "C": "Cautious. Macro headwinds present — favor defensive positioning, lower leverage, strong operators.",
    "D": "Avoid or hold. Material macro risks; new deployment requires exceptional asset quality and pricing.",
}


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
        logger.debug("Query failed: %s", exc)
        return []


def _get_fred_latest(db: Session, series_id: str) -> Tuple[Optional[float], Optional[float]]:
    """Return (latest_value, value_12m_ago) for a FRED series."""
    rows = _safe_query(db, """
        (SELECT date, value FROM fred_interest_rates
         WHERE series_id = :sid ORDER BY date DESC LIMIT 14)
        UNION ALL
        (SELECT date, value FROM fred_economic_indicators
         WHERE series_id = :sid ORDER BY date DESC LIMIT 14)
        UNION ALL
        (SELECT date, value FROM fred_housing_market
         WHERE series_id = :sid ORDER BY date DESC LIMIT 14)
        UNION ALL
        (SELECT date, value FROM fred_consumer_sentiment
         WHERE series_id = :sid ORDER BY date DESC LIMIT 14)
        ORDER BY date DESC LIMIT 14
    """, {"sid": series_id})

    if not rows:
        return None, None

    # Sort by date desc
    rows_sorted = sorted(rows, key=lambda r: r[0], reverse=True)
    latest = float(rows_sorted[0][1]) if rows_sorted[0][1] is not None else None

    # Find ~12 months ago (index 11 or 12 in monthly data)
    prev = None
    if len(rows_sorted) >= 12:
        prev = float(rows_sorted[11][1]) if rows_sorted[11][1] is not None else None

    return latest, prev


def _get_fred_commodity_latest(db: Session, series_id: str) -> Tuple[Optional[float], Optional[float]]:
    """Return (latest_value, value_12m_ago) for a FRED commodity series.

    Separate from _get_fred_latest() to avoid breaking the main UNION ALL
    if fred_commodities table doesn't exist.
    """
    rows = _safe_query(db, """
        SELECT date, value FROM fred_commodities
        WHERE series_id = :sid ORDER BY date DESC LIMIT 14
    """, {"sid": series_id})

    if not rows:
        return None, None

    rows_sorted = sorted(rows, key=lambda r: r[0], reverse=True)
    latest = float(rows_sorted[0][1]) if rows_sorted[0][1] is not None else None
    prev = None
    if len(rows_sorted) >= 12:
        prev = float(rows_sorted[11][1]) if rows_sorted[11][1] is not None else None
    return latest, prev


def _get_bea_gdp_growth(db: Session, industry_id: str) -> Optional[float]:
    """Return YoY GDP growth % for a BEA industry, or None if unavailable.

    Queries bea_gdp_industry (Table 1 = Value Added, quarterly) for the latest
    two same-quarter values and computes percentage change.
    """
    rows = _safe_query(db, """
        SELECT time_period, data_value
        FROM bea_gdp_industry
        WHERE table_id = '1'
          AND industry_id = :iid
          AND frequency = 'Q'
          AND data_value IS NOT NULL
        ORDER BY time_period DESC
        LIMIT 8
    """, {"iid": industry_id})

    if len(rows) < 2:
        return None

    # rows are (time_period, data_value) sorted desc, e.g. ("2025Q3", 1234.5)
    # Find latest quarter and same quarter from prior year
    latest_period = str(rows[0][0])
    latest_val = float(rows[0][1])

    # Derive the year-ago quarter (e.g. "2025Q3" -> "2024Q3")
    try:
        yr = int(latest_period[:4])
        q_suffix = latest_period[4:]  # e.g. "Q3"
        target = f"{yr - 1}{q_suffix}"
    except (ValueError, IndexError):
        return None

    for r in rows:
        if str(r[0]) == target and r[1] is not None:
            prev_val = float(r[1])
            if prev_val > 0:
                return ((latest_val - prev_val) / prev_val) * 100
    return None


def _get_bls_latest(db: Session, series_id: str) -> Tuple[Optional[float], Optional[float]]:
    """Return (latest_value, value_12m_ago) for a BLS series."""
    rows = _safe_query(db, """
        (SELECT year, period, value::numeric FROM bls_ces_employment
         WHERE series_id = :sid ORDER BY year DESC, period DESC LIMIT 14)
        UNION ALL
        (SELECT year, period, value FROM bls_cps_labor_force
         WHERE series_id = :sid ORDER BY year DESC, period DESC LIMIT 14)
        UNION ALL
        (SELECT year, period, value FROM bls_jolts
         WHERE series_id = :sid ORDER BY year DESC, period DESC LIMIT 14)
        ORDER BY year DESC, period DESC LIMIT 14
    """, {"sid": series_id})

    if not rows:
        return None, None

    rows_sorted = sorted(rows, key=lambda r: (r[0], r[1]), reverse=True)
    latest = float(rows_sorted[0][2]) if rows_sorted[0][2] is not None else None
    prev = (
        float(rows_sorted[11][2])
        if len(rows_sorted) >= 12 and rows_sorted[11][2] is not None
        else None
    )
    return latest, prev


# ---------------------------------------------------------------------------
# Core scorer
# ---------------------------------------------------------------------------

class DealEnvironmentScorer:
    """
    Computes deal attractiveness scores for PE sectors based on live macro data.
    """

    def __init__(self, db: Session):
        self.db = db
        self._macro_cache: Dict = {}

    def score_all_sectors(self) -> List[DealEnvironmentScore]:
        """Score all configured sectors. Returns list sorted by score desc."""
        self._load_macro_inputs()
        return sorted(
            [self.score_sector(slug) for slug in SECTOR_CONFIGS],
            key=lambda s: s.score,
            reverse=True,
        )

    def score_sector(self, sector_slug: str) -> DealEnvironmentScore:
        """Score a single sector."""
        config = SECTOR_CONFIGS.get(sector_slug)
        if not config:
            raise ValueError(f"Unknown sector: {sector_slug}")

        if not self._macro_cache:
            self._load_macro_inputs()

        factors: List[ScoreFactor] = []
        base_score = 100

        # --- Factor 1: Rate environment (25% weight) ---
        ffr = self._macro_cache.get("DFF")
        rate_sensitivity = config["sensitivity"].get("rates", "medium")
        rate_mult = {"very_high": 1.5, "high": 1.25, "medium": 1.0, "low": 0.5}.get(rate_sensitivity, 1.0)

        if ffr is not None:
            if ffr > 5.0:
                penalty = int(25 * rate_mult)
                reading = f"FFR {ffr:.2f}% — elevated; LBO financing cost +150-200bps vs. 2021"
                impact = "negative"
            elif ffr > 4.0:
                penalty = int(12 * rate_mult)
                reading = f"FFR {ffr:.2f}% — moderately elevated"
                impact = "negative"
            elif ffr > 2.5:
                penalty = int(5 * rate_mult)
                reading = f"FFR {ffr:.2f}% — neutral range"
                impact = "neutral"
            else:
                penalty = 0
                reading = f"FFR {ffr:.2f}% — accommodative; deal financing conditions favorable"
                impact = "positive"
            base_score -= penalty
            factors.append(ScoreFactor("Rate environment", reading, -penalty, impact, "FRED DFF"))

        # --- Factor 2: Yield curve (20% weight) ---
        dgs10 = self._macro_cache.get("DGS10")
        dgs2 = self._macro_cache.get("DGS2")
        if dgs10 is not None and dgs2 is not None:
            spread = dgs10 - dgs2
            if spread < -0.5:
                penalty = 20
                reading = f"Yield curve inverted ({spread:.2f}pp) — recession signal"
                impact = "negative"
                base_score -= penalty
                factors.append(ScoreFactor("Yield curve", reading, -penalty, impact, "FRED DGS10-DGS2"))
            elif spread < 0:
                penalty = 12
                reading = f"Yield curve slightly inverted ({spread:.2f}pp) — caution"
                impact = "negative"
                base_score -= penalty
                factors.append(ScoreFactor("Yield curve", reading, -penalty, impact, "FRED DGS10-DGS2"))
            elif spread > 1.5:
                bonus = 8
                base_score += bonus
                reading = f"Yield curve normal (+{spread:.2f}pp) — favorable credit environment"
                impact = "positive"
                factors.append(ScoreFactor("Yield curve", reading, bonus, impact, "FRED DGS10-DGS2"))
            else:
                reading = f"Yield curve near-flat ({spread:.2f}pp)"
                factors.append(ScoreFactor("Yield curve", reading, 0, "neutral", "FRED DGS10-DGS2"))

        # --- Factor 3: Sector labor momentum (20% weight) ---
        labor_sensitivity = config["sensitivity"].get("labor", "medium")
        labor_mult = {"very_high": 1.5, "high": 1.25, "medium": 1.0, "low": 0.5}.get(labor_sensitivity, 1.0)

        for series_id in config.get("bls_series", []):
            latest_emp, prev_emp = _get_bls_latest(self.db, series_id)
            if latest_emp is not None and prev_emp is not None and prev_emp > 0:
                delta_pct = ((latest_emp - prev_emp) / prev_emp) * 100
                if delta_pct > 3:
                    bonus = int(15 * labor_mult)
                    base_score += bonus
                    reading = f"Sector employment +{delta_pct:.1f}% (12m) — strong expansion"
                    impact = "positive"
                elif delta_pct > 1:
                    bonus = int(7 * labor_mult)
                    base_score += bonus
                    reading = f"Sector employment +{delta_pct:.1f}% (12m) — steady growth"
                    impact = "positive"
                elif delta_pct > -1:
                    bonus = 0
                    reading = f"Sector employment {delta_pct:+.1f}% (12m) — flat"
                    impact = "neutral"
                else:
                    penalty = int(15 * labor_mult)
                    base_score -= penalty
                    bonus = -penalty
                    reading = f"Sector employment {delta_pct:.1f}% (12m) — contracting"
                    impact = "negative"
                factors.append(ScoreFactor("Sector labor", reading, bonus, impact, f"BLS {series_id}"))
            break  # use first BLS series only

        # --- Factor 4: Consumer confidence (15% weight) ---
        consumer_sensitivity = config["sensitivity"].get("consumer", "medium")
        consumer_mult = {"very_high": 1.5, "high": 1.25, "medium": 1.0, "low": 0.5}.get(consumer_sensitivity, 1.0)
        umcsent = self._macro_cache.get("UMCSENT")
        if umcsent is not None and consumer_mult > 0.3:
            if umcsent > 80:
                bonus = int(12 * consumer_mult)
                base_score += bonus
                reading = f"Consumer sentiment {umcsent:.0f} — strong demand outlook"
                impact = "positive"
            elif umcsent > 65:
                bonus = int(5 * consumer_mult)
                base_score += bonus
                reading = f"Consumer sentiment {umcsent:.0f} — moderate demand"
                impact = "neutral"
            else:
                penalty = int(10 * consumer_mult)
                base_score -= penalty
                bonus = -penalty
                reading = f"Consumer sentiment {umcsent:.0f} — weak demand signal"
                impact = "negative"
            factors.append(ScoreFactor("Consumer confidence", reading, bonus, impact, "FRED UMCSENT"))

        # --- Factor 5: CPI (input cost pressure) (10% weight) ---
        cpi_yoy = self._macro_cache.get("CPIAUCSL_YOY")
        if cpi_yoy is not None:
            if cpi_yoy > 5.0:
                penalty = 12
                base_score -= penalty
                reading = f"CPI YoY {cpi_yoy:.1f}% — input cost inflation severe"
                impact = "negative"
                factors.append(ScoreFactor("Input costs (CPI)", reading, -penalty, impact, "BLS CPIAUCSL"))
            elif cpi_yoy > 3.5:
                penalty = 6
                base_score -= penalty
                reading = f"CPI YoY {cpi_yoy:.1f}% — moderate inflation headwind"
                impact = "negative"
                factors.append(ScoreFactor("Input costs (CPI)", reading, -penalty, impact, "BLS CPIAUCSL"))
            elif cpi_yoy < 2.0:
                bonus = 5
                base_score += bonus
                reading = f"CPI YoY {cpi_yoy:.1f}% — inflation contained, margin support"
                impact = "positive"
                factors.append(ScoreFactor("Input costs (CPI)", reading, bonus, impact, "BLS CPIAUCSL"))
            else:
                reading = f"CPI YoY {cpi_yoy:.1f}% — inflation normalizing"
                factors.append(ScoreFactor("Input costs (CPI)", reading, 0, "neutral", "BLS CPIAUCSL"))

        # --- Factor 6: Energy input cost pressure (10% weight) ---
        energy_sensitivity = config["sensitivity"].get("energy", "low")
        energy_mult = {"very_high": 1.5, "high": 1.25, "medium": 1.0, "low": 0.5}.get(energy_sensitivity, 0.5)
        energy_yoy = self._macro_cache.get("ENERGY_COST_YOY")

        if energy_yoy is not None:
            if energy_yoy > 30:
                penalty = int(10 * energy_mult)
                reading = f"Energy costs +{energy_yoy:.0f}% YoY — severe input cost pressure"
                impact = "negative"
            elif energy_yoy > 15:
                penalty = int(6 * energy_mult)
                reading = f"Energy costs +{energy_yoy:.0f}% YoY — elevated input costs"
                impact = "negative"
            elif energy_yoy > 0:
                penalty = int(2 * energy_mult)
                reading = f"Energy costs +{energy_yoy:.0f}% YoY — moderate headwind"
                impact = "neutral"
            elif energy_yoy > -15:
                penalty = 0
                reading = f"Energy costs {energy_yoy:.0f}% YoY — stable to declining"
                impact = "neutral"
            else:
                penalty = -int(5 * energy_mult)
                reading = f"Energy costs {energy_yoy:.0f}% YoY — significant input cost relief"
                impact = "positive"
            base_score -= penalty
            factors.append(ScoreFactor("Energy input costs", reading, -penalty, impact, "FRED DCOILWTICO/DHHNGSP"))

        # --- Factor 7: Sector GDP growth (12% weight) ---
        gdp_growth = self._macro_cache.get(f"GDP_GROWTH_{sector_slug}")
        if gdp_growth is not None:
            bea_id = SECTOR_BEA_INDUSTRY_MAP.get(sector_slug, "?")
            if gdp_growth > 4.0:
                bonus = 12
                reading = f"Sector GDP +{gdp_growth:.1f}% YoY — strong expansion"
                impact = "positive"
            elif gdp_growth > 2.0:
                bonus = 6
                reading = f"Sector GDP +{gdp_growth:.1f}% YoY — healthy growth"
                impact = "positive"
            elif gdp_growth > 0:
                bonus = 2
                reading = f"Sector GDP +{gdp_growth:.1f}% YoY — modest growth"
                impact = "neutral"
            elif gdp_growth > -2.0:
                bonus = -5
                reading = f"Sector GDP {gdp_growth:+.1f}% YoY — contraction signal"
                impact = "negative"
            else:
                bonus = -12
                reading = f"Sector GDP {gdp_growth:+.1f}% YoY — recessionary"
                impact = "negative"
            base_score += bonus
            factors.append(ScoreFactor("Sector GDP growth", reading, bonus, impact, f"BEA GDPbyIndustry {bea_id}"))

        # --- Factor 8: Dollar strength / FX risk (5% weight, optional) ---
        usd_chg = self._macro_cache.get("DTWEXBGS_3M_CHG")
        if usd_chg is not None:
            fx_sensitivity = config["sensitivity"].get("fx_export", "low")
            fx_mult = {"high": 1.25, "medium": 1.0, "low": 0.5}.get(fx_sensitivity, 0.5)

            if usd_chg > 5.0:
                penalty = int(8 * fx_mult)
                reading = f"USD +{usd_chg:.1f}% (3m) — strong dollar headwind for exporters"
                impact = "negative"
            elif usd_chg > 2.0:
                penalty = int(3 * fx_mult)
                reading = f"USD +{usd_chg:.1f}% (3m) — moderate dollar strength"
                impact = "neutral"
            elif usd_chg > -2.0:
                penalty = 0
                reading = f"USD {usd_chg:+.1f}% (3m) — stable"
                impact = "neutral"
            else:
                penalty = -int(4 * fx_mult)
                reading = f"USD {usd_chg:.1f}% (3m) — weak dollar supports export competitiveness"
                impact = "positive"
            base_score -= penalty
            factors.append(ScoreFactor("Dollar strength", reading, -penalty, impact, "FRED DTWEXBGS"))

        # Clamp
        score = max(0, min(100, base_score))

        # Grade + signal
        grade = next((g for threshold, g in GRADE_THRESHOLDS if score >= threshold), "D")
        signal = next((s for threshold, s in SIGNAL_THRESHOLDS if score >= threshold), "red")

        return DealEnvironmentScore(
            sector=sector_slug,
            sector_label=config["label"],
            score=score,
            grade=grade,
            signal=signal,
            recommendation=SECTOR_RECOMMENDATIONS[grade],
            factors=factors,
            macro_inputs={
                "fed_funds_rate": self._macro_cache.get("DFF"),
                "ten_year_yield": self._macro_cache.get("DGS10"),
                "yield_spread": (
                    (self._macro_cache.get("DGS10") or 0) - (self._macro_cache.get("DGS2") or 0)
                    if self._macro_cache.get("DGS10") is not None and self._macro_cache.get("DGS2") is not None
                    else None
                ),
                "consumer_sentiment": self._macro_cache.get("UMCSENT"),
                "cpi_yoy_pct": self._macro_cache.get("CPIAUCSL_YOY"),
                "oil_price": self._macro_cache.get("DCOILWTICO"),
                "natgas_price": self._macro_cache.get("DHHNGSP"),
                "energy_cost_yoy_pct": self._macro_cache.get("ENERGY_COST_YOY"),
                "sector_gdp_growth_pct": self._macro_cache.get(f"GDP_GROWTH_{sector_slug}"),
                "usd_3m_change_pct": self._macro_cache.get("DTWEXBGS_3M_CHG"),
            },
        )

    def _load_macro_inputs(self):
        """Load all required macro series into cache."""
        series_to_load = [
            "DFF", "DGS10", "DGS2", "UNRATE", "UMCSENT",
            "CPIAUCSL", "TOTALSA", "INDPRO", "TCU",
        ]
        for sid in series_to_load:
            latest, prev = _get_fred_latest(self.db, sid)
            self._macro_cache[sid] = latest
            if sid == "CPIAUCSL" and latest is not None and prev is not None and prev > 0:
                self._macro_cache["CPIAUCSL_YOY"] = ((latest - prev) / prev) * 100

        # --- Energy cost data (FRED commodities — separate table) ---
        for sid in ["DCOILWTICO", "DHHNGSP"]:
            latest, prev = _get_fred_commodity_latest(self.db, sid)
            self._macro_cache[sid] = latest
            if latest is not None and prev is not None and prev > 0:
                self._macro_cache[f"{sid}_YOY"] = ((latest - prev) / prev) * 100

        # Blended energy cost YoY (average of oil + gas if both available)
        oil_yoy = self._macro_cache.get("DCOILWTICO_YOY")
        gas_yoy = self._macro_cache.get("DHHNGSP_YOY")
        if oil_yoy is not None and gas_yoy is not None:
            self._macro_cache["ENERGY_COST_YOY"] = (oil_yoy + gas_yoy) / 2
        elif oil_yoy is not None:
            self._macro_cache["ENERGY_COST_YOY"] = oil_yoy
        elif gas_yoy is not None:
            self._macro_cache["ENERGY_COST_YOY"] = gas_yoy

        # --- BEA GDP growth by sector ---
        for sector_slug, bea_id in SECTOR_BEA_INDUSTRY_MAP.items():
            growth = _get_bea_gdp_growth(self.db, bea_id)
            if growth is not None:
                self._macro_cache[f"GDP_GROWTH_{sector_slug}"] = growth

        # --- Dollar strength (optional — only if DTWEXBGS ingested) ---
        dtwex_latest, dtwex_prev = _get_fred_commodity_latest(self.db, "DTWEXBGS")
        if dtwex_latest is None:
            dtwex_latest, dtwex_prev = _get_fred_latest(self.db, "DTWEXBGS")
        self._macro_cache["DTWEXBGS"] = dtwex_latest
        if dtwex_latest is not None and dtwex_prev is not None and dtwex_prev > 0:
            self._macro_cache["DTWEXBGS_3M_CHG"] = ((dtwex_latest - dtwex_prev) / dtwex_prev) * 100
