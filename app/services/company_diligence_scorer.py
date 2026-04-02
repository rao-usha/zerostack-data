"""
Company Diligence Composite Scorer — Chain 2 of PLAN_052.

Joins 8 public data sources into a single 0-100 company health score
for PE target screening. Six weighted factors:
  Revenue Concentration (USAspending, SAM.gov)
  Environmental Risk (EPA ECHO)
  Safety Risk (OSHA)
  Legal Exposure (CourtListener)
  Innovation Capacity (USPTO)
  Growth Momentum (Job Postings)

Missing data sources are skipped gracefully — confidence reflects coverage.
"""
from __future__ import annotations
import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional
from sqlalchemy.orm import Session
from sqlalchemy import text

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class DiligenceFactor:
    factor: str
    score: int               # 0-100 (100 = best / lowest risk)
    weight: float            # 0.0-1.0
    reading: str             # narrative
    impact: str              # "positive", "negative", "neutral", "warning"
    data_source: str
    details: Dict = field(default_factory=dict)


@dataclass
class CompanyDiligenceScore:
    company_name: str
    score: int               # 0-100 weighted composite
    grade: str               # A, B, C, D
    signal: str              # green, yellow, red
    recommendation: str
    factors: List[DiligenceFactor] = field(default_factory=list)
    sources_matched: List[str] = field(default_factory=list)
    sources_empty: List[str] = field(default_factory=list)
    confidence: float = 0.0  # 0.0-1.0, % of factors with data
    red_flags: List[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

FACTOR_WEIGHTS = {
    "revenue_concentration": 0.20,
    "environmental_risk": 0.15,
    "safety_risk": 0.15,
    "legal_exposure": 0.10,
    "innovation_capacity": 0.15,
    "growth_momentum": 0.25,
}

GRADE_THRESHOLDS = [(80, "A"), (65, "B"), (50, "C"), (0, "D")]
SIGNAL_THRESHOLDS = [(70, "green"), (50, "yellow"), (0, "red")]

RECOMMENDATIONS = {
    "A": "Strong diligence profile. No material red flags across public data sources.",
    "B": "Acceptable profile with minor concerns. Investigate flagged factors before proceeding.",
    "C": "Elevated risk. Multiple public data sources flag concerns — deeper diligence required.",
    "D": "High risk. Material regulatory, legal, or operational red flags. Proceed with extreme caution.",
}


# ---------------------------------------------------------------------------
# Safe query helper (same pattern as deal_environment_scorer)
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
        logger.debug("Diligence query failed: %s", exc)
        return []


def _name_pattern(company_name: str) -> str:
    """Build ILIKE pattern from company name. Strips common suffixes."""
    clean = company_name.strip()
    for suffix in [", Inc.", ", Inc", " Inc.", " Inc", ", LLC", " LLC",
                   ", LP", " LP", " Corp.", " Corp", " Corporation",
                   ", Ltd.", " Ltd", " Company", " Co."]:
        if clean.upper().endswith(suffix.upper()):
            clean = clean[:len(clean) - len(suffix)]
            break
    return f"%{clean}%"


# ---------------------------------------------------------------------------
# Source matchers
# ---------------------------------------------------------------------------

def _match_usaspending(db: Session, name: str, state: Optional[str]) -> Optional[Dict]:
    """USAspending: total federal contract awards."""
    pattern = _name_pattern(name)
    params: dict = {"pattern": pattern}
    state_clause = ""
    if state:
        state_clause = "AND place_of_performance_state = :state"
        params["state"] = state.upper()

    rows = _safe_query(db, f"""
        SELECT COALESCE(SUM(award_amount), 0) as total_awards,
               COUNT(*) as num_contracts,
               MAX(award_amount) as max_single
        FROM usaspending_awards
        WHERE recipient_name ILIKE :pattern {state_clause}
    """, params)

    if not rows or rows[0][1] == 0:
        return None
    return {
        "total_awards": float(rows[0][0] or 0),
        "num_contracts": int(rows[0][1]),
        "max_single": float(rows[0][2] or 0),
    }


def _match_epa(db: Session, name: str, state: Optional[str]) -> Optional[Dict]:
    """EPA ECHO: environmental violations and penalties."""
    pattern = _name_pattern(name)
    params: dict = {"pattern": pattern}
    state_clause = ""
    if state:
        state_clause = "AND state = :state"
        params["state"] = state.upper()

    rows = _safe_query(db, f"""
        SELECT COALESCE(SUM(penalty_amount), 0) as total_penalties,
               COALESCE(SUM(violation_count), 0) as total_violations,
               COUNT(*) as num_facilities,
               SUM(CASE WHEN compliance_status = 'Violation Identified' THEN 1 ELSE 0 END) as active_violations
        FROM epa_echo_facilities
        WHERE facility_name ILIKE :pattern {state_clause}
    """, params)

    if not rows or rows[0][2] == 0:
        return None
    return {
        "total_penalties": float(rows[0][0] or 0),
        "total_violations": int(rows[0][1] or 0),
        "num_facilities": int(rows[0][2]),
        "active_violations": int(rows[0][3] or 0),
    }


def _match_osha(db: Session, name: str, state: Optional[str]) -> Optional[Dict]:
    """OSHA: workplace safety inspections and penalties."""
    pattern = _name_pattern(name)
    params: dict = {"pattern": pattern}
    state_clause = ""
    if state:
        state_clause = "AND site_state = :state"
        params["state"] = state.upper()

    rows = _safe_query(db, f"""
        SELECT COALESCE(SUM(total_current_penalty), 0) as total_penalty,
               COALESCE(SUM(total_violations), 0) as total_violations,
               COALESCE(SUM(violation_type_s), 0) as serious_count,
               COUNT(*) as num_inspections
        FROM osha_inspections
        WHERE establishment_name ILIKE :pattern {state_clause}
    """, params)

    if not rows or rows[0][3] == 0:
        return None
    return {
        "total_penalty": float(rows[0][0] or 0),
        "total_violations": int(rows[0][1] or 0),
        "serious_count": int(rows[0][2] or 0),
        "num_inspections": int(rows[0][3]),
    }


def _match_courtlistener(db: Session, name: str) -> Optional[Dict]:
    """CourtListener: litigation / bankruptcy dockets."""
    pattern = _name_pattern(name)
    rows = _safe_query(db, """
        SELECT COUNT(*) as case_count,
               SUM(CASE WHEN chapter = '11' THEN 1 ELSE 0 END) as ch11_count,
               SUM(CASE WHEN chapter = '7' THEN 1 ELSE 0 END) as ch7_count
        FROM courtlistener_dockets
        WHERE case_name ILIKE :pattern
    """, {"pattern": pattern})

    if not rows or rows[0][0] == 0:
        return None
    return {
        "case_count": int(rows[0][0]),
        "ch11_count": int(rows[0][1] or 0),
        "ch7_count": int(rows[0][2] or 0),
    }


def _match_fdic(db: Session, name: str, state: Optional[str]) -> Optional[Dict]:
    """FDIC: bank financial health (only relevant for financial companies)."""
    pattern = _name_pattern(name)
    params: dict = {"pattern": pattern}
    state_clause = ""
    if state:
        state_clause = "AND i.stalp = :state"
        params["state"] = state.upper()

    rows = _safe_query(db, f"""
        SELECT f.name, f.asset, f.roa, f.nim, f.eq, f.netinc, f.repdte
        FROM fdic_bank_financials f
        JOIN fdic_institutions i ON f.cert = i.cert
        WHERE f.name ILIKE :pattern {state_clause}
        ORDER BY f.repdte DESC
        LIMIT 1
    """, params)

    if not rows:
        return None
    r = rows[0]
    return {
        "name": r[0],
        "total_assets": float(r[1] or 0),
        "roa": float(r[2] or 0),
        "nim": float(r[3] or 0),
        "equity": float(r[4] or 0),
        "net_income": float(r[5] or 0),
        "report_date": str(r[6]),
    }


def _match_uspto(db: Session, name: str) -> Optional[Dict]:
    """USPTO: patent portfolio via assignees table."""
    pattern = _name_pattern(name)
    rows = _safe_query(db, """
        SELECT assignee_name, patent_count,
               first_patent_date, last_patent_date
        FROM uspto_assignees
        WHERE assignee_name ILIKE :pattern
        ORDER BY patent_count DESC
        LIMIT 1
    """, {"pattern": pattern})

    if not rows:
        return None
    r = rows[0]
    return {
        "assignee_name": r[0],
        "patent_count": int(r[1] or 0),
        "first_patent_date": str(r[2]) if r[2] else None,
        "last_patent_date": str(r[3]) if r[3] else None,
    }


def _match_job_postings(db: Session, name: str) -> Optional[Dict]:
    """Job Postings: hiring momentum via industrial_companies linkage."""
    pattern = _name_pattern(name)

    # Find company_id via industrial_companies
    company_rows = _safe_query(db, """
        SELECT id, name FROM industrial_companies
        WHERE name ILIKE :pattern
        ORDER BY id
        LIMIT 1
    """, {"pattern": pattern})

    if not company_rows:
        return None

    company_id = company_rows[0][0]

    # Current open postings
    posting_rows = _safe_query(db, """
        SELECT COUNT(*) as total_open,
               SUM(CASE WHEN seniority_level IN ('vp', 'c_suite', 'director') THEN 1 ELSE 0 END) as senior_hiring
        FROM job_postings
        WHERE company_id = :cid AND status = 'open'
    """, {"cid": company_id})

    total_open = int(posting_rows[0][0]) if posting_rows else 0
    senior_hiring = int(posting_rows[0][1] or 0) if posting_rows else 0

    # Growth trend from snapshots (latest 2 snapshots)
    snap_rows = _safe_query(db, """
        SELECT snapshot_date, total_open
        FROM job_posting_snapshots
        WHERE company_id = :cid
        ORDER BY snapshot_date DESC
        LIMIT 2
    """, {"cid": company_id})

    growth_pct = None
    if len(snap_rows) >= 2 and snap_rows[1][1] and snap_rows[1][1] > 0:
        curr = float(snap_rows[0][1])
        prev = float(snap_rows[1][1])
        growth_pct = ((curr - prev) / prev) * 100

    return {
        "total_open": total_open,
        "senior_hiring": senior_hiring,
        "growth_pct": growth_pct,
        "company_id": company_id,
    }


# ---------------------------------------------------------------------------
# Factor scorers (each returns 0-100, higher = better / lower risk)
# ---------------------------------------------------------------------------

def _score_revenue_concentration(data: Optional[Dict]) -> tuple[int, str, str]:
    """Score government revenue dependency. Lower dependency = higher score."""
    if data is None:
        return 100, "No federal contract history found", "neutral"

    total = data["total_awards"]
    n = data["num_contracts"]

    if total > 100_000_000:
        return 40, f"${total / 1e6:.0f}M in {n} federal contracts — high gov dependency", "warning"
    elif total > 10_000_000:
        return 70, f"${total / 1e6:.1f}M in {n} federal contracts — moderate gov exposure", "neutral"
    elif total > 1_000_000:
        return 85, f"${total / 1e6:.1f}M in {n} federal contracts — limited gov exposure", "neutral"
    else:
        return 95, f"${total / 1e3:.0f}K in {n} federal contracts — minimal", "positive"


def _score_environmental(data: Optional[Dict]) -> tuple[int, str, str]:
    """Score environmental risk from EPA ECHO."""
    if data is None:
        return 100, "No EPA ECHO records found", "neutral"

    penalties = data["total_penalties"]
    violations = data["total_violations"]
    active = data["active_violations"]

    if penalties > 1_000_000:
        score = 20
        reading = f"${penalties / 1e6:.1f}M in EPA penalties, {violations} violations across {data['num_facilities']} facilities — severe"
        impact = "negative"
    elif penalties > 100_000:
        score = 50
        reading = f"${penalties / 1e3:.0f}K in EPA penalties, {violations} violations — elevated"
        impact = "warning"
    elif penalties > 10_000:
        score = 70
        reading = f"${penalties / 1e3:.0f}K in EPA penalties, {violations} violations — moderate"
        impact = "neutral"
    elif penalties > 0:
        score = 90
        reading = f"${penalties:.0f} in EPA penalties, {violations} violations — minor"
        impact = "neutral"
    else:
        score = 95
        reading = f"{data['num_facilities']} EPA-tracked facilities, no penalties"
        impact = "positive"

    if active > 0:
        score = max(score - 10, 0)
        reading += f" ({active} active violations)"

    return score, reading, impact


def _score_safety(data: Optional[Dict]) -> tuple[int, str, str]:
    """Score workplace safety risk from OSHA."""
    if data is None:
        return 100, "No OSHA inspection records found", "neutral"

    penalty = data["total_penalty"]
    serious = data["serious_count"]

    if penalty > 500_000 or serious > 20:
        return 20, f"${penalty / 1e3:.0f}K OSHA penalties, {serious} serious violations — severe", "negative"
    elif penalty > 50_000:
        return 50, f"${penalty / 1e3:.0f}K OSHA penalties, {serious} serious violations — elevated", "warning"
    elif penalty > 5_000:
        return 70, f"${penalty / 1e3:.0f}K OSHA penalties, {serious} serious — moderate", "neutral"
    else:
        return 90, f"${penalty:.0f} OSHA penalties, {data['num_inspections']} inspections — clean", "positive"


def _score_legal(data: Optional[Dict]) -> tuple[int, str, str]:
    """Score legal exposure from CourtListener."""
    if data is None:
        return 100, "No bankruptcy/litigation dockets found", "neutral"

    cases = data["case_count"]
    ch11 = data["ch11_count"]

    if ch11 > 0:
        return 10, f"Chapter 11 bankruptcy filing found + {cases} total dockets — critical", "negative"
    elif cases > 5:
        return 40, f"{cases} court dockets — elevated litigation exposure", "warning"
    elif cases > 2:
        return 60, f"{cases} court dockets — moderate litigation", "neutral"
    elif cases > 0:
        return 80, f"{cases} court docket(s) — minor", "neutral"
    else:
        return 100, "No court dockets found", "positive"


def _score_innovation(data: Optional[Dict]) -> tuple[int, str, str]:
    """Score innovation capacity from USPTO patents."""
    if data is None:
        return 50, "No USPTO patent records found (may not be IP-dependent)", "neutral"

    patents = data["patent_count"]

    if patents >= 100:
        return 95, f"{patents} patents — strong IP portfolio", "positive"
    elif patents >= 50:
        return 85, f"{patents} patents — solid IP position", "positive"
    elif patents >= 10:
        return 70, f"{patents} patents — moderate IP", "neutral"
    elif patents > 0:
        return 55, f"{patents} patent(s) — limited IP", "neutral"
    else:
        return 30, "0 patents — no IP defensibility", "warning"


def _score_growth(data: Optional[Dict]) -> tuple[int, str, str]:
    """Score growth momentum from job postings."""
    if data is None:
        return 50, "No job posting data found", "neutral"

    total_open = data["total_open"]
    growth = data.get("growth_pct")
    senior = data["senior_hiring"]

    if growth is not None:
        if growth > 20:
            score = 95
            reading = f"{total_open} open positions (+{growth:.0f}% growth) — aggressive hiring"
            impact = "positive"
        elif growth > 5:
            score = 80
            reading = f"{total_open} open positions (+{growth:.0f}% growth) — healthy expansion"
            impact = "positive"
        elif growth > -5:
            score = 60
            reading = f"{total_open} open positions ({growth:+.0f}% change) — stable"
            impact = "neutral"
        else:
            score = 40
            reading = f"{total_open} open positions ({growth:.0f}% decline) — contracting"
            impact = "warning"
    else:
        if total_open > 50:
            score = 75
            reading = f"{total_open} open positions — active hiring (no trend data)"
            impact = "positive"
        elif total_open > 10:
            score = 60
            reading = f"{total_open} open positions — moderate hiring"
            impact = "neutral"
        else:
            score = 45
            reading = f"{total_open} open positions — limited hiring"
            impact = "neutral"

    if senior > 3:
        reading += f" ({senior} senior/exec roles — leadership buildout)"

    return score, reading, impact


# ---------------------------------------------------------------------------
# Core scorer
# ---------------------------------------------------------------------------

class CompanyDiligenceScorer:
    """
    Scores a company across 6 diligence factors using 8 public data sources.
    """

    def __init__(self, db: Session):
        self.db = db

    def score_company(
        self,
        company_name: str,
        state: Optional[str] = None,
        naics: Optional[str] = None,
    ) -> CompanyDiligenceScore:
        """Score a single company. Returns composite score with factor breakdown."""

        sources_matched = []
        sources_empty = []
        factors = []
        red_flags = []

        # --- 1. Revenue Concentration (USAspending) ---
        usa_data = _match_usaspending(self.db, company_name, state)
        rev_score, rev_reading, rev_impact = _score_revenue_concentration(usa_data)
        factors.append(DiligenceFactor(
            factor="Revenue concentration",
            score=rev_score, weight=FACTOR_WEIGHTS["revenue_concentration"],
            reading=rev_reading, impact=rev_impact,
            data_source="USAspending",
            details=usa_data or {},
        ))
        if usa_data:
            sources_matched.append("USAspending")
            if rev_score <= 40:
                red_flags.append(f"High government revenue dependency: ${usa_data['total_awards'] / 1e6:.0f}M in federal contracts")
        else:
            sources_empty.append("USAspending")

        # --- 2. Environmental Risk (EPA ECHO) ---
        epa_data = _match_epa(self.db, company_name, state)
        env_score, env_reading, env_impact = _score_environmental(epa_data)
        factors.append(DiligenceFactor(
            factor="Environmental risk",
            score=env_score, weight=FACTOR_WEIGHTS["environmental_risk"],
            reading=env_reading, impact=env_impact,
            data_source="EPA ECHO",
            details=epa_data or {},
        ))
        if epa_data:
            sources_matched.append("EPA ECHO")
            if env_score <= 50:
                red_flags.append(f"Significant EPA penalties: ${epa_data['total_penalties'] / 1e3:.0f}K")
        else:
            sources_empty.append("EPA ECHO")

        # --- 3. Safety Risk (OSHA) ---
        osha_data = _match_osha(self.db, company_name, state)
        safety_score, safety_reading, safety_impact = _score_safety(osha_data)
        factors.append(DiligenceFactor(
            factor="Safety risk",
            score=safety_score, weight=FACTOR_WEIGHTS["safety_risk"],
            reading=safety_reading, impact=safety_impact,
            data_source="OSHA",
            details=osha_data or {},
        ))
        if osha_data:
            sources_matched.append("OSHA")
            if safety_score <= 50:
                red_flags.append(f"OSHA safety concerns: ${osha_data['total_penalty'] / 1e3:.0f}K penalties")
        else:
            sources_empty.append("OSHA")

        # --- 4. Legal Exposure (CourtListener) ---
        legal_data = _match_courtlistener(self.db, company_name)
        legal_score, legal_reading, legal_impact = _score_legal(legal_data)
        factors.append(DiligenceFactor(
            factor="Legal exposure",
            score=legal_score, weight=FACTOR_WEIGHTS["legal_exposure"],
            reading=legal_reading, impact=legal_impact,
            data_source="CourtListener",
            details=legal_data or {},
        ))
        if legal_data:
            sources_matched.append("CourtListener")
            if legal_score <= 40:
                red_flags.append("Bankruptcy or significant litigation on record")
        else:
            sources_empty.append("CourtListener")

        # --- 5. Innovation Capacity (USPTO) ---
        patent_data = _match_uspto(self.db, company_name)
        innov_score, innov_reading, innov_impact = _score_innovation(patent_data)
        factors.append(DiligenceFactor(
            factor="Innovation capacity",
            score=innov_score, weight=FACTOR_WEIGHTS["innovation_capacity"],
            reading=innov_reading, impact=innov_impact,
            data_source="USPTO",
            details=patent_data or {},
        ))
        if patent_data:
            sources_matched.append("USPTO")
        else:
            sources_empty.append("USPTO")

        # --- 6. Growth Momentum (Job Postings) ---
        jobs_data = _match_job_postings(self.db, company_name)
        growth_score, growth_reading, growth_impact = _score_growth(jobs_data)
        factors.append(DiligenceFactor(
            factor="Growth momentum",
            score=growth_score, weight=FACTOR_WEIGHTS["growth_momentum"],
            reading=growth_reading, impact=growth_impact,
            data_source="Job Postings",
            details=jobs_data or {},
        ))
        if jobs_data:
            sources_matched.append("Job Postings")
            if growth_score <= 40:
                red_flags.append("Hiring is contracting — potential operational downturn")
        else:
            sources_empty.append("Job Postings")

        # --- FDIC bonus check (financial sector only) ---
        fdic_data = _match_fdic(self.db, company_name, state)
        if fdic_data:
            sources_matched.append("FDIC")
            roa = fdic_data.get("roa", 0)
            if roa < 0:
                red_flags.append(f"Negative ROA ({roa:.2f}%) — bank profitability concern")

        # --- Composite score ---
        # Weighted average of factor scores
        total_weight = sum(f.weight for f in factors)
        if total_weight > 0:
            composite = sum(f.score * f.weight for f in factors) / total_weight
        else:
            composite = 50

        confidence = len(sources_matched) / max(len(sources_matched) + len(sources_empty), 1)

        # Penalize score when confidence is low — unknown companies shouldn't get high grades
        # At 0% confidence, pull score toward 50 (unknown). At 100%, keep as-is.
        adjusted = composite * confidence + 50 * (1 - confidence)
        score = max(0, min(100, int(round(adjusted))))

        grade = next((g for threshold, g in GRADE_THRESHOLDS if score >= threshold), "D")
        signal = next((s for threshold, s in SIGNAL_THRESHOLDS if score >= threshold), "red")

        return CompanyDiligenceScore(
            company_name=company_name,
            score=score,
            grade=grade,
            signal=signal,
            recommendation=RECOMMENDATIONS[grade],
            factors=factors,
            sources_matched=sources_matched,
            sources_empty=sources_empty,
            confidence=round(confidence, 2),
            red_flags=red_flags,
        )
