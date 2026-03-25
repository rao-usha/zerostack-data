"""
Career Pedigree Scorer.

Scores every person's career quality using existing experience + education data.
No HTTP calls. Pure computation on existing DB rows.
"""

import logging
from typing import List, Optional
from datetime import date
from sqlalchemy.orm import Session

from app.core.people_models import Person, PersonExperience, PersonEducation

logger = logging.getLogger(__name__)

# ── Employer tier lookups (lowercase match) ───────────────────────────────────

TIER_1_CONSULTING = {"mckinsey", "bain", "bcg", "boston consulting", "monitor", "booz"}
TIER_1_FINANCE    = {"goldman sachs", "morgan stanley", "jp morgan", "blackstone", "kkr",
                     "apollo", "carlyle", "warburg pincus", "general atlantic", "tpg",
                     "advent international", "francisco partners", "vista equity",
                     "thoma bravo", "silver lake"}
TIER_1_TECH       = {"google", "apple", "microsoft", "meta", "amazon", "nvidia",
                     "salesforce", "oracle", "sap", "servicenow"}
TIER_2_CONSULTING = {"deloitte", "pwc", "ey", "kpmg", "accenture", "oliver wyman",
                     "roland berger", "a.t. kearney", "strategy&"}
TIER_2_FINANCE    = {"barclays", "credit suisse", "ubs", "lazard", "moelis", "evercore",
                     "jefferies", "houlihan lokey", "piper sandler", "raymond james"}

ELITE_MBA_SCHOOLS = {
    "harvard business school", "wharton", "stanford gsb", "mit sloan",
    "columbia business school", "chicago booth", "kellogg", "tuck",
    "haas", "yale som", "fuqua", "ross", "stern"
}
ELITE_UNDERGRAD   = {"harvard", "yale", "princeton", "mit", "stanford",
                     "columbia", "penn", "dartmouth", "brown", "cornell"}

PE_SPONSOR_KEYWORDS = {
    "portfolio", "holdings", "acquisition", "backed",
    "partners", "capital", "equity",
}

TITLE_VELOCITY_BENCHMARKS = {
    "c_suite":   {"fast": 18, "avg": 25, "slow": 35},
    "president": {"fast": 15, "avg": 22, "slow": 30},
    "evp":       {"fast": 12, "avg": 18, "slow": 25},
    "svp":       {"fast": 10, "avg": 15, "slow": 22},
    "vp":        {"fast":  8, "avg": 12, "slow": 18},
}


class PedigreeScorer:
    """Computes and caches pedigree scores for people in the DB."""

    def score_person(self, person_id: int, db: Session):
        """Score a single person and upsert the result."""
        from app.core.people_models import PersonPedigreeScore
        experience = (
            db.query(PersonExperience)
            .filter(PersonExperience.person_id == person_id)
            .order_by(PersonExperience.start_year.asc().nullslast())
            .all()
        )
        education = (
            db.query(PersonEducation)
            .filter(PersonEducation.person_id == person_id)
            .all()
        )

        if not experience and not education:
            return None

        employer_q  = self._score_employer_quality(experience)
        velocity    = self._score_career_velocity(experience)
        edu_score   = self._score_education(education)
        pe_exp      = self._has_pe_experience(experience)
        exit_exp    = self._has_exit_experience(experience)
        elite_edu   = self._has_elite_education(education)
        tier1       = self._has_tier1_employer(experience)
        top_emp     = self._top_employers(experience)
        mba_school  = self._mba_school(education)

        overall = (
            employer_q  * 0.35 +
            velocity    * 0.25 +
            edu_score   * 0.15 +
            (15 if pe_exp   else 0) +
            (10 if exit_exp else 0)
        )
        overall = round(min(100, max(0, overall)), 1)

        existing = db.query(PersonPedigreeScore).filter_by(person_id=person_id).first()
        if existing:
            row = existing
        else:
            row = PersonPedigreeScore(person_id=person_id)
            db.add(row)

        row.overall_pedigree_score = overall
        row.employer_quality_score = employer_q
        row.career_velocity_score  = velocity
        row.education_score        = edu_score
        row.pe_experience          = pe_exp
        row.exit_experience        = exit_exp
        row.elite_education        = elite_edu
        row.tier1_employer         = tier1
        row.top_employers          = top_emp
        row.mba_school             = mba_school
        row.total_roles            = len(experience)
        row.avg_tenure_months      = self._avg_tenure(experience)

        db.commit()
        db.refresh(row)
        return row

    def score_company(self, company_id: int, db: Session) -> list:
        """Score all current executives at a company."""
        from app.core.people_models import CompanyPerson
        person_ids = [
            r[0] for r in
            db.query(CompanyPerson.person_id)
            .filter(CompanyPerson.company_id == company_id, CompanyPerson.is_current == True)
            .all()
        ]
        results = []
        for pid in person_ids:
            score = self.score_person(pid, db)
            if score:
                results.append(score)
        return results

    def _score_employer_quality(self, exp: list) -> float:
        best = 0
        for e in exp:
            name = (e.company_name or "").lower()
            if any(t in name for t in TIER_1_CONSULTING | TIER_1_FINANCE | TIER_1_TECH):
                best = max(best, 90)
            elif any(t in name for t in TIER_2_CONSULTING | TIER_2_FINANCE):
                best = max(best, 65)
            else:
                best = max(best, 30)
        return float(best)

    def _score_career_velocity(self, exp: list) -> float:
        if not exp:
            return 0
        start_years = [e.start_year for e in exp if e.start_year]
        if not start_years:
            return 50
        career_start = min(start_years)
        c_suite_year = None
        for e in exp:
            title = (e.title_normalized or e.title or "").lower()
            if any(t in title for t in ["ceo", "cfo", "coo", "cto", "cmo", "chief"]):
                if e.start_year:
                    c_suite_year = e.start_year
                    break
        if not c_suite_year:
            return 40
        years_to_csuite = c_suite_year - career_start
        bench = TITLE_VELOCITY_BENCHMARKS["c_suite"]
        if years_to_csuite <= bench["fast"]:
            return 95
        elif years_to_csuite <= bench["avg"]:
            return 70
        elif years_to_csuite <= bench["slow"]:
            return 45
        return 25

    def _score_education(self, edu: list) -> float:
        best = 0
        for e in edu:
            name = (e.institution or "").lower()
            if any(s in name for s in ELITE_MBA_SCHOOLS) and e.degree_type == "masters":
                best = max(best, 95)
            elif any(s in name for s in ELITE_UNDERGRAD):
                best = max(best, 75)
            elif e.degree_type in ("masters", "doctorate"):
                best = max(best, 55)
            elif e.degree_type == "bachelors":
                best = max(best, 35)
        return float(best)

    def _has_pe_experience(self, exp: list) -> bool:
        for e in exp:
            name = (e.company_name or "").lower()
            if any(k in name for k in ["holdings", "acquisition", "portfolio"]):
                return True
        return False

    def _has_exit_experience(self, exp: list) -> bool:
        employers = {e.company_name for e in exp if e.company_name}
        return len(employers) >= 3

    def _has_elite_education(self, edu: list) -> bool:
        for e in edu:
            name = (e.institution or "").lower()
            if any(s in name for s in ELITE_MBA_SCHOOLS | ELITE_UNDERGRAD):
                return True
        return False

    def _has_tier1_employer(self, exp: list) -> bool:
        for e in exp:
            name = (e.company_name or "").lower()
            if any(t in name for t in TIER_1_CONSULTING | TIER_1_FINANCE | TIER_1_TECH):
                return True
        return False

    def _top_employers(self, exp: list) -> list:
        top = []
        for e in exp:
            name = (e.company_name or "").lower()
            if any(t in name for t in TIER_1_CONSULTING | TIER_1_FINANCE | TIER_1_TECH):
                top.append(e.company_name)
        return list(dict.fromkeys(top))[:5]

    def _mba_school(self, edu: list) -> Optional[str]:
        for e in edu:
            name = (e.institution or "").lower()
            if any(s in name for s in ELITE_MBA_SCHOOLS) and e.degree_type == "masters":
                return e.institution
        return None

    def _avg_tenure(self, exp: list) -> Optional[int]:
        tenures = [e.duration_months for e in exp if e.duration_months]
        if not tenures:
            return None
        return round(sum(tenures) / len(tenures))
