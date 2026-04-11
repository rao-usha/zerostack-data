# PLAN 033 — Executive Intelligence Agents (3-Agent Parallel)

**Date:** 2026-03-23
**Status:** Awaiting approval
**Goal:** Add three intelligence layers on top of the existing people collection pipeline:
1. Career Pedigree Scorer — score every exec's quality from existing data (no new HTTP collection)
2. Board Interlock Agent — map board co-directorships and compute relationship graph
3. SEC Proxy Comp Agent — parse DEF 14A compensation tables and Form 4 insider transactions

**Master agent:** merges all `people_models.py` additions, registers new routers in `main.py`, restarts API, verifies endpoints, commits.

---

## What Already Exists (Do NOT Rebuild)

| Asset | Location |
|---|---|
| `Person`, `CompanyPerson`, `PersonExperience`, `PersonEducation` models | `app/core/people_models.py` |
| `SECAgent` + `FilingFetcher` + `SECParser` | `app/sources/people_collection/sec_agent.py` |
| `BaseCollector` with rate limiting + retries | `app/sources/people_collection/base_collector.py` |
| `CompanyPerson.base_salary_usd`, `total_compensation_usd`, `equity_awards_usd` | Already in schema — never populated |
| `LLMClient` async interface | `app/agentic/llm_client.py` |
| People analytics router | `app/api/v1/people_analytics.py` |
| People router | `app/api/v1/people.py` |
| Router registration | `app/main.py` |

---

## New DB Tables (summary)

All three agents add tables to `app/core/people_models.py`, appended after the existing `PeopleWatchlistPerson` class (~line 600+). Master agent owns this merge.

| Table | Owner | Purpose |
|---|---|---|
| `person_pedigree_scores` | Agent 1 | Cached pedigree score per person |
| `board_seats` | Agent 2 | Every board seat per person (current + past) |
| `board_interlocks` | Agent 2 | Computed co-director pairings |
| `insider_transactions` | Agent 3 | Form 4 buy/sell activity |

Compensation columns (`base_salary_usd` etc.) already exist in `company_people` — Agent 3 populates them.

---

## Agent 1: Career Pedigree Scorer

**Worktree files:** `app/services/pedigree_scorer.py` (new), `app/api/v1/people_analytics.py` (add endpoints)
**No new HTTP collection — pure computation from existing `people_experience` + `people_education` data.**

### Step 1 — New table in `people_models.py`

Append after existing classes:

```python
class PersonPedigreeScore(Base):
    """Cached career pedigree score per person. Recomputed on demand or nightly."""
    __tablename__ = "person_pedigree_scores"

    id = Column(Integer, primary_key=True, autoincrement=True)
    person_id = Column(Integer, ForeignKey("people.id"), nullable=False, index=True)

    # Composite score
    overall_pedigree_score = Column(Numeric(5, 1))   # 0-100

    # Component scores
    employer_quality_score = Column(Numeric(5, 1))   # 0-100: based on employer tier
    career_velocity_score  = Column(Numeric(5, 1))   # 0-100: years-to-level vs. benchmark
    education_score        = Column(Numeric(5, 1))   # 0-100: school tier + degree type

    # Boolean flags (most useful for filtering)
    pe_experience   = Column(Boolean, default=False)  # worked in PE-backed company
    exit_experience = Column(Boolean, default=False)  # at company through M&A close
    elite_education = Column(Boolean, default=False)  # top-10 MBA or equivalent
    tier1_employer  = Column(Boolean, default=False)  # McKinsey/Goldman/FAANG tier

    # Detail
    mba_school     = Column(String(200))   # "Harvard Business School" if applicable
    top_employers  = Column(JSON)          # ["McKinsey", "Goldman Sachs"]
    pe_employers   = Column(JSON)          # ["Blackstone Portfolio Co", ...]
    total_roles    = Column(Integer)
    avg_tenure_months = Column(Integer)

    # Housekeeping
    scored_at  = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("person_id", name="uq_pedigree_person"),
        Index("ix_pedigree_score", "overall_pedigree_score"),
        Index("ix_pedigree_flags", "pe_experience", "exit_experience", "tier1_employer"),
    )
```

### Step 2 — New service `app/services/pedigree_scorer.py`

```python
"""
Career Pedigree Scorer.

Scores every person's career quality using existing experience + education data.
No HTTP calls. Pure computation on existing DB rows.
"""

import logging
from typing import List, Optional
from datetime import date
from sqlalchemy.orm import Session

from app.core.people_models import Person, PersonExperience, PersonEducation, PersonPedigreeScore

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

# Keywords that suggest a company was PE-backed (in company name / description)
PE_SPONSOR_KEYWORDS = {
    "portfolio", "holdings", "acquisition", "backed",
    "partners", "capital", "equity",   # weak signal alone — only count with context
}

# Title levels for velocity scoring (years-to-reach benchmarks)
TITLE_VELOCITY_BENCHMARKS = {
    "c_suite":   {"fast": 18, "avg": 25, "slow": 35},   # years from career start
    "president": {"fast": 15, "avg": 22, "slow": 30},
    "evp":       {"fast": 12, "avg": 18, "slow": 25},
    "svp":       {"fast": 10, "avg": 15, "slow": 22},
    "vp":        {"fast":  8, "avg": 12, "slow": 18},
}


class PedigreeScorer:
    """Computes and caches pedigree scores for people in the DB."""

    def score_person(self, person_id: int, db: Session) -> Optional[PersonPedigreeScore]:
        """Score a single person and upsert the result."""
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

        # Weighted composite
        overall = (
            employer_q  * 0.35 +
            velocity    * 0.25 +
            edu_score   * 0.15 +
            (15 if pe_exp   else 0) +
            (10 if exit_exp else 0)
        )
        overall = round(min(100, max(0, overall)), 1)

        # Upsert
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

    def score_company(self, company_id: int, db: Session) -> List[PersonPedigreeScore]:
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

    # ── Private helpers ──────────────────────────────────────────────────────

    def _score_employer_quality(self, exp: List[PersonExperience]) -> float:
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

    def _score_career_velocity(self, exp: List[PersonExperience]) -> float:
        """Years from first job to C-suite (or current highest level). Faster = higher score."""
        if not exp:
            return 0
        start_years = [e.start_year for e in exp if e.start_year]
        if not start_years:
            return 50  # unknown — neutral
        career_start = min(start_years)
        c_suite_year = None
        for e in exp:
            title = (e.title_normalized or e.title or "").lower()
            if any(t in title for t in ["ceo", "cfo", "coo", "cto", "cmo", "chief"]):
                if e.start_year:
                    c_suite_year = e.start_year
                    break
        if not c_suite_year:
            return 40  # never reached C-suite in our data
        years_to_csuite = c_suite_year - career_start
        bench = TITLE_VELOCITY_BENCHMARKS["c_suite"]
        if years_to_csuite <= bench["fast"]:
            return 95
        elif years_to_csuite <= bench["avg"]:
            return 70
        elif years_to_csuite <= bench["slow"]:
            return 45
        return 25

    def _score_education(self, edu: List[PersonEducation]) -> float:
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

    def _has_pe_experience(self, exp: List[PersonExperience]) -> bool:
        for e in exp:
            name = (e.company_name or "").lower()
            if any(k in name for k in ["holdings", "acquisition", "portfolio"]):
                return True
        return False

    def _has_exit_experience(self, exp: List[PersonExperience]) -> bool:
        # Conservative proxy: multiple employers suggests they've seen transitions
        employers = {e.company_name for e in exp if e.company_name}
        return len(employers) >= 3  # refine later with actual M&A event data

    def _has_elite_education(self, edu: List[PersonEducation]) -> bool:
        for e in edu:
            name = (e.institution or "").lower()
            if any(s in name for s in ELITE_MBA_SCHOOLS | ELITE_UNDERGRAD):
                return True
        return False

    def _has_tier1_employer(self, exp: List[PersonExperience]) -> bool:
        for e in exp:
            name = (e.company_name or "").lower()
            if any(t in name for t in TIER_1_CONSULTING | TIER_1_FINANCE | TIER_1_TECH):
                return True
        return False

    def _top_employers(self, exp: List[PersonExperience]) -> List[str]:
        top = []
        for e in exp:
            name = (e.company_name or "").lower()
            if any(t in name for t in TIER_1_CONSULTING | TIER_1_FINANCE | TIER_1_TECH):
                top.append(e.company_name)
        return list(dict.fromkeys(top))[:5]  # deduplicate, keep order, max 5

    def _mba_school(self, edu: List[PersonEducation]) -> Optional[str]:
        for e in edu:
            name = (e.institution or "").lower()
            if any(s in name for s in ELITE_MBA_SCHOOLS) and e.degree_type == "masters":
                return e.institution
        return None

    def _avg_tenure(self, exp: List[PersonExperience]) -> Optional[int]:
        tenures = [e.duration_months for e in exp if e.duration_months]
        if not tenures:
            return None
        return round(sum(tenures) / len(tenures))
```

### Step 3 — New endpoints in `app/api/v1/people_analytics.py`

Add these three endpoints at the bottom of the file (after the existing `get_portfolio_analytics` endpoint):

```python
from app.services.pedigree_scorer import PedigreeScorer

@router.post("/companies/{company_id}/score-pedigree")
async def score_company_pedigree(
    company_id: int,
    db: Session = Depends(get_db),
):
    """
    Compute and cache pedigree scores for all current executives at a company.
    Returns scored count and team summary.
    """
    scorer = PedigreeScorer()
    scores = scorer.score_company(company_id, db)
    if not scores:
        raise HTTPException(status_code=404, detail="No executives found for company")
    return {
        "company_id": company_id,
        "scored": len(scores),
        "team_avg_pedigree": round(sum(s.overall_pedigree_score for s in scores) / len(scores), 1),
        "pe_experienced": sum(1 for s in scores if s.pe_experience),
        "tier1_employers": sum(1 for s in scores if s.tier1_employer),
        "elite_educated": sum(1 for s in scores if s.elite_education),
        "members": [
            {
                "person_id": s.person_id,
                "overall": s.overall_pedigree_score,
                "pe_experience": s.pe_experience,
                "exit_experience": s.exit_experience,
                "tier1_employer": s.tier1_employer,
                "elite_education": s.elite_education,
                "top_employers": s.top_employers,
                "mba_school": s.mba_school,
            }
            for s in sorted(scores, key=lambda x: x.overall_pedigree_score or 0, reverse=True)
        ],
    }


@router.get("/companies/{company_id}/pedigree-report")
async def get_company_pedigree_report(
    company_id: int,
    db: Session = Depends(get_db),
):
    """
    Return cached pedigree scores for a company's leadership team.
    Call POST /score-pedigree first to compute fresh scores.
    """
    from app.core.people_models import PersonPedigreeScore, CompanyPerson, Person
    rows = (
        db.query(PersonPedigreeScore, Person)
        .join(Person, PersonPedigreeScore.person_id == Person.id)
        .join(CompanyPerson, CompanyPerson.person_id == Person.id)
        .filter(
            CompanyPerson.company_id == company_id,
            CompanyPerson.is_current == True,
        )
        .order_by(PersonPedigreeScore.overall_pedigree_score.desc().nullslast())
        .all()
    )
    if not rows:
        raise HTTPException(status_code=404, detail="No pedigree data — run POST /score-pedigree first")
    scores = [r[0] for r in rows]
    people = [r[1] for r in rows]
    return {
        "company_id": company_id,
        "scored_count": len(scores),
        "team_avg_pedigree": round(sum(s.overall_pedigree_score or 0 for s in scores) / len(scores), 1),
        "flags": {
            "pe_experienced_pct": round(sum(1 for s in scores if s.pe_experience) / len(scores) * 100),
            "tier1_employer_pct": round(sum(1 for s in scores if s.tier1_employer) / len(scores) * 100),
            "elite_education_pct": round(sum(1 for s in scores if s.elite_education) / len(scores) * 100),
        },
        "members": [
            {
                "person_id": s.person_id,
                "full_name": p.full_name,
                "overall_pedigree_score": s.overall_pedigree_score,
                "employer_quality_score": s.employer_quality_score,
                "career_velocity_score": s.career_velocity_score,
                "pe_experience": s.pe_experience,
                "exit_experience": s.exit_experience,
                "tier1_employer": s.tier1_employer,
                "elite_education": s.elite_education,
                "top_employers": s.top_employers or [],
                "mba_school": s.mba_school,
                "avg_tenure_months": s.avg_tenure_months,
                "scored_at": s.scored_at.isoformat() if s.scored_at else None,
            }
            for s, p in zip(scores, people)
        ],
    }
```

Also add `GET /people/{person_id}/pedigree` to `app/api/v1/people.py` at the bottom:

```python
from app.services.pedigree_scorer import PedigreeScorer

@router.get("/{person_id}/pedigree")
async def get_person_pedigree(
    person_id: int,
    recompute: bool = Query(False, description="Force recompute even if cached"),
    db: Session = Depends(get_db),
):
    """Return pedigree score for a person, optionally recomputing."""
    from app.core.people_models import PersonPedigreeScore
    person = db.query(Person).filter(Person.id == person_id).first()
    if not person:
        raise HTTPException(status_code=404, detail="Person not found")
    if recompute:
        scorer = PedigreeScorer()
        score = scorer.score_person(person_id, db)
    else:
        score = db.query(PersonPedigreeScore).filter_by(person_id=person_id).first()
        if not score:
            scorer = PedigreeScorer()
            score = scorer.score_person(person_id, db)
    if not score:
        raise HTTPException(status_code=404, detail="Insufficient data to score this person")
    return {
        "person_id": person_id,
        "full_name": person.full_name,
        "overall_pedigree_score": score.overall_pedigree_score,
        "employer_quality_score": score.employer_quality_score,
        "career_velocity_score": score.career_velocity_score,
        "education_score": score.education_score,
        "pe_experience": score.pe_experience,
        "exit_experience": score.exit_experience,
        "tier1_employer": score.tier1_employer,
        "elite_education": score.elite_education,
        "top_employers": score.top_employers or [],
        "mba_school": score.mba_school,
        "avg_tenure_months": score.avg_tenure_months,
        "scored_at": score.scored_at.isoformat() if score.scored_at else None,
    }
```

---

## Agent 2: Board Interlock Agent

**Worktree files:** `app/sources/people_collection/board_agent.py` (new), `app/services/board_interlock_service.py` (new), `app/api/v1/board_interlocks.py` (new)

### Step 1 — New tables in `people_models.py`

```python
class BoardSeat(Base):
    """
    Every board seat held by a person — current and historical.
    Sourced from SEC DEF 14A 'Other Directorships' section and company websites.
    """
    __tablename__ = "board_seats"

    id = Column(Integer, primary_key=True, autoincrement=True)
    person_id   = Column(Integer, ForeignKey("people.id"), nullable=False, index=True)

    # Company
    company_name = Column(String(500), nullable=False)
    company_id   = Column(Integer, ForeignKey("industrial_companies.id"))  # if in our DB
    company_type = Column(String(50))   # public, private, pe_backed, nonprofit, advisory
    ticker       = Column(String(20))   # if public

    # Role
    role        = Column(String(200))   # "Independent Director", "Lead Director"
    committee   = Column(String(500))   # "Audit Committee Chair, Compensation Committee"
    is_chair    = Column(Boolean, default=False)

    # Tenure
    start_date  = Column(Date)
    end_date    = Column(Date)
    is_current  = Column(Boolean, default=True, index=True)

    # Source
    source      = Column(String(100))   # sec_proxy, website, press_release, linkedin
    source_url  = Column(String(500))
    scraped_at  = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("person_id", "company_name", "is_current", name="uq_board_seat"),
        Index("ix_board_seat_person", "person_id", "is_current"),
        Index("ix_board_seat_company", "company_name"),
    )


class BoardInterlock(Base):
    """
    Computed co-director pairings. Person A and Person B both sit on the same board.
    Recomputed whenever BoardSeat data changes.
    """
    __tablename__ = "board_interlocks"

    id             = Column(Integer, primary_key=True, autoincrement=True)
    person_id_a    = Column(Integer, ForeignKey("people.id"), nullable=False)
    person_id_b    = Column(Integer, ForeignKey("people.id"), nullable=False)
    shared_company = Column(String(500), nullable=False)
    company_id     = Column(Integer, ForeignKey("industrial_companies.id"))
    overlap_start  = Column(Date)
    overlap_end    = Column(Date)
    is_current     = Column(Boolean, default=True, index=True)
    computed_at    = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("person_id_a", "person_id_b", "shared_company", name="uq_interlock"),
        Index("ix_interlock_a", "person_id_a", "is_current"),
        Index("ix_interlock_b", "person_id_b", "is_current"),
    )
```

### Step 2 — New collector `app/sources/people_collection/board_agent.py`

```python
"""
Board Seat Collection Agent.

Collects board directorships from two sources:
1. SEC DEF 14A — "Other Public Company Directorships" table (already fetched by FilingFetcher)
2. Company website "Board of Directors" pages (distinct from leadership pages)

Uses existing FilingFetcher + LLMClient infrastructure.
"""

import logging
import re
from typing import List, Optional
from datetime import datetime

from app.sources.people_collection.base_collector import BaseCollector
from app.sources.people_collection.filing_fetcher import FilingFetcher
from app.agentic.llm_client import LLMClient

logger = logging.getLogger(__name__)

BOARD_EXTRACTION_PROMPT = """
Extract board of directors information from this text. Return a JSON array where each element has:
{
  "person_name": "Full Name",
  "role": "Independent Director" (or specific role),
  "committee": "Audit Committee" (comma-separated if multiple, null if none),
  "is_chair": false,
  "other_directorships": ["Company A", "Company B"]  (from "Other Public Directorships" section)
}
Only include actual board members. Exclude executives unless they also have a board seat.
"""

PROXY_OTHER_DIRECTORSHIPS_PROMPT = """
From this SEC proxy filing text, extract the "Other Directorships" or "Other Public Company Directorships"
table. Return JSON array:
[
  {
    "director_name": "Full Name",
    "other_companies": [
      {"company_name": "XYZ Corp", "role": "Director", "since_year": 2019}
    ]
  }
]
Return empty array if this section is not present.
"""


class BoardAgent(BaseCollector):
    """Collects board seat data from SEC proxies and company websites."""

    def __init__(self):
        super().__init__(source_type="sec_edgar")
        self.fetcher = FilingFetcher()
        self.llm     = LLMClient()

    async def close(self):
        await super().close()
        await self.fetcher.close()

    async def collect_from_proxy(self, cik: str, company_name: str) -> List[dict]:
        """
        Parse DEF 14A for board members + their other directorships.
        Returns list of dicts: {person_name, role, committee, other_companies: [...]}
        """
        filings = await self.fetcher.get_filings(cik=cik, form_type="DEF 14A", limit=1)
        if not filings:
            logger.info(f"No DEF 14A found for {company_name} (CIK {cik})")
            return []

        accession = filings[0].get("accession_number")
        text = await self.fetcher.get_filing_text(accession)
        if not text:
            return []

        # Extract board section (typically 5-30 pages into the proxy)
        board_section = self._extract_board_section(text)
        if not board_section:
            return []

        # LLM extraction for board + other directorships
        try:
            response = await self.llm.complete(
                prompt=f"{PROXY_OTHER_DIRECTORSHIPS_PROMPT}\n\n---\n\n{board_section[:8000]}"
            )
            data = response.parse_json()
            return data if isinstance(data, list) else []
        except Exception as e:
            logger.error(f"LLM extraction failed for {company_name}: {e}")
            return []

    async def collect_from_website(self, company_id: int, board_url: str) -> List[dict]:
        """
        Scrape company board page for director listings.
        Returns list of dicts: {person_name, role, committee}
        """
        try:
            html = await self.fetch(board_url)
        except Exception as e:
            logger.warning(f"Failed to fetch board page {board_url}: {e}")
            return []

        from app.sources.people_collection.html_cleaner import HTMLCleaner
        text = HTMLCleaner().clean(html)

        try:
            response = await self.llm.complete(
                prompt=f"{BOARD_EXTRACTION_PROMPT}\n\n---\n\n{text[:6000]}"
            )
            data = response.parse_json()
            return data if isinstance(data, list) else []
        except Exception as e:
            logger.error(f"Board page extraction failed for {board_url}: {e}")
            return []

    def _extract_board_section(self, text: str) -> Optional[str]:
        """Find the director/board section in a proxy filing."""
        markers = [
            "other directorships", "other public company directorships",
            "director qualifications", "information about our directors",
            "nominees for director"
        ]
        text_lower = text.lower()
        for marker in markers:
            idx = text_lower.find(marker)
            if idx != -1:
                return text[max(0, idx - 200): idx + 12000]
        return None
```

### Step 3 — New service `app/services/board_interlock_service.py`

```python
"""
Board Interlock Service.

Computes pairwise co-director relationships from BoardSeat data.
"""

import logging
from typing import List, Dict
from itertools import combinations
from sqlalchemy.orm import Session

from app.core.people_models import BoardSeat, BoardInterlock, Person

logger = logging.getLogger(__name__)


class BoardInterlockService:

    def compute_interlocks_for_company(self, company_name: str, db: Session) -> int:
        """
        For all current board members of company_name, find their other board seats
        and create BoardInterlock records for each pair.
        Returns count of interlocks created/updated.
        """
        # Get all current board members for this company
        seats = (
            db.query(BoardSeat)
            .filter(BoardSeat.company_name == company_name, BoardSeat.is_current == True)
            .all()
        )
        if len(seats) < 2:
            return 0

        person_ids = [s.person_id for s in seats]
        count = 0

        # For each director, find ALL their other current board seats
        for pid_a, pid_b in combinations(person_ids, 2):
            # Find all companies where both serve (current)
            seats_a = {s.company_name for s in
                       db.query(BoardSeat).filter(BoardSeat.person_id == pid_a, BoardSeat.is_current == True).all()}
            seats_b = {s.company_name for s in
                       db.query(BoardSeat).filter(BoardSeat.person_id == pid_b, BoardSeat.is_current == True).all()}
            shared = seats_a & seats_b

            for shared_co in shared:
                existing = (
                    db.query(BoardInterlock)
                    .filter(
                        BoardInterlock.person_id_a == min(pid_a, pid_b),
                        BoardInterlock.person_id_b == max(pid_a, pid_b),
                        BoardInterlock.shared_company == shared_co,
                    )
                    .first()
                )
                if not existing:
                    db.add(BoardInterlock(
                        person_id_a=min(pid_a, pid_b),
                        person_id_b=max(pid_a, pid_b),
                        shared_company=shared_co,
                        is_current=True,
                    ))
                    count += 1

        db.commit()
        return count

    def get_network_graph(self, company_id: int, db: Session) -> dict:
        """
        Return nodes + edges for the board network centered on a company.
        Nodes = people, edges = shared board seats.
        """
        from app.core.people_models import CompanyPerson
        # Get board members of this company
        board_pids = [
            r[0] for r in
            db.query(BoardSeat.person_id)
            .filter(BoardSeat.company_id == company_id, BoardSeat.is_current == True)
            .all()
        ]
        if not board_pids:
            return {"nodes": [], "edges": [], "stats": {"total_nodes": 0, "total_edges": 0}}

        # Get all interlocks involving these people
        from sqlalchemy import or_
        interlocks = (
            db.query(BoardInterlock)
            .filter(
                or_(
                    BoardInterlock.person_id_a.in_(board_pids),
                    BoardInterlock.person_id_b.in_(board_pids),
                ),
                BoardInterlock.is_current == True,
            )
            .all()
        )

        # Build node set
        all_pids = set(board_pids)
        for il in interlocks:
            all_pids.add(il.person_id_a)
            all_pids.add(il.person_id_b)

        people = {p.id: p for p in db.query(Person).filter(Person.id.in_(all_pids)).all()}
        nodes = [
            {"id": pid, "name": people[pid].full_name if pid in people else f"Person {pid}",
             "is_center_board": pid in board_pids}
            for pid in all_pids
        ]
        edges = [
            {"source": il.person_id_a, "target": il.person_id_b,
             "shared_company": il.shared_company}
            for il in interlocks
        ]
        return {
            "nodes": nodes,
            "edges": edges,
            "stats": {"total_nodes": len(nodes), "total_edges": len(edges)},
        }
```

### Step 4 — New API router `app/api/v1/board_interlocks.py`

```python
"""
Board Interlock API endpoints.
"""

from typing import Optional, List
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from pydantic import BaseModel

from app.core.database import get_db

router = APIRouter(prefix="/board-interlocks", tags=["Board Interlocks"])


@router.get("/person/{person_id}/seats")
async def get_person_board_seats(
    person_id: int,
    current_only: bool = Query(True),
    db: Session = Depends(get_db),
):
    """All board seats held by a person."""
    from app.core.people_models import BoardSeat, Person
    person = db.query(Person).filter(Person.id == person_id).first()
    if not person:
        raise HTTPException(status_code=404, detail="Person not found")
    q = db.query(BoardSeat).filter(BoardSeat.person_id == person_id)
    if current_only:
        q = q.filter(BoardSeat.is_current == True)
    seats = q.order_by(BoardSeat.start_date.desc().nullslast()).all()
    return {
        "person_id": person_id,
        "full_name": person.full_name,
        "board_seats": [
            {"company_name": s.company_name, "company_type": s.company_type,
             "role": s.role, "committee": s.committee, "is_chair": s.is_chair,
             "start_date": s.start_date.isoformat() if s.start_date else None,
             "end_date": s.end_date.isoformat() if s.end_date else None,
             "is_current": s.is_current}
            for s in seats
        ],
    }


@router.get("/person/{person_id}/co-directors")
async def get_co_directors(
    person_id: int,
    db: Session = Depends(get_db),
):
    """People who currently sit on any board with this person."""
    from app.core.people_models import BoardInterlock, Person
    from sqlalchemy import or_
    interlocks = (
        db.query(BoardInterlock)
        .filter(
            or_(BoardInterlock.person_id_a == person_id, BoardInterlock.person_id_b == person_id),
            BoardInterlock.is_current == True,
        )
        .all()
    )
    if not interlocks:
        return {"person_id": person_id, "co_directors": [], "shared_boards": 0}

    co_ids = {(il.person_id_b if il.person_id_a == person_id else il.person_id_a): il.shared_company
              for il in interlocks}
    people = {p.id: p for p in db.query(Person).filter(Person.id.in_(co_ids.keys())).all()}
    return {
        "person_id": person_id,
        "co_directors": [
            {"person_id": pid, "full_name": people[pid].full_name if pid in people else None,
             "shared_board": company}
            for pid, company in co_ids.items()
        ],
        "shared_boards": len(interlocks),
    }


@router.get("/company/{company_id}/network")
async def get_company_board_network(
    company_id: int,
    db: Session = Depends(get_db),
):
    """Board network graph for a company — nodes (directors) and edges (co-directorships)."""
    from app.services.board_interlock_service import BoardInterlockService
    service = BoardInterlockService()
    return service.get_network_graph(company_id, db)


@router.post("/compute/{company_id}")
async def compute_interlocks(
    company_id: int,
    db: Session = Depends(get_db),
):
    """Trigger interlock computation for a company's board members."""
    from app.core.people_models import IndustrialCompany
    from app.services.board_interlock_service import BoardInterlockService
    company = db.query(IndustrialCompany).filter(IndustrialCompany.id == company_id).first()
    if not company:
        raise HTTPException(status_code=404, detail="Company not found")
    service = BoardInterlockService()
    count = service.compute_interlocks_for_company(company.name, db)
    return {"company_id": company_id, "interlocks_computed": count}


@router.post("/collect/{company_id}")
async def collect_board_seats(
    company_id: int,
    db: Session = Depends(get_db),
):
    """
    Run BoardAgent to collect board seat data for a company via SEC DEF 14A.
    Requires company to have a CIK set in industrial_companies.
    """
    from app.core.people_models import IndustrialCompany, BoardSeat, Person
    from app.sources.people_collection.board_agent import BoardAgent
    from app.sources.people_collection.person_matcher import PersonMatcher

    company = db.query(IndustrialCompany).filter(IndustrialCompany.id == company_id).first()
    if not company:
        raise HTTPException(status_code=404, detail="Company not found")
    if not company.cik:
        raise HTTPException(status_code=400, detail="Company has no CIK — cannot fetch SEC proxy")

    agent = BoardAgent()
    try:
        results = await agent.collect_from_proxy(cik=company.cik, company_name=company.name)
    finally:
        await agent.close()

    created = 0
    for item in results:
        # Match to person record
        person_name = item.get("director_name") or item.get("person_name")
        if not person_name:
            continue
        # Try to find existing person
        person = db.query(Person).filter(Person.full_name.ilike(f"%{person_name}%")).first()
        person_id = person.id if person else None

        # Upsert board seat for THIS company
        seat = db.query(BoardSeat).filter(
            BoardSeat.company_name == company.name,
            BoardSeat.person_id == person_id if person_id else False,
        ).first() if person_id else None

        if not seat:
            seat = BoardSeat(
                person_id=person_id,
                company_name=company.name,
                company_id=company_id,
                role=item.get("role"),
                committee=item.get("committee"),
                is_current=True,
                source="sec_proxy",
            )
            db.add(seat)
            created += 1

        # Store other directorships as additional BoardSeat rows
        for other in item.get("other_companies", []):
            other_name = other.get("company_name")
            if not other_name or not person_id:
                continue
            existing_other = db.query(BoardSeat).filter(
                BoardSeat.person_id == person_id,
                BoardSeat.company_name == other_name,
                BoardSeat.is_current == True,
            ).first()
            if not existing_other:
                db.add(BoardSeat(
                    person_id=person_id,
                    company_name=other_name,
                    company_type="public",
                    role=other.get("role", "Director"),
                    is_current=True,
                    source="sec_proxy",
                ))
                created += 1

    db.commit()
    return {"company_id": company_id, "seats_created": created, "directors_found": len(results)}
```

### Step 5 — Register router in `app/main.py`

Find the section where other people routers are registered and add:

```python
from app.api.v1 import board_interlocks
app.include_router(board_interlocks.router, prefix="/api/v1")
```

---

## Agent 3: SEC Proxy Compensation Agent

**Worktree files:** `app/sources/people_collection/proxy_comp_agent.py` (new), `app/api/v1/people.py` (add endpoints)
**Populates existing `base_salary_usd`, `total_compensation_usd`, `equity_awards_usd` columns in `company_people`.**

### Step 1 — New table in `people_models.py`

```python
class InsiderTransaction(Base):
    """
    SEC Form 4 insider buy/sell activity per person per company.
    Key signal: heavy selling before a planned exit is a retention/alignment red flag.
    """
    __tablename__ = "insider_transactions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    person_id    = Column(Integer, ForeignKey("people.id"), index=True)
    company_id   = Column(Integer, ForeignKey("industrial_companies.id"), index=True)
    company_name = Column(String(500))
    ticker       = Column(String(20))

    # Transaction
    transaction_date  = Column(Date, nullable=False, index=True)
    transaction_type  = Column(String(50))   # buy, sell, option_exercise, gift, grant
    shares            = Column(Integer)
    price_per_share   = Column(Numeric(10, 2))
    total_value_usd   = Column(Numeric(15, 2))
    shares_owned_after = Column(Integer)

    # Context
    is_10b5_plan  = Column(Boolean, default=False)  # pre-planned trading (less signal)
    form4_url     = Column(String(500))
    filed_at      = Column(Date)

    created_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        Index("ix_insider_person_date", "person_id", "transaction_date"),
        Index("ix_insider_company_date", "company_id", "transaction_date"),
    )
```

### Step 2 — New agent `app/sources/people_collection/proxy_comp_agent.py`

```python
"""
SEC Proxy Compensation Agent.

Parses the Summary Compensation Table from DEF 14A filings and populates:
- company_people.base_salary_usd
- company_people.total_compensation_usd
- company_people.equity_awards_usd
- company_people.compensation_year

Also parses Form 4 filings and creates InsiderTransaction records.
Uses existing FilingFetcher (already fetches DEF 14A for SECAgent).
"""

import logging
import re
from typing import List, Optional
from datetime import date

from app.sources.people_collection.base_collector import BaseCollector
from app.sources.people_collection.filing_fetcher import FilingFetcher
from app.agentic.llm_client import LLMClient

logger = logging.getLogger(__name__)

COMP_TABLE_PROMPT = """
Extract the Summary Compensation Table from this SEC proxy filing excerpt.
Return a JSON array where each element is one executive row:
{
  "name": "Full Name",
  "title": "Chief Executive Officer",
  "year": 2024,
  "salary_usd": 850000,
  "bonus_usd": 200000,
  "stock_awards_usd": 1500000,
  "option_awards_usd": 500000,
  "non_equity_incentive_usd": 300000,
  "total_comp_usd": 3350000
}
Use null for missing fields. Only include named executives from the table — do not invent data.
"""

FORM4_PROMPT = """
Extract insider transactions from this SEC Form 4 filing.
Return JSON:
{
  "reporting_person": "Full Name",
  "transactions": [
    {
      "transaction_date": "2024-03-15",
      "transaction_type": "sell",
      "shares": 10000,
      "price_per_share": 42.50,
      "total_value_usd": 425000,
      "shares_owned_after": 85000,
      "is_10b5_plan": false
    }
  ]
}
transaction_type: buy, sell, option_exercise, gift, grant
"""


class ProxyCompAgent(BaseCollector):
    """Parses DEF 14A comp tables and Form 4 filings."""

    def __init__(self):
        super().__init__(source_type="sec_edgar")
        self.fetcher = FilingFetcher()
        self.llm     = LLMClient()

    async def close(self):
        await super().close()
        await self.fetcher.close()

    async def collect_comp(self, company_id: int, cik: str, company_name: str,
                           db) -> dict:
        """Parse DEF 14A and update company_people compensation fields."""
        from app.core.people_models import CompanyPerson, Person

        filings = await self.fetcher.get_filings(cik=cik, form_type="DEF 14A", limit=3)
        if not filings:
            return {"status": "no_filings", "company_id": company_id}

        updated = 0
        for filing in filings:
            text = await self.fetcher.get_filing_text(filing["accession_number"])
            if not text:
                continue

            comp_section = self._extract_comp_section(text)
            if not comp_section:
                continue

            try:
                response = await self.llm.complete(
                    prompt=f"{COMP_TABLE_PROMPT}\n\n---\n\n{comp_section[:8000]}"
                )
                comp_rows = response.parse_json()
            except Exception as e:
                logger.error(f"Comp extraction failed: {e}")
                continue

            for row in (comp_rows or []):
                name = row.get("name", "")
                year = row.get("year")
                if not name or not year:
                    continue
                # Find matching person at this company
                cp = (
                    db.query(CompanyPerson)
                    .join(Person, CompanyPerson.person_id == Person.id)
                    .filter(
                        CompanyPerson.company_id == company_id,
                        CompanyPerson.is_current == True,
                        Person.full_name.ilike(f"%{name.split()[-1]}%"),  # last name match
                    )
                    .first()
                )
                if cp:
                    cp.base_salary_usd       = row.get("salary_usd")
                    cp.total_compensation_usd = row.get("total_comp_usd")
                    cp.equity_awards_usd      = (row.get("stock_awards_usd") or 0) + (row.get("option_awards_usd") or 0)
                    cp.compensation_year      = year
                    updated += 1

            db.commit()
            if updated > 0:
                break  # stop after first filing with comp data

        return {"status": "ok", "company_id": company_id, "executives_updated": updated}

    async def collect_form4(self, company_id: int, cik: str, company_name: str,
                            db, days_back: int = 730) -> dict:
        """Fetch and parse Form 4 filings, store InsiderTransaction records."""
        from app.core.people_models import InsiderTransaction, Person
        from datetime import datetime, timedelta

        filings = await self.fetcher.get_filings(
            cik=cik, form_type="4", limit=50,
            after_date=(datetime.utcnow() - timedelta(days=days_back)).strftime("%Y-%m-%d")
        )

        created = 0
        for filing in filings:
            text = await self.fetcher.get_filing_text(filing["accession_number"])
            if not text:
                continue
            try:
                response = await self.llm.complete(
                    prompt=f"{FORM4_PROMPT}\n\n---\n\n{text[:4000]}"
                )
                data = response.parse_json()
            except Exception as e:
                logger.warning(f"Form 4 parse failed: {e}")
                continue

            person_name = data.get("reporting_person", "")
            person = db.query(Person).filter(Person.full_name.ilike(f"%{person_name.split()[-1]}%")).first() if person_name else None

            for txn in (data.get("transactions") or []):
                try:
                    txn_date = date.fromisoformat(txn["transaction_date"])
                except (KeyError, ValueError):
                    continue
                existing = db.query(InsiderTransaction).filter(
                    InsiderTransaction.person_id == (person.id if person else None),
                    InsiderTransaction.company_id == company_id,
                    InsiderTransaction.transaction_date == txn_date,
                    InsiderTransaction.shares == txn.get("shares"),
                ).first()
                if not existing:
                    db.add(InsiderTransaction(
                        person_id=person.id if person else None,
                        company_id=company_id,
                        company_name=company_name,
                        transaction_date=txn_date,
                        transaction_type=txn.get("transaction_type"),
                        shares=txn.get("shares"),
                        price_per_share=txn.get("price_per_share"),
                        total_value_usd=txn.get("total_value_usd"),
                        shares_owned_after=txn.get("shares_owned_after"),
                        is_10b5_plan=txn.get("is_10b5_plan", False),
                        form4_url=filing.get("filing_url"),
                        filed_at=date.fromisoformat(filing["filing_date"]) if filing.get("filing_date") else None,
                    ))
                    created += 1

        db.commit()
        return {"status": "ok", "company_id": company_id, "transactions_created": created}

    def _extract_comp_section(self, text: str) -> Optional[str]:
        """Find Summary Compensation Table section in proxy text."""
        markers = [
            "summary compensation table",
            "executive compensation table",
            "named executive officer compensation",
        ]
        text_lower = text.lower()
        for marker in markers:
            idx = text_lower.find(marker)
            if idx != -1:
                return text[max(0, idx - 100): idx + 10000]
        return None
```

### Step 3 — New endpoints added to `app/api/v1/people.py`

Add at the bottom of the file:

```python
from app.sources.people_collection.proxy_comp_agent import ProxyCompAgent

@router.get("/{person_id}/compensation-history")
async def get_compensation_history(
    person_id: int,
    db: Session = Depends(get_db),
):
    """Multi-year compensation across all public company roles for a person."""
    from app.core.people_models import CompanyPerson, IndustrialCompany
    person = db.query(Person).filter(Person.id == person_id).first()
    if not person:
        raise HTTPException(status_code=404, detail="Person not found")
    rows = (
        db.query(CompanyPerson, IndustrialCompany)
        .join(IndustrialCompany, CompanyPerson.company_id == IndustrialCompany.id)
        .filter(
            CompanyPerson.person_id == person_id,
            CompanyPerson.total_compensation_usd.isnot(None),
        )
        .order_by(CompanyPerson.compensation_year.desc().nullslast())
        .all()
    )
    return {
        "person_id": person_id,
        "full_name": person.full_name,
        "compensation_history": [
            {
                "company": co.name,
                "title": cp.title,
                "year": cp.compensation_year,
                "base_salary_usd": float(cp.base_salary_usd) if cp.base_salary_usd else None,
                "total_compensation_usd": float(cp.total_compensation_usd),
                "equity_awards_usd": float(cp.equity_awards_usd) if cp.equity_awards_usd else None,
            }
            for cp, co in rows
        ],
    }


@router.get("/{person_id}/insider-transactions")
async def get_insider_transactions(
    person_id: int,
    limit: int = Query(50, le=200),
    transaction_type: Optional[str] = Query(None, description="buy, sell, option_exercise"),
    db: Session = Depends(get_db),
):
    """Form 4 insider transaction history for a person."""
    from app.core.people_models import InsiderTransaction
    person = db.query(Person).filter(Person.id == person_id).first()
    if not person:
        raise HTTPException(status_code=404, detail="Person not found")
    q = db.query(InsiderTransaction).filter(InsiderTransaction.person_id == person_id)
    if transaction_type:
        q = q.filter(InsiderTransaction.transaction_type == transaction_type)
    txns = q.order_by(InsiderTransaction.transaction_date.desc()).limit(limit).all()
    total_sold = sum(t.total_value_usd or 0 for t in txns if t.transaction_type == "sell")
    total_bought = sum(t.total_value_usd or 0 for t in txns if t.transaction_type == "buy")
    return {
        "person_id": person_id,
        "full_name": person.full_name,
        "summary": {
            "total_transactions": len(txns),
            "total_sold_usd": float(total_sold),
            "total_bought_usd": float(total_bought),
            "net_activity": "selling" if total_sold > total_bought else "buying" if total_bought > total_sold else "neutral",
        },
        "transactions": [
            {
                "date": t.transaction_date.isoformat(),
                "type": t.transaction_type,
                "company": t.company_name,
                "shares": t.shares,
                "price": float(t.price_per_share) if t.price_per_share else None,
                "total_value_usd": float(t.total_value_usd) if t.total_value_usd else None,
                "shares_owned_after": t.shares_owned_after,
                "is_10b5_plan": t.is_10b5_plan,
            }
            for t in txns
        ],
    }


@router.post("/companies/{company_id}/collect-comp")
async def collect_executive_comp(
    company_id: int,
    include_form4: bool = Query(True, description="Also collect Form 4 insider transactions"),
    db: Session = Depends(get_db),
):
    """
    Trigger SEC proxy comp collection for a company.
    Requires company to have a CIK set. Populates base_salary_usd, total_compensation_usd,
    equity_awards_usd on existing company_people rows.
    """
    from app.core.people_models import IndustrialCompany
    company = db.query(IndustrialCompany).filter(IndustrialCompany.id == company_id).first()
    if not company:
        raise HTTPException(status_code=404, detail="Company not found")
    if not company.cik:
        raise HTTPException(status_code=400, detail="Company has no CIK")
    agent = ProxyCompAgent()
    try:
        comp_result = await agent.collect_comp(
            company_id=company_id, cik=company.cik,
            company_name=company.name, db=db,
        )
        form4_result = {}
        if include_form4:
            form4_result = await agent.collect_form4(
                company_id=company_id, cik=company.cik,
                company_name=company.name, db=db,
            )
    finally:
        await agent.close()
    return {**comp_result, "form4": form4_result}


@router.get("/companies/{company_id}/executive-comp")
async def get_executive_comp(
    company_id: int,
    db: Session = Depends(get_db),
):
    """Current team compensation table for a company."""
    from app.core.people_models import CompanyPerson, IndustrialCompany
    company = db.query(IndustrialCompany).filter(IndustrialCompany.id == company_id).first()
    if not company:
        raise HTTPException(status_code=404, detail="Company not found")
    rows = (
        db.query(CompanyPerson, Person)
        .join(Person, CompanyPerson.person_id == Person.id)
        .filter(
            CompanyPerson.company_id == company_id,
            CompanyPerson.is_current == True,
            CompanyPerson.total_compensation_usd.isnot(None),
        )
        .order_by(CompanyPerson.total_compensation_usd.desc().nullslast())
        .all()
    )
    return {
        "company_id": company_id,
        "company_name": company.name,
        "executives": [
            {
                "person_id": cp.person_id,
                "full_name": p.full_name,
                "title": cp.title,
                "year": cp.compensation_year,
                "base_salary_usd": float(cp.base_salary_usd) if cp.base_salary_usd else None,
                "total_compensation_usd": float(cp.total_compensation_usd),
                "equity_awards_usd": float(cp.equity_awards_usd) if cp.equity_awards_usd else None,
            }
            for cp, p in rows
        ],
    }
```

---

## Master Agent: Post-Merge Tasks

After all three worktrees complete, the master agent must:

1. **Merge `people_models.py`**: Append `PersonPedigreeScore`, `BoardSeat`, `BoardInterlock`, `InsiderTransaction` classes (in that order) and add `from datetime import datetime` to imports if not already present.

2. **Register board_interlocks router in `app/main.py`**: Find where people routers are registered and add:
   ```python
   from app.api.v1 import board_interlocks
   app.include_router(board_interlocks.router, prefix="/api/v1")
   ```

3. **Restart API**: `docker-compose restart api`, wait 30s.

4. **Verify endpoints**:
   ```bash
   # Agent 1 — pedigree scorer
   curl -s -X POST http://localhost:8001/api/v1/people-analytics/companies/1/score-pedigree | python -m json.tool
   curl -s http://localhost:8001/api/v1/people-analytics/companies/1/pedigree-report | python -m json.tool

   # Agent 2 — board interlocks (tables created, endpoint reachable)
   curl -s http://localhost:8001/api/v1/board-interlocks/person/1/seats | python -m json.tool

   # Agent 3 — comp history (tables created, endpoint reachable)
   curl -s http://localhost:8001/api/v1/people/1/compensation-history | python -m json.tool
   ```

5. **Fix any post-merge bugs** (see `feedback_master_agent_role.md` for common patterns).

6. **Commit** with message: `feat: add career pedigree scoring, board interlock graph, and SEC proxy comp agents`

---

## Routing note for `app/main.py`

`board_interlocks` router uses prefix `/board-interlocks` — no conflict with existing routes. Verify by checking that no existing router uses this prefix before registering.

---

## Copy-paste agent instructions

### Agent 1 instruction
```
You are Agent 1 for PLAN_033. Working directory: this worktree.
Spec: write BYPASS_TRIVIAL to docs/specs/.active_spec before editing.

Create app/services/pedigree_scorer.py with the full PedigreeScorer class from PLAN_033.
Add PersonPedigreeScore model to app/core/people_models.py (append after PeopleWatchlistPerson class).
Add 3 endpoints to app/api/v1/people_analytics.py: POST /companies/{id}/score-pedigree and GET /companies/{id}/pedigree-report.
Add GET /{person_id}/pedigree endpoint to app/api/v1/people.py.
Do NOT edit app/main.py — master agent handles router registration.
Copy exact code from PLAN_033 — do not paraphrase or abbreviate.
```

### Agent 2 instruction
```
You are Agent 2 for PLAN_033. Working directory: this worktree.
Spec: write BYPASS_TRIVIAL to docs/specs/.active_spec before editing.

Create app/sources/people_collection/board_agent.py with the full BoardAgent class from PLAN_033.
Create app/services/board_interlock_service.py with the full BoardInterlockService class from PLAN_033.
Create app/api/v1/board_interlocks.py with the full router from PLAN_033.
Add BoardSeat and BoardInterlock models to app/core/people_models.py (append after PersonPedigreeScore if Agent 1 ran first, otherwise after PeopleWatchlistPerson).
Do NOT edit app/main.py — master agent handles router registration.
Copy exact code from PLAN_033 — do not paraphrase or abbreviate.
```

### Agent 3 instruction
```
You are Agent 3 for PLAN_033. Working directory: this worktree.
Spec: write BYPASS_TRIVIAL to docs/specs/.active_spec before editing.

Create app/sources/people_collection/proxy_comp_agent.py with the full ProxyCompAgent class from PLAN_033.
Add InsiderTransaction model to app/core/people_models.py (append after BoardInterlock if Agents 1+2 ran first, otherwise after PeopleWatchlistPerson).
Add 4 endpoints to app/api/v1/people.py: GET /{person_id}/compensation-history, GET /{person_id}/insider-transactions, POST /companies/{id}/collect-comp, GET /companies/{id}/executive-comp.
Do NOT edit app/main.py — master agent handles router registration.
Copy exact code from PLAN_033 — do not paraphrase or abbreviate.
```
