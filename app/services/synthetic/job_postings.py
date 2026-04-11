"""
Synthetic Job Postings Generator — PLAN_053 Phase A1.

Generates realistic job postings for seeded companies to unblock
exec_signal_scorer and company_diligence_scorer (growth factor).
All records are inserted with data_origin='synthetic' on the ingestion job.
"""
from __future__ import annotations

import logging
import random
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.core.models import IngestionJob, JobStatus

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Sector → role distribution (what % of roles fall in each department)
# ---------------------------------------------------------------------------

SECTOR_ROLE_MIX: Dict[str, Dict[str, float]] = {
    "technology": {
        "Engineering": 0.40, "Product": 0.12, "Sales": 0.12,
        "Marketing": 0.08, "Operations": 0.08, "Finance": 0.06,
        "HR": 0.05, "Legal": 0.03, "IT": 0.03, "Customer Success": 0.03,
    },
    "healthcare": {
        "Clinical": 0.30, "Nursing": 0.15, "Operations": 0.12,
        "Administration": 0.10, "Finance": 0.08, "Research": 0.07,
        "IT": 0.05, "HR": 0.05, "Sales": 0.04, "Legal": 0.04,
    },
    "industrials": {
        "Operations": 0.25, "Engineering": 0.20, "Manufacturing": 0.15,
        "Sales": 0.10, "Supply Chain": 0.08, "Finance": 0.07,
        "HR": 0.05, "IT": 0.04, "Quality": 0.03, "Legal": 0.03,
    },
    "financial": {
        "Finance": 0.25, "Operations": 0.15, "Risk": 0.12,
        "Technology": 0.12, "Compliance": 0.08, "Sales": 0.08,
        "HR": 0.06, "Legal": 0.06, "Marketing": 0.04, "IT": 0.04,
    },
    "consumer": {
        "Sales": 0.20, "Marketing": 0.18, "Operations": 0.15,
        "Supply Chain": 0.10, "Finance": 0.08, "Product": 0.08,
        "HR": 0.06, "IT": 0.05, "Customer Service": 0.05, "Legal": 0.05,
    },
    "energy": {
        "Operations": 0.25, "Engineering": 0.22, "Field Services": 0.15,
        "Safety": 0.08, "Finance": 0.07, "Sales": 0.06,
        "HR": 0.05, "IT": 0.04, "Legal": 0.04, "Environmental": 0.04,
    },
}

# Default mix for sectors not explicitly listed
DEFAULT_ROLE_MIX: Dict[str, float] = {
    "Operations": 0.20, "Engineering": 0.15, "Sales": 0.12,
    "Finance": 0.10, "Marketing": 0.08, "HR": 0.07,
    "IT": 0.07, "Product": 0.06, "Legal": 0.05, "Administration": 0.10,
}

# ---------------------------------------------------------------------------
# Seniority levels + title templates per department
# ---------------------------------------------------------------------------

SENIORITY_WEIGHTS = {
    "c_suite": 0.02,
    "vp": 0.03,
    "director": 0.10,
    "manager": 0.25,
    "senior": 0.25,
    "mid": 0.20,
    "entry": 0.15,
}

TITLE_TEMPLATES: Dict[str, Dict[str, List[str]]] = {
    "c_suite": {
        "Engineering": ["CTO", "Chief Technology Officer", "Chief Engineering Officer"],
        "Finance": ["CFO", "Chief Financial Officer"],
        "Operations": ["COO", "Chief Operating Officer"],
        "Sales": ["CRO", "Chief Revenue Officer"],
        "Marketing": ["CMO", "Chief Marketing Officer"],
        "HR": ["CHRO", "Chief People Officer"],
        "Legal": ["General Counsel", "Chief Legal Officer"],
        "_default": ["Chief {dept} Officer"],
    },
    "vp": {
        "_default": ["VP of {dept}", "Vice President, {dept}", "SVP of {dept}"],
    },
    "director": {
        "_default": ["Director of {dept}", "Senior Director, {dept}", "Director, {dept} Operations"],
    },
    "manager": {
        "_default": ["{dept} Manager", "Senior {dept} Manager", "Manager, {dept}"],
    },
    "senior": {
        "Engineering": ["Senior Software Engineer", "Senior Data Engineer", "Senior DevOps Engineer", "Staff Engineer"],
        "Sales": ["Senior Account Executive", "Senior Sales Manager", "Enterprise Account Executive"],
        "_default": ["Senior {dept} Specialist", "Senior {dept} Analyst", "Lead {dept} Associate"],
    },
    "mid": {
        "Engineering": ["Software Engineer", "Data Engineer", "DevOps Engineer", "Backend Engineer"],
        "Sales": ["Account Executive", "Business Development Representative"],
        "_default": ["{dept} Specialist", "{dept} Analyst", "{dept} Associate"],
    },
    "entry": {
        "Engineering": ["Junior Software Engineer", "Associate Engineer", "Software Engineer I"],
        "_default": ["{dept} Coordinator", "Associate {dept} Analyst", "{dept} Assistant"],
    },
}

US_STATES = [
    "CA", "NY", "TX", "FL", "IL", "PA", "OH", "GA", "NC", "MI",
    "NJ", "VA", "WA", "AZ", "MA", "TN", "IN", "MO", "MD", "WI",
    "CO", "MN", "SC", "AL", "LA", "KY", "OR", "OK", "CT", "UT",
]


def _pick_title(seniority: str, department: str) -> str:
    """Pick a random title for the seniority+department combo."""
    templates = TITLE_TEMPLATES.get(seniority, {})
    options = templates.get(department, templates.get("_default", ["{dept} Specialist"]))
    title = random.choice(options)
    return title.replace("{dept}", department)


class SyntheticJobPostingsGenerator:
    """Generates synthetic job postings for seeded companies."""

    def __init__(self, db: Session):
        self.db = db

    def generate(
        self,
        n_per_company: int = 80,
        seed: Optional[int] = None,
    ) -> Dict:
        """Generate synthetic job postings and insert into DB.

        Returns summary dict with counts and job_id.
        """
        if seed is not None:
            random.seed(seed)

        # Get seeded companies
        companies = self._get_companies()
        if not companies:
            return {"status": "no_companies", "postings_created": 0}

        # Create ingestion job
        job = IngestionJob(
            source="synthetic_job_postings",
            status=JobStatus.RUNNING,
            config={"n_per_company": n_per_company, "n_companies": len(companies)},
            data_origin="synthetic",
        )
        self.db.add(job)
        self.db.flush()

        total_inserted = 0
        batch = []

        for company in companies:
            cid = company["id"]
            sector = self._detect_sector(company.get("industry", ""))
            role_mix = SECTOR_ROLE_MIX.get(sector, DEFAULT_ROLE_MIX)
            # Scale postings by company size if available
            n = min(n_per_company, max(10, n_per_company))
            state = company.get("state") or random.choice(US_STATES)

            for _ in range(n):
                dept = self._weighted_pick(role_mix)
                seniority = self._weighted_pick(SENIORITY_WEIGHTS)
                title = _pick_title(seniority, dept)
                days_ago = self._pick_posted_days_ago()

                batch.append({
                    "company_id": cid,
                    "title": title,
                    "title_normalized": title.lower(),
                    "department": dept,
                    "seniority_level": seniority,
                    "status": "open",
                    "location": f"{state}, US",
                    "employment_type": "full_time",
                    "workplace_type": random.choice(["onsite", "hybrid", "remote"]),
                    "ats_type": "synthetic",
                    "posted_date": datetime.utcnow() - timedelta(days=days_ago),
                    "first_seen_at": datetime.utcnow() - timedelta(days=days_ago),
                    "last_seen_at": datetime.utcnow(),
                    "created_at": datetime.utcnow(),
                })

                if len(batch) >= 500:
                    self._insert_batch(batch)
                    total_inserted += len(batch)
                    batch = []

        if batch:
            self._insert_batch(batch)
            total_inserted += len(batch)

        job.status = JobStatus.SUCCESS
        job.rows_inserted = total_inserted
        job.completed_at = datetime.utcnow()
        self.db.commit()

        logger.info(
            "Synthetic job postings generated",
            extra={"companies": len(companies), "postings": total_inserted, "job_id": job.id},
        )

        return {
            "status": "success",
            "job_id": job.id,
            "companies": len(companies),
            "postings_created": total_inserted,
            "data_origin": "synthetic",
        }

    def _get_companies(self) -> List[Dict]:
        """Get seeded companies from industrial_companies + pe_portfolio_companies."""
        companies = []

        # Industrial companies (job_postings.company_id FK points here)
        rows = self.db.execute(text(
            "SELECT id, name, headquarters_state, industry_segment "
            "FROM industrial_companies LIMIT 200"
        )).fetchall()
        for r in rows:
            companies.append({"id": r[0], "name": r[1], "state": r[2], "industry": r[3] or ""})

        return companies

    def _detect_sector(self, industry: str) -> str:
        """Map industry string to sector key using prefix matching."""
        ind = industry.lower()
        # Order matters: check more specific sectors first
        sector_keywords = {
            "consumer": ["consumer", "retail", "food", "beverage", "restaurant", "apparel"],
            "healthcare": ["health", "medical", "pharma", "biotech", "hospital", "care"],
            "financial": ["finance", "bank", "insurance", "invest", "capital"],
            "energy": ["energy", "oil", "gas", "solar", "wind", "power", "utility"],
            "technology": ["tech", "software", "saas", "cloud", "data", "cyber"],
            "industrials": ["industrial", "manufacturing", "aerospace", "defense", "auto"],
        }
        for sector, keywords in sector_keywords.items():
            for kw in keywords:
                # Prefix match: kw must start a word, not appear mid-word
                # e.g., "retail" should not match "ai"
                idx = ind.find(kw)
                if idx >= 0 and (idx == 0 or not ind[idx - 1].isalpha()):
                    return sector
        return "industrials"

    @staticmethod
    def _weighted_pick(weights: Dict[str, float]) -> str:
        """Pick a key from a {key: weight} dict."""
        keys = list(weights.keys())
        vals = list(weights.values())
        return random.choices(keys, weights=vals, k=1)[0]

    @staticmethod
    def _pick_posted_days_ago() -> int:
        """70% last 30 days, 20% 30-60, 10% 60-90."""
        r = random.random()
        if r < 0.70:
            return random.randint(0, 30)
        elif r < 0.90:
            return random.randint(30, 60)
        else:
            return random.randint(60, 90)

    def _insert_batch(self, batch: List[Dict]) -> None:
        """Bulk insert a batch of job postings."""
        if not batch:
            return
        cols = list(batch[0].keys())
        placeholders = ", ".join(f":{c}" for c in cols)
        col_names = ", ".join(cols)
        self.db.execute(
            text(f"INSERT INTO job_postings ({col_names}) VALUES ({placeholders})"),
            batch,
        )
