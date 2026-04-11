"""
Synthetic LP-GP Universe Generator — PLAN_053 Phase A2.

Generates a realistic LP fund universe and LP→GP commitment relationships
to unblock gp_pipeline_scorer and lp_gp_graph services.
All records are inserted with data_origin='synthetic' on the ingestion job.
"""
from __future__ import annotations

import logging
import math
import random
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.core.models import IngestionJob, JobStatus

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# LP type distribution + AUM ranges (billions USD)
# ---------------------------------------------------------------------------

LP_TYPE_CONFIG = {
    "public_pension": {
        "weight": 0.40,
        "aum_range": (20, 500),
        "tier": 2,
        "jurisdictions": [
            "CA", "NY", "TX", "FL", "OH", "PA", "IL", "NJ", "WA", "NC",
            "VA", "MI", "GA", "MN", "WI", "CO", "OR", "CT", "MA", "TN",
        ],
    },
    "endowment": {
        "weight": 0.20,
        "aum_range": (2, 50),
        "tier": 1,
        "jurisdictions": ["MA", "CT", "NY", "CA", "TX", "PA", "NC", "IL", "MI", "NJ"],
    },
    "insurance": {
        "weight": 0.15,
        "aum_range": (10, 200),
        "tier": 3,
        "jurisdictions": ["CT", "NY", "NJ", "IL", "OH", "PA", "MA", "GA", "TX", "CA"],
    },
    "sovereign_wealth": {
        "weight": 0.10,
        "aum_range": (50, 800),
        "tier": 1,
        "jurisdictions": ["AE", "NO", "SG", "KW", "SA", "QA", "KR", "CN", "AU", "MY"],
    },
    "family_office": {
        "weight": 0.10,
        "aum_range": (0.5, 10),
        "tier": 5,
        "jurisdictions": ["NY", "CA", "FL", "TX", "CT", "IL", "CO", "WA", "MA", "NV"],
    },
    "fund_of_funds": {
        "weight": 0.05,
        "aum_range": (1, 30),
        "tier": 4,
        "jurisdictions": ["NY", "CT", "MA", "CA", "IL", "PA", "NJ", "TX", "CO", "GA"],
    },
}

# Real LP institution name templates by type
LP_NAME_TEMPLATES = {
    "public_pension": [
        "{state} Public Employees' Retirement System",
        "{state} State Teachers' Retirement System",
        "{state} Municipal Employees' Pension Fund",
        "{state} Police & Fire Retirement System",
        "{state} State Investment Board",
        "{state} Public Employees' Benefit Authority",
    ],
    "endowment": [
        "{inst} University Endowment",
        "{inst} Foundation",
        "{inst} College Investment Fund",
        "The {inst} Endowment Fund",
    ],
    "insurance": [
        "{inst} Life Insurance Co.",
        "{inst} Mutual Insurance",
        "{inst} Re Insurance Group",
        "{inst} Financial Group",
    ],
    "sovereign_wealth": [
        "{inst} Investment Authority",
        "{inst} Sovereign Fund",
        "{inst} Government Investment Corp",
        "{inst} National Wealth Fund",
    ],
    "family_office": [
        "{inst} Family Office",
        "The {inst} Family Foundation",
        "{inst} Capital Partners",
        "{inst} Private Wealth",
    ],
    "fund_of_funds": [
        "{inst} Partners Fund of Funds",
        "{inst} Private Equity Partners",
        "{inst} Capital Allocation Fund",
    ],
}

ENDOWMENT_NAMES = [
    "Harvard", "Yale", "Stanford", "Princeton", "MIT", "Duke", "Columbia",
    "Penn", "Northwestern", "Chicago", "Cornell", "Dartmouth", "Brown",
    "Rice", "Emory", "Vanderbilt", "Georgetown", "USC", "Notre Dame",
    "Carnegie Mellon", "Johns Hopkins", "WashU", "Tufts", "Brandeis",
]

INSURANCE_NAMES = [
    "Metropolitan", "Prudential", "Lincoln", "Hartford", "Guardian",
    "Pacific", "American General", "Liberty", "Transamerica", "Unum",
    "Cigna", "Aetna", "Humana", "Aflac", "Allstate",
]

SOVEREIGN_NAMES = [
    "Abu Dhabi", "Norway Government", "Singapore", "Kuwait", "Saudi",
    "Qatar", "Korea", "China State", "Australia Future", "Malaysia Khazanah",
]

FAMILY_NAMES = [
    "Walton", "Koch", "Mars", "Cargill", "Pritzker",
    "Johnson", "Cox", "Newhouse", "Lauder", "Duncan",
    "Bass", "Hunt", "Crown", "Ziff", "Hearst",
]

FOF_NAMES = [
    "Adams Street", "HarbourVest", "Pantheon", "Hamilton Lane", "StepStone",
    "Neuberger Berman", "Cambridge Associates", "Commonfund", "GCM Grosvenor", "Alpinvest",
]

STATE_NAMES = {
    "CA": "California", "NY": "New York", "TX": "Texas", "FL": "Florida",
    "OH": "Ohio", "PA": "Pennsylvania", "IL": "Illinois", "NJ": "New Jersey",
    "WA": "Washington", "NC": "North Carolina", "VA": "Virginia", "MI": "Michigan",
    "GA": "Georgia", "MN": "Minnesota", "WI": "Wisconsin", "CO": "Colorado",
    "OR": "Oregon", "CT": "Connecticut", "MA": "Massachusetts", "TN": "Tennessee",
}


class SyntheticLpGpGenerator:
    """Generates synthetic LP fund universe and LP-GP relationships."""

    def __init__(self, db: Session):
        self.db = db

    def generate(
        self,
        n_lps: int = 500,
        seed: Optional[int] = None,
    ) -> Dict:
        """Generate LP funds and LP-GP relationships, insert into DB."""
        if seed is not None:
            random.seed(seed)

        # Create ingestion job
        job = IngestionJob(
            source="synthetic_lp_gp",
            status=JobStatus.RUNNING,
            config={"n_lps": n_lps},
            data_origin="synthetic",
        )
        self.db.add(job)
        self.db.flush()

        # Get real GP firm names from pe_firm table
        gp_firms = self._get_gp_firms()
        if not gp_firms:
            job.status = JobStatus.FAILED
            job.error_message = "No PE firms found in pe_firm table"
            self.db.commit()
            return {"status": "no_gp_firms", "lps_created": 0, "relationships_created": 0}

        # Generate LP funds
        lps = self._generate_lps(n_lps)
        lp_ids = self._insert_lps(lps)

        # Generate LP-GP relationships with power-law distribution
        relationships = self._generate_relationships(lp_ids, lps, gp_firms)
        n_rels = self._insert_relationships(relationships)

        job.status = JobStatus.SUCCESS
        job.rows_inserted = len(lp_ids) + n_rels
        job.completed_at = datetime.utcnow()
        self.db.commit()

        logger.info(
            "Synthetic LP-GP universe generated",
            extra={"lps": len(lp_ids), "relationships": n_rels, "job_id": job.id},
        )

        return {
            "status": "success",
            "job_id": job.id,
            "lps_created": len(lp_ids),
            "relationships_created": n_rels,
            "gp_firms_linked": len(gp_firms),
            "data_origin": "synthetic",
        }

    def _get_gp_firms(self) -> List[Dict]:
        """Get seeded PE firms from pe_firm table."""
        rows = self.db.execute(text(
            "SELECT id, name FROM pe_firms ORDER BY id LIMIT 200"
        )).fetchall()
        return [{"id": r[0], "name": r[1]} for r in rows]

    def _generate_lps(self, n_lps: int) -> List[Dict]:
        """Generate LP fund records with type distribution."""
        lps = []
        type_counts = {}

        # Determine count per type
        for lp_type, cfg in LP_TYPE_CONFIG.items():
            count = max(1, round(n_lps * cfg["weight"]))
            type_counts[lp_type] = count

        # Adjust to hit target
        total = sum(type_counts.values())
        if total != n_lps:
            diff = n_lps - total
            type_counts["public_pension"] += diff

        name_pool = {
            "endowment": list(ENDOWMENT_NAMES),
            "insurance": list(INSURANCE_NAMES),
            "sovereign_wealth": list(SOVEREIGN_NAMES),
            "family_office": list(FAMILY_NAMES),
            "fund_of_funds": list(FOF_NAMES),
        }
        used_names = set()

        for lp_type, count in type_counts.items():
            cfg = LP_TYPE_CONFIG[lp_type]
            templates = LP_NAME_TEMPLATES[lp_type]

            for i in range(count):
                jurisdiction = random.choice(cfg["jurisdictions"])

                if lp_type == "public_pension":
                    state_name = STATE_NAMES.get(jurisdiction, jurisdiction)
                    template = templates[i % len(templates)]
                    name = template.replace("{state}", state_name)
                else:
                    pool = name_pool.get(lp_type, [])
                    if pool:
                        inst = pool[i % len(pool)]
                    else:
                        inst = f"LP-{i+1}"
                    template = templates[i % len(templates)]
                    name = template.replace("{inst}", inst)

                # Ensure unique names
                base_name = name
                suffix = 1
                while name in used_names:
                    suffix += 1
                    name = f"{base_name} {suffix}"
                used_names.add(name)

                aum_min, aum_max = cfg["aum_range"]
                aum = round(random.uniform(aum_min, aum_max), 1)

                lps.append({
                    "name": name,
                    "lp_type": lp_type,
                    "lp_tier": cfg["tier"],
                    "jurisdiction": jurisdiction,
                    "aum_usd_billions": str(aum),
                    "aum_numeric": aum,
                    "collection_priority": 5,
                    "has_cafr": 1 if lp_type == "public_pension" else 0,
                })

        return lps

    def _insert_lps(self, lps: List[Dict]) -> List[int]:
        """Insert LP funds and return their IDs."""
        lp_ids = []
        for lp in lps:
            # Use INSERT ... ON CONFLICT DO NOTHING to handle existing names
            result = self.db.execute(
                text(
                    "INSERT INTO lp_fund (name, lp_type, lp_tier, jurisdiction, "
                    "aum_usd_billions, collection_priority, has_cafr, created_at) "
                    "VALUES (:name, :lp_type, :lp_tier, :jurisdiction, "
                    ":aum_usd_billions, :collection_priority, :has_cafr, :created_at) "
                    "ON CONFLICT (name) DO UPDATE SET "
                    "aum_usd_billions = COALESCE(EXCLUDED.aum_usd_billions, lp_fund.aum_usd_billions), "
                    "lp_tier = COALESCE(EXCLUDED.lp_tier, lp_fund.lp_tier) "
                    "RETURNING id"
                ),
                {
                    "name": lp["name"],
                    "lp_type": lp["lp_type"],
                    "lp_tier": lp["lp_tier"],
                    "jurisdiction": lp["jurisdiction"],
                    "aum_usd_billions": lp["aum_usd_billions"],
                    "collection_priority": lp["collection_priority"],
                    "has_cafr": lp["has_cafr"],
                    "created_at": datetime.utcnow(),
                },
            )
            row = result.fetchone()
            lp_ids.append(row[0])
            lp["id"] = row[0]

        self.db.flush()
        return lp_ids

    def _generate_relationships(
        self,
        lp_ids: List[int],
        lps: List[Dict],
        gp_firms: List[Dict],
    ) -> List[Dict]:
        """Generate LP-GP relationships with power-law distribution.

        Mega-GPs get 15-25 LPs, mid-market get 3-8.
        """
        relationships = []

        # Sort GPs by id (proxy for prominence — lower IDs are major firms)
        sorted_gps = sorted(gp_firms, key=lambda g: g["id"])

        for lp, lp_id in zip(lps, lp_ids):
            aum = lp["aum_numeric"]
            lp_tier = lp["lp_tier"]

            # Larger LPs commit to more GPs
            if aum > 100:
                n_gps = random.randint(8, min(20, len(sorted_gps)))
            elif aum > 30:
                n_gps = random.randint(4, min(12, len(sorted_gps)))
            else:
                n_gps = random.randint(2, min(6, len(sorted_gps)))

            # Power-law: bias toward top GPs
            weights = [1.0 / (i + 1) ** 0.6 for i in range(len(sorted_gps))]
            chosen_gps = random.choices(sorted_gps, weights=weights, k=n_gps)
            # Deduplicate
            seen_gps = set()
            unique_gps = []
            for gp in chosen_gps:
                if gp["id"] not in seen_gps:
                    seen_gps.add(gp["id"])
                    unique_gps.append(gp)

            for gp in unique_gps:
                # Commitment = 1-5% of AUM
                pct = random.uniform(0.01, 0.05)
                commitment_usd = round(aum * pct * 1e9, 0)

                # Vintage years
                n_vintages = random.randint(1, 4)
                first_vintage = random.randint(2015, 2025 - n_vintages + 1)
                last_vintage = first_vintage + n_vintages - 1

                trend = random.choice(["growing", "stable", "declining", "new"])
                if n_vintages == 1:
                    trend = "new"

                relationships.append({
                    "lp_id": lp_id,
                    "gp_name": gp["name"],
                    "gp_firm_id": gp["id"],
                    "first_vintage": first_vintage,
                    "last_vintage": last_vintage,
                    "total_vintages_committed": n_vintages,
                    "total_committed_usd": commitment_usd,
                    "avg_commitment_usd": round(commitment_usd / n_vintages, 0),
                    "commitment_trend": trend,
                })

        return relationships

    def _insert_relationships(self, relationships: List[Dict]) -> int:
        """Insert LP-GP relationships, skip duplicates."""
        inserted = 0
        for rel in relationships:
            try:
                self.db.execute(
                    text(
                        "INSERT INTO lp_gp_relationships "
                        "(lp_id, gp_name, gp_firm_id, first_vintage, last_vintage, "
                        "total_vintages_committed, total_committed_usd, avg_commitment_usd, "
                        "commitment_trend, created_at, last_updated) "
                        "VALUES (:lp_id, :gp_name, :gp_firm_id, :first_vintage, :last_vintage, "
                        ":total_vintages_committed, :total_committed_usd, :avg_commitment_usd, "
                        ":commitment_trend, :created_at, :last_updated) "
                        "ON CONFLICT (lp_id, gp_name) DO UPDATE SET "
                        "total_committed_usd = EXCLUDED.total_committed_usd, "
                        "total_vintages_committed = EXCLUDED.total_vintages_committed, "
                        "last_vintage = EXCLUDED.last_vintage, "
                        "commitment_trend = EXCLUDED.commitment_trend, "
                        "last_updated = EXCLUDED.last_updated"
                    ),
                    {
                        **rel,
                        "created_at": datetime.utcnow(),
                        "last_updated": datetime.utcnow(),
                    },
                )
                inserted += 1
            except Exception as e:
                logger.debug(f"LP-GP insert skipped: {e}")

        self.db.flush()
        return inserted
