"""
Functional Org Mapper - Maps reporting chains for specific functions.

After people are collected from all sources, this module builds
function-specific org charts (starting with technology) by combining
database data with targeted LinkedIn discovery.

Key capabilities:
- Identify functional leaders at each business unit
- Fill gaps via targeted LinkedIn discovery
- Build cross-subsidiary reporting chains (subsidiary CTO -> parent CTO)
- 3-level deep mapping: C-suite -> VPs -> Directors
"""

import asyncio
import json
import logging
import re
from typing import List, Optional, Dict, Any, Tuple

from sqlalchemy.orm import Session

from app.core.people_models import (
    CompanyPerson,
    Person,
    IndustrialCompany,
    OrgChartSnapshot,
)
from app.sources.people_collection.types import (
    ExtractedPerson,
    TitleLevel,
    ExtractionConfidence,
)
from app.sources.people_collection.llm_extractor import LLMExtractor

logger = logging.getLogger(__name__)


# Function keyword configurations for identifying functional leaders
FUNCTION_KEYWORDS = {
    "technology": {
        "title_patterns": [
            r'\bcto\b', r'\bcio\b', r'\bciso\b', r'\bcdo\b',
            r'\bchief\s+technology\b', r'\bchief\s+information\b',
            r'\bchief\s+data\b', r'\bchief\s+digital\b',
            r'\bchief\s+security\b',
            r'\bvp\b.*\bengineering\b', r'\bvp\b.*\btechnology\b',
            r'\bvp\b.*\binfrastructure\b', r'\bvp\b.*\bdata\b',
            r'\bvp\b.*\bplatform\b', r'\bvp\b.*\bsecurity\b',
            r'\bvp\b.*\bit\b',
            r'\bvice\s+president\b.*\bengineering\b',
            r'\bvice\s+president\b.*\btechnology\b',
            r'\bdirector\b.*\bengineering\b', r'\bdirector\b.*\btechnology\b',
            r'\bdirector\b.*\bit\b', r'\bdirector\b.*\bdevops\b',
            r'\bdirector\b.*\binfrastructure\b',
            r'\bhead\s+of\s+(?:engineering|technology|it)\b',
            r'\bchief\s+architect\b',
        ],
        "department_keywords": [
            "technology", "engineering", "it", "infrastructure",
            "data", "platform", "security", "devops", "software",
            "information technology",
        ],
    },
    "finance": {
        "title_patterns": [
            r'\bcfo\b', r'\bchief\s+financial\b', r'\bchief\s+accounting\b',
            r'\bcontroller\b', r'\btreasurer\b',
            r'\bvp\b.*\bfinance\b', r'\bvp\b.*\baccounting\b',
            r'\bdirector\b.*\bfinance\b', r'\bhead\s+of\s+finance\b',
        ],
        "department_keywords": [
            "finance", "accounting", "treasury", "financial",
        ],
    },
    "legal": {
        "title_patterns": [
            r'\bgeneral\s+counsel\b', r'\bclo\b', r'\bchief\s+legal\b',
            r'\bchief\s+compliance\b',
            r'\bvp\b.*\blegal\b', r'\bdeputy\s+general\s+counsel\b',
            r'\bdirector\b.*\blegal\b', r'\bhead\s+of\s+legal\b',
        ],
        "department_keywords": [
            "legal", "compliance", "regulatory",
        ],
    },
}


class FunctionalOrgMapper:
    """
    Maps reporting chains for a specific function (e.g., technology)
    across a company and its subsidiaries.
    """

    def __init__(self):
        self._llm = LLMExtractor()
        self._linkedin_discovery = None

    def _get_linkedin_discovery(self):
        """Lazy init LinkedIn discovery."""
        if self._linkedin_discovery is None:
            from app.sources.people_collection.linkedin_discovery import LinkedInDiscovery
            self._linkedin_discovery = LinkedInDiscovery()
        return self._linkedin_discovery

    async def map_function(
        self,
        company_id: int,
        function_name: str,
        db_session: Session,
        depth: int = 3,
        include_subsidiaries: bool = True,
        fill_gaps_via_linkedin: bool = True,
        max_linkedin_searches: int = 30,
    ) -> Dict[str, Any]:
        """
        Build functional org chart across company and subsidiaries.

        Args:
            company_id: Parent company database ID
            function_name: Function to map ("technology", "finance", "legal")
            db_session: SQLAlchemy session
            depth: Levels below C-suite to map
            include_subsidiaries: Include subsidiary companies
            fill_gaps_via_linkedin: Use LinkedIn to find missing leaders
            max_linkedin_searches: Max LinkedIn searches for gap filling

        Returns:
            Dict with functional org chart data and stats
        """
        if function_name not in FUNCTION_KEYWORDS:
            logger.warning(
                f"[FunctionalOrgMapper] Unknown function: {function_name}. "
                f"Available: {list(FUNCTION_KEYWORDS.keys())}"
            )
            return {"error": f"Unknown function: {function_name}"}

        company = db_session.query(IndustrialCompany).filter(
            IndustrialCompany.id == company_id
        ).first()

        if not company:
            return {"error": f"Company {company_id} not found"}

        logger.info(
            f"[FunctionalOrgMapper] Mapping {function_name} org for {company.name} "
            f"(depth={depth}, include_subs={include_subsidiaries})"
        )

        func_config = FUNCTION_KEYWORDS[function_name]

        # Get all company IDs to search (parent + subsidiaries)
        company_ids = [company_id]
        company_names = {company_id: company.name}

        if include_subsidiaries:
            subsidiaries = db_session.query(IndustrialCompany).filter(
                IndustrialCompany.parent_company_id == company_id,
                IndustrialCompany.status == "active",
            ).all()

            for sub in subsidiaries:
                company_ids.append(sub.id)
                company_names[sub.id] = sub.name

        # Step 1: Find existing people with function-related titles
        leaders_by_company = self._identify_functional_leaders(
            company_ids, func_config, db_session
        )

        total_existing = sum(len(v) for v in leaders_by_company.values())
        logger.info(
            f"[FunctionalOrgMapper] Found {total_existing} existing {function_name} "
            f"leaders across {len(leaders_by_company)} companies"
        )

        # Step 2: Identify gaps
        gaps = []
        for cid in company_ids:
            leaders = leaders_by_company.get(cid, [])
            has_c_level = any(
                self._is_c_level_tech(p["title"], func_config)
                for p in leaders
            )

            if not has_c_level:
                gaps.append({
                    "company_id": cid,
                    "company_name": company_names.get(cid, "Unknown"),
                    "missing": "c_level",
                    "existing_count": len(leaders),
                })
            elif len(leaders) < 3 and depth >= 2:
                gaps.append({
                    "company_id": cid,
                    "company_name": company_names.get(cid, "Unknown"),
                    "missing": "vp_level",
                    "existing_count": len(leaders),
                })

        logger.info(
            f"[FunctionalOrgMapper] Found {len(gaps)} companies with gaps"
        )

        # Step 3: Fill gaps via LinkedIn
        linkedin_people = []
        if fill_gaps_via_linkedin and gaps:
            linkedin_people = await self._fill_gaps_via_linkedin(
                gaps, func_config, function_name, max_linkedin_searches
            )

            # Store LinkedIn-discovered people
            if linkedin_people:
                from app.sources.people_collection.orchestrator import PeopleCollectionOrchestrator
                orchestrator = PeopleCollectionOrchestrator(db_session=db_session)

                for gap in gaps:
                    gap_people = [
                        p for p in linkedin_people
                        if p.extraction_notes and gap["company_name"] in (p.extraction_notes or "")
                    ]
                    if gap_people:
                        gap_company = db_session.get(IndustrialCompany, gap["company_id"])
                        if gap_company:
                            await orchestrator._store_people(
                                gap_people, gap_company, db_session
                            )

                # Re-query to get updated leaders
                leaders_by_company = self._identify_functional_leaders(
                    company_ids, func_config, db_session
                )

        # Step 4: Infer cross-subsidiary reporting
        reporting_updates = await self._infer_cross_subsidiary_reporting(
            leaders_by_company, company_id, company_names, func_config, db_session
        )

        # Step 5: Build functional org chart
        all_leaders = []
        for leaders in leaders_by_company.values():
            all_leaders.extend(leaders)

        chart = self._build_functional_chart(
            all_leaders, company_names, function_name
        )

        result = {
            "function": function_name,
            "company": company.name,
            "total_people": len(all_leaders),
            "companies_covered": len(leaders_by_company),
            "gaps_found": len(gaps),
            "linkedin_people_added": len(linkedin_people),
            "reporting_links_inferred": len(reporting_updates),
            "chart": chart,
            "by_company": {
                company_names.get(cid, str(cid)): len(leaders)
                for cid, leaders in leaders_by_company.items()
            },
        }

        logger.info(
            f"[FunctionalOrgMapper] {function_name} org mapped: "
            f"{len(all_leaders)} people across {len(leaders_by_company)} companies"
        )

        return result

    def _identify_functional_leaders(
        self,
        company_ids: List[int],
        func_config: Dict,
        db_session: Session,
    ) -> Dict[int, List[Dict[str, Any]]]:
        """
        Find existing people with function-related titles, grouped by company.
        """
        results: Dict[int, List[Dict]] = {}

        # Query all current people for these companies
        people = db_session.query(CompanyPerson, Person).join(
            Person, CompanyPerson.person_id == Person.id
        ).filter(
            CompanyPerson.company_id.in_(company_ids),
            CompanyPerson.is_current == True,
        ).all()

        for cp, person in people:
            title = (cp.title or "").lower()
            department = (cp.department or "").lower()

            # Check if this person matches the function
            is_match = False

            # Check title patterns
            for pattern in func_config["title_patterns"]:
                if re.search(pattern, title):
                    is_match = True
                    break

            # Check department keywords
            if not is_match:
                for keyword in func_config["department_keywords"]:
                    if keyword.lower() in department:
                        is_match = True
                        break

            if is_match:
                if cp.company_id not in results:
                    results[cp.company_id] = []

                results[cp.company_id].append({
                    "company_person_id": cp.id,
                    "person_id": person.id,
                    "company_id": cp.company_id,
                    "name": person.full_name,
                    "title": cp.title or "",
                    "title_level": cp.title_level,
                    "management_level": cp.management_level,
                    "reports_to_id": cp.reports_to_id,
                    "department": cp.department,
                })

        return results

    def _is_c_level_tech(self, title: str, func_config: Dict) -> bool:
        """Check if a title is C-level for this function."""
        title_lower = (title or "").lower()

        c_level_patterns = [
            p for p in func_config["title_patterns"]
            if any(k in p for k in ['chief', 'cto', 'cio', 'cfo', 'ciso', 'cdo', 'clo', 'general counsel'])
        ]

        for pattern in c_level_patterns:
            if re.search(pattern, title_lower):
                return True
        return False

    async def _fill_gaps_via_linkedin(
        self,
        gaps: List[Dict],
        func_config: Dict,
        function_name: str,
        max_searches: int,
    ) -> List[ExtractedPerson]:
        """
        Use LinkedIn discovery to find missing functional leaders.
        """
        from app.sources.people_collection.linkedin_discovery import LinkedInDiscovery

        discovery = LinkedInDiscovery()
        all_people: List[ExtractedPerson] = []

        try:
            searches_remaining = max_searches

            for gap in gaps:
                if searches_remaining <= 0:
                    break

                company_name = gap["company_name"]
                searches_for_company = min(
                    searches_remaining,
                    10 if gap["missing"] == "c_level" else 5,
                )

                people = await discovery.discover_people(
                    company_name=company_name,
                    target_department=function_name,
                    max_searches=searches_for_company,
                )

                for p in people:
                    p.extraction_notes = (
                        f"LinkedIn discovery for {function_name} at {company_name}"
                    )

                all_people.extend(people)
                searches_remaining -= searches_for_company

                logger.info(
                    f"[FunctionalOrgMapper] LinkedIn gap-fill for {company_name}: "
                    f"found {len(people)} people"
                )

        finally:
            await discovery.close()

        return all_people

    async def _infer_cross_subsidiary_reporting(
        self,
        leaders_by_company: Dict[int, List[Dict]],
        parent_company_id: int,
        company_names: Dict[int, str],
        func_config: Dict,
        db_session: Session,
    ) -> List[Tuple[int, int]]:
        """
        Infer reporting relationships between subsidiary functional leaders
        and parent company functional leaders.

        E.g., subsidiary CTO reports to parent company CTO/CIO.
        """
        updates = []

        # Find parent company's top functional leader
        parent_leaders = leaders_by_company.get(parent_company_id, [])
        parent_c_level = None

        for leader in parent_leaders:
            if self._is_c_level_tech(leader["title"], func_config):
                parent_c_level = leader
                break

        if not parent_c_level:
            logger.info(
                "[FunctionalOrgMapper] No parent-level functional leader found, "
                "skipping cross-subsidiary reporting inference"
            )
            return updates

        # For each subsidiary, find its top functional leader and link to parent
        for cid, leaders in leaders_by_company.items():
            if cid == parent_company_id:
                continue

            if not leaders:
                continue

            # Find the top person at this subsidiary
            sub_top = min(leaders, key=lambda l: l.get("management_level", 99))

            # Only link if they don't already have a reports_to
            if sub_top.get("reports_to_id"):
                continue

            # Infer: subsidiary top functional leader -> parent functional leader
            try:
                cp = db_session.get(CompanyPerson, sub_top["company_person_id"])
                if cp:
                    cp.reports_to_id = parent_c_level["company_person_id"]
                    updates.append((sub_top["company_person_id"], parent_c_level["company_person_id"]))

                    logger.debug(
                        f"[FunctionalOrgMapper] Linked {sub_top['name']} "
                        f"({company_names.get(cid)}) -> {parent_c_level['name']}"
                    )
            except Exception as e:
                logger.warning(
                    f"[FunctionalOrgMapper] Failed to link reporting: {e}"
                )

        if updates:
            db_session.commit()

        logger.info(
            f"[FunctionalOrgMapper] Inferred {len(updates)} cross-subsidiary "
            f"reporting relationships"
        )

        return updates

    def _build_functional_chart(
        self,
        all_leaders: List[Dict],
        company_names: Dict[int, str],
        function_name: str,
    ) -> Dict[str, Any]:
        """
        Build a hierarchical chart of functional leaders.
        """
        # Group by management level
        by_level: Dict[int, List[Dict]] = {}
        for leader in all_leaders:
            level = leader.get("management_level", 99)
            if level not in by_level:
                by_level[level] = []
            by_level[level].append(leader)

        # Build simplified chart
        chart = {
            "function": function_name,
            "levels": {},
        }

        level_names = {
            1: "CEO/Executive",
            2: "C-Suite",
            3: "EVP",
            4: "SVP",
            5: "VP",
            6: "Director",
            7: "Manager",
        }

        for level in sorted(by_level.keys()):
            level_label = level_names.get(level, f"Level {level}")
            chart["levels"][level_label] = [
                {
                    "name": l["name"],
                    "title": l["title"],
                    "company": company_names.get(l.get("company_id"), ""),
                    "reports_to_id": l.get("reports_to_id"),
                }
                for l in by_level[level]
            ]

        return chart

    async def close(self):
        """Close resources."""
        if self._linkedin_discovery:
            await self._linkedin_discovery.close()
            self._linkedin_discovery = None
