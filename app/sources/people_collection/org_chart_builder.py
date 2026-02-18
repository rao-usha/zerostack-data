"""
LLM-powered org chart construction.

After deep collection gathers 100+ people, this module builds the organizational
hierarchy using a multi-pass approach:

Pass 1 - Title-based hierarchy (no LLM): Map titles to management levels
Pass 2 - Division grouping (LLM): Group people into business units
Pass 3 - Reporting chain inference (LLM): Infer reports_to relationships
Pass 4 - Store: Update company_people and create org_chart_snapshots entry
"""

import json
import logging
import re
from datetime import date
from typing import List, Optional, Dict, Any, Tuple

from sqlalchemy.orm import Session

from app.core.people_models import (
    CompanyPerson,
    Person,
    OrgChartSnapshot,
)
from app.sources.people_collection.types import (
    ExtractedPerson,
    TitleLevel,
)

logger = logging.getLogger(__name__)


# Title patterns mapped to management levels (1 = CEO, higher = lower rank)
TITLE_LEVEL_MAP = {
    TitleLevel.C_SUITE: 2,
    TitleLevel.PRESIDENT: 2,
    TitleLevel.EVP: 3,
    TitleLevel.SVP: 4,
    TitleLevel.VP: 5,
    TitleLevel.DIRECTOR: 6,
    TitleLevel.MANAGER: 7,
    TitleLevel.BOARD: 2,  # Board members at same level as C-suite
    TitleLevel.INDIVIDUAL: 8,
    TitleLevel.UNKNOWN: 9,
}

# CEO-specific titles that go to level 1
CEO_PATTERNS = [
    r'\bchief executive officer\b',
    r'\bceo\b',
    r'\bchairman.*ceo\b',
    r'\bceo.*chairman\b',
    r'\bpresident.*ceo\b',
    r'\bceo.*president\b',
]


class OrgChartBuilder:
    """
    Builds organizational hierarchy from collected people data.

    Uses a multi-pass approach:
    1. Title-based hierarchy assignment
    2. LLM-powered division grouping
    3. LLM-powered reporting chain inference
    4. Database storage
    """

    def __init__(self):
        self._llm_client = None

    def _get_llm_client(self):
        """Get or create OpenAI client for org chart inference."""
        if self._llm_client is None:
            import openai
            self._llm_client = openai.OpenAI()
        return self._llm_client

    async def build_org_chart(
        self,
        company_id: int,
        company_name: str,
        db_session: Session,
        division_context: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Build org chart from people already stored in the database.

        Args:
            company_id: Company database ID
            company_name: Company name
            db_session: SQLAlchemy session
            division_context: Optional context about company divisions
                (e.g., "Prudential has divisions: PGIM, Retirement, Group Insurance")

        Returns:
            Dict with org chart data and stats
        """
        logger.info(f"[OrgChartBuilder] Building org chart for {company_name} (id={company_id})")

        # Load all current people for this company
        company_people = db_session.query(CompanyPerson, Person).join(
            Person, CompanyPerson.person_id == Person.id
        ).filter(
            CompanyPerson.company_id == company_id,
            CompanyPerson.is_current == True,
        ).all()

        if not company_people:
            logger.warning(f"[OrgChartBuilder] No people found for company {company_id}")
            return {"error": "No people found", "total_people": 0}

        logger.info(f"[OrgChartBuilder] Found {len(company_people)} people for {company_name}")

        # Pass 1: Title-based hierarchy
        people_data = self._pass1_title_hierarchy(company_people)

        # Pass 2: Division grouping (LLM)
        if len(people_data) >= 5:
            people_data = await self._pass2_division_grouping(
                people_data, company_name, division_context
            )

        # Pass 3: Reporting chain inference (LLM)
        if len(people_data) >= 3:
            people_data = await self._pass3_reporting_chains(
                people_data, company_name
            )

        # Pass 4: Store results
        chart_data = self._pass4_store(
            people_data, company_id, company_name, db_session
        )

        logger.info(
            f"[OrgChartBuilder] Org chart built for {company_name}: "
            f"{chart_data.get('total_people', 0)} people, "
            f"{chart_data.get('max_depth', 0)} depth levels, "
            f"{len(chart_data.get('departments', []))} departments"
        )

        return chart_data

    def _pass1_title_hierarchy(
        self,
        company_people: List[Tuple[CompanyPerson, Person]],
    ) -> List[Dict[str, Any]]:
        """
        Pass 1: Assign management levels based on titles.

        No LLM needed - pure pattern matching.
        """
        logger.info("[OrgChartBuilder] Pass 1: Title-based hierarchy assignment")

        people_data = []

        for cp, person in company_people:
            title = cp.title or ""
            title_lower = title.lower()

            # Determine management level
            management_level = 9  # Default to unknown

            # Check for CEO first (level 1)
            is_ceo = any(re.search(p, title_lower) for p in CEO_PATTERNS)
            if is_ceo:
                management_level = 1
            else:
                # Map title_level to management level
                title_level_str = cp.title_level or "unknown"
                try:
                    title_level = TitleLevel(title_level_str)
                except ValueError:
                    title_level = TitleLevel.UNKNOWN
                management_level = TITLE_LEVEL_MAP.get(title_level, 9)

            # Refine based on specific title patterns
            if management_level > 2:
                if 'chairman' in title_lower and 'vice' not in title_lower:
                    management_level = 1
                elif 'president' in title_lower and 'vice' not in title_lower:
                    # President of a division = level 3, president of company = level 2
                    management_level = min(management_level, 2)

            people_data.append({
                "company_person_id": cp.id,
                "person_id": person.id,
                "name": person.full_name,
                "title": title,
                "title_level": cp.title_level,
                "management_level": management_level,
                "is_board_member": cp.is_board_member,
                "department": cp.department,
                "division": None,  # To be set in Pass 2
                "reports_to_id": None,  # To be set in Pass 3
                "reports_to_name": None,
            })

        # Sort by management level
        people_data.sort(key=lambda p: p["management_level"])

        logger.info(
            f"[OrgChartBuilder] Pass 1 complete: "
            f"Level 1 (CEO): {sum(1 for p in people_data if p['management_level'] == 1)}, "
            f"Level 2 (C-suite): {sum(1 for p in people_data if p['management_level'] == 2)}, "
            f"Level 3 (EVP): {sum(1 for p in people_data if p['management_level'] == 3)}, "
            f"Level 4 (SVP): {sum(1 for p in people_data if p['management_level'] == 4)}, "
            f"Level 5 (VP): {sum(1 for p in people_data if p['management_level'] == 5)}"
        )

        return people_data

    async def _pass2_division_grouping(
        self,
        people_data: List[Dict[str, Any]],
        company_name: str,
        division_context: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Pass 2: Group people into business divisions using LLM.
        """
        logger.info("[OrgChartBuilder] Pass 2: LLM division grouping")

        # Build a summary of people for the LLM
        people_summary = []
        for p in people_data:
            people_summary.append(f"- {p['name']}: {p['title']}")

        people_text = "\n".join(people_summary)

        context = ""
        if division_context:
            context = f"\nKnown division structure: {division_context}\n"

        prompt = (
            f"You are analyzing the organizational structure of {company_name}.{context}\n\n"
            f"Below is a list of executives and their titles. Group each person into "
            f"the most appropriate business division or function. "
            f"Return a JSON object where each key is a division name (e.g., 'Corporate', "
            f"'Investment Management', 'Insurance', 'Finance', 'Legal', 'Technology', etc.) "
            f"and the value is a list of person names belonging to that division.\n\n"
            f"For people whose division is unclear from their title, use 'Corporate' as default.\n\n"
            f"People:\n{people_text}\n\n"
            f"Return ONLY valid JSON, no markdown."
        )

        try:
            import asyncio
            client = self._get_llm_client()

            response = await asyncio.get_running_loop().run_in_executor(
                None,
                lambda: client.chat.completions.create(
                    model="gpt-4o",
                    messages=[{"role": "user", "content": prompt}],
                    max_tokens=4000,
                    temperature=0.1,
                )
            )

            # Track LLM cost
            try:
                from app.core.llm_cost_tracker import get_cost_tracker
                tracker = get_cost_tracker()
                in_tok = response.usage.prompt_tokens if response.usage else 0
                out_tok = response.usage.completion_tokens if response.usage else 0
                await tracker.record(
                    model="gpt-4o",
                    input_tokens=in_tok,
                    output_tokens=out_tok,
                    source="org_chart",
                    prompt_chars=len(prompt),
                )
            except Exception as track_err:
                logger.debug(f"[OrgChartBuilder] Cost tracking failed: {track_err}")

            text = response.choices[0].message.content.strip()

            # Parse JSON
            # Remove markdown code fences if present
            if text.startswith("```"):
                text = re.sub(r'^```(?:json)?\n?', '', text)
                text = re.sub(r'\n?```$', '', text)

            divisions = json.loads(text)

            # Build name -> division mapping
            name_to_division = {}
            for division, names in divisions.items():
                for name in names:
                    name_to_division[name.lower().strip()] = division

            # Apply divisions to people data
            for p in people_data:
                name_lower = p["name"].lower().strip()
                p["division"] = name_to_division.get(name_lower, "Corporate")

            divisions_found = set(p["division"] for p in people_data)
            logger.info(
                f"[OrgChartBuilder] Pass 2 complete: {len(divisions_found)} divisions identified: "
                f"{', '.join(sorted(divisions_found))}"
            )

        except Exception as e:
            logger.warning(f"[OrgChartBuilder] Pass 2 LLM failed: {e}")
            # Fallback: assign all to Corporate
            for p in people_data:
                p["division"] = "Corporate"

        return people_data

    async def _pass3_reporting_chains(
        self,
        people_data: List[Dict[str, Any]],
        company_name: str,
    ) -> List[Dict[str, Any]]:
        """
        Pass 3: Infer reporting chains within each division using LLM.
        """
        logger.info("[OrgChartBuilder] Pass 3: LLM reporting chain inference")

        # Group by division
        divisions: Dict[str, List[Dict]] = {}
        for p in people_data:
            div = p.get("division", "Corporate")
            if div not in divisions:
                divisions[div] = []
            divisions[div].append(p)

        # Find CEO (level 1) - everyone at level 2 reports to CEO
        ceo = None
        for p in people_data:
            if p["management_level"] == 1:
                ceo = p
                break

        # For level 2 people, they report to CEO
        if ceo:
            for p in people_data:
                if p["management_level"] == 2 and p["company_person_id"] != ceo["company_person_id"]:
                    p["reports_to_id"] = ceo["company_person_id"]
                    p["reports_to_name"] = ceo["name"]

        # For each division, infer reporting chains for levels 3+
        for division_name, division_people in divisions.items():
            if len(division_people) < 2:
                continue

            # Find the division head (lowest management_level in this division)
            division_people_sorted = sorted(division_people, key=lambda p: p["management_level"])
            division_head = division_people_sorted[0]

            # People at levels 3+ need reporting relationships
            subordinates = [
                p for p in division_people_sorted
                if p["management_level"] > division_head["management_level"]
            ]

            if not subordinates:
                continue

            # Build prompt for LLM
            people_text = "\n".join(
                f"- {p['name']}: {p['title']} (level {p['management_level']})"
                for p in division_people_sorted
            )

            prompt = (
                f"You are analyzing the reporting structure within the '{division_name}' "
                f"division of {company_name}.\n\n"
                f"People in this division (sorted by seniority):\n{people_text}\n\n"
                f"For each person (except the most senior), determine who they most likely "
                f"report to based on their titles and levels. "
                f"Return a JSON array of objects with 'name' and 'reports_to' fields. "
                f"Only include people who have a clear reporting relationship.\n\n"
                f"Return ONLY valid JSON, no markdown."
            )

            try:
                import asyncio
                client = self._get_llm_client()

                response = await asyncio.get_running_loop().run_in_executor(
                    None,
                    lambda: client.chat.completions.create(
                        model="gpt-4o",
                        messages=[{"role": "user", "content": prompt}],
                        max_tokens=2000,
                        temperature=0.1,
                    )
                )

                # Track LLM cost
                try:
                    from app.core.llm_cost_tracker import get_cost_tracker
                    tracker = get_cost_tracker()
                    in_tok = response.usage.prompt_tokens if response.usage else 0
                    out_tok = response.usage.completion_tokens if response.usage else 0
                    await tracker.record(
                        model="gpt-4o",
                        input_tokens=in_tok,
                        output_tokens=out_tok,
                        source="org_chart",
                        prompt_chars=len(prompt),
                    )
                except Exception as track_err:
                    logger.debug(f"[OrgChartBuilder] Cost tracking failed: {track_err}")

                text = response.choices[0].message.content.strip()

                # Parse JSON
                if text.startswith("```"):
                    text = re.sub(r'^```(?:json)?\n?', '', text)
                    text = re.sub(r'\n?```$', '', text)

                reporting_data = json.loads(text)

                # Build name lookup for this division
                name_lookup = {}
                for p in division_people:
                    name_lookup[p["name"].lower().strip()] = p

                # Apply reporting relationships
                if isinstance(reporting_data, list):
                    for entry in reporting_data:
                        name = entry.get("name", "").lower().strip()
                        reports_to = entry.get("reports_to", "").lower().strip()

                        if name in name_lookup and reports_to in name_lookup:
                            person = name_lookup[name]
                            manager = name_lookup[reports_to]
                            person["reports_to_id"] = manager["company_person_id"]
                            person["reports_to_name"] = manager["name"]

            except Exception as e:
                logger.warning(
                    f"[OrgChartBuilder] Pass 3 LLM failed for {division_name}: {e}"
                )
                # Fallback: everyone reports to division head
                for p in subordinates:
                    if not p.get("reports_to_id"):
                        p["reports_to_id"] = division_head["company_person_id"]
                        p["reports_to_name"] = division_head["name"]

        logger.info(
            f"[OrgChartBuilder] Pass 3 complete: "
            f"{sum(1 for p in people_data if p.get('reports_to_id'))} reporting relationships set"
        )

        return people_data

    def _pass4_store(
        self,
        people_data: List[Dict[str, Any]],
        company_id: int,
        company_name: str,
        db_session: Session,
    ) -> Dict[str, Any]:
        """
        Pass 4: Store org chart results in the database.

        Updates company_people with management_level and reports_to_id.
        Creates an org_chart_snapshots entry with hierarchical JSON.
        """
        logger.info("[OrgChartBuilder] Pass 4: Storing org chart results")

        # Update company_people records
        updated = 0
        for p in people_data:
            try:
                cp = db_session.query(CompanyPerson).get(p["company_person_id"])
                if cp:
                    cp.management_level = p["management_level"]
                    cp.reports_to_id = p.get("reports_to_id")
                    if p.get("department") is None and p.get("division"):
                        cp.department = p["division"]
                    updated += 1
            except Exception as e:
                logger.warning(f"[OrgChartBuilder] Failed to update CP {p['company_person_id']}: {e}")

        # Build hierarchical chart data
        chart_data = self._build_chart_json(people_data)

        # Determine departments/divisions
        departments = list(set(
            p.get("division") or p.get("department") or "Unknown"
            for p in people_data
        ))

        # Calculate max depth
        max_depth = max(p["management_level"] for p in people_data) if people_data else 0

        # Create or update snapshot
        today = date.today()
        existing_snapshot = db_session.query(OrgChartSnapshot).filter(
            OrgChartSnapshot.company_id == company_id,
            OrgChartSnapshot.snapshot_date == today,
        ).first()

        if existing_snapshot:
            existing_snapshot.chart_data = chart_data
            existing_snapshot.total_people = len(people_data)
            existing_snapshot.max_depth = max_depth
            existing_snapshot.departments = departments
            existing_snapshot.source = "deep_collection_inferred"
        else:
            snapshot = OrgChartSnapshot(
                company_id=company_id,
                snapshot_date=today,
                chart_data=chart_data,
                total_people=len(people_data),
                max_depth=max_depth,
                departments=departments,
                source="deep_collection_inferred",
            )
            db_session.add(snapshot)

        db_session.commit()

        logger.info(
            f"[OrgChartBuilder] Pass 4 complete: {updated} records updated, "
            f"snapshot saved with {len(people_data)} people"
        )

        return {
            "total_people": len(people_data),
            "max_depth": max_depth,
            "departments": departments,
            "chart_data": chart_data,
            "records_updated": updated,
        }

    def _build_chart_json(
        self,
        people_data: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """
        Build hierarchical JSON representation of the org chart.

        Returns nested structure with root node and children.
        """
        # Build lookup by company_person_id
        by_id = {p["company_person_id"]: p for p in people_data}

        # Build children lookup
        children_of: Dict[int, List[Dict]] = {}
        roots = []

        for p in people_data:
            reports_to = p.get("reports_to_id")
            if reports_to and reports_to in by_id:
                if reports_to not in children_of:
                    children_of[reports_to] = []
                children_of[reports_to].append(p)
            else:
                roots.append(p)

        def build_node(person: Dict) -> Dict:
            cp_id = person["company_person_id"]
            node = {
                "person_id": person["person_id"],
                "company_person_id": cp_id,
                "name": person["name"],
                "title": person["title"],
                "management_level": person["management_level"],
                "division": person.get("division"),
                "is_board_member": person.get("is_board_member", False),
                "children": [],
            }

            for child in children_of.get(cp_id, []):
                node["children"].append(build_node(child))

            # Sort children by management level
            node["children"].sort(key=lambda c: c["management_level"])

            return node

        # Build the tree
        if len(roots) == 1:
            chart = {"root": build_node(roots[0])}
        else:
            # Multiple roots - wrap in a virtual root
            chart = {
                "root": {
                    "name": "Organization",
                    "title": "Top Level",
                    "management_level": 0,
                    "children": [build_node(r) for r in sorted(roots, key=lambda r: r["management_level"])],
                }
            }

        chart["metadata"] = {
            "generated_date": date.today().isoformat(),
            "total_nodes": len(people_data),
            "root_count": len(roots),
        }

        return chart
