"""
Investor Profile Enrichment Engine.

Enriches LP and Family Office profiles with contacts,
AUM history, and investment preferences.
"""

import json
import logging
from typing import Dict, List, Optional
from datetime import datetime, timedelta
from collections import defaultdict

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


class InvestorEnrichmentEngine:
    """
    Investor profile enrichment engine.

    Analyzes portfolio data to derive investment preferences,
    tracks AUM history, and extracts contact information.
    """

    def __init__(self, db: Session):
        self.db = db

    def _ensure_tables(self) -> None:
        """Ensure enrichment tables exist."""
        create_contacts = text("""
            CREATE TABLE IF NOT EXISTS investor_contacts (
                id SERIAL PRIMARY KEY,
                investor_id INTEGER NOT NULL,
                investor_type VARCHAR(50) NOT NULL,
                name VARCHAR(255) NOT NULL,
                title VARCHAR(255),
                email VARCHAR(255),
                phone VARCHAR(50),
                linkedin_url TEXT,
                role_type VARCHAR(50),
                is_primary BOOLEAN DEFAULT FALSE,
                source VARCHAR(100),
                confidence_score FLOAT DEFAULT 0.5,
                verified_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        create_aum = text("""
            CREATE TABLE IF NOT EXISTS investor_aum_history (
                id SERIAL PRIMARY KEY,
                investor_id INTEGER NOT NULL,
                investor_type VARCHAR(50) NOT NULL,
                aum_usd BIGINT,
                aum_date DATE NOT NULL,
                source VARCHAR(100),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(investor_id, investor_type, aum_date)
            )
        """)

        create_prefs = text("""
            CREATE TABLE IF NOT EXISTS investor_preferences (
                id SERIAL PRIMARY KEY,
                investor_id INTEGER NOT NULL,
                investor_type VARCHAR(50) NOT NULL,
                preferred_sectors JSONB,
                avoided_sectors JSONB,
                preferred_stages JSONB,
                preferred_regions JSONB,
                avg_check_size_usd BIGINT,
                min_check_size_usd BIGINT,
                max_check_size_usd BIGINT,
                investments_per_year FLOAT,
                last_investment_date DATE,
                analyzed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(investor_id, investor_type)
            )
        """)

        try:
            self.db.execute(create_contacts)
            self.db.execute(create_aum)
            self.db.execute(create_prefs)
            self.db.commit()
        except Exception as e:
            logger.warning(f"Table creation warning: {e}")
            self.db.rollback()

    def get_investor_info(self, investor_id: int, investor_type: str) -> Optional[Dict]:
        """Get basic investor information."""
        if investor_type == "lp":
            query = text("""
                SELECT id, name, lp_type as subtype, jurisdiction as location,
                       NULL as aum, website_url as website
                FROM lp_fund WHERE id = :id
            """)
        else:
            query = text("""
                SELECT id, name, type as subtype, region as location,
                       estimated_aum as aum, website
                FROM family_offices WHERE id = :id
            """)

        result = self.db.execute(query, {"id": investor_id})
        row = result.mappings().fetchone()
        return dict(row) if row else None

    def analyze_preferences(self, investor_id: int, investor_type: str) -> Dict:
        """
        Analyze portfolio to derive investment preferences.

        Examines portfolio companies to determine:
        - Preferred sectors/industries
        - Preferred investment stages
        - Geographic preferences
        - Check size patterns
        """
        self._ensure_tables()

        # Get portfolio companies
        query = text("""
            SELECT company_name, company_industry, company_stage,
                   company_location, investment_amount_usd, investment_date
            FROM portfolio_companies
            WHERE investor_id = :investor_id
              AND investor_type = :investor_type
              AND current_holding = 1
        """)
        result = self.db.execute(
            query, {"investor_id": investor_id, "investor_type": investor_type}
        )
        companies = list(result.mappings())

        if not companies:
            return {
                "sectors": [],
                "stages": [],
                "regions": [],
                "check_sizes": {},
                "company_count": 0,
            }

        # Analyze sectors
        sector_counts = defaultdict(int)
        for c in companies:
            industry = c.get("company_industry")
            if industry:
                sector_counts[industry] += 1

        total = len(companies)
        sectors = [
            {"sector": s, "weight": round(count / total, 2), "company_count": count}
            for s, count in sorted(sector_counts.items(), key=lambda x: -x[1])
        ]

        # Analyze stages
        stage_counts = defaultdict(int)
        for c in companies:
            stage = c.get("company_stage")
            if stage:
                stage_counts[stage] += 1

        stages = [
            {"stage": s, "weight": round(count / total, 2), "company_count": count}
            for s, count in sorted(stage_counts.items(), key=lambda x: -x[1])
        ]

        # Analyze regions
        region_counts = defaultdict(int)
        for c in companies:
            location = c.get("company_location")
            if location:
                # Extract region from location (simplified)
                region = self._extract_region(location)
                region_counts[region] += 1

        regions = [
            {"region": r, "weight": round(count / total, 2), "company_count": count}
            for r, count in sorted(region_counts.items(), key=lambda x: -x[1])
        ]

        # Analyze check sizes
        amounts = []
        for c in companies:
            amt = c.get("investment_amount_usd")
            if amt:
                try:
                    # Parse amount (may be string with $, M, B)
                    parsed = self._parse_amount(amt)
                    if parsed:
                        amounts.append(parsed)
                except (ValueError, TypeError):
                    pass

        check_sizes = {}
        if amounts:
            check_sizes = {
                "avg": int(sum(amounts) / len(amounts)),
                "min": min(amounts),
                "max": max(amounts),
                "count": len(amounts),
            }

        # Save preferences
        self._save_preferences(
            investor_id,
            investor_type,
            {
                "sectors": sectors,
                "stages": stages,
                "regions": regions,
                "check_sizes": check_sizes,
            },
        )

        return {
            "sectors": sectors[:10],  # Top 10
            "stages": stages,
            "regions": regions[:10],
            "check_sizes": check_sizes,
            "company_count": total,
        }

    def _extract_region(self, location: str) -> str:
        """Extract region from location string."""
        location_lower = location.lower()

        # US regions
        us_states = [
            "california",
            "new york",
            "texas",
            "florida",
            "massachusetts",
            "washington",
            "colorado",
            "illinois",
            "georgia",
            "virginia",
        ]
        for state in us_states:
            if state in location_lower:
                return "North America"

        if "usa" in location_lower or "united states" in location_lower:
            return "North America"

        # Other regions
        if any(
            x in location_lower for x in ["uk", "london", "germany", "france", "europe"]
        ):
            return "Europe"
        if any(
            x in location_lower
            for x in ["china", "japan", "singapore", "asia", "india"]
        ):
            return "Asia Pacific"
        if any(x in location_lower for x in ["brazil", "mexico", "latin"]):
            return "Latin America"

        return "Other"

    def _parse_amount(self, amount: str) -> Optional[int]:
        """Parse amount string to integer."""
        if isinstance(amount, (int, float)):
            return int(amount)

        if not amount:
            return None

        # Remove common prefixes/suffixes
        amount = str(amount).replace("$", "").replace(",", "").strip()

        multiplier = 1
        if amount.endswith("B") or amount.endswith("b"):
            multiplier = 1_000_000_000
            amount = amount[:-1]
        elif amount.endswith("M") or amount.endswith("m"):
            multiplier = 1_000_000
            amount = amount[:-1]
        elif amount.endswith("K") or amount.endswith("k"):
            multiplier = 1_000
            amount = amount[:-1]

        try:
            return int(float(amount) * multiplier)
        except (ValueError, TypeError):
            return None

    def _save_preferences(
        self, investor_id: int, investor_type: str, prefs: Dict
    ) -> None:
        """Save analyzed preferences to database."""
        query = text("""
            INSERT INTO investor_preferences (
                investor_id, investor_type, preferred_sectors, preferred_stages,
                preferred_regions, avg_check_size_usd, min_check_size_usd,
                max_check_size_usd, analyzed_at
            ) VALUES (
                :investor_id, :investor_type, :sectors, :stages,
                :regions, :avg, :min, :max, NOW()
            )
            ON CONFLICT (investor_id, investor_type) DO UPDATE SET
                preferred_sectors = EXCLUDED.preferred_sectors,
                preferred_stages = EXCLUDED.preferred_stages,
                preferred_regions = EXCLUDED.preferred_regions,
                avg_check_size_usd = EXCLUDED.avg_check_size_usd,
                min_check_size_usd = EXCLUDED.min_check_size_usd,
                max_check_size_usd = EXCLUDED.max_check_size_usd,
                analyzed_at = NOW()
        """)

        check_sizes = prefs.get("check_sizes", {})

        self.db.execute(
            query,
            {
                "investor_id": investor_id,
                "investor_type": investor_type,
                "sectors": json.dumps(prefs.get("sectors", [])),
                "stages": json.dumps(prefs.get("stages", [])),
                "regions": json.dumps(prefs.get("regions", [])),
                "avg": check_sizes.get("avg"),
                "min": check_sizes.get("min"),
                "max": check_sizes.get("max"),
            },
        )
        self.db.commit()

    def calculate_commitment_pace(self, investor_id: int, investor_type: str) -> Dict:
        """
        Calculate investment commitment pace.

        Analyzes investment dates to determine:
        - Investments per year
        - Average days between investments
        - Last investment date
        """
        query = text("""
            SELECT investment_date
            FROM portfolio_companies
            WHERE investor_id = :investor_id
              AND investor_type = :investor_type
              AND investment_date IS NOT NULL
            ORDER BY investment_date DESC
        """)
        result = self.db.execute(
            query, {"investor_id": investor_id, "investor_type": investor_type}
        )
        dates = [row["investment_date"] for row in result.mappings()]

        if not dates:
            return {
                "investments_per_year": 0,
                "last_investment_date": None,
                "avg_days_between_investments": None,
                "total_investments": 0,
            }

        last_date = dates[0]

        # Calculate investments per year
        if len(dates) >= 2:
            first_date = dates[-1]
            days_span = (last_date - first_date).days
            years_span = max(days_span / 365.25, 0.25)  # At least 3 months
            investments_per_year = round(len(dates) / years_span, 1)

            # Calculate average days between investments
            gaps = []
            for i in range(len(dates) - 1):
                gap = (dates[i] - dates[i + 1]).days
                if gap > 0:
                    gaps.append(gap)

            avg_days = int(sum(gaps) / len(gaps)) if gaps else None
        else:
            investments_per_year = len(dates)
            avg_days = None

        # Update preferences table with pace data
        update_query = text("""
            UPDATE investor_preferences
            SET investments_per_year = :pace,
                last_investment_date = :last_date
            WHERE investor_id = :investor_id
              AND investor_type = :investor_type
        """)
        self.db.execute(
            update_query,
            {
                "investor_id": investor_id,
                "investor_type": investor_type,
                "pace": investments_per_year,
                "last_date": last_date,
            },
        )
        self.db.commit()

        return {
            "investments_per_year": investments_per_year,
            "last_investment_date": last_date.isoformat() if last_date else None,
            "avg_days_between_investments": avg_days,
            "total_investments": len(dates),
        }

    def extract_contacts(self, investor_id: int, investor_type: str) -> List[Dict]:
        """
        Extract contacts from available sources.

        Sources:
        - Form ADV filings (for LPs and family offices)
        - Family office contacts table (existing data)
        """
        self._ensure_tables()
        contacts = []

        # Check existing family_office_contacts if applicable
        if investor_type == "family_office":
            query = text("""
                SELECT name, title, email, phone, linkedin_url, role,
                       confidence_level, source
                FROM family_office_contacts
                WHERE family_office_id = :investor_id
            """)
            try:
                result = self.db.execute(query, {"investor_id": investor_id})
                for row in result.mappings():
                    contacts.append(
                        {
                            "name": row["name"],
                            "title": row.get("title"),
                            "email": row.get("email"),
                            "phone": row.get("phone"),
                            "linkedin_url": row.get("linkedin_url"),
                            "role_type": self._classify_role(
                                row.get("title"), row.get("role")
                            ),
                            "is_primary": row.get("role") == "primary",
                            "source": row.get("source", "family_office_contacts"),
                            "confidence_score": self._parse_confidence(
                                row.get("confidence_level")
                            ),
                        }
                    )
            except Exception as e:
                logger.warning(f"Error fetching family office contacts: {e}")

        # Get investor name for Form ADV lookup
        investor_info = self.get_investor_info(investor_id, investor_type)
        if investor_info:
            # Try to extract from Form ADV (placeholder - would integrate with SEC)
            # For now, check if we have any manually entered contacts
            pass

        # Save contacts to investor_contacts table
        for contact in contacts:
            self._save_contact(investor_id, investor_type, contact)

        return contacts

    def _classify_role(self, title: str, role: str = None) -> str:
        """Classify contact role type from title."""
        if not title:
            return "other"

        title_lower = title.lower()

        if any(x in title_lower for x in ["chief investment", "cio"]):
            return "cio"
        if any(x in title_lower for x in ["partner", "managing director", "md"]):
            return "partner"
        if any(x in title_lower for x in ["analyst", "associate"]):
            return "analyst"
        if any(
            x in title_lower for x in ["ceo", "chief executive", "president", "founder"]
        ):
            return "executive"
        if any(x in title_lower for x in ["cfo", "chief financial"]):
            return "cfo"

        return "other"

    def _parse_confidence(self, level: str) -> float:
        """Parse confidence level to float."""
        if not level:
            return 0.5

        level_lower = level.lower()
        if level_lower == "high":
            return 0.9
        if level_lower == "medium":
            return 0.7
        if level_lower == "low":
            return 0.4

        return 0.5

    def _save_contact(
        self, investor_id: int, investor_type: str, contact: Dict
    ) -> None:
        """Save contact to database."""
        query = text("""
            INSERT INTO investor_contacts (
                investor_id, investor_type, name, title, email, phone,
                linkedin_url, role_type, is_primary, source, confidence_score
            ) VALUES (
                :investor_id, :investor_type, :name, :title, :email, :phone,
                :linkedin, :role_type, :is_primary, :source, :confidence
            )
            ON CONFLICT DO NOTHING
        """)

        try:
            self.db.execute(
                query,
                {
                    "investor_id": investor_id,
                    "investor_type": investor_type,
                    "name": contact.get("name"),
                    "title": contact.get("title"),
                    "email": contact.get("email"),
                    "phone": contact.get("phone"),
                    "linkedin": contact.get("linkedin_url"),
                    "role_type": contact.get("role_type"),
                    "is_primary": contact.get("is_primary", False),
                    "source": contact.get("source"),
                    "confidence": contact.get("confidence_score", 0.5),
                },
            )
            self.db.commit()
        except Exception as e:
            logger.warning(f"Error saving contact: {e}")
            self.db.rollback()

    def track_aum_history(self, investor_id: int, investor_type: str) -> List[Dict]:
        """
        Build AUM history from available sources.

        Sources:
        - Current AUM from investor record
        - Historical Form ADV filings
        """
        self._ensure_tables()

        investor_info = self.get_investor_info(investor_id, investor_type)
        if not investor_info:
            return []

        # Get current AUM and save as history point
        current_aum = investor_info.get("aum")
        if current_aum:
            try:
                aum_value = self._parse_amount(current_aum)
                if aum_value:
                    self._save_aum_snapshot(
                        investor_id, investor_type, aum_value, "investor_record"
                    )
            except (ValueError, TypeError):
                pass

        # Get AUM history
        query = text("""
            SELECT aum_usd, aum_date, source
            FROM investor_aum_history
            WHERE investor_id = :investor_id
              AND investor_type = :investor_type
            ORDER BY aum_date DESC
            LIMIT 20
        """)
        result = self.db.execute(
            query, {"investor_id": investor_id, "investor_type": investor_type}
        )

        history = [
            {
                "date": row["aum_date"].isoformat(),
                "aum_usd": row["aum_usd"],
                "source": row["source"],
            }
            for row in result.mappings()
        ]

        return history

    def _save_aum_snapshot(
        self, investor_id: int, investor_type: str, aum_value: int, source: str
    ) -> None:
        """Save AUM snapshot to history."""
        today = datetime.utcnow().date()

        query = text("""
            INSERT INTO investor_aum_history (
                investor_id, investor_type, aum_usd, aum_date, source
            ) VALUES (
                :investor_id, :investor_type, :aum, :date, :source
            )
            ON CONFLICT (investor_id, investor_type, aum_date) DO UPDATE SET
                aum_usd = EXCLUDED.aum_usd,
                source = EXCLUDED.source
        """)

        try:
            self.db.execute(
                query,
                {
                    "investor_id": investor_id,
                    "investor_type": investor_type,
                    "aum": aum_value,
                    "date": today,
                    "source": source,
                },
            )
            self.db.commit()
        except Exception as e:
            logger.warning(f"Error saving AUM snapshot: {e}")
            self.db.rollback()

    async def enrich_investor(self, investor_id: int, investor_type: str) -> Dict:
        """
        Run full enrichment for an investor.

        Performs:
        1. Preference analysis from portfolio
        2. Commitment pace calculation
        3. Contact extraction
        4. AUM history tracking
        """
        self._ensure_tables()

        investor_info = self.get_investor_info(investor_id, investor_type)
        if not investor_info:
            return {"error": "Investor not found"}

        results = {
            "investor_id": investor_id,
            "investor_type": investor_type,
            "investor_name": investor_info.get("name"),
            "preferences": None,
            "commitment_pace": None,
            "contacts": None,
            "aum_history": None,
        }

        # Analyze preferences
        try:
            results["preferences"] = self.analyze_preferences(
                investor_id, investor_type
            )
        except Exception as e:
            logger.error(f"Preference analysis error: {e}")
            results["preferences"] = {"error": str(e)}

        # Calculate commitment pace
        try:
            results["commitment_pace"] = self.calculate_commitment_pace(
                investor_id, investor_type
            )
        except Exception as e:
            logger.error(f"Commitment pace error: {e}")
            results["commitment_pace"] = {"error": str(e)}

        # Extract contacts
        try:
            results["contacts"] = self.extract_contacts(investor_id, investor_type)
        except Exception as e:
            logger.error(f"Contact extraction error: {e}")
            results["contacts"] = []

        # Track AUM history
        try:
            results["aum_history"] = self.track_aum_history(investor_id, investor_type)
        except Exception as e:
            logger.error(f"AUM history error: {e}")
            results["aum_history"] = []

        return results

    def get_contacts(self, investor_id: int, investor_type: str) -> List[Dict]:
        """Get stored contacts for an investor."""
        self._ensure_tables()

        query = text("""
            SELECT name, title, email, phone, linkedin_url,
                   role_type, is_primary, source, confidence_score
            FROM investor_contacts
            WHERE investor_id = :investor_id
              AND investor_type = :investor_type
            ORDER BY is_primary DESC, confidence_score DESC
        """)
        result = self.db.execute(
            query, {"investor_id": investor_id, "investor_type": investor_type}
        )

        return [dict(row) for row in result.mappings()]

    def get_aum_history(self, investor_id: int, investor_type: str) -> Dict:
        """Get AUM history for an investor."""
        history = self.track_aum_history(investor_id, investor_type)

        if not history:
            return {"current_aum_usd": None, "history": [], "growth_rate_1y": None}

        current = history[0]["aum_usd"] if history else None

        # Calculate 1-year growth rate
        growth_rate = None
        if len(history) >= 2:
            one_year_ago = datetime.utcnow().date() - timedelta(days=365)
            for item in history:
                item_date = datetime.fromisoformat(item["date"]).date()
                if item_date <= one_year_ago:
                    if current and item["aum_usd"]:
                        growth_rate = round(
                            (current - item["aum_usd"]) / item["aum_usd"], 3
                        )
                    break

        return {
            "current_aum_usd": current,
            "history": history,
            "growth_rate_1y": growth_rate,
        }

    def get_preferences(self, investor_id: int, investor_type: str) -> Dict:
        """Get stored preferences for an investor."""
        self._ensure_tables()

        query = text("""
            SELECT preferred_sectors, preferred_stages, preferred_regions,
                   avg_check_size_usd, min_check_size_usd, max_check_size_usd,
                   investments_per_year, last_investment_date, analyzed_at
            FROM investor_preferences
            WHERE investor_id = :investor_id
              AND investor_type = :investor_type
        """)
        result = self.db.execute(
            query, {"investor_id": investor_id, "investor_type": investor_type}
        )
        row = result.mappings().fetchone()

        if not row:
            # Run analysis if no stored preferences
            return self.analyze_preferences(investor_id, investor_type)

        return {
            "sectors": row["preferred_sectors"] or [],
            "stages": row["preferred_stages"] or [],
            "regions": row["preferred_regions"] or [],
            "check_sizes": {
                "avg": row["avg_check_size_usd"],
                "min": row["min_check_size_usd"],
                "max": row["max_check_size_usd"],
            },
            "commitment_pace": {
                "investments_per_year": row["investments_per_year"],
                "last_investment_date": row["last_investment_date"].isoformat()
                if row["last_investment_date"]
                else None,
            },
            "analyzed_at": row["analyzed_at"].isoformat()
            if row["analyzed_at"]
            else None,
        }
