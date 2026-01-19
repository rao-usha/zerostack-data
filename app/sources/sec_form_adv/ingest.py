"""
SEC Form ADV Ingestion Service.

Stores investment adviser data in PostgreSQL.
"""

import logging
import json
from typing import Dict, List, Optional, Any
from datetime import datetime

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.sources.sec_form_adv.client import FormADVClient

logger = logging.getLogger(__name__)


class FormADVIngestionService:
    """
    Service for ingesting and storing Form ADV adviser data.
    """

    def __init__(self, db: Session):
        self.db = db
        self.client = FormADVClient()
        self._ensure_tables()

    def _ensure_tables(self):
        """Create tables if they don't exist."""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS form_adv_advisers (
            id SERIAL PRIMARY KEY,
            crd_number VARCHAR(20) NOT NULL UNIQUE,
            sec_number VARCHAR(20),
            legal_name VARCHAR(500) NOT NULL,
            dba_name VARCHAR(500),
            website VARCHAR(500),
            main_office_address TEXT,
            main_office_city VARCHAR(100),
            main_office_state VARCHAR(10),
            main_office_country VARCHAR(50) DEFAULT 'United States',
            main_office_zip VARCHAR(20),
            regulatory_aum BIGINT,
            discretionary_aum BIGINT,
            non_discretionary_aum BIGINT,
            total_accounts INTEGER,
            discretionary_accounts INTEGER,
            pct_individuals INTEGER DEFAULT 0,
            pct_high_net_worth INTEGER DEFAULT 0,
            pct_banking_institutions INTEGER DEFAULT 0,
            pct_investment_companies INTEGER DEFAULT 0,
            pct_pension_plans INTEGER DEFAULT 0,
            pct_pooled_investment_vehicles INTEGER DEFAULT 0,
            pct_charitable_organizations INTEGER DEFAULT 0,
            pct_corporations INTEGER DEFAULT 0,
            pct_state_municipal INTEGER DEFAULT 0,
            pct_other INTEGER DEFAULT 0,
            total_employees INTEGER,
            employees_investment_advisory INTEGER,
            employees_registered_reps INTEGER,
            sec_registered BOOLEAN DEFAULT TRUE,
            registration_date DATE,
            fiscal_year_end VARCHAR(10),
            form_of_organization VARCHAR(50),
            country_of_organization VARCHAR(50) DEFAULT 'United States',
            state_of_organization VARCHAR(50),
            has_custody BOOLEAN DEFAULT FALSE,
            custody_client_cash BOOLEAN DEFAULT FALSE,
            custody_client_securities BOOLEAN DEFAULT FALSE,
            fee_types JSONB,
            compensation_types JSONB,
            has_disciplinary_events BOOLEAN DEFAULT FALSE,
            disciplinary_details JSONB,
            filing_date DATE,
            data_source VARCHAR(50) DEFAULT 'sample',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS idx_adv_crd ON form_adv_advisers(crd_number);
        CREATE INDEX IF NOT EXISTS idx_adv_name ON form_adv_advisers(legal_name);
        CREATE INDEX IF NOT EXISTS idx_adv_aum ON form_adv_advisers(regulatory_aum);
        CREATE INDEX IF NOT EXISTS idx_adv_state ON form_adv_advisers(main_office_state);
        """
        try:
            self.db.execute(text(create_table_sql))
            self.db.commit()
            logger.info("Form ADV tables ready")
        except Exception as e:
            logger.warning(f"Table creation warning: {e}")
            self.db.rollback()

    def ingest_sample_data(self) -> Dict[str, Any]:
        """
        Ingest sample adviser data for testing.

        Returns:
            Ingestion result summary
        """
        result = {
            "advisers_found": 0,
            "advisers_ingested": 0,
            "advisers_skipped": 0,
            "errors": []
        }

        sample_advisers = self.client.get_sample_advisers()
        result["advisers_found"] = len(sample_advisers)

        for adviser in sample_advisers:
            try:
                if self._adviser_exists(adviser["crd_number"]):
                    result["advisers_skipped"] += 1
                    continue

                self._store_adviser(adviser)
                result["advisers_ingested"] += 1

            except Exception as e:
                result["errors"].append(f"Error storing {adviser.get('legal_name')}: {str(e)}")

        return result

    def _adviser_exists(self, crd_number: str) -> bool:
        """Check if adviser already exists in database."""
        query = text("SELECT 1 FROM form_adv_advisers WHERE crd_number = :crd LIMIT 1")
        result = self.db.execute(query, {"crd": crd_number})
        return result.fetchone() is not None

    def _store_adviser(self, adviser: Dict):
        """Store adviser data in database."""
        insert_sql = text("""
            INSERT INTO form_adv_advisers (
                crd_number, sec_number, legal_name, dba_name,
                main_office_city, main_office_state,
                regulatory_aum, discretionary_aum, non_discretionary_aum,
                total_employees, employees_investment_advisory,
                sec_registered, form_of_organization,
                pct_individuals, pct_high_net_worth, pct_pension_plans,
                pct_pooled_investment_vehicles, pct_other,
                data_source
            ) VALUES (
                :crd_number, :sec_number, :legal_name, :dba_name,
                :main_office_city, :main_office_state,
                :regulatory_aum, :discretionary_aum, :non_discretionary_aum,
                :total_employees, :employees_investment_advisory,
                :sec_registered, :form_of_organization,
                :pct_individuals, :pct_high_net_worth, :pct_pension_plans,
                :pct_pooled_investment_vehicles, :pct_other,
                :data_source
            )
            ON CONFLICT (crd_number) DO UPDATE SET
                updated_at = CURRENT_TIMESTAMP,
                regulatory_aum = EXCLUDED.regulatory_aum,
                discretionary_aum = EXCLUDED.discretionary_aum
        """)

        non_disc = adviser.get("regulatory_aum", 0) - adviser.get("discretionary_aum", 0)
        if non_disc < 0:
            non_disc = 0

        self.db.execute(insert_sql, {
            "crd_number": adviser.get("crd_number"),
            "sec_number": adviser.get("sec_number"),
            "legal_name": adviser.get("legal_name"),
            "dba_name": adviser.get("dba_name"),
            "main_office_city": adviser.get("main_office_city"),
            "main_office_state": adviser.get("main_office_state"),
            "regulatory_aum": adviser.get("regulatory_aum"),
            "discretionary_aum": adviser.get("discretionary_aum"),
            "non_discretionary_aum": non_disc,
            "total_employees": adviser.get("total_employees"),
            "employees_investment_advisory": adviser.get("employees_investment_advisory"),
            "sec_registered": adviser.get("sec_registered", True),
            "form_of_organization": adviser.get("form_of_organization"),
            "pct_individuals": adviser.get("pct_individuals", 0),
            "pct_high_net_worth": adviser.get("pct_high_net_worth", 0),
            "pct_pension_plans": adviser.get("pct_pension_plans", 0),
            "pct_pooled_investment_vehicles": adviser.get("pct_pooled_investment_vehicles", 0),
            "pct_other": adviser.get("pct_other", 0),
            "data_source": "sample",
        })
        self.db.commit()

    def search_advisers(
        self,
        name: Optional[str] = None,
        state: Optional[str] = None,
        min_aum: Optional[int] = None,
        max_aum: Optional[int] = None,
        limit: int = 50,
        offset: int = 0
    ) -> Dict[str, Any]:
        """
        Search advisers in database.

        Returns:
            Search results with advisers and total count
        """
        conditions = ["1=1"]
        params = {"limit": limit, "offset": offset}

        if name:
            conditions.append("(legal_name ILIKE :name OR dba_name ILIKE :name)")
            params["name"] = f"%{name}%"

        if state:
            conditions.append("main_office_state = :state")
            params["state"] = state.upper()

        if min_aum:
            conditions.append("regulatory_aum >= :min_aum")
            params["min_aum"] = min_aum

        if max_aum:
            conditions.append("regulatory_aum <= :max_aum")
            params["max_aum"] = max_aum

        where_clause = " AND ".join(conditions)

        # Get total count
        count_query = text(f"SELECT COUNT(*) FROM form_adv_advisers WHERE {where_clause}")
        total = self.db.execute(count_query, params).scalar()

        # Get advisers
        query = text(f"""
            SELECT crd_number, sec_number, legal_name, dba_name,
                   main_office_city, main_office_state,
                   regulatory_aum, discretionary_aum,
                   total_employees, form_of_organization
            FROM form_adv_advisers
            WHERE {where_clause}
            ORDER BY regulatory_aum DESC NULLS LAST
            LIMIT :limit OFFSET :offset
        """)

        result = self.db.execute(query, params)
        advisers = []
        for row in result.mappings():
            advisers.append({
                "crd_number": row["crd_number"],
                "sec_number": row["sec_number"],
                "legal_name": row["legal_name"],
                "dba_name": row["dba_name"],
                "location": f"{row['main_office_city']}, {row['main_office_state']}" if row["main_office_city"] else row["main_office_state"],
                "regulatory_aum": row["regulatory_aum"],
                "discretionary_aum": row["discretionary_aum"],
                "total_employees": row["total_employees"],
                "form_of_organization": row["form_of_organization"],
            })

        return {
            "total": total,
            "limit": limit,
            "offset": offset,
            "advisers": advisers
        }

    def get_adviser(self, crd_number: str) -> Optional[Dict]:
        """Get a specific adviser by CRD number."""
        query = text("""
            SELECT * FROM form_adv_advisers WHERE crd_number = :crd
        """)
        result = self.db.execute(query, {"crd": crd_number})
        row = result.mappings().fetchone()

        if not row:
            return None

        return {
            "crd_number": row["crd_number"],
            "sec_number": row["sec_number"],
            "legal_name": row["legal_name"],
            "dba_name": row["dba_name"],
            "website": row["website"],
            "location": {
                "address": row["main_office_address"],
                "city": row["main_office_city"],
                "state": row["main_office_state"],
                "country": row["main_office_country"],
                "zip": row["main_office_zip"],
            },
            "aum": {
                "regulatory": row["regulatory_aum"],
                "discretionary": row["discretionary_aum"],
                "non_discretionary": row["non_discretionary_aum"],
            },
            "clients": {
                "total_accounts": row["total_accounts"],
                "discretionary_accounts": row["discretionary_accounts"],
                "breakdown": {
                    "individuals": row["pct_individuals"],
                    "high_net_worth": row["pct_high_net_worth"],
                    "banking_institutions": row["pct_banking_institutions"],
                    "investment_companies": row["pct_investment_companies"],
                    "pension_plans": row["pct_pension_plans"],
                    "pooled_investment_vehicles": row["pct_pooled_investment_vehicles"],
                    "charitable_organizations": row["pct_charitable_organizations"],
                    "corporations": row["pct_corporations"],
                    "state_municipal": row["pct_state_municipal"],
                    "other": row["pct_other"],
                }
            },
            "employees": {
                "total": row["total_employees"],
                "investment_advisory": row["employees_investment_advisory"],
                "registered_reps": row["employees_registered_reps"],
            },
            "registration": {
                "sec_registered": row["sec_registered"],
                "registration_date": row["registration_date"].isoformat() if row["registration_date"] else None,
                "form_of_organization": row["form_of_organization"],
                "state_of_organization": row["state_of_organization"],
            },
            "custody": {
                "has_custody": row["has_custody"],
                "client_cash": row["custody_client_cash"],
                "client_securities": row["custody_client_securities"],
            },
            "regulatory": {
                "has_disciplinary_events": row["has_disciplinary_events"],
            },
        }

    def get_aum_rankings(self, limit: int = 20) -> Dict[str, Any]:
        """Get top advisers by AUM."""
        query = text("""
            SELECT crd_number, legal_name, dba_name,
                   main_office_state, regulatory_aum, discretionary_aum,
                   total_employees
            FROM form_adv_advisers
            WHERE regulatory_aum IS NOT NULL
            ORDER BY regulatory_aum DESC
            LIMIT :limit
        """)

        result = self.db.execute(query, {"limit": limit})
        rankings = []
        for i, row in enumerate(result.mappings()):
            rankings.append({
                "rank": i + 1,
                "crd_number": row["crd_number"],
                "legal_name": row["legal_name"],
                "dba_name": row["dba_name"],
                "state": row["main_office_state"],
                "regulatory_aum": row["regulatory_aum"],
                "discretionary_aum": row["discretionary_aum"],
                "total_employees": row["total_employees"],
            })

        # Get totals
        totals_query = text("""
            SELECT COUNT(*) as count,
                   SUM(COALESCE(regulatory_aum, 0)) as total_aum
            FROM form_adv_advisers
        """)
        totals = self.db.execute(totals_query).mappings().fetchone()

        return {
            "rankings": rankings,
            "total_advisers": totals["count"],
            "total_aum": totals["total_aum"],
        }

    def get_stats(self) -> Dict[str, Any]:
        """Get aggregate statistics."""
        stats_query = text("""
            SELECT
                COUNT(*) as total_advisers,
                SUM(COALESCE(regulatory_aum, 0)) as total_aum,
                AVG(COALESCE(regulatory_aum, 0)) as avg_aum,
                SUM(COALESCE(total_employees, 0)) as total_employees,
                COUNT(DISTINCT main_office_state) as states_represented
            FROM form_adv_advisers
        """)
        result = self.db.execute(stats_query).mappings().fetchone()

        # By state
        state_query = text("""
            SELECT main_office_state as state, COUNT(*) as count,
                   SUM(COALESCE(regulatory_aum, 0)) as total_aum
            FROM form_adv_advisers
            WHERE main_office_state IS NOT NULL
            GROUP BY main_office_state
            ORDER BY total_aum DESC
            LIMIT 10
        """)
        states = self.db.execute(state_query).mappings().fetchall()

        # By organization type
        org_query = text("""
            SELECT form_of_organization as org_type, COUNT(*) as count
            FROM form_adv_advisers
            WHERE form_of_organization IS NOT NULL
            GROUP BY form_of_organization
            ORDER BY count DESC
        """)
        orgs = self.db.execute(org_query).mappings().fetchall()

        return {
            "total_advisers": result["total_advisers"],
            "total_aum": result["total_aum"],
            "average_aum": int(result["avg_aum"]) if result["avg_aum"] else 0,
            "total_employees": result["total_employees"],
            "states_represented": result["states_represented"],
            "by_state": [{"state": r["state"], "count": r["count"], "aum": r["total_aum"]} for r in states],
            "by_organization": [{"type": r["org_type"], "count": r["count"]} for r in orgs],
        }

    def get_by_state(self, state: Optional[str] = None) -> Dict[str, Any]:
        """Get advisers grouped by state."""
        if state:
            query = text("""
                SELECT crd_number, legal_name, dba_name, regulatory_aum
                FROM form_adv_advisers
                WHERE main_office_state = :state
                ORDER BY regulatory_aum DESC NULLS LAST
                LIMIT 50
            """)
            result = self.db.execute(query, {"state": state.upper()})
            advisers = [dict(r) for r in result.mappings()]
            return {
                "state": state.upper(),
                "count": len(advisers),
                "advisers": advisers
            }

        # All states summary
        query = text("""
            SELECT main_office_state as state,
                   COUNT(*) as count,
                   SUM(COALESCE(regulatory_aum, 0)) as total_aum
            FROM form_adv_advisers
            WHERE main_office_state IS NOT NULL
            GROUP BY main_office_state
            ORDER BY count DESC
        """)
        result = self.db.execute(query)
        states = [{"state": r["state"], "count": r["count"], "total_aum": r["total_aum"]}
                  for r in result.mappings()]

        return {
            "total_states": len(states),
            "states": states
        }
