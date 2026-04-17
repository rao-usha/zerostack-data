"""
Deal Probability Engine — Company Universe Builder (SPEC 045, PLAN_059 Phase 1).

Builds the universe of companies scored by the Deal Probability Engine by
unioning three sources:
  1. pe_portfolio_companies (active PE/VC-backed + private)
  2. industrial_companies (employee_count > 50)
  3. form_d_filings (recent 12 months — private issuers raising capital)

Dedupe is performed via normalized name + sector. Each source's rows are
upserted into txn_prob_companies with universe_source tracking so we can
attribute signal quality back to the origin.

Graceful degradation: if any source table is missing or empty, the builder
continues with the remaining sources and returns accurate counters.
"""

import logging
import re
from typing import Dict, List

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.core.probability_models import TxnProbCompany

logger = logging.getLogger(__name__)


# Build configuration
MIN_INDUSTRIAL_EMPLOYEE_COUNT = 50
FORM_D_LOOKBACK_DAYS = 365

# Ownership statuses that qualify for scoring (exclude Public — different signal set)
QUALIFYING_OWNERSHIP = {"PE-Backed", "VC-Backed", "Private"}


class CompanyUniverseBuilder:
    """
    Builds and refreshes the scored company universe.

    Usage:
        builder = CompanyUniverseBuilder(db)
        stats = builder.build_universe()
        # {'inserted': 120, 'updated': 5, 'skipped': 0, 'total': 125, 'by_source': {...}}
    """

    def __init__(self, db: Session):
        self.db = db

    # -------------------------------------------------------------------
    # Public API
    # -------------------------------------------------------------------

    def build_universe(self) -> Dict:
        """
        Build (or extend) the universe from all three sources.

        Returns a stats dict:
            {
                "inserted": int,
                "updated": int,
                "skipped": int,
                "total": int,
                "by_source": {"pe_portfolio": int, "industrial": int, "form_d": int},
            }
        """
        stats = {
            "inserted": 0,
            "updated": 0,
            "skipped": 0,
            "total": 0,
            "by_source": {"pe_portfolio": 0, "industrial": 0, "form_d": 0},
        }

        for source_name, loader in (
            ("pe_portfolio", self._load_from_pe_portfolio),
            ("industrial", self._load_from_industrial),
            ("form_d", self._load_from_form_d),
        ):
            try:
                rows = loader()
            except Exception as exc:  # pragma: no cover - defensive
                logger.warning("universe source %s failed: %s", source_name, exc)
                rows = []

            stats["by_source"][source_name] = len(rows)
            for row in rows:
                outcome = self._upsert_company(row, source=source_name)
                stats[outcome] += 1

            # Commit after each source so a later source's failure (including
            # a loader query rollback) can't clobber earlier inserts.
            try:
                self.db.commit()
            except Exception as exc:
                logger.warning("commit after %s failed: %s", source_name, exc)
                self.db.rollback()

        stats["total"] = self.db.query(TxnProbCompany).count()
        return stats

    def refresh_universe(self) -> Dict:
        """
        Daily refresh: adds new companies, marks stale ones inactive.

        A company becomes inactive if it's absent from all three sources in
        the current refresh pass. Phase 1 just calls build_universe — the
        inactive-marking heuristic is reserved for Phase 2+ when we have
        signal data to judge staleness.
        """
        return self.build_universe()

    # -------------------------------------------------------------------
    # Source loaders
    # -------------------------------------------------------------------

    def _load_from_pe_portfolio(self) -> List[Dict]:
        """Active PE/VC-backed/private companies from pe_portfolio_companies."""
        sql = text(
            """
            SELECT
                id,
                name,
                industry,
                sector,
                naics_code,
                headquarters_state,
                headquarters_city,
                employee_count,
                founded_year,
                ownership_status
            FROM pe_portfolio_companies
            WHERE status = 'Active'
              AND ownership_status = ANY(:qualifying)
              AND name IS NOT NULL
            """
        )
        try:
            result = self.db.execute(
                sql, {"qualifying": list(QUALIFYING_OWNERSHIP)}
            ).mappings()
            return [
                {
                    "company_name": r["name"],
                    "canonical_company_id": r["id"],
                    "sector": r["sector"] or r["industry"],
                    "industry": r["industry"],
                    "naics_code": r["naics_code"],
                    "hq_state": r["headquarters_state"],
                    "hq_city": r["headquarters_city"],
                    "employee_count_est": r["employee_count"],
                    "founded_year": r["founded_year"],
                    "ownership_status": r["ownership_status"],
                }
                for r in result
            ]
        except Exception as exc:
            self.db.rollback()
            logger.debug("pe_portfolio load failed: %s", exc)
            return []

    def _load_from_industrial(self) -> List[Dict]:
        """Industrial companies with meaningful headcount."""
        sql = text(
            """
            SELECT
                id,
                name,
                industry_segment,
                sub_segment,
                naics_code,
                headquarters_state,
                headquarters_city,
                employee_count,
                founded_year,
                ownership_type
            FROM industrial_companies
            WHERE name IS NOT NULL
              AND (employee_count IS NULL OR employee_count >= :min_employees)
            """
        )
        try:
            result = self.db.execute(
                sql, {"min_employees": MIN_INDUSTRIAL_EMPLOYEE_COUNT}
            ).mappings()
            return [
                {
                    "company_name": r["name"],
                    "canonical_company_id": None,
                    "sector": r["industry_segment"],
                    "industry": r["sub_segment"] or r["industry_segment"],
                    "naics_code": r["naics_code"],
                    "hq_state": r["headquarters_state"],
                    "hq_city": r["headquarters_city"],
                    "employee_count_est": r["employee_count"],
                    "founded_year": r["founded_year"],
                    "ownership_status": r["ownership_type"] or "Private",
                }
                for r in result
            ]
        except Exception as exc:
            self.db.rollback()
            logger.debug("industrial load failed: %s", exc)
            return []

    def _load_from_form_d(self) -> List[Dict]:
        """
        Recent Form D filers — private companies raising capital.

        Collapses multiple filings per issuer to one record, taking the most
        recent filing's industry/state metadata.
        """
        sql = text(
            """
            SELECT DISTINCT ON (LOWER(issuer_name))
                issuer_name,
                industry_group,
                issuer_state
            FROM form_d_filings
            WHERE issuer_name IS NOT NULL
              AND filing_date >= NOW() - make_interval(days => :lookback)
            ORDER BY LOWER(issuer_name), filing_date DESC
            """
        )
        try:
            result = self.db.execute(sql, {"lookback": FORM_D_LOOKBACK_DAYS}).mappings()
            return [
                {
                    "company_name": r["issuer_name"],
                    "canonical_company_id": None,
                    "sector": r["industry_group"],
                    "industry": r["industry_group"],
                    "naics_code": None,
                    "hq_state": r["issuer_state"],
                    "hq_city": None,
                    "employee_count_est": None,
                    "founded_year": None,
                    "ownership_status": "Private",
                }
                for r in result
            ]
        except Exception as exc:
            self.db.rollback()
            logger.debug("form_d load failed: %s", exc)
            return []

    # -------------------------------------------------------------------
    # Upsert + dedup
    # -------------------------------------------------------------------

    def _upsert_company(self, row: Dict, source: str) -> str:
        """
        Insert or update a company. Returns outcome string: inserted|updated|skipped.

        Dedup key: (normalized_name, sector). If a row already exists for this
        key, we enrich missing fields but do not overwrite good data.
        """
        name = row.get("company_name")
        if not name:
            return "skipped"

        normalized = self._normalize_name(name)
        sector = row.get("sector")

        existing = (
            self.db.query(TxnProbCompany)
            .filter(
                TxnProbCompany.normalized_name == normalized,
                TxnProbCompany.sector == sector,
            )
            .first()
        )

        if existing:
            # Enrich: fill fields that are currently null
            changed = False
            for field in (
                "canonical_company_id",
                "industry",
                "naics_code",
                "hq_state",
                "hq_city",
                "employee_count_est",
                "founded_year",
                "ownership_status",
            ):
                if getattr(existing, field) is None and row.get(field) is not None:
                    setattr(existing, field, row[field])
                    changed = True
            if changed:
                return "updated"
            return "skipped"

        company = TxnProbCompany(
            company_name=name,
            normalized_name=normalized,
            canonical_company_id=row.get("canonical_company_id"),
            sector=sector,
            industry=row.get("industry"),
            naics_code=row.get("naics_code"),
            hq_state=row.get("hq_state"),
            hq_city=row.get("hq_city"),
            employee_count_est=row.get("employee_count_est"),
            founded_year=row.get("founded_year"),
            ownership_status=row.get("ownership_status"),
            universe_source=source,
            is_active=True,
        )
        # Use a savepoint so a flush failure (e.g. unique collision) rolls
        # back only this row, not the whole batch.
        savepoint = self.db.begin_nested()
        try:
            self.db.add(company)
            self.db.flush()
            savepoint.commit()
            return "inserted"
        except Exception as exc:
            savepoint.rollback()
            logger.debug("upsert collision for %s: %s", name, exc)
            return "skipped"

    @staticmethod
    def _normalize_name(name: str) -> str:
        """
        Normalize a company name for dedup matching.

        Rules: lowercase, strip whitespace, remove common suffixes
        (Inc, LLC, Corp, Co, Ltd, LP, Holdings), collapse punctuation.
        """
        if not name:
            return ""
        n = name.lower().strip()
        n = re.sub(r"[.,&()\"']", " ", n)
        n = re.sub(
            r"\b(inc(orporated)?|llc|corp(oration)?|co|ltd|lp|limited|holdings?|"
            r"group|company|companies|partners|capital)\b",
            " ",
            n,
        )
        n = re.sub(r"\s+", " ", n).strip()
        return n
