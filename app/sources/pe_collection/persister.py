"""
PE Collection Persister.

Writes PECollectedItem objects to the PE database tables.
Handles deduplication, confidence-based merging, FK resolution via caches,
and two-phase processing to respect foreign key dependencies.
"""

import logging
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Dict, List, Optional, Any

from sqlalchemy import func
from sqlalchemy.orm import Session

from app.core.pe_models import (
    PEFirm,
    PEFund,
    PEPortfolioCompany,
    PEFundInvestment,
    PECompanyFinancials,
    PECompanyValuation,
    PEDeal,
    PEDealParticipant,
    PEPerson,
    PEPersonEducation,
    PEPersonExperience,
    PEFirmPeople,
    PEFirmNews,
)
from app.sources.pe_collection.types import PECollectionResult, PECollectedItem

logger = logging.getLogger(__name__)

CONFIDENCE_ORDER = {"high": 4, "medium": 3, "llm_extracted": 2, "low": 1}

# Phase 1 item types create entities; Phase 2 item types reference them
PHASE_1_TYPES = {
    "firm_update",
    "form_adv_filing",
    "portfolio_company",
    "team_member",
    "person",
    "related_person",
    "company_update",
}
PHASE_2_TYPES = {
    "13f_holding",
    "13d_stake",
    "form_d_filing",
    "deal_8k_filing",
    "deal_press_release",
    "deal",
    "firm_news",
    "company_financial",
    "company_valuation",
}


class PEPersister:
    """
    Persists PE collection results to database tables.

    Processes items in two phases:
      Phase 1: Create/update entities (firms, companies, people)
      Phase 2: Create relationships and transactions (deals, holdings, news, financials)

    Each item is wrapped in try/except so one failure doesn't block others.
    """

    def __init__(self, db: Session):
        self.db = db
        self.stats: Dict[str, Any] = {
            "persisted": 0,
            "updated": 0,
            "skipped": 0,
            "failed": 0,
            "errors": [],
        }
        # In-memory caches for FK resolution
        self._firm_cache: Dict[str, int] = {}  # lowercase name -> id
        self._company_cache: Dict[str, int] = {}  # lowercase name -> id
        self._person_cache: Dict[str, int] = {}  # lowercase name or linkedin_url -> id
        self._fund_cache: Dict[int, int] = {}  # firm_id -> synthetic holdings fund id

    # ------------------------------------------------------------------
    # Main entry point
    # ------------------------------------------------------------------

    def persist_results(self, results: List[PECollectionResult]) -> Dict[str, Any]:
        """
        Persist all collected items from all results.

        Items are split into two phases for FK ordering, then dispatched
        to per-item-type handlers.

        Returns:
            Stats dict with persisted/updated/skipped/failed counts.
        """
        # Pre-warm caches
        self._warm_caches()

        # Flatten all items, keeping entity_id from each result
        phase1_items: List[tuple] = []
        phase2_items: List[tuple] = []

        for result in results:
            if not result.success:
                continue
            for item in result.items:
                entry = (result.entity_id, result.entity_name, item)
                if item.item_type in PHASE_1_TYPES:
                    phase1_items.append(entry)
                elif item.item_type in PHASE_2_TYPES:
                    phase2_items.append(entry)
                else:
                    logger.warning(f"Unknown item_type '{item.item_type}', skipping")
                    self.stats["skipped"] += 1

        # Phase 1: entities
        logger.info(f"PE Persister Phase 1: {len(phase1_items)} entity items")
        for entity_id, entity_name, item in phase1_items:
            self._dispatch_item(entity_id, entity_name, item)

        try:
            self.db.commit()
        except Exception as e:
            logger.error(f"Phase 1 commit failed: {e}")
            self.db.rollback()

        # Phase 2: relationships
        logger.info(f"PE Persister Phase 2: {len(phase2_items)} relationship items")
        for entity_id, entity_name, item in phase2_items:
            self._dispatch_item(entity_id, entity_name, item)

        try:
            self.db.commit()
        except Exception as e:
            logger.error(f"Phase 2 commit failed: {e}")
            self.db.rollback()

        logger.info(
            f"PE Persister done: persisted={self.stats['persisted']}, "
            f"updated={self.stats['updated']}, skipped={self.stats['skipped']}, "
            f"failed={self.stats['failed']}"
        )
        return self.stats

    # ------------------------------------------------------------------
    # Dispatcher
    # ------------------------------------------------------------------

    _HANDLER_MAP = {
        "firm_update": "_persist_firm_update",
        "form_adv_filing": "_persist_form_adv_filing",
        "portfolio_company": "_persist_portfolio_company",
        "team_member": "_persist_team_member",
        "person": "_persist_person",
        "related_person": "_persist_related_person",
        "13f_holding": "_persist_13f_holding",
        "13d_stake": "_persist_13d_stake",
        "form_d_filing": "_persist_form_d_filing",
        "deal_8k_filing": "_persist_deal_8k_filing",
        "deal_press_release": "_persist_deal_press_release",
        "deal": "_persist_deal",
        "firm_news": "_persist_firm_news",
        "company_financial": "_persist_company_financial",
        "company_valuation": "_persist_company_valuation",
        "company_update": "_persist_company_update",
    }

    def _dispatch_item(
        self, entity_id: int, entity_name: str, item: PECollectedItem
    ) -> None:
        handler_name = self._HANDLER_MAP.get(item.item_type)
        if not handler_name:
            self.stats["skipped"] += 1
            return
        try:
            handler = getattr(self, handler_name)
            handler(entity_id, entity_name, item)
            self.db.flush()
        except Exception as e:
            logger.error(
                f"Failed to persist {item.item_type} for entity {entity_id}: {e}"
            )
            self.db.rollback()
            # Clear caches — rollback may have removed objects they reference
            self._fund_cache.clear()
            self._company_cache.clear()
            self._person_cache.clear()
            self.stats["failed"] += 1
            self.stats["errors"].append(
                f"{item.item_type}(entity={entity_id}): {str(e)[:200]}"
            )

    # ------------------------------------------------------------------
    # Cache warm-up
    # ------------------------------------------------------------------

    def _warm_caches(self) -> None:
        """Pre-load caches from existing DB rows."""
        for firm in self.db.query(PEFirm.id, PEFirm.name).all():
            self._firm_cache[firm.name.lower()] = firm.id

        for co in self.db.query(PEPortfolioCompany.id, PEPortfolioCompany.name).all():
            self._company_cache[co.name.lower()] = co.id

        for p in self.db.query(
            PEPerson.id, PEPerson.full_name, PEPerson.linkedin_url
        ).all():
            self._person_cache[p.full_name.lower()] = p.id
            if p.linkedin_url:
                self._person_cache[p.linkedin_url] = p.id

        for fund in (
            self.db.query(PEFund.id, PEFund.firm_id)
            .filter(PEFund.strategy == "13F Reported Holdings")
            .all()
        ):
            self._fund_cache[fund.firm_id] = fund.id

    # ------------------------------------------------------------------
    # Helper methods
    # ------------------------------------------------------------------

    def _find_or_create_company(self, name: str, **extra) -> int:
        """Find existing company by name or create a minimal record. Returns id."""
        key = name.strip().lower()
        if key in self._company_cache:
            return self._company_cache[key]

        existing = (
            self.db.query(PEPortfolioCompany)
            .filter(func.lower(PEPortfolioCompany.name) == key)
            .first()
        )
        if existing:
            self._company_cache[key] = existing.id
            return existing.id

        company = PEPortfolioCompany(
            name=name.strip(),
            status="Active",
            **{k: v for k, v in extra.items() if v is not None},
        )
        self.db.add(company)
        self.db.flush()
        self._company_cache[key] = company.id
        return company.id

    def _find_or_create_person(
        self, full_name: str, linkedin_url: Optional[str] = None
    ) -> int:
        """Find existing person or create minimal record. Returns id."""
        # Check linkedin first (most reliable)
        if linkedin_url and linkedin_url in self._person_cache:
            return self._person_cache[linkedin_url]

        key = full_name.strip().lower()
        if key in self._person_cache:
            return self._person_cache[key]

        # DB lookup
        if linkedin_url:
            existing = (
                self.db.query(PEPerson)
                .filter(PEPerson.linkedin_url == linkedin_url)
                .first()
            )
            if existing:
                self._person_cache[linkedin_url] = existing.id
                self._person_cache[key] = existing.id
                return existing.id

        existing = (
            self.db.query(PEPerson)
            .filter(func.lower(PEPerson.full_name) == key)
            .first()
        )
        if existing:
            self._person_cache[key] = existing.id
            if existing.linkedin_url:
                self._person_cache[existing.linkedin_url] = existing.id
            return existing.id

        # Create
        person = PEPerson(full_name=full_name.strip(), linkedin_url=linkedin_url)
        self.db.add(person)
        self.db.flush()
        self._person_cache[key] = person.id
        if linkedin_url:
            self._person_cache[linkedin_url] = person.id
        return person.id

    def _find_or_create_holdings_fund(self, firm_id: int, firm_name: str) -> int:
        """Get or create synthetic '13F Holdings' fund for a firm. Returns fund id."""
        if firm_id in self._fund_cache:
            return self._fund_cache[firm_id]

        existing = (
            self.db.query(PEFund)
            .filter(
                PEFund.firm_id == firm_id,
                PEFund.strategy == "13F Reported Holdings",
            )
            .first()
        )
        if existing:
            self._fund_cache[firm_id] = existing.id
            return existing.id

        fund = PEFund(
            firm_id=firm_id,
            name=f"{firm_name} - 13F Holdings",
            strategy="13F Reported Holdings",
            status="Active",
        )
        self.db.add(fund)
        self.db.flush()
        self._fund_cache[firm_id] = fund.id
        return fund.id

    def _should_update(
        self, new_confidence: str, existing_confidence: Optional[str]
    ) -> bool:
        """Return True if new confidence is >= existing."""
        new_rank = CONFIDENCE_ORDER.get(new_confidence, 1)
        existing_rank = CONFIDENCE_ORDER.get(existing_confidence, 0)
        return new_rank >= existing_rank

    def _null_preserving_update(
        self,
        instance: Any,
        data: Dict[str, Any],
        confidence: str,
        existing_confidence: Optional[str] = None,
        fields: Optional[List[str]] = None,
    ) -> bool:
        """
        Update an ORM instance, filling nulls unconditionally and overwriting
        non-null fields only if new confidence >= existing.

        Returns True if any field was changed.
        """
        changed = False
        can_overwrite = self._should_update(confidence, existing_confidence)
        target_fields = fields or list(data.keys())

        for field in target_fields:
            if field not in data:
                continue
            new_val = data[field]
            if new_val is None:
                continue
            current_val = getattr(instance, field, None)
            if current_val is None:
                setattr(instance, field, new_val)
                changed = True
            elif can_overwrite:
                setattr(instance, field, new_val)
                changed = True
        return changed

    def _parse_date(self, date_str: Any) -> Optional[date]:
        """Best-effort date parsing."""
        if date_str is None:
            return None
        if isinstance(date_str, date):
            return date_str
        if isinstance(date_str, datetime):
            return date_str.date()
        if isinstance(date_str, str):
            for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%m/%d/%Y", "%Y%m%d"):
                try:
                    return datetime.strptime(date_str, fmt).date()
                except ValueError:
                    continue
        return None

    def _parse_datetime(self, dt_str: Any) -> Optional[datetime]:
        """Best-effort datetime parsing."""
        if dt_str is None:
            return None
        if isinstance(dt_str, datetime):
            return dt_str
        if isinstance(dt_str, date):
            return datetime.combine(dt_str, datetime.min.time())
        if isinstance(dt_str, str):
            for fmt in (
                "%Y-%m-%dT%H:%M:%SZ",
                "%Y-%m-%dT%H:%M:%S",
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%d",
            ):
                try:
                    return datetime.strptime(dt_str, fmt)
                except ValueError:
                    continue
        return None

    def _to_decimal(self, value: Any) -> Optional[Decimal]:
        """Convert value to Decimal, returning None on failure."""
        if value is None:
            return None
        try:
            return Decimal(str(value))
        except (InvalidOperation, ValueError, TypeError):
            return None

    # ------------------------------------------------------------------
    # Phase 1 handlers — entity creation / updates
    # ------------------------------------------------------------------

    def _persist_firm_update(
        self, entity_id: int, entity_name: str, item: PECollectedItem
    ) -> None:
        """Update pe_firms with collected metadata."""
        firm = self.db.query(PEFirm).filter(PEFirm.id == entity_id).first()
        if not firm:
            logger.warning(f"firm_update: PEFirm id={entity_id} not found, skipping")
            self.stats["skipped"] += 1
            return

        data = item.data
        update_fields = {
            "headquarters_city": data.get("headquarters_city"),
            "headquarters_state": data.get("headquarters_state"),
            "cik": data.get("cik"),
            "linkedin_url": data.get("linkedin_url"),
            "twitter_url": data.get("twitter_url") or data.get("twitter_handle"),
            "contact_email": data.get("contact_email"),
        }
        # Filter out twitter_url since PEFirm doesn't have it - store in data_sources
        # Actually check which fields exist on the model
        valid_fields = {}
        for k, v in update_fields.items():
            if hasattr(firm, k):
                valid_fields[k] = v

        changed = self._null_preserving_update(
            firm,
            valid_fields,
            item.confidence,
            existing_confidence=firm.confidence_score and "medium",
        )

        # Append source to data_sources
        source_label = item.source_url or item.data.get("source_type", "collected")
        sources = firm.data_sources or []
        if isinstance(sources, list) and source_label not in sources:
            firm.data_sources = sources + [source_label]
            changed = True

        if changed:
            self.stats["updated"] += 1
        else:
            self.stats["skipped"] += 1

    def _persist_form_adv_filing(
        self, entity_id: int, entity_name: str, item: PECollectedItem
    ) -> None:
        """Append Form ADV filing info to firm data_sources."""
        firm = self.db.query(PEFirm).filter(PEFirm.id == entity_id).first()
        if not firm:
            self.stats["skipped"] += 1
            return

        data = item.data
        filing_url = data.get("filing_url") or item.source_url
        sources = firm.data_sources or []
        if isinstance(sources, list):
            label = f"SEC Form ADV ({data.get('form_type', 'ADV')})"
            if label not in sources:
                firm.data_sources = sources + [label]

        # Mark as SEC registered
        if not firm.is_sec_registered:
            firm.is_sec_registered = True

        self.stats["updated"] += 1

    def _persist_portfolio_company(
        self, entity_id: int, entity_name: str, item: PECollectedItem
    ) -> None:
        """Insert or update pe_portfolio_companies."""
        data = item.data
        name = data.get("name")
        if not name:
            self.stats["skipped"] += 1
            return

        key = name.strip().lower()
        existing = (
            self.db.query(PEPortfolioCompany)
            .filter(func.lower(PEPortfolioCompany.name) == key)
            .first()
        )

        if existing:
            changed = self._null_preserving_update(
                existing,
                {
                    "website": data.get("website"),
                    "description": data.get("description"),
                    "current_pe_owner": data.get("current_pe_owner") or entity_name,
                    "ownership_status": data.get("ownership_status"),
                    "industry": data.get("industry"),
                },
                item.confidence,
            )
            self._company_cache[key] = existing.id
            if changed:
                self.stats["updated"] += 1
            else:
                self.stats["skipped"] += 1
        else:
            company = PEPortfolioCompany(
                name=name.strip(),
                website=data.get("website"),
                description=data.get("description"),
                current_pe_owner=data.get("current_pe_owner") or entity_name,
                ownership_status=data.get("ownership_status", "PE-Backed"),
                industry=data.get("industry"),
                status="Active",
            )
            self.db.add(company)
            self.db.flush()
            self._company_cache[key] = company.id
            self.stats["persisted"] += 1

    def _persist_team_member(
        self, entity_id: int, entity_name: str, item: PECollectedItem
    ) -> None:
        """Create person + firm link from website team page."""
        data = item.data
        full_name = data.get("full_name")
        if not full_name:
            self.stats["skipped"] += 1
            return

        person_id = self._find_or_create_person(full_name)

        # Update person title
        person = self.db.query(PEPerson).filter(PEPerson.id == person_id).first()
        if person and not person.current_title and data.get("title"):
            person.current_title = data["title"]
            person.current_company = entity_name

        # Create firm-person link if not exists
        existing_link = (
            self.db.query(PEFirmPeople)
            .filter(
                PEFirmPeople.firm_id == entity_id,
                PEFirmPeople.person_id == person_id,
            )
            .first()
        )
        if not existing_link:
            link = PEFirmPeople(
                firm_id=entity_id,
                person_id=person_id,
                title=data.get("title", "Team Member"),
                is_current=True,
            )
            self.db.add(link)
            self.stats["persisted"] += 1
        else:
            self.stats["skipped"] += 1

    def _persist_person(
        self, entity_id: int, entity_name: str, item: PECollectedItem
    ) -> None:
        """Full person record from Bio Extractor: person + education + experience + firm link."""
        data = item.data
        full_name = data.get("full_name")
        if not full_name:
            self.stats["skipped"] += 1
            return

        linkedin_url = data.get("linkedin_url")
        person_id = self._find_or_create_person(full_name, linkedin_url)

        # Update person fields
        person = self.db.query(PEPerson).filter(PEPerson.id == person_id).first()
        if person:
            self._null_preserving_update(
                person,
                {
                    "current_title": data.get("title"),
                    "current_company": data.get("firm_name") or entity_name,
                    "bio": data.get("bio"),
                    "linkedin_url": linkedin_url,
                },
                item.confidence,
            )

        # Education entries
        for edu in data.get("education", []):
            institution = edu.get("institution")
            if not institution:
                continue
            exists = (
                self.db.query(PEPersonEducation)
                .filter(
                    PEPersonEducation.person_id == person_id,
                    func.lower(PEPersonEducation.institution) == institution.lower(),
                )
                .first()
            )
            if not exists:
                self.db.add(
                    PEPersonEducation(
                        person_id=person_id,
                        institution=institution,
                        degree=edu.get("degree"),
                        field_of_study=edu.get("field"),
                    )
                )

        # Experience entries
        for exp in data.get("experience", []):
            company = exp.get("company")
            title = exp.get("title")
            if not company or not title:
                continue
            exists = (
                self.db.query(PEPersonExperience)
                .filter(
                    PEPersonExperience.person_id == person_id,
                    func.lower(PEPersonExperience.company) == company.lower(),
                    func.lower(PEPersonExperience.title) == title.lower(),
                )
                .first()
            )
            if not exists:
                self.db.add(
                    PEPersonExperience(
                        person_id=person_id,
                        company=company,
                        title=title,
                    )
                )

        # Firm link
        firm_id = data.get("firm_id") or entity_id
        existing_link = (
            self.db.query(PEFirmPeople)
            .filter(
                PEFirmPeople.firm_id == firm_id,
                PEFirmPeople.person_id == person_id,
            )
            .first()
        )
        if not existing_link:
            self.db.add(
                PEFirmPeople(
                    firm_id=firm_id,
                    person_id=person_id,
                    title=data.get("title", "Team Member"),
                    is_current=True,
                )
            )

        self.stats["persisted"] += 1

    def _persist_related_person(
        self, entity_id: int, entity_name: str, item: PECollectedItem
    ) -> None:
        """Create person from Form D related_person."""
        data = item.data
        name = data.get("name")
        if not name:
            self.stats["skipped"] += 1
            return

        person_id = self._find_or_create_person(name)

        # Optionally link to firm
        existing_link = (
            self.db.query(PEFirmPeople)
            .filter(
                PEFirmPeople.firm_id == entity_id,
                PEFirmPeople.person_id == person_id,
            )
            .first()
        )
        if not existing_link:
            self.db.add(
                PEFirmPeople(
                    firm_id=entity_id,
                    person_id=person_id,
                    title=data.get("relationship", "Related Person"),
                    is_current=True,
                )
            )
            self.stats["persisted"] += 1
        else:
            self.stats["skipped"] += 1

    def _persist_company_update(
        self, entity_id: int, entity_name: str, item: PECollectedItem
    ) -> None:
        """Update pe_portfolio_companies from public comps data."""
        company = (
            self.db.query(PEPortfolioCompany)
            .filter(PEPortfolioCompany.id == entity_id)
            .first()
        )
        if not company:
            # entity_id might be in data
            cid = item.data.get("company_id", entity_id)
            company = (
                self.db.query(PEPortfolioCompany)
                .filter(PEPortfolioCompany.id == cid)
                .first()
            )
        if not company:
            self.stats["skipped"] += 1
            return

        data = item.data
        changed = self._null_preserving_update(
            company,
            {
                "industry": data.get("industry"),
                "sector": data.get("sector"),
                "description": data.get("description"),
                "employee_count": data.get("employee_count"),
                "headquarters_city": data.get("headquarters_city"),
                "headquarters_state": data.get("headquarters_state"),
                "headquarters_country": data.get("headquarters_country"),
                "website": data.get("website"),
                "ticker": data.get("ticker"),
            },
            item.confidence,
        )
        if changed:
            self.stats["updated"] += 1
        else:
            self.stats["skipped"] += 1

    # ------------------------------------------------------------------
    # Phase 2 handlers — relationships, transactions, financials
    # ------------------------------------------------------------------

    def _persist_13f_holding(
        self, entity_id: int, entity_name: str, item: PECollectedItem
    ) -> None:
        """Create portfolio company + fund investment from 13F holding."""
        data = item.data
        issuer_name = data.get("issuer_name")
        if not issuer_name:
            self.stats["skipped"] += 1
            return

        company_id = self._find_or_create_company(
            issuer_name,
            ownership_status="13F Reported",
            ticker=data.get("security_class"),
        )

        firm_id = data.get("firm_id") or entity_id
        firm_name = data.get("firm_name") or entity_name
        fund_id = self._find_or_create_holdings_fund(firm_id, firm_name)

        report_date = self._parse_date(data.get("report_date"))

        # Dedup: same fund, company, and quarter
        existing = (
            self.db.query(PEFundInvestment)
            .filter(
                PEFundInvestment.fund_id == fund_id,
                PEFundInvestment.company_id == company_id,
                PEFundInvestment.investment_date == report_date,
            )
            .first()
        )
        if existing:
            # Update value if changed
            new_val = self._to_decimal(data.get("value_usd"))
            if new_val and new_val != existing.invested_amount_usd:
                existing.invested_amount_usd = new_val
                self.stats["updated"] += 1
            else:
                self.stats["skipped"] += 1
            return

        investment = PEFundInvestment(
            fund_id=fund_id,
            company_id=company_id,
            investment_date=report_date,
            investment_type="13F Holding",
            invested_amount_usd=self._to_decimal(data.get("value_usd")),
            status="Active",
        )
        self.db.add(investment)
        self.stats["persisted"] += 1

    def _persist_13d_stake(
        self, entity_id: int, entity_name: str, item: PECollectedItem
    ) -> None:
        """Record 13D stake — mainly metadata, company discovery happens elsewhere."""
        data = item.data
        # 13D filings often lack issuer_name in our collector's output
        # They are primarily informational; skip if no actionable data
        issuer_name = data.get("issuer_name")
        if not issuer_name:
            self.stats["skipped"] += 1
            return

        company_id = self._find_or_create_company(issuer_name)
        firm_id = data.get("firm_id") or entity_id
        firm_name = data.get("firm_name") or entity_name
        fund_id = self._find_or_create_holdings_fund(firm_id, firm_name)

        report_date = self._parse_date(data.get("report_date"))

        existing = (
            self.db.query(PEFundInvestment)
            .filter(
                PEFundInvestment.fund_id == fund_id,
                PEFundInvestment.company_id == company_id,
                PEFundInvestment.investment_type == "13D Stake",
            )
            .first()
        )
        if existing:
            self.stats["skipped"] += 1
            return

        investment = PEFundInvestment(
            fund_id=fund_id,
            company_id=company_id,
            investment_date=report_date,
            investment_type="13D Stake",
            status="Active",
        )
        self.db.add(investment)
        self.stats["persisted"] += 1

    def _persist_form_d_filing(
        self, entity_id: int, entity_name: str, item: PECollectedItem
    ) -> None:
        """Create deal + portfolio company from Form D filing."""
        data = item.data
        source_url = item.source_url
        if not source_url:
            source_url = data.get("filing_url", "")

        # Dedup by source_url
        if source_url:
            existing = (
                self.db.query(PEDeal).filter(PEDeal.source_url == source_url).first()
            )
            if existing:
                self.stats["skipped"] += 1
                return

        issuer_name = data.get("issuer_name")
        if not issuer_name:
            self.stats["skipped"] += 1
            return

        company_id = self._find_or_create_company(
            issuer_name, industry=data.get("industry")
        )

        deal = PEDeal(
            company_id=company_id,
            deal_type="Private Placement",
            deal_sub_type=data.get("exemption"),
            deal_name=f"{issuer_name} - Form D",
            announced_date=self._parse_date(data.get("filing_date")),
            enterprise_value_usd=self._to_decimal(data.get("offering_amount")),
            buyer_name=entity_name,
            status="Filed",
            data_source="SEC Form D",
            source_url=source_url,
        )
        self.db.add(deal)
        self.stats["persisted"] += 1

    def _persist_deal_8k_filing(
        self, entity_id: int, entity_name: str, item: PECollectedItem
    ) -> None:
        """Create deal record from 8-K filing."""
        data = item.data
        source_url = item.source_url
        if not source_url:
            source_url = data.get("url", "")

        if source_url:
            existing = (
                self.db.query(PEDeal).filter(PEDeal.source_url == source_url).first()
            )
            if existing:
                self.stats["skipped"] += 1
                return

        company_name = data.get("company_name") or data.get("title", "Unknown")
        company_id = self._find_or_create_company(company_name)

        deal = PEDeal(
            company_id=company_id,
            deal_type="8-K Event",
            deal_name=data.get("title", f"{company_name} - 8-K"),
            announced_date=self._parse_date(data.get("filing_date")),
            buyer_name=data.get("firm_name") or entity_name,
            status="Filed",
            data_source="SEC 8-K",
            source_url=source_url,
        )
        self.db.add(deal)
        self.stats["persisted"] += 1

    def _persist_deal_press_release(
        self, entity_id: int, entity_name: str, item: PECollectedItem
    ) -> None:
        """Record a press release that may describe a deal (low confidence)."""
        data = item.data
        source_url = data.get("url") or item.source_url
        if not source_url:
            self.stats["skipped"] += 1
            return

        existing = (
            self.db.query(PEDeal).filter(PEDeal.press_release_url == source_url).first()
        )
        if existing:
            self.stats["skipped"] += 1
            return

        # Also check source_url
        existing2 = (
            self.db.query(PEDeal).filter(PEDeal.source_url == source_url).first()
        )
        if existing2:
            self.stats["skipped"] += 1
            return

        title = data.get("title", "Press Release")
        # We don't create a full deal for low-confidence press releases
        # unless they were later parsed by LLM (which produces item_type="deal")
        # Just create a minimal placeholder deal
        deal = PEDeal(
            company_id=self._find_or_create_company(
                data.get("firm_name") or entity_name
            ),
            deal_type="Announced",
            deal_name=title[:500] if title else "Press Release",
            buyer_name=data.get("firm_name") or entity_name,
            status="Announced",
            data_source=data.get("source", "Press Release"),
            press_release_url=source_url,
            source_url=source_url,
        )
        self.db.add(deal)
        self.stats["persisted"] += 1

    def _persist_deal(
        self, entity_id: int, entity_name: str, item: PECollectedItem
    ) -> None:
        """Full deal from LLM-parsed press release."""
        data = item.data
        source_url = item.source_url
        if not source_url:
            source_url = data.get("url", "")

        # Dedup by source_url
        if source_url:
            existing = (
                self.db.query(PEDeal).filter(PEDeal.source_url == source_url).first()
            )
            if existing:
                # Update with richer LLM data
                self._null_preserving_update(
                    existing,
                    {
                        "deal_type": data.get("deal_type"),
                        "enterprise_value_usd": self._to_decimal(
                            data.get("enterprise_value_usd")
                        ),
                        "announced_date": self._parse_date(data.get("announced_date")),
                        "closed_date": self._parse_date(data.get("closed_date")),
                        "seller_name": data.get("seller"),
                    },
                    item.confidence,
                )
                self.stats["updated"] += 1
                return

        target = data.get("target_company")
        if not target:
            self.stats["skipped"] += 1
            return

        company_id = self._find_or_create_company(
            target, description=data.get("target_description")
        )

        deal = PEDeal(
            company_id=company_id,
            deal_type=data.get("deal_type", "Announced"),
            deal_name=data.get("deal_name") or data.get("pr_title") or f"{target} Deal",
            announced_date=self._parse_date(data.get("announced_date")),
            closed_date=self._parse_date(data.get("closed_date")),
            enterprise_value_usd=self._to_decimal(data.get("enterprise_value_usd")),
            buyer_name=data.get("firm_name") or entity_name,
            seller_name=data.get("seller"),
            status="Announced",
            data_source="Press Release (LLM)",
            source_url=source_url,
        )
        self.db.add(deal)
        self.db.flush()

        # Lead participant
        self.db.add(
            PEDealParticipant(
                deal_id=deal.id,
                firm_id=entity_id,
                participant_name=data.get("firm_name") or entity_name,
                participant_type="PE Firm",
                role="Lead Sponsor",
                is_lead=True,
            )
        )

        # Co-investors
        for co_name in data.get("co_investors", []):
            if not co_name:
                continue
            # Try to resolve firm_id
            co_firm_id = self._firm_cache.get(co_name.strip().lower())
            self.db.add(
                PEDealParticipant(
                    deal_id=deal.id,
                    firm_id=co_firm_id,
                    participant_name=co_name.strip(),
                    participant_type="Co-Investor",
                    role="Co-Investor",
                    is_lead=False,
                )
            )

        self.stats["persisted"] += 1

    def _persist_firm_news(
        self, entity_id: int, entity_name: str, item: PECollectedItem
    ) -> None:
        """Insert pe_firm_news. Dedup by source_url UNIQUE constraint."""
        data = item.data
        source_url = data.get("url") or item.source_url
        if not source_url:
            self.stats["skipped"] += 1
            return

        # Check unique constraint before attempting insert
        existing = (
            self.db.query(PEFirmNews)
            .filter(PEFirmNews.source_url == source_url)
            .first()
        )
        if existing:
            self.stats["skipped"] += 1
            return

        firm_id = data.get("entity_id") or entity_id
        news = PEFirmNews(
            firm_id=firm_id,
            title=data.get("title", "Untitled")[:1000],
            source_name=data.get("source_name"),
            source_url=source_url,
            summary=data.get("summary") or data.get("description"),
            published_date=self._parse_datetime(data.get("published_date")),
            news_type=data.get("news_type"),
            sentiment=data.get("sentiment"),
            sentiment_score=self._to_decimal(data.get("relevance_score")),
        )
        self.db.add(news)
        self.stats["persisted"] += 1

    def _persist_company_financial(
        self, entity_id: int, entity_name: str, item: PECollectedItem
    ) -> None:
        """Insert pe_company_financials from public comps data."""
        data = item.data
        company_id = data.get("company_id") or entity_id
        fiscal_year = datetime.utcnow().year
        fiscal_period = "TTM"

        # Dedup by unique constraint
        existing = (
            self.db.query(PECompanyFinancials)
            .filter(
                PECompanyFinancials.company_id == company_id,
                PECompanyFinancials.fiscal_year == fiscal_year,
                PECompanyFinancials.fiscal_period == fiscal_period,
            )
            .first()
        )
        if existing:
            changed = self._null_preserving_update(
                existing,
                {
                    "revenue_usd": self._to_decimal(data.get("revenue")),
                    "ebitda_usd": self._to_decimal(data.get("ebitda")),
                    "gross_profit_usd": self._to_decimal(data.get("gross_profit")),
                    "ebit_usd": self._to_decimal(data.get("operating_income")),
                    "net_income_usd": self._to_decimal(data.get("net_income")),
                    "total_assets_usd": self._to_decimal(data.get("total_assets")),
                    "total_debt_usd": self._to_decimal(data.get("total_debt")),
                    "cash_usd": self._to_decimal(data.get("total_cash")),
                    "shareholders_equity_usd": self._to_decimal(
                        data.get("total_stockholder_equity")
                    ),
                    "free_cash_flow_usd": self._to_decimal(data.get("free_cash_flow")),
                    "operating_cash_flow_usd": self._to_decimal(
                        data.get("operating_cash_flow")
                    ),
                    "gross_margin_pct": self._to_decimal(data.get("gross_margin")),
                    "ebitda_margin_pct": self._to_decimal(data.get("operating_margin")),
                    "revenue_growth_pct": self._to_decimal(data.get("revenue_growth")),
                },
                item.confidence,
                existing.confidence,
            )
            if changed:
                self.stats["updated"] += 1
            else:
                self.stats["skipped"] += 1
            return

        fin = PECompanyFinancials(
            company_id=company_id,
            fiscal_year=fiscal_year,
            fiscal_period=fiscal_period,
            revenue_usd=self._to_decimal(data.get("revenue")),
            ebitda_usd=self._to_decimal(data.get("ebitda")),
            gross_profit_usd=self._to_decimal(data.get("gross_profit")),
            ebit_usd=self._to_decimal(data.get("operating_income")),
            net_income_usd=self._to_decimal(data.get("net_income")),
            total_assets_usd=self._to_decimal(data.get("total_assets")),
            total_debt_usd=self._to_decimal(data.get("total_debt")),
            cash_usd=self._to_decimal(data.get("total_cash")),
            shareholders_equity_usd=self._to_decimal(
                data.get("total_stockholder_equity")
            ),
            free_cash_flow_usd=self._to_decimal(data.get("free_cash_flow")),
            operating_cash_flow_usd=self._to_decimal(data.get("operating_cash_flow")),
            gross_margin_pct=self._to_decimal(data.get("gross_margin")),
            ebitda_margin_pct=self._to_decimal(data.get("operating_margin")),
            revenue_growth_pct=self._to_decimal(data.get("revenue_growth")),
            data_source="Yahoo Finance",
            confidence=item.confidence,
        )
        self.db.add(fin)
        self.stats["persisted"] += 1

    def _persist_company_valuation(
        self, entity_id: int, entity_name: str, item: PECollectedItem
    ) -> None:
        """Insert pe_company_valuations."""
        data = item.data
        company_id = data.get("company_id") or entity_id
        valuation_date = self._parse_date(data.get("valuation_date")) or date.today()

        # Determine source
        valuation_method = data.get("valuation_method")
        if valuation_method:
            data_source = "LLM Estimate"
            methodology = valuation_method
        else:
            data_source = "Yahoo Finance"
            methodology = "Market Data"

        # Check for existing valuation same day
        existing = (
            self.db.query(PECompanyValuation)
            .filter(
                PECompanyValuation.company_id == company_id,
                PECompanyValuation.valuation_date == valuation_date,
                PECompanyValuation.data_source == data_source,
            )
            .first()
        )
        if existing:
            changed = self._null_preserving_update(
                existing,
                {
                    "enterprise_value_usd": self._to_decimal(
                        data.get("enterprise_value")
                        or data.get("estimated_enterprise_value_usd")
                    ),
                    "equity_value_usd": self._to_decimal(
                        data.get("market_cap") or data.get("estimated_equity_value_usd")
                    ),
                    "ev_revenue_multiple": self._to_decimal(
                        data.get("ev_to_revenue") or data.get("ev_to_revenue_multiple")
                    ),
                    "ev_ebitda_multiple": self._to_decimal(
                        data.get("ev_to_ebitda") or data.get("ev_to_ebitda_multiple")
                    ),
                    "price_earnings_multiple": self._to_decimal(
                        data.get("trailing_pe")
                    ),
                },
                item.confidence,
                existing.confidence,
            )
            if changed:
                self.stats["updated"] += 1
            else:
                self.stats["skipped"] += 1
            return

        val = PECompanyValuation(
            company_id=company_id,
            valuation_date=valuation_date,
            enterprise_value_usd=self._to_decimal(
                data.get("enterprise_value")
                or data.get("estimated_enterprise_value_usd")
            ),
            equity_value_usd=self._to_decimal(
                data.get("market_cap") or data.get("estimated_equity_value_usd")
            ),
            ev_revenue_multiple=self._to_decimal(
                data.get("ev_to_revenue") or data.get("ev_to_revenue_multiple")
            ),
            ev_ebitda_multiple=self._to_decimal(
                data.get("ev_to_ebitda") or data.get("ev_to_ebitda_multiple")
            ),
            price_earnings_multiple=self._to_decimal(data.get("trailing_pe")),
            valuation_type="Market Data"
            if data_source == "Yahoo Finance"
            else "Third-Party",
            methodology=methodology,
            data_source=data_source,
            source_url=item.source_url,
            confidence=item.confidence,
        )
        self.db.add(val)
        self.stats["persisted"] += 1
