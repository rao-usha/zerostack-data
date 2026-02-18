"""
Unit tests for PEPersister.

Tests the persistence layer that writes PECollectedItem objects to PE database
tables. Uses in-memory SQLite via the shared test_db fixture.
"""

import pytest
from datetime import date, datetime
from decimal import Decimal

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
from app.sources.pe_collection.persister import PEPersister
from app.sources.pe_collection.types import (
    PECollectedItem,
    PECollectionResult,
    PECollectionSource,
    EntityType,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_item(item_type: str, data: dict, source_url: str = None,
               confidence: str = "medium") -> PECollectedItem:
    return PECollectedItem(
        item_type=item_type,
        entity_type=EntityType.FIRM,
        data=data,
        source_url=source_url,
        confidence=confidence,
    )


def _make_result(entity_id: int, entity_name: str,
                 items: list, success: bool = True) -> PECollectionResult:
    return PECollectionResult(
        entity_id=entity_id,
        entity_name=entity_name,
        entity_type=EntityType.FIRM,
        source=PECollectionSource.FIRM_WEBSITE,
        success=success,
        items=items,
    )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def pe_db(test_db):
    """Seed a PEFirm and PEPortfolioCompany, return the session."""
    firm = PEFirm(id=1, name="Blackstone", status="Active")
    test_db.add(firm)
    company = PEPortfolioCompany(id=1, name="TechCo", status="Active")
    test_db.add(company)
    test_db.commit()
    return test_db


@pytest.fixture
def persister(pe_db):
    return PEPersister(pe_db)


# ===================================================================
# Helper method tests
# ===================================================================

class TestShouldUpdate:
    def test_higher_confidence(self, persister):
        assert persister._should_update("high", "medium") is True

    def test_equal_confidence(self, persister):
        assert persister._should_update("medium", "medium") is True

    def test_lower_confidence(self, persister):
        assert persister._should_update("low", "high") is False


class TestNullPreservingUpdate:
    def test_fills_nulls_regardless_of_confidence(self, persister, pe_db):
        company = pe_db.get(PEPortfolioCompany, 1)
        assert company.industry is None
        changed = persister._null_preserving_update(
            company,
            {"industry": "Technology"},
            "low",
            existing_confidence="high",
        )
        assert changed is True
        assert company.industry == "Technology"

    def test_respects_confidence_for_non_null(self, persister, pe_db):
        company = pe_db.get(PEPortfolioCompany, 1)
        company.industry = "Original"
        pe_db.flush()

        changed = persister._null_preserving_update(
            company,
            {"industry": "New"},
            "low",
            existing_confidence="high",
        )
        assert changed is False
        assert company.industry == "Original"

    def test_overwrites_when_confidence_sufficient(self, persister, pe_db):
        company = pe_db.get(PEPortfolioCompany, 1)
        company.industry = "Original"
        pe_db.flush()

        changed = persister._null_preserving_update(
            company,
            {"industry": "New"},
            "high",
            existing_confidence="medium",
        )
        assert changed is True
        assert company.industry == "New"


class TestParseDateFormats:
    @pytest.mark.parametrize("input_val,expected", [
        ("2024-01-15", date(2024, 1, 15)),
        ("01/15/2024", date(2024, 1, 15)),
        ("20240115", date(2024, 1, 15)),
        # datetime is a subclass of date, so isinstance(dt, date) is True
        # and _parse_date returns it as-is (still date-compatible)
        (datetime(2024, 1, 15, 12, 0), datetime(2024, 1, 15, 12, 0)),
        (date(2024, 1, 15), date(2024, 1, 15)),
        (None, None),
        ("not-a-date", None),
    ])
    def test_parse_date(self, persister, input_val, expected):
        assert persister._parse_date(input_val) == expected


class TestParseDatetimeFormats:
    @pytest.mark.parametrize("input_val,expected", [
        ("2024-01-15T10:30:00Z", datetime(2024, 1, 15, 10, 30, 0)),
        ("2024-01-15T10:30:00", datetime(2024, 1, 15, 10, 30, 0)),
        ("2024-01-15 10:30:00", datetime(2024, 1, 15, 10, 30, 0)),
        ("2024-01-15", datetime(2024, 1, 15, 0, 0, 0)),
        (datetime(2024, 1, 15, 10, 30), datetime(2024, 1, 15, 10, 30)),
        (date(2024, 1, 15), datetime(2024, 1, 15, 0, 0)),
        (None, None),
    ])
    def test_parse_datetime(self, persister, input_val, expected):
        assert persister._parse_datetime(input_val) == expected


class TestToDecimal:
    @pytest.mark.parametrize("input_val,expected", [
        (100, Decimal("100")),
        (3.14, Decimal("3.14")),
        ("1000000", Decimal("1000000")),
        (None, None),
        ("invalid", None),
    ])
    def test_to_decimal(self, persister, input_val, expected):
        assert persister._to_decimal(input_val) == expected


# ===================================================================
# Phase 1 handler tests
# ===================================================================

class TestPersistFirmUpdate:
    def test_updates_existing_firm(self, persister, pe_db):
        item = _make_item("firm_update", {
            "headquarters_city": "New York",
            "headquarters_state": "NY",
            "cik": "0001234567",
        }, source_url="https://example.com/blackstone")

        results = persister.persist_results([
            _make_result(1, "Blackstone", [item])
        ])
        assert results["updated"] == 1
        firm = pe_db.get(PEFirm, 1)
        assert firm.headquarters_city == "New York"
        assert firm.cik == "0001234567"
        assert "https://example.com/blackstone" in (firm.data_sources or [])

    def test_missing_firm_skipped(self, persister, pe_db):
        item = _make_item("firm_update", {"headquarters_city": "Boston"})
        results = persister.persist_results([
            _make_result(999, "NoSuchFirm", [item])
        ])
        assert results["skipped"] >= 1


class TestPersistPortfolioCompany:
    def test_creates_new_company(self, persister, pe_db):
        item = _make_item("portfolio_company", {
            "name": "NewCo",
            "website": "https://newco.com",
            "description": "A new company",
        })
        results = persister.persist_results([
            _make_result(1, "Blackstone", [item])
        ])
        assert results["persisted"] >= 1
        co = pe_db.query(PEPortfolioCompany).filter_by(name="NewCo").first()
        assert co is not None
        assert co.website == "https://newco.com"
        assert co.current_pe_owner == "Blackstone"

    def test_updates_existing_company(self, persister, pe_db):
        item = _make_item("portfolio_company", {
            "name": "TechCo",
            "website": "https://techco.com",
            "industry": "Technology",
        })
        results = persister.persist_results([
            _make_result(1, "Blackstone", [item])
        ])
        assert results["updated"] >= 1
        co = pe_db.get(PEPortfolioCompany, 1)
        assert co.website == "https://techco.com"


class TestPersistTeamMember:
    def test_creates_person_and_link(self, persister, pe_db):
        item = _make_item("team_member", {
            "full_name": "Jane Doe",
            "title": "Managing Director",
        })
        results = persister.persist_results([
            _make_result(1, "Blackstone", [item])
        ])
        assert results["persisted"] >= 1
        person = pe_db.query(PEPerson).filter_by(full_name="Jane Doe").first()
        assert person is not None
        link = pe_db.query(PEFirmPeople).filter_by(
            firm_id=1, person_id=person.id
        ).first()
        assert link is not None
        assert link.title == "Managing Director"

    def test_duplicate_link_skipped(self, persister, pe_db):
        person = PEPerson(full_name="Existing Person")
        pe_db.add(person)
        pe_db.flush()
        link = PEFirmPeople(firm_id=1, person_id=person.id, title="VP", is_current=True)
        pe_db.add(link)
        pe_db.commit()

        item = _make_item("team_member", {
            "full_name": "Existing Person",
            "title": "VP",
        })
        results = persister.persist_results([
            _make_result(1, "Blackstone", [item])
        ])
        assert results["skipped"] >= 1


class TestPersistPerson:
    def test_person_with_education_experience(self, persister, pe_db):
        item = _make_item("person", {
            "full_name": "Bob Smith",
            "title": "Partner",
            "bio": "Experienced investor",
            "education": [
                {"institution": "Harvard", "degree": "MBA", "field": "Finance"},
            ],
            "experience": [
                {"company": "Goldman Sachs", "title": "VP"},
            ],
        })
        results = persister.persist_results([
            _make_result(1, "Blackstone", [item])
        ])
        assert results["persisted"] >= 1

        person = pe_db.query(PEPerson).filter_by(full_name="Bob Smith").first()
        assert person is not None
        assert person.bio == "Experienced investor"

        edu = pe_db.query(PEPersonEducation).filter_by(person_id=person.id).first()
        assert edu is not None
        assert edu.institution == "Harvard"

        exp = pe_db.query(PEPersonExperience).filter_by(person_id=person.id).first()
        assert exp is not None
        assert exp.company == "Goldman Sachs"

        link = pe_db.query(PEFirmPeople).filter_by(
            firm_id=1, person_id=person.id
        ).first()
        assert link is not None


class TestPersistRelatedPerson:
    def test_creates_person_and_firm_link(self, persister, pe_db):
        item = _make_item("related_person", {
            "name": "Carol White",
            "relationship": "Director",
        })
        results = persister.persist_results([
            _make_result(1, "Blackstone", [item])
        ])
        assert results["persisted"] >= 1

        person = pe_db.query(PEPerson).filter_by(full_name="Carol White").first()
        assert person is not None
        link = pe_db.query(PEFirmPeople).filter_by(
            firm_id=1, person_id=person.id
        ).first()
        assert link is not None
        assert link.title == "Director"


class TestPersistCompanyUpdate:
    def test_updates_company_fields(self, persister, pe_db):
        item = _make_item("company_update", {
            "industry": "Software",
            "sector": "Technology",
            "employee_count": 500,
            "ticker": "TCO",
        })
        results = persister.persist_results([
            _make_result(1, "Blackstone", [item])
        ])
        assert results["updated"] >= 1
        co = pe_db.get(PEPortfolioCompany, 1)
        assert co.industry == "Software"
        assert co.ticker == "TCO"


class TestFindOrCreateCompanyCached:
    def test_second_call_returns_cached_id(self, persister, pe_db):
        id1 = persister._find_or_create_company("BrandNewCo")
        id2 = persister._find_or_create_company("BrandNewCo")
        assert id1 == id2
        count = pe_db.query(PEPortfolioCompany).filter_by(name="BrandNewCo").count()
        assert count == 1


# ===================================================================
# Phase 2 handler tests
# ===================================================================

class TestPersistFirmNews:
    def test_creates_news_row(self, persister, pe_db):
        item = _make_item("firm_news", {
            "title": "Blackstone Raises Fund X",
            "url": "https://news.example.com/article1",
            "source_name": "Reuters",
            "summary": "Big fundraise",
            "published_date": "2024-06-15T10:00:00Z",
            "news_type": "Fundraise",
            "sentiment": "Positive",
            "relevance_score": 0.95,
        })
        results = persister.persist_results([
            _make_result(1, "Blackstone", [item])
        ])
        assert results["persisted"] >= 1
        news = pe_db.query(PEFirmNews).filter_by(firm_id=1).first()
        assert news is not None
        assert news.title == "Blackstone Raises Fund X"
        assert news.source_name == "Reuters"
        assert news.sentiment == "Positive"

    def test_dedup_by_source_url(self, persister, pe_db):
        url = "https://news.example.com/dup"
        existing = PEFirmNews(
            firm_id=1, title="Old", source_url=url,
        )
        pe_db.add(existing)
        pe_db.commit()

        item = _make_item("firm_news", {
            "title": "New Title",
            "url": url,
        })
        results = persister.persist_results([
            _make_result(1, "Blackstone", [item])
        ])
        assert results["skipped"] >= 1
        count = pe_db.query(PEFirmNews).filter_by(source_url=url).count()
        assert count == 1


class TestPersistCompanyValuation:
    def test_creates_new_valuation(self, persister, pe_db):
        item = _make_item("company_valuation", {
            "company_id": 1,
            "valuation_date": "2024-06-01",
            "valuation_method": "Comparable Companies",
            "estimated_enterprise_value_usd": 5000000000,
            "estimated_equity_value_usd": 3000000000,
            "ev_to_revenue_multiple": 8.5,
            "ev_to_ebitda_multiple": 15.2,
        })
        results = persister.persist_results([
            _make_result(1, "Blackstone", [item])
        ])
        assert results["persisted"] >= 1
        val = pe_db.query(PECompanyValuation).filter_by(company_id=1).first()
        assert val is not None
        assert val.data_source == "LLM Estimate"
        assert val.methodology == "Comparable Companies"

    def test_updates_existing_same_day_source(self, persister, pe_db):
        existing = PECompanyValuation(
            company_id=1,
            valuation_date=date(2024, 6, 1),
            data_source="LLM Estimate",
            confidence="low",
        )
        pe_db.add(existing)
        pe_db.commit()

        item = _make_item("company_valuation", {
            "company_id": 1,
            "valuation_date": "2024-06-01",
            "valuation_method": "DCF",
            "estimated_enterprise_value_usd": 6000000000,
        }, confidence="medium")
        results = persister.persist_results([
            _make_result(1, "Blackstone", [item])
        ])
        assert results["updated"] >= 1


class TestPersistCompanyFinancial:
    def test_creates_new_financial(self, persister, pe_db):
        item = _make_item("company_financial", {
            "company_id": 1,
            "revenue": 100000000,
            "ebitda": 20000000,
            "net_income": 10000000,
        })
        results = persister.persist_results([
            _make_result(1, "Blackstone", [item])
        ])
        assert results["persisted"] >= 1
        fin = pe_db.query(PECompanyFinancials).filter_by(company_id=1).first()
        assert fin is not None
        assert fin.revenue_usd == Decimal("100000000")

    def test_updates_existing_same_period(self, persister, pe_db):
        year = datetime.utcnow().year
        existing = PECompanyFinancials(
            company_id=1,
            fiscal_year=year,
            fiscal_period="TTM",
            revenue_usd=Decimal("50000000"),
            confidence="low",
        )
        pe_db.add(existing)
        pe_db.commit()

        item = _make_item("company_financial", {
            "company_id": 1,
            "revenue": 100000000,
            "ebitda": 20000000,
        }, confidence="medium")
        results = persister.persist_results([
            _make_result(1, "Blackstone", [item])
        ])
        assert results["updated"] >= 1
        fin = pe_db.query(PECompanyFinancials).filter_by(company_id=1).first()
        assert fin.ebitda_usd == Decimal("20000000")


class TestPersistDeal:
    def test_creates_deal_with_participants(self, persister, pe_db):
        item = _make_item("deal", {
            "target_company": "TargetInc",
            "deal_type": "LBO",
            "deal_name": "TargetInc Acquisition",
            "enterprise_value_usd": 2000000000,
            "announced_date": "2024-03-15",
            "co_investors": ["KKR", "Carlyle"],
        }, source_url="https://pr.example.com/deal1")
        results = persister.persist_results([
            _make_result(1, "Blackstone", [item])
        ])
        assert results["persisted"] >= 1

        deal = pe_db.query(PEDeal).filter_by(deal_name="TargetInc Acquisition").first()
        assert deal is not None
        assert deal.deal_type == "LBO"
        assert deal.buyer_name == "Blackstone"

        participants = pe_db.query(PEDealParticipant).filter_by(deal_id=deal.id).all()
        assert len(participants) == 3  # lead + 2 co-investors
        lead = [p for p in participants if p.is_lead][0]
        assert lead.participant_name == "Blackstone"


class TestPersist13FHolding:
    def test_creates_company_fund_and_investment(self, persister, pe_db):
        item = _make_item("13f_holding", {
            "issuer_name": "Apple Inc",
            "security_class": "AAPL",
            "value_usd": 50000000,
            "report_date": "2024-03-31",
        })
        results = persister.persist_results([
            _make_result(1, "Blackstone", [item])
        ])
        assert results["persisted"] >= 1

        company = pe_db.query(PEPortfolioCompany).filter_by(name="Apple Inc").first()
        assert company is not None

        fund = pe_db.query(PEFund).filter_by(
            firm_id=1, strategy="13F Reported Holdings"
        ).first()
        assert fund is not None

        inv = pe_db.query(PEFundInvestment).filter_by(
            fund_id=fund.id, company_id=company.id
        ).first()
        assert inv is not None
        assert inv.invested_amount_usd == Decimal("50000000")
        assert inv.investment_type == "13F Holding"


# ===================================================================
# Orchestration / integration tests
# ===================================================================

class TestPersistResultsOrchestration:
    def test_phase_ordering(self, persister, pe_db):
        """Phase 1 (portfolio_company) commits before Phase 2 (company_financial)."""
        company_item = _make_item("portfolio_company", {
            "name": "PhaseTestCo",
            "industry": "Finance",
        })
        financial_item = _make_item("company_financial", {
            "revenue": 50000000,
        })
        # company_financial's entity_id won't match the new company,
        # but the point is both phases execute without FK errors
        results = persister.persist_results([
            _make_result(1, "Blackstone", [company_item, financial_item])
        ])
        # At least company was persisted
        co = pe_db.query(PEPortfolioCompany).filter_by(name="PhaseTestCo").first()
        assert co is not None

    def test_skips_failed_results(self, persister, pe_db):
        item = _make_item("firm_update", {"headquarters_city": "Boston"})
        result = _make_result(1, "Blackstone", [item], success=False)
        stats = persister.persist_results([result])
        assert stats["persisted"] == 0
        assert stats["updated"] == 0

    def test_unknown_item_type_skipped(self, persister, pe_db):
        item = _make_item("unknown_widget", {"foo": "bar"})
        stats = persister.persist_results([
            _make_result(1, "Blackstone", [item])
        ])
        assert stats["skipped"] >= 1

    def test_item_failure_isolation(self, persister, pe_db):
        """One bad item doesn't block processing of subsequent items."""
        # related_person with no name -> will be skipped inside handler
        bad_item = _make_item("related_person", {})  # missing "name"
        good_item = _make_item("team_member", {
            "full_name": "Good Person",
            "title": "Analyst",
        })
        stats = persister.persist_results([
            _make_result(1, "Blackstone", [bad_item, good_item])
        ])
        # good_item should still be persisted
        person = pe_db.query(PEPerson).filter_by(full_name="Good Person").first()
        assert person is not None
        assert stats["skipped"] >= 1
        assert stats["persisted"] >= 1
