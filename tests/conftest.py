"""
Pytest configuration and shared fixtures.
"""
import os
import pytest
from datetime import date, datetime, timedelta
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from app.core.models import Base
from app.core.config import reset_settings
from app.core.people_models import (
    Person, IndustrialCompany, CompanyPerson, PersonExperience,
    PersonEducation, OrgChartSnapshot, LeadershipChange, PeopleCollectionJob,
    PeoplePortfolio, PeoplePortfolioCompany, PeoplePeerSet, PeoplePeerSetMember,
    PeopleWatchlist, PeopleWatchlistPerson,
)


@pytest.fixture(scope="function")
def clean_env(monkeypatch):
    """
    Clean environment for testing.
    
    Removes all app-related env vars to ensure clean state.
    """
    env_vars = [
        "DATABASE_URL",
        "CENSUS_SURVEY_API_KEY",
        "MAX_CONCURRENCY",
        "LOG_LEVEL",
        "RUN_INTEGRATION_TESTS",
        "MAX_RETRIES",
        "RETRY_BACKOFF_FACTOR",
        "MAX_REQUESTS_PER_SECOND"
    ]
    for var in env_vars:
        monkeypatch.delenv(var, raising=False)
    
    # Reset settings singleton
    reset_settings()
    
    yield
    
    # Reset again after test
    reset_settings()


@pytest.fixture(scope="function")
def test_db():
    """
    Create an in-memory SQLite database for testing.
    
    Fresh database for each test.
    """
    # Use in-memory SQLite
    engine = create_engine("sqlite:///:memory:")
    
    # Create all tables
    Base.metadata.create_all(bind=engine)
    
    # Create session factory
    TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    
    # Create session
    db = TestingSessionLocal()
    
    try:
        yield db
    finally:
        db.close()
        Base.metadata.drop_all(bind=engine)


@pytest.fixture(scope="function")
def db_session(test_db):
    """
    Alias for test_db fixture for backward compatibility.
    """
    yield test_db


# =============================================================================
# People Platform Fixtures
# =============================================================================

@pytest.fixture
def sample_company(test_db):
    """Create a sample industrial company."""
    import uuid
    unique_suffix = str(uuid.uuid4())[:8]
    company = IndustrialCompany(
        name=f"Acme Industrial Supply {unique_suffix}",
        website="https://www.acmeindustrial.com",
        leadership_page_url="https://www.acmeindustrial.com/about/leadership",
        headquarters_city="Chicago",
        headquarters_state="IL",
        industry_segment="distribution",
        sub_segment="fasteners",
        employee_count=500,
        revenue_usd=100000000,
        ownership_type="private",
    )
    test_db.add(company)
    test_db.commit()
    test_db.refresh(company)
    return company


@pytest.fixture
def sample_companies(test_db):
    """Create multiple sample companies."""
    import uuid
    unique_suffix = str(uuid.uuid4())[:8]
    companies = [
        IndustrialCompany(
            name=f"Alpha Industrial {unique_suffix}",
            industry_segment="distribution",
            employee_count=500,
            revenue_usd=100000000,
            ownership_type="private",
        ),
        IndustrialCompany(
            name=f"Beta Masters Corp {unique_suffix}",
            industry_segment="distribution",
            employee_count=300,
            revenue_usd=50000000,
            ownership_type="pe_backed",
            cik="0001234567",
        ),
        IndustrialCompany(
            name=f"Gamma Inc {unique_suffix}",
            industry_segment="distribution",
            employee_count=1000,
            revenue_usd=200000000,
            ownership_type="public",
            ticker="FAST",
            cik="0009876543",
        ),
    ]
    for company in companies:
        test_db.add(company)
    test_db.commit()
    for company in companies:
        test_db.refresh(company)
    return companies


@pytest.fixture
def sample_person(test_db):
    """Create a sample person."""
    person = Person(
        full_name="John Smith",
        first_name="John",
        last_name="Smith",
        email="john.smith@acmeindustrial.com",
        email_confidence="verified",
        linkedin_url="https://linkedin.com/in/johnsmith",
        city="Chicago",
        state="IL",
        bio="Experienced industrial executive with 20+ years in distribution.",
        confidence_score=0.95,
    )
    test_db.add(person)
    test_db.commit()
    test_db.refresh(person)
    return person


@pytest.fixture
def sample_people(test_db):
    """Create multiple sample people."""
    people = [
        Person(
            full_name="John Smith",
            first_name="John",
            last_name="Smith",
            email="john.smith@acme.com",
            linkedin_url="https://linkedin.com/in/johnsmith",
        ),
        Person(
            full_name="Jane Doe",
            first_name="Jane",
            last_name="Doe",
            email="jane.doe@acme.com",
            linkedin_url="https://linkedin.com/in/janedoe",
        ),
        Person(
            full_name="Bob Johnson",
            first_name="Bob",
            last_name="Johnson",
            linkedin_url="https://linkedin.com/in/bobjohnson",
        ),
        Person(
            full_name="Alice Williams",
            first_name="Alice",
            last_name="Williams",
            photo_url="https://example.com/alice.jpg",
            bio="CFO with strong financial background.",
        ),
    ]
    for person in people:
        test_db.add(person)
    test_db.commit()
    for person in people:
        test_db.refresh(person)
    return people


@pytest.fixture
def sample_company_person(test_db, sample_company, sample_person):
    """Create a sample company-person relationship."""
    cp = CompanyPerson(
        company_id=sample_company.id,
        person_id=sample_person.id,
        title="Chief Executive Officer",
        title_normalized="CEO",
        title_level="c_suite",
        is_current=True,
        is_board_member=True,
        start_date=date(2020, 1, 1),
        source="website",
    )
    test_db.add(cp)
    test_db.commit()
    test_db.refresh(cp)
    return cp


@pytest.fixture
def sample_leadership_team(test_db, sample_company, sample_people):
    """Create a full leadership team for a company."""
    roles = [
        ("Chief Executive Officer", "CEO", "c_suite", True, True),
        ("Chief Financial Officer", "CFO", "c_suite", True, False),
        ("VP of Sales", "VP Sales", "vp", True, False),
        ("Director of Operations", "Director Ops", "director", True, False),
    ]
    company_people = []
    for i, (title, normalized, level, is_current, is_board) in enumerate(roles):
        if i < len(sample_people):
            cp = CompanyPerson(
                company_id=sample_company.id,
                person_id=sample_people[i].id,
                title=title,
                title_normalized=normalized,
                title_level=level,
                is_current=is_current,
                is_board_member=is_board,
                start_date=date(2020, 1, 1),
                source="website",
            )
            test_db.add(cp)
            company_people.append(cp)
    test_db.commit()
    for cp in company_people:
        test_db.refresh(cp)
    return company_people


@pytest.fixture
def sample_leadership_changes(test_db, sample_company, sample_people):
    """Create sample leadership changes."""
    changes = [
        LeadershipChange(
            company_id=sample_company.id,
            person_id=sample_people[0].id,
            person_name=sample_people[0].full_name,
            change_type="hire",
            new_title="CEO",
            announced_date=date.today() - timedelta(days=5),
            detected_date=date.today() - timedelta(days=4),
            is_c_suite=True,
            significance_score=9,
        ),
        LeadershipChange(
            company_id=sample_company.id,
            person_id=sample_people[1].id,
            person_name=sample_people[1].full_name,
            change_type="promotion",
            old_title="VP Finance",
            new_title="CFO",
            announced_date=date.today() - timedelta(days=3),
            detected_date=date.today() - timedelta(days=2),
            is_c_suite=True,
            significance_score=8,
        ),
        LeadershipChange(
            company_id=sample_company.id,
            person_id=sample_people[2].id,
            person_name=sample_people[2].full_name,
            change_type="departure",
            old_title="VP Sales",
            announced_date=date.today() - timedelta(days=1),
            detected_date=date.today(),
            is_c_suite=False,
            significance_score=5,
        ),
    ]
    for change in changes:
        test_db.add(change)
    test_db.commit()
    for change in changes:
        test_db.refresh(change)
    return changes


@pytest.fixture
def sample_portfolio(test_db, sample_companies):
    """Create a sample portfolio with companies."""
    portfolio = PeoplePortfolio(
        name="Test PE Fund I",
        pe_firm="Test Capital Partners",
        description="First test fund",
        portfolio_type="pe_portfolio",
        is_active=True,
    )
    test_db.add(portfolio)
    test_db.commit()
    test_db.refresh(portfolio)

    # Add companies to portfolio
    for company in sample_companies[:2]:
        pc = PeoplePortfolioCompany(
            portfolio_id=portfolio.id,
            company_id=company.id,
            investment_date=date(2022, 1, 1),
            is_active=True,
        )
        test_db.add(pc)
    test_db.commit()

    return portfolio


@pytest.fixture
def sample_watchlist(test_db, sample_people):
    """Create a sample watchlist with people."""
    watchlist = PeopleWatchlist(
        name="Key Executives to Track",
        description="Important executives in our market",
    )
    test_db.add(watchlist)
    test_db.commit()
    test_db.refresh(watchlist)

    # Add people to watchlist
    for person in sample_people[:2]:
        wp = PeopleWatchlistPerson(
            watchlist_id=watchlist.id,
            person_id=person.id,
            tags=["potential_hire"],
        )
        test_db.add(wp)
    test_db.commit()

    return watchlist


@pytest.fixture
def sample_collection_job(test_db, sample_company):
    """Create a sample collection job."""
    job = PeopleCollectionJob(
        job_type="website_crawl",
        company_id=sample_company.id,
        company_ids=[sample_company.id],
        config={"source": "test"},
        status="pending",
    )
    test_db.add(job)
    test_db.commit()
    test_db.refresh(job)
    return job


@pytest.fixture
def sample_census_metadata():
    """
    Sample Census metadata for testing (no network required).
    
    Simulates response from https://api.census.gov/data/2023/acs/acs5/variables.json
    """
    return {
        "variables": {
            "B01001_001E": {
                "label": "Estimate!!Total:",
                "concept": "SEX BY AGE",
                "predicateType": "int",
                "group": "B01001",
                "limit": 0,
                "predicateOnly": True
            },
            "B01001_002E": {
                "label": "Estimate!!Total:!!Male:",
                "concept": "SEX BY AGE",
                "predicateType": "int",
                "group": "B01001",
                "limit": 0,
                "predicateOnly": True
            },
            "B01001_003E": {
                "label": "Estimate!!Total:!!Male:!!Under 5 years",
                "concept": "SEX BY AGE",
                "predicateType": "int",
                "group": "B01001",
                "limit": 0,
                "predicateOnly": True
            },
            "B01001_001M": {
                "label": "Margin of Error!!Total:",
                "concept": "SEX BY AGE",
                "predicateType": "int",
                "group": "B01001",
                "limit": 0,
                "predicateOnly": True
            },
            "NAME": {
                "label": "Geographic Area Name",
                "concept": "Geography",
                "predicateType": "string",
                "group": "N/A",
                "limit": 0,
                "predicateOnly": False
            },
            "GEO_ID": {
                "label": "Geographic Identifier",
                "concept": "Geography",
                "predicateType": "string",
                "group": "N/A",
                "limit": 0,
                "predicateOnly": False
            },
            # Different table to test filtering
            "B02001_001E": {
                "label": "Estimate!!Total:",
                "concept": "RACE",
                "predicateType": "int",
                "group": "B02001",
                "limit": 0,
                "predicateOnly": True
            }
        }
    }





