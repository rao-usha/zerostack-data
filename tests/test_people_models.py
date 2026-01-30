"""
Unit tests for People & Org Chart Platform database models.
"""
import pytest
from datetime import date, datetime
from sqlalchemy.exc import IntegrityError
from app.core.people_models import (
    Person, IndustrialCompany, CompanyPerson, PersonExperience,
    PersonEducation, OrgChartSnapshot, LeadershipChange, PeopleCollectionJob,
    PeoplePortfolio, PeoplePortfolioCompany, PeoplePeerSet, PeoplePeerSetMember,
    PeopleWatchlist, PeopleWatchlistPerson,
)


class TestPersonModel:
    """Tests for the Person model."""

    @pytest.mark.unit
    def test_person_creation(self, test_db):
        """Test creating a person record."""
        person = Person(
            full_name="John Smith",
            first_name="John",
            last_name="Smith",
            email="john.smith@example.com",
            linkedin_url="https://linkedin.com/in/johnsmith",
        )
        test_db.add(person)
        test_db.commit()
        test_db.refresh(person)

        assert person.id is not None
        assert person.full_name == "John Smith"
        assert person.is_canonical is True
        assert person.country == "USA"
        assert isinstance(person.created_at, datetime)

    @pytest.mark.unit
    def test_person_linkedin_unique(self, test_db):
        """Test that LinkedIn URL must be unique."""
        person1 = Person(
            full_name="John Smith",
            linkedin_url="https://linkedin.com/in/unique",
        )
        test_db.add(person1)
        test_db.commit()

        person2 = Person(
            full_name="Jane Doe",
            linkedin_url="https://linkedin.com/in/unique",  # Same URL
        )
        test_db.add(person2)

        with pytest.raises(IntegrityError):
            test_db.commit()

    @pytest.mark.unit
    def test_person_name_required(self, test_db):
        """Test that full_name is required."""
        person = Person(
            email="test@example.com",
        )
        test_db.add(person)

        with pytest.raises(IntegrityError):
            test_db.commit()


class TestIndustrialCompanyModel:
    """Tests for the IndustrialCompany model."""

    @pytest.mark.unit
    def test_company_creation(self, test_db):
        """Test creating a company record."""
        company = IndustrialCompany(
            name="Test Corp",
            website="https://testcorp.com",
            industry_segment="distribution",
            ownership_type="private",
        )
        test_db.add(company)
        test_db.commit()
        test_db.refresh(company)

        assert company.id is not None
        assert company.name == "Test Corp"
        assert company.headquarters_country == "USA"
        assert company.status == "active"
        assert company.is_subsidiary is False

    @pytest.mark.unit
    def test_company_name_unique(self, test_db):
        """Test that company name must be unique."""
        company1 = IndustrialCompany(name="Unique Corp")
        test_db.add(company1)
        test_db.commit()

        company2 = IndustrialCompany(name="Unique Corp")
        test_db.add(company2)

        with pytest.raises(IntegrityError):
            test_db.commit()

    @pytest.mark.unit
    def test_company_parent_relationship(self, test_db):
        """Test parent-subsidiary relationship."""
        parent = IndustrialCompany(name="Parent Corp")
        test_db.add(parent)
        test_db.commit()
        test_db.refresh(parent)

        subsidiary = IndustrialCompany(
            name="Subsidiary Inc",
            parent_company_id=parent.id,
            is_subsidiary=True,
        )
        test_db.add(subsidiary)
        test_db.commit()
        test_db.refresh(subsidiary)

        assert subsidiary.parent_company_id == parent.id
        assert subsidiary.is_subsidiary is True


class TestCompanyPersonModel:
    """Tests for the CompanyPerson model."""

    @pytest.mark.unit
    def test_company_person_creation(self, sample_company, sample_person, test_db):
        """Test creating a company-person relationship."""
        cp = CompanyPerson(
            company_id=sample_company.id,
            person_id=sample_person.id,
            title="Chief Executive Officer",
            title_level="c_suite",
            is_current=True,
        )
        test_db.add(cp)
        test_db.commit()
        test_db.refresh(cp)

        assert cp.id is not None
        assert cp.company_id == sample_company.id
        assert cp.person_id == sample_person.id
        assert cp.is_board_member is False
        assert cp.confidence == "medium"

    @pytest.mark.unit
    def test_company_person_board_member(self, sample_company, sample_person, test_db):
        """Test board member flags."""
        cp = CompanyPerson(
            company_id=sample_company.id,
            person_id=sample_person.id,
            title="Board Chair",
            is_current=True,
            is_board_member=True,
            is_board_chair=True,
            board_committee="audit",
        )
        test_db.add(cp)
        test_db.commit()
        test_db.refresh(cp)

        assert cp.is_board_member is True
        assert cp.is_board_chair is True
        assert cp.board_committee == "audit"


class TestLeadershipChangeModel:
    """Tests for the LeadershipChange model."""

    @pytest.mark.unit
    def test_leadership_change_creation(self, sample_company, sample_person, test_db):
        """Test creating a leadership change record."""
        change = LeadershipChange(
            company_id=sample_company.id,
            person_id=sample_person.id,
            person_name=sample_person.full_name,
            change_type="hire",
            new_title="CEO",
            announced_date=date.today(),
            is_c_suite=True,
            significance_score=9,
        )
        test_db.add(change)
        test_db.commit()
        test_db.refresh(change)

        assert change.id is not None
        assert change.change_type == "hire"
        assert change.is_c_suite is True
        assert change.detected_date is not None

    @pytest.mark.unit
    def test_leadership_change_types(self, sample_company, test_db):
        """Test various change types."""
        change_types = ["hire", "departure", "promotion", "demotion", "retirement"]

        for i, change_type in enumerate(change_types):
            change = LeadershipChange(
                company_id=sample_company.id,
                person_name=f"Person {i}",
                change_type=change_type,
                effective_date=date.today(),
            )
            test_db.add(change)

        test_db.commit()

        changes = test_db.query(LeadershipChange).filter(
            LeadershipChange.company_id == sample_company.id
        ).all()
        assert len(changes) == 5


class TestPeopleCollectionJobModel:
    """Tests for the PeopleCollectionJob model."""

    @pytest.mark.unit
    def test_collection_job_creation(self, sample_company, test_db):
        """Test creating a collection job."""
        job = PeopleCollectionJob(
            job_type="website_crawl",
            company_id=sample_company.id,
            config={"depth": 2},
            status="pending",
        )
        test_db.add(job)
        test_db.commit()
        test_db.refresh(job)

        assert job.id is not None
        assert job.status == "pending"
        assert job.people_found == 0

    @pytest.mark.unit
    def test_collection_job_batch(self, sample_companies, test_db):
        """Test batch collection job with multiple companies."""
        company_ids = [c.id for c in sample_companies]
        job = PeopleCollectionJob(
            job_type="sec_parse",
            company_ids=company_ids,
            config={"filing_type": "8-K"},
            status="running",
        )
        test_db.add(job)
        test_db.commit()
        test_db.refresh(job)

        assert job.company_ids == company_ids
        assert len(job.company_ids) == 3


class TestPortfolioModels:
    """Tests for portfolio-related models."""

    @pytest.mark.unit
    def test_portfolio_creation(self, test_db):
        """Test creating a portfolio."""
        portfolio = PeoplePortfolio(
            name="Test Fund I",
            pe_firm="Test Capital",
            portfolio_type="pe_portfolio",
        )
        test_db.add(portfolio)
        test_db.commit()
        test_db.refresh(portfolio)

        assert portfolio.id is not None
        assert portfolio.is_active is True

    @pytest.mark.unit
    def test_portfolio_company_membership(self, sample_companies, test_db):
        """Test portfolio company membership."""
        portfolio = PeoplePortfolio(name="Fund II")
        test_db.add(portfolio)
        test_db.commit()
        test_db.refresh(portfolio)

        for company in sample_companies:
            pc = PeoplePortfolioCompany(
                portfolio_id=portfolio.id,
                company_id=company.id,
                investment_date=date(2023, 1, 1),
                is_active=True,
            )
            test_db.add(pc)

        test_db.commit()

        members = test_db.query(PeoplePortfolioCompany).filter(
            PeoplePortfolioCompany.portfolio_id == portfolio.id
        ).all()
        assert len(members) == 3


class TestWatchlistModels:
    """Tests for watchlist-related models."""

    @pytest.mark.unit
    def test_watchlist_creation(self, test_db):
        """Test creating a watchlist."""
        watchlist = PeopleWatchlist(
            name="Key Execs",
            user_id="user123",
            description="Important executives to track",
        )
        test_db.add(watchlist)
        test_db.commit()
        test_db.refresh(watchlist)

        assert watchlist.id is not None

    @pytest.mark.unit
    def test_watchlist_person_membership(self, sample_people, test_db):
        """Test watchlist person membership."""
        watchlist = PeopleWatchlist(name="My List")
        test_db.add(watchlist)
        test_db.commit()
        test_db.refresh(watchlist)

        for person in sample_people[:2]:
            wp = PeopleWatchlistPerson(
                watchlist_id=watchlist.id,
                person_id=person.id,
                tags=["ceo_candidate"],
            )
            test_db.add(wp)

        test_db.commit()

        members = test_db.query(PeopleWatchlistPerson).filter(
            PeopleWatchlistPerson.watchlist_id == watchlist.id
        ).all()
        assert len(members) == 2
        assert members[0].tags == ["ceo_candidate"]


class TestPeerSetModels:
    """Tests for peer set models."""

    @pytest.mark.unit
    def test_peer_set_creation(self, test_db):
        """Test creating a peer set."""
        peer_set = PeoplePeerSet(
            name="Fastener Distributors",
            industry="distribution",
            criteria={"revenue_min": 50000000, "revenue_max": 500000000},
        )
        test_db.add(peer_set)
        test_db.commit()
        test_db.refresh(peer_set)

        assert peer_set.id is not None
        assert peer_set.criteria["revenue_min"] == 50000000

    @pytest.mark.unit
    def test_peer_set_members(self, sample_companies, test_db):
        """Test peer set membership with primary company."""
        peer_set = PeoplePeerSet(name="Comparison Set")
        test_db.add(peer_set)
        test_db.commit()
        test_db.refresh(peer_set)

        # First company is primary
        for i, company in enumerate(sample_companies):
            member = PeoplePeerSetMember(
                peer_set_id=peer_set.id,
                company_id=company.id,
                is_primary=(i == 0),
            )
            test_db.add(member)

        test_db.commit()

        members = test_db.query(PeoplePeerSetMember).filter(
            PeoplePeerSetMember.peer_set_id == peer_set.id
        ).all()
        assert len(members) == 3

        primary = [m for m in members if m.is_primary]
        assert len(primary) == 1
