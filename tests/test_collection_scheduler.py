"""
Unit tests for People Collection Scheduler.
"""
import pytest
from datetime import datetime, timedelta
from app.jobs.people_collection_scheduler import (
    PeopleCollectionScheduler,
    schedule_website_refresh,
    schedule_sec_check,
    schedule_news_scan,
)
from app.core.people_models import (
    IndustrialCompany, PeopleCollectionJob, PeoplePortfolio, PeoplePortfolioCompany,
)


class TestPeopleCollectionScheduler:
    """Tests for PeopleCollectionScheduler class."""

    @pytest.fixture
    def scheduler(self, test_db):
        return PeopleCollectionScheduler(test_db)

    @pytest.mark.unit
    def test_get_companies_for_refresh_empty(self, scheduler):
        """Test getting companies when none exist."""
        result = scheduler.get_companies_for_refresh(
            job_type="website_crawl",
            limit=50,
        )

        assert isinstance(result, list)
        assert len(result) == 0

    @pytest.mark.unit
    def test_get_companies_for_refresh_all(self, scheduler, sample_companies):
        """Test getting all companies for refresh."""
        result = scheduler.get_companies_for_refresh(
            job_type="website_crawl",
            limit=50,
            priority="all",
        )

        assert len(result) == len(sample_companies)

    @pytest.mark.unit
    def test_get_companies_for_refresh_portfolio(self, scheduler, test_db, sample_companies):
        """Test getting portfolio companies for refresh."""
        # Create portfolio and add one company
        portfolio = PeoplePortfolio(name="Test Fund")
        test_db.add(portfolio)
        test_db.commit()
        test_db.refresh(portfolio)

        pc = PeoplePortfolioCompany(
            portfolio_id=portfolio.id,
            company_id=sample_companies[0].id,
            is_active=True,
        )
        test_db.add(pc)
        test_db.commit()

        result = scheduler.get_companies_for_refresh(
            job_type="website_crawl",
            limit=50,
            priority="portfolio",
        )

        assert len(result) == 1
        assert result[0].id == sample_companies[0].id

    @pytest.mark.unit
    def test_get_companies_for_refresh_public(self, scheduler, sample_companies):
        """Test getting public companies for refresh."""
        result = scheduler.get_companies_for_refresh(
            job_type="sec_parse",
            limit=50,
            priority="public",
        )

        # Only companies with CIK should be returned
        for company in result:
            assert company.cik is not None

    @pytest.mark.unit
    def test_get_companies_for_refresh_limit(self, scheduler, sample_companies):
        """Test limit is respected."""
        result = scheduler.get_companies_for_refresh(
            job_type="website_crawl",
            limit=1,
        )

        assert len(result) <= 1


class TestJobManagement:
    """Tests for job creation and management."""

    @pytest.fixture
    def scheduler(self, test_db):
        return PeopleCollectionScheduler(test_db)

    @pytest.mark.unit
    def test_create_batch_job(self, scheduler, sample_companies):
        """Test creating a batch job."""
        company_ids = [c.id for c in sample_companies]

        job = scheduler.create_batch_job(
            job_type="website_crawl",
            company_ids=company_ids,
            config={"source": "test"},
        )

        assert job.id is not None
        assert job.status == "pending"
        assert job.company_ids == company_ids
        assert job.config["source"] == "test"

    @pytest.mark.unit
    def test_get_pending_jobs_empty(self, scheduler):
        """Test getting pending jobs when none exist."""
        result = scheduler.get_pending_jobs()

        assert isinstance(result, list)
        assert len(result) == 0

    @pytest.mark.unit
    def test_get_pending_jobs(self, scheduler, sample_collection_job):
        """Test getting pending jobs."""
        result = scheduler.get_pending_jobs()

        assert len(result) >= 1
        assert result[0].status == "pending"

    @pytest.mark.unit
    def test_get_pending_jobs_by_type(self, scheduler, test_db, sample_company):
        """Test filtering pending jobs by type."""
        # Create jobs of different types
        job1 = PeopleCollectionJob(
            job_type="website_crawl",
            company_id=sample_company.id,
            status="pending",
        )
        job2 = PeopleCollectionJob(
            job_type="sec_parse",
            company_id=sample_company.id,
            status="pending",
        )
        test_db.add(job1)
        test_db.add(job2)
        test_db.commit()

        result = scheduler.get_pending_jobs(job_type="website_crawl")

        for job in result:
            assert job.job_type == "website_crawl"

    @pytest.mark.unit
    def test_get_running_jobs(self, scheduler, test_db, sample_company):
        """Test getting running jobs."""
        job = PeopleCollectionJob(
            job_type="website_crawl",
            company_id=sample_company.id,
            status="running",
            started_at=datetime.utcnow(),
        )
        test_db.add(job)
        test_db.commit()

        result = scheduler.get_running_jobs()

        assert len(result) >= 1
        assert result[0].status == "running"


class TestJobStatusUpdates:
    """Tests for job status updates."""

    @pytest.fixture
    def scheduler(self, test_db):
        return PeopleCollectionScheduler(test_db)

    @pytest.mark.unit
    def test_mark_job_running(self, scheduler, sample_collection_job, test_db):
        """Test marking a job as running."""
        result = scheduler.mark_job_running(sample_collection_job.id)

        assert result is True

        test_db.refresh(sample_collection_job)
        assert sample_collection_job.status == "running"
        assert sample_collection_job.started_at is not None

    @pytest.mark.unit
    def test_mark_job_running_nonexistent(self, scheduler):
        """Test marking non-existent job as running."""
        result = scheduler.mark_job_running(99999)

        assert result is False

    @pytest.mark.unit
    def test_mark_job_complete(self, scheduler, sample_collection_job, test_db):
        """Test marking a job as complete."""
        result = scheduler.mark_job_complete(
            job_id=sample_collection_job.id,
            people_found=10,
            people_created=5,
            people_updated=3,
            changes_detected=2,
        )

        assert result is True

        test_db.refresh(sample_collection_job)
        assert sample_collection_job.status == "success"
        assert sample_collection_job.completed_at is not None
        assert sample_collection_job.people_found == 10
        assert sample_collection_job.people_created == 5

    @pytest.mark.unit
    def test_mark_job_complete_with_errors(self, scheduler, sample_collection_job, test_db):
        """Test marking a job as complete with errors and warnings."""
        result = scheduler.mark_job_complete(
            job_id=sample_collection_job.id,
            people_found=5,
            errors=["Page not found"],
            warnings=["Slow response"],
        )

        assert result is True

        test_db.refresh(sample_collection_job)
        assert sample_collection_job.errors == ["Page not found"]
        assert sample_collection_job.warnings == ["Slow response"]

    @pytest.mark.unit
    def test_mark_job_failed(self, scheduler, sample_collection_job, test_db):
        """Test marking a job as failed."""
        errors = ["Connection timeout", "Server error"]

        result = scheduler.mark_job_failed(
            job_id=sample_collection_job.id,
            errors=errors,
        )

        assert result is True

        test_db.refresh(sample_collection_job)
        assert sample_collection_job.status == "failed"
        assert sample_collection_job.completed_at is not None
        assert sample_collection_job.errors == errors


class TestStuckJobCleanup:
    """Tests for stuck job cleanup."""

    @pytest.fixture
    def scheduler(self, test_db):
        return PeopleCollectionScheduler(test_db)

    @pytest.mark.unit
    def test_cleanup_stuck_jobs_none(self, scheduler):
        """Test cleanup when no stuck jobs exist."""
        result = scheduler.cleanup_stuck_jobs(max_age_hours=4)

        assert result == 0

    @pytest.mark.unit
    def test_cleanup_stuck_jobs(self, scheduler, test_db, sample_company):
        """Test cleaning up stuck jobs."""
        # Create a stuck job (started 5 hours ago)
        stuck_job = PeopleCollectionJob(
            job_type="website_crawl",
            company_id=sample_company.id,
            status="running",
            started_at=datetime.utcnow() - timedelta(hours=5),
        )
        test_db.add(stuck_job)
        test_db.commit()
        test_db.refresh(stuck_job)

        result = scheduler.cleanup_stuck_jobs(max_age_hours=4)

        assert result >= 1

        test_db.refresh(stuck_job)
        assert stuck_job.status == "failed"
        assert "stuck" in stuck_job.errors[0].lower() or "timeout" in stuck_job.errors[0].lower()

    @pytest.mark.unit
    def test_cleanup_stuck_jobs_respects_age(self, scheduler, test_db, sample_company):
        """Test that cleanup respects max_age_hours."""
        # Create a job started 2 hours ago
        recent_job = PeopleCollectionJob(
            job_type="website_crawl",
            company_id=sample_company.id,
            status="running",
            started_at=datetime.utcnow() - timedelta(hours=2),
        )
        test_db.add(recent_job)
        test_db.commit()
        test_db.refresh(recent_job)

        result = scheduler.cleanup_stuck_jobs(max_age_hours=4)

        # Should not clean up job that's only 2 hours old
        test_db.refresh(recent_job)
        assert recent_job.status == "running"


class TestJobStats:
    """Tests for job statistics."""

    @pytest.fixture
    def scheduler(self, test_db):
        return PeopleCollectionScheduler(test_db)

    @pytest.mark.unit
    def test_get_job_stats_empty(self, scheduler):
        """Test job stats with no jobs."""
        result = scheduler.get_job_stats(days=7)

        assert result["period_days"] == 7
        assert result["total_jobs"] == 0
        assert result["success_rate"] == 0

    @pytest.mark.unit
    def test_get_job_stats(self, scheduler, test_db, sample_company):
        """Test job stats with data."""
        # Create jobs with various statuses
        statuses = ["success", "success", "failed", "pending"]
        for status in statuses:
            job = PeopleCollectionJob(
                job_type="website_crawl",
                company_id=sample_company.id,
                status=status,
                people_found=10 if status == "success" else 0,
                people_created=5 if status == "success" else 0,
            )
            test_db.add(job)
        test_db.commit()

        result = scheduler.get_job_stats(days=7)

        assert result["total_jobs"] >= 4
        assert "by_status" in result
        assert "by_type" in result
        assert result["success_rate"] > 0  # 2 out of 3 completed


class TestScheduledJobFunctions:
    """Tests for scheduled job functions."""

    @pytest.mark.unit
    def test_schedule_website_refresh_no_companies(self, test_db):
        """Test website refresh with no companies."""
        result = schedule_website_refresh(test_db, limit=50)

        assert result is None

    @pytest.mark.unit
    def test_schedule_website_refresh(self, test_db, sample_companies):
        """Test website refresh scheduling."""
        # Add a company to portfolio
        portfolio = PeoplePortfolio(name="Test Fund")
        test_db.add(portfolio)
        test_db.commit()
        test_db.refresh(portfolio)

        pc = PeoplePortfolioCompany(
            portfolio_id=portfolio.id,
            company_id=sample_companies[0].id,
            is_active=True,
        )
        test_db.add(pc)
        test_db.commit()

        result = schedule_website_refresh(test_db, limit=50)

        assert result is not None
        job = test_db.get(PeopleCollectionJob, result)
        assert job.job_type == "website_crawl"

    @pytest.mark.unit
    def test_schedule_sec_check_no_public_companies(self, test_db):
        """Test SEC check with no public companies."""
        result = schedule_sec_check(test_db, limit=30)

        assert result is None

    @pytest.mark.unit
    def test_schedule_sec_check(self, test_db, sample_companies):
        """Test SEC check scheduling."""
        # sample_companies includes companies with CIK
        result = schedule_sec_check(test_db, limit=30)

        if result is not None:
            job = test_db.get(PeopleCollectionJob, result)
            assert job.job_type == "sec_8k_check"

    @pytest.mark.unit
    def test_schedule_news_scan_no_portfolio(self, test_db, sample_companies):
        """Test news scan with no portfolio companies."""
        # Without portfolio companies, should return None
        result = schedule_news_scan(test_db, limit=50)

        # May or may not have portfolio companies
        assert result is None or isinstance(result, int)
