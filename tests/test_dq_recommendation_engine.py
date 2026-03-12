"""
Unit tests for DQ Recommendation Engine.
"""

import pytest
from datetime import datetime, timedelta, date
from unittest.mock import patch, MagicMock

from app.core.models import (
    Base,
    DataProfileColumn,
    DataProfileSnapshot,
    DataQualityResult,
    DataQualityRule,
    DatasetRegistry,
    DQAnomalyAlert,
    DQQualitySnapshot,
    DQRecommendation,
    IngestionJob,
    JobStatus,
    AnomalyAlertStatus,
    AnomalyAlertType,
    RecommendationCategory,
    RecommendationPriority,
    RecommendationStatus,
    RuleSeverity,
    RuleType,
)
from app.core.dq_recommendation_engine import (
    Recommendation,
    _analyze_quality_snapshots,
    _analyze_anomalies,
    _analyze_rule_violations,
    _analyze_completeness_gaps,
    _analyze_missing_coverage,
    _analyze_job_history,
    _analyze_freshness,
    generate_recommendations,
    get_recommendations,
    apply_recommendation,
    dismiss_recommendation,
    get_review_summary,
)


class TestAnalyzeQualitySnapshots:
    """Tests for quality snapshot analysis."""

    @pytest.mark.unit
    def test_low_completeness_generates_critical(self, test_db):
        """Completeness < 50% should generate critical recommendation."""
        snap = DQQualitySnapshot(
            snapshot_date=date.today(),
            source="test_source",
            table_name="test_table",
            quality_score=40.0,
            completeness_score=30.0,
            freshness_score=80.0,
            validity_score=90.0,
            consistency_score=85.0,
        )
        test_db.add(snap)
        test_db.commit()

        recs = _analyze_quality_snapshots(test_db)
        critical = [r for r in recs if r.priority == "critical"]
        assert len(critical) >= 1
        assert any("completeness" in r.title.lower() for r in critical)

    @pytest.mark.unit
    def test_low_freshness_generates_high(self, test_db):
        """Freshness < 20 should generate high priority recommendation."""
        snap = DQQualitySnapshot(
            snapshot_date=date.today(),
            source="stale_source",
            table_name="stale_table",
            quality_score=50.0,
            completeness_score=80.0,
            freshness_score=10.0,
            validity_score=90.0,
            consistency_score=85.0,
        )
        test_db.add(snap)
        test_db.commit()

        recs = _analyze_quality_snapshots(test_db)
        high = [r for r in recs if r.priority == "high"]
        assert len(high) >= 1
        assert any("stale" in r.title.lower() for r in high)

    @pytest.mark.unit
    def test_quality_drop_detected(self, test_db):
        """Quality score drop > 20pts in 7 days should be flagged."""
        old = DQQualitySnapshot(
            snapshot_date=date.today() - timedelta(days=5),
            source="drop_src",
            table_name="drop_tbl",
            quality_score=90.0,
            completeness_score=90.0,
            freshness_score=90.0,
        )
        new = DQQualitySnapshot(
            snapshot_date=date.today(),
            source="drop_src",
            table_name="drop_tbl",
            quality_score=60.0,
            completeness_score=90.0,
            freshness_score=90.0,
        )
        test_db.add_all([old, new])
        test_db.commit()

        recs = _analyze_quality_snapshots(test_db)
        degradation = [r for r in recs if "degradation" in r.title.lower()]
        assert len(degradation) >= 1

    @pytest.mark.unit
    def test_healthy_snapshot_no_recs(self, test_db):
        """Healthy scores should not generate recommendations."""
        snap = DQQualitySnapshot(
            snapshot_date=date.today(),
            source="healthy",
            table_name="healthy_tbl",
            quality_score=95.0,
            completeness_score=95.0,
            freshness_score=95.0,
            validity_score=95.0,
            consistency_score=95.0,
        )
        test_db.add(snap)
        test_db.commit()

        recs = _analyze_quality_snapshots(test_db)
        assert len(recs) == 0


class TestAnalyzeAnomalies:
    """Tests for anomaly analysis."""

    @pytest.mark.unit
    def test_multiple_open_anomalies(self, test_db):
        """3+ open anomalies on same table should generate recommendation."""
        for i in range(4):
            alert = DQAnomalyAlert(
                table_name="problem_table",
                source="src",
                alert_type=AnomalyAlertType.NULL_RATE_SPIKE,
                severity=RuleSeverity.WARNING,
                status=AnomalyAlertStatus.OPEN,
                message=f"Anomaly {i}",
            )
            test_db.add(alert)
        test_db.commit()

        recs = _analyze_anomalies(test_db)
        assert len(recs) == 1
        assert "4 unresolved" in recs[0].title

    @pytest.mark.unit
    def test_few_anomalies_no_rec(self, test_db):
        """< 3 open anomalies should not generate recommendation."""
        for i in range(2):
            alert = DQAnomalyAlert(
                table_name="ok_table",
                source="src",
                alert_type=AnomalyAlertType.ROW_COUNT_SWING,
                severity=RuleSeverity.INFO,
                status=AnomalyAlertStatus.OPEN,
                message=f"Anomaly {i}",
            )
            test_db.add(alert)
        test_db.commit()

        recs = _analyze_anomalies(test_db)
        assert len(recs) == 0


class TestAnalyzeRuleViolations:
    """Tests for rule violation analysis."""

    @pytest.mark.unit
    def test_many_violations_flagged(self, test_db):
        """5+ recent violations on a table should generate recommendation."""
        for i in range(6):
            result = DataQualityResult(
                rule_id=1,
                source="src",
                dataset_name="bad_table",
                passed=0,
                severity=RuleSeverity.ERROR,
                evaluated_at=datetime.utcnow() - timedelta(days=1),
            )
            test_db.add(result)
        test_db.commit()

        recs = _analyze_rule_violations(test_db)
        assert len(recs) >= 1


class TestAnalyzeJobHistory:
    """Tests for job history analysis."""

    @pytest.mark.unit
    def test_frequent_failures_detected(self, test_db):
        """Source with 3+ failures in last 5 runs should be flagged."""
        for i in range(5):
            status = JobStatus.FAILED if i < 4 else JobStatus.SUCCESS
            job = IngestionJob(
                source="failing_source",
                status=status,
                config={},
                created_at=datetime.utcnow() - timedelta(hours=i),
            )
            test_db.add(job)
        test_db.commit()

        recs = _analyze_job_history(test_db)
        critical = [r for r in recs if r.priority == "critical"]
        assert len(critical) >= 1
        assert any("failing_source" in r.title for r in critical)

    @pytest.mark.unit
    def test_zero_row_runs_detected(self, test_db):
        """Source succeeding with 0 rows should be flagged."""
        for i in range(6):
            job = IngestionJob(
                source="empty_source",
                status=JobStatus.SUCCESS,
                config={},
                rows_inserted=0,
                created_at=datetime.utcnow() - timedelta(hours=i),
            )
            test_db.add(job)
        test_db.commit()

        recs = _analyze_job_history(test_db)
        zero_recs = [r for r in recs if "0 rows" in r.title]
        assert len(zero_recs) >= 1


class TestAnalyzeFreshness:
    """Tests for freshness analysis."""

    @pytest.mark.unit
    def test_overdue_source_flagged(self, test_db):
        """Source not run in 14+ days should be flagged."""
        job = IngestionJob(
            source="old_source",
            status=JobStatus.SUCCESS,
            config={},
            completed_at=datetime.utcnow() - timedelta(days=20),
            created_at=datetime.utcnow() - timedelta(days=20),
        )
        test_db.add(job)
        test_db.commit()

        recs = _analyze_freshness(test_db)
        assert len(recs) >= 1
        assert any("old_source" in r.title for r in recs)


class TestAnalyzeMissingCoverage:
    """Tests for missing coverage analysis."""

    @pytest.mark.unit
    def test_source_without_rules(self, test_db):
        """Source with no DQ rules should get low-priority recommendation."""
        reg = DatasetRegistry(
            source="norules_src",
            dataset_id="ds1",
            table_name="tbl1",
            display_name="Test",
        )
        test_db.add(reg)
        test_db.commit()

        recs = _analyze_missing_coverage(test_db)
        rule_recs = [r for r in recs if "no dq rules" in r.title.lower()]
        assert len(rule_recs) >= 1

    @pytest.mark.unit
    def test_table_never_profiled(self, test_db):
        """Table without profile should get low-priority recommendation."""
        reg = DatasetRegistry(
            source="noprofile_src",
            dataset_id="ds2",
            table_name="noprofile_tbl",
            display_name="Test",
        )
        test_db.add(reg)
        test_db.commit()

        recs = _analyze_missing_coverage(test_db)
        profile_recs = [r for r in recs if "never profiled" in r.title.lower()]
        assert len(profile_recs) >= 1


class TestGenerateRecommendations:
    """Tests for the full orchestrator."""

    @pytest.mark.unit
    def test_generates_and_persists(self, test_db):
        """Full run should persist recommendations to DB."""
        # Seed some data that triggers recommendations
        snap = DQQualitySnapshot(
            snapshot_date=date.today(),
            source="test",
            table_name="test_tbl",
            quality_score=30.0,
            completeness_score=20.0,
            freshness_score=10.0,
        )
        test_db.add(snap)
        test_db.commit()

        recs = generate_recommendations(test_db)
        assert len(recs) > 0

        # Verify persisted
        stored = test_db.query(DQRecommendation).all()
        assert len(stored) == len(recs)
        assert all(r.status == RecommendationStatus.OPEN for r in stored)

    @pytest.mark.unit
    def test_deduplication(self, test_db):
        """Duplicate signals should be deduplicated."""
        # Two snapshots for same source but different tables, both low completeness
        for i in range(2):
            snap = DQQualitySnapshot(
                snapshot_date=date.today(),
                source="dup_src",
                table_name=f"dup_tbl_{i}",
                quality_score=30.0,
                completeness_score=20.0,
                freshness_score=90.0,
            )
            test_db.add(snap)
        test_db.commit()

        recs = generate_recommendations(test_db)
        titles = [r.title for r in recs]
        # Each unique (category, source, table, title) should appear only once
        # Both tables have low completeness, but they're different tables so both should appear
        completeness_titles = [t for t in titles if "completeness" in t.lower()]
        assert len(completeness_titles) == 2

    @pytest.mark.unit
    def test_old_recs_expired(self, test_db):
        """Recommendations older than 7 days should be dismissed."""
        old_rec = DQRecommendation(
            category=RecommendationCategory.DATA_QUALITY,
            priority=RecommendationPriority.LOW,
            title="Old rec",
            description="Should be dismissed",
            suggested_action="None",
            status=RecommendationStatus.OPEN,
            created_at=datetime.utcnow() - timedelta(days=10),
        )
        test_db.add(old_rec)
        test_db.commit()

        generate_recommendations(test_db)

        refreshed = test_db.query(DQRecommendation).filter(
            DQRecommendation.title == "Old rec"
        ).first()
        assert refreshed.status == RecommendationStatus.DISMISSED


class TestGetRecommendations:
    """Tests for query function."""

    @pytest.mark.unit
    def test_filter_by_category(self, test_db):
        """Should filter by category."""
        r1 = DQRecommendation(
            category=RecommendationCategory.DATA_QUALITY,
            priority=RecommendationPriority.HIGH,
            title="DQ rec",
            description="d",
            suggested_action="a",
            status=RecommendationStatus.OPEN,
        )
        r2 = DQRecommendation(
            category=RecommendationCategory.ORCHESTRATION,
            priority=RecommendationPriority.MEDIUM,
            title="Orch rec",
            description="d",
            suggested_action="a",
            status=RecommendationStatus.OPEN,
        )
        test_db.add_all([r1, r2])
        test_db.commit()

        results = get_recommendations(test_db, category="data_quality")
        assert all(
            r.category == RecommendationCategory.DATA_QUALITY for r in results
        )

    @pytest.mark.unit
    def test_filter_by_priority(self, test_db):
        """Should filter by priority."""
        r1 = DQRecommendation(
            category=RecommendationCategory.DATA_QUALITY,
            priority=RecommendationPriority.CRITICAL,
            title="Crit",
            description="d",
            suggested_action="a",
            status=RecommendationStatus.OPEN,
        )
        r2 = DQRecommendation(
            category=RecommendationCategory.DATA_QUALITY,
            priority=RecommendationPriority.LOW,
            title="Low",
            description="d",
            suggested_action="a",
            status=RecommendationStatus.OPEN,
        )
        test_db.add_all([r1, r2])
        test_db.commit()

        results = get_recommendations(test_db, priority="critical")
        assert len(results) == 1
        assert results[0].title == "Crit"


class TestApplyDismiss:
    """Tests for apply/dismiss actions."""

    @pytest.mark.unit
    def test_apply_recommendation(self, test_db):
        """Apply should set status and timestamp."""
        rec = DQRecommendation(
            category=RecommendationCategory.ORCHESTRATION,
            priority=RecommendationPriority.HIGH,
            title="Test apply",
            description="d",
            suggested_action="a",
            status=RecommendationStatus.OPEN,
            auto_fixable=True,
            fix_action="re_ingest",
        )
        test_db.add(rec)
        test_db.commit()

        result = apply_recommendation(test_db, rec.id)
        assert result.status == RecommendationStatus.APPLIED
        assert result.applied_at is not None

    @pytest.mark.unit
    def test_dismiss_recommendation(self, test_db):
        """Dismiss should set status and timestamp."""
        rec = DQRecommendation(
            category=RecommendationCategory.DATA_QUALITY,
            priority=RecommendationPriority.LOW,
            title="Test dismiss",
            description="d",
            suggested_action="a",
            status=RecommendationStatus.OPEN,
        )
        test_db.add(rec)
        test_db.commit()

        result = dismiss_recommendation(test_db, rec.id)
        assert result.status == RecommendationStatus.DISMISSED
        assert result.dismissed_at is not None

    @pytest.mark.unit
    def test_apply_nonexistent_returns_none(self, test_db):
        """Applying nonexistent ID should return None."""
        assert apply_recommendation(test_db, 99999) is None


class TestGetReviewSummary:
    """Tests for review summary."""

    @pytest.mark.unit
    def test_summary_with_data(self, test_db):
        """Summary should include source scores and rec counts."""
        snap = DQQualitySnapshot(
            snapshot_date=date.today(),
            source="summary_src",
            table_name="summary_tbl",
            quality_score=85.0,
            completeness_score=90.0,
            freshness_score=80.0,
        )
        rec = DQRecommendation(
            category=RecommendationCategory.DATA_QUALITY,
            priority=RecommendationPriority.HIGH,
            title="Summary test",
            description="d",
            suggested_action="a",
            status=RecommendationStatus.OPEN,
        )
        test_db.add_all([snap, rec])
        test_db.commit()

        summary = get_review_summary(test_db)
        assert "summary_src" in summary["source_scores"]
        assert summary["source_scores"]["summary_src"]["quality_score"] == 85.0
        assert "generated_at" in summary
        assert len(summary["top_recommendations"]) >= 1

    @pytest.mark.unit
    def test_summary_empty_db(self, test_db):
        """Summary on empty DB should return valid structure."""
        summary = get_review_summary(test_db)
        assert summary["open_anomalies"] == 0
        assert summary["source_scores"] == {}
        assert summary["top_recommendations"] == []
