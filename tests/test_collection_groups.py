"""
Tests for flexible collection groups (Phase 3).

Covers:
- resolve_collection_groups() returns defaults when DB empty
- resolve_collection_groups() returns DB groups when populated
- seed_default_collection_groups() is idempotent
- DEFAULT_COLLECTION_GROUPS covers all tier sources
- launch_batch_collection accepts group_name parameter
"""

import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime


@pytest.mark.unit
class TestDefaultCollectionGroups:
    """Tests for default collection group definitions."""

    def test_four_default_groups_defined(self):
        from app.core.batch_service import DEFAULT_COLLECTION_GROUPS
        assert len(DEFAULT_COLLECTION_GROUPS) == 4

    def test_default_groups_have_required_fields(self):
        from app.core.batch_service import DEFAULT_COLLECTION_GROUPS
        for g in DEFAULT_COLLECTION_GROUPS:
            assert "name" in g
            assert "priority" in g
            assert "max_concurrent" in g
            assert "sources" in g
            assert len(g["sources"]) > 0

    def test_default_priorities_ordered(self):
        from app.core.batch_service import DEFAULT_COLLECTION_GROUPS
        priorities = [g["priority"] for g in DEFAULT_COLLECTION_GROUPS]
        assert priorities == sorted(priorities)

    def test_critical_group_has_treasury(self):
        from app.core.batch_service import DEFAULT_COLLECTION_GROUPS
        critical = [g for g in DEFAULT_COLLECTION_GROUPS if g["name"] == "critical"][0]
        assert "treasury" in critical["sources"]
        assert "fred" in critical["sources"]


@pytest.mark.unit
class TestResolveCollectionGroups:
    """Tests for resolve_collection_groups()."""

    def test_returns_defaults_when_db_empty(self):
        from app.core.batch_service import resolve_collection_groups

        db = MagicMock()
        db.query.return_value.filter.return_value.all.return_value = []

        groups = resolve_collection_groups(db)

        assert len(groups) == 4
        assert groups[0]["name"] == "critical"
        assert groups[0]["priority"] == 1

    def test_returns_db_groups_when_populated(self):
        from app.core.batch_service import resolve_collection_groups

        db = MagicMock()

        # First query: CollectionGroup
        mock_group = MagicMock()
        mock_group.name = "my_group"
        mock_group.priority = 2
        mock_group.max_concurrent = 5
        mock_group.enabled = True

        # Second query: SourceConfig for group
        mock_sc = MagicMock()
        mock_sc.source = "fred"
        mock_sc.depends_on = None
        mock_sc.priority = 2

        call_count = [0]

        def mock_query(model):
            result = MagicMock()
            if call_count[0] == 0:
                call_count[0] += 1
                result.filter.return_value.all.return_value = [mock_group]
                return result
            else:
                result.filter.return_value.order_by.return_value.all.return_value = [mock_sc]
                return result

        db.query = mock_query

        groups = resolve_collection_groups(db)

        assert len(groups) == 1
        assert groups[0]["name"] == "my_group"
        assert groups[0]["sources"][0]["key"] == "fred"


@pytest.mark.unit
class TestSeedDefaultGroups:
    """Tests for seed_default_collection_groups()."""

    def test_seeds_four_groups(self):
        from app.core.batch_service import seed_default_collection_groups

        db = MagicMock()
        # No existing groups
        db.query.return_value.filter.return_value.first.return_value = None

        created = seed_default_collection_groups(db)

        assert created == 4
        db.commit.assert_called_once()

    def test_idempotent_when_groups_exist(self):
        from app.core.batch_service import seed_default_collection_groups

        db = MagicMock()
        # All groups already exist
        db.query.return_value.filter.return_value.first.return_value = MagicMock()

        created = seed_default_collection_groups(db)

        assert created == 0


@pytest.mark.unit
class TestLaunchWithGroup:
    """Tests for launch_batch_collection with group_name."""

    @pytest.mark.asyncio
    @patch("app.core.batch_service.WORKER_MODE", True)
    @patch("app.core.batch_service.resolve_effective_tiers")
    @patch("app.core.batch_service.submit_job")
    async def test_accepts_group_name_parameter(self, mock_submit, mock_resolve):
        """launch_batch_collection should accept group_name without error."""
        from app.core.batch_service import launch_batch_collection

        mock_resolve.return_value = []  # No tiers = no jobs
        db = MagicMock()

        result = await launch_batch_collection(db, group_name="critical")

        assert "batch_run_id" in result
        assert result["total_jobs"] == 0

    @pytest.mark.asyncio
    @patch("app.core.batch_service.WORKER_MODE", True)
    @patch("app.core.batch_service.resolve_effective_tiers")
    @patch("app.core.batch_service.submit_job")
    async def test_accepts_mode_parameter(self, mock_submit, mock_resolve):
        """launch_batch_collection should accept mode parameter."""
        from app.core.batch_service import launch_batch_collection

        mock_resolve.return_value = []
        db = MagicMock()

        result = await launch_batch_collection(db, mode="incremental")

        assert "batch_run_id" in result


@pytest.mark.unit
class TestCollectionGroupModel:
    """Tests for CollectionGroup SQLAlchemy model."""

    def test_model_has_required_columns(self):
        from app.core.models import CollectionGroup

        assert hasattr(CollectionGroup, "name")
        assert hasattr(CollectionGroup, "description")
        assert hasattr(CollectionGroup, "priority")
        assert hasattr(CollectionGroup, "max_concurrent")
        assert hasattr(CollectionGroup, "enabled")

    def test_source_config_has_scheduling_fields(self):
        from app.core.models import SourceConfig

        assert hasattr(SourceConfig, "schedule_cron")
        assert hasattr(SourceConfig, "schedule_frequency")
        assert hasattr(SourceConfig, "collection_group")
        assert hasattr(SourceConfig, "priority")
        assert hasattr(SourceConfig, "depends_on")
        assert hasattr(SourceConfig, "enabled")
        assert hasattr(SourceConfig, "supports_incremental")
        assert hasattr(SourceConfig, "watermark_column")
