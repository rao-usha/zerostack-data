"""
Unit tests for app.core.job_splitter — parallel job splitting.
"""

import pytest
from unittest.mock import patch, MagicMock

from app.core.job_splitter import (
    ALL_STATES,
    SPLIT_REGISTRY,
    SplitConfig,
    get_split_config,
    split_into_state_groups,
    create_split_jobs,
)


class TestAllStates:
    """Verify the ALL_STATES constant."""

    def test_has_51_entries(self):
        """50 states + DC."""
        assert len(ALL_STATES) == 51

    def test_includes_dc(self):
        assert "DC" in ALL_STATES

    def test_all_two_letter_codes(self):
        for code in ALL_STATES:
            assert len(code) == 2
            assert code == code.upper()

    def test_no_duplicates(self):
        assert len(ALL_STATES) == len(set(ALL_STATES))


class TestSplitRegistry:
    """Verify SPLIT_REGISTRY contents."""

    def test_registry_has_entries(self):
        assert len(SPLIT_REGISTRY) >= 10

    def test_all_entries_are_tuples(self):
        for key, entry in SPLIT_REGISTRY.items():
            assert isinstance(entry, tuple), f"{key} is not a tuple"
            assert len(entry) == 3, f"{key} should have 3 fields"

    def test_nrel_is_registered(self):
        assert "nrel_resource" in SPLIT_REGISTRY
        split_by, size, domain = SPLIT_REGISTRY["nrel_resource"]
        assert split_by == "state"
        assert size == 5
        assert domain == "developer.nrel.gov"

    def test_epa_sources_share_domain(self):
        """EPA SDWIS, ACRES, and Envirofacts should share data.epa.gov."""
        epa_sources = ["epa_sdwis", "epa_acres", "epa_envirofacts"]
        for src in epa_sources:
            if src in SPLIT_REGISTRY:
                _, _, domain = SPLIT_REGISTRY[src]
                assert domain == "data.epa.gov", f"{src} should use data.epa.gov"


class TestGetSplitConfig:
    """Test get_split_config() lookups."""

    def test_known_source(self):
        config = get_split_config("nrel_resource")
        assert config is not None
        assert isinstance(config, SplitConfig)
        assert config.source_key == "nrel_resource"
        assert config.split_by == "state"
        assert config.partition_size == 5
        assert config.rate_limit_domain == "developer.nrel.gov"

    def test_unknown_source(self):
        config = get_split_config("nonexistent_source")
        assert config is None

    def test_non_splittable_source(self):
        """Sources not in registry should return None."""
        config = get_split_config("treasury")
        assert config is None


class TestSplitIntoStateGroups:
    """Test state partitioning logic."""

    def test_nrel_creates_11_groups(self):
        """51 states / 5 per group = 11 groups (10×5 + 1×1)."""
        groups = split_into_state_groups("nrel_resource")
        assert len(groups) == 11
        # First 10 groups should have 5 states each
        for g in groups[:10]:
            assert len(g) == 5
        # Last group has remaining 1 state
        assert len(groups[10]) == 1

    def test_eia_creates_4_groups(self):
        """51 states / 13 per group = 4 groups."""
        groups = split_into_state_groups("eia")
        assert len(groups) == 4
        assert len(groups[0]) == 13
        assert len(groups[1]) == 13
        assert len(groups[2]) == 13
        assert len(groups[3]) == 12  # remainder

    def test_custom_state_list(self):
        """Should respect explicit state list."""
        groups = split_into_state_groups("nrel_resource", states=["TX", "CA", "NY"])
        assert len(groups) == 1  # 3 states, partition_size=5 → 1 group
        assert groups[0] == ["TX", "CA", "NY"]

    def test_custom_state_list_multi_groups(self):
        """Larger explicit list should split correctly."""
        states = ["TX", "CA", "NY", "FL", "IL", "PA", "OH", "GA", "NC", "MI", "NJ", "VA"]
        groups = split_into_state_groups("nrel_resource", states=states)
        # 12 states / 5 per group = 3 groups
        assert len(groups) == 3
        assert len(groups[0]) == 5
        assert len(groups[1]) == 5
        assert len(groups[2]) == 2

    def test_non_splittable_returns_single_group(self):
        """Unknown source returns all states as one group."""
        groups = split_into_state_groups("treasury")
        assert len(groups) == 1
        assert groups[0] == ALL_STATES

    def test_all_states_covered(self):
        """All states should appear exactly once across groups."""
        groups = split_into_state_groups("nrel_resource")
        all_from_groups = []
        for g in groups:
            all_from_groups.extend(g)
        assert sorted(all_from_groups) == sorted(ALL_STATES)

    def test_no_empty_groups(self):
        """No group should be empty."""
        for source_key in SPLIT_REGISTRY:
            groups = split_into_state_groups(source_key)
            for g in groups:
                assert len(g) > 0, f"{source_key} produced an empty group"

    def test_all_partition_sizes_produce_valid_splits(self):
        """Every registered source should produce valid state groups."""
        for source_key in SPLIT_REGISTRY:
            groups = split_into_state_groups(source_key)
            total_states = sum(len(g) for g in groups)
            assert total_states == 51, f"{source_key}: expected 51 states, got {total_states}"


class TestCreateSplitJobs:
    """Test create_split_jobs() with mocked DB and submit_job."""

    @patch("app.core.job_queue_service.submit_job")
    def test_creates_correct_number_of_jobs(self, mock_submit):
        mock_submit.return_value = {"mode": "queued", "job_queue_id": 1}
        mock_db = MagicMock()

        job_ids = create_split_jobs(
            db=mock_db,
            source_key="nrel_resource",
            job_type="site_intel",
            base_payload={"sources": ["nrel_resource"]},
        )

        # 51 states / 5 = 11 groups = 11 calls
        assert mock_submit.call_count == 11
        assert len(job_ids) == 11

    @patch("app.core.job_queue_service.submit_job")
    def test_each_job_has_different_states(self, mock_submit):
        call_idx = [0]

        def side_effect(**kwargs):
            call_idx[0] += 1
            return {"mode": "queued", "job_queue_id": call_idx[0]}

        mock_submit.side_effect = side_effect
        mock_db = MagicMock()

        create_split_jobs(
            db=mock_db,
            source_key="nrel_resource",
            job_type="site_intel",
            base_payload={},
        )

        # Collect all states from payloads
        all_states = []
        for call in mock_submit.call_args_list:
            payload = call.kwargs["payload"]
            assert "states" in payload
            assert "split_group" in payload
            assert "split_total" in payload
            assert payload["split_total"] == 11
            all_states.extend(payload["states"])

        assert sorted(all_states) == sorted(ALL_STATES)

    @patch("app.core.job_queue_service.submit_job")
    def test_passes_priority_and_status(self, mock_submit):
        mock_submit.return_value = {"mode": "queued", "job_queue_id": 1}
        mock_db = MagicMock()

        create_split_jobs(
            db=mock_db,
            source_key="eia",
            job_type="site_intel",
            base_payload={},
            priority=7,
            queue_status="blocked",
        )

        for call in mock_submit.call_args_list:
            assert call.kwargs["priority"] == 7
            assert call.kwargs["status"] == "blocked"

    @patch("app.core.job_queue_service.submit_job")
    def test_respects_custom_states(self, mock_submit):
        mock_submit.return_value = {"mode": "queued", "job_queue_id": 1}
        mock_db = MagicMock()

        job_ids = create_split_jobs(
            db=mock_db,
            source_key="nrel_resource",
            job_type="site_intel",
            base_payload={},
            states=["TX", "CA", "NY"],
        )

        # 3 states / 5 per group = 1 job
        assert mock_submit.call_count == 1
        payload = mock_submit.call_args_list[0].kwargs["payload"]
        assert payload["states"] == ["TX", "CA", "NY"]

    def test_raises_for_unknown_source(self):
        mock_db = MagicMock()
        with pytest.raises(ValueError, match="not in SPLIT_REGISTRY"):
            create_split_jobs(
                db=mock_db,
                source_key="treasury",
                job_type="ingestion",
                base_payload={},
            )

    @patch("app.core.job_queue_service.submit_job")
    def test_sets_sources_for_site_intel(self, mock_submit):
        """site_intel jobs should always have sources list in payload."""
        mock_submit.return_value = {"mode": "queued", "job_queue_id": 1}
        mock_db = MagicMock()

        create_split_jobs(
            db=mock_db,
            source_key="nrel_resource",
            job_type="site_intel",
            base_payload={},
        )

        for call in mock_submit.call_args_list:
            payload = call.kwargs["payload"]
            assert payload["sources"] == ["nrel_resource"]
