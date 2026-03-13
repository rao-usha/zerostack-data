"""
Tests for database-driven tier configuration (#6).

Covers:
- Effective tiers = hardcoded when DB is empty
- Tier disable removes all sources from tier
- Source move between tiers
- Source disable removes it from batch
- Config override merges with defaults
"""

import pytest
from unittest.mock import MagicMock, patch
from copy import deepcopy

from app.core.batch_service import (
    resolve_effective_tiers,
    TIERS,
    TIER_BY_LEVEL,
    SourceDef,
    Tier,
)


def _make_empty_db():
    """Create a mock DB session that returns empty query results."""
    db = MagicMock()
    db.query.return_value.all.return_value = []
    return db


class TestResolveEffectiveTiers:
    """Test resolve_effective_tiers function."""

    def test_empty_db_returns_hardcoded_tiers(self):
        """When no DB overrides exist, effective tiers match hardcoded TIERS."""
        db = _make_empty_db()
        effective = resolve_effective_tiers(db)

        # Should have same number of tiers
        assert len(effective) == len(TIERS)

        # Each tier should have same level and source count
        for orig, eff in zip(TIERS, effective):
            assert eff.level == orig.level
            assert len(eff.sources) == len(orig.sources)
            assert eff.priority == orig.priority
            assert eff.max_concurrent == orig.max_concurrent

    def test_tier_disable_removes_sources(self):
        """Disabling a tier should remove all its sources."""
        tier_config = MagicMock()
        tier_config.tier_level = 1
        tier_config.enabled = False
        tier_config.priority = None
        tier_config.max_concurrent = None

        db = MagicMock()
        # First call returns tier configs, second returns source overrides
        db.query.return_value.all.side_effect = [[tier_config], []]

        effective = resolve_effective_tiers(db)

        # Tier 1 should be removed (no sources → filtered out)
        tier_levels = [t.level for t in effective]
        assert 1 not in tier_levels

    def test_tier_priority_override(self):
        """Overriding tier priority should change the effective priority."""
        tier_config = MagicMock()
        tier_config.tier_level = 2
        tier_config.enabled = True
        tier_config.priority = 99
        tier_config.max_concurrent = None

        db = MagicMock()
        db.query.return_value.all.side_effect = [[tier_config], []]

        effective = resolve_effective_tiers(db)

        tier_2 = next(t for t in effective if t.level == 2)
        assert tier_2.priority == 99

    def test_tier_max_concurrent_override(self):
        """Overriding max_concurrent should change the effective value."""
        tier_config = MagicMock()
        tier_config.tier_level = 3
        tier_config.enabled = True
        tier_config.priority = None
        tier_config.max_concurrent = 5

        db = MagicMock()
        db.query.return_value.all.side_effect = [[tier_config], []]

        effective = resolve_effective_tiers(db)

        tier_3 = next(t for t in effective if t.level == 3)
        assert tier_3.max_concurrent == 5

    def test_source_disable(self):
        """Disabling a source should remove it from batch."""
        source_override = MagicMock()
        source_override.source_key = "fred"
        source_override.enabled = False
        source_override.tier_level = None
        source_override.default_config = None

        db = MagicMock()
        db.query.return_value.all.side_effect = [[], [source_override]]

        effective = resolve_effective_tiers(db)

        # fred should not appear in any tier
        all_source_keys = [s.key for t in effective for s in t.sources]
        assert "fred" not in all_source_keys

    def test_source_move_between_tiers(self):
        """Moving a source to a different tier should update placement."""
        source_override = MagicMock()
        source_override.source_key = "fred"
        source_override.enabled = True
        source_override.tier_level = 3  # Move from tier 1 to tier 3
        source_override.default_config = None

        db = MagicMock()
        db.query.return_value.all.side_effect = [[], [source_override]]

        effective = resolve_effective_tiers(db)

        # fred should be in tier 3, not tier 1
        tier_1 = next((t for t in effective if t.level == 1), None)
        tier_3 = next((t for t in effective if t.level == 3), None)

        if tier_1:
            tier_1_keys = [s.key for s in tier_1.sources]
            assert "fred" not in tier_1_keys

        tier_3_keys = [s.key for s in tier_3.sources]
        assert "fred" in tier_3_keys

    def test_source_config_override_merges(self):
        """Config override should merge with existing default_config."""
        source_override = MagicMock()
        source_override.source_key = "fred"
        source_override.enabled = True
        source_override.tier_level = None  # Keep in same tier
        source_override.default_config = {"custom_param": "value"}

        db = MagicMock()
        db.query.return_value.all.side_effect = [[], [source_override]]

        effective = resolve_effective_tiers(db)

        # Find fred and check its config was merged
        fred = None
        for tier in effective:
            for s in tier.sources:
                if s.key == "fred":
                    fred = s
                    break
        assert fred is not None
        assert fred.default_config.get("custom_param") == "value"
        # Original config should still be present
        assert fred.default_config.get("incremental") is True

    def test_does_not_mutate_hardcoded_tiers(self):
        """resolve_effective_tiers should not mutate the global TIERS."""
        original_tier1_sources = len(TIERS[0].sources)
        original_tier1_priority = TIERS[0].priority

        source_override = MagicMock()
        source_override.source_key = "fred"
        source_override.enabled = False
        source_override.tier_level = None
        source_override.default_config = None

        db = MagicMock()
        db.query.return_value.all.side_effect = [[], [source_override]]

        resolve_effective_tiers(db)

        # Hardcoded TIERS should be unchanged
        assert len(TIERS[0].sources) == original_tier1_sources
        assert TIERS[0].priority == original_tier1_priority
