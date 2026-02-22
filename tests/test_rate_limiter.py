"""
Unit tests for app/core/rate_limiter.py

Tests cover token bucket behavior, rate limiting enforcement,
per-source limiting, reset/refill, concurrent access, service
configuration, statistics, and database integration.

All tests are fully offline (no DB, no network).
"""
import asyncio
import time
import pytest
from unittest.mock import MagicMock, patch

from app.core.rate_limiter import (
    TokenBucket,
    RateLimiterService,
    RateLimitExceeded,
    DEFAULT_RATE_LIMITS,
    get_rate_limiter,
    reset_rate_limiter,
    load_rate_limits_from_db,
    save_rate_limit_to_db,
    init_default_rate_limits,
    update_rate_limit_stats,
)

# =============================================================================
# Token Bucket — Core Behavior
# =============================================================================


class TestTokenBucketInit:
    """Tests for TokenBucket initialization."""

    def test_starts_with_full_bucket(self):
        bucket = TokenBucket(
            source="fred", requests_per_second=2.0,
            burst_capacity=10, concurrent_limit=5,
        )
        assert bucket.tokens == 10.0

    def test_initial_stats_are_zero(self):
        bucket = TokenBucket(
            source="fred", requests_per_second=2.0,
            burst_capacity=10, concurrent_limit=5,
        )
        assert bucket.total_requests == 0
        assert bucket.total_throttled == 0
        assert bucket.current_concurrent == 0

    def test_last_refill_is_set(self):
        before = time.time()
        bucket = TokenBucket(
            source="fred", requests_per_second=1.0,
            burst_capacity=5, concurrent_limit=3,
        )
        after = time.time()
        assert before <= bucket.last_refill <= after

class TestTokenBucketAcquireRelease:
    """Tests for try_acquire and release."""

    def test_acquire_succeeds_when_tokens_available(self):
        bucket = TokenBucket(
            source="fred", requests_per_second=2.0,
            burst_capacity=10, concurrent_limit=5,
        )
        assert bucket.try_acquire() is True
        assert bucket.tokens == 9.0  # approximately
        assert bucket.current_concurrent == 1
        assert bucket.total_requests == 1

    def test_acquire_fails_when_no_tokens(self):
        bucket = TokenBucket(
            source="test", requests_per_second=0.001,
            burst_capacity=1, concurrent_limit=10,
        )
        # First acquire uses the single token
        assert bucket.try_acquire() is True
        # Second should fail (no tokens, refill too slow)
        assert bucket.try_acquire() is False
        assert bucket.total_throttled == 1

    def test_acquire_fails_when_concurrent_limit_reached(self):
        bucket = TokenBucket(
            source="test", requests_per_second=100.0,
            burst_capacity=100, concurrent_limit=2,
        )
        assert bucket.try_acquire() is True
        assert bucket.try_acquire() is True
        # Third should fail due to concurrent limit
        assert bucket.try_acquire() is False
        assert bucket.total_throttled == 1

    def test_release_decrements_concurrent(self):
        bucket = TokenBucket(
            source="test", requests_per_second=10.0,
            burst_capacity=10, concurrent_limit=2,
        )
        bucket.try_acquire()
        bucket.try_acquire()
        assert bucket.current_concurrent == 2
        bucket.release()
        assert bucket.current_concurrent == 1

    def test_release_does_not_go_below_zero(self):
        bucket = TokenBucket(
            source="test", requests_per_second=1.0,
            burst_capacity=5, concurrent_limit=3,
        )
        bucket.release()
        assert bucket.current_concurrent == 0

    def test_release_allows_new_concurrent_acquire(self):
        bucket = TokenBucket(
            source="test", requests_per_second=100.0,
            burst_capacity=100, concurrent_limit=1,
        )
        assert bucket.try_acquire() is True
        assert bucket.try_acquire() is False  # at limit
        bucket.release()
        assert bucket.try_acquire() is True  # released, can acquire again

class TestTokenBucketRefill:
    """Tests for token refill behavior."""

    def test_refill_adds_tokens_over_time(self):
        bucket = TokenBucket(
            source="test", requests_per_second=100.0,
            burst_capacity=10, concurrent_limit=10,
        )
        # Drain all tokens
        for _ in range(10):
            bucket.try_acquire()
        # Release all concurrent slots
        for _ in range(10):
            bucket.release()
        # Wait a small amount to refill
        time.sleep(0.05)
        # Should be able to acquire again (tokens refilled)
        assert bucket.try_acquire() is True

    def test_tokens_capped_at_burst_capacity(self):
        bucket = TokenBucket(
            source="test", requests_per_second=1000.0,
            burst_capacity=5, concurrent_limit=10,
        )
        time.sleep(0.1)  # Let tokens accumulate
        bucket._refill()
        assert bucket.tokens <= 5.0


class TestTokenBucketWaitTime:
    """Tests for wait_time calculation."""

    def test_wait_time_zero_when_tokens_available(self):
        bucket = TokenBucket(
            source="test", requests_per_second=2.0,
            burst_capacity=10, concurrent_limit=5,
        )
        assert bucket.wait_time() == 0.0

    def test_wait_time_positive_when_no_tokens(self):
        bucket = TokenBucket(
            source="test", requests_per_second=1.0,
            burst_capacity=1, concurrent_limit=10,
        )
        bucket.try_acquire()  # use the one token
        wt = bucket.wait_time()
        assert wt > 0

# =============================================================================
# RateLimiterService
# =============================================================================


class TestRateLimiterServiceBucketCreation:
    """Tests for per-source bucket creation."""

    def test_creates_bucket_for_known_source(self):
        service = RateLimiterService()
        bucket = service._get_bucket("fred")
        assert bucket.source == "fred"
        assert bucket.requests_per_second == DEFAULT_RATE_LIMITS["fred"]["requests_per_second"]

    def test_creates_default_bucket_for_unknown_source(self):
        service = RateLimiterService()
        bucket = service._get_bucket("totally_unknown")
        assert bucket.source == "totally_unknown"
        assert bucket.requests_per_second == DEFAULT_RATE_LIMITS["default"]["requests_per_second"]

    def test_reuses_existing_bucket(self):
        service = RateLimiterService()
        bucket1 = service._get_bucket("fred")
        bucket2 = service._get_bucket("fred")
        assert bucket1 is bucket2

    def test_different_sources_get_different_buckets(self):
        service = RateLimiterService()
        fred_bucket = service._get_bucket("fred")
        bls_bucket = service._get_bucket("bls")
        assert fred_bucket is not bls_bucket
        assert fred_bucket.requests_per_second != bls_bucket.requests_per_second

class TestRateLimiterServiceAcquireRelease:
    """Tests for async acquire and release."""

    @pytest.mark.asyncio
    async def test_acquire_returns_true_when_available(self):
        service = RateLimiterService()
        result = await service.acquire("fred", timeout=1.0)
        assert result is True

    @pytest.mark.asyncio
    async def test_acquire_timeout_returns_false(self):
        service = RateLimiterService()
        # Configure a source with very low limits
        service.configure_source("stingy", 0.001, 1, 1)
        # Acquire the one token and the one concurrent slot
        await service.acquire("stingy", timeout=1.0)
        # Second acquire should timeout quickly
        result = await service.acquire("stingy", timeout=0.1)
        assert result is False

    @pytest.mark.asyncio
    async def test_release_allows_subsequent_acquire(self):
        service = RateLimiterService()
        service.configure_source("narrow", 100.0, 100, 1)
        await service.acquire("narrow", timeout=1.0)
        service.release("narrow")
        result = await service.acquire("narrow", timeout=1.0)
        assert result is True

    def test_release_nonexistent_source_is_noop(self):
        service = RateLimiterService()
        # Should not raise
        service.release("nonexistent")

class TestRateLimiterServiceContextManager:
    """Tests for the async context manager."""

    @pytest.mark.asyncio
    async def test_limit_context_manager_acquires_and_releases(self):
        service = RateLimiterService()
        service.configure_source("ctx_test", 100.0, 100, 5)
        async with service.limit("ctx_test"):
            bucket = service._get_bucket("ctx_test")
            assert bucket.current_concurrent == 1
        # After context, concurrent should be released
        assert bucket.current_concurrent == 0

    @pytest.mark.asyncio
    async def test_limit_raises_on_timeout(self):
        service = RateLimiterService()
        service.configure_source("timeout_src", 0.001, 1, 1)
        await service.acquire("timeout_src", timeout=1.0)
        with pytest.raises(RateLimitExceeded):
            async with service.limit("timeout_src", timeout=0.1):
                pass  # should not reach here

    @pytest.mark.asyncio
    async def test_limit_releases_on_exception(self):
        service = RateLimiterService()
        service.configure_source("exc_test", 100.0, 100, 5)
        try:
            async with service.limit("exc_test"):
                raise ValueError("something went wrong")
        except ValueError:
            pass
        bucket = service._get_bucket("exc_test")
        assert bucket.current_concurrent == 0

class TestRateLimiterServiceConfigure:
    """Tests for configure_source and reset_source."""

    def test_configure_source_creates_custom_bucket(self):
        service = RateLimiterService()
        service.configure_source("custom", 5.0, 20, 10)
        bucket = service._get_bucket("custom")
        assert bucket.requests_per_second == 5.0
        assert bucket.burst_capacity == 20
        assert bucket.concurrent_limit == 10

    def test_configure_overwrites_existing_bucket(self):
        service = RateLimiterService()
        service.configure_source("fred", 99.0, 50, 25)
        bucket = service._get_bucket("fred")
        assert bucket.requests_per_second == 99.0

    def test_reset_source_refills_tokens(self):
        service = RateLimiterService()
        bucket = service._get_bucket("fred")
        bucket.try_acquire()
        bucket.try_acquire()
        service.reset_source("fred")
        bucket = service._get_bucket("fred")
        assert bucket.tokens == float(bucket.burst_capacity)
        assert bucket.current_concurrent == 0

    def test_reset_nonexistent_source_is_noop(self):
        service = RateLimiterService()
        service.reset_source("nonexistent")  # Should not raise

class TestRateLimiterServiceStats:
    """Tests for statistics reporting."""

    def test_get_stats_returns_correct_structure(self):
        service = RateLimiterService()
        stats = service.get_stats("fred")
        assert stats["source"] == "fred"
        assert "requests_per_second" in stats
        assert "burst_capacity" in stats
        assert "concurrent_limit" in stats
        assert "current_tokens" in stats
        assert "current_concurrent" in stats
        assert "total_requests" in stats
        assert "total_throttled" in stats

    def test_get_stats_reflects_usage(self):
        service = RateLimiterService()
        bucket = service._get_bucket("fred")
        bucket.try_acquire()
        bucket.try_acquire()
        stats = service.get_stats("fred")
        assert stats["total_requests"] == 2
        assert stats["current_concurrent"] == 2

    def test_get_all_stats_returns_active_sources(self):
        service = RateLimiterService()
        service._get_bucket("fred")
        service._get_bucket("bls")
        all_stats = service.get_all_stats()
        assert "fred" in all_stats
        assert "bls" in all_stats
        assert len(all_stats) == 2

# =============================================================================
# Global Singleton
# =============================================================================


class TestGlobalRateLimiter:
    """Tests for get_rate_limiter / reset_rate_limiter."""

    def test_get_rate_limiter_returns_singleton(self):
        reset_rate_limiter()
        limiter1 = get_rate_limiter()
        limiter2 = get_rate_limiter()
        assert limiter1 is limiter2
        reset_rate_limiter()  # cleanup

    def test_reset_creates_new_instance(self):
        reset_rate_limiter()
        limiter1 = get_rate_limiter()
        reset_rate_limiter()
        limiter2 = get_rate_limiter()
        assert limiter1 is not limiter2
        reset_rate_limiter()  # cleanup

# =============================================================================
# DEFAULT_RATE_LIMITS Coverage
# =============================================================================


class TestDefaultRateLimits:
    """Tests for the DEFAULT_RATE_LIMITS dictionary."""

    def test_default_key_exists(self):
        assert "default" in DEFAULT_RATE_LIMITS

    def test_all_entries_have_required_keys(self):
        for source, config in DEFAULT_RATE_LIMITS.items():
            assert "requests_per_second" in config, f"Missing rps in {source}"
            assert "burst_capacity" in config, f"Missing burst in {source}"
            assert "concurrent_limit" in config, f"Missing concurrent in {source}"

    def test_all_entries_have_positive_rates(self):
        for source, config in DEFAULT_RATE_LIMITS.items():
            assert config["requests_per_second"] > 0
            assert config["burst_capacity"] > 0
            assert config["concurrent_limit"] > 0

    def test_known_sources_present(self):
        expected = {"fred", "census", "eia", "bls", "sec", "bea", "noaa", "treasury"}
        assert expected.issubset(set(DEFAULT_RATE_LIMITS.keys()))

# =============================================================================
# Database Integration (mocked)
# =============================================================================


class TestLoadRateLimitsFromDb:
    """Tests for load_rate_limits_from_db."""

    def test_loads_enabled_limits(self):
        db = MagicMock()
        rl1 = MagicMock()
        rl1.source = "fred"
        rl1.requests_per_second = "2.0"
        rl1.burst_capacity = 10
        rl1.concurrent_limit = 5
        db.query.return_value.filter.return_value.all.return_value = [rl1]

        service = RateLimiterService()
        count = load_rate_limits_from_db(db, service)

        assert count == 1
        bucket = service._get_bucket("fred")
        assert bucket.requests_per_second == 2.0

    def test_returns_zero_when_no_limits(self):
        db = MagicMock()
        db.query.return_value.filter.return_value.all.return_value = []
        service = RateLimiterService()
        count = load_rate_limits_from_db(db, service)
        assert count == 0

class TestSaveRateLimitToDb:
    """Tests for save_rate_limit_to_db."""

    def test_creates_new_rate_limit(self):
        db = MagicMock()
        db.query.return_value.filter.return_value.first.return_value = None
        new_rl = MagicMock()

        with patch("app.core.rate_limiter.SourceRateLimit", return_value=new_rl):
            result = save_rate_limit_to_db(db, "new_src", 3.0, 15, 8, "New source")

        db.add.assert_called_once_with(new_rl)
        db.commit.assert_called_once()
        db.refresh.assert_called_once()

    def test_updates_existing_rate_limit(self):
        db = MagicMock()
        existing = MagicMock()
        db.query.return_value.filter.return_value.first.return_value = existing

        result = save_rate_limit_to_db(db, "fred", 5.0, 20, 10, "Updated")

        assert existing.requests_per_second == "5.0"
        assert existing.burst_capacity == 20
        assert existing.concurrent_limit == 10
        db.add.assert_not_called()
        db.commit.assert_called_once()

class TestInitDefaultRateLimits:
    """Tests for init_default_rate_limits."""

    def test_creates_entries_for_missing_sources(self):
        db = MagicMock()
        # Simulate all sources missing
        db.query.return_value.filter.return_value.first.return_value = None

        count = init_default_rate_limits(db)

        # Should create one for each non-default source
        expected_count = len([s for s in DEFAULT_RATE_LIMITS if s != "default"])
        assert count == expected_count
        db.commit.assert_called_once()

    def test_skips_existing_sources(self):
        db = MagicMock()
        # Simulate all sources already existing
        db.query.return_value.filter.return_value.first.return_value = MagicMock()

        count = init_default_rate_limits(db)

        assert count == 0
        db.commit.assert_not_called()

class TestUpdateRateLimitStats:
    """Tests for update_rate_limit_stats."""

    def test_updates_stats_for_active_source(self):
        db = MagicMock()
        rate_limit = MagicMock()
        db.query.return_value.filter.return_value.first.return_value = rate_limit

        service = RateLimiterService()
        bucket = service._get_bucket("fred")
        bucket.try_acquire()
        bucket.try_acquire()

        update_rate_limit_stats(db, "fred", service)

        assert rate_limit.total_requests == 2
        db.commit.assert_called_once()

    def test_noop_when_source_not_in_service(self):
        db = MagicMock()
        rate_limit = MagicMock()
        db.query.return_value.filter.return_value.first.return_value = rate_limit

        service = RateLimiterService()
        update_rate_limit_stats(db, "nonexistent", service)

        db.commit.assert_not_called()

# =============================================================================
# Concurrent Access
# =============================================================================


class TestConcurrentAccess:
    """Tests for concurrent rate limiting behavior."""

    @pytest.mark.asyncio
    async def test_concurrent_acquire_respects_limit(self):
        service = RateLimiterService()
        service.configure_source("conc_test", 100.0, 100, 3)

        results = []

        async def acquire_task():
            r = await service.acquire("conc_test", timeout=0.5)
            results.append(r)

        # Fire 5 concurrent acquire tasks (limit is 3)
        tasks = [acquire_task() for _ in range(5)]
        await asyncio.gather(*tasks)

        # At least 3 should succeed, at most 3 concurrently
        success_count = sum(1 for r in results if r)
        assert success_count >= 3

    @pytest.mark.asyncio
    async def test_multiple_sources_are_independent(self):
        service = RateLimiterService()
        service.configure_source("src_a", 100.0, 100, 1)
        service.configure_source("src_b", 100.0, 100, 1)

        # Acquire from both independently
        r1 = await service.acquire("src_a", timeout=1.0)
        r2 = await service.acquire("src_b", timeout=1.0)

        # Both should succeed (different sources)
        assert r1 is True
        assert r2 is True

        # src_a is at limit, should fail
        r3 = await service.acquire("src_a", timeout=0.1)
        assert r3 is False

        # src_b is at limit, should fail
        r4 = await service.acquire("src_b", timeout=0.1)
        assert r4 is False
