"""cms stays out of the nightly batch until its client is rate limited.

Its utilization ingest splits into parallel state groups that open ~50
connections to data.cms.gov and wedge Docker Desktop networking (2026-09-23..25).
"""
import pytest

from app.core.batch_service import DEFAULT_COLLECTION_GROUPS, TIERS


@pytest.mark.unit
def test_cms_not_in_nightly_tiers():
    keys = [s.key for tier in TIERS for s in tier.sources]
    assert "cms" not in keys
    assert not any(k.startswith("cms:") for k in keys)


@pytest.mark.unit
def test_cms_not_in_default_collection_groups():
    keys = [k for g in DEFAULT_COLLECTION_GROUPS for k in g["sources"]]
    assert not any(k == "cms" or k.startswith("cms:") for k in keys)
