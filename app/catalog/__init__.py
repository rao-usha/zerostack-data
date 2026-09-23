"""
Dataset catalog (SPEC_123): one declared DatasetSpec registry that is both
the ops spine and the customer contract.

    from app.catalog import get_catalog, get_spec
"""

from app.catalog.registry import (  # noqa: F401
    dataset_for_producer,
    filter_specs,
    get_catalog,
    get_spec,
    producer_index,
)
from app.catalog.spec import DatasetSpec  # noqa: F401
