"""
Deals collector for Family Office investment tracking.

DISABLED (PLAN_082 / SPEC_104). The previous implementation built "investments"
from Google News headline fragments and treated every Form D issuer that
matched a family office name as an FO investment. Every row it produced was
junk. It returns no items until a source with real investor attribution
(e.g. Form D related persons from the bulk SEC loaders) is wired in.
"""

import logging
from datetime import datetime
from typing import Optional

from app.sources.family_office_collection.base_collector import FoBaseCollector
from app.sources.family_office_collection.types import (
    FoCollectionResult,
    FoCollectionSource,
)

logger = logging.getLogger(__name__)

DISABLED_REASON = (
    "FO deal extraction disabled: headline/Form D matching produced false investments (PLAN_082)"
)


class FoDealsCollector(FoBaseCollector):
    """Family office deal/investment collector (currently disabled)."""

    @property
    def source_type(self) -> FoCollectionSource:
        return FoCollectionSource.DEALS

    async def collect(
        self,
        fo_id: int,
        fo_name: str,
        website_url: Optional[str] = None,
        principal_name: Optional[str] = None,
        principal_family: Optional[str] = None,
        days_back: int = 180,
        **kwargs,
    ) -> FoCollectionResult:
        self.reset_tracking()
        logger.info(f"Skipping deals for {fo_name}: {DISABLED_REASON}")
        return self._create_result(
            fo_id=fo_id,
            fo_name=fo_name,
            success=True,
            items=[],
            warnings=[DISABLED_REASON],
            started_at=datetime.utcnow(),
        )
