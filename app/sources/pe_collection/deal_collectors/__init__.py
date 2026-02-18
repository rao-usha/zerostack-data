"""
Deal collectors.

Collectors for gathering M&A and investment data:
- SEC Form D for private placements
- Press release deal announcements
"""

from app.sources.pe_collection.deal_collectors.sec_formd_collector import (
    SECFormDCollector,
)
from app.sources.pe_collection.deal_collectors.press_release_collector import (
    PressReleaseCollector,
)

__all__ = [
    "SECFormDCollector",
    "PressReleaseCollector",
]
