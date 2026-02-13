"""
Deal collectors.

Collectors for gathering M&A and investment data:
- SEC Form D for private placements
- Press release monitors
- Deal announcement extraction
"""

from app.sources.pe_collection.deal_collectors.sec_formd_collector import SECFormDCollector

__all__ = [
    "SECFormDCollector",
]
