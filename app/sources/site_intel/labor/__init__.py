"""
Labor Market Collectors.

Data sources:
- BLS OES: Occupational wages
- BLS QCEW: Industry employment
- Census LEHD: Commuting patterns
- Census ACS: Demographics, education
"""

from app.sources.site_intel.labor.bls_collector import BLSLaborCollector

__all__ = ["BLSLaborCollector"]
