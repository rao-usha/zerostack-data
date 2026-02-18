"""
Public LP (Limited Partner) Strategies Data Source Adapter.

Focused on PUBLIC LP investment strategy documents from public pension funds
such as CalPERS, CalSTRS, New York State Common Retirement Fund, and Texas TRS.

This source adapter handles:
- Investment committee presentations
- Quarterly investment reports
- Policy statements
- Pacing plans

Data is PUBLIC and obtained from official LP websites and disclosure portals.

Usage:
    from app.sources.public_lp_strategies.ingest import register_lp_fund, register_lp_document
    from app.sources.public_lp_strategies.types import LpFundInput, LpDocumentInput
"""

__version__ = "0.1.0"
__source_name__ = "public_lp_strategies"
