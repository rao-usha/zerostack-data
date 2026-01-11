"""
Bureau of Economic Analysis (BEA) data source adapter.

Provides access to:
- NIPA (National Income and Product Accounts) - GDP, PCE, Investment
- Regional Economic Accounts - GDP by state/metro, Personal Income
- International Transactions - Trade balance, foreign investment
- Industry Accounts - Input-Output tables, GDP by industry

API Documentation: https://apps.bea.gov/api/
API Registration: https://apps.bea.gov/api/signup/

API Key: Required (free registration)
Rate Limits: 100 requests per minute, 100 MB per request, 30 errors per minute
"""

from app.sources.bea.client import BEAClient
from app.sources.bea import metadata
from app.sources.bea import ingest

__all__ = ["BEAClient", "metadata", "ingest"]
