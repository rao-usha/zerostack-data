"""
Treasury FiscalData source module.

Provides access to U.S. Treasury fiscal and debt data including:
- Daily Treasury Balance (Deposits/Withdrawals/Operating Cash)
- Total Public Debt Outstanding
- Average Interest Rates on Treasury Securities
- Monthly Treasury Statement (Revenue & Spending)
- Treasury Auction Results

API Documentation: https://fiscaldata.treasury.gov/api-documentation/
API Key: NOT REQUIRED (1,000 requests per minute)

All data is public domain from the U.S. Department of the Treasury.
"""

__all__ = ["client", "ingest", "metadata"]
