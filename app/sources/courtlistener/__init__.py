"""
CourtListener Bankruptcy Dockets source module.

Provides access to federal bankruptcy court docket data via
the CourtListener REST API v4 (Free Law Project).

Data includes bankruptcy filings, case information, court details,
and docket metadata for Chapter 7, 11, and 13 cases.
"""

__all__ = ["client", "ingest", "metadata"]
