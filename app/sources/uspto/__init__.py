"""
USPTO Patent Data source module.

Provides access to US patent data via the PatentsView API including:
- Granted patents and pre-grant publications
- Inventors and assignees (disambiguated)
- Patent classifications (CPC, IPC, USPC, WIPO)
- Citation networks

API Documentation: https://search.patentsview.org/docs/
"""

__all__ = ["client", "ingest", "metadata"]
