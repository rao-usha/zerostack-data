"""
NPPES NPI Registry data source.

This module provides ingestion and search functions for the
National Plan and Provider Enumeration System (NPPES) NPI Registry,
maintained by CMS/HHS. No API key is required.

Data includes:
- Individual (NPI-1) and organizational (NPI-2) healthcare providers
- Practice addresses, taxonomy codes, enumeration dates
- Useful for MedSpa discovery and healthcare provider mapping
"""

__all__ = ["client", "ingest", "metadata"]
