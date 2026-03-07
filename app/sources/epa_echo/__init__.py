"""
EPA ECHO (Enforcement and Compliance History Online) source module.

Provides access to EPA facility compliance and enforcement data including:
- Facility search by state, NAICS, ZIP code, media program
- Compliance status and violation history
- Inspection and penalty records
- Environmental program participation (AIR, WATER, RCRA, SDWA)

All data is publicly available via the EPA ECHO REST API.
No API key required.
"""

__all__ = ["client", "ingest", "metadata"]
