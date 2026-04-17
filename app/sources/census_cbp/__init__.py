"""
Census County Business Patterns (CBP) source module.

Provides access to establishment, employment, and payroll data by state
and NAICS industry from the Census Bureau:
- Number of establishments (ESTAB)
- Number of employees (EMP)
- Annual payroll in $1000s (PAYANN)
- NAICS industry classification

All data is publicly available via the Census Bureau API.
No API key required (optional key for higher rate limits).
"""

__all__ = ["client", "ingest", "metadata"]
