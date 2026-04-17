"""
Census Business Formation Statistics (BFS) source module.

Provides access to business application data by state from the Census Bureau:
- Total business applications (BA_BA)
- Applications with planned wages (BA_WBA)
- High-propensity business applications (BA_HBA)
- Applications with first payroll (BA_CBA)

All data is publicly available via the Census Bureau API.
No API key required (optional key for higher rate limits).
"""

__all__ = ["client", "ingest", "metadata"]
