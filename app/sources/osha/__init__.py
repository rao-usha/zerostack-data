"""
OSHA Inspections and Violations source module.

Provides access to OSHA enforcement data via bulk CSV downloads
from the Department of Labor (DOL) enforcement data catalog.

Data includes workplace inspections, violations, penalties, and
abatement information for establishments across the United States.
"""

__all__ = ["client", "ingest", "metadata"]
