"""
CMS Hospital Provider Data source module.

Provides access to hospital quality ratings and provider information
from the CMS (Centers for Medicare & Medicaid Services) Provider Data API.

Includes hospital type, ownership, emergency services, overall quality rating,
and domain-specific ratings (mortality, readmission, patient experience, etc.).

All data is publicly available via the CMS Provider Data API.
No API key required.
"""

__all__ = ["client", "ingest", "metadata"]
