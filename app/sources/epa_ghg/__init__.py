"""
EPA Greenhouse Gas Reporting Program (GHGRP) source module.

Provides access to facility-level greenhouse gas emissions data from the
EPA Envirofacts REST API. Includes facility location, industry type,
total reported emissions, and parent company information.

All data is publicly available via the EPA Envirofacts API.
No API key required.
"""

__all__ = ["client", "ingest", "metadata"]
