"""
Company data enrichment module.

Provides enrichment of portfolio company data with financials,
funding, employees, and industry classification.
"""
from app.enrichment.company import CompanyEnrichmentEngine

__all__ = ["CompanyEnrichmentEngine"]
