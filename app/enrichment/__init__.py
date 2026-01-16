"""
Company and Investor data enrichment module.

Provides enrichment of:
- Portfolio company data (financials, funding, employees, industry)
- Investor profiles (contacts, AUM history, preferences)
"""
from app.enrichment.company import CompanyEnrichmentEngine
from app.enrichment.investor import InvestorEnrichmentEngine

__all__ = ["CompanyEnrichmentEngine", "InvestorEnrichmentEngine"]
