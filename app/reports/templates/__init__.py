"""Report templates package."""

from app.reports.templates.investor_profile import InvestorProfileTemplate
from app.reports.templates.portfolio_detail import PortfolioDetailTemplate
from app.reports.templates.data_quality import DataQualityTemplate
from app.reports.templates.medspa_market import MedSpaMarketTemplate

__all__ = [
    "InvestorProfileTemplate",
    "PortfolioDetailTemplate",
    "DataQualityTemplate",
    "MedSpaMarketTemplate",
]
