"""
Analytics module for dashboard and reporting.

T13: Dashboard Analytics API
T17: Portfolio Comparison Tool (future)
T18: Investor Similarity & Recommendations
"""

__all__ = []

# T13: Dashboard Analytics (Tab 1)
try:
    from app.analytics.dashboard import DashboardAnalytics, get_dashboard_analytics
    __all__.extend(["DashboardAnalytics", "get_dashboard_analytics"])
except ImportError:
    pass

# T18: Recommendations (Tab 2)
try:
    from app.analytics.recommendations import RecommendationEngine
    __all__.append("RecommendationEngine")
except ImportError:
    pass
