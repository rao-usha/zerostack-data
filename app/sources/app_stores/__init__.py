"""
App Store data source module.

Provides iOS App Store and Google Play app metrics.
"""

from app.sources.app_stores.client import AppStoreClient

__all__ = ["AppStoreClient"]
