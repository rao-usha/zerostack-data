"""Google Places API (New / v1) client for the Atlas competition lookup.

SPEC_101 — used by app/services/atlas/ratings.py to layer ratings onto
the CBP-derived count. ToS:
  - 30-day max cache TTL
  - "Powered by Google" attribution when displaying
  - No place_id leakage outside the cache table
"""
from app.sources.google_places.client import (
    GooglePlacesClient,
    GooglePlacesError,
)

__all__ = ["GooglePlacesClient", "GooglePlacesError"]
