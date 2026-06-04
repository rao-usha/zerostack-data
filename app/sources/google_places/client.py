"""SPEC_101 — Google Places API (New / v1) client.

We only use one endpoint: `places:searchNearby` — POST with a JSON body.
It returns up to 20 places near a lat/lon filtered by Place Type. The
fields we ask for are the minimal set needed to aggregate ratings:
  - id (Place ID — never exposed past the cache table per ToS)
  - rating         (0.0 – 5.0)
  - userRatingCount (int, may be 0)
We deliberately do NOT request displayName, formattedAddress,
reviews, or photos — neither needed for an aggregate, and reviews
text would introduce PII concerns.

Auth: `X-Goog-Api-Key` header (NOT a `?key=` query param; the New
Places API uses headers).

Field mask: required by the v1 API on every request; we pass exactly
the three fields above.

Soft-fail: returns an empty list on any non-200 response. Errors are
logged WARN, never raised at the call site.
"""
from __future__ import annotations

import logging
import os
from typing import Any, Dict, List, Optional

import httpx

logger = logging.getLogger(__name__)

_GOOGLE_PLACES_URL = "https://places.googleapis.com/v1/places:searchNearby"
_DEFAULT_FIELD_MASK = "places.id,places.rating,places.userRatingCount"
_MAX_RESULTS = 20  # API hard cap


class GooglePlacesError(Exception):
    """Raised only for programmer errors (missing key). Network /
    quota failures are soft-failed in `search_nearby`."""


class GooglePlacesClient:
    """Thin wrapper around places:searchNearby. Reads
    GOOGLE_PLACES_API_KEY at construction (or accepts an explicit
    override for testing). Stateless beyond the key + timeout."""

    def __init__(self,
                 api_key: Optional[str] = None,
                 timeout_seconds: float = 8.0) -> None:
        self.api_key = api_key or os.environ.get("GOOGLE_PLACES_API_KEY", "")
        self.timeout_seconds = float(timeout_seconds)

    @property
    def has_key(self) -> bool:
        return bool(self.api_key)

    def search_nearby(self,
                       lat: float, lon: float,
                       radius_m: float,
                       included_types: List[str],
                       max_count: int = _MAX_RESULTS,
                       field_mask: str = _DEFAULT_FIELD_MASK,
                       ) -> List[Dict[str, Any]]:
        """Return a list of place dicts (id/rating/userRatingCount).
        Soft-fails with [] on any failure including a missing key."""
        if not self.has_key:
            logger.info("google_places: no GOOGLE_PLACES_API_KEY; skipping")
            return []
        if not included_types:
            return []
        body: Dict[str, Any] = {
            "includedTypes": [str(t) for t in included_types][:5],
            "maxResultCount": min(_MAX_RESULTS, max(1, int(max_count))),
            "locationRestriction": {
                "circle": {
                    "center": {"latitude": float(lat), "longitude": float(lon)},
                    "radius": max(1.0, float(radius_m)),
                },
            },
        }
        headers = {
            "Content-Type": "application/json",
            "X-Goog-Api-Key": self.api_key,
            "X-Goog-FieldMask": field_mask,
        }
        try:
            with httpx.Client(timeout=self.timeout_seconds) as cli:
                resp = cli.post(_GOOGLE_PLACES_URL, json=body, headers=headers)
            if resp.status_code != 200:
                logger.warning(
                    "google_places: searchNearby HTTP %s (%s)",
                    resp.status_code,
                    resp.text[:200] if resp.text else "",
                )
                return []
            data = resp.json()
        except Exception as exc:  # noqa: BLE001
            logger.warning("google_places: searchNearby failed: %s", exc)
            return []
        return list(data.get("places") or [])
