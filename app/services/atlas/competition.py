"""SPEC_091 — Atlas Decision Map: real competition data via Yelp Fusion.

Given a focal lat/lon + radius + thesis term (e.g. "Furniture stores"),
return the competing businesses within radius. Used by the Trade Area
panel to paint orange competition pins on top of the radius circle, and
by the Pilot `find_competition` tool.

Yelp Fusion caps `radius` at 40 km (~25 mi); we cap accordingly. The
service soft-fails (returns an empty list + error string) when
`YELP_API_KEY` is unset or Yelp errors, so the rest of trade-area mode
keeps working.
"""
from __future__ import annotations

import asyncio
import logging
import os
from typing import Any, Dict, List, Optional

from app.services.atlas.trade_area import haversine_miles

logger = logging.getLogger(__name__)

_YELP_MAX_RADIUS_MI = 25.0
_METERS_PER_MILE = 1609.34


def _miles_to_meters(mi: float) -> int:
    return int(round(mi * _METERS_PER_MILE))


def _cap_radius(radius_mi: Optional[float]) -> float:
    try:
        v = float(radius_mi or 5.0)
    except (TypeError, ValueError):
        v = 5.0
    return max(0.5, min(_YELP_MAX_RADIUS_MI, v))


async def _do_search(
    lat: float, lon: float, radius_mi: float,
    term: Optional[str], categories: Optional[str], limit: int,
) -> Dict[str, Any]:
    """Single Yelp search call. Soft-fails when the key is missing."""
    if not os.getenv("YELP_API_KEY"):
        return {
            "count": 0, "total": 0, "businesses": [],
            "error": "YELP_API_KEY not set",
            "term_used": term, "radius_mi": radius_mi,
        }
    # Lazy import — keeps the service importable without Yelp deps
    from app.sources.yelp.client import YelpClient
    async with YelpClient(api_key=os.getenv("YELP_API_KEY")) as yc:
        res = await yc.search_businesses(
            latitude=lat, longitude=lon,
            term=term, categories=categories,
            radius=_miles_to_meters(radius_mi),
            limit=min(50, max(1, int(limit or 20))),
        )
    raw: List[Dict[str, Any]] = res.get("businesses", []) or []
    businesses: List[Dict[str, Any]] = []
    for b in raw:
        coord = (b.get("coordinates") or {})
        bla = coord.get("latitude")
        blo = coord.get("longitude")
        if bla is None or blo is None:
            continue
        loc = b.get("location") or {}
        addr = ", ".join(loc.get("display_address") or [])
        businesses.append({
            "id":           b.get("id"),
            "name":         b.get("name"),
            "lat":          bla, "lon": blo,
            "rating":       b.get("rating"),
            "review_count": b.get("review_count"),
            "url":          b.get("url"),
            "address":      addr,
            "distance_mi":  round(haversine_miles(lat, lon, bla, blo), 2),
        })
    businesses.sort(key=lambda x: x["distance_mi"])
    return {
        "count":      len(businesses),
        "total":      res.get("total", len(businesses)),
        "businesses": businesses,
        "term_used":  term,
        "radius_mi":  radius_mi,
    }


def find_competition(
    lat: float, lon: float,
    radius_mi: Optional[float] = 5.0,
    term: Optional[str] = None,
    categories: Optional[str] = None,
    limit: int = 20,
) -> Dict[str, Any]:
    """Synchronous wrapper for the trade-area / API caller. Returns the
    same response shape on success and on soft-fail."""
    rm = _cap_radius(radius_mi)
    try:
        return asyncio.run(_do_search(
            float(lat), float(lon), rm, term, categories, limit))
    except Exception as exc:  # noqa: BLE001
        logger.warning("find_competition failed: %s", exc)
        return {
            "count": 0, "total": 0, "businesses": [],
            "error": str(exc),
            "term_used": term, "radius_mi": rm,
        }
