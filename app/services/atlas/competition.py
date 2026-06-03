"""SPEC_100 — Atlas Decision Map: CBP-backed competition lookup.

Replaces the SPEC_091 Yelp Fusion path. Given a focal county (FIPS) +
neighbour counties (from trade_area) + a thesis-derived NAICS prefix,
aggregate establishment counts from `census_cbp_county_yearly` and
return a per-county breakdown sorted by establishments descending.

Yelp gave us:
  - opt-in directory pins (lat/lon)
  - ratings + review counts
  - business names

CBP gives us:
  - federal establishment census counts (more authoritative than Yelp)
  - county-grain only (no individual pins)
  - no ratings / reviews

For the Decision Map's "is this a competitive trade area" question, the
CBP count is the stronger signal. Ratings are explicitly out of scope
for this spec (carved out as SPEC_101).

The module keeps `find_competition(lat, lon, ..., term=...)` as a thin
backward-compat adaptor so existing callers (Pilot tool, API) keep
working through the swap. It resolves lat/lon → focal_geo_id via the
nearest county centroid, resolves term → NAICS via industry_naics, and
delegates to `find_competition_cbp`.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.services.atlas.industry_naics import (
    industry_to_naics, naics_label_short,
)
from app.services.atlas.trade_area import haversine_miles

logger = logging.getLogger(__name__)

# CBP's latest stable vintage.
_DEFAULT_YEAR = 2022


def find_competition_cbp(
    db: Session,
    focal_geo_id: str,
    neighbor_geo_ids: Optional[List[str]] = None,
    naics: Optional[str] = None,
    year: int = _DEFAULT_YEAR,
) -> Dict[str, Any]:
    """SPEC_100 — aggregate CBP establishment counts at NAICS across
    the focal county + its trade-area neighbours.

    Args:
        focal_geo_id: 5-digit county FIPS.
        neighbor_geo_ids: list of 5-digit FIPS within the trade area.
        naics: NAICS code or `None`. When `None`, falls back to the
               all-sectors total (`'00'`) — same number the existing
               Establishments line on the card shows, so a thesis with
               no industry mapping still gets a useful answer.
        year: CBP vintage. Default 2022.

    Returns:
        {
          "count": int,                # focal + neighbours
          "focal_count": int,
          "neighbours_count": int,
          "per_county": [
              {"geo_id", "name", "establishments", "distance_mi",
               "is_focal"},
              ...
          ],                            # sorted by establishments desc
          "naics_used": str,            # the code actually queried
          "naics_label": str,
          "year": int,
          "error": Optional[str],
        }
    """
    focal = str(focal_geo_id or "").strip()
    if not focal or len(focal) != 5 or not focal.isdigit():
        return _empty_response(naics, year,
                                error=f"invalid focal_geo_id {focal_geo_id!r}")

    all_geo_ids = [focal] + [g for g in (neighbor_geo_ids or [])
                              if g and g != focal]
    # De-dupe while preserving order
    seen = set()
    ordered: List[str] = []
    for g in all_geo_ids:
        if g not in seen:
            seen.add(g)
            ordered.append(g)

    code = (naics or "00").strip()

    # SQL — exact match on the NAICS code, NOT a LIKE prefix. The
    # backfill stores all NAICS levels separately; aggregating across
    # levels would double-count (e.g. 442 + 4421 + 442110). Caller
    # picked the right level via industry_to_naics.
    rows = db.execute(text("""
        SELECT c.geo_id, c.establishments
        FROM census_cbp_county_yearly c
        WHERE c.geo_id = ANY(:geo_ids)
          AND c.year = :year
          AND c.naics_code = :naics
    """), {
        "geo_ids": ordered,
        "year": year,
        "naics": code,
    }).all()

    # Build the per-county breakdown. Names + centroids come from the
    # county_centroids cache so we can compute distance_mi from focal.
    from app.services.atlas.trade_area import county_centroids
    centroids = county_centroids(db)
    focal_centroid = centroids.get(focal)

    by_geo = {r.geo_id: int(r.establishments or 0) for r in rows}
    name_lookup: Dict[str, str] = {}

    per_county: List[Dict[str, Any]] = []
    for g in ordered:
        n = by_geo.get(g, 0)
        cent = centroids.get(g)
        if cent and focal_centroid:
            f_lat, f_lon, _ = focal_centroid
            g_lat, g_lon, g_name = cent
            distance_mi = round(
                haversine_miles(f_lat, f_lon, g_lat, g_lon), 2)
            name = g_name
        else:
            distance_mi = 0.0 if g == focal else None
            name = name_lookup.get(g, g)
        per_county.append({
            "geo_id": g,
            "name": name,
            "establishments": n,
            "distance_mi": distance_mi,
            "is_focal": g == focal,
        })

    # Sort by establishments desc; focal stays at index 0 if tied at top.
    per_county.sort(
        key=lambda r: (not r["is_focal"], -(r["establishments"] or 0)))

    focal_count = next((r["establishments"] for r in per_county
                        if r["is_focal"]), 0)
    neighbours_count = sum(r["establishments"] for r in per_county
                           if not r["is_focal"])
    total = focal_count + neighbours_count

    return {
        "count": total,
        "focal_count": focal_count,
        "neighbours_count": neighbours_count,
        "per_county": per_county,
        "naics_used": code,
        "naics_label": naics_label_short(code) if code != "00"
                       else "All establishments",
        "year": year,
        "error": None,
    }


def _empty_response(naics: Optional[str], year: int,
                     error: Optional[str] = None) -> Dict[str, Any]:
    code = (naics or "00").strip()
    return {
        "count": 0,
        "focal_count": 0,
        "neighbours_count": 0,
        "per_county": [],
        "naics_used": code,
        "naics_label": naics_label_short(code) if code != "00"
                       else "All establishments",
        "year": year,
        "error": error,
    }


# ─── Backward-compat adaptor ───────────────────────────────────────────
# The SPEC_091 signature was
#   find_competition(lat, lon, radius_mi=, term=, categories=, limit=)
# Existing callers (the Pilot tool, the /atlas/competition endpoint)
# still pass that shape. We resolve lat/lon → focal_geo_id via the
# nearest county centroid, resolve term → NAICS, and delegate.


def _nearest_county_geo_id(db: Session, lat: float, lon: float
                             ) -> Optional[str]:
    """Return the FIPS of the county whose centroid is closest to
    (lat, lon). None if no centroids are loaded."""
    from app.services.atlas.trade_area import county_centroids
    centroids = county_centroids(db)
    if not centroids:
        return None
    best_id, best_d = None, float("inf")
    for gid, (clat, clon, _name) in centroids.items():
        d = haversine_miles(lat, lon, clat, clon)
        if d < best_d:
            best_d, best_id = d, gid
    return best_id


def _neighbor_geo_ids_for(db: Session, focal_geo_id: str,
                            radius_mi: float) -> List[str]:
    """Quick neighbour lookup — same haversine sweep that trade_area
    does, but returns just the FIPS list. Kept here so the legacy
    adaptor doesn't have to construct a full trade_area payload."""
    from app.services.atlas.trade_area import county_centroids
    centroids = county_centroids(db)
    focal = centroids.get(focal_geo_id)
    if not focal:
        return []
    f_lat, f_lon, _ = focal
    out: List[str] = []
    for gid, (lat, lon, _name) in centroids.items():
        if gid == focal_geo_id:
            continue
        if haversine_miles(f_lat, f_lon, lat, lon) <= radius_mi:
            out.append(gid)
    return out


def find_competition(
    lat: float, lon: float,
    radius_mi: Optional[float] = 5.0,
    term: Optional[str] = None,
    categories: Optional[str] = None,   # kept for shape; ignored
    limit: int = 20,                    # kept for shape; ignored
    db: Optional[Session] = None,
    naics_hint: Optional[str] = None,
) -> Dict[str, Any]:
    """SPEC_091 legacy signature → SPEC_100 CBP delegation.

    Resolves the lat/lon → nearest county FIPS, the `term` → NAICS via
    the industry_naics resolver, and calls `find_competition_cbp`.
    Returns the new CBP shape — callers updated alongside this swap.

    `categories` and `limit` are kept in the signature for backwards
    compatibility but are ignored (CBP gives counts, not businesses).
    """
    rm = float(radius_mi or 5.0)
    if db is None:
        # The legacy API endpoint passes its own Session; if not, we
        # need to open one ourselves so this stays callable from the
        # Pilot tool with the request-scoped session.
        from app.core.database import get_session_factory
        Session = get_session_factory()
        db = Session()
        _own_session = True
    else:
        _own_session = False
    try:
        focal_geo_id = _nearest_county_geo_id(db, float(lat), float(lon))
        if not focal_geo_id:
            return _empty_response(
                naics_hint, _DEFAULT_YEAR,
                error="no county centroids loaded")
        neighbours = _neighbor_geo_ids_for(db, focal_geo_id, rm)
        naics = industry_to_naics(term, naics_hint=naics_hint)
        result = find_competition_cbp(
            db, focal_geo_id=focal_geo_id,
            neighbor_geo_ids=neighbours,
            naics=naics, year=_DEFAULT_YEAR,
        )
        # Add the radius so the caller can echo it back to the UI
        result["radius_mi"] = rm
        result["term_used"] = term
        return result
    except Exception as exc:  # noqa: BLE001
        logger.warning("find_competition (CBP) failed: %s", exc)
        out = _empty_response(naics_hint, _DEFAULT_YEAR,
                               error="CBP lookup unavailable")
        out["radius_mi"] = rm
        out["term_used"] = term
        return out
    finally:
        if _own_session:
            db.close()
