"""
SPEC_077 A.2 — bulk-ingest sub-county TIGER boundaries into geojson_boundaries.

Reuses the existing GeoJSONFetcher (TIGERweb REST API client) for the
actual fetch + pagination + retries. This module just orchestrates
across geo levels and persists into the existing geojson_boundaries
table, which already supports geo_level as a column.

Two callable entry points:
    ingest_tracts(db)       — loops per state, ~74k features total
    ingest_zctas(db)        — single national call, ~33k features

Idempotent: DELETEs then INSERTs per geo_level (cheaper than per-row
UPSERTs at this volume, and we never want stale partial state).
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.sources.census.geojson import GeoJSONFetcher

logger = logging.getLogger(__name__)


# Real US state FIPS codes (no IRS-style aggregations).
US_STATE_FIPS = [
    "01","02","04","05","06","08","09","10","11","12","13","15","16","17","18","19",
    "20","21","22","23","24","25","26","27","28","29","30","31","32","33","34","35",
    "36","37","38","39","40","41","42","44","45","46","47","48","49","50","51","53",
    "54","55","56",
    # Territories — Census tracts exist for these, ZCTAs limited
    "60","66","69","72","78",
]

DATASET_ID = "tigerweb_2020"


def _extract_tract_attrs(feature: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Pull geo_id + geo_name from a TIGERweb tract feature.
    geo_id = 11-digit GEOID (state + county + tract)."""
    props = feature.get("properties") or feature.get("attributes") or {}
    geoid = props.get("GEOID") or props.get("GEOID20") or props.get("GEOID10")
    if not geoid or len(str(geoid)) != 11:
        return None
    name = props.get("NAME") or props.get("NAMELSAD") or geoid
    return {"geo_id": str(geoid), "geo_name": str(name)[:255]}


def _extract_zcta_attrs(feature: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Pull geo_id + geo_name from a TIGERweb ZCTA feature.
    geo_id = 5-digit ZCTA5CE."""
    props = feature.get("properties") or feature.get("attributes") or {}
    zcta = props.get("ZCTA5") or props.get("ZCTA5CE20") or props.get("ZCTA5CE10")
    if not zcta or len(str(zcta)) != 5:
        return None
    name = f"ZCTA {zcta}"
    return {"geo_id": str(zcta), "geo_name": name}


def _bbox_from_geometry(geom: Dict[str, Any]) -> Optional[Dict[str, str]]:
    """Compute axis-aligned bounding box from a Polygon/MultiPolygon."""
    if not geom or geom.get("type") not in ("Polygon", "MultiPolygon"):
        return None
    if geom["type"] == "Polygon":
        rings = geom["coordinates"]
    else:
        rings = [r for poly in geom["coordinates"] for r in poly]
    xs, ys = [], []
    for ring in rings:
        for pt in ring:
            xs.append(pt[0]); ys.append(pt[1])
    if not xs:
        return None
    return {
        "bbox_minx": f"{min(xs):.6f}", "bbox_miny": f"{min(ys):.6f}",
        "bbox_maxx": f"{max(xs):.6f}", "bbox_maxy": f"{max(ys):.6f}",
    }


async def _ingest_features(
    db: Session,
    geo_level: str,
    features: List[Dict[str, Any]],
    extract_attrs,
) -> int:
    """Bulk INSERT features. Returns count inserted."""
    if not features:
        return 0

    insert_sql = text("""
        INSERT INTO geojson_boundaries
            (dataset_id, geo_level, geo_id, geo_name, geojson,
             bbox_minx, bbox_miny, bbox_maxx, bbox_maxy, created_at)
        VALUES
            (:dataset_id, :geo_level, :geo_id, :geo_name, CAST(:geojson AS JSON),
             :bbox_minx, :bbox_miny, :bbox_maxx, :bbox_maxy, NOW())
    """)

    batch: List[Dict[str, Any]] = []
    inserted = 0
    import json
    for feat in features:
        attrs = extract_attrs(feat)
        if not attrs:
            continue
        geom = feat.get("geometry")
        if not geom:
            continue
        bbox = _bbox_from_geometry(geom) or {
            "bbox_minx": None, "bbox_miny": None,
            "bbox_maxx": None, "bbox_maxy": None,
        }
        # Wrap as a Feature (matches existing schema convention)
        feature_wrap = {
            "type": "Feature",
            "properties": {"geo_id": attrs["geo_id"], "geo_name": attrs["geo_name"]},
            "geometry": geom,
        }
        batch.append({
            "dataset_id": DATASET_ID,
            "geo_level": geo_level,
            "geo_id": attrs["geo_id"],
            "geo_name": attrs["geo_name"],
            "geojson": json.dumps(feature_wrap),
            **bbox,
        })
        if len(batch) >= 500:
            db.execute(insert_sql, batch)
            db.commit()
            inserted += len(batch)
            batch = []
    if batch:
        db.execute(insert_sql, batch)
        db.commit()
        inserted += len(batch)
    return inserted


async def ingest_tracts(db: Session, state_fips_codes: Optional[List[str]] = None) -> Dict[str, Any]:
    """Ingest census tracts for all (or the given) US states."""
    started = datetime.utcnow()
    state_fips_codes = state_fips_codes or US_STATE_FIPS

    # Clear existing tract rows so re-runs don't double-insert.
    db.execute(text("DELETE FROM geojson_boundaries WHERE geo_level = 'tract'"))
    db.commit()

    fetcher = GeoJSONFetcher(year=2020)
    total_features = 0
    per_state: Dict[str, int] = {}
    for st in state_fips_codes:
        try:
            logger.info("Fetching tracts for state %s", st)
            gj = await fetcher.fetch_geojson(geo_level="tract", state_fips=st)
            feats = gj.get("features", [])
            n = await _ingest_features(db, "tract", feats, _extract_tract_attrs)
            per_state[st] = n
            total_features += n
            logger.info("State %s: inserted %d tract rows", st, n)
        except Exception as exc:  # noqa: BLE001
            logger.exception("State %s tract fetch failed: %s", st, exc)
            per_state[st] = -1

    final = db.execute(text(
        "SELECT COUNT(*) FROM geojson_boundaries WHERE geo_level = 'tract'"
    )).scalar()
    return {
        "geo_level": "tract",
        "rows_inserted": total_features,
        "rows_in_table": int(final or 0),
        "per_state": per_state,
        "duration_seconds": (datetime.utcnow() - started).total_seconds(),
    }


async def ingest_zctas(db: Session, page_size: int = 200) -> Dict[str, Any]:
    """Ingest all US ZCTAs in paginated fetches.

    TIGERweb's WAF blocks ZCTA queries with the default 1000-feature
    page + outFields=* (returns HTML 200 reject due to response size).
    Workaround: page_size=200 + narrow outFields. ~165 pages × 5s each
    = ~15min wall clock.
    """
    started = datetime.utcnow()

    db.execute(text("DELETE FROM geojson_boundaries WHERE geo_level = 'zcta'"))
    db.commit()

    import httpx
    base_url = (
        "https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb/"
        "tigerWMS_Current/MapServer/2/query"
    )
    common_params = {
        "where": "1=1",
        "outFields": "ZCTA5,GEOID",
        "f": "geojson",
        "resultRecordCount": str(page_size),
    }

    total_inserted = 0
    offset = 0
    consecutive_failures = 0
    async with httpx.AsyncClient(timeout=60.0) as cli:
        while True:
            params = {**common_params, "resultOffset": str(offset)}
            try:
                resp = await cli.get(base_url, params=params)
                resp.raise_for_status()
                # Detect WAF reject (HTML body despite 200)
                ct = resp.headers.get("content-type", "")
                if "html" in ct.lower() or resp.text.lstrip().startswith("<"):
                    raise ValueError("WAF reject (HTML body)")
                page = resp.json()
            except Exception as exc:  # noqa: BLE001
                consecutive_failures += 1
                logger.warning("ZCTA page offset=%d failed: %s (failure %d/3)",
                               offset, exc, consecutive_failures)
                if consecutive_failures >= 3:
                    logger.error("ZCTA fetch giving up after 3 consecutive failures; "
                                  "partial data preserved (%d rows)", total_inserted)
                    break
                await asyncio.sleep(2 ** consecutive_failures)
                continue
            consecutive_failures = 0
            feats = page.get("features", [])
            # Insert this page immediately so a crash later doesn't lose it.
            if feats:
                inserted = await _ingest_features(db, "zcta", feats, _extract_zcta_attrs)
                total_inserted += inserted
                if total_inserted % 2000 < page_size:
                    logger.info("ZCTA progress: %d rows inserted (offset %d)",
                                total_inserted, offset)
            if len(feats) < page_size:
                break
            offset += page_size
            await asyncio.sleep(0.4)   # polite throttle

    n = total_inserted

    final = db.execute(text(
        "SELECT COUNT(*) FROM geojson_boundaries WHERE geo_level = 'zcta'"
    )).scalar()
    return {
        "geo_level": "zcta",
        "page_size": page_size,
        "fetched_features": len(all_features),
        "rows_inserted": n,
        "rows_in_table": int(final or 0),
        "duration_seconds": (datetime.utcnow() - started).total_seconds(),
    }
