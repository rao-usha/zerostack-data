"""
Fast USGS 3DEP Elevation Collector — concurrent queries for remaining states.

Uses asyncio.gather to query all 5 points per county concurrently, and
processes 3 counties concurrently via semaphore. ~5x faster than sequential.

Usage (from host):
  MSYS_NO_PATHCONV=1 docker exec -d nexdata-api-1 python /app/scripts/collect_elevation.py
  MSYS_NO_PATHCONV=1 docker exec nexdata-api-1 cat /tmp/elev_progress.log
"""

import asyncio
import csv
import io
import os
import statistics
import time
import traceback
from datetime import datetime

import httpx
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

EPQS_URL = "https://epqs.nationalmap.gov/v1/json"
CENTROIDS_URL = (
    "https://www2.census.gov/geo/docs/reference/cenpop2020/county/"
    "CenPop2020_Mean_CO.txt"
)
LAT_OFFSET = 0.072
LNG_OFFSET = 0.090
CONCURRENCY = 3  # max concurrent county queries

STATE_MAP = {
    "Alabama": "AL", "Alaska": "AK", "Arizona": "AZ", "Arkansas": "AR",
    "California": "CA", "Colorado": "CO", "Connecticut": "CT", "Delaware": "DE",
    "District of Columbia": "DC", "Florida": "FL", "Georgia": "GA", "Hawaii": "HI",
    "Idaho": "ID", "Illinois": "IL", "Indiana": "IN", "Iowa": "IA",
    "Kansas": "KS", "Kentucky": "KY", "Louisiana": "LA", "Maine": "ME",
    "Maryland": "MD", "Massachusetts": "MA", "Michigan": "MI", "Minnesota": "MN",
    "Mississippi": "MS", "Missouri": "MO", "Montana": "MT", "Nebraska": "NE",
    "Nevada": "NV", "New Hampshire": "NH", "New Jersey": "NJ", "New Mexico": "NM",
    "New York": "NY", "North Carolina": "NC", "North Dakota": "ND", "Ohio": "OH",
    "Oklahoma": "OK", "Oregon": "OR", "Pennsylvania": "PA", "Rhode Island": "RI",
    "South Carolina": "SC", "South Dakota": "SD", "Tennessee": "TN", "Texas": "TX",
    "Utah": "UT", "Vermont": "VT", "Virginia": "VA", "Washington": "WA",
    "West Virginia": "WV", "Wisconsin": "WI", "Wyoming": "WY",
}

LOG_PATH = "/tmp/elev_progress.log"


def log(msg):
    with open(LOG_PATH, "a") as f:
        f.write(f"[{time.strftime('%H:%M:%S')}] {msg}\n")


async def get_elev(client, lat, lng):
    """Query EPQS with retry."""
    for attempt in range(3):
        try:
            r = await client.get(
                EPQS_URL,
                params={"x": str(lng), "y": str(lat), "wkid": "4326", "units": "Feet"},
            )
            if r.status_code != 200:
                await asyncio.sleep(0.5)
                continue
            j = r.json()
            v = j.get("value")
            return float(v) if v is not None else None
        except Exception:
            if attempt < 2:
                await asyncio.sleep(0.5 * (attempt + 1))
    return None


async def collect_county(client, sem, c):
    """Collect 5 elevation points for one county concurrently."""
    async with sem:
        pts = [
            (c["la"], c["lo"]),
            (c["la"] + LAT_OFFSET, c["lo"]),
            (c["la"] - LAT_OFFSET, c["lo"]),
            (c["la"], c["lo"] + LNG_OFFSET),
            (c["la"], c["lo"] - LNG_OFFSET),
        ]
        # Query all 5 points concurrently
        results = await asyncio.gather(
            *[get_elev(client, lat, lng) for lat, lng in pts]
        )
        elevations = [e for e in results if e is not None]

        if elevations:
            return {
                "s": c["s"], "c": c["c"], "f": c["f"],
                "mn": round(min(elevations), 2),
                "mx": round(max(elevations), 2),
                "me": round(statistics.mean(elevations), 2),
                "rg": round(max(elevations) - min(elevations), 2),
                "pt": len(elevations),
            }
        return None


async def main():
    with open(LOG_PATH, "w") as f:
        f.write("")

    log("Starting FAST elevation collection")

    try:
        engine = create_engine(os.environ["DATABASE_URL"])
        Session = sessionmaker(bind=engine)
        db = Session()

        done = set(
            r[0]
            for r in db.execute(
                text("SELECT DISTINCT state FROM county_elevation")
            ).fetchall()
        )
        log(f"Already done ({len(done)}): {sorted(done)}")

        # Download centroids
        log("Downloading county centroids...")
        async with httpx.AsyncClient(timeout=60.0) as dl:
            resp = await dl.get(CENTROIDS_URL)
            csv_text = resp.text.lstrip("\ufeff")
        log("Centroids downloaded")

        centroids = []
        for row in csv.DictReader(io.StringIO(csv_text)):
            fips = row.get("STATEFP", "").strip() + row.get("COUNTYFP", "").strip()
            if len(fips) != 5:
                continue
            try:
                lat = float(row.get("LATITUDE", "0").strip().lstrip("+"))
                lng = float(row.get("LONGITUDE", "0").strip().lstrip("+"))
            except ValueError:
                continue
            if lat == 0 or lng == 0:
                continue
            st = STATE_MAP.get(row.get("STNAME", "").strip(), "")
            if not st or st in done:
                continue
            centroids.append({
                "s": st,
                "c": row.get("COUNAME", "").strip(),
                "f": fips,
                "la": lat,
                "lo": lng,
            })

        by_state = {}
        for c in centroids:
            by_state.setdefault(c["s"], []).append(c)

        log(f"Todo: {len(centroids)} counties across {len(by_state)} states")
        start = time.time()
        grand_total = 0

        sem = asyncio.Semaphore(CONCURRENCY)

        async with httpx.AsyncClient(
            timeout=30.0,
            limits=httpx.Limits(max_connections=10, max_keepalive_connections=5),
        ) as client:
            for st in sorted(by_state.keys()):
                counties = by_state[st]
                st_start = time.time()

                try:
                    # Process all counties in state concurrently (bounded by semaphore)
                    tasks = [collect_county(client, sem, c) for c in counties]
                    results = await asyncio.gather(*tasks, return_exceptions=True)

                    inserted = 0
                    for r in results:
                        if isinstance(r, Exception):
                            continue
                        if r is None:
                            continue
                        try:
                            db.execute(
                                text("""
                                    INSERT INTO county_elevation
                                    (state, county, fips_code, min_elevation_ft,
                                     max_elevation_ft, mean_elevation_ft,
                                     elevation_range_ft, sample_points, source, collected_at)
                                    VALUES (:s, :c, :f, :mn, :mx, :me, :rg, :pt, 'usgs_3dep', :now)
                                    ON CONFLICT (fips_code) DO UPDATE SET
                                        min_elevation_ft = EXCLUDED.min_elevation_ft,
                                        max_elevation_ft = EXCLUDED.max_elevation_ft,
                                        mean_elevation_ft = EXCLUDED.mean_elevation_ft,
                                        elevation_range_ft = EXCLUDED.elevation_range_ft,
                                        sample_points = EXCLUDED.sample_points,
                                        collected_at = EXCLUDED.collected_at
                                """),
                                {**r, "now": datetime.utcnow()},
                            )
                            inserted += 1
                        except Exception as ex:
                            log(f"  DB error {r['f']}: {ex}")
                            db.rollback()

                    db.commit()
                except Exception as ex:
                    log(f"{st}: ERROR - {ex}")
                    db.rollback()
                    inserted = 0

                grand_total += inserted
                elapsed = int(time.time() - st_start)
                total_elapsed = int(time.time() - start)

                try:
                    db_total = db.execute(
                        text("SELECT COUNT(*) FROM county_elevation")
                    ).scalar()
                except Exception:
                    db_total = "?"

                log(
                    f"{st}: +{inserted}/{len(counties)} in {elapsed}s "
                    f"(DB: {db_total}, total: {total_elapsed}s)"
                )

        db.close()
        total_elapsed = int(time.time() - start)
        log(f"COMPLETE: +{grand_total} new counties in {total_elapsed}s")

    except Exception as ex:
        log(f"FATAL: {ex}")
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())
