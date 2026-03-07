"""Fast concurrent elevation collection for remaining states."""
import asyncio
import csv
import io
import statistics
import time
from datetime import datetime

import httpx
from sqlalchemy import text

from app.core.database import get_db

EPQS = "https://epqs.nationalmap.gov/v1/json"
CENT = "https://www2.census.gov/geo/docs/reference/cenpop2020/county/CenPop2020_Mean_CO.txt"
LOG = "/tmp/elev_run.log"
LAT_OFF, LNG_OFF = 0.072, 0.090

SM = {
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

SQL = """INSERT INTO county_elevation
    (state, county, fips_code, min_elevation_ft, max_elevation_ft,
     mean_elevation_ft, elevation_range_ft, sample_points, source, collected_at)
    VALUES (:s, :c, :f, :mn, :mx, :me, :rg, :pt, 'usgs_3dep', :now)
    ON CONFLICT (fips_code) DO UPDATE SET
    min_elevation_ft=EXCLUDED.min_elevation_ft, max_elevation_ft=EXCLUDED.max_elevation_ft,
    mean_elevation_ft=EXCLUDED.mean_elevation_ft, elevation_range_ft=EXCLUDED.elevation_range_ft,
    sample_points=EXCLUDED.sample_points, collected_at=EXCLUDED.collected_at"""


def log(msg):
    line = f"[{time.strftime('%H:%M:%S')}] {msg}"
    print(line, flush=True)
    with open(LOG, "a") as f:
        f.write(line + "\n")


async def query_elev(client, sem, lat, lng):
    async with sem:
        await asyncio.sleep(0.05)
        try:
            r = await client.get(
                EPQS,
                params={"x": str(lng), "y": str(lat), "wkid": "4326", "units": "Feet"},
            )
            r.raise_for_status()
            v = r.json().get("value")
            return float(v) if v is not None else None
        except Exception:
            return None


async def collect_county(client, sem, c):
    pts = [
        (c["la"], c["lo"]),
        (c["la"] + LAT_OFF, c["lo"]),
        (c["la"] - LAT_OFF, c["lo"]),
        (c["la"], c["lo"] + LNG_OFF),
        (c["la"], c["lo"] - LNG_OFF),
    ]
    results = await asyncio.gather(*[query_elev(client, sem, p[0], p[1]) for p in pts])
    es = [e for e in results if e is not None]
    if not es:
        return None
    return {
        "s": c["s"], "c": c["c"], "f": c["f"],
        "mn": round(min(es), 2), "mx": round(max(es), 2),
        "me": round(statistics.mean(es), 2), "rg": round(max(es) - min(es), 2),
        "pt": len(es), "now": datetime.utcnow(),
    }


async def main():
    db = next(get_db())
    done = set(
        r[0]
        for r in db.execute(text("SELECT DISTINCT state FROM county_elevation")).fetchall()
    )
    log(f"Done ({len(done)}): {sorted(done)}")

    async with httpx.AsyncClient(timeout=60.0) as dl:
        resp = await dl.get(CENT)
        t = resp.text.lstrip("\ufeff")

    centroids = []
    for row in csv.DictReader(io.StringIO(t)):
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
        st = SM.get(row.get("STNAME", "").strip(), "")
        if not st or st in done:
            continue
        centroids.append(
            {"s": st, "c": row.get("COUNAME", "").strip(), "f": fips, "la": lat, "lo": lng}
        )

    n_states = len(set(c["s"] for c in centroids))
    log(f"Todo: {len(centroids)} counties across {n_states} states")

    sem = asyncio.Semaphore(8)
    start = time.time()

    async with httpx.AsyncClient(timeout=30.0) as client:
        for i in range(0, len(centroids), 20):
            chunk = centroids[i : i + 20]
            results = await asyncio.gather(
                *[collect_county(client, sem, c) for c in chunk]
            )
            records = [r for r in results if r is not None]
            for rec in records:
                db.execute(text(SQL), rec)
            db.commit()
            total = db.execute(
                text("SELECT COUNT(*) FROM county_elevation")
            ).scalar()
            elapsed = int(time.time() - start)
            log(f"{i + len(chunk)}/{len(centroids)} counties | DB: {total} | {elapsed}s")

    total = db.execute(text("SELECT COUNT(*) FROM county_elevation")).scalar()
    states = db.execute(
        text("SELECT COUNT(DISTINCT state) FROM county_elevation")
    ).scalar()
    log(f"COMPLETE: {total} rows, {states} states in {int(time.time() - start)}s")
    db.close()


asyncio.run(main())
