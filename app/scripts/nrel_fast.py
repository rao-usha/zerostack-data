"""Fast NREL solar resource collection for all US counties."""
import asyncio
import csv
import io
import time
from datetime import datetime

import httpx
from sqlalchemy import text

from app.core.database import get_db

NREL_URL = "https://developer.nrel.gov/api/solar/solar_resource/v1.json"
CENT_URL = "https://www2.census.gov/geo/docs/reference/cenpop2020/county/CenPop2020_Mean_CO.txt"
import os
API_KEY = os.environ.get("NREL_API_KEY", "DEMO_KEY")

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

SQL = """INSERT INTO renewable_resource
    (resource_type, latitude, longitude, state, county,
     ghi_kwh_m2_day, dni_kwh_m2_day, capacity_factor_pct,
     source, collected_at)
    VALUES ('solar', :lat, :lng, :state, :county,
            :ghi, :dni, :cf, 'nrel', :now)
    ON CONFLICT (latitude, longitude) DO UPDATE SET
    ghi_kwh_m2_day=EXCLUDED.ghi_kwh_m2_day,
    dni_kwh_m2_day=EXCLUDED.dni_kwh_m2_day,
    capacity_factor_pct=EXCLUDED.capacity_factor_pct,
    collected_at=EXCLUDED.collected_at"""


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


async def query_nrel(client, sem, lat, lng):
    """Query NREL Solar Resource API for a single point."""
    async with sem:
        await asyncio.sleep(0.25)  # ~4 req/sec per slot, ~24 total with sem=6
        try:
            r = await client.get(
                NREL_URL,
                params={"api_key": API_KEY, "lat": str(lat), "lon": str(lng)},
            )
            if r.status_code == 429:
                retry = int(r.headers.get("Retry-After", 60))
                log(f"  429 rate limited, sleeping {retry}s...")
                await asyncio.sleep(retry)
                r = await client.get(
                    NREL_URL,
                    params={"api_key": API_KEY, "lat": str(lat), "lon": str(lng)},
                )
            r.raise_for_status()
            data = r.json()
            outputs = data.get("outputs", {})
            avg_ghi = outputs.get("avg_ghi", {})
            avg_dni = outputs.get("avg_dni", {})
            ghi = avg_ghi.get("annual") if isinstance(avg_ghi, dict) else None
            dni = avg_dni.get("annual") if isinstance(avg_dni, dict) else None
            cf = round(ghi / 24 / 1000 * 0.2 * 100, 2) if ghi else None
            return {"ghi": round(ghi, 2) if ghi else None,
                    "dni": round(dni, 2) if dni else None,
                    "cf": cf}
        except Exception as e:
            return None


async def main():
    db = next(get_db())

    # Get already-collected lat/lng pairs
    done = set()
    try:
        rows = db.execute(text(
            "SELECT latitude, longitude FROM renewable_resource"
        )).fetchall()
        for r in rows:
            done.add((float(r[0]), float(r[1])))
    except Exception:
        pass
    log(f"Already have {len(done)} points")

    # Load Census centroids
    async with httpx.AsyncClient(timeout=60.0) as dl:
        resp = await dl.get(CENT_URL)
        raw = resp.text.lstrip("\ufeff")

    centroids = []
    for row in csv.DictReader(io.StringIO(raw)):
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
        if not st:
            continue
        if (lat, lng) in done:
            continue
        centroids.append({
            "state": st, "county": row.get("COUNAME", "").strip(),
            "fips": fips, "lat": lat, "lng": lng,
        })

    log(f"Todo: {len(centroids)} counties")
    if not centroids:
        log("All counties already collected!")
        return

    sem = asyncio.Semaphore(6)
    start = time.time()

    async with httpx.AsyncClient(timeout=30.0) as client:
        for i in range(0, len(centroids), 20):
            chunk = centroids[i:i + 20]
            results = await asyncio.gather(
                *[query_nrel(client, sem, c["lat"], c["lng"]) for c in chunk]
            )
            now = datetime.utcnow()
            inserted = 0
            for j, res in enumerate(results):
                if res is not None:
                    c = chunk[j]
                    db.execute(text(SQL), {
                        "lat": c["lat"], "lng": c["lng"],
                        "state": c["state"], "county": c["county"],
                        "ghi": res["ghi"], "dni": res["dni"],
                        "cf": res["cf"], "now": now,
                    })
                    inserted += 1
            db.commit()
            total = db.execute(text("SELECT COUNT(*) FROM renewable_resource")).scalar()
            elapsed = int(time.time() - start)
            log(f"{i + len(chunk)}/{len(centroids)} | +{inserted} | DB: {total} | {elapsed}s")

    total = db.execute(text("SELECT COUNT(*) FROM renewable_resource")).scalar()
    states = db.execute(text("SELECT COUNT(DISTINCT state) FROM renewable_resource")).scalar()
    log(f"COMPLETE: {total} rows, {states} states in {int(time.time() - start)}s")
    db.close()


asyncio.run(main())
