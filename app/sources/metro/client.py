"""
MetroDataClient — fetches development characteristics data for US metros.

Four async data sources, all federal/no-auth (except ACS and BLS which use
existing API keys):

  1. Census BPS Metro  — annual building permits by unit type (MSA level)
  2. FHFA HPI          — house price index filtered to MSA geography
  3. Census ACS        — population, income, housing units, cost burden
  4. BLS LAUS Metro    — unemployment rate for metro areas
"""

import asyncio
import csv
import io
import logging
import os
import random
from typing import Any, Dict, List, Optional, Tuple

import httpx

logger = logging.getLogger(__name__)

# BLS LAUS area code → CBSA code mapping for top metros.
# BLS uses its own 7-digit area codes; these map to Census CBSA codes.
# Format: bls_area_code -> cbsa_code
BLS_LAUS_METRO_MAP: Dict[str, str] = {
    "0019820": "19820",  # Detroit-Warren-Dearborn, MI
    "0035620": "35620",  # New York-Newark-Jersey City
    "0031080": "31080",  # Los Angeles-Long Beach-Anaheim
    "0016980": "16980",  # Chicago-Naperville-Elgin
    "0019100": "19100",  # Dallas-Fort Worth-Arlington
    "0026420": "26420",  # Houston-The Woodlands-Sugar Land
    "0047900": "47900",  # Washington-Arlington-Alexandria
    "0033100": "33100",  # Miami-Fort Lauderdale-Pompano Beach
    "0037980": "37980",  # Philadelphia-Camden-Wilmington
    "0012060": "12060",  # Atlanta-Sandy Springs-Alpharetta
    "0038060": "38060",  # Phoenix-Mesa-Chandler
    "0014460": "14460",  # Boston-Cambridge-Newton
    "0041860": "41860",  # San Francisco-Oakland-Berkeley
    "0040900": "40900",  # Riverside-San Bernardino-Ontario
    "0042660": "42660",  # Seattle-Tacoma-Bellevue
    "0033460": "33460",  # Minneapolis-St. Paul-Bloomington
    "0041740": "41740",  # San Diego-Chula Vista-Carlsbad
    "0045300": "45300",  # Tampa-St. Petersburg-Clearwater
    "0019740": "19740",  # Denver-Aurora-Lakewood
    "0041700": "41700",  # San Antonio-New Braunfels
    "0036740": "36740",  # Orlando-Kissimmee-Sanford
    "0012580": "12580",  # Baltimore-Columbia-Towson
    "0016740": "16740",  # Charlotte-Concord-Gastonia
    "0041180": "41180",  # St. Louis
    "0012420": "12420",  # Austin-Round Rock-Georgetown
    "0038300": "38300",  # Pittsburgh
    "0017140": "17140",  # Cincinnati
    "0029820": "29820",  # Las Vegas-Henderson-Paradise
    "0018140": "18140",  # Columbus, OH
    "0026900": "26900",  # Indianapolis-Carmel-Anderson
    "0028140": "28140",  # Kansas City
    "0034980": "34980",  # Nashville
    "0017460": "17460",  # Cleveland-Elyria
    "0041940": "41940",  # San Jose-Sunnyvale-Santa Clara
    "0038900": "38900",  # Portland-Vancouver-Hillsboro
    "0039580": "39580",  # Raleigh-Cary
    "0047220": "47220",  # Virginia Beach-Norfolk-Newport News
    "0032820": "32820",  # Memphis
    "0039300": "39300",  # Providence-Warwick
    "0031140": "31140",  # Louisville/Jefferson County
    "0026180": "26180",  # Urban Honolulu
    "0015380": "15380",  # Buffalo-Cheektowaga
    "0040060": "40060",  # Richmond
    "0036500": "36500",  # Omaha-Council Bluffs
    "0041420": "41420",  # Salt Lake City
    "0025540": "25540",  # Hartford-East Hartford-Middletown
    "0010580": "10580",  # Albany-Schenectady-Troy
    "0045060": "45060",  # Syracuse
    "0024660": "24660",  # Greenville-Anderson
    "0046060": "46060",  # Tucson
    "0014260": "14260",  # Boise City
    "0028940": "28940",  # Knoxville
    "0010740": "10740",  # Albuquerque
    "0013820": "13820",  # Birmingham-Hoover
    "0019780": "19780",  # Des Moines-West Des Moines
    "0036260": "36260",  # Oklahoma City
    "0020500": "20500",  # Durham-Chapel Hill
    "0046140": "46140",  # Tulsa
    "0039340": "39340",  # Provo-Orem
    "0024340": "24340",  # Grand Rapids-Kentwood
    "0023420": "23420",  # Fresno
    "0033220": "33220",  # Milwaukee-Waukesha
    "0025420": "25420",  # Harrisburg-Carlisle
    "0031700": "31700",  # Manchester-Nashua
    "0040380": "40380",  # Rochester, NY
    "0035380": "35380",  # New Orleans-Metairie
    "0029260": "29260",  # Jacksonville, FL
    "0045780": "45780",  # Toledo
    "0032580": "32580",  # McAllen-Edinburg-Mission
    "0030780": "30780",  # Little Rock
    "0042540": "42540",  # Scranton--Wilkes-Barre
    "0044060": "44060",  # Spokane-Spokane Valley
    "0031540": "31540",  # Madison, WI
    "0020940": "20940",  # El Paso
    "0039900": "39900",  # Reno
    "0043580": "43580",  # Sioux Falls
    "0022380": "22380",  # Fayetteville-Springdale-Rogers
    "0017820": "17820",  # Colorado Springs
    "0012940": "12940",  # Baton Rouge
    "0035840": "35840",  # North Port-Sarasota-Bradenton
    "0031180": "31180",  # Lubbock
    "0048620": "48620",  # Worcester
    "0045220": "45220",  # Tallahassee
    "0044180": "44180",  # Springfield, MO
    "0026620": "26620",  # Huntsville
    "0011700": "11700",  # Asheville
    "0041060": "41060",  # Salem, OR
    "0029540": "29540",  # Lansing-East Lansing
    "0024580": "24580",  # Greensboro-High Point
    "0048060": "48060",  # Wichita
    "0030460": "30460",  # Lexington-Fayette
    "0029420": "29420",  # Lakeland-Winter Haven
    "0010420": "10420",  # Akron
    "0042340": "42340",  # Savannah
    "0038940": "38940",  # Port St. Lucie
    "0037340": "37340",  # Palm Bay-Melbourne-Titusville
    "0010900": "10900",  # Allentown-Bethlehem-Easton
    "0011460": "11460",  # Ann Arbor
    "0017980": "17980",  # Columbia, SC
    "0013460": "13460",  # Bend, OR
    "0019460": "19460",  # Deltona-Daytona Beach-Ormond Beach
    "0014540": "14540",  # Boulder
    "0021660": "21660",  # Eugene-Springfield
    "0022140": "22140",  # Fargo
    "0034740": "34740",  # Myrtle Beach-Conway-North Myrtle Beach
    "0033700": "33700",  # Modesto
    "0019340": "19340",  # Dayton-Kettering
    "0017300": "17300",  # Clarksville
    "0022900": "22900",  # Fort Collins
    "0016860": "16860",  # Chattanooga
    "0023540": "23540",  # Gainesville, FL
    "0016060": "16060",  # Cape Coral-Fort Myers
    "0030700": "30700",  # Lincoln, NE
    "0029460": "29460",  # Lancaster, PA
    "0021780": "21780",  # Evansville
    "0018580": "18580",  # Corpus Christi
    "0046340": "46340",  # Tyler
    "0022220": "22220",  # Fayetteville, NC
    "0048260": "48260",  # Wilmington, NC
    "0043300": "43300",  # Shreveport-Bossier City
    "0035300": "35300",  # New Haven-Milford
    "0014860": "14860",  # Bridgeport-Stamford-Norwalk
    "0045940": "45940",  # Trenton-Princeton
    "0046700": "46700",  # Vallejo
    "0016940": "16940",  # Chico
    "0033660": "33660",  # Mobile
    "0036100": "36100",  # Ocala
    "0027140": "27140",  # Jackson, MS
    "0013140": "13140",  # Beaumont-Port Arthur
    "0047380": "47380",  # Waco
    "0011260": "11260",  # Anchorage
    "0024500": "24500",  # Greeley
    "0017660": "17660",  # Coeur d'Alene
    "0045820": "45820",  # Topeka
    "0022500": "22500",  # Flint
    "0012100": "12100",  # Atlantic City-Hammonton
    "0012260": "12260",  # Augusta-Richmond County
    "0028700": "28700",  # Kingsport-Bristol
    "0047460": "47460",  # Warner Robins
    "0039660": "39660",  # Rapid City
    "0049020": "49020",  # York-Hanover
    "0049180": "49180",  # Youngstown-Warren-Boardman
    "0049340": "49340",  # Yuma
    "0044700": "44700",  # Stockton
    "0040420": "40420",  # Rockford
    "0046220": "46220",  # Tuscaloosa
    "0044300": "44300",  # State College
    "0026980": "26980",  # Iowa City
    "0028420": "28420",  # Kennewick-Richland
    "0020260": "20260",  # Duluth
    "0043780": "43780",  # South Bend-Mishawaka
    "0013380": "13380",  # Bellingham
    "0039380": "39380",  # Pueblo
    "0040220": "40220",  # Roanoke
    "0038860": "38860",  # Portland-South Portland, ME
}

# Reverse map: cbsa_code -> bls_area_code
CBSA_TO_BLS_MAP: Dict[str, str] = {v: k for k, v in BLS_LAUS_METRO_MAP.items()}


class MetroDataClient:
    """
    Async client for fetching metro development characteristics from federal sources.

    Sources:
      - Census BPS: https://www2.census.gov/econ/bps/Metro/ma{year}a.txt
      - FHFA HPI:   https://www.fhfa.gov/hpi/download/monthly/hpi_master.csv
      - Census ACS: https://api.census.gov/data/{year}/acs/acs5
      - BLS LAUS:   https://api.bls.gov/publicAPI/v2/timeseries/data/
    """

    FHFA_URL = "https://www.fhfa.gov/hpi/download/monthly/hpi_master.csv"
    # Census BPS: new CBSA-level format (Jan 2024+)
    BPS_URL = "https://www2.census.gov/econ/bps/CBSA%20(beginning%20Jan%202024)/cbsa{year}a.txt"
    ACS_URL = "https://api.census.gov/data/{year}/acs/acs5"
    BLS_URL = "https://api.bls.gov/publicAPI/v2/timeseries/data/"

    def __init__(self) -> None:
        self._census_key = os.getenv("CENSUS_SURVEY_API_KEY", "")
        self._bls_key = os.getenv("BLS_API_KEY", "")
        self._client: Optional[httpx.AsyncClient] = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(120.0, connect=15.0),
                follow_redirects=True,
            )
        return self._client

    async def close(self) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None

    # -------------------------------------------------------------------------
    # Census BPS — Metro annual building permits
    # -------------------------------------------------------------------------

    async def fetch_bps_metro(self, year: int) -> List[Dict[str, Any]]:
        """
        Fetch Census BPS CBSA-level annual building permit data (2024+ format).

        URL: https://www2.census.gov/econ/bps/CBSA%20(beginning%20Jan%202024)/cbsa{year}a.txt

        CSV columns (29 total):
          [0]  date code (YYYYMM, 99 = annual)
          [1]  CSA code
          [2]  CBSA code (5-digit)
          [3]  header type
          [4]  metro name
          [5-7]   1-unit  (bldgs, units, value)
          [8-10]  2-units (bldgs, units, value)
          [11-13] 3-4 units (bldgs, units, value)
          [14-16] 5+ units  (bldgs, units, value)
          [17-28] reported versions (duplicates, ignored)
        """
        url = self.BPS_URL.format(year=year)
        client = await self._get_client()

        for attempt in range(3):
            try:
                logger.info(f"Fetching Census BPS metro data for {year} (attempt {attempt+1})")
                resp = await client.get(url)
                resp.raise_for_status()
                return self._parse_bps_metro(resp.text, year)
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 404:
                    logger.warning(f"BPS metro data not found for year {year}")
                    return []
                if attempt < 2:
                    await self._backoff(attempt)
                else:
                    raise
            except Exception as e:
                logger.error(f"BPS fetch error (year={year}): {e}")
                if attempt < 2:
                    await self._backoff(attempt)
                else:
                    raise
        return []

    def _parse_bps_metro(self, text: str, year: int) -> List[Dict[str, Any]]:
        """
        Parse Census BPS CBSA annual file (2024+ format, 29-column CSV).

        Header rows start with "Survey" or "Date" or are blank — skip them.
        Data rows start with a 6-digit date code (e.g. 202499 for 2024 annual).
        """
        def safe_int(val: str) -> Optional[int]:
            try:
                cleaned = str(val).strip().replace(",", "")
                return int(cleaned) if cleaned else None
            except (ValueError, TypeError):
                return None

        records = []
        reader = csv.reader(io.StringIO(text))

        for row in reader:
            if not row:
                continue
            first = str(row[0]).strip()
            # Skip header rows — data rows start with a numeric date code
            if not first.isdigit():
                continue
            if len(row) < 17:
                continue

            try:
                cbsa_code = str(row[2]).strip().zfill(5)
                name = str(row[4]).strip()

                units_1    = safe_int(row[6])
                units_2    = safe_int(row[9])
                units_3to4 = safe_int(row[12])
                units_5plus = safe_int(row[15])

                parts = [x for x in [units_1, units_2, units_3to4, units_5plus] if x is not None]
                total = sum(parts) if parts else None

                records.append({
                    "cbsa_code": cbsa_code,
                    "cbsa_name": name,
                    "bps_year": year,
                    "permits_1unit": units_1,
                    "permits_3to4": units_3to4,   # field name matches ingest.py expectation
                    "permits_5plus": units_5plus,
                    "permits_total": total,
                })
            except Exception as e:
                logger.debug(f"BPS row parse error: {e} — row={row}")
                continue

        logger.info(f"Parsed {len(records)} BPS metro records for {year}")
        return records

    # -------------------------------------------------------------------------
    # FHFA HPI — House Price Index at MSA level
    # -------------------------------------------------------------------------

    async def fetch_fhfa_msa(self) -> Dict[str, Dict[str, Any]]:
        """
        Fetch FHFA HPI master CSV and return per-MSA price dynamics.

        Returns dict keyed by cbsa_code (= FHFA place_id for MSA rows):
          {cbsa_code: {hpi_current, hpi_yoy_pct, hpi_5yr_pct, hpi_vintage}}
        """
        client = await self._get_client()

        for attempt in range(3):
            try:
                logger.info(f"Fetching FHFA HPI master CSV (attempt {attempt+1})")
                resp = await client.get(self.FHFA_URL)
                resp.raise_for_status()
                return self._parse_fhfa_msa(resp.text)
            except Exception as e:
                logger.error(f"FHFA fetch error: {e}")
                if attempt < 2:
                    await self._backoff(attempt)
                else:
                    raise
        return {}

    def _parse_fhfa_msa(self, csv_text: str) -> Dict[str, Dict[str, Any]]:
        """
        Parse FHFA master CSV, filter to MSA rows, compute YoY and 5yr % change.

        FHFA hpi_master.csv columns:
          hpi_type, hpi_flavor, frequency, level, place_name, place_id,
          yr, period, index_nsa, index_sa
        """
        # Group time series by place_id
        msa_series: Dict[str, List[Tuple[str, float]]] = {}

        reader = csv.DictReader(io.StringIO(csv_text))
        for row in reader:
            if row.get("level", "").strip() != "MSA":
                continue

            place_id = str(row.get("place_id", "")).strip().zfill(5)
            yr = row.get("yr", "").strip()
            period = row.get("period", "").strip()
            index_val = row.get("index_nsa", "").strip()

            if not (place_id and yr and period and index_val):
                continue

            try:
                idx = float(index_val)
                period_key = f"{yr}{int(period):02d}"
                if place_id not in msa_series:
                    msa_series[place_id] = []
                msa_series[place_id].append((period_key, idx))
            except (ValueError, TypeError):
                continue

        # Compute current, YoY, 5yr for each MSA
        result: Dict[str, Dict[str, Any]] = {}
        for place_id, series in msa_series.items():
            series.sort(key=lambda x: x[0])
            if not series:
                continue

            current_key, current_val = series[-1]

            # YoY: ~4 quarters back
            yoy_pct = None
            if len(series) >= 4:
                _, val_1yr = series[-5] if len(series) >= 5 else series[-4]
                if val_1yr and val_1yr != 0:
                    yoy_pct = round((current_val - val_1yr) / val_1yr * 100, 2)

            # 5yr: ~20 quarters back
            fiveyr_pct = None
            if len(series) >= 20:
                _, val_5yr = series[-21] if len(series) >= 21 else series[-20]
                if val_5yr and val_5yr != 0:
                    fiveyr_pct = round((current_val - val_5yr) / val_5yr * 100, 2)

            result[place_id] = {
                "hpi_current": round(current_val, 3),
                "hpi_yoy_pct": yoy_pct,
                "hpi_5yr_pct": fiveyr_pct,
                "hpi_vintage": current_key,
            }

        logger.info(f"Parsed FHFA HPI for {len(result)} MSAs")
        return result

    # -------------------------------------------------------------------------
    # Census ACS — Population, income, housing at CBSA level
    # -------------------------------------------------------------------------

    async def fetch_acs_cbsa(self, year: int = 2023) -> Dict[str, Dict[str, Any]]:
        """
        Fetch Census ACS 5-year estimates at CBSA level.

        Variables:
          B01003_001E — total population
          B19013_001E — median household income
          B25001_001E — total housing units
          B25070_010E — households with gross rent 50%+ of income (severe cost burden)

        Returns dict keyed by cbsa_code.
        """
        variables = "B01003_001E,B19013_001E,B25001_001E,B25070_010E"
        # Build URL manually — httpx would double-encode the + in the geo param
        base_url = self.ACS_URL.format(year=year)
        key_param = f"&key={self._census_key}" if self._census_key else ""
        url = (
            f"{base_url}?get=NAME,{variables}"
            f"&for=metropolitan+statistical+area/micropolitan+statistical+area:*"
            f"{key_param}"
        )

        client = await self._get_client()

        for attempt in range(3):
            try:
                logger.info(f"Fetching Census ACS {year} CBSA data (attempt {attempt+1})")
                resp = await client.get(url)
                resp.raise_for_status()
                return self._parse_acs_cbsa(resp.json())
            except Exception as e:
                logger.error(f"ACS CBSA fetch error: {e}")
                if attempt < 2:
                    await self._backoff(attempt)
                else:
                    raise
        return {}

    def _parse_acs_cbsa(self, data: List[List[str]]) -> Dict[str, Dict[str, Any]]:
        """
        Parse Census API array-of-arrays response.
        First row is headers; remaining rows are data.
        Last column is the CBSA GEOID (matches cbsa_code).
        """
        if not data or len(data) < 2:
            return {}

        headers = data[0]
        # Find column indices
        try:
            idx_name = headers.index("NAME")
            idx_pop = headers.index("B01003_001E")
            idx_inc = headers.index("B19013_001E")
            idx_units = headers.index("B25001_001E")
            idx_burden = headers.index("B25070_010E")
            idx_geo = len(headers) - 1  # CBSA GEOID is last column
        except ValueError as e:
            logger.error(f"ACS response missing expected column: {e}")
            return {}

        def safe_int(val: str) -> Optional[int]:
            try:
                v = int(val)
                return None if v < 0 else v
            except (ValueError, TypeError):
                return None

        result: Dict[str, Dict[str, Any]] = {}
        for row in data[1:]:
            if len(row) <= idx_geo:
                continue
            cbsa_code = str(row[idx_geo]).strip().zfill(5)
            population = safe_int(row[idx_pop])
            housing_units = safe_int(row[idx_units])
            rent_burden_households = safe_int(row[idx_burden])

            # Compute severe cost burden % (rent-burdened HHs / total housing units proxy)
            cost_burden_pct = None
            if rent_burden_households is not None and housing_units and housing_units > 0:
                cost_burden_pct = round(rent_burden_households / housing_units * 100, 2)

            result[cbsa_code] = {
                "population": population,
                "median_hh_income": safe_int(row[idx_inc]),
                "housing_units_total": housing_units,
                "cost_burden_severe_pct": cost_burden_pct,
                "cbsa_name_acs": str(row[idx_name]).strip(),
            }

        logger.info(f"Parsed Census ACS for {len(result)} CBSAs")
        return result

    # -------------------------------------------------------------------------
    # BLS LAUS — Metro area unemployment
    # -------------------------------------------------------------------------

    async def fetch_bls_laus_metro(
        self, cbsa_codes: List[str]
    ) -> Dict[str, Dict[str, Any]]:
        """
        Fetch BLS LAUS unemployment data for metro areas.

        BLS LAUS metro series format:
          LAUMT{7-digit-area-code}0000000000003  (unemployment rate)
          LAUMT{7-digit-area-code}0000000000006  (labor force)

        Batches 25 series per request (conservative; 50 allowed with key).
        Returns dict keyed by cbsa_code.
        """
        # Build list of (cbsa_code, rate_series, lf_series) for metros we have a mapping for
        requests_list = []
        for cbsa in cbsa_codes:
            bls_area = CBSA_TO_BLS_MAP.get(cbsa)
            if bls_area:
                rate_series = f"LAUMT{bls_area}0000000000003"
                lf_series = f"LAUMT{bls_area}0000000000006"
                requests_list.append((cbsa, rate_series, lf_series))

        if not requests_list:
            logger.info("No BLS LAUS metro mappings found for requested CBSAs")
            return {}

        # Batch into groups of 25 series (2 series per metro = 12 metros per batch)
        all_series = []
        series_to_cbsa: Dict[str, str] = {}
        for cbsa, rate_s, lf_s in requests_list:
            all_series.extend([rate_s, lf_s])
            series_to_cbsa[rate_s] = cbsa
            series_to_cbsa[lf_s] = cbsa

        batch_size = 24  # Conservative (12 metros per batch)
        results: Dict[str, Dict[str, Any]] = {}

        client = await self._get_client()
        import json

        for i in range(0, len(all_series), batch_size):
            batch = all_series[i : i + batch_size]
            payload: Dict[str, Any] = {
                "seriesid": batch,
                "startyear": "2023",
                "endyear": "2024",
            }
            if self._bls_key:
                payload["registrationkey"] = self._bls_key

            try:
                logger.info(f"BLS LAUS batch {i//batch_size + 1}: {len(batch)} series")
                resp = await client.post(
                    self.BLS_URL,
                    content=json.dumps(payload),
                    headers={"Content-Type": "application/json"},
                )
                resp.raise_for_status()
                data = resp.json()

                if data.get("status") != "REQUEST_SUCCEEDED":
                    logger.warning(f"BLS LAUS non-success status: {data.get('status')}")
                    continue

                for series in data.get("Results", {}).get("series", []):
                    sid = series.get("seriesID", "")
                    cbsa = series_to_cbsa.get(sid)
                    if not cbsa:
                        continue
                    if cbsa not in results:
                        results[cbsa] = {}

                    # Get most recent value
                    data_points = series.get("data", [])
                    if not data_points:
                        continue
                    # BLS data is ordered newest first
                    latest = data_points[0]
                    try:
                        val = float(latest.get("value", ""))
                    except (ValueError, TypeError):
                        continue

                    if sid.endswith("0000000000003"):  # unemployment rate
                        results[cbsa]["unemployment_rate"] = round(val, 2)
                    elif sid.endswith("0000000000006"):  # labor force
                        results[cbsa]["labor_force_size"] = int(val)

                await asyncio.sleep(0.5)  # BLS rate limit courtesy

            except Exception as e:
                logger.error(f"BLS LAUS batch error: {e}")
                await self._backoff(0)
                continue

        logger.info(f"Fetched BLS LAUS for {len(results)} metros")
        return results

    # -------------------------------------------------------------------------
    # Helpers
    # -------------------------------------------------------------------------

    async def _backoff(self, attempt: int) -> None:
        delay = min(2.0 * (2.0 ** attempt), 30.0)
        jitter = delay * 0.2 * (2 * random.random() - 1)
        await asyncio.sleep(max(0.5, delay + jitter))
