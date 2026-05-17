"""
Build reference data JSON files for SPEC_060.

Outputs:
    data/reference/naics_2022.json
    data/reference/msa.json
    data/reference/naics_sic_crosswalk.json

Run inside the API container (has pandas, openpyxl, httpx):

    docker exec -e PGPASSWORD=Nex2026 nexdata-api-1 python /app/scripts/build_reference_data.py
"""

from __future__ import annotations

import io
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import httpx
import pandas as pd

OUT_DIR = Path(os.environ.get("REF_OUT_DIR", "/tmp/reference"))
OUT_DIR.mkdir(parents=True, exist_ok=True)

UA = "NexdataResearch/1.0 (research@nexdata.com; respectful research bot)"
HEADERS = {"User-Agent": UA, "Accept": "*/*"}


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def fetch_first_ok(urls: list[str], timeout: float = 60.0) -> tuple[str, bytes, list[str]]:
    """Try URLs in order; return (winning_url, content, failed_urls)."""
    failed: list[str] = []
    for url in urls:
        for attempt in range(3):
            try:
                with httpx.Client(headers=HEADERS, follow_redirects=True, timeout=timeout) as c:
                    r = c.get(url)
                if r.status_code == 200 and r.content:
                    return url, r.content, failed
                print(f"  [{r.status_code}] attempt {attempt+1} {url}", flush=True)
            except Exception as e:
                print(f"  [ERR] attempt {attempt+1} {url}: {e}", flush=True)
            time.sleep(1.5 * (attempt + 1))
        failed.append(url)
    raise RuntimeError(f"All URLs failed. Failed list: {failed}")


# ---------------------------------------------------------------------------
# File 1: NAICS 2022
# ---------------------------------------------------------------------------
def build_naics_2022() -> dict:
    print("\n=== Building naics_2022.json ===", flush=True)
    t0 = time.time()
    urls = [
        "https://www.census.gov/naics/2022NAICS/2-6%20digit_2022_Codes.xlsx",
        "https://www2.census.gov/programs-surveys/economic-census/reference/2022-naics/2-6%20digit_2022_Codes.xlsx",
        "https://www.census.gov/naics/2022NAICS/2022_NAICS_Structure.xlsx",
    ]
    src, blob, failed = fetch_first_ok(urls)
    print(f"  Fetched {len(blob)} bytes from {src}", flush=True)

    df = pd.read_excel(io.BytesIO(blob), dtype=str, header=None)
    # Identify the code + title columns. Census file usually has a header row.
    # Find the header row by scanning rows for 'Code' / '2022 NAICS Code' patterns.
    header_row = None
    for i in range(min(10, len(df))):
        row_vals = [str(v).strip().lower() for v in df.iloc[i].tolist() if pd.notna(v)]
        joined = " | ".join(row_vals)
        if "code" in joined and ("title" in joined or "naics" in joined):
            header_row = i
            break

    if header_row is None:
        # fallback: assume row 1
        header_row = 1

    df = pd.read_excel(io.BytesIO(blob), dtype=str, header=header_row)
    df.columns = [str(c).strip() for c in df.columns]
    # find code col + title col
    code_col = None
    title_col = None
    for c in df.columns:
        cl = c.lower()
        if code_col is None and "code" in cl:
            code_col = c
        if title_col is None and ("title" in cl or "naics title" in cl):
            title_col = c
    if code_col is None or title_col is None:
        # try by position
        code_col = df.columns[1] if len(df.columns) > 2 else df.columns[0]
        title_col = df.columns[2] if len(df.columns) > 2 else df.columns[1]
    print(f"  Using code col='{code_col}', title col='{title_col}'", flush=True)

    data: dict[str, dict] = {}
    for _, row in df.iterrows():
        raw_code = row[code_col]
        raw_title = row[title_col]
        if pd.isna(raw_code) or pd.isna(raw_title):
            continue
        code = str(raw_code).strip()
        title = str(raw_title).strip()
        # Sector codes like "31-33", "44-45", "48-49" — expand to each
        if "-" in code and len(code) <= 6:
            try:
                lo, hi = code.split("-")
                lo_i = int(lo)
                hi_i = int(hi)
                for c in range(lo_i, hi_i + 1):
                    cs = f"{c:02d}"
                    data[cs] = {"code": cs, "label": title, "parent_code": None, "digits": 2}
            except Exception:
                pass
            continue
        # strip trailing 'T' or other non-digits
        code_digits = "".join(ch for ch in code if ch.isdigit())
        if not code_digits or len(code_digits) < 2 or len(code_digits) > 6:
            continue
        digits = len(code_digits)
        parent = code_digits[:-1] if digits > 2 else None
        # parent of 3-digit is its 2-digit; that 2-digit might be a sector range entry already
        data[code_digits] = {
            "code": code_digits,
            "label": title,
            "parent_code": parent,
            "digits": digits,
        }

    # Fix parent_code where parent doesn't exist as a stand-alone key (sector ranges)
    # For 3-digit codes 31x/32x/33x, parent is 31/32/33 which we created above. Good.
    # But ensure parents at 2-digit exist; if not, set parent_code to None.
    for k, v in list(data.items()):
        if v["parent_code"] and v["parent_code"] not in data:
            v["parent_code"] = None

    out = {
        "_meta": {
            "source": src,
            "fetched_at": now_iso(),
            "row_count": len(data),
        },
        "data": data,
    }
    path = OUT_DIR / "naics_2022.json"
    path.write_text(json.dumps(out, indent=2), encoding="utf-8")
    elapsed = time.time() - t0
    print(f"  wrote {path} ({path.stat().st_size} bytes, {len(data)} rows) in {elapsed:.1f}s", flush=True)
    return {"path": str(path), "bytes": path.stat().st_size, "rows": len(data),
            "elapsed": elapsed, "failed": failed, "source": src}


# ---------------------------------------------------------------------------
# File 2: MSA delineation
# ---------------------------------------------------------------------------
def build_msa() -> dict:
    print("\n=== Building msa.json ===", flush=True)
    t0 = time.time()
    urls = [
        "https://www2.census.gov/programs-surveys/metro-micro/geographies/reference-files/2023/delineation-files/list1_2023.xlsx",
        "https://www2.census.gov/programs-surveys/metro-micro/geographies/reference-files/2020/delineation-files/list1_2020.xls",
    ]
    src, blob, failed = fetch_first_ok(urls)
    print(f"  Fetched {len(blob)} bytes from {src}", flush=True)

    # CBSA list1 has a couple of preamble rows. Read once without header to locate.
    engine = "openpyxl" if src.endswith(".xlsx") else "xlrd"
    try:
        scout = pd.read_excel(io.BytesIO(blob), dtype=str, header=None, engine=engine)
    except Exception as e:
        # try the other engine
        engine = "xlrd" if engine == "openpyxl" else "openpyxl"
        scout = pd.read_excel(io.BytesIO(blob), dtype=str, header=None, engine=engine)

    header_row = None
    for i in range(min(10, len(scout))):
        row_vals = [str(v).strip() for v in scout.iloc[i].tolist() if pd.notna(v)]
        joined = " | ".join(row_vals).lower()
        if "cbsa code" in joined or ("cbsa" in joined and "title" in joined):
            header_row = i
            break
    if header_row is None:
        header_row = 2
    print(f"  header row = {header_row}", flush=True)

    df = pd.read_excel(io.BytesIO(blob), dtype=str, header=header_row, engine=engine)
    df.columns = [str(c).strip() for c in df.columns]
    print(f"  columns: {list(df.columns)[:12]}", flush=True)

    # Identify the columns we need
    def find_col(candidates):
        for cand in candidates:
            for c in df.columns:
                if cand.lower() == c.lower().strip():
                    return c
        for cand in candidates:
            for c in df.columns:
                if cand.lower() in c.lower():
                    return c
        return None

    col_cbsa = find_col(["CBSA Code"])
    col_title = find_col(["CBSA Title"])
    col_type = find_col(["Metropolitan/Micropolitan Statistical Area",
                         "Metropolitan/Micropolitan", "LSAD"])
    col_state_abbr = find_col(["State Name"])  # we'll derive from FIPS instead
    col_state_fips = find_col(["FIPS State Code", "State Code", "FIPS State"])
    col_county_fips = find_col(["FIPS County Code", "County Code", "FIPS County"])

    print(f"  cols cbsa={col_cbsa} title={col_title} type={col_type} stateFIPS={col_state_fips} countyFIPS={col_county_fips}", flush=True)

    # State FIPS -> abbr map (50 + DC + PR + territories)
    STATE_FIPS_TO_ABBR = {
        "01": "AL", "02": "AK", "04": "AZ", "05": "AR", "06": "CA", "08": "CO",
        "09": "CT", "10": "DE", "11": "DC", "12": "FL", "13": "GA", "15": "HI",
        "16": "ID", "17": "IL", "18": "IN", "19": "IA", "20": "KS", "21": "KY",
        "22": "LA", "23": "ME", "24": "MD", "25": "MA", "26": "MI", "27": "MN",
        "28": "MS", "29": "MO", "30": "MT", "31": "NE", "32": "NV", "33": "NH",
        "34": "NJ", "35": "NM", "36": "NY", "37": "NC", "38": "ND", "39": "OH",
        "40": "OK", "41": "OR", "42": "PA", "44": "RI", "45": "SC", "46": "SD",
        "47": "TN", "48": "TX", "49": "UT", "50": "VT", "51": "VA", "53": "WA",
        "54": "WV", "55": "WI", "56": "WY", "60": "AS", "66": "GU", "69": "MP",
        "72": "PR", "78": "VI",
    }

    msas: dict[str, dict] = {}
    skipped_micro = 0
    for _, row in df.iterrows():
        cbsa = row.get(col_cbsa) if col_cbsa else None
        if pd.isna(cbsa) or cbsa is None:
            continue
        cbsa = str(cbsa).strip()
        if not cbsa.isdigit():
            continue
        type_val = str(row.get(col_type, "")).strip().lower() if col_type else ""
        if "micropolitan" in type_val and "metropolitan" not in type_val:
            skipped_micro += 1
            continue
        # Some rows are "Metropolitan Statistical Area"; keep only those
        if "metropolitan" not in type_val:
            # if we have no type col, accept all and filter later
            if col_type is not None:
                continue
        title = str(row.get(col_title, "")).strip() if col_title else ""
        sfips_raw = row.get(col_state_fips) if col_state_fips else None
        cfips_raw = row.get(col_county_fips) if col_county_fips else None
        if pd.isna(sfips_raw) or pd.isna(cfips_raw):
            continue
        sfips = str(sfips_raw).strip().zfill(2)
        cfips = str(cfips_raw).strip().zfill(3)
        county_fips = sfips + cfips
        abbr = STATE_FIPS_TO_ABBR.get(sfips)

        if cbsa not in msas:
            msas[cbsa] = {
                "cbsa_code": cbsa,
                "title": title,
                "state_abbrs": [],
                "county_fips_list": [],
            }
        if abbr and abbr not in msas[cbsa]["state_abbrs"]:
            msas[cbsa]["state_abbrs"].append(abbr)
        if county_fips not in msas[cbsa]["county_fips_list"]:
            msas[cbsa]["county_fips_list"].append(county_fips)

    # Sort lists deterministically
    for v in msas.values():
        v["state_abbrs"].sort()
        v["county_fips_list"].sort()

    print(f"  skipped {skipped_micro} micropolitan rows", flush=True)
    out = {
        "_meta": {
            "source": src,
            "fetched_at": now_iso(),
            "row_count": len(msas),
        },
        "data": msas,
    }
    path = OUT_DIR / "msa.json"
    path.write_text(json.dumps(out, indent=2), encoding="utf-8")
    elapsed = time.time() - t0
    print(f"  wrote {path} ({path.stat().st_size} bytes, {len(msas)} rows) in {elapsed:.1f}s", flush=True)
    return {"path": str(path), "bytes": path.stat().st_size, "rows": len(msas),
            "elapsed": elapsed, "failed": failed, "source": src}


# ---------------------------------------------------------------------------
# File 3: NAICS -> SIC crosswalk filtered to SEC-present SICs
# ---------------------------------------------------------------------------
def fetch_sec_sics() -> list[str]:
    """Get distinct SICs. Prefer SEC_SIC_FILE if set, else query cloud Postgres."""
    sic_file = os.environ.get("SEC_SIC_FILE")
    if sic_file and Path(sic_file).exists():
        lines = Path(sic_file).read_text(encoding="utf-8").splitlines()
        return [ln.strip() for ln in lines if ln.strip()]
    import sqlalchemy as sa
    pw = os.environ.get("PGPASSWORD", "Nex2026")
    host = os.environ.get("PGHOST", "host.docker.internal")
    port = os.environ.get("PGPORT", "5435")
    url = f"postgresql+psycopg2://nexdata:{pw}@{host}:{port}/nexdata"
    engine = sa.create_engine(url, future=True)
    with engine.connect() as conn:
        rows = conn.execute(sa.text(
            "SELECT DISTINCT sic_code FROM sec_company_metadata "
            "WHERE sic_code IS NOT NULL ORDER BY sic_code"
        )).fetchall()
    return [str(r[0]).strip() for r in rows if r[0] is not None]


def build_crosswalk() -> dict:
    print("\n=== Building naics_sic_crosswalk.json ===", flush=True)
    t0 = time.time()
    urls = [
        "https://www.census.gov/naics/concordances/2002_to_1987_NAICS.xls",
        "https://www.census.gov/naics/concordances/1987_SIC_to_2002_NAICS.xls",
    ]
    src, blob, failed = fetch_first_ok(urls)
    print(f"  Fetched {len(blob)} bytes from {src}", flush=True)

    # Try both engines (these are old .xls files)
    df = None
    for engine in ("xlrd", "openpyxl"):
        try:
            df = pd.read_excel(io.BytesIO(blob), dtype=str, header=None, engine=engine)
            print(f"  Parsed with engine={engine}, shape={df.shape}", flush=True)
            break
        except Exception as e:
            print(f"  engine={engine} failed: {e}", flush=True)
    if df is None:
        raise RuntimeError("Could not parse crosswalk Excel")

    # Find header row
    header_row = None
    for i in range(min(10, len(df))):
        row_vals = [str(v).strip().lower() for v in df.iloc[i].tolist() if pd.notna(v)]
        joined = " | ".join(row_vals)
        if "naics" in joined and "sic" in joined:
            header_row = i
            break
    if header_row is None:
        header_row = 1

    df = pd.read_excel(io.BytesIO(blob), dtype=str, header=header_row,
                       engine="xlrd" if src.endswith(".xls") else "openpyxl")
    df.columns = [str(c).strip() for c in df.columns]
    print(f"  columns: {list(df.columns)}", flush=True)

    # Identify NAICS & SIC columns
    naics_col = None
    sic_col = None
    for c in df.columns:
        cl = c.lower()
        if "naics" in cl and naics_col is None:
            naics_col = c
        if "sic" in cl and sic_col is None:
            sic_col = c
    print(f"  naics_col={naics_col}, sic_col={sic_col}", flush=True)

    # Get distinct SICs from cloud DB
    sec_sics = fetch_sec_sics()
    sec_sic_set = set(sec_sics)
    print(f"  Found {len(sec_sics)} distinct SICs in sec_company_metadata", flush=True)

    # Build prefix index of SEC SICs: by full 4-digit, 3-digit, 2-digit.
    # Many SEC sic_codes are stubs at the 2-digit "Major Group" level (e.g. "1000",
    # "1040", "2300" — codes ending in "00" or with trailing zeros). The Census
    # crosswalk uses concrete 4-digit SICs, so we match a crosswalk SIC `s` to a
    # SEC sic `p` if `p` is a prefix of `s` after trimming p's trailing zeros to
    # >=2 chars (Major Group), or `p == s` exactly.
    def matching_sec_sics(crosswalk_sic: str) -> list[str]:
        """Return SEC sic codes that 'cover' this 4-digit crosswalk SIC."""
        matches: set[str] = set()
        if crosswalk_sic in sec_sic_set:
            matches.add(crosswalk_sic)
        # Try Major Group prefix (2-digit padded with zeros, e.g. "10" -> "1000")
        for n_significant in (3, 2):
            prefix = crosswalk_sic[:n_significant]
            stub = prefix + "0" * (4 - n_significant)
            if stub in sec_sic_set:
                matches.add(stub)
        return sorted(matches)

    crosswalk: dict[str, set[str]] = {}
    for _, row in df.iterrows():
        n_raw = row.get(naics_col)
        s_raw = row.get(sic_col)
        if pd.isna(n_raw) or pd.isna(s_raw):
            continue
        n = "".join(ch for ch in str(n_raw) if ch.isdigit())
        s = "".join(ch for ch in str(s_raw) if ch.isdigit())
        if not n or not s:
            continue
        n4 = n[:4]
        if len(n4) != 4 or len(s) < 2:
            continue
        # Normalize s to 4 digits (some xls cells may drop leading zeros)
        s = s.zfill(4)[:4]
        sec_matches = matching_sec_sics(s)
        if not sec_matches:
            continue
        for sm in sec_matches:
            crosswalk.setdefault(n4, set()).add(sm)

    final = {k: sorted(v) for k, v in sorted(crosswalk.items())}
    out = {
        "_meta": {
            "source": f"Census crosswalk ({src}) filtered to SEC-present SICs",
            "fetched_at": now_iso(),
            "row_count": len(final),
            "sec_distinct_sic_count": len(sec_sics),
        },
        "data": final,
    }
    path = OUT_DIR / "naics_sic_crosswalk.json"
    path.write_text(json.dumps(out, indent=2), encoding="utf-8")
    elapsed = time.time() - t0
    print(f"  wrote {path} ({path.stat().st_size} bytes, {len(final)} naics-4 entries) in {elapsed:.1f}s", flush=True)
    return {"path": str(path), "bytes": path.stat().st_size, "rows": len(final),
            "elapsed": elapsed, "failed": failed, "source": src,
            "sec_sic_count": len(sec_sics)}


# ---------------------------------------------------------------------------
if __name__ == "__main__":
    results = {}
    try:
        results["naics_2022"] = build_naics_2022()
    except Exception as e:
        print(f"NAICS build FAILED: {e}", flush=True)
        import traceback; traceback.print_exc()
    try:
        results["msa"] = build_msa()
    except Exception as e:
        print(f"MSA build FAILED: {e}", flush=True)
        import traceback; traceback.print_exc()
    try:
        results["crosswalk"] = build_crosswalk()
    except Exception as e:
        print(f"Crosswalk build FAILED: {e}", flush=True)
        import traceback; traceback.print_exc()

    print("\n\n========== SUMMARY ==========", flush=True)
    print(json.dumps(results, indent=2, default=str), flush=True)
