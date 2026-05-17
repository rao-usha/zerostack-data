"""
Census CBP coverage audit — SPEC_060 of PLAN_065.

Answers the gate question for PLAN_065 SPEC_064 (intake UX):

    "Is NAICS-4 × MSA feasible from either CBP table on cloud?"

There are two CBP tables on cloud:
  * census_business_patterns — state × NAICS × year. 63k rows on cloud.
  * census_cbp                — county/state/national × NAICS × year, with
                                richer columns (HHI, size distribution).
                                7k rows on cloud.

This script connects through the cloud-sql-proxy (must be running on
127.0.0.1:5435), inspects coverage of both tables, and writes a CSV with:

  1. A summary block at the top with the gate verdict (yes / no / partial)
  2. Per-NAICS-4 coverage rows (NAICS, table, populated states, populated counties)
  3. Per-MSA coverage rows (CBSA, MSA name, % of constituent counties covered)

Output: data/reference/cbp_coverage_<YYYY-MM-DD>.csv

Usage (from host):
    python scripts/audit_cbp_coverage.py
Or from inside any container that can reach host.docker.internal:5435:
    DATABASE_URL=postgresql://nexdata:Nex2026@host.docker.internal:5435/nexdata \
        python scripts/audit_cbp_coverage.py

The script is read-only; it makes no schema or data changes on either DB.
"""

from __future__ import annotations

import csv
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

# Make the repo root importable so we can use app.services.diligence.taxonomies
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from app.services.diligence.taxonomies import (  # noqa: E402
    REF_DIR,
    load_msa,
)

try:
    import psycopg2
    import psycopg2.extras
except ImportError:
    print("psycopg2 is required. Install with: pip install psycopg2-binary", file=sys.stderr)
    sys.exit(2)


CLOUD_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://nexdata:Nex2026@127.0.0.1:5435/nexdata",
)


def main() -> int:
    out_path = REF_DIR / f"cbp_coverage_{datetime.now(timezone.utc).strftime('%Y-%m-%d')}.csv"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"Connecting to {CLOUD_URL.split('@')[-1]} ...")
    try:
        conn = psycopg2.connect(CLOUD_URL, connect_timeout=10)
    except Exception as exc:  # noqa: BLE001
        print(f"WARN: connection failed: {exc}", file=sys.stderr)
        # Write a partial CSV with the failure note so the gate question still
        # surfaces in CI / reviewable artifacts.
        _write_partial(out_path, str(exc))
        return 0

    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cbp_rows, naics_state_grain = _audit_business_patterns(cur)
            ccbp_rows, naics_county_grain = _audit_census_cbp(cur)
            msa_coverage = _audit_msa_coverage(cur, load_msa())
    finally:
        conn.close()

    verdict = _make_verdict(naics_state_grain, naics_county_grain, msa_coverage)
    _write_csv(out_path, verdict, cbp_rows, ccbp_rows, msa_coverage)
    print(f"\nWrote: {out_path}")
    print(f"\n=== GATE VERDICT for PLAN_065 SPEC_064 intake UX ===")
    for line in verdict["summary_lines"]:
        print("  " + line)
    return 0


# ── Per-table audits ────────────────────────────────────────────────────────

def _audit_business_patterns(cur):
    """census_business_patterns is state × NAICS × year. Returns
    (per-NAICS-4 rows, dict of grain summary)."""
    cur.execute("""
        SELECT length(naics_code) AS digits, count(*) AS n,
               count(distinct state_fips) AS n_states,
               count(distinct year) AS n_years
        FROM census_business_patterns
        GROUP BY 1 ORDER BY 1
    """)
    grain = {r["digits"]: dict(r) for r in cur.fetchall()}

    # Per-NAICS-4 detail
    cur.execute("""
        SELECT naics_code,
               count(distinct state_fips) AS states_covered,
               count(distinct year) AS years_covered,
               sum(establishments) AS sum_estabs
        FROM census_business_patterns
        WHERE length(naics_code) = 4
        GROUP BY 1
        ORDER BY 1
    """)
    rows = [
        {
            "naics_code": r["naics_code"],
            "table": "census_business_patterns",
            "states_covered": r["states_covered"],
            "counties_covered": "n/a (state grain)",
            "years_covered": r["years_covered"],
            "sum_establishments": r["sum_estabs"] or 0,
        }
        for r in cur.fetchall()
    ]
    return rows, grain


def _audit_census_cbp(cur):
    """census_cbp may have national/state/county grain. Returns
    (per-NAICS-4 rows, grain summary)."""
    cur.execute("""
        SELECT geo_level, length(naics_code) AS digits, count(*) AS n,
               count(distinct county_fips) FILTER (WHERE county_fips IS NOT NULL) AS n_counties,
               count(distinct state_fips) FILTER (WHERE state_fips IS NOT NULL) AS n_states,
               count(distinct year) AS n_years
        FROM census_cbp
        GROUP BY 1, 2 ORDER BY 1, 2
    """)
    grain = [dict(r) for r in cur.fetchall()]

    cur.execute("""
        SELECT naics_code,
               count(distinct state_fips) FILTER (WHERE state_fips IS NOT NULL) AS states_covered,
               count(distinct county_fips) FILTER (WHERE county_fips IS NOT NULL) AS counties_covered,
               count(distinct year) AS years_covered,
               sum(establishments) AS sum_estabs
        FROM census_cbp
        WHERE length(naics_code) = 4
        GROUP BY 1
        ORDER BY 1
    """)
    rows = [
        {
            "naics_code": r["naics_code"],
            "table": "census_cbp",
            "states_covered": r["states_covered"],
            "counties_covered": r["counties_covered"],
            "years_covered": r["years_covered"],
            "sum_establishments": r["sum_estabs"] or 0,
        }
        for r in cur.fetchall()
    ]
    return rows, grain


def _audit_msa_coverage(cur, msa_dict):
    """For each MSA, what fraction of its constituent counties appear in
    census_cbp's county-grain rows? This is the MSA-rollup feasibility test."""
    cur.execute("""
        SELECT DISTINCT county_fips
        FROM census_cbp
        WHERE county_fips IS NOT NULL
    """)
    covered_counties = {r["county_fips"] for r in cur.fetchall()}

    out = []
    for cbsa, rec in msa_dict.items():
        total = len(rec.county_fips_list)
        if total == 0:
            continue
        covered = sum(1 for c in rec.county_fips_list if c in covered_counties)
        out.append({
            "cbsa_code": cbsa,
            "msa_title": rec.title,
            "total_counties": total,
            "covered_counties": covered,
            "coverage_pct": round(100.0 * covered / total, 1),
        })
    return out


# ── Verdict ─────────────────────────────────────────────────────────────────

def _make_verdict(business_grain, census_grain, msa_coverage):
    """Synthesize the gate verdict from the three audits."""
    # Does census_cbp have NAICS-4 county-grain rows at all?
    has_county_naics4 = any(
        g.get("geo_level") == "county" and g.get("digits") == 4 and g.get("n", 0) > 0
        for g in census_grain
    )

    # How many MSAs have ≥50% of their counties covered in census_cbp county grain?
    msas_50plus = sum(1 for r in msa_coverage if r["coverage_pct"] >= 50)
    msas_total = len(msa_coverage)
    msas_full = sum(1 for r in msa_coverage if r["coverage_pct"] >= 90)

    if has_county_naics4 and msas_50plus >= int(0.5 * msas_total):
        verdict_code = "YES"
        verdict_text = (
            "NAICS-4 × MSA is feasible from census_cbp. Ship MSA picker as planned."
        )
    elif has_county_naics4 and msas_50plus >= 50:
        verdict_code = "PARTIAL"
        verdict_text = (
            "NAICS-4 × MSA works for the top ~{n} MSAs; below that the data is "
            "sparse. Ship MSA picker but grey out / warn for under-covered MSAs."
        ).format(n=msas_50plus)
    else:
        verdict_code = "NO"
        verdict_text = (
            "NAICS-4 × MSA is NOT feasible. Fall back to NAICS × state for the "
            "structural-density section (SPEC_061 §3); intake (SPEC_064) becomes "
            "NAICS × state with optional county/MSA narrowing for other sections."
        )

    # Use ASCII-safe characters in stdout messages — Windows cp1252 chokes on
    # `≥` / `≤` etc. when the audit is run from the host shell. CSV keeps UTF-8.
    summary_lines = [
        f"verdict: {verdict_code}",
        verdict_text,
        f"census_business_patterns NAICS digit coverage: " +
            ", ".join(f"{d}d={info['n']}" for d, info in sorted(business_grain.items())),
        f"census_cbp grain breakdown: " +
            "; ".join(
                f"{g['geo_level']}/{g['digits']}d: n={g['n']}, "
                f"counties={g['n_counties']}, states={g['n_states']}"
                for g in census_grain
            ),
        f"MSA coverage in census_cbp county rows: "
            f"{msas_full}/{msas_total} MSAs >=90% covered, "
            f"{msas_50plus}/{msas_total} >=50%",
    ]
    return {
        "code": verdict_code,
        "text": verdict_text,
        "summary_lines": summary_lines,
    }


# ── Output ──────────────────────────────────────────────────────────────────

def _write_csv(out_path, verdict, cbp_rows, ccbp_rows, msa_coverage):
    with out_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["# SPEC_060 CBP coverage audit", datetime.now(timezone.utc).isoformat()])
        w.writerow(["# verdict", verdict["code"]])
        for line in verdict["summary_lines"]:
            w.writerow(["# " + line])
        w.writerow([])

        # Section 1: per-NAICS-4 detail from both tables
        w.writerow(["section", "naics_code", "table", "states_covered",
                    "counties_covered", "years_covered", "sum_establishments"])
        for r in cbp_rows + ccbp_rows:
            w.writerow(["naics_detail", r["naics_code"], r["table"],
                        r["states_covered"], r["counties_covered"],
                        r["years_covered"], r["sum_establishments"]])
        w.writerow([])

        # Section 2: per-MSA coverage
        w.writerow(["section", "cbsa_code", "msa_title", "total_counties",
                    "covered_counties", "coverage_pct"])
        for r in sorted(msa_coverage, key=lambda x: -x["coverage_pct"]):
            w.writerow(["msa_coverage", r["cbsa_code"], r["msa_title"],
                        r["total_counties"], r["covered_counties"],
                        r["coverage_pct"]])


def _write_partial(out_path, err):
    with out_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["# SPEC_060 CBP coverage audit — PARTIAL (DB unreachable)"])
        w.writerow(["# error", err])
        w.writerow(["# verdict", "UNKNOWN"])
        w.writerow(["# action", "Start cloud-sql-proxy on 127.0.0.1:5435 and re-run."])


if __name__ == "__main__":
    sys.exit(main())
