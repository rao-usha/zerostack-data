"""One-off smoke test for SPEC_061 Market Intelligence Pack against cloud DB.

Runs from the host (where cloud-sql-proxy listens on 127.0.0.1:5435). Renders
a sample report for NAICS 3323 (Architectural and Structural Metals Mfg) ×
MSA 26420 (Houston-Pasadena-The Woodlands, TX) and writes the HTML.

Usage:
    python scripts/smoke_test_mip.py
"""
from __future__ import annotations

import os
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

# Skip heavy app imports — only need the template + sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from app.reports.templates.market_intelligence_pack import MarketIntelligencePackTemplate

# No default: the credentials live in the environment, never in the repo.
CLOUD_URL = os.environ.get("DATABASE_URL")
if not CLOUD_URL:
    sys.exit("DATABASE_URL is required (e.g. postgresql://nexdata:<password>@127.0.0.1:5435/nexdata)")

# Sample: Building equipment contractors × Houston.
# We initially picked 3323 (Architectural & Structural Metals Mfg) but the
# cloud CBP coverage skews heavily toward construction (NAICS 23) / utilities
# (22) / mining (21) sectors — not manufacturing. NAICS 2382 has 15k TX
# establishments at the state grain, exercises §3 fully.
SAMPLE_PARAMS = {
    "naics_code": "2382",
    "geography_mode": "msa",
    "msa_code": "26420",
    "client_note": (
        "SPEC_061 smoke test — Houston building-equipment contractors "
        "(HVAC / plumbing / electrical) landscape."
    ),
}


def main() -> int:
    print(f"Connecting to: {CLOUD_URL.split('@')[-1]}")
    engine = create_engine(CLOUD_URL, future=True)
    tpl = MarketIntelligencePackTemplate()

    print(f"\nRunning gather_data for NAICS={SAMPLE_PARAMS['naics_code']}, "
          f"MSA={SAMPLE_PARAMS.get('msa_code', '—')} ...")
    start = datetime.now()
    with Session(engine) as db:
        data = tpl.gather_data(db, SAMPLE_PARAMS)
    gather_elapsed = (datetime.now() - start).total_seconds()
    print(f"  gather_data: {gather_elapsed:.2f}s")

    print(f"\nProvenance ({len(data['_provenance'])} sources):")
    for p in data["_provenance"]:
        print(f"  {p['section']:<25} <- {p['table']:<30} ({p['rows']} rows)")

    print(f"\nFallback used: {data['_fallback_used']}")
    if not data['_fallback_used']:
        print(f"MSA county coverage: {data['_msa_county_coverage_pct']:.0f}%")

    print(f"\nRendering HTML ...")
    start = datetime.now()
    html = tpl.render_html(data)
    render_elapsed = (datetime.now() - start).total_seconds()
    print(f"  render_html: {render_elapsed:.2f}s; {len(html):,} bytes")

    out_dir = ROOT / "data" / "reference"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "mip_smoke_sample.html"
    out_path.write_text(html, encoding="utf-8")
    print(f"\nWrote: {out_path}")
    print(f"\nOpen with:  Start-Process '{out_path}'")
    return 0


if __name__ == "__main__":
    sys.exit(main())
