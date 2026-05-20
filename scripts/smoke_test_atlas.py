"""One-off smoke test for SPEC_064 Atlas against the cloud DB.

Runs the canonical smoke query — "Houston building equipment contractors" —
through AtlasService.explore() and prints a structural summary.

Usage (from the api container, with cloud DB):
    docker exec -e DATABASE_URL=postgresql://nexdata:Nex2026@host.docker.internal:5435/nexdata \
        nexdata-api-1 python scripts/smoke_test_atlas.py
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from app.services.atlas import AtlasService

CLOUD_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://nexdata:Nex2026@127.0.0.1:5435/nexdata",
)
QUERY = "Houston building equipment contractors"


def main() -> int:
    print(f"Connecting to: {CLOUD_URL.split('@')[-1]}")
    engine = create_engine(CLOUD_URL, future=True)
    with Session(engine) as db:
        expl = AtlasService(db).explore(query=QUERY)
    d = expl.to_dict()

    print(f"\n=== Atlas exploration: {QUERY!r} ===")
    print(f"slug:    {d['slug']}")
    print(f"msa:     {d['resolved_entities']['msa']}")
    print(f"naics:   {d['resolved_entities']['naics']}")
    print(f"datasets:{d['resolved_entities']['datasets']}")
    print(f"\ncards ({len(d['cards'])}):")
    for c in d["cards"]:
        print(f"  - {c['id']:<22} conf={c['confidence']:<7} "
              f"{len(c['metrics'])} metrics  {len(c['provenance'])} provenance")
        for m in c["metrics"][:3]:
            print(f"      · {m['label']}: {m['value']}")
    print(f"\nconnections ({len(d['connections'])}):")
    for cn in d["connections"]:
        print(f"  - {cn['source_card']} -> {cn['target_card']}  ({cn['relationship']})")
    print(f"\nrelated_queries ({len(d['related_queries'])}):")
    for rq in d["related_queries"]:
        print(f"  - {rq['query']}  [{rq['kind']}]")
    print(f"\nsummary headline: {d['summary']['headline']}")
    print(f"coverage_notes:   {len(d['summary']['coverage_notes'])}")
    for note in d["summary"]["coverage_notes"]:
        print(f"  · {note}")
    print(f"\nshare_url: {d['share_url']}")

    # Acceptance gate per SPEC_064.
    ok = (
        d["resolved_entities"]["msa"]
        and d["resolved_entities"]["msa"]["code"] == "26420"
        and d["resolved_entities"]["naics"]
        and d["resolved_entities"]["naics"]["code"] == "2382"
    )
    card_ok = len(d["cards"]) >= 4
    print(f"\n{'PASS' if ok and card_ok else 'CHECK'}: "
          f"resolution={'ok' if ok else 'FAIL'}, "
          f"cards={len(d['cards'])} ({'>=4 ok' if card_ok else '<4 — see coverage notes'})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
