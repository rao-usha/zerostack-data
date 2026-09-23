"""Mart input assertions (SPEC_126a, PLAN_085 §D5).

A mart may only build on inputs whose latest release actually loaded. Before
this, a failed 13F load on the 9th meant the 10th's resolver and marts ran on
last quarter's data and reported success.

For each input bulk source:

- the **latest discovery batch** (every ``raw.source_release`` row discovered
  within ``BATCH_WINDOW`` of the newest ``discovered_at``) must be ``loaded``.
  ``discovered_at`` is written once, so it orders releases by when the
  publisher produced them; the batch catches ``ria:2026-06`` loaded next to
  ``era:2026-06`` failed. Older unloaded releases are counted, not refused.
- the newest ``loaded_at`` must be within the source's max age.

A source with no loaded release at all is a missing input.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from sqlalchemy import text

BATCH_WINDOW = timedelta(hours=6)

# Newest loaded_at, in days. Quarterly sources: the zip lands weeks after the
# quarter ends and loaded releases are skipped, so right before the next quarter
# lands the newest load is ~4 months old.
DAILY, MONTHLY, QUARTERLY = 7, 75, 150
MAX_AGE_DAYS: Dict[str, float] = {
    "sec_iapd_feed": DAILY,
    "sec_edgar_submissions": DAILY,
    "sec_adv_roster": MONTHLY,
    "sec_adv_schedule_d": MONTHLY,
    "sec_form_d": QUARTERLY,
    "sec_insider": QUARTERLY,
    "sec_13f": QUARTERLY,
}

# Which bulk sources each stage reads (see the SQL in app/marts, app/entities).
PE_MART_STAGE_INPUTS: Dict[str, List[str]] = {
    "firms": ["sec_adv_roster"],
    "adv_private_funds": ["sec_adv_schedule_d"],
    "funds": ["sec_form_d", "sec_adv_roster"],
    "people": ["sec_form_d", "sec_adv_roster"],
}
ENTITY_STAGE_INPUTS: Dict[str, List[str]] = {
    "feeds": ["sec_adv_roster", "sec_iapd_feed", "sec_13f", "sec_form_d",
              "sec_edgar_submissions", "sec_insider"],
    "bridge": ["sec_13f", "sec_adv_roster"],
}

_ALL = {"1", "true", "all", "*", "yes"}


def sources_for(stage_inputs: Mapping[str, Sequence[str]], stages: Iterable[str]) -> List[str]:
    """Distinct input sources of the stages that will run, sorted."""
    out = set()
    for stage in stages:
        out.update(stage_inputs.get(stage, ()))
    return sorted(out)


def normalize_override(value) -> Tuple[bool, List[str]]:
    """``True``/``"all"`` -> (True, []); a list or comma string -> (False, names)."""
    if value is None or value is False:
        return False, []
    if value is True:
        return True, []
    if isinstance(value, str):
        value = value.split(",")
    names = [str(v).strip() for v in value if str(v).strip()]
    if any(n.lower() in _ALL for n in names):
        return True, []
    return False, names


def _iso(v) -> Optional[str]:
    return v.isoformat() if v is not None else None


def check_source(conn, source: str, now: datetime, max_age_days: float) -> Dict[str, Any]:
    """Assess one input. Returns a JSON-able record with ``ok`` and ``problem``."""
    rows = conn.execute(text(
        "SELECT release_key, status, discovered_at, loaded_at, "
        "LEFT(COALESCE(error, ''), 200) AS error "
        "FROM raw.source_release WHERE source = :s "
        "ORDER BY discovered_at DESC, id DESC"
    ), {"s": source}).mappings().all()
    newest_loaded = conn.execute(text(
        "SELECT MAX(loaded_at) FROM raw.source_release WHERE source = :s AND status = 'loaded'"
    ), {"s": source}).scalar()

    record: Dict[str, Any] = {
        "source": source, "release_keys": [], "statuses": {}, "loaded_at": _iso(newest_loaded),
        "age_days": None, "max_age_days": max_age_days, "older_unloaded": 0,
        "ok": True, "problem": None,
    }
    if not rows or newest_loaded is None:
        record.update(ok=False, problem=f"{source}: no loaded release")
        return record

    top = rows[0]["discovered_at"]
    batch = [r for r in rows if r["discovered_at"] >= top - BATCH_WINDOW]
    older = rows[len(batch):]
    record["release_keys"] = [r["release_key"] for r in batch]
    record["statuses"] = {r["release_key"]: r["status"] for r in batch}
    record["older_unloaded"] = sum(1 for r in older if r["status"] != "loaded")

    bad = [r for r in batch if r["status"] != "loaded"]
    age = (now - newest_loaded).total_seconds() / 86400
    record["age_days"] = round(age, 1)
    if bad:
        parts = ", ".join(
            f"{r['release_key']} is {r['status']}" + (f" ({r['error']})" if r["error"] else "")
            for r in bad[:3]
        )
        record.update(ok=False, problem=f"{source}: latest release {parts}")
    elif age > max_age_days:
        record.update(ok=False, problem=(
            f"{source}: newest load is {age:.0f} days old (max {max_age_days:g})"))
    return record


def check_inputs(
    conn,
    sources: Sequence[str],
    now: Optional[datetime] = None,
    max_age_days: Optional[Mapping[str, float]] = None,
) -> Tuple[List[Dict[str, Any]], List[str]]:
    """Assess every input. Returns (records, problems)."""
    now = now or datetime.utcnow()
    if not sources:
        return [], []
    if conn.execute(text("SELECT to_regclass('raw.source_release')")).scalar() is None:
        problems = [f"{s}: no loaded release (raw.source_release missing)" for s in sources]
        return [{"source": s, "ok": False, "problem": p} for s, p in zip(sources, problems)], problems
    overrides = {k: float(v) for k, v in (max_age_days or {}).items()}
    records = [
        check_source(conn, s, now, overrides.get(s, MAX_AGE_DAYS.get(s, QUARTERLY)))
        for s in sources
    ]
    return records, [r["problem"] for r in records if not r["ok"]]
