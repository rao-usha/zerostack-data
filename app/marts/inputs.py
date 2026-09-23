"""Mart input assertions (SPEC_126a, PLAN_085 §D5).

A mart may only build on inputs whose latest release actually loaded. Before
this, a failed 13F load on the 9th meant the 10th's resolver and marts ran on
last quarter's data and reported success.

For each input bulk source:

- the **latest release** is chosen by the *period its release key names*
  (``2026q3``, ``ria:2026-06-01``, ``edition:2026-09-22``, ``adv1:2026-06:<upload>``,
  ``01jun2026-31aug2026_form13f``), not by when we discovered it. A backfill
  (``?since=2019-01-01``) discovers old quarters *now*: ordering by
  ``discovered_at`` let those hide a failed newest quarter, and let one bad
  2019 zip block the mart until a new release appeared. Within the newest
  period each *series* (the key text before the period: ``ria:`` / ``era:``)
  must have its most recently discovered release ``loaded``, so
  ``ria:2026-06-01`` loaded next to ``era:2026-06-01`` failed still refuses.
  Older unloaded releases are counted (``older_unloaded``), never refused.
  Keys with no recognisable period fall back to the newest discovery batch
  (rows discovered within ``BATCH_WINDOW`` of the newest ``discovered_at``).
- **staleness** is the age of the newest period's first sighting
  (``discovered_at`` of its rows, written once and never bumped), not
  ``MAX(loaded_at)`` over every row: retrying an old failed release, or
  reloading any release, must not make a source whose publisher went quiet
  look fresh. ``loaded_at`` of the newest period is recorded alongside.

A source with no loaded release at all is a missing input.

Upstream marts (``check_upstream_mart``): the ``pe_marts`` firms stage joins
``core.identifier``, the entity master, so the latest real ``entity_resolve``
build in ``core.mart_build`` must be ``success`` and recent. Overridable with
``input_override=["entity_resolve"]`` like any bulk input.
"""

from __future__ import annotations

import re
from datetime import date, datetime, timedelta
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from sqlalchemy import text

BATCH_WINDOW = timedelta(hours=6)

# Age of the newest period's first sighting, in days. Quarterly sources: the
# zip lands weeks after the quarter ends, so right before the next quarter
# lands the newest one was first seen ~4 months ago.
DAILY, MONTHLY, QUARTERLY = 7, 75, 150
MAX_AGE_DAYS: Dict[str, float] = {
    "sec_iapd_feed": DAILY,
    "sec_edgar_submissions": DAILY,
    "sec_adv_roster": MONTHLY,
    "sec_adv_schedule_d": MONTHLY,
    "sec_form_d": QUARTERLY,
    "sec_insider": QUARTERLY,
    "sec_13f": QUARTERLY,
    # upstream mart: entity_resolve runs monthly (the 10th)
    "entity_resolve": 45,
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
# Which upstream marts (core.mart_build) each stage reads.
PE_MART_STAGE_UPSTREAM: Dict[str, List[str]] = {
    "firms": ["entity_resolve"],  # pe_firms_sec joins core.identifier for the CIK
}

# Loaded release keys kept per input in the ledger (newest period first).
LOADED_KEYS_KEPT = 100

_ALL = {"1", "true", "all", "*", "yes"}


def sources_for(stage_inputs: Mapping[str, Sequence[str]], stages: Iterable[str]) -> List[str]:
    """Distinct inputs of the stages that will run, sorted."""
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


def override_payload(input_override=None, gate_override=None,
                     input_max_age_days=None) -> Dict[str, Any]:
    """Queue-payload keys from the admin override query params of the build
    endpoints. Non-list values (a bare ``Query()`` default on a direct call)
    are ignored. ``input_max_age_days`` items are ``source=days``; a malformed
    item raises ValueError."""
    out: Dict[str, Any] = {}
    for key, value in (("input_override", input_override), ("gate_override", gate_override)):
        if isinstance(value, list) and value:
            out[key] = value
    if isinstance(input_max_age_days, list) and input_max_age_days:
        ages: Dict[str, float] = {}
        for item in input_max_age_days:
            name, sep, days = str(item).partition("=")
            try:
                if not sep or not name.strip():
                    raise ValueError
                ages[name.strip()] = float(days)
            except ValueError:
                raise ValueError(f"input_max_age_days item {item!r} is not source=days")
        out["input_max_age_days"] = ages
    return out


def _iso(v) -> Optional[str]:
    return v.isoformat() if v is not None else None


# --- the period a release key names ------------------------------------------------

_MONTHS = {m: i for i, m in enumerate(
    ("jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"), 1)}
_QUARTER_RE = re.compile(r"(\d{4})q([1-4])", re.I)
_RANGE_RE = re.compile(r"\d{1,2}[a-z]{3}\d{4}-(\d{1,2})([a-z]{3})(\d{4})", re.I)
_DATE_RE = re.compile(r"(\d{4})-(\d{2})(?:-(\d{2}))?")


def release_period(release_key: str) -> Tuple[Optional[date], str]:
    """(period date, series) for a release key; (None, key) when unrecognised.

    The period is the end of a quarter / 13F range, or the date (first of the
    month when only ``YYYY-MM``) in the key. The series is the key text before
    the period (``"ria:"``, ``"adv1:"``, ``""``).
    """
    key = release_key or ""
    best = None
    for rx, kind in ((_RANGE_RE, "range"), (_QUARTER_RE, "quarter"), (_DATE_RE, "date")):
        m = rx.search(key)
        if m is None:
            continue
        try:
            if kind == "range":
                d = date(int(m.group(3)), _MONTHS[m.group(2).lower()], int(m.group(1)))
            elif kind == "quarter":
                year, q = int(m.group(1)), int(m.group(2))
                d = (date(year, 12, 31) if q == 4
                     else date(year, 3 * q + 1, 1) - timedelta(days=1))
            else:
                d = date(int(m.group(1)), int(m.group(2)), int(m.group(3) or 1))
        except (KeyError, ValueError):
            continue
        best = (d, key[:m.start()])
        break
    return best if best else (None, key)


# --- bulk inputs -------------------------------------------------------------------


def _latest(rows: Sequence[Mapping[str, Any]]) -> Tuple[List[Mapping[str, Any]], List[Mapping[str, Any]], str]:
    """(the rows that make up the latest release, the rest, how it was chosen)."""
    periods = [(release_period(r["release_key"]), r) for r in rows]
    dated = [(p, r) for p, r in periods if p[0] is not None]
    if dated:
        newest = max(p[0] for p, _ in dated)
        in_period = [(p[1], r) for p, r in dated if p[0] == newest]
        # per series, the most recently discovered release of the newest period
        by_series: Dict[str, Mapping[str, Any]] = {}
        for series, r in in_period:
            cur = by_series.get(series)
            if cur is None or (r["discovered_at"], r["id"]) > (cur["discovered_at"], cur["id"]):
                by_series[series] = r
        latest = [by_series[s] for s in sorted(by_series)]
        chosen = {id(r) for r in latest}
        return latest, [r for r in rows if id(r) not in chosen], f"period {newest.isoformat()}"
    top = max(r["discovered_at"] for r in rows)
    batch = [r for r in rows if r["discovered_at"] >= top - BATCH_WINDOW]
    chosen = {id(r) for r in batch}
    return batch, [r for r in rows if id(r) not in chosen], "discovery batch"


def _period_sort_key(r: Mapping[str, Any]):
    d, _ = release_period(r["release_key"])
    return (d or date.min, r["discovered_at"], r["id"])


def _is_snapshot(source: str) -> bool:
    try:
        from app.ingest.bulk.registry import get_source
        return bool(getattr(get_source(source), "snapshot", False))
    except Exception:
        return False


def check_source(conn, source: str, now: datetime, max_age_days: float) -> Dict[str, Any]:
    """Assess one input. Returns a JSON-able record with ``ok`` and ``problem``."""
    rows = conn.execute(text(
        "SELECT id, release_key, status, discovered_at, loaded_at, "
        "LEFT(COALESCE(error, ''), 200) AS error "
        "FROM raw.source_release WHERE source = :s "
        "ORDER BY discovered_at DESC, id DESC"
    ), {"s": source}).mappings().all()

    loaded = sorted((r for r in rows if r["status"] == "loaded"), key=_period_sort_key,
                    reverse=True)
    record: Dict[str, Any] = {
        "source": source, "kind": "bulk", "release_keys": [], "statuses": {},
        "latest_by": None, "first_seen": None, "loaded_at": None,
        "age_days": None, "max_age_days": max_age_days, "older_unloaded": 0,
        "loaded_release_count": len(loaded),
        "loaded_release_keys": [r["release_key"] for r in loaded[:LOADED_KEYS_KEPT]],
        "ok": True, "problem": None,
    }
    if not loaded:
        record.update(ok=False, problem=f"{source}: no loaded release")
        return record

    latest, older, how = _latest(rows)
    record["latest_by"] = how
    record["release_keys"] = [r["release_key"] for r in latest]
    record["statuses"] = {r["release_key"]: r["status"] for r in latest}
    record["older_unloaded"] = sum(1 for r in older if r["status"] != "loaded")
    # first sighting of the newest period: immune to reloads and old retries
    first_seen = max(r["discovered_at"] for r in latest)
    latest_loaded = [r["loaded_at"] for r in latest if r["loaded_at"] is not None]
    record["first_seen"] = _iso(first_seen)
    record["loaded_at"] = _iso(max(latest_loaded)) if latest_loaded else None

    bad = [r for r in latest if r["status"] != "loaded"]
    # Snapshot sources (SPEC_122) mint no release when upstream is unchanged;
    # they bump loaded_at on the previous one instead, so that re-verification
    # is what "current" means for them.
    as_of = first_seen
    if _is_snapshot(source) and latest_loaded:
        as_of = max(first_seen, max(latest_loaded))
    age = (now - as_of).total_seconds() / 86400
    record["age_days"] = round(age, 1)
    if bad:
        parts = ", ".join(
            f"{r['release_key']} is {r['status']}" + (f" ({r['error']})" if r["error"] else "")
            for r in bad[:3]
        )
        record.update(ok=False, problem=f"{source}: latest release {parts}")
    elif age > max_age_days:
        record.update(ok=False, problem=(
            f"{source}: newest release {', '.join(record['release_keys'][:3])} "
            f"was first seen {age:.0f} days ago (max {max_age_days:g})"))
    return record


# --- upstream marts ------------------------------------------------------------------


def check_upstream_mart(conn, mart: str, now: datetime, max_age_days: float) -> Dict[str, Any]:
    """The latest finished real build of ``mart`` must be a recent success.

    A mart with no ledger history (built before SPEC_126a) passes with a note:
    its tables predate the ledger, not a failure.
    """
    record: Dict[str, Any] = {
        "source": mart, "kind": "mart", "build_id": None, "status": None, "release_keys": [],
        "loaded_at": None, "age_days": None, "max_age_days": max_age_days,
        "ok": True, "problem": None, "note": None,
    }
    if conn.execute(text("SELECT to_regclass('core.mart_build')")).scalar() is None:
        record["note"] = "no ledger (core.mart_build missing)"
        return record
    last = conn.execute(text(
        "SELECT id, status, finished_at, LEFT(COALESCE(refusal_reason, error, ''), 200) AS why "
        "FROM core.mart_build WHERE mart = :m AND NOT dry_run AND status <> 'running' "
        "ORDER BY id DESC LIMIT 1"
    ), {"m": mart}).mappings().first()
    if last is None:
        record["note"] = "no recorded build yet (tables predate the ledger)"
        return record
    record.update(build_id=last["id"], status=last["status"], loaded_at=_iso(last["finished_at"]),
                  release_keys=[f"mart_build:{last['id']}"])
    if last["status"] != "success":
        record.update(ok=False, problem=(
            f"{mart}: latest build #{last['id']} {last['status']}"
            + (f" ({last['why']})" if last["why"] else "")))
        return record
    if last["finished_at"] is not None:
        age = (now - last["finished_at"]).total_seconds() / 86400
        record["age_days"] = round(age, 1)
        if age > max_age_days:
            record.update(ok=False, problem=(
                f"{mart}: latest successful build #{last['id']} is {age:.0f} days old "
                f"(max {max_age_days:g})"))
    return record


def check_inputs(
    conn,
    sources: Sequence[str],
    now: Optional[datetime] = None,
    max_age_days: Optional[Mapping[str, float]] = None,
    upstream: Sequence[str] = (),
) -> Tuple[List[Dict[str, Any]], List[str]]:
    """Assess every bulk input and upstream mart. Returns (records, problems)."""
    now = now or datetime.utcnow()
    overrides = {k: float(v) for k, v in (max_age_days or {}).items()}

    def limit(name: str) -> float:
        return overrides.get(name, MAX_AGE_DAYS.get(name, QUARTERLY))

    records: List[Dict[str, Any]] = []
    if sources:
        if conn.execute(text("SELECT to_regclass('raw.source_release')")).scalar() is None:
            records += [{"source": s, "kind": "bulk", "ok": False,
                         "problem": f"{s}: no loaded release (raw.source_release missing)"}
                        for s in sources]
        else:
            records += [check_source(conn, s, now, limit(s)) for s in sources]
    records += [check_upstream_mart(conn, m, now, limit(m)) for m in upstream]
    return records, [r["problem"] for r in records if not r["ok"]]
