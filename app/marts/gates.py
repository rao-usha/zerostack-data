"""Ship gates as code (SPEC_126a).

The gates SPEC_117-119 ran once, by hand, from a scratchpad script — the ones
that caught AngelList's platform adviser taking 31% of fund attribution and the
149,507 undated Schedule D funds — now run after every guarded build, before
it commits (see ``build_ledger.run_guarded``).

Absolute one-time ranges (SPEC_119's "pairs 9,700-10,200") became ratios and
tolerances against the previous successful build, so they hold as the data
grows. Each gate returns::

    {"passed": bool, "skipped": bool, "value": ..., "threshold": ..., "detail": str}

A gate is *skipped* (and passes) when its stage did not run or, for the
tolerance gates, when there is no previous successful build to compare to.
"""

from __future__ import annotations

import os
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Tuple

from sqlalchemy import text

DEFAULT_MAX_DROP = 0.20
MAX_DROP_ENV = "MART_GATE_MAX_DROP"

Result = Dict[str, Any]


def max_drop() -> float:
    try:
        return float(os.environ.get(MAX_DROP_ENV, DEFAULT_MAX_DROP))
    except ValueError:
        return DEFAULT_MAX_DROP


def _res(passed: bool, value=None, threshold=None, detail: str = "", **extra) -> Result:
    out = {"passed": bool(passed), "skipped": False, "value": value,
           "threshold": threshold, "detail": detail}
    out.update(extra)
    return out


def _skip(detail: str) -> Result:
    return {"passed": True, "skipped": True, "value": None, "threshold": None, "detail": detail}


def _num(stage: Mapping[str, Any], key: str) -> float:
    v = stage.get(key) if isinstance(stage, Mapping) else None
    return v if isinstance(v, (int, float)) and not isinstance(v, bool) else 0


def _linked(funds: Mapping[str, Any]) -> int:
    return sum(v for k, v in funds.items()
               if k.startswith("linked_") and isinstance(v, int) and not isinstance(v, bool))


def _ratio_gate(summary, stage: str, num: Callable, den: Callable, *, lo=None, hi=None,
                what: str = "") -> Result:
    s = summary.get(stage)
    if not isinstance(s, Mapping):
        return _skip(f"stage {stage} did not run")
    d = den(s)
    if not d:
        return _skip(f"no {stage} denominator")
    r = num(s) / d
    ok = (lo is None or r >= lo) and (hi is None or r <= hi)
    bound = f">= {lo}" if hi is None else (f"<= {hi}" if lo is None else f"in [{lo}, {hi}]")
    return _res(ok, round(r, 4), bound, f"{what} {r:.1%} (must be {bound})")


# --- pe_marts ---------------------------------------------------------------


def _firms_nonempty(s, counts, base) -> Result:
    f = s.get("firms")
    if not isinstance(f, Mapping):
        return _skip("stage firms did not run")
    n = _num(f, "candidates")
    return _res(n > 0, n, "> 0", f"{n} PE/VC advisers in the ADV roster")


def _adv_undated(s, counts, base) -> Result:
    return _ratio_gate(s, "adv_private_funds", lambda a: _num(a, "skipped_undated"),
                       lambda a: _num(a, "candidates") + _num(a, "skipped_undated"), hi=0.01,
                       what="undated Schedule D funds")


def _funds_reproduce(s, counts, base) -> Result:
    """SPEC_118 gate (a): the build must reproduce >=95% of the links already there."""
    return _ratio_gate(s, "funds", lambda f: _num(f, "agreed"),
                       lambda f: _num(f, "agreed") + _num(f, "disagreed"), lo=0.95,
                       what="existing links reproduced")


def _funds_link_rate(s, counts, base) -> Result:
    return _ratio_gate(s, "funds", _linked, lambda f: _num(f, "candidates"), lo=0.40,
                       what="funds attributed to a firm")


def _platform_share(s, counts, base) -> Result:
    return _ratio_gate(s, "funds", lambda f: _num(f, "linked_adv_platform"), _linked, hi=0.40,
                       what="links held by filing platforms")


def _platform_advisers(s, counts, base) -> Result:
    f = s.get("funds")
    if not isinstance(f, Mapping) or "platform_advisers" not in f:
        return _skip("stage funds did not run")
    n = _num(f, "platform_advisers")
    return _res(n <= 10, n, "<= 10", f"{n} advisers classed as filing platforms (the cliff sat at 4)")


def _people_pairs(p):
    return _num(p, "candidate_pairs")


def _people_signer(s, counts, base) -> Result:
    return _ratio_gate(s, "people", lambda p: _num(p, "tier_form_d_signer"), _people_pairs,
                       lo=0.90, hi=0.99, what="pairs in the form_d_signer tier")


def _people_admin(s, counts, base) -> Result:
    return _ratio_gate(s, "people", lambda p: _num(p, "tier_fund_admin"), _people_pairs,
                       hi=0.05, what="pairs in the fund_admin tier")


def _people_platform(s, counts, base) -> Result:
    return _ratio_gate(s, "people", lambda p: _num(p, "tier_platform_fund"), _people_pairs,
                       hi=0.02, what="pairs in the platform_fund tier")


def _zero(stage: str, key: str, what: str):
    def gate(s, counts, base) -> Result:
        st = s.get(stage)
        if not isinstance(st, Mapping):
            return _skip(f"stage {stage} did not run")
        n = _num(st, key)
        return _res(n == 0, n, "== 0", f"{n} {what}")
    return gate


# --- entity_resolve -----------------------------------------------------------


def _feeds_total(feeds: Mapping[str, Any]) -> int:
    return sum(v for v in feeds.values() if isinstance(v, int) and not isinstance(v, bool))


def _feeds_nonempty(s, counts, base) -> Result:
    f = s.get("feeds")
    if not isinstance(f, Mapping):
        return _skip("stage feeds did not run")
    n = _feeds_total(f)
    return _res(n > 0, n, "> 0", f"{n} source records fed")


MASS_MERGE_MIN = 500
MASS_MERGE_SHARE = 0.02


def _mass_merge(s, counts, base) -> Result:
    r = s.get("resolve")
    if not isinstance(r, Mapping):
        return _skip("stage resolve did not run")
    prev_live = ((base or {}).get("tables") or {}).get("entities_live")
    if not prev_live:
        return _skip("no previous entity count")
    limit = max(MASS_MERGE_MIN, int(prev_live * MASS_MERGE_SHARE))
    n = _num(r, "entities_merged")
    return _res(n <= limit, n, f"<= {limit}", f"{n} entities merged in one run")


# --- tolerance vs the previous successful build --------------------------------


def _drop(value, previous, what: str) -> Result:
    if previous is None or not isinstance(previous, (int, float)) or previous <= 0:
        return _skip("no baseline")
    tol = max_drop()
    floor = previous * (1 - tol)
    drop = 1 - value / previous
    return _res(value >= floor, value, f">= {floor:.0f}",
                f"{what}: {previous} -> {value} ({drop:+.0%} drop, tolerance {tol:.0%})",
                baseline=previous)


def _stage_value(summary, stage: str, key: str):
    st = summary.get(stage)
    if not isinstance(st, Mapping):
        return None
    if key == "total":
        return _feeds_total(st)
    v = st.get(key)
    return v if isinstance(v, (int, float)) and not isinstance(v, bool) else None


# (stage, key) pairs compared against the previous success.
STAGE_DROPS: Dict[str, List[Tuple[str, str]]] = {
    "pe_marts": [("firms", "candidates"), ("adv_private_funds", "candidates"),
                 ("funds", "candidates"), ("people", "candidate_pairs"),
                 ("people", "firms_covered")],
    "entity_resolve": [("feeds", "total"), ("bridge", "accepted")],
}

GateFn = Callable[[Mapping[str, Any], Mapping[str, int], Optional[Mapping[str, Any]]], Result]

GATES: Dict[str, List[Tuple[str, GateFn]]] = {
    "pe_marts": [
        ("firms_nonempty", _firms_nonempty),
        ("adv_undated_share", _adv_undated),
        ("funds_reproduce_links", _funds_reproduce),
        ("funds_link_rate", _funds_link_rate),
        ("platform_link_share", _platform_share),
        ("platform_adviser_count", _platform_advisers),
        ("people_signer_share", _people_signer),
        ("people_fund_admin_share", _people_admin),
        ("people_platform_fund_share", _people_platform),
        ("people_no_silent_merge", _zero("people", "collides_same_firm",
                                         "incoming people collide with a same-firm person")),
        ("people_links_resolved", _zero("people", "link_person_missing",
                                        "firm links whose person id could not be read back")),
    ],
    "entity_resolve": [
        ("feeds_nonempty", _feeds_nonempty),
        ("entity_mass_merge", _mass_merge),
    ],
}

# Published rows counted after the build (inside its transaction), compared to
# the previous success. (name, table, where, columns the where needs)
PUBLISHED_COUNTS: Dict[str, List[Tuple[str, str, str, Sequence[str]]]] = {
    "pe_marts": [
        ("pe_firms_sec", "public.pe_firms", "CAST(data_sources AS TEXT) LIKE '%SEC ADV%'",
         ("data_sources",)),
        ("pe_funds_sec", "public.pe_funds", "data_source = 'SEC Form D'", ("data_source",)),
        ("pe_funds_attributed", "public.pe_funds",
         "data_source = 'SEC Form D' AND firm_id IS NOT NULL", ("data_source", "firm_id")),
        ("sec_adv_private_funds", "public.sec_adv_private_funds", "", ()),
        ("pe_people_sec", "public.pe_people", "source_key LIKE 'secformd:%'", ("source_key",)),
        ("pe_firm_people_sec", "public.pe_firm_people", "data_source = 'SEC Form D'",
         ("data_source",)),
    ],
    "entity_resolve": [
        ("source_record", "core.source_record", "", ()),
        ("entities_live", "core.entity", "dissolved_at IS NULL", ("dissolved_at",)),
        ("bridge", "core.cik_crd_bridge", "", ()),
    ],
}


def count_published(conn, mart: str) -> Dict[str, int]:
    """Row counts of the mart's published tables. Tables or columns that do not
    exist are left out rather than failing the (open) build transaction."""
    out: Dict[str, int] = {}
    for name, table, where, cols in PUBLISHED_COUNTS.get(mart, []):
        if conn.execute(text("SELECT to_regclass(:t)"), {"t": table}).scalar() is None:
            continue
        if cols:
            schema, tbl = table.split(".", 1)
            have = {r[0] for r in conn.execute(text(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = :s AND table_name = :t"), {"s": schema, "t": tbl})}
            if not set(cols) <= have:
                continue
        sql = f"SELECT COUNT(*) FROM {table}" + (f" WHERE {where}" if where else "")
        out[name] = int(conn.execute(text(sql)).scalar() or 0)
    return out


def evaluate(
    mart: str,
    summary: Mapping[str, Any],
    counts: Mapping[str, int],
    baseline: Optional[Mapping[str, Any]],
) -> Dict[str, Result]:
    """Every gate of ``mart``. ``baseline`` is the previous successful build's
    ``stage_counts`` ({"stages": ..., "tables": ...}) or None."""
    results: Dict[str, Result] = {}
    for name, fn in GATES.get(mart, []):
        results[name] = fn(summary, counts, baseline)

    prev_stages = (baseline or {}).get("stages") or {}
    for stage, key in STAGE_DROPS.get(mart, []):
        name = f"drop:{stage}.{key}"
        value = _stage_value(summary, stage, key)
        if value is None:
            results[name] = _skip(f"stage {stage} did not run")
            continue
        results[name] = _drop(value, _stage_value(prev_stages, stage, key), f"{stage}.{key}")

    prev_tables = (baseline or {}).get("tables") or {}
    for name, value in counts.items():
        results[f"rows:{name}"] = _drop(value, prev_tables.get(name), f"{name} rows")
    return results


def failures(results: Mapping[str, Result], override=None) -> List[str]:
    """Names of failed gates not covered by ``override`` (True or a list of names)."""
    from app.marts.inputs import normalize_override

    all_, names = normalize_override(override)
    return [k for k, r in results.items()
            if not r.get("passed") and not all_ and k not in names]
