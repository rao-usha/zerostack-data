"""
Eval Scorer — assertion type implementations for SPEC_039 / PLAN_041.

Each assertion_type maps to a scorer function with signature:
    _score_<type>(case: EvalCase, output: CapturedOutput, db: Session) -> ScorerResult

EvalScorer.score() is the main dispatcher.

Tier 1 (hard):   score = 100 if passed else 0
Tier 2 (soft):   partial credit proportional to how close actual is to expected
Tier 3 (LLM):    handled separately in eval_scorer_llm.py (Phase 6)

Partial credit formula for range assertions:
    if actual < min:  score = round(100 * actual / min)
    if actual > max:  score = round(100 * max / actual)
    if in range:      score = 100
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import Any

from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# CapturedOutput — passed from EvalRunner to every scorer function
# ---------------------------------------------------------------------------

@dataclass
class CapturedOutput:
    """Raw output captured from an agent, API call, or DB snapshot."""
    mode: str                        # "db_snapshot"|"api_response"|"agent_output"|"report_output"
    entity_id: int | None = None
    entity_type: str | None = None
    raw: Any = None                  # dict | list | str depending on mode
    status_code: int | None = None   # for api_response mode
    latency_ms: float | None = None  # for api_response mode
    cost_usd: float = 0.0
    error: str | None = None


# ---------------------------------------------------------------------------
# ScorerResult — returned by every scorer function
# ---------------------------------------------------------------------------

@dataclass
class ScorerResult:
    passed: bool
    score: float                     # 0–100
    actual_value: Any = None
    expected_value: Any = None
    failure_reason: str | None = None
    partial_credit: bool = False
    # LLM judge fields (Tier 3 only)
    llm_judge_prompt: str | None = None
    llm_judge_response: str | None = None
    llm_judge_reasoning: str | None = None
    llm_cost_usd: float = 0.0


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _range_score(actual: float, min_val: float, max_val: float) -> float:
    """Partial credit for range assertions. Returns 0–100."""
    if actual < min_val:
        return round(100 * actual / min_val) if min_val > 0 else 0.0
    if actual > max_val:
        return round(100 * max_val / actual) if actual > 0 else 0.0
    return 100.0


def _get_nested(data: Any, path: str) -> Any:
    """Traverse a dot-separated field path into a nested dict."""
    for key in path.split("."):
        if isinstance(data, dict):
            data = data.get(key)
        else:
            return None
    return data


def _normalize_name(name: str) -> str:
    """Lowercase, strip punctuation/suffixes for fuzzy matching."""
    name = name.lower()
    for suffix in [" jr", " sr", " iii", " ii", " iv", " phd", " md", "."]:
        name = name.replace(suffix, "")
    return name.strip()


def _fuzzy_name_match(a: str, b: str, threshold: float = 0.85) -> bool:
    """Simple character-level similarity. Returns True if similarity >= threshold."""
    a, b = _normalize_name(a), _normalize_name(b)
    if a == b:
        return True
    # Levenshtein-style ratio via difflib
    from difflib import SequenceMatcher
    ratio = SequenceMatcher(None, a, b).ratio()
    return ratio >= threshold


# ---------------------------------------------------------------------------
# People / Org Chart scorers
# ---------------------------------------------------------------------------

def _score_ceo_exists(case, output: CapturedOutput, db: Session) -> ScorerResult:
    """Tier 1: at least one current person with management_level=1, not a board member."""
    from app.core.people_models import CompanyPerson
    entity_id = output.entity_id or case.entity_id
    rows = (
        db.query(CompanyPerson)
        .filter(
            CompanyPerson.company_id == entity_id,
            CompanyPerson.is_current.is_(True),
            CompanyPerson.management_level == 1,
            CompanyPerson.is_board_member.is_(False),
        )
        .all()
    )
    passed = len(rows) > 0
    return ScorerResult(
        passed=passed,
        score=100.0 if passed else 0.0,
        actual_value={"management_level_1_count": len(rows), "titles": [r.title for r in rows]},
        expected_value={"min_count": 1},
        failure_reason=None if passed else "No active person with management_level=1 and is_board_member=False found.",
    )


def _score_no_duplicate_ceo(case, output: CapturedOutput, db: Session) -> ScorerResult:
    """Tier 1: at most 1 active person with 'ceo' or 'chief executive' in title."""
    from app.core.people_models import CompanyPerson
    entity_id = output.entity_id or case.entity_id
    rows = (
        db.query(CompanyPerson)
        .filter(
            CompanyPerson.company_id == entity_id,
            CompanyPerson.is_current.is_(True),
        )
        .all()
    )
    ceo_rows = [
        r for r in rows
        if r.title and ("ceo" in r.title.lower() or "chief executive" in r.title.lower())
    ]
    passed = len(ceo_rows) <= 1
    return ScorerResult(
        passed=passed,
        score=100.0 if passed else 0.0,
        actual_value={"ceo_count": len(ceo_rows), "titles": [r.title for r in ceo_rows]},
        expected_value={"max_count": 1},
        failure_reason=None if passed else f"Found {len(ceo_rows)} active people with CEO-like titles.",
    )


def _score_person_exists(case, output: CapturedOutput, db: Session) -> ScorerResult:
    """Tier 1/2: named person found via fuzzy name match in company_people → people join."""
    from app.core.people_models import CompanyPerson, Person
    params = case.assertion_params or {}
    target_name = params.get("full_name", "")
    threshold = float(params.get("fuzzy_threshold", 0.85))
    entity_id = output.entity_id or case.entity_id

    rows = (
        db.query(CompanyPerson, Person.full_name)
        .join(Person, CompanyPerson.person_id == Person.id)
        .filter(
            CompanyPerson.company_id == entity_id,
            CompanyPerson.is_current.is_(True),
        )
        .all()
    )
    match_name = None
    for _cp, full_name in rows:
        if full_name and _fuzzy_name_match(full_name, target_name, threshold):
            match_name = full_name
            break
    passed = match_name is not None
    return ScorerResult(
        passed=passed,
        score=100.0 if passed else 0.0,
        actual_value={"matched_name": match_name, "searched": target_name},
        expected_value={"full_name": target_name, "fuzzy_threshold": threshold},
        failure_reason=None if passed else f"No current person matching '{target_name}' (threshold {threshold}).",
    )


def _score_no_extraction_errors(case, output: CapturedOutput, db: Session) -> ScorerResult:
    """Tier 1: CollectionResult.errors is empty (or below max_errors threshold)."""
    params = case.assertion_params or {}
    max_errors = int(params.get("max_errors", 0))
    raw = output.raw or {}
    errors = raw.get("errors", []) if isinstance(raw, dict) else []
    passed = len(errors) <= max_errors
    return ScorerResult(
        passed=passed,
        score=100.0 if passed else 0.0,
        actual_value={"error_count": len(errors), "errors": errors[:5]},
        expected_value={"max_errors": max_errors},
        failure_reason=None if passed else f"{len(errors)} extraction errors (max allowed: {max_errors}).",
    )


def _score_headcount_range(case, output: CapturedOutput, db: Session) -> ScorerResult:
    """Tier 2: total people count within [min, max] — partial credit if close."""
    from app.core.people_models import OrgChartSnapshot
    params = case.assertion_params or {}
    min_val = float(params.get("min", 0))
    max_val = float(params.get("max", 9999))
    entity_id = output.entity_id or case.entity_id

    # Try output first (agent_output mode), then DB snapshot
    actual = None
    if isinstance(output.raw, dict):
        actual = output.raw.get("total_people") or output.raw.get("people_found")
    if actual is None:
        snap = (
            db.query(OrgChartSnapshot)
            .filter(OrgChartSnapshot.company_id == entity_id)
            .order_by(OrgChartSnapshot.snapshot_date.desc())
            .first()
        )
        actual = snap.total_people if snap else 0

    actual = actual or 0
    score = _range_score(actual, min_val, max_val)
    passed = min_val <= actual <= max_val
    return ScorerResult(
        passed=passed,
        score=score,
        partial_credit=not passed and score > 0,
        actual_value={"total_people": actual},
        expected_value={"min": min_val, "max": max_val},
        failure_reason=None if passed else f"Found {actual} people, expected [{min_val}–{max_val}].",
    )


def _score_has_person_with_title(case, output: CapturedOutput, db: Session) -> ScorerResult:
    """Tier 2: count active people whose title contains a keyword."""
    from app.core.people_models import CompanyPerson
    params = case.assertion_params or {}
    title_contains = params.get("title_contains", "").lower()
    min_count = int(params.get("min_count", 1))
    entity_id = output.entity_id or case.entity_id

    rows = (
        db.query(CompanyPerson)
        .filter(
            CompanyPerson.company_id == entity_id,
            CompanyPerson.is_current.is_(True),
        )
        .all()
    )
    matches = [r for r in rows if r.title and title_contains in r.title.lower()]
    count = len(matches)
    score = _range_score(count, min_count, 9999)
    passed = count >= min_count
    return ScorerResult(
        passed=passed,
        score=score,
        partial_credit=not passed and score > 0,
        actual_value={"count": count, "titles": [r.title for r in matches[:5]]},
        expected_value={"title_contains": title_contains, "min_count": min_count},
        failure_reason=None if passed else f"Found {count} people with '{title_contains}' in title (need {min_count}).",
    )


def _score_person_has_title(case, output: CapturedOutput, db: Session) -> ScorerResult:
    """Tier 2: a named person's current title matches expected title (fuzzy)."""
    from app.core.people_models import CompanyPerson
    params = case.assertion_params or {}
    target_name = params.get("full_name", "")
    expected_title = params.get("expected_title", "").lower()
    entity_id = output.entity_id or case.entity_id

    rows = (
        db.query(CompanyPerson)
        .filter(
            CompanyPerson.company_id == entity_id,
            CompanyPerson.is_current.is_(True),
        )
        .all()
    )
    person_row = next(
        (r for r in rows if r.full_name and _fuzzy_name_match(r.full_name, target_name)),
        None,
    )
    if person_row is None:
        return ScorerResult(
            passed=False, score=0.0,
            actual_value={"found": False},
            expected_value={"full_name": target_name, "expected_title": expected_title},
            failure_reason=f"Person '{target_name}' not found in company.",
        )
    actual_title = (person_row.title or "").lower()
    from difflib import SequenceMatcher
    similarity = SequenceMatcher(None, actual_title, expected_title).ratio()
    passed = similarity >= 0.75
    score = round(similarity * 100)
    return ScorerResult(
        passed=passed,
        score=float(score),
        partial_credit=not passed and score > 0,
        actual_value={"name": person_row.full_name, "title": person_row.title},
        expected_value={"full_name": target_name, "expected_title": expected_title},
        failure_reason=None if passed else f"Title '{person_row.title}' doesn't match expected '{expected_title}' (similarity {similarity:.2f}).",
    )


def _score_org_depth_range(case, output: CapturedOutput, db: Session) -> ScorerResult:
    """Tier 2: org chart max_depth within [min_depth, max_depth]."""
    from app.core.people_models import OrgChartSnapshot
    params = case.assertion_params or {}
    min_depth = float(params.get("min_depth", 1))
    max_depth = float(params.get("max_depth", 20))
    entity_id = output.entity_id or case.entity_id

    actual = None
    if isinstance(output.raw, dict):
        actual = output.raw.get("max_depth") or output.raw.get("org_depth")
    if actual is None:
        snap = (
            db.query(OrgChartSnapshot)
            .filter(OrgChartSnapshot.company_id == entity_id)
            .order_by(OrgChartSnapshot.snapshot_date.desc())
            .first()
        )
        actual = snap.max_depth if snap else 0

    actual = actual or 0
    score = _range_score(actual, min_depth, max_depth)
    passed = min_depth <= actual <= max_depth
    return ScorerResult(
        passed=passed,
        score=score,
        partial_credit=not passed and score > 0,
        actual_value={"max_depth": actual},
        expected_value={"min_depth": min_depth, "max_depth": max_depth},
        failure_reason=None if passed else f"Org depth {actual} not in [{min_depth}–{max_depth}].",
    )


def _score_confidence_threshold(case, output: CapturedOutput, db: Session) -> ScorerResult:
    """Tier 2: average confidence score across extracted people >= min_avg_confidence."""
    from app.core.people_models import CompanyPerson, Person
    params = case.assertion_params or {}
    min_avg = float(params.get("min_avg_confidence", 0.7))
    entity_id = output.entity_id or case.entity_id

    rows = (
        db.query(CompanyPerson, Person)
        .join(Person, Person.id == CompanyPerson.person_id)
        .filter(
            CompanyPerson.company_id == entity_id,
            CompanyPerson.is_current.is_(True),
        )
        .all()
    )
    scores = [float(p.confidence_score) for _, p in rows if p.confidence_score is not None]
    avg = sum(scores) / len(scores) if scores else 0.0
    passed = avg >= min_avg
    score = round(min(avg / min_avg, 1.0) * 100) if min_avg > 0 else 100
    return ScorerResult(
        passed=passed,
        score=float(score),
        partial_credit=not passed and score > 0,
        actual_value={"avg_confidence": round(avg, 3), "sample_size": len(scores)},
        expected_value={"min_avg_confidence": min_avg},
        failure_reason=None if passed else f"Avg confidence {avg:.3f} < threshold {min_avg}.",
    )


def _score_confidence_distribution(case, output: CapturedOutput, db: Session) -> ScorerResult:
    """Tier 2: >= min_high_pct fraction of extracted people have confidence='high'."""
    from app.core.people_models import CompanyPerson
    params = case.assertion_params or {}
    min_high_pct = float(params.get("min_high_pct", 0.5))
    entity_id = output.entity_id or case.entity_id

    rows = (
        db.query(CompanyPerson)
        .filter(
            CompanyPerson.company_id == entity_id,
            CompanyPerson.is_current.is_(True),
        )
        .all()
    )
    if not rows:
        return ScorerResult(passed=False, score=0.0, actual_value={"total": 0},
                            failure_reason="No current people found.")
    high = sum(1 for r in rows if r.confidence == "high")
    pct = high / len(rows)
    score = round(min(pct / min_high_pct, 1.0) * 100) if min_high_pct > 0 else 100
    passed = pct >= min_high_pct
    return ScorerResult(
        passed=passed,
        score=float(score),
        partial_credit=not passed and score > 0,
        actual_value={"high_confidence_pct": round(pct, 3), "high": high, "total": len(rows)},
        expected_value={"min_high_pct": min_high_pct},
        failure_reason=None if passed else f"{pct:.1%} high-confidence, need {min_high_pct:.1%}.",
    )


def _score_source_pages_found(case, output: CapturedOutput, db: Session) -> ScorerResult:
    """Tier 2: number of pages successfully scraped >= min_pages."""
    params = case.assertion_params or {}
    min_pages = int(params.get("min_pages", 1))
    raw = output.raw or {}
    actual = 0
    if isinstance(raw, dict):
        actual = raw.get("pages_crawled") or raw.get("pages_found") or raw.get("urls_checked") or 0
    score = _range_score(actual, min_pages, 9999)
    passed = actual >= min_pages
    return ScorerResult(
        passed=passed,
        score=score,
        partial_credit=not passed and score > 0,
        actual_value={"pages_found": actual},
        expected_value={"min_pages": min_pages},
        failure_reason=None if passed else f"Only {actual} pages found (need {min_pages}).",
    )


def _score_dept_coverage(case, output: CapturedOutput, db: Session) -> ScorerResult:
    """Tier 2: all required department names are present in the org chart."""
    from app.core.people_models import OrgChartSnapshot
    params = case.assertion_params or {}
    required = [d.lower() for d in params.get("required_depts", [])]
    entity_id = output.entity_id or case.entity_id

    snap = (
        db.query(OrgChartSnapshot)
        .filter(OrgChartSnapshot.company_id == entity_id)
        .order_by(OrgChartSnapshot.snapshot_date.desc())
        .first()
    )
    found_depts = []
    if snap and snap.chart_data:
        chart = snap.chart_data if isinstance(snap.chart_data, dict) else {}
        found_depts = [d.lower() for d in chart.get("departments", [])]

    missing = [d for d in required if not any(d in f or f in d for f in found_depts)]
    passed = len(missing) == 0
    score = round(100 * (len(required) - len(missing)) / len(required)) if required else 100
    return ScorerResult(
        passed=passed,
        score=float(score),
        partial_credit=not passed and score > 0,
        actual_value={"found_depts": found_depts[:10]},
        expected_value={"required_depts": required},
        failure_reason=None if passed else f"Missing departments: {missing}.",
    )


# ---------------------------------------------------------------------------
# API Response scorers
# ---------------------------------------------------------------------------

def _score_response_status_200(case, output: CapturedOutput, db: Session) -> ScorerResult:
    """Tier 1: HTTP response status is 200."""
    actual = output.status_code
    if actual is None and isinstance(output.raw, dict):
        actual = output.raw.get("status_code")
    passed = actual == 200
    return ScorerResult(
        passed=passed,
        score=100.0 if passed else 0.0,
        actual_value={"status_code": actual},
        expected_value={"status_code": 200},
        failure_reason=None if passed else f"HTTP {actual} (expected 200).",
    )


def _score_response_field_present(case, output: CapturedOutput, db: Session) -> ScorerResult:
    """Tier 1: a dot-path field exists and is non-null in the response JSON."""
    params = case.assertion_params or {}
    field_path = params.get("field_path", "")
    raw = output.raw if isinstance(output.raw, dict) else {}
    value = _get_nested(raw, field_path)
    passed = value is not None
    return ScorerResult(
        passed=passed,
        score=100.0 if passed else 0.0,
        actual_value={"field_path": field_path, "value": str(value)[:200] if value else None},
        expected_value={"field_path": field_path, "expected": "non-null"},
        failure_reason=None if passed else f"Field '{field_path}' is null or missing.",
    )


def _score_response_no_error_key(case, output: CapturedOutput, db: Session) -> ScorerResult:
    """Tier 1: response JSON has no 'error' or 'detail' top-level key."""
    raw = output.raw if isinstance(output.raw, dict) else {}
    error_val = raw.get("error") or raw.get("detail")
    passed = error_val is None
    return ScorerResult(
        passed=passed,
        score=100.0 if passed else 0.0,
        actual_value={"error": error_val},
        expected_value={"error": None},
        failure_reason=None if passed else f"Response contains error: {str(error_val)[:200]}",
    )


def _score_response_field_range(case, output: CapturedOutput, db: Session) -> ScorerResult:
    """Tier 2: a numeric field in the response is within [min, max]."""
    params = case.assertion_params or {}
    field_path = params.get("field_path", "")
    min_val = float(params.get("min", 0))
    max_val = float(params.get("max", 9999))
    raw = output.raw if isinstance(output.raw, dict) else {}
    actual = _get_nested(raw, field_path)
    if actual is None:
        return ScorerResult(passed=False, score=0.0,
                            actual_value={"field_path": field_path, "value": None},
                            failure_reason=f"Field '{field_path}' not found.")
    actual = float(actual)
    score = _range_score(actual, min_val, max_val)
    passed = min_val <= actual <= max_val
    return ScorerResult(
        passed=passed,
        score=score,
        partial_credit=not passed and score > 0,
        actual_value={"field_path": field_path, "value": actual},
        expected_value={"min": min_val, "max": max_val},
        failure_reason=None if passed else f"{field_path}={actual} not in [{min_val}–{max_val}].",
    )


def _score_response_list_length(case, output: CapturedOutput, db: Session) -> ScorerResult:
    """Tier 2: a list field in the response has length within [min, max]."""
    params = case.assertion_params or {}
    field_path = params.get("field_path", "")
    min_val = float(params.get("min", 0))
    max_val = float(params.get("max", 9999))
    raw = output.raw if isinstance(output.raw, dict) else {}
    lst = _get_nested(raw, field_path) if field_path else raw
    if not isinstance(lst, list):
        lst = []
    actual = len(lst)
    score = _range_score(actual, min_val, max_val)
    passed = min_val <= actual <= max_val
    return ScorerResult(
        passed=passed,
        score=score,
        partial_credit=not passed and score > 0,
        actual_value={"field_path": field_path, "length": actual},
        expected_value={"min": min_val, "max": max_val},
        failure_reason=None if passed else f"List '{field_path}' has {actual} items, expected [{min_val}–{max_val}].",
    )


def _score_response_time_ms(case, output: CapturedOutput, db: Session) -> ScorerResult:
    """Tier 2: API response latency within budget."""
    params = case.assertion_params or {}
    max_ms = float(params.get("max_ms", 5000))
    actual = output.latency_ms or 0.0
    passed = actual <= max_ms
    score = round(min(max_ms / actual, 1.0) * 100) if actual > 0 else 100
    return ScorerResult(
        passed=passed,
        score=float(score),
        partial_credit=not passed and score > 0,
        actual_value={"latency_ms": actual},
        expected_value={"max_ms": max_ms},
        failure_reason=None if passed else f"Latency {actual:.0f}ms exceeds {max_ms:.0f}ms budget.",
    )


def _score_response_word_count_range(case, output: CapturedOutput, db: Session) -> ScorerResult:
    """Tier 2: word count of a text field in the response within [min, max]."""
    params = case.assertion_params or {}
    field_path = params.get("field_path", "")
    min_val = float(params.get("min", 0))
    max_val = float(params.get("max", 99999))
    raw = output.raw if isinstance(output.raw, dict) else {}
    text = _get_nested(raw, field_path) if field_path else (raw if isinstance(raw, str) else "")
    actual = len(str(text or "").split()) if text else 0
    score = _range_score(actual, min_val, max_val)
    passed = min_val <= actual <= max_val
    return ScorerResult(
        passed=passed,
        score=score,
        partial_credit=not passed and score > 0,
        actual_value={"word_count": actual},
        expected_value={"min": min_val, "max": max_val},
        failure_reason=None if passed else f"Word count {actual} not in [{min_val}–{max_val}].",
    )


# ---------------------------------------------------------------------------
# Report Generator scorers
# ---------------------------------------------------------------------------

def _score_report_section_present(case, output: CapturedOutput, db: Session) -> ScorerResult:
    """Tier 1: HTML/text output contains an expected section heading."""
    params = case.assertion_params or {}
    section_name = params.get("section_name", "")
    html = output.raw if isinstance(output.raw, str) else ""
    # Match h1-h6 tags or plain text containing the section name (case-insensitive)
    pattern = re.compile(re.escape(section_name), re.IGNORECASE)
    passed = bool(pattern.search(html))
    return ScorerResult(
        passed=passed,
        score=100.0 if passed else 0.0,
        actual_value={"searched_for": section_name, "found": passed},
        expected_value={"section_name": section_name},
        failure_reason=None if passed else f"Section '{section_name}' not found in report output.",
    )


def _score_report_no_empty_tables(case, output: CapturedOutput, db: Session) -> ScorerResult:
    """Tier 1: no tables in the report consist entirely of N/A or zero values."""
    html = output.raw if isinstance(output.raw, str) else ""
    # Find <td> content — flag if every cell in a table is "N/A", "—", "0", or empty
    tables = re.findall(r"<table[^>]*>(.*?)</table>", html, re.DOTALL | re.IGNORECASE)
    empty_tables = 0
    for table in tables:
        cells = re.findall(r"<td[^>]*>(.*?)</td>", table, re.DOTALL | re.IGNORECASE)
        cell_texts = [re.sub(r"<[^>]+>", "", c).strip() for c in cells]
        real_cells = [c for c in cell_texts if c and c not in ("N/A", "—", "-", "0", "0.0", "")]
        if cells and not real_cells:
            empty_tables += 1
    passed = empty_tables == 0
    return ScorerResult(
        passed=passed,
        score=100.0 if passed else 0.0,
        actual_value={"empty_tables": empty_tables, "total_tables": len(tables)},
        expected_value={"max_empty_tables": 0},
        failure_reason=None if passed else f"{empty_tables} table(s) with all N/A or zero values.",
    )


def _score_report_word_count(case, output: CapturedOutput, db: Session) -> ScorerResult:
    """Tier 2: narrative word count of the report within [min, max]."""
    params = case.assertion_params or {}
    min_val = float(params.get("min", 100))
    max_val = float(params.get("max", 99999))
    html = output.raw if isinstance(output.raw, str) else ""
    # Strip HTML tags for word count
    text = re.sub(r"<[^>]+>", " ", html)
    actual = len(text.split())
    score = _range_score(actual, min_val, max_val)
    passed = min_val <= actual <= max_val
    return ScorerResult(
        passed=passed,
        score=score,
        partial_credit=not passed and score > 0,
        actual_value={"word_count": actual},
        expected_value={"min": min_val, "max": max_val},
        failure_reason=None if passed else f"Report word count {actual} not in [{min_val}–{max_val}].",
    )


def _score_report_data_cells_pct(case, output: CapturedOutput, db: Session) -> ScorerResult:
    """Tier 2: >= min_pct of data cells in report contain real (non-N/A) values."""
    params = case.assertion_params or {}
    min_pct = float(params.get("min_pct", 0.7))
    html = output.raw if isinstance(output.raw, str) else ""
    cells = re.findall(r"<td[^>]*>(.*?)</td>", html, re.DOTALL | re.IGNORECASE)
    cell_texts = [re.sub(r"<[^>]+>", "", c).strip() for c in cells]
    if not cell_texts:
        return ScorerResult(passed=False, score=0.0, actual_value={"total_cells": 0},
                            failure_reason="No data cells found in report.")
    real = sum(1 for c in cell_texts if c and c not in ("N/A", "—", "-", "0", ""))
    pct = real / len(cell_texts)
    score = round(min(pct / min_pct, 1.0) * 100) if min_pct > 0 else 100
    passed = pct >= min_pct
    return ScorerResult(
        passed=passed,
        score=float(score),
        partial_credit=not passed and score > 0,
        actual_value={"real_cells_pct": round(pct, 3), "real": real, "total": len(cell_texts)},
        expected_value={"min_pct": min_pct},
        failure_reason=None if passed else f"Only {pct:.1%} data cells have real values (need {min_pct:.1%}).",
    )


def _score_thesis_mentions_entity(case, output: CapturedOutput, db: Session) -> ScorerResult:
    """Tier 2: entity name appears in the output text."""
    params = case.assertion_params or {}
    entity_name = params.get("entity_name", case.entity_name or "")
    text = output.raw if isinstance(output.raw, str) else str(output.raw or "")
    passed = entity_name.lower() in text.lower() if entity_name else False
    return ScorerResult(
        passed=passed,
        score=100.0 if passed else 0.0,
        actual_value={"searched_for": entity_name, "found": passed},
        expected_value={"entity_name": entity_name},
        failure_reason=None if passed else f"Entity name '{entity_name}' not mentioned in output.",
    )


# ---------------------------------------------------------------------------
# PE / 3PL / LP scorers
# ---------------------------------------------------------------------------

def _score_deal_count_range(case, output: CapturedOutput, db: Session) -> ScorerResult:
    """Tier 2: PE firm deal count within [min, max]."""
    from app.core.pe_models import PEDeal
    params = case.assertion_params or {}
    min_val = float(params.get("min", 0))
    max_val = float(params.get("max", 9999))
    entity_id = output.entity_id or case.entity_id

    actual = db.query(PEDeal).filter(PEDeal.firm_id == entity_id).count()
    score = _range_score(actual, min_val, max_val)
    passed = min_val <= actual <= max_val
    return ScorerResult(
        passed=passed,
        score=score,
        partial_credit=not passed and score > 0,
        actual_value={"deal_count": actual},
        expected_value={"min": min_val, "max": max_val},
        failure_reason=None if passed else f"Deal count {actual} not in [{min_val}–{max_val}].",
    )


def _score_has_deal_with_status(case, output: CapturedOutput, db: Session) -> ScorerResult:
    """Tier 1: at least one deal with the given status exists for this firm."""
    from app.core.pe_models import PEDeal
    params = case.assertion_params or {}
    status = params.get("status", "closed")
    entity_id = output.entity_id or case.entity_id

    count = db.query(PEDeal).filter(
        PEDeal.firm_id == entity_id,
        PEDeal.status == status,
    ).count()
    passed = count > 0
    return ScorerResult(
        passed=passed,
        score=100.0 if passed else 0.0,
        actual_value={"count_with_status": count, "status": status},
        expected_value={"min_count": 1, "status": status},
        failure_reason=None if passed else f"No deals with status='{status}' found.",
    )


def _score_enrichment_coverage_pct(case, output: CapturedOutput, db: Session) -> ScorerResult:
    """Tier 2: >= min_pct of records have a specific field populated."""
    from app.core.models_site_intel import ThreePLCompany
    params = case.assertion_params or {}
    field_name = params.get("field", "website")
    min_pct = float(params.get("min_pct", 0.5))

    total = db.query(ThreePLCompany).count()
    if total == 0:
        return ScorerResult(passed=False, score=0.0, actual_value={"total": 0},
                            failure_reason="No 3PL companies found.")
    enriched = db.query(ThreePLCompany).filter(
        getattr(ThreePLCompany, field_name, None).isnot(None)
    ).count()
    pct = enriched / total
    score = round(min(pct / min_pct, 1.0) * 100) if min_pct > 0 else 100
    passed = pct >= min_pct
    return ScorerResult(
        passed=passed,
        score=float(score),
        partial_credit=not passed and score > 0,
        actual_value={"field": field_name, "coverage_pct": round(pct, 3), "enriched": enriched, "total": total},
        expected_value={"min_pct": min_pct},
        failure_reason=None if passed else f"{pct:.1%} coverage for '{field_name}' (need {min_pct:.1%}).",
    )


def _score_score_field_range(case, output: CapturedOutput, db: Session) -> ScorerResult:
    """Tier 2: a numeric score field in the API response is within [min, max]."""
    # Reuses response_field_range logic
    return _score_response_field_range(case, output, db)


def _score_lp_count_range(case, output: CapturedOutput, db: Session) -> ScorerResult:
    """Tier 2: LP record count within [min, max]."""
    params = case.assertion_params or {}
    min_val = float(params.get("min", 0))
    max_val = float(params.get("max", 9999))

    try:
        from app.core.lp_models import LP
        actual = db.query(LP).count()
    except Exception:
        actual = 0

    score = _range_score(actual, min_val, max_val)
    passed = min_val <= actual <= max_val
    return ScorerResult(
        passed=passed,
        score=score,
        partial_credit=not passed and score > 0,
        actual_value={"lp_count": actual},
        expected_value={"min": min_val, "max": max_val},
        failure_reason=None if passed else f"LP count {actual} not in [{min_val}–{max_val}].",
    )


# ---------------------------------------------------------------------------
# LLM Judge (Tier 3) — shared helper + domain-specific scorers
# ---------------------------------------------------------------------------

def _raw_to_str(raw: Any, max_chars: int = 3000) -> str:
    """Serialize captured output to a string, truncated for LLM prompts."""
    import json as _json
    if isinstance(raw, (dict, list)):
        return _json.dumps(raw, default=str)[:max_chars]
    return str(raw or "")[:max_chars]


def _call_llm_judge_sync(prompt: str, pass_threshold: float = 70.0) -> ScorerResult:
    """
    Shared LLM judge harness. Calls gpt-4o-mini synchronously.
    Returns a ScorerResult with llm_judge_* fields populated.
    Gracefully returns failure (score=0) when OPENAI_API_KEY is unset.
    """
    import json as _json
    import os

    api_key = os.environ.get("OPENAI_API_KEY", "").strip()
    if not api_key:
        return ScorerResult(
            passed=False,
            score=0.0,
            failure_reason="OPENAI_API_KEY not set — LLM judge skipped.",
            llm_judge_prompt=prompt,
        )

    try:
        import openai  # noqa: PLC0415
        client = openai.OpenAI(api_key=api_key)
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.1,
            max_tokens=200,
        )
        raw_response = response.choices[0].message.content or ""

        # Cost: gpt-4o-mini $0.15/1M input, $0.60/1M output
        input_tokens = response.usage.prompt_tokens
        output_tokens = response.usage.completion_tokens
        cost_usd = (input_tokens * 0.15 + output_tokens * 0.60) / 1_000_000

        # Parse JSON response (strip markdown fences if present)
        clean = raw_response.strip()
        if clean.startswith("```"):
            clean = re.sub(r"^```[a-z]*\n?", "", clean)
            clean = re.sub(r"\n?```$", "", clean.strip())
        try:
            parsed = _json.loads(clean)
            llm_score = float(parsed.get("score", 0))
            reasoning = str(parsed.get("reasoning", ""))
        except Exception:
            llm_score = 0.0
            reasoning = f"Failed to parse LLM response: {raw_response[:200]}"

        passed = llm_score >= pass_threshold
        return ScorerResult(
            passed=passed,
            score=llm_score,
            partial_credit=not passed and llm_score > 0,
            actual_value={"llm_score": llm_score, "pass_threshold": pass_threshold},
            expected_value={"min_score": pass_threshold},
            failure_reason=None if passed else f"LLM score {llm_score:.0f} < threshold {pass_threshold:.0f}: {reasoning}",
            llm_judge_prompt=prompt,
            llm_judge_response=raw_response,
            llm_judge_reasoning=reasoning,
            llm_cost_usd=cost_usd,
        )
    except Exception as exc:
        logger.warning("LLM judge error: %s", exc)
        return ScorerResult(
            passed=False,
            score=0.0,
            failure_reason=f"LLM judge error: {exc}",
            llm_judge_prompt=prompt,
        )


def _score_llm_judge(case, output: CapturedOutput, db: Session) -> ScorerResult:
    """Tier 3: LLM evaluates output quality against free-form criteria.

    assertion_params:
      criteria        — free-form evaluation rubric (required)
      pass_threshold  — minimum score to pass (default 70)
    """
    params = case.assertion_params or {}
    criteria = params.get("criteria", "Evaluate the quality and completeness of the output.")
    pass_threshold = float(params.get("pass_threshold", 70.0))
    raw_str = _raw_to_str(output.raw)

    prompt = (
        "You are evaluating the quality of an AI pipeline output.\n\n"
        f"Evaluation Criteria:\n{criteria}\n\n"
        f"Output to Evaluate:\n{raw_str}\n\n"
        "Respond with JSON only (no markdown):\n"
        '{"score": <0-100>, "reasoning": "<brief explanation>"}\n\n'
        "Score guide: 0-30 = fails criteria, 31-69 = partially meets, 70-100 = meets or exceeds."
    )
    return _call_llm_judge_sync(prompt, pass_threshold)


def _score_llm_org_chart_quality(case, output: CapturedOutput, db: Session) -> ScorerResult:
    """Tier 3: LLM evaluates org chart hierarchy quality.

    Rubric: hierarchy plausibility (CEO→VP→Director ordering), no circular
    reporting, department groupings make business sense, title consistency.

    assertion_params:
      pass_threshold  — minimum score to pass (default 70)
      focus_areas     — optional list of extra rubric points
    """
    params = case.assertion_params or {}
    pass_threshold = float(params.get("pass_threshold", 70.0))
    extra = params.get("focus_areas", [])
    raw_str = _raw_to_str(output.raw)

    extra_points = "\n".join(f"- {p}" for p in extra) if extra else ""
    prompt = (
        "You are evaluating the quality of an organizational chart extracted by an AI pipeline.\n\n"
        "Score the org chart on the following rubric (0-100):\n"
        "- Hierarchy plausibility: CEO or President at top, VPs below, Directors below VPs\n"
        "- No obvious circular reporting chains (person reports to their own subordinate)\n"
        "- Department groupings make business sense for the company type\n"
        "- Title consistency: similar seniority levels use consistent naming conventions\n"
        "- No obvious extraction artifacts (duplicated names, placeholder text, truncated titles)\n"
        f"{extra_points}\n\n"
        f"Org Chart Data:\n{raw_str}\n\n"
        "Respond with JSON only (no markdown):\n"
        '{"score": <0-100>, "reasoning": "<2-3 sentences on what you observed>"}\n\n'
        "Score guide: 0-30 = clearly broken, 31-69 = partially valid, 70-100 = credible org structure."
    )
    return _call_llm_judge_sync(prompt, pass_threshold)


def _score_llm_people_plausibility(case, output: CapturedOutput, db: Session) -> ScorerResult:
    """Tier 3: LLM evaluates whether extracted people records look real and consistent.

    Rubric: names look real, titles match seniority, no extraction artifacts,
    no placeholder text, sources are credible.

    assertion_params:
      pass_threshold  — minimum score to pass (default 70)
      focus_areas     — optional list of extra rubric points
    """
    params = case.assertion_params or {}
    pass_threshold = float(params.get("pass_threshold", 70.0))
    extra = params.get("focus_areas", [])
    raw_str = _raw_to_str(output.raw)

    extra_points = "\n".join(f"- {p}" for p in extra) if extra else ""
    prompt = (
        "You are evaluating a list of people records extracted by an AI pipeline.\n\n"
        "Score the people data on the following rubric (0-100):\n"
        "- Names look like real human names (no random strings, no ALL CAPS artifacts)\n"
        "- Titles are appropriate business titles that match expected seniority levels\n"
        "- No obvious extraction artifacts (e.g. 'DIRECTOR DIRECTOR', 'N/A', 'Unknown')\n"
        "- No placeholder or template text in any field\n"
        "- The overall set of people is plausible for the company type and size\n"
        f"{extra_points}\n\n"
        f"People Records:\n{raw_str}\n\n"
        "Respond with JSON only (no markdown):\n"
        '{"score": <0-100>, "reasoning": "<2-3 sentences on what you observed>"}\n\n'
        "Score guide: 0-30 = clearly bad data, 31-69 = mixed quality, 70-100 = looks like real people."
    )
    return _call_llm_judge_sync(prompt, pass_threshold)


def _score_llm_report_coherence(case, output: CapturedOutput, db: Session) -> ScorerResult:
    """Tier 3: LLM evaluates whether a generated report is coherent and useful.

    Rubric: narrative flows logically, data cited supports conclusions,
    no contradictions, no boilerplate filler, executive summary matches body.

    assertion_params:
      pass_threshold  — minimum score to pass (default 70)
      focus_areas     — optional list of extra rubric points
    """
    params = case.assertion_params or {}
    pass_threshold = float(params.get("pass_threshold", 70.0))
    extra = params.get("focus_areas", [])
    raw_str = _raw_to_str(output.raw)

    extra_points = "\n".join(f"- {p}" for p in extra) if extra else ""
    prompt = (
        "You are evaluating a business intelligence report generated by an AI pipeline.\n\n"
        "Score the report on the following rubric (0-100):\n"
        "- Narrative coherence: the report flows logically from section to section\n"
        "- Data-backed conclusions: claims are supported by specific numbers or facts in the report\n"
        "- No internal contradictions (e.g. summary says X, body says not-X)\n"
        "- No boilerplate filler ('This report provides a comprehensive overview...')\n"
        "- Executive summary (if present) accurately reflects the body of the report\n"
        f"{extra_points}\n\n"
        f"Report Content (truncated):\n{raw_str}\n\n"
        "Respond with JSON only (no markdown):\n"
        '{"score": <0-100>, "reasoning": "<2-3 sentences on what you observed>"}\n\n'
        "Score guide: 0-30 = incoherent/generic, 31-69 = partially useful, 70-100 = credible business report."
    )
    return _call_llm_judge_sync(prompt, pass_threshold)


# ---------------------------------------------------------------------------
# Dispatcher
# ---------------------------------------------------------------------------

_SCORERS = {
    # People / Org Chart
    "ceo_exists":               _score_ceo_exists,
    "no_duplicate_ceo":         _score_no_duplicate_ceo,
    "person_exists":            _score_person_exists,
    "no_extraction_errors":     _score_no_extraction_errors,
    "headcount_range":          _score_headcount_range,
    "has_person_with_title":    _score_has_person_with_title,
    "person_has_title":         _score_person_has_title,
    "org_depth_range":          _score_org_depth_range,
    "confidence_threshold":     _score_confidence_threshold,
    "confidence_distribution":  _score_confidence_distribution,
    "source_pages_found":       _score_source_pages_found,
    "dept_coverage":            _score_dept_coverage,
    # API Response
    "response_status_200":      _score_response_status_200,
    "response_field_present":   _score_response_field_present,
    "response_no_error_key":    _score_response_no_error_key,
    "response_field_range":     _score_response_field_range,
    "response_list_length":     _score_response_list_length,
    "response_time_ms":         _score_response_time_ms,
    "response_word_count_range":_score_response_word_count_range,
    # Report Generator
    "report_section_present":   _score_report_section_present,
    "report_no_empty_tables":   _score_report_no_empty_tables,
    "report_word_count":        _score_report_word_count,
    "report_data_cells_pct":    _score_report_data_cells_pct,
    "thesis_mentions_entity":   _score_thesis_mentions_entity,
    # PE / 3PL / LP
    "deal_count_range":         _score_deal_count_range,
    "has_deal_with_status":     _score_has_deal_with_status,
    "enrichment_coverage_pct":  _score_enrichment_coverage_pct,
    "score_field_range":        _score_score_field_range,
    "lp_count_range":           _score_lp_count_range,
    # LLM Judge (Tier 3)
    "llm_judge":                    _score_llm_judge,
    "llm_org_chart_quality":        _score_llm_org_chart_quality,
    "llm_people_plausibility":      _score_llm_people_plausibility,
    "llm_report_coherence":         _score_llm_report_coherence,
}


class EvalScorer:
    """Main scorer dispatcher. Call EvalScorer.score(case, output, db)."""

    SUPPORTED_TYPES = set(_SCORERS.keys())

    @staticmethod
    def score(case, output: CapturedOutput, db: Session) -> ScorerResult:
        """
        Dispatch to the correct scorer function for case.assertion_type.
        Catches all exceptions so a bad scorer never kills a whole run.
        """
        fn = _SCORERS.get(case.assertion_type)
        if fn is None:
            return ScorerResult(
                passed=False,
                score=0.0,
                failure_reason=f"Unknown assertion_type '{case.assertion_type}'.",
            )
        try:
            return fn(case, output, db)
        except Exception as exc:
            logger.warning(
                "EvalScorer: assertion '%s' (case %s) raised: %s",
                case.assertion_type, getattr(case, "id", "?"), exc,
            )
            return ScorerResult(
                passed=False,
                score=0.0,
                failure_reason=f"Scorer error: {exc}",
            )
