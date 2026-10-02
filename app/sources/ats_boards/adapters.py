"""
Board JSON -> one normalized posting shape (SPEC_151). Pure: no I/O.

Only the official public job-board endpoints are named here:

- Greenhouse Job Board API: ``boards-api.greenhouse.io/v1/boards/<token>`` (board name) and
  ``/jobs?content=true&pay_transparency=true`` (``pay_input_ranges`` only appears with the flag).
- Lever Postings API: ``api.lever.co/v0/postings/<site>?mode=json``.
- Ashby public job posting API: ``api.ashbyhq.com/posting-api/job-board/<org>?includeCompensation=true``.

Fields read are the posting's own: title, team, location, dates, URL, description and pay.
No applicant, recruiter or hiring-manager field is read. E-mail addresses and phone numbers
inside descriptions are redacted before anything is stored.
"""

from __future__ import annotations

import hashlib
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from app.sources.ats_boards import pay

ATS_TYPES = ("greenhouse", "lever", "ashby")
_TOKEN = re.compile(r"^[a-z0-9][a-z0-9._-]{0,99}$", re.I)

_EMAIL = re.compile(r"[\w.+-]+@[\w-]+(?:\.[\w-]+)+")
# phone numbers: (212) 555-0199, 212-555-0199, +1 212 555 0199 -- not "$168,000" or years
_PHONE = re.compile(r"(?<![\w$.,])(?:\+?\d{1,2}[\s.-])?\(?\d{3}\)?[\s.-]\d{3}[\s.-]\d{4}(?![\w,])")


def _check_token(token: str) -> str:
    if not token or not _TOKEN.match(token) or ".." in token:
        raise ValueError(f"not a board token: {token!r}")
    return token


def board_jobs_url(ats: str, token: str) -> str:
    _check_token(token)
    if ats == "greenhouse":
        return f"https://boards-api.greenhouse.io/v1/boards/{token}/jobs?content=true&pay_transparency=true"
    if ats == "lever":
        return f"https://api.lever.co/v0/postings/{token}?mode=json"
    if ats == "ashby":
        return f"https://api.ashbyhq.com/posting-api/job-board/{token}?includeCompensation=true"
    raise ValueError(f"unknown ats {ats!r}")


def board_meta_url(ats: str, token: str) -> Optional[str]:
    _check_token(token)
    return f"https://boards-api.greenhouse.io/v1/boards/{token}" if ats == "greenhouse" else None


def jobs_from_payload(ats: str, payload: Any) -> List[Dict[str, Any]]:
    if ats == "lever":
        return [j for j in payload if isinstance(j, dict)] if isinstance(payload, list) else []
    if isinstance(payload, dict):
        return [j for j in (payload.get("jobs") or []) if isinstance(j, dict)]
    return []


def redact(text: str) -> str:
    if not text:
        return text
    return _PHONE.sub("[phone]", _EMAIL.sub("[email]", text))


def _iso(v: Any) -> Optional[str]:
    if v in (None, ""):
        return None
    if isinstance(v, (int, float)):
        try:
            return datetime.fromtimestamp(float(v) / 1000.0, tz=timezone.utc).isoformat()
        except (OverflowError, OSError, ValueError):
            return None
    return str(v)


def _uniq(xs) -> List[str]:
    out: List[str] = []
    for x in xs:
        x = (x or "").strip() if isinstance(x, str) else None
        if x and x not in out:
            out.append(x)
    return out


def posting_text(ats: str, raw: Dict[str, Any]) -> str:
    """The posting's readable text (pay parsing, verification, storage after redaction)."""
    if ats == "greenhouse":
        return pay.clean(raw.get("content"))
    if ats == "lever":
        parts = [raw.get("descriptionPlain") or pay.clean(raw.get("description"))]
        for lst in raw.get("lists") or []:
            if isinstance(lst, dict):
                parts.append(pay.clean(lst.get("text")) + ": " + pay.clean(lst.get("content")))
        parts += [raw.get("additionalPlain") or pay.clean(raw.get("additional")),
                  raw.get("salaryDescriptionPlain") or pay.clean(raw.get("salaryDescription"))]
        return " ".join(p.strip() for p in parts if p and p.strip())
    if ats == "ashby":
        return raw.get("descriptionPlain") or pay.clean(raw.get("descriptionHtml"))
    return ""


def normalize(ats: str, raw: Dict[str, Any]) -> Dict[str, Any]:
    if ats == "greenhouse":
        loc = raw.get("location")
        depts = raw.get("departments") or []
        rec = {
            "external_id": str(raw.get("id") or ""),
            "title": raw.get("title") or "",
            "department": depts[0].get("name") if depts and isinstance(depts[0], dict) else None,
            "team": None,
            "location": loc.get("name") if isinstance(loc, dict) else (loc or None),
            "locations_all": _uniq(o.get("name") for o in raw.get("offices") or [] if isinstance(o, dict)),
            "employment_type": None,
            "workplace_type": None,
            "posted_at": _iso(raw.get("first_published")),
            "source_updated_at": _iso(raw.get("updated_at")),
            "source_url": raw.get("absolute_url"),
        }
    elif ats == "lever":
        cat = raw.get("categories") or {}
        rec = {
            "external_id": str(raw.get("id") or ""),
            "title": raw.get("text") or "",
            "department": cat.get("department"),
            "team": cat.get("team"),
            "location": cat.get("location"),
            "locations_all": _uniq(cat.get("allLocations") or [cat.get("location")]),
            "employment_type": cat.get("commitment"),
            "workplace_type": raw.get("workplaceType"),
            "posted_at": _iso(raw.get("createdAt")),
            "source_updated_at": None,
            "source_url": raw.get("hostedUrl"),
        }
    elif ats == "ashby":
        secondary = [s.get("location") for s in raw.get("secondaryLocations") or [] if isinstance(s, dict)]
        rec = {
            "external_id": str(raw.get("id") or ""),
            "title": raw.get("title") or "",
            "department": raw.get("department"),
            "team": raw.get("team"),
            "location": raw.get("location"),
            "locations_all": _uniq([raw.get("location")] + secondary),
            "employment_type": raw.get("employmentType"),
            "workplace_type": raw.get("workplaceType") or ("Remote" if raw.get("isRemote") else None),
            "posted_at": _iso(raw.get("publishedAt")),
            "source_updated_at": None,
            "source_url": raw.get("jobUrl"),
        }
    else:
        raise ValueError(f"unknown ats {ats!r}")
    text = posting_text(ats, raw)
    p = pay.best(ats, raw, text)
    rec.update(p.as_columns() if p else dict(pay.EMPTY_COLUMNS))
    if rec.get("pay_snippet"):
        rec["pay_snippet"] = redact(rec["pay_snippet"])
    stored = redact(text)
    rec["description_text"] = stored or None
    rec["description_sha256"] = hashlib.sha256(stored.encode("utf-8")).hexdigest() if stored else None
    return rec
