"""
Pure normalizers for EDGAR submissions fields (SPEC_111).

Ported from wildcard-workbench ``ingest/norm.py`` (clean_ein, state2, zip5,
phone10), with one fix learned there: foreign filers report the EIN
``999999999``, so every all-same-digit EIN counts as a sentinel and becomes None.
"""

from __future__ import annotations

import re
import unicodedata
from datetime import date
from typing import Iterable, Optional

_NON_ALNUM_RE = re.compile(r"[^A-Za-z0-9]+")


def s(value) -> Optional[str]:
    """Strip; '' / None -> None. EDGAR uses '' for 'not populated'."""
    out = str(value).strip() if value is not None else ""
    return out or None


def clean_ein(value) -> Optional[str]:
    """9-digit EIN, or None for blank / wrong length / sentinel (000000000, 999999999, ...)."""
    digits = re.sub(r"\D", "", str(value or ""))
    if len(digits) != 9 or len(set(digits)) == 1:
        return None
    return digits


def state2(value) -> Optional[str]:
    """2-letter code, or None. Accepts 'US-PA'. EDGAR country codes like 'K8' / 'A6' -> None."""
    if not value:
        return None
    folded = unicodedata.normalize("NFKD", str(value))
    folded = "".join(ch for ch in folded if not unicodedata.combining(ch))
    code = _NON_ALNUM_RE.sub("", folded).upper()
    if code.startswith("US") and len(code) == 4:
        code = code[2:]
    return code if len(code) == 2 and code.isalpha() else None


def zip5(value) -> Optional[str]:
    """First 5 digits of a US zip, or None. '95630-1234' -> '95630'."""
    digits = re.sub(r"\D", "", str(value or ""))
    if len(digits) < 5 or digits[:5] == "00000":
        return None
    return digits[:5]


def phone10(value) -> Optional[str]:
    """US 10-digit phone or None. Cuts the extension first, then a leading country '1'."""
    text = str(value or "").casefold()
    text = re.split(r"(?:\bx\b|\bext\b|extension|\bx\d)", text, maxsplit=1)[0]
    digits = re.sub(r"\D", "", text)
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    if len(digits) < 10:
        return None
    digits = digits[:10]
    return digits if digits.strip("0") else None


def parse_date(value) -> Optional[date]:
    """'2020-08-28T00:00:00.000Z' / '2020-08-28' -> date, else None."""
    text = str(value or "")[:10]
    try:
        return date.fromisoformat(text)
    except ValueError:
        return None


def pg_text_array(values: Optional[Iterable]) -> Optional[str]:
    """Python strings -> Postgres TEXT[] literal for COPY; empty -> None."""
    items = [str(v) for v in (values or []) if v is not None and str(v).strip()]
    if not items:
        return None
    quoted = ('"' + v.replace("\\", "\\\\").replace('"', '\\"') + '"' for v in items)
    return "{" + ",".join(quoted) + "}"
