"""
Pay parser for job postings (SPEC_151). Pure: no I/O.

Order: structured pay fields first (Greenhouse ``pay_input_ranges``, Lever
``salaryRange``, Ashby ``compensation.summaryComponents``), else US
pay-transparency text ("between $60,000 and $70,000/year", "$22 to $24hr",
"$120K-$150K", "OTE $200,000", "starts at $25").

Precision over recall. A money amount in text counts only when
- it carries a currency marker ($, US$, CA$, C$, A$, GBP sign, EUR sign, or a trailing
  ISO code on a range),
- a pay cue (pay, salary, compensation, wage, rate, OTE, on-target, earnings,
  hourly, base) sits shortly before it,
- no disqualifier sits right before or after it (reimbursement, stipend, budget,
  revenue, funding, bonus, discount, P&L ...), it has no million/billion suffix,
- the interval is explicit (/year, per hour, hr, annually, hourly cue ...) or the
  magnitude makes it a yearly salary (>= 10,000), and
- the value is plausible for that interval.
Anything else returns None: no pay is better than wrong pay. Every result keeps
the raw snippet it came from and a confidence (``high`` | ``medium``).

pay_v2 (SPEC_156, the 2026-10-06 review sample): "+ Bonus" / "plus commission" / "(including a
bonus)" after a salary range no longer disqualifies it, and a disqualifier before the amount is
overridden by a salary cue after it ("Monthly Stipend The salary for this role is $X-$Y"); an
add-on amount ("+ $11,500 Variable") is not pay; a range whose top is > 4x its bottom is not one
role's band ("$100k - $500k for all engineers"); two-decimal amounts in the hourly band are hourly;
Greenhouse "cents" in a zero-decimal currency (JPY, KRW, ...) are whole units; plausibility is
judged in rough US-dollar terms for every currency.
"""

from __future__ import annotations

import html
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

PARSER_VERSION = "pay_v2"

PLAUSIBLE = {
    "year": (10_000, 2_000_000),
    "month": (800, 100_000),
    "week": (200, 20_000),
    "day": (50, 5_000),
    "hour": (7, 500),
}


# currencies without minor units: Greenhouse "min_cents" is already whole units (pay_v1 divided by 100)
ZERO_DECIMAL = frozenset({"JPY", "KRW", "VND", "CLP", "ISK", "PYG", "UGX"})
# ROUGH US dollars per unit, used ONLY to judge plausibility (never stored, never converted)
USD_PER_UNIT = {"USD": 1.0, "CAD": 0.73, "AUD": 0.66, "NZD": 0.6, "SGD": 0.74, "HKD": 0.128, "GBP": 1.27,
                "EUR": 1.08, "CHF": 1.1, "SEK": 0.095, "NOK": 0.093, "DKK": 0.145, "PLN": 0.25, "ILS": 0.27,
                "INR": 0.012, "MXN": 0.055, "BRL": 0.18, "ZAR": 0.055, "JPY": 0.0067, "KRW": 0.00072,
                "VND": 0.00004, "CLP": 0.00105, "ISK": 0.0072, "PYG": 0.00013, "UGX": 0.00027}
MAX_RANGE_RATIO = 4             # a band wider than 4x is a company-wide statement, not one role's pay


@dataclass(frozen=True)
class Pay:
    min: Optional[float]
    max: Optional[float]
    currency: Optional[str]
    interval: Optional[str]
    kind: str               # base | ote | unspecified
    source: str             # structured | text
    snippet: Optional[str]
    confidence: str         # high | medium

    def as_columns(self) -> Dict[str, Any]:
        return {"pay_min": self.min, "pay_max": self.max, "pay_currency": self.currency,
                "pay_interval": self.interval, "pay_kind": self.kind, "pay_source": self.source,
                "pay_snippet": self.snippet, "pay_confidence": self.confidence,
                "pay_parser": PARSER_VERSION}


EMPTY_COLUMNS = {"pay_min": None, "pay_max": None, "pay_currency": None, "pay_interval": None,
                 "pay_kind": None, "pay_source": None, "pay_snippet": None, "pay_confidence": None,
                 "pay_parser": PARSER_VERSION}


def _num(v: Any) -> Optional[float]:
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return int(f) if f == int(f) else f


def _usd(v: Optional[float], currency: Optional[str]) -> Optional[float]:
    return None if v is None else v * USD_PER_UNIT.get((currency or "USD").upper(), 1.0)


def _plausible(interval: Optional[str], lo: Optional[float], hi: Optional[float], currency: str = "USD") -> bool:
    if interval not in PLAUSIBLE or lo is None:
        return False
    a, b = PLAUSIBLE[interval]
    return (all(v is None or a <= v <= b for v in (_usd(lo, currency), _usd(hi, currency)))
            and (hi is None or hi >= lo))


def _interval_by_magnitude(v: Optional[float], currency: str = "USD") -> Optional[str]:
    if v is None:
        return None
    return "year" if _usd(v, currency) >= 10_000 else None


# ---------------------------------------------------------------------------
# structured
# ---------------------------------------------------------------------------

_LEVER_INTERVAL = {"per-year-salary": "year", "per-month-salary": "month", "per-week-salary": "week",
                   "per-day-wage": "day", "per-hour-wage": "hour"}
_ASHBY_INTERVAL = {"1 YEAR": "year", "1 MONTH": "month", "1 WEEK": "week", "1 DAY": "day", "1 HOUR": "hour",
                   "6 MONTH": None, "3 MONTH": None, "2 WEEK": None}
_ASHBY_PAY_TYPES = {"salary": "base", "hourly": "base", "commission": None, "bonus": None,
                    "equitypercentage": None, "equitycashvalue": None}


def from_structured(ats: str, raw: Dict[str, Any]) -> Optional[Pay]:
    raw = raw or {}
    if ats == "greenhouse":
        ranges = [r for r in (raw.get("pay_input_ranges") or []) if isinstance(r, dict)]
        cur = ((ranges[0].get("currency_type") if ranges else None) or "USD").upper()
        unit = 1 if cur in ZERO_DECIMAL else 100
        lows = [_num(r.get("min_cents")) for r in ranges]
        highs = [_num(r.get("max_cents")) for r in ranges]
        lows = [v / unit for v in lows if v]
        highs = [v / unit for v in highs if v]
        if not lows:
            return None
        lo, hi = _num(min(lows)), _num(max(highs)) if highs else None
        interval = _interval_by_magnitude(lo, cur)
        if not _plausible(interval, lo, hi, cur):
            return None
        snippet = "; ".join(f"{r.get('title') or ''} {r.get('min_cents')}-{r.get('max_cents')} cents "
                            f"{r.get('currency_type') or ''}".strip() for r in ranges)[:300]
        return Pay(lo, hi, cur, interval, "unspecified", "structured", "pay_input_ranges: " + snippet,
                   "high" if len(ranges) == 1 else "medium")
    if ats == "lever":
        sr = raw.get("salaryRange") or {}
        if not isinstance(sr, dict):
            return None
        lo, hi = _num(sr.get("min")), _num(sr.get("max"))
        interval = _LEVER_INTERVAL.get(sr.get("interval") or "")
        cur = (sr.get("currency") or "USD").upper()
        if not lo or not _plausible(interval, lo, hi, cur):
            return None
        return Pay(lo, hi, cur, interval, "unspecified", "structured",
                   f"salaryRange: {sr}"[:300], "high")
    if ats == "ashby":
        comp = raw.get("compensation") or {}
        comps = comp.get("summaryComponents") or [] if isinstance(comp, dict) else []
        for c in comps:
            if not isinstance(c, dict):
                continue
            ctype = str(c.get("compensationType") or "").replace(" ", "").lower()
            if _ASHBY_PAY_TYPES.get(ctype) is None:
                continue
            lo, hi = _num(c.get("minValue")), _num(c.get("maxValue"))
            interval = _ASHBY_INTERVAL.get(str(c.get("interval") or "").upper())
            if not lo or not _plausible(interval, lo, hi, (c.get("currencyCode") or "USD").upper()):
                continue
            return Pay(lo, hi, (c.get("currencyCode") or "USD").upper(), interval, "base", "structured",
                       f"summaryComponents: {c}"[:300], "high")
        return None
    return None


# ---------------------------------------------------------------------------
# text
# ---------------------------------------------------------------------------

_TAG = re.compile(r"<[^>]+>")
_WS = re.compile(r"\s+")

_CUR = r"(?P<{n}>US\$|USD\s?\$|CA\$|C\$|CAD\s?\$|A\$|AU\$|\$|£|€)"
_NUM = r"(?P<{n}>\d{{1,3}}(?:,\d{{3}})+(?:\.\d{{1,2}})?|\d+(?:\.\d{{1,2}})?)"
_SUF = r"(?P<{n}>\s?(?:k|K)\b|\s?(?:mm|MM|M|million|mil|bn|B|billion)\b)?"
_SEP = r"\s*(?:-|–|—|to|and)\s*"
_CODE = r"(?:\s?(?P<code>USD|CAD|GBP|EUR|AUD|SGD|NZD|INR|JPY|CHF|SEK|NOK|DKK|MXN|BRL|HKD|ILS|PLN|ZAR)\b)?"

_RANGE = re.compile(
    _CUR.format(n="c1") + r"\s?" + _NUM.format(n="n1") + _SUF.format(n="s1")
    + r"(?:" + _SEP + r"(?:" + _CUR.format(n="c2") + r"\s?)?" + _NUM.format(n="n2") + _SUF.format(n="s2") + r")?"
    + _CODE
)
# "90,000 - 110,000 USD": no leading symbol, a trailing ISO code (ranges only)
_CODED = re.compile(
    _NUM.format(n="n1") + _SUF.format(n="s1") + _SEP + _NUM.format(n="n2") + _SUF.format(n="s2")
    + r"\s?(?P<code>USD|CAD|GBP|EUR|AUD|SGD|NZD|INR|JPY|CHF|SEK|NOK|DKK|MXN|BRL|HKD|ILS|PLN|ZAR)\b"
)

_CUR_MAP = {"US$": "USD", "USD$": "USD", "USD $": "USD", "$": "USD", "CA$": "CAD", "C$": "CAD",
            "CAD$": "CAD", "CAD $": "CAD", "A$": "AUD", "AU$": "AUD", "£": "GBP", "€": "EUR"}

_INTERVAL_AFTER = re.compile(
    r"^\s*(?:(?:USD|CAD|GBP|EUR|AUD|SGD|NZD|INR|JPY|CHF|SEK|NOK|DKK|MXN|BRL|HKD|ILS|PLN|ZAR)\s*)?(?:/|per\s+|an?\s+|each\s+)?\s*"
    r"(?P<i>year|yr|annum|annually|annual|hour|hr|hourly|month|mo|monthly|week|wk|weekly|day|daily)\b",
    re.I)
_INTERVAL_WORD = {"year": "year", "yr": "year", "annum": "year", "annually": "year", "annual": "year",
                  "hour": "hour", "hr": "hour", "hourly": "hour", "month": "month", "mo": "month",
                  "monthly": "month", "week": "week", "wk": "week", "weekly": "week", "day": "day",
                  "daily": "day"}

_CUE = re.compile(r"\b(pay|paid|salary|salaries|compensation|wages?|rate|OTE|on[- ]target|earnings|"
                  r"hourly|base|annually|per annum|yearly)\b", re.I)
_HOURLY_CUE = re.compile(r"\b(hourly|per hour|an hour)\b", re.I)
_ANNUAL_CUE = re.compile(r"\b(annual (base )?salary|annual(ly)?|per year|per annum|yearly)\b", re.I)
_VARIABLE = re.compile(r"\b(variable|commissions?|incentive)\b", re.I)
_SALARY = re.compile(r"\b(salary|base|hourly|wages?|OTE|on[- ]target|earnings)\b", re.I)
_OTE = re.compile(r"\bOTE\b|on[- ]target", re.I)
_BASE = re.compile(r"\bbase\b", re.I)

# disqualifiers right before the amount (<= ~45 chars, same clause)
_NEG_BEFORE = re.compile(
    r"\b(reimburse\w*|stipend|budgets?|revenue|funding|raised|valuation|series [a-h]|bonus|"
    r"allowance|discount|tuition|P&L|GMV|ARR|saved|savings|grant|donat\w*|equity)\b[^.;:!?$,()]*$", re.I)
# pay_v2: a salary cue AFTER the disqualifier, still before the amount, wins
# ("Monthly Stipend The salary for this role is $50,000-$80,000")
_SALARY_AFTER_NEG = re.compile(r"\b(salary|base|pay range|pay rate|compensation|wages?|hourly rate)\b", re.I)
# pay_v2: what follows the amount is an ADD-ON to it, not what it is ("+ Bonus", "plus commission")
_ADDON_AFTER = re.compile(r"^\s*(?:\(\s*)?(?:\+|plus\b|and\b|including\b|incl\b|with\b)", re.I)
# pay_v2: the amount itself is variable / commission / incentive ("+ $11,500 Variable")
_VARIABLE_AFTER = re.compile(r"^\s*(?:USD\s*)?(?:in\s+)?(?:annual\s+|target\s+)?(variable(?!\s+based)|commissions?|incentive)",
                             re.I)
# disqualifiers right after the amount (<= 3 words; "plus bonus" / "and equity" do not count)
_NEG_AFTER = re.compile(
    r"^[^.;!?]{0,6}?(?:(?!plus\b|and\b|\+)\w+\s+){0,2}(?:sign[- ]?on |signing |relocation |referral )?"
    r"(bonus|stipend|reimburse\w*|allowance|budget|credit|discount|in (?:annual )?(?:revenue|sales|payments|"
    r"funding|arr|gmv)|of funding|series [a-h]\b|valuation|grant)",
    re.I)


def clean(text: Optional[str]) -> str:
    """HTML (possibly entity-escaped twice, as Greenhouse sends it) -> plain text."""
    if not text:
        return ""
    t = html.unescape(html.unescape(text))
    t = _TAG.sub(" ", t)
    return _WS.sub(" ", html.unescape(t)).strip()


def _value(num: str, suf: Optional[str]) -> Optional[float]:
    v = float(num.replace(",", ""))
    s = (suf or "").strip().lower()
    if s in ("mm", "m", "million", "mil", "bn", "b", "billion"):
        return None                                  # never a salary
    if s == "k":
        v *= 1000
    return _num(v)


# a bare "$" in the amount's own clause next to a country marker is that country's dollar
_DOLLAR_COUNTRY = (
    (re.compile(r"\b(CAN|Canada|Canadian|Toronto|Vancouver|Montreal)\b"), "CAD"),
    (re.compile(r"\b(AUS|Australia|Australian|Sydney|Melbourne)\b"), "AUD"),
    (re.compile(r"\b(Singapore|SGP)\b"), "SGD"),
    (re.compile(r"\b(New Zealand|NZ)\b"), "NZD"),
    (re.compile(r"\b(Mexico|MEX)\b"), "MXN"),
    (re.compile(r"\b(Hong Kong)\b"), "HKD"),
)


def _currency(m: "re.Match", clause: str = "") -> str:
    code = m.groupdict().get("code")
    if code:
        return code.upper()
    c1 = (m.groupdict().get("c1") or "$").upper()
    if c1 == "$":
        for rx, cur in _DOLLAR_COUNTRY:
            if rx.search(clause):
                return cur
    return _CUR_MAP.get(c1, _CUR_MAP.get(c1.replace(" ", ""), "USD"))


def _candidates(text: str) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    spans = []
    matches = []
    for rx in (_RANGE, _CODED):
        for m in rx.finditer(text):
            if any(a <= m.start() < b for a, b in spans):
                continue
            spans.append((m.start(), m.end()))
            matches.append(m)
    matches.sort(key=lambda m: m.start())
    last_end = None
    for m in matches:
        # a range listed right after an accepted one ("Zone A: $X - $Y; Zone B: ...") shares its cue
        chained = last_end is not None and 0 <= m.start() - last_end <= 40
        c = _judge(text, m, chained)
        if c:
            if chained:
                c["kind"] = out[-1]["kind"]      # a zone list shares the kind of its first range
            out.append(c)
            last_end = m.end()
    return out


def _judge(text: str, m: "re.Match", chained: bool = False) -> Optional[Dict[str, Any]]:
    g = m.groupdict()
    s1, s2 = g.get("s1"), g.get("s2")
    if g.get("n2") and not s1 and (s2 or "").strip().lower() == "k" and float(g["n1"].replace(",", "")) < 1000:
        s1 = "k"                                     # "$120-150K": the 'k' covers both ends
    lo = _value(g["n1"], s1)
    if lo is None:
        return None
    hi = None
    if g.get("n2"):
        hi = _value(g["n2"], s2)
        if hi is None:
            return None
        if g.get("c2") is None and not (0.5 * lo <= hi <= 5 * lo):
            return None                              # "$90,000 and 2 years" is not a range
    if g.get("n2") and hi is not None and hi > MAX_RANGE_RATIO * lo:
        return None                                  # "$100k - $500k for all engineers"
    before = text[max(0, m.start() - 160):m.start()]
    after = text[m.end():m.end() + 40]
    near_before = before[-45:]
    if re.search(r"\+\s*$", before[-4:]) or _VARIABLE_AFTER.search(after):
        return None                                  # an add-on amount ("+ $11,500 Variable")
    neg = _NEG_BEFORE.search(near_before)
    if neg and not _SALARY_AFTER_NEG.search(near_before[neg.end(1):]):
        return None
    if not _ADDON_AFTER.search(after) and _NEG_AFTER.search(after):
        return None
    # the amount's own sentence names variable / commission / incentive pay and no salary or base:
    # it is not base pay ("Target Variable range for this role is: $X - $Y")
    sentence = re.split(r"[.!?]\s|\d", before)[-1]      # since the last sentence end or amount
    if _VARIABLE.search(sentence) and not _SALARY.search(sentence):
        return None
    cue_window = before[-120:] + " " + after[:25]
    if not chained and not _CUE.search(cue_window):
        return None
    explicit = None
    mi = _INTERVAL_AFTER.search(after)
    if mi:
        explicit = _INTERVAL_WORD[mi.group("i").lower()]
    elif _HOURLY_CUE.search(cue_window):
        explicit = "hour"
    elif _ANNUAL_CUE.search(cue_window):
        explicit = "year"
    currency = _currency(m, sentence)
    interval = explicit or _interval_by_magnitude(lo, currency)
    if interval is None and "." in g["n1"] and (not g.get("n2") or "." in g["n2"]) and \
            _plausible("hour", lo, hi, currency):
        interval = "hour"                            # "$33.17 - $44.39 USD": cents in the hourly band
    if not _plausible(interval, lo, hi, currency):
        return None
    # a floor ("starts at $25") only for a single amount; "ranges from $X to $Y" is a range
    starts_at = g.get("n2") is None and (
        bool(re.search(r"\b(starts?|starting) (at|from)\s*$", before[-25:], re.I))
        or bool(re.search(r"\b(from|minimum of|at least)\s*$", before[-20:], re.I)))
    if hi is None and not starts_at:
        hi = lo
    kind = "ote" if _OTE.search(before[-80:] + " " + after[:30]) else (
        "base" if _BASE.search(before[-120:]) else "unspecified")
    start = max(0, m.start() - 80)
    snippet = text[start:min(len(text), m.end() + 30)].strip()
    return {"min": lo, "max": hi, "currency": currency, "interval": interval, "kind": kind,
            "explicit": explicit is not None, "range": g.get("n2") is not None, "starts_at": starts_at,
            "snippet": snippet, "pos": m.start()}


def parse_text(text: Optional[str]) -> Optional[Pay]:
    t = clean(text)
    if not t:
        return None
    cands = _candidates(t)
    if not cands:
        return None
    cands.sort(key=lambda c: c["pos"])
    # the first explicit candidate leads; else the first one
    lead = next((c for c in cands if c["explicit"]), cands[0])
    # never mix base with OTE (or anything else): same interval, currency and kind only
    group = [c for c in cands if c["interval"] == lead["interval"] and c["currency"] == lead["currency"]
             and c["kind"] == lead["kind"]]
    distinct = {(c["min"], c["max"]) for c in group}
    lo = min(c["min"] for c in group)
    highs = [c["max"] for c in group if c["max"] is not None]
    hi = max(highs) if highs else None
    if lead["starts_at"] and len(distinct) == 1:
        hi = None
    confidence = "high" if (lead["explicit"] and len(distinct) == 1 and not lead["starts_at"]) else "medium"
    snippet = lead["snippet"]
    if len(distinct) > 1:
        snippet = f"{snippet} [+{len(distinct) - 1} more range(s)]"
    return Pay(lo, hi, lead["currency"], lead["interval"], lead["kind"], "text", snippet[:400], confidence)


def best(ats: str, raw: Dict[str, Any], text: Optional[str]) -> Optional[Pay]:
    return from_structured(ats, raw) or parse_text(text)


