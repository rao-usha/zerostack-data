"""
Gated shared keys (SPEC_150) -- the pure same-legal-person rules.

An EIN or a CRD shared by two EDGAR filers (CIKs) says they are RELATED: a parent
and its subsidiary, an issuer and an insider who filed under its EIN, an insurer and
its separate accounts, a sponsor and its ESOP, an adviser and a second firm filing a
13F on its cover-page CRD. It does not say they are one legal person. So in
`resolve_core.plan()` an EIN or CRD (and a CIK<->CRD bridge edge) joins two CIK
groups only when one of the corroborations here holds for that pair.

Everything is pure and DATA-RELATIVE: no rule reads the clock or the run date, so
the same data gives the same decisions on any day (review 2026-10-01: a run-date
dormancy test would have merged two live entities three months later with no new
data). The filer profile of a CIK (current and former names with their dates, SIC,
the recent-filing window and its size, insider / Form D / 13F / 10-K counts) is
loaded by `resolve.py` from NexData's SEC tables; without one the rules see only the
record's name and never call the two CIKs sequential.

MEASURED 2026-10-01 (471 live multi-CIK entities, 1,368 CIKs): gate_v1 kept 30
whole; gate_v2 (review fixes: V4 name adopted, data-relative sequential, heavy-filer
first date, A2 hints) keeps 27 whole (VEEA, Hadron Energy and BGC split as V4),
every other decision unchanged; multi-CIK entities 471 -> 28. The windows below are
the ones the owner approved with the gate_v1 measurement.
"""

import re
from datetime import date

from app.entities import norm

GATE_VERSION = "gate_v2"

DORMANT_DAYS = 120       # the old CIK filed nothing for this long while the new one kept filing
SUCC_GAP_DAYS = 200      # the new CIK's first filing is at most this long after the old's last
SUCC_OVERLAP_DAYS = 31   # tolerated overlap (a late amendment on the old CIK)
A3_OVERLAP_DAYS = 180    # an equal base name with conflicting forms overlapping less than this
RECENT_WINDOW = 1000     # EDGAR's "recent" filings list holds at most this many filings

# EDGAR conformed-name tail tags: '/ADV', '/NY/', '\DE\', '/MA/', '/BD', '/GA'
_TAG = re.compile(r"\s*[/\\][A-Z0-9.&]{1,6}[/\\]?\s*$", re.I)
_APOS = re.compile(r"['’`]")

FORMS = {"inc": "corp", "incorporated": "corp", "corp": "corp", "corporation": "corp",
         "llc": "llc", "lc": "llc", "lp": "lp", "llp": "llp", "lllp": "lllp", "ltd": "ltd",
         "limited": "ltd", "plc": "plc", "sa": "sa", "ag": "ag", "gmbh": "gmbh", "pc": "pc",
         "pllc": "pllc", "na": "na", "nv": "nv", "bv": "bv"}

_ROMAN = {"i": "1", "ii": "2", "iii": "3", "iv": "4", "v": "5", "vi": "6", "vii": "7",
          "viii": "8", "ix": "9", "x": "10"}

# A vehicle: a fund, series, account, plan or GP -- it joins only on its own name.
_VEH = re.compile(
    r"\b(FUNDS?|SERIES|SEPARATE ACCOUNT|ACCOUNT|VARIABLE|ANNUITY|SPV|CO-?INVEST(MENT)?|FEEDER|"
    r"MASTER|PARALLEL|QP|DST|PLAN|ESOP|EMPLOYEE STOCK|401\(?K\)?|PORTFOLIO|FUNDING|TRUST \d{4}|"
    r"\d{4}-[A-Z0-9]+|SPC|SICAV|SCSP|RAIF|AGGREGATOR|OPPORTUNITY ZONE|REIT|SEGREGATED|BLOCKER|"
    r"ROLLOVER|GP|GENPAR|STUDENT LOAN TRUST|RECEIVABLES|MORTGAGE TRUST|AUTO (LOAN|LEASE|RECEIVABLES))\b",
    re.I)
_FUND_MGMT = re.compile(r"\bFUNDS? (MANAGEMENT|ADVISORS?|ADVISERS?|MANAGERS?)\b", re.I)
# a numbered entity name ('... II, L.P.', '... 2024-1') with no SIC and no 13F
_NUMV = re.compile(r"\b([IVX]{1,5}|\d{1,3})(-[A-Z])?\b,?\s*(L\.?P\.?|LLC|L\.?L\.?C\.?|LTD|LLLP)?\s*$", re.I)
# any of these makes a name an organization's, never a person's
_ORG = re.compile(
    r"\b(LLC|L\.L\.C|LP|L\.P|INC|CORP|CO|COMPANY|FUND|TRUST|MANAGEMENT|CAPITAL|PARTNERS|ADVISORS?|"
    r"ADVISERS?|GROUP|HOLDINGS?|BANK|ASSOCIATES|LTD|LIMITED|INVEST\w*|FINANCIAL|SECURITIES|ASSET|"
    r"VENTURES?|FOUNDATION|ESTATE|PLAN|ACCOUNT|SERIES|N\.A|PLC|AG|SA|GMBH|SOCIETY|ASSOCIATION|"
    r"UNIVERSITY|INSURANCE|GLOBAL|INTERNATIONAL|SYSTEMS|ENTERPRISES|INDUSTRIES|CORPORATION|"
    r"INCORPORATED|SOLUTIONS|SERVICES|LABS?|WEALTH|PRIVATE|FAMILY|OFFICE|STRATEG\w*|RESEARCH|"
    r"MEDICAL|ENERGY|PROPERTIES|REALTY|BANCORP|VENTURE|DIGITAL|TECH\w*|HOLDCO|JV|PROJECT|DEVELOPMENT)\b",
    re.I)

# R3: the extra tokens of a CRD-sharing name prefix must be a strategy label, not
# structure ('GP', 'Holdings', 'Management') or a jurisdiction ('UK', 'DIFC').
STRUCT = {
    "gp", "genpar", "general", "partner", "holdings", "holding", "group", "management", "manager",
    "advisors", "advisers", "advisory", "capital", "partners", "fund", "funds", "bank", "trust",
    "securities", "financial", "investments", "investment", "international", "global", "usa", "us",
    "opportunities", "associates", "inc", "llc", "lp", "corp", "co", "ltd", "sponsor", "development",
    "asset", "ventures", "venture", "opportunity", "strategic", "credit", "equity", "life", "insurance",
    "na", "national", "association", "company", "plc", "series", "account", "master", "feeder",
    "offshore", "onshore", "parallel", "qp", "spv", "reit", "op", "operating", "bancorp", "bancshares",
    "services", "uk", "cayman", "europe", "jersey", "hong", "kong", "canada", "japan", "lux",
    "singapore", "australia", "difc", "dubai", "asia", "hk", "ireland", "india", "china", "germany",
    "france", "switzerland", "america", "americas", "north", "south", "east", "west", "pte", "pty",
    "kk", "sarl", "gmbh", "ag",
}


# ---------------------------------------------------------------------------
# names
# ---------------------------------------------------------------------------

def _prep(name):
    s = name
    for _ in range(3):
        s2 = _TAG.sub("", s)
        if s2 == s or not s2.strip():
            break
        s = s2
    s = _APOS.sub("", s)
    f = norm._fold(s)
    m = norm._DBA_MARKER_RE.search(f)
    if m:
        f = f[:m.start()].strip() or f
    txt = " " + f + " "
    txt = txt.replace(" limited partnership ", " lp ").replace(" limited liability company ", " llc ")
    for x, y in ((" l l l p ", " lllp "), (" l l c ", " llc "), (" l l p ", " llp "), (" l p ", " lp "),
                 (" s a ", " sa "), (" n a ", " na ")):
        txt = txt.replace(x, y)
    return txt.split()


def enorm(name):
    """EDGAR-aware name: tail tags and apostrophes stripped, legal-form suffixes
    stripped at the tail, every designator token (letters, numerals) kept."""
    if not name:
        return None
    return " ".join(norm._strip_suffixes(_prep(name))) or None


def legal_form(name):
    """The legal-form class of the name's tail suffix, or None when it has none."""
    if not name:
        return None
    t = _prep(name)
    return FORMS.get(t[-1]) if t else None


def name_keys(n, vehicle=False):
    """Equality forms of a normalized name: exact (Roman numerals as digits), space-free,
    and -- for non-vehicles only, where reordering cannot hide a designator -- the
    sorted token set (minus 'and' / 'of' / 'a')."""
    if not n:
        return set()
    n = " ".join(_ROMAN.get(t, t) for t in n.split())
    out = {("n", n), ("c", n.replace(" ", ""))}
    if not vehicle:
        out.add(("t", " ".join(sorted(set(n.split()) - {"and", "of", "a"}))))
    return out


# ---------------------------------------------------------------------------
# filer features
# ---------------------------------------------------------------------------

def _d(v):
    if v is None or v == "":
        return None
    if isinstance(v, date):
        return v if type(v) is date else v.date()
    return date.fromisoformat(str(v)[:10])


def _former(p):
    """[(name, from, to)] -- a former name is a plain string or a dated dict
    ({name, from_date, to_date}, as `resolve._load_profiles` reads it)."""
    out = []
    for x in p.get("former_names") or []:
        if isinstance(x, dict):
            out.append((x.get("name"), _d(x.get("from_date")), _d(x.get("to_date"))))
        else:
            out.append((x, None, None))
    return [x for x in out if x[0]]


def _first_filed(p, former):
    """(first filing, floor). EDGAR's recent list holds RECENT_WINDOW filings, so for a
    heavy filer its earliest date is only a floor: the earliest former-name date is used
    when it is older, else the first filing is unknown (None) -- never 'recent'."""
    first = _d(p.get("first_filed"))
    if first is None or int(p.get("recent_filing_count") or 0) < RECENT_WINDOW:
        return first, first
    older = [f for _n, f, _t in former if f and f < first]
    return (min(older) if older else None), first


def features(cik, profile, fallback_name=None):
    """-> the feature dict the rules read, for one CIK. `profile` may be None."""
    p = profile or {}
    nm = p.get("name") or fallback_name or ""
    former = _former(p)
    names = [nm] + [n for n, _f, _t in former]
    formd = int(p.get("formd_filings") or 0)
    f13 = int(p.get("f13_filings") or 0)
    k10 = int(p.get("k10_filings") or 0)
    alpha = re.findall(r"[A-Za-z]+", nm)
    person = bool((not _ORG.search(nm)) and not re.search(r"\d", nm) and 2 <= len(alpha) <= 4
                  and (p.get("insider_owner") or int(p.get("owner_filings") or 0) or f13)
                  and not p.get("insider_issuer") and not formd and not p.get("formd_pooled")
                  and not k10 and not p.get("tickers"))
    nm_v = _FUND_MGMT.sub("", nm)
    vehicle = bool(_VEH.search(nm_v) or p.get("formd_pooled") or p.get("sic") == "6189"
                   or (_NUMV.search(nm) and not p.get("sic") and not f13))
    cur = enorm(nm)
    curkeys = name_keys(cur, vehicle)
    fkeys = {k for x in names[1:] for k in name_keys(enorm(x), vehicle)}
    # renamed INTO the current name: when the last differing former name ended
    renamed_at = max((t for n, _f, t in former
                      if t and not (name_keys(enorm(n), vehicle) & curkeys)), default=None)
    first, floor = _first_filed(p, former)
    return {
        "cik": cik, "name": nm, "cur": cur, "form": legal_form(nm),
        "curkeys": curkeys, "fkeys": fkeys, "allkeys": curkeys | fkeys,
        "renamed_at": renamed_at,
        "dform": (p.get("latest_form") or "").startswith("D") and not f13 and not k10,
        "person": person, "vehicle": vehicle, "sic": p.get("sic"), "k10": k10,
        "first": first, "first_floor": floor, "last": _d(p.get("last_filed")),
    }


def operating(f):
    return not f["vehicle"] and not f["person"]


# ---------------------------------------------------------------------------
# pair rules
# ---------------------------------------------------------------------------

def sequential(a, b):
    """One CIK went dormant, then the other started within the gap (either order)
    -> (old cik, new cik) or None. DATA-RELATIVE: the old CIK filed nothing for
    DORMANT_DAYS while the new one kept filing (the new CIK's last filing is at
    least that long after the old one's), so the answer changes only with new
    filings, never with the run date. Needs both filing windows."""
    for x, y in ((a, b), (b, a)):
        xl, yf, yl = x["last"], y["first"], y["last"]
        if (xl and yf and yl and (yl - xl).days >= DORMANT_DAYS
                and (yf - xl).days <= SUCC_GAP_DAYS and (xl - yf).days <= SUCC_OVERLAP_DAYS):
            return x["cik"], y["cik"]
    return None


def overlap_days(a, b):
    if not (a["first"] and a["last"] and b["first"] and b["last"]):
        return None
    return (min(a["last"], b["last"]) - max(a["first"], b["first"])).days


def name_adopted(x, y):
    """`x` took `y`'s name while `y` was filing (a de-SPAC: Plum Acquisition became
    VEEA INC. after the private Veea Inc. had filed Form Ds; GigCapital7 became Hadron
    Energy): x was renamed INTO the shared name, y filed under it before that rename,
    their filing windows overlap, and y never carried x's old names (one firm
    renaming two of its accounts together is not this)."""
    r = x["renamed_at"]
    if not r or not y["first"] or y["first"] >= r or (y["fkeys"] & x["fkeys"]):
        return False
    xf = x["first"] or x["first_floor"]
    return bool(xf and x["last"] and y["last"] and y["last"] >= xf and y["first"] <= x["last"])


def corroborate(a, b, share_crd):
    """Same legal person? -> (rule, None) to join, or (None, refusal reason).
    Checked in order; the vetoes first."""
    if a["person"] or b["person"]:
        if a["person"] and b["person"] and a["curkeys"] & b["curkeys"]:
            return "R1_name_equal", None
        return None, "V1_person"                     # a person never joins an organization
    if a["curkeys"] & b["curkeys"]:
        if name_adopted(a, b) or name_adopted(b, a):
            return None, "V4_name_adopted_concurrent"  # an acquirer took the target's name
        if a["form"] is None or b["form"] is None or a["form"] == b["form"]:
            return "R1_name_equal", None             # incl. '/ADV', '/NY/' tag-only differences
        if sequential(a, b):
            return "R1c_form_conversion", None       # LP -> LLP, LLC -> Inc, one after the other
        return None, "V2_form_conflict"              # Ltd and LP side by side: two persons
    former = bool(a["allkeys"] & b["allkeys"])
    if former and sequential(a, b):
        return "R2_former_name_succession", None
    if a["vehicle"] or b["vehicle"]:
        return None, "V3_vehicle"                    # vehicles join only on R1 / R1c / R2
    if share_crd:
        if a["cur"] and b["cur"]:
            x, y = sorted([a["cur"].split(), b["cur"].split()], key=len)
            if len(x) >= 2 and y[:len(x)] == x:
                rest = y[len(x):]
                if rest and not any(t in STRUCT or _designator(t) for t in rest):
                    return "R3_crd_name_prefix", None   # one adviser, a strategy-labelled 13F CIK
        if sequential(a, b):
            return "R4_crd_succession", None         # the old and new CIK of one adviser
    # a former-name match while both file is the holding-company reorg pattern
    return None, ("R2_concurrent" if former else "no_corroboration")


def _designator(t):
    return bool(re.fullmatch(r"[ivx]{1,5}|\d+|[a-z]", t))


def ambiguity(a, b, share_crd, share_ein, crd_hint=False):
    """A pair the rules split that still carries a same-person hint -> flag or None.
    Review evidence only: a flag never merges anything. `crd_hint`: both CIKs name
    one CRD somewhere (a record, or a 13F bridge edge the resolver does not union on)."""
    if a["person"] or b["person"]:
        return None
    if a["curkeys"] & b["curkeys"]:
        if name_adopted(a, b) or name_adopted(b, a):
            return "A6_name_adopted_concurrent"      # V4: equal names, two companies
        # equal base name, legal forms conflict
        ov = overlap_days(a, b)
        if ov is None or ov < A3_OVERLAP_DAYS:
            return "A3_form_conflict_short_overlap"
        return None
    if a["allkeys"] & b["allkeys"] and not (a["sic"] or b["sic"] or a["k10"] or b["k10"]):
        return "A5_former_name_concurrent_nonissuer"
    if a["vehicle"] or b["vehicle"]:
        return None
    if share_ein and not share_crd and sequential(a, b) and not (a["dform"] and b["dform"]):
        return "A1_ein_sequential_unnamed"
    if share_ein and (share_crd or crd_hint):        # the strongest same-firm hint the rules split
        if not (set((a["cur"] or "").split()) & set((b["cur"] or "").split())):
            return "A4_ein_and_crd_unrelated_names"
        return "A2_ein_and_crd_related_names"
    return None


def record_name_keys(legal_name):
    """Equality forms of a no-CIK record's name (both the vehicle and non-vehicle forms)."""
    n = enorm(legal_name)
    return name_keys(n) | name_keys(n, True)
