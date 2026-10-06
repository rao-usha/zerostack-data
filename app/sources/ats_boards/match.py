"""
Firm <-> board matching for target discovery (SPEC_155). Pure: no I/O.

Rule R7, measured on the 2026-10-05 pilot (300 kept PE targets, 48 board hits hand-checked:
20 true, 21 false, 7 empty; R7 accepted 17 true and 0 false, in-sample):

- the board has at least one posting, and
- (a) the firm's FULL name matches -- the Greenhouse board name equals one of its names, or a
  name appears in >= 50% of postings, or the board's intro text names it -- and either the match
  was the board name, or the name is not a one-word name under 6 characters, or the firm's city
  / state appears in some posting; or
- (b) a CANONICAL name (leading "the" / "ai" and generic tail words such as data / ai / labs /
  holdings dropped) matches the same three ways AND the firm's city / state appears in some
  posting.

What separated true from false links in the pilot was name PLUS location: suffix stripping
alone let in 9 false links (Parametrix, Lattice, Manifest, Beam, ...). Domain agreement does not
help (the token comes from the name, so the domain "agrees" either way) and is not used.

Rule R8 (SPEC_156, the 2026-10-06 review of run 1: 6 confirmed false links of 291 -- Verve, Axios,
Socket, One Medical, Orchestra, Metabase -- precision 92% on 50 hand-checked links) adds:

- names are normalized by ``mnorm``, which never drops a token ("Metabase Q" stays "metabase q",
  "TIFIN AG Inc." stays "tifin ag"); ``norm.norm`` merged distinct names;
- an AMBIGUOUS one-word name (document frequency >= 3 in NexData's own name corpus, or unknown)
  matched by name only -- even an exact board name -- needs a location hit or a Form D person in
  the postings (Verve, Socket, Motive);
- a match on a previous name ONLY (Form D / EDGAR previous names) needs the same (inKind);
- a stored city that is a state code ("Ny") is not a city;
- a canonical match that dropped a descriptive word and whose canonical name is one ambiguous token
  needs the full name in some posting, a Form D person, or the firm's city in >= 10% of postings
  (Axios HQ in Arlington vs Axios Media, also in Arlington).

Everything else is a CANDIDATE (some name signal: an empty board, or a name without location;
never linked to a firm) or REJECTED (no name signal). The score is a ranking aid for the reviewer,
not a probability.
"""

from __future__ import annotations

import re
from collections import Counter
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

from app.entities import domains, norm
from app.sources.ats_boards import adapters, discover, pay

RULE_VERSION = "R8-2026-10-06"    # SPEC_156: ambiguous / previous names need corroboration; mnorm
MIN_SHARE = 0.5
SHORT_LEN = 6                   # a one-word canonical name shorter than this is "short"
MAX_ALIASES = 2                 # alias / previous-name variants per site
AMBIGUOUS_DF = 3                # a one-word name in >= 3 corpus names is shared with other firms
AMBIGUOUS_CITY_SHARE = 0.10     # an ambiguous canonical match needs the firm's city in >= 10% of postings

# legal forms stripped from the tail by mnorm; after one is stripped, a FOREIGN form is part of the
# name ("TIFIN AG Inc." -> "tifin ag"), a domestic chain is not ("Foo Co Inc" -> "foo")
_LEGAL_TAIL = set(norm.SUFFIX_TOKENS) - {"the"}
_FOREIGN_FORMS = {"ag", "nv", "bv", "sa", "sas", "srl", "spa", "oy", "ab", "as", "aps", "kk", "gmbh", "plc",
                  "ltda", "sarl", "ulc"}
_LEGAL_PHRASES = ("co ltd", "co inc", "pty ltd", "pte ltd", "s a de c v", "sa de cv", "s de r l de c v",
                  "s de r l")

# pilot evalrules.TAIL: trailing words canonical names drop ("Rockfish Data" -> "rockfish")
CANON_TAIL = {"data", "ai", "labs", "lab", "technologies", "technology", "tech", "software", "solutions",
              "systems", "group", "holdings", "hq", "app", "apps", "platform", "health", "studio", "studios",
              "companies", "company", "global", "io", "com", "co", "network", "networks", "media", "topco"}
_DROP_ANYWHERE = {"the", "com", "io"}
# previous / alias names that are a fund vehicle, not the company
_FUNDISH = re.compile(r"\b(seed|angels|spv|fund|series [a-z]|lp|investors?)\b")
_PLACEHOLDER = {"none", "null", "n a", "na", "unknown", "tbd"}

STATES = {"AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas", "CA": "California",
          "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware", "FL": "Florida", "GA": "Georgia",
          "HI": "Hawaii", "ID": "Idaho", "IL": "Illinois", "IN": "Indiana", "IA": "Iowa", "KS": "Kansas",
          "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine", "MD": "Maryland", "MA": "Massachusetts",
          "MI": "Michigan", "MN": "Minnesota", "MS": "Mississippi", "MO": "Missouri", "MT": "Montana",
          "NE": "Nebraska", "NV": "Nevada", "NH": "New Hampshire", "NJ": "New Jersey", "NM": "New Mexico",
          "NY": "New York", "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio", "OK": "Oklahoma",
          "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island", "SC": "South Carolina",
          "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas", "UT": "Utah", "VT": "Vermont",
          "VA": "Virginia", "WA": "Washington", "WV": "West Virginia", "WI": "Wisconsin", "WY": "Wyoming",
          "DC": "District of Columbia"}
_STATE_NAMES = {v.lower() for v in STATES.values()}
# abbreviations that are also English words: only after a comma ("Portland, OR")
_WORDY_ABBR = {"IN", "OR", "ME", "OK", "HI", "ID"}

_DOMAIN_TEXT = re.compile(r"\b(?:https?://)?(?:www\.)?([a-z0-9-]+(?:\.[a-z0-9-]+)*\."
                          r"(?:com|io|ai|co|net|org|app|health|tech|us|so|dev|to))\b", re.I)


@dataclass
class Target:
    target_id: int
    name: str
    city: Optional[str] = None
    state: Optional[str] = None
    cik: Optional[str] = None
    ein: Optional[str] = None
    core_entity_id: Optional[int] = None
    archetype: Optional[str] = None
    aliases: List[str] = field(default_factory=list)          # core canonical name + core.alias
    previous_names: List[str] = field(default_factory=list)   # Form D issuer / EDGAR previous names
    persons: List[str] = field(default_factory=list)          # Form D related persons (count only)
    # SPEC_156: corpus document frequency of the firm's one-word names (None = unknown -> ambiguous)
    name_df: Optional[Dict[str, int]] = None


@dataclass
class Verdict:
    verdict: str                     # verified | candidate | rejected
    reason: str
    rule: Optional[str]
    score: float
    evidence: Dict[str, Any]


# ---------------------------------------------------------------------------
# names
# ---------------------------------------------------------------------------


def mnorm(name: Optional[str]) -> Optional[str]:
    """A company name for MATCHING: ``norm.norm`` without merging distinct names. Folds, cuts a DBA,
    strips legal forms from the tail (a foreign form after a stripped one stays: "TIFIN AG Inc." ->
    "tifin ag") and a leading "the", and keeps every other token, single letters included
    ("Metabase Q, Inc." -> "metabase q"; norm.norm gives "metabase")."""
    folded = norm._fold(name)
    if not folded:
        return None
    m = norm._DBA_MARKER_RE.search(folded)
    if m:
        folded = folded[:m.start()].strip() or folded[m.end():].strip()
    for phrase in _LEGAL_PHRASES:
        if folded.endswith(" " + phrase) and folded[:-(len(phrase) + 1)].strip():
            folded = folded[:-(len(phrase) + 1)].strip()
            break
    toks = folded.split()
    stripped = 0
    while len(toks) > 1 and toks[-1] in _LEGAL_TAIL:
        if stripped and toks[-1] in _FOREIGN_FORMS:
            break
        toks.pop()
        stripped += 1
    while len(toks) > 1 and toks[0] == "the":
        toks.pop(0)
    return " ".join(toks) or None


def canon(name: Optional[str]) -> str:
    toks = [t for t in (mnorm(name) or "").split() if t not in _DROP_ANYWHERE]
    while len(toks) > 1 and toks[0] == "ai":
        toks = toks[1:]
    while len(toks) > 1 and toks[-1] in CANON_TAIL:
        toks = toks[:-1]
    return " ".join(toks)


def canon_dropped(name: Optional[str]) -> List[str]:
    """Descriptive words canon() removed from a name (not the / com / io): "Luminate Health" -> ["health"]."""
    full = [t for t in (mnorm(name) or "").split() if t not in _DROP_ANYWHERE]
    kept = canon(name).split()
    return [t for t in full if t not in kept]


def _usable(raw: Any) -> bool:
    if raw is None:
        return False
    folded = norm._fold(str(raw))
    return bool(folded) and folded not in _PLACEHOLDER and not _FUNDISH.search(folded)


def _other_names(t: Target) -> List[str]:
    return [x for x in list(t.aliases) + list(t.previous_names) if _usable(x)]


def firm_names(t: Target) -> Tuple[List[str], List[str]]:
    raws = [t.name] + _other_names(t)
    names = sorted({n for n in (mnorm(x) for x in raws) if n})
    canon_names = sorted({c for c in (canon(x) for x in raws) if c})
    return names, canon_names


def previous_only_names(t: Target) -> Set[str]:
    """Names that come ONLY from Form D / EDGAR previous names (not the name or a current alias)."""
    raws = [t.name] + [a for a in t.aliases if _usable(a)]
    cur = {n for n in (mnorm(x) for x in raws) if n}
    # "Prismatic LLC" -> "Prismatic Software Inc.": the current name minus a descriptive word is current
    cur |= {c for c in (canon(x) for x in raws) if c}
    return {n for n in (mnorm(x) for x in t.previous_names if _usable(x)) if n} - cur


def name_tokens(t: Target) -> Set[str]:
    """The one-word names / canonical names whose corpus frequency R8 needs."""
    names, cn = firm_names(t)
    return {x for x in names + cn if x and " " not in x}


def _df(t: Target, name: str) -> Optional[int]:
    if t.name_df is None:
        return None
    return int(t.name_df.get(name, 0))


def _ambiguous(t: Target, hits: Sequence[str]) -> bool:
    """Every hit is one word shared by >= AMBIGUOUS_DF names in the corpus (unknown counts as shared)."""
    if not hits:
        return False
    for h in hits:
        if " " in h:
            return False
        d = _df(t, h)
        if d is not None and d < AMBIGUOUS_DF:
            return False
    return True


def variants(t: Target, ats: str) -> List[Tuple[str, str]]:
    """(token, kind) in try order. Greenhouse: joined, short | hyphen, inc, <= 2 alias, first_word.
    Lever: joined, short | hyphen, <= 2 alias (inc / first_word found no true Lever board)."""
    out: List[Tuple[str, str]] = []

    def add(s: str, kind: str) -> bool:
        s = re.sub(r"[^a-z0-9-]", "", (s or "").lower()).strip("-")
        if len(s) < 3 or any(s == x for x, _k in out):
            return False
        try:
            adapters._check_token(s)
        except ValueError:
            return False
        out.append((s, kind))
        return True

    n = norm.norm(t.name) or ""
    toks = n.split()
    if not toks:
        return out
    add("".join(toks), "joined")
    short = list(toks)
    while len(short) > 1 and short[-1] in discover._GENERIC_TAIL:
        short.pop()
    if short != toks:
        add("".join(short), "short")
    elif len(toks) > 1:
        add("-".join(toks), "hyphen")
    if ats == "greenhouse":
        add("".join(toks) + "inc", "inc")
    added = 0
    for raw in _other_names(t):
        if added >= MAX_ALIASES:
            break
        o = norm.norm(raw) or ""
        if o and o != n and add("".join(o.split()), "alias"):
            added += 1
    if ats == "greenhouse" and len(toks) > 1 and len(toks[0]) >= 4:
        add(toks[0], "first_word")
    return out


# ---------------------------------------------------------------------------
# location
# ---------------------------------------------------------------------------


def _state_hit(st: str, txt: str) -> bool:
    if st not in STATES:
        return False
    lead = r",\s*" if st in _WORDY_ABBR else r"(?:,\s*|\s|\(|^)"
    if re.search(lead + st + r"(?![A-Za-z])", txt):
        return True
    if st == "DC" and re.search(r"\bD\.\s?C\.", txt):
        return True
    low = txt.lower()
    name = STATES[st].lower()
    for m in re.finditer(r"(?<![a-z])" + re.escape(name) + r"(?![a-z])", low):
        before, after = low[:m.start()], low[m.end():]
        if st == "VA" and before.endswith("west "):
            continue
        if st == "WA" and re.match(r",?\s*d\.?\s?c\b", after):
            continue
        return True
    return False


def location_hits(t: Target, postings: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    city = norm._fold(t.city or "")
    st = (t.state or "").strip().upper()
    if len(city) <= 2 or city.upper() in STATES:
        city = ""                       # SPEC_156: a state code stored as the city ("Ny") is not a city
    # a city that is also a state name ("Washington" in DC) only counts beside a state hit
    ambiguous = city in _STATE_NAMES and STATES.get(st, "").lower() != city
    city_n = st_n = 0
    examples: List[str] = []
    for p in postings or []:
        locs = [p.get("location") or ""] + [x for x in (p.get("locations_all") or []) if isinstance(x, str)]
        txt = " | ".join(x for x in locs if x)
        sthit = bool(st) and _state_hit(st, txt)
        cityhit = bool(city) and f" {city} " in f" {norm._fold(txt)} " and (sthit or not ambiguous)
        city_n += cityhit
        st_n += sthit
        if (cityhit or sthit) and len(examples) < 3 and txt[:80] not in examples:
            examples.append(txt[:80])
    n = max(1, len(postings or []))
    return {"city_share": round(city_n / n, 4), "state_share": round(st_n / n, 4),
            "any": bool(city_n or st_n), "examples": examples}


# ---------------------------------------------------------------------------
# verdict
# ---------------------------------------------------------------------------


def _mentions(names: Sequence[str], text: str) -> bool:
    page = " " + norm._fold(text or "") + " "
    return any(len(n) >= 3 and f" {n} " in page for n in names)


def _hits(names: Sequence[str], board_norm: Optional[str], texts: Sequence[str], intro: str) -> List[str]:
    """The names that match by any of the three ways: board name, >= MIN_SHARE of postings, intro."""
    n = len(texts)
    out = []
    for nm in names:
        if (board_norm and nm == board_norm) or _mentions([nm], intro) or (
                n and sum(1 for x in texts if _mentions([nm], x)) / n >= MIN_SHARE):
            out.append(nm)
    return out


def _posting_text(p: Dict[str, Any]) -> str:
    return (p.get("description_text") or "") + " " + (p.get("title") or "")


def features(ats: str, t: Target, meta: Optional[Dict[str, Any]], postings: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    names, cn = firm_names(t)
    postings = list(postings or [])
    n = len(postings)
    board_name = (meta or {}).get("name") if ats == "greenhouse" else None
    bn = mnorm(board_name) if board_name else None
    intro = pay.clean((meta or {}).get("content")) if meta else ""
    texts = [_posting_text(p) for p in postings]
    share = sum(1 for x in texts if _mentions(names, x)) / n if n else 0.0
    cshare = sum(1 for x in texts if _mentions(cn, x)) / n if n else 0.0
    loc = location_hits(t, postings)
    people = [norm._fold(p) for p in t.persons or [] if len(norm._fold(p).split()) >= 2]
    page = " " + norm._fold(" ".join([intro] + texts)) + " " if people else ""
    full_hits = _hits(names, bn, texts, intro)
    canon_hits = _hits(cn, canon(board_name) if board_name else None, texts, intro)
    prev = previous_only_names(t)
    dropped = canon_dropped(t.name)
    return {
        "postings": n,
        "board_name": board_name,
        "board_name_exact": bool(bn) and bn in names,
        "board_name_canon": bool(board_name) and canon(board_name) in cn,
        "name_share": round(share, 4),
        "canon_share": round(cshare, 4),
        "intro_names_firm": _mentions(names, intro),
        "intro_names_canon": _mentions(cn, intro),
        "city_share": loc["city_share"],
        "state_share": loc["state_share"],
        "location": loc["any"],
        "location_examples": loc["examples"],
        "short_name": bool(cn) and all(" " not in c and len(c) < SHORT_LEN for c in cn),
        "canon_dropped_words": dropped,
        "persons_found": sum(1 for p in set(people) if f" {p} " in page),
        # SPEC_156 (R8): which names matched, how common they are, and what that implies
        "full_hits": full_hits,
        "canon_hits": canon_hits,
        "name_df": {h: _df(t, h) for h in sorted(set(full_hits + canon_hits)) if " " not in h},
        "ambiguous_full": _ambiguous(t, full_hits),
        "previous_only": bool(full_hits) and all(h in prev for h in full_hits),
        "ambiguous_canon": bool(dropped) and _ambiguous(t, canon_hits),
        "names": names,
        "canon_names": cn,
        "firm_city": t.city,
        "firm_state": t.state,
    }


def decide(f: Dict[str, Any]) -> Tuple[str, str, Optional[str]]:
    """features -> (verdict, reason, rule). Reads only the feature dict (the pilot replay feeds it)."""
    loc = f.get("location")
    if loc is None:
        loc = (f.get("city_share") or 0) > 0 or (f.get("state_share") or 0) > 0
    full = bool(f.get("board_name_exact") or (f.get("name_share") or 0) >= MIN_SHARE or f.get("intro_names_firm"))
    can = bool(f.get("board_name_canon") or (f.get("canon_share") or 0) >= MIN_SHARE or f.get("intro_names_canon"))
    short = bool(f.get("short_name"))
    if not f.get("postings"):
        return ("candidate", "empty_board", None) if (full or can) else ("rejected", "name_mismatch", None)
    # R8: corroboration beyond the name -- a location hit or a Form D person named in the postings
    corroborated = bool(loc or f.get("persons_found"))
    held = None
    if full and (f.get("board_name_exact") or not short or loc):
        if f.get("previous_only") and not corroborated:
            held = "previous_name_without_location"
        elif f.get("ambiguous_full") and not corroborated:
            held = "ambiguous_name_without_location"
        else:
            return "verified", "R8_full_name", "R8_full_name"
    if can and loc:
        # R7b (live run 2026-10-05, Luminate Health -> Luminate the LA data firm): when the canonical
        # name dropped a descriptive word ("health"), a state hit alone is not enough: the city must match
        if not ((f.get("city_share") or 0) > 0 or not f.get("canon_dropped_words")):
            return "candidate", "canonical_state_only", None
        # R8 (Axios HQ vs Axios Media, both Arlington VA): an ambiguous one-word canonical name needs
        # the full name somewhere, a Form D person, or the firm's city in >= 10% of postings
        if f.get("ambiguous_canon") and not (
                (f.get("name_share") or 0) > 0 or f.get("intro_names_firm") or f.get("persons_found")
                or (f.get("city_share") or 0) >= AMBIGUOUS_CITY_SHARE):
            return "candidate", held or "ambiguous_canonical", None
        if not held:
            return "verified", "R8_canonical_location", "R8_canonical_location"
    if held:
        return "candidate", held, None
    if full or can:
        return "candidate", ("short_name_without_location" if full and short else "name_without_location"), None
    return "rejected", "name_mismatch", None


def score(f: Dict[str, Any]) -> float:
    s_name = 0.0
    if f.get("board_name_exact"):
        s_name = 0.55
    if (f.get("name_share") or 0) >= MIN_SHARE:
        s_name = max(s_name, 0.35 + 0.2 * f["name_share"])
    if f.get("intro_names_firm"):
        s_name = max(s_name, 0.45)
    if f.get("board_name_canon"):
        s_name = max(s_name, 0.4)
    if (f.get("canon_share") or 0) >= MIN_SHARE:
        s_name = max(s_name, 0.25 + 0.15 * f["canon_share"])
    if f.get("intro_names_canon"):
        s_name = max(s_name, 0.35)
    s_loc = (0.15 + 0.1 * max(f.get("city_share") or 0, f.get("state_share") or 0)) if f.get("location") else 0.0
    s_vol = 0.05 * min(f.get("postings") or 0, 20) / 20
    s_people = 0.1 if f.get("persons_found") else 0.0
    s_short = 0.1 if f.get("short_name") else 0.0
    s_amb = 0.1 if (f.get("ambiguous_full") or f.get("ambiguous_canon") or f.get("previous_only")) else 0.0
    return round(max(0.0, min(1.0, s_name + s_loc + s_vol + s_people - s_short - s_amb)), 3)


def verdict(ats: str, t: Target, meta: Optional[Dict[str, Any]], postings: Sequence[Dict[str, Any]]) -> Verdict:
    f = features(ats, t, meta, postings)
    v, reason, rule = decide(f)
    sc = score(f)
    ev = {"rule": rule, "rule_version": RULE_VERSION, **f, "verdict": v, "reason": reason, "score": sc}
    return Verdict(v, reason, rule, sc, ev)


# ---------------------------------------------------------------------------
# careers domain (SPEC_148: one source family, a weak claim)
# ---------------------------------------------------------------------------


def _name_keys(t: Target) -> List[str]:
    names, cn = firm_names(t)
    return sorted({x.replace(" ", "") for x in names + cn if len(x.replace(" ", "")) >= 4})


def _label_matches(dom: str, keys: Sequence[str], head_ok: bool = False) -> bool:
    """The domain's first label equals / starts with a name key; with ``head_ok`` (own-site posting
    URLs only) a label of >= 5 chars that is the head of a name key also counts ("delfina" for
    Delfina Care)."""
    label = dom.split(".")[0].replace("-", "")
    return any(label == k or label.startswith(k) or (head_ok and len(label) >= 5 and k.startswith(label))
               for k in keys)


def careers_domain(t: Target, postings: Sequence[Dict[str, Any]], meta: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """The firm's own domain as a verified board shows it, or None.

    posting_url: the registrable domain most posting URLs share (>= 50%), ATS hosts dropped;
    posting_text: a domain in posting / intro text. Either way the domain's first label must equal
    or start with one of the firm's names (>= 4 chars): text domains are noisy (b.tech,
    cypress.io), and a careers site on another name may be a parent's."""
    postings = list(postings or [])
    keys = _name_keys(t)
    if not postings or not keys:
        return None
    n = len(postings)
    by_url: Counter = Counter()
    examples: Dict[str, List[str]] = {}
    for p in postings:
        d = domains.domain(p.get("source_url")) if p.get("source_url") else None
        if d:
            by_url[d] += 1
            examples.setdefault(d, []).append(p["source_url"])
    if by_url:
        d, c = by_url.most_common(1)[0]
        if c * 2 >= n and _label_matches(d, keys, head_ok=True):
            return {"domain": d, "basis": "posting_url", "postings_with_domain": c, "postings": n,
                    "examples": examples[d][:2], "name_keys": keys}
    by_text: Counter = Counter()
    intro = pay.clean((meta or {}).get("content")) if meta else ""
    for txt in [intro] + [p.get("description_text") or "" for p in postings]:
        found = set()
        for m in _DOMAIN_TEXT.findall(txt or ""):
            d = domains.domain(m.lower())
            if d and _label_matches(d, keys):
                found.add(d)
        for d in found:
            by_text[d] += 1
    if by_text:
        d, c = by_text.most_common(1)[0]
        return {"domain": d, "basis": "posting_text", "postings_with_domain": c, "postings": n,
                "name_keys": keys}
    return None
