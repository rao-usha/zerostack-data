"""Name / address normalization for entity resolution -- pure, stdlib, versioned.

DB-free and import-free of anything in this app: every function here is a pure
string function, so the whole module is unit-testable with `python ingest/norm.py`
(self-test at the bottom, house convention).

WHY THE HARNESS OWNS THIS: a connector never normalizes a name itself. If two
connectors normalize differently the corpus fragments across versions and the
blocking keys stop meeting. Connectors emit RAW names in er_records(); the
harness calls norm()/core() on the way into workbench.er_source_record and
stamps NAME_NORM_VERSION alongside. Bumping NAME_NORM_VERSION invalidates
stored matches by design -- it is stored per row so that is detectable.

Two outputs per name, both stored:
  norm(name)  -- casefold, unaccent, DBA/FKA/AKA truncation, legal-form suffix
                 stripped at the TAIL ONLY and only if something survives
                 ('LLC Industries' must not become '').
  core(name)  -- norm() minus high-frequency corporate noise tokens. This is
                 what the (state, left(name_core,6)) blocking key uses.

MEASURED LIMITATION, shipped as a test rather than hidden: dotted foreign
suffixes shatter into single letters before suffix-stripping
('Sanchez Hermanos S.A. de C.V.' -> 's a de c v' tokens). Known multi-token
suffixes are matched as PHRASES (SUFFIX_PHRASES) which fixes the common ones;
anything not in that list still shatters, and the self-test says so out loud.

NEVER stem, NEVER soundex: phonetic matching is tuned for surnames and
produces nonsense on brand names (OVME / AVIVA collide).
"""

import re
import unicodedata

NAME_NORM_VERSION = "v1"

# Truncate at the first DBA/FKA/AKA marker: everything after it is a second
# name, not part of this one. Matched AFTER punctuation folding, so 'd/b/a'
# and 'D.B.A.' both arrive as 'd b a'.
_DBA_MARKER_RE = re.compile(
    r"\b(d b a|dba|f k a|fka|a k a|aka|n k a|nka|"
    r"formerly known as|doing business as|formerly)\b"
)

# Legal-form tokens stripped at the tail only, one at a time, never to empty.
SUFFIX_TOKENS = {
    "inc", "incorporated", "corp", "corporation", "co", "company", "llc",
    "lc", "pllc", "llp", "lllp", "lp", "ltd", "limited", "plc", "pc", "pa",
    "psc", "gmbh", "ag", "nv", "bv", "sa", "sas", "srl", "spa", "oy", "ab",
    "as", "aps", "kk", "ulc", "ltda", "sarl", "the",
}

# Multi-token legal suffixes matched as PHRASES at the tail, before token
# stripping -- the fix for dotted foreign forms that punctuation folding
# shatters into single letters.
SUFFIX_PHRASES = (
    "s a de c v", "sa de cv", "de c v", "s de r l de c v", "s de r l",
    "s a r l", "s p a", "b v", "n v", "a g", "s a", "pty ltd", "pte ltd",
    "co ltd", "co inc", "and co", "s l",
)

# High-frequency corporate noise dropped by core(). Measured document
# frequencies in the current 11,886-name corpus drove this list; rarity itself
# is computed from the corpus at resolve time (workbench.er_token_df), never
# hardcoded -- this set only removes the tokens that are noise in EVERY corpus.
NOISE_TOKENS = {
    "holdings", "holding", "group", "usa", "us", "national", "enterprises",
    "enterprise", "industries", "partners", "associates", "services",
    "service", "solutions", "systems", "international", "global",
}

# Single-letter tokens that are real acronyms, kept when shattered forms are
# pruned. Everything else of length 1 is punctuation debris.
KNOWN_SINGLE_LETTERS = {"a", "i", "j", "k", "m", "s", "t", "v", "x", "z"}

# Registered-agent / mail-drop names. An address carrying one of these is an
# agent address: it contributes ZERO to matching (never down-weighted -- a
# down-weighted 1209 Orange Street still accumulates across features).
AGENT_NAME_MARKERS = (
    "registered agent", "national registered agents", "nrai",
    "ct corporation", "c t corporation", "corporation service company",
    "csc", "incorp", "harvard business services", "legalzoom",
    "cogency global", "vcorp", "northwest registered agent",
    "united states corporation agents", "capitol services",
)

_WS_RE = re.compile(r"\s+")
_NON_ALNUM_RE = re.compile(r"[^a-z0-9]+")

# Street-token abbreviations. USPS-ish, deliberately small: the goal is that
# '123 North Main Street Suite 400' and '123 N MAIN ST STE 400' produce the
# same street_norm, not a full CASS standardization.
_STREET_ABBREV = {
    "street": "st", "str": "st", "avenue": "ave", "av": "ave",
    "boulevard": "blvd", "road": "rd", "drive": "dr", "lane": "ln",
    "court": "ct", "circle": "cir", "place": "pl", "parkway": "pkwy",
    "highway": "hwy", "square": "sq", "terrace": "ter", "trail": "trl",
    "north": "n", "south": "s", "east": "e", "west": "w",
    "northeast": "ne", "northwest": "nw", "southeast": "se", "southwest": "sw",
    "suite": "ste", "apartment": "apt", "building": "bldg", "floor": "fl",
}
_UNIT_MARKERS = {"ste", "apt", "unit", "fl", "rm", "bldg", "#"}


def _fold(s):
    """Casefold + NFKD unaccent + punctuation -> spaces. The common prefix of
    every normalizer here. unicodedata does the unaccent job, which is why no
    `unaccent` extension is installed (that would write into schema public)."""
    if not s:
        return ""
    s = unicodedata.normalize("NFKD", str(s))
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.casefold().replace("&", " and ")
    s = _NON_ALNUM_RE.sub(" ", s)
    return _WS_RE.sub(" ", s).strip()


def _strip_suffixes(tokens):
    """Drop legal-form phrases then tokens from the TAIL, never to empty."""
    text = " ".join(tokens)
    changed = True
    while changed:
        changed = False
        for phrase in SUFFIX_PHRASES:
            if text.endswith(" " + phrase):
                candidate = text[: -(len(phrase) + 1)].strip()
                if candidate:            # never strip to empty
                    text, changed = candidate, True
                    break
    out = text.split()
    while len(out) > 1 and out[-1] in SUFFIX_TOKENS:
        out.pop()
    while len(out) > 1 and out[0] == "the":
        out.pop(0)
    return out


def norm(name):
    """Normalized company name, or None when nothing survives.

    None (never '') is the honest answer for a name that normalizes empty:
    2 of 11,886 corpus names do, and they must be excluded from name blocking
    entirely rather than matching each other on an empty string.
    """
    folded = _fold(name)
    if not folded:
        return None
    m = _DBA_MARKER_RE.search(folded)
    if m:
        folded = folded[: m.start()].strip() or folded[m.end():].strip()
    tokens = _strip_suffixes(folded.split())
    tokens = [t for t in tokens
              if len(t) > 1 or t in KNOWN_SINGLE_LETTERS or t.isdigit()]
    out = " ".join(tokens).strip()
    return out or None


def core(name):
    """norm() minus high-frequency corporate noise. Falls back to norm() when
    the name is ALL noise ('National Holdings Group')."""
    base = norm(name)
    if base is None:
        return None
    kept = [t for t in base.split() if t not in NOISE_TOKENS]
    return " ".join(kept) if kept else base


def tokens(name):
    """Token list of norm(name) -- the unit token frequency is counted over
    (workbench.er_token_df) and IDF-weighted name overlap is summed over."""
    base = norm(name)
    return base.split() if base else []


def phone10(value):
    """US 10-digit phone or None. Extension cut FIRST (else 'x12' becomes part
    of the number and silently shifts every digit), then leading country '1'."""
    s = str(value or "").casefold()
    s = re.split(r"(?:\bx\b|\bext\b|extension|\bx\d)", s, maxsplit=1)[0]
    digits = re.sub(r"\D", "", s)
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    if len(digits) < 10:
        return None
    digits = digits[:10]
    return digits if digits.strip("0") else None


def zip5(value):
    """First 5 digits of a US zip, or None. '95630-1234' -> '95630'."""
    digits = re.sub(r"\D", "", str(value or ""))
    if len(digits) < 5:
        return None
    out = digits[:5]
    return out if out != "00000" else None


def state2(value):
    """2-letter US state code, or None. Handles GLEIF's 'US-PA' form."""
    s = _fold(value).upper().replace(" ", "")
    if s.startswith("US") and len(s) == 4:
        s = s[2:]
    return s if len(s) == 2 and s.isalpha() else None


def street_norm(line1, line2=None):
    """(street_norm, unit) -- number + abbreviated street tokens, unit split off.

    Suite/floor/apartment noise is the single biggest cause of false negatives
    on address matching, so it is split into its own field rather than dropped
    silently.
    """
    raw = " ".join(x for x in (line1, line2) if x)
    folded = _fold(raw)
    if not folded:
        return None, None
    parts = [_STREET_ABBREV.get(t, t) for t in folded.split()]
    unit = None
    for i, tok in enumerate(parts):
        if tok in _UNIT_MARKERS and i > 0:
            unit = " ".join(parts[i + 1:]) or None
            parts = parts[:i]
            break
    out = " ".join(parts).strip()
    return (out or None), unit


def is_agent_address(care_of_name=None, street=None):
    """True when this address is a registered agent / mail drop.

    Form 5500 hands us SPONS_DFE_CARE_OF_NAME explicitly, and any 'c/o' prefix
    is the same signal. Agent addresses are excluded as a blocking key and
    contribute zero weight -- 1209 Orange Street (National Registered Agents)
    alone would otherwise link tens of thousands of unrelated entities.
    """
    blob = _fold(" ".join(x for x in (care_of_name, street) if x))
    if not blob:
        return False
    if care_of_name and str(care_of_name).strip():
        return True
    if blob.startswith("c o ") or " c o " in blob:
        return True
    return any(marker in blob for marker in AGENT_NAME_MARKERS)


def domain(value):
    """Registrable-ish domain from a URL or bare host, or None.

    Deliberately naive (no public-suffix list, no new dependency): scheme and
    path stripped, 'www.' dropped, lowercased. Good enough as a blocking key,
    never used as proof on its own.
    """
    s = (str(value or "")).strip().casefold()
    if not s:
        return None
    s = re.sub(r"^[a-z]+://", "", s).split("/")[0].split("?")[0]
    s = s.split("@")[-1].split(":")[0]
    if s.startswith("www."):
        s = s[4:]
    if "." not in s or " " in s:
        return None
    return s or None


def clean_ein(value):
    """9-digit EIN, or None for blank / all-zero / the '000000000' sentinel.

    MEASURED: EDGAR submissions carry EIN on 12/12 sampled operating CIKs but
    2 of them are the sentinel (Denison Mines, Commvault) -- joining on it
    would fuse unrelated companies. The sentinel is nulled, never joined.
    """
    digits = re.sub(r"\D", "", str(value or ""))
    if len(digits) != 9 or not digits.strip("0"):
        return None
    return digits


def _selftest():
    """Run: python ingest/norm.py -- prints PASS/FAIL per case, exit 1 on any FAIL."""
    cases = [
        ("norm", "The Kroger Co.", "kroger"),
        ("norm", "ACME INDUSTRIAL HOLDINGS, LLC", "acme industrial holdings"),
        ("core", "ACME INDUSTRIAL HOLDINGS, LLC", "acme industrial"),
        ("norm", "Café Rio Holdings LP", "cafe rio holdings"),
        ("core", "Café Rio Holdings LP", "cafe rio"),
        # never strip to empty: the suffix IS the name here
        ("norm", "LLC Industries", "llc industries"),
        ("norm", "Smith & Wesson Inc", "smith and wesson"),
        ("norm", "MERIDIAN DENTAL d/b/a BRIGHT SMILES", "meridian dental"),
        # PHRASE suffix matching -- the documented fix for dotted foreign forms
        ("norm", "Sanchez Hermanos S.A. de C.V.", "sanchez hermanos"),
        ("norm", "Ficticia S.à r.l.", "ficticia"),
        # ...and the limitation that survives it: a dotted form NOT in
        # SUFFIX_PHRASES still shatters into single letters. Named here on
        # purpose -- the honest fix is to extend SUFFIX_PHRASES, and this
        # test is where the next reader finds that out.
        ("norm", "Empresa S.A.P.I. de C.V.", "empresa s a i"),
        ("norm", "   ", None),
        ("norm", "!!!", None),
        ("core", "National Holdings Group", "national holdings group"),
        ("phone10", "(916) 868-6960", "9168686960"),
        ("phone10", "1-916-868-6960 x12", "9168686960"),
        ("zip5", "95630-1234", "95630"),
        ("state2", "US-PA", "PA"),
        ("clean_ein", "000000000", None),
        ("clean_ein", "26-1640968", "261640968"),
        ("domain", "https://WWW.Example.com/about", "example.com"),
    ]
    fns = {"norm": norm, "core": core, "phone10": phone10, "zip5": zip5,
           "state2": state2, "clean_ein": clean_ein, "domain": domain}
    failures = 0
    for fn, arg, expected in cases:
        got = fns[fn](arg)
        ok = got == expected
        failures += 0 if ok else 1
        print(f"{'PASS' if ok else 'FAIL'}  {fn}({arg!r}) -> {got!r}"
              + ("" if ok else f"   expected {expected!r}"))
    st, unit = street_norm("123 North Main Street", "Suite 400")
    ok = (st, unit) == ("123 n main st", "400")
    failures += 0 if ok else 1
    print(f"{'PASS' if ok else 'FAIL'}  street_norm -> {(st, unit)!r}")
    ok = is_agent_address(care_of_name="NATIONAL REGISTERED AGENTS",
                          street="1209 ORANGE STREET") is True
    failures += 0 if ok else 1
    print(f"{'PASS' if ok else 'FAIL'}  is_agent_address(1209 Orange St)")
    print(f"norm version {NAME_NORM_VERSION}: "
          f"{len(cases) + 2 - failures}/{len(cases) + 2} passed")
    return 1 if failures else 0


if __name__ == "__main__":
    import sys
    sys.exit(_selftest())
