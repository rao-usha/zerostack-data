"""
Licence guard: a defined detector for licensed code content (SPEC_163; PLAN_100 §5.4 guard 5, §10).

Nexdata policy (PLAN_100 §2, §3): **no CPT (AMA) and no SNOMED CT content** in briefs, prompts,
model outputs, scoring artefacts or releases. CPT Level I codes may appear only as join values
(``cms_medicare_utilization.hcpcs_cd``); ``hcpcs_desc`` (AMA CPT descriptors) never leaves the
database. CDT (ADA; the HCPCS D-series) is treated the same way. Later specs call this module
to block such content (SPEC_165 brief/runner guards, SPEC_166 scoring).

Detectors
---------
``cpt_code``        A token with the CPT shape ``^\\d{4}[0-9FTU]$`` (plan §5.4 (a)): Category I
                    (5 digits, 00100-99607), Category II (``dddd F``), Category III (``dddd T``),
                    PLA (``dddd U``). In free text the token must stand alone (no adjacent letter
                    or digit), so 10-digit NPIs, 8-digit dates and HCPCS Level II codes
                    (``A0427``) never match; ZIP+4 (``12345-6789``) and decimals (``12345.67``)
                    are excluded.
``cpt_descriptor``  Text that reproduces an AMA CPT descriptor from a caller-supplied set
                    (normally ``load_cpt_descriptors(conn)``: the distinct ``hcpcs_desc`` of
                    Level-I-shaped rows in ``cms_medicare_utilization``, read in-process, never
                    sent anywhere). Exact (normalised) containment, or fuzzy: >= ``threshold``
                    (default 90, plan §5.4 (c)) percent of the descriptor's characters matched
                    in order (difflib), after a token-overlap prefilter.
``cdt_code``        ``D\\d{4}`` (HCPCS D-series = ADA CDT).
``snomed_uri``      SNOMED CT system identifiers: ``snomed.info/sct``, ``snomed.info/id``, the
                    OID ``2.16.840.1.113883.6.96``.
``snomed_id``       A 6-18 digit SCTID that passes the Verhoeff check digit and has a valid
                    partition identifier (00/01/02/10/11/12), when **anchored**: within 40
                    characters after ``SNOMED``/``SCTID``/``SCT``, or the ``code`` of a JSON
                    object whose ``system`` is SNOMED. ``snomed_bare=True`` flags any such
                    number without an anchor.

Precision limits (read before using a finding as proof)
-------------------------------------------------------
- **5-digit numeric tokens are ambiguous.** ZIP codes, county FIPS codes, counts and money
  amounts written without separators have the same shape as a Category I CPT code, so in free
  text ``cpt_code`` has low precision (high recall). It is a blocking guard, not an
  identification: callers whitelist legitimate values with ``allow_codes`` (bundle examples,
  join values) or skip JSON paths with ``skip_path`` (e.g. a zip5 binding example).
  Codes written with a separator inside them (``992 13``) are not detected.
- ``cpt_descriptor`` only knows the descriptors it is given; with no set it detects nothing.
  Short descriptors (< ``min_len`` = 16 normalised characters) are ignored to avoid matching
  ordinary words; paraphrases below the threshold are missed.
- ``cdt_code`` collides with undotted ICD-10-CM D-codes (``D6851`` for ``D68.51``); write ICD
  codes with the dot.
- Bare ``snomed_id`` has roughly a 0.6% false-positive rate on random 6-18 digit numbers
  (1/10 Verhoeff x 6/100 partition), so about 1 in 170 NPIs would match -- keep the default
  (anchored) mode for anything that contains NPIs. Anchored mode misses unlabelled SCTIDs.
- SNOMED descriptions (terms) are not detected: there is no licensed term list to compare
  against, by design.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from difflib import SequenceMatcher
from typing import Any, Callable, FrozenSet, Iterable, List, Optional, Sequence

CPT_CODE_RE = re.compile(r"^\d{4}[0-9FTU]$")
_CPT_TOKEN_RE = re.compile(r"(?<![0-9A-Za-z])(\d{4}[0-9FTU])(?![0-9A-Za-z])(?!-\d{4})(?!\.\d)")
_CDT_TOKEN_RE = re.compile(r"(?<![0-9A-Za-z])(D\d{4})(?![0-9A-Za-z])")
_SNOMED_URI_RE = re.compile(r"snomed\.info/(?:sct|id)|2\.16\.840\.1\.113883\.6\.96", re.I)
_SNOMED_ANCHOR_RE = re.compile(r"(?<![A-Za-z])(?:snomed(?:[\s_-]?ct)?|sctid|sct)(?![A-Za-z])", re.I)
_DIGITS_RE = re.compile(r"(?<!\d)(\d{6,18})(?!\d)")
_SCTID_PARTITIONS = {"00", "01", "02", "10", "11", "12"}
_WORD_RE = re.compile(r"[a-z0-9]+")

# Verhoeff tables
_D = [[0, 1, 2, 3, 4, 5, 6, 7, 8, 9], [1, 2, 3, 4, 0, 6, 7, 8, 9, 5], [2, 3, 4, 0, 1, 7, 8, 9, 5, 6],
      [3, 4, 0, 1, 2, 8, 9, 5, 6, 7], [4, 0, 1, 2, 3, 9, 5, 6, 7, 8], [5, 9, 8, 7, 6, 0, 4, 3, 2, 1],
      [6, 5, 9, 8, 7, 1, 0, 4, 3, 2], [7, 6, 5, 9, 8, 2, 1, 0, 4, 3], [8, 7, 6, 5, 9, 3, 2, 1, 0, 4],
      [9, 8, 7, 6, 5, 4, 3, 2, 1, 0]]
_P = [[0, 1, 2, 3, 4, 5, 6, 7, 8, 9], [1, 5, 7, 6, 2, 8, 3, 0, 9, 4], [5, 8, 0, 3, 7, 9, 6, 1, 4, 2],
      [8, 9, 1, 6, 0, 4, 3, 5, 2, 7], [9, 4, 5, 3, 1, 2, 6, 8, 7, 0], [4, 2, 8, 6, 5, 7, 3, 9, 0, 1],
      [2, 7, 9, 3, 8, 0, 6, 4, 1, 5], [7, 0, 4, 6, 9, 1, 3, 2, 5, 8]]

CPT_DESCRIPTOR_SQL = ("SELECT DISTINCT hcpcs_desc FROM cms_medicare_utilization "
                      "WHERE hcpcs_desc IS NOT NULL AND hcpcs_cd ~ '^[0-9]{4}[0-9FTU]$'")


class LicenceViolation(ValueError):
    """Licensed code content found where policy forbids it."""

    def __init__(self, findings: Sequence["Finding"]):
        self.findings = list(findings)
        head = "; ".join(f"{f.kind} {f.value!r} at {f.location or '<text>'}" for f in self.findings[:5])
        super().__init__(f"{len(self.findings)} licence finding(s): {head}")


@dataclass(frozen=True)
class Finding:
    kind: str           # cpt_code | cpt_descriptor | cdt_code | snomed_uri | snomed_id
    value: str
    location: str = ""  # JSON path ($.a[0].b) when scanning JSON
    detail: str = ""


# ---------------------------------------------------------------------------
# code-shape detectors
# ---------------------------------------------------------------------------

def is_cpt_code(value: Any) -> bool:
    """True for a whole string with the CPT shape (Category I in 00100-99607, II, III, PLA)."""
    s = str(value).strip() if value is not None else ""
    if not CPT_CODE_RE.match(s):
        return False
    return not s.isdigit() or 100 <= int(s) <= 99607


def find_cpt_codes(text: str, allow_codes: Iterable[str] = ()) -> List[str]:
    allow = set(allow_codes)
    return [m.group(1) for m in _CPT_TOKEN_RE.finditer(text or "")
            if is_cpt_code(m.group(1)) and m.group(1) not in allow]


def find_cdt_codes(text: str, allow_codes: Iterable[str] = ()) -> List[str]:
    allow = set(allow_codes)
    return [m.group(1) for m in _CDT_TOKEN_RE.finditer(text or "") if m.group(1) not in allow]


def verhoeff_valid(digits: str) -> bool:
    c = 0
    for i, ch in enumerate(reversed(digits)):
        c = _D[c][_P[i % 8][int(ch)]]
    return c == 0


def is_sctid(value: Any) -> bool:
    """Structural SCTID test: 6-18 digits, no leading zero, valid partition, Verhoeff OK."""
    s = str(value).strip() if value is not None else ""
    return (s.isdigit() and 6 <= len(s) <= 18 and s[0] != "0"
            and s[-3:-1] in _SCTID_PARTITIONS and verhoeff_valid(s))


def find_snomed(text: str, bare: bool = False) -> List[Finding]:
    text = text or ""
    out = [Finding("snomed_uri", m.group(0)) for m in _SNOMED_URI_RE.finditer(text)]
    if bare:
        out += [Finding("snomed_id", m.group(1), detail="bare") for m in _DIGITS_RE.finditer(text)
                if is_sctid(m.group(1))]
        return out
    seen = set()
    for a in _SNOMED_ANCHOR_RE.finditer(text):
        window = text[a.end():a.end() + 40]
        for m in _DIGITS_RE.finditer(window):
            pos = a.end() + m.start()
            if pos not in seen and is_sctid(m.group(1)):
                seen.add(pos)
                out.append(Finding("snomed_id", m.group(1), detail="anchored"))
    return out


# ---------------------------------------------------------------------------
# CPT descriptors
# ---------------------------------------------------------------------------

def _norm(s: str) -> str:
    return " ".join(_WORD_RE.findall((s or "").lower()))


class CptDescriptorIndex:
    """In-memory CPT descriptor matcher. Holds the descriptors only in process memory."""

    def __init__(self, descriptors: Iterable[str], threshold: int = 90, min_len: int = 16):
        self.threshold = threshold
        self._items = []
        for d in {(_norm(x)) for x in descriptors if x}:
            if len(d) >= min_len:
                toks = {t for t in d.split() if len(t) >= 3}
                self._items.append((d, toks))

    def __len__(self) -> int:
        return len(self._items)

    def match(self, text: str) -> List[Finding]:
        t = _norm(text)
        if not t:
            return []
        ttoks = set(t.split())
        out = []
        for d, toks in self._items:
            if d in t:
                out.append(Finding("cpt_descriptor", d, detail="exact"))
                continue
            if toks and len(toks & ttoks) / len(toks) < 0.6:
                continue
            blocks = SequenceMatcher(None, d, t, autojunk=False).get_matching_blocks()
            score = 100.0 * sum(b.size for b in blocks) / len(d)
            if score >= self.threshold:
                out.append(Finding("cpt_descriptor", d, detail=f"fuzzy {score:.0f}"))
        return out


def load_cpt_descriptors(conn) -> List[str]:
    """Distinct CPT descriptors from ``cms_medicare_utilization`` (Level-I-shaped rows only).
    Read-only, constant SQL; the result stays in process memory (never logged, never sent)."""
    from sqlalchemy import text

    return [r[0] for r in conn.execute(text(CPT_DESCRIPTOR_SQL))]


# ---------------------------------------------------------------------------
# scanning
# ---------------------------------------------------------------------------

def scan_text(text: str, *, cpt_descriptors: Optional[CptDescriptorIndex] = None,
              allow_codes: Iterable[str] = (), snomed_bare: bool = False,
              location: str = "") -> List[Finding]:
    allow: FrozenSet[str] = frozenset(allow_codes)
    out = [Finding("cpt_code", c, location) for c in find_cpt_codes(text, allow)]
    out += [Finding("cdt_code", c, location) for c in find_cdt_codes(text, allow)]
    out += [Finding(f.kind, f.value, location, f.detail) for f in find_snomed(text, bare=snomed_bare)]
    if cpt_descriptors is not None:
        out += [Finding(f.kind, f.value, location, f.detail) for f in cpt_descriptors.match(text)]
    return out


def scan_json(obj: Any, *, cpt_descriptors: Optional[CptDescriptorIndex] = None,
              allow_codes: Iterable[str] = (), snomed_bare: bool = False,
              skip_path: Optional[Callable[[str], bool]] = None) -> List[Finding]:
    """Scan every key and string / number leaf of a JSON-like value. ``skip_path(path)`` returning
    True skips that subtree (e.g. binding join values). A dict whose ``system`` names SNOMED
    has its ``code`` flagged as ``snomed_id`` whatever its shape."""
    allow = frozenset(allow_codes)
    out: List[Finding] = []

    def walk(node: Any, path: str) -> None:
        if skip_path is not None and skip_path(path):
            return
        if isinstance(node, dict):
            system = node.get("system")
            if isinstance(system, str) and _SNOMED_URI_RE.search(system) and node.get("code") is not None:
                out.append(Finding("snomed_id", str(node["code"]), f"{path}.code", "system=SNOMED"))
            for k, v in node.items():
                out.extend(scan_text(str(k), allow_codes=allow, snomed_bare=False, location=f"{path}.{k}#key"))
                walk(v, f"{path}.{k}")
        elif isinstance(node, (list, tuple)):
            for i, v in enumerate(node):
                walk(v, f"{path}[{i}]")
        elif isinstance(node, bool) or node is None:
            return
        elif isinstance(node, (int, float, str)):
            out.extend(scan_text(str(node), cpt_descriptors=cpt_descriptors, allow_codes=allow,
                                 snomed_bare=snomed_bare, location=path))

    walk(obj, "$")
    return out


def assert_clean(obj: Any, **kw) -> None:
    """Raise LicenceViolation when ``obj`` (str or JSON-like) has any finding."""
    findings = scan_text(obj, **kw) if isinstance(obj, str) else scan_json(obj, **kw)
    if findings:
        raise LicenceViolation(findings)
