"""
Telling a person's name from a company's, in SEC Form D related-person rows.

The form has `first_name` / `middle_name` / `last_name` and filers put whatever
they like in them. Measured on the 87,179 rows that land on an attributed PE/VC
fund: ~40% of distinct name tuples are **entities**, mis-split across the
fields, and 23,597 rows carry a placeholder first name:

    first_name='LLC'   last_name='Fund GP,'
    first_name='N/A'   last_name='Belltower Fund Group, Ltd.'
    first_name='--'    last_name='Maples Fiduciary Services (Delaware) Inc.'

Three constraints hold this together. Each is a measured bug if broken:

1. **Whole tokens only, never substrings.** ``LIKE '%co%'`` removes 600 real
   people -- Michael **Co**llins (311 rows), Scott Voss via `%ss%`, and so on;
   ``%lp%`` costs Volpert and Alpern, ``%inc%`` costs Vincent and Reyna.
2. **Never look at `middle_name`.** Every tuple flagged by a middle-field token
   alone is a real person with a corrupted middle field (`Anthony|GP|Cusano`).
3. **Glue single-letter runs within one field, never across fields.** `L.L.C.`
   has to become `llc`, but `Brian | C. | O'Connor` must not become `co`.

Surnames that merely resemble the vocabulary are deliberately absent from it:
Marks, Gross, Rich, Price, Banks, Bond and Young are people.
"""

from __future__ import annotations

from typing import List, Optional, Tuple

from app.entities.norm import _fold

# Legal forms and the corporate vocabulary a fund vehicle's name is built from.
# Every entry must be a word no plausible surname equals -- check before adding.
ENTITY_TOKENS = frozenset({
    "llc", "lp", "llp", "lllp", "ltd", "limited", "inc", "corp", "corporation",
    "company", "co", "plc", "sarl", "sa", "gmbh", "bv", "nv", "ag", "pte",
    "fund", "funds", "gp", "partners", "partner", "capital", "management",
    "advisors", "advisers", "ventures", "holdings", "group", "trust",
    "associates", "investments", "equity", "asset", "securities", "sicav",
    "scsp", "lllp", "series", "manager", "managers",
})

# A first name that is not a name. Folded before comparison.
PLACEHOLDER_FIRST = frozenset({
    "", "n a", "na", "none", "nil", "null", "unknown", "x", "xx", "xxx",
    ".", "-", "--", "---", "*", "**", "tbd",
})

SEE_CLARIFICATION = "see clarification"

MAX_NAME_TOKENS = 4
MIN_NAME_TOKENS = 2


def _glue_single_letters(tokens: List[str]) -> List[str]:
    """Emit the tokens plus the glue of each maximal run of single letters.

    `['l','l','c']` also yields `'llc'`, so a dotted legal form is caught. The
    caller passes ONE field at a time -- gluing across fields turns
    `Brian | C. | O'Connor` into `co`.
    """
    out = list(tokens)
    run: List[str] = []
    for tok in tokens + [""]:
        if len(tok) == 1 and tok.isalpha():
            run.append(tok)
            continue
        if len(run) > 1:
            out.append("".join(run))
        run = []
    return out


def field_tokens(value: Optional[str]) -> List[str]:
    """Folded tokens of one name field, with dotted legal forms glued."""
    folded = _fold(value)
    if not folded:
        return []
    return _glue_single_letters(folded.split())


def looks_like_entity(first: Optional[str], last: Optional[str]) -> Tuple[bool, bool]:
    """(is_entity, only_via_glue) for a first/last pair. Middle is never read.

    `only_via_glue` separates the dotted-legal-form catch into its own counter
    so the glue rule's contribution stays auditable.
    """
    plain = False
    glued = False
    for value in (first, last):
        folded = _fold(value)
        if not folded:
            continue
        raw = folded.split()
        if any(tok in ENTITY_TOKENS for tok in raw):
            plain = True
        elif any(tok in ENTITY_TOKENS for tok in _glue_single_letters(raw)):
            glued = True
    return (plain or glued), (glued and not plain)


def is_placeholder(first: Optional[str], last: Optional[str]) -> bool:
    """A row whose name fields carry no name at all."""
    return _fold(first) in PLACEHOLDER_FIRST or not _fold(last)


def is_see_clarification(first: Optional[str], last: Optional[str]) -> bool:
    """`See Clarification` is a pointer to another field, not a person."""
    return SEE_CLARIFICATION in (_fold(first), _fold(last))


def rescue_name_in_last_field(first: Optional[str], last: Optional[str]
                              ) -> Optional[Tuple[str, str]]:
    """A whole human name typed into `last_name` with a placeholder first.

    `'-' | '' | 'Brandon Green'` is a person; measured at 13 rows. Only a clean
    two-token last field with no entity vocabulary qualifies, so nothing is
    invented.
    """
    folded = _fold(last)
    toks = folded.split()
    if len(toks) != 2 or any(t in ENTITY_TOKENS for t in toks):
        return None
    return toks[0], toks[1]


def name_norm(first: Optional[str], last: Optional[str]) -> str:
    """The identity key: folded first + last, middle deliberately excluded.

    `middle_name` is corrupt in this source -- 319 name+firm groups carry two
    or more conflicting non-empty middle values for one person at one firm,
    with initials running in blocks down the filing sequence -- so including it
    would split one human into several. `norm.core()` is also wrong here: it
    strips `co`, `ltd` and `the` as legal suffixes, which mangles surnames.
    """
    return " ".join(f"{_fold(first)} {_fold(last)}".split())


def token_count_ok(norm_value: str) -> bool:
    n = len(norm_value.split())
    return MIN_NAME_TOKENS <= n <= MAX_NAME_TOKENS


def display_name(first: Optional[str], middle: Optional[str],
                 last: Optional[str]) -> str:
    """The human-facing spelling for `pe_people.full_name` (NOT NULL).

    Raw cased, not the normalized key, or the whole table renders lowercase.
    Embedded newlines and tabs are stripped: 72 legacy rows contain them.
    """
    parts = [(p or "").strip() for p in (first, middle, last)]
    joined = " ".join(p for p in parts if p)
    return " ".join(joined.split())
