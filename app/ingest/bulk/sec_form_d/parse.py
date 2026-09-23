"""
Pure parsing of SEC Form D quarterly data set zips (SPEC_108). No DB access.

Each ``iter_*`` function streams one TSV member of the zip and yields typed
tuples in the order of the matching ``*_COLS`` list. Blank values become None.

Real format (verified 2023q3 + 2026q2): UTF-8, tab-separated, unquoted,
``FILING_DATE`` as ``DD-MON-YYYY``, ``SALE_DATE`` ISO, amounts may be
``Indefinite``, RECIPIENTS uses the literal ``None`` for missing CRD numbers,
sequence keys start at 101.
"""

from __future__ import annotations

import csv
import io
import re
import zipfile
from datetime import date
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Set, Tuple

csv.field_size_limit(10_000_000)

BIGINT_MAX = 9_223_372_036_854_775_807
INT_MAX = 2_147_483_647

MONTHS = {"JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
          "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12}

# Same labels the XML path (app/sources/sec_form_d/parser.py) stores, so
# `federal_exemptions @> '["Rule 506(b)"]'` works for both kinds of rows.
EXEMPTION_MAP = {
    "06b": "Rule 506(b)",
    "06c": "Rule 506(c)",
    "04": "Rule 504",
    "05": "Rule 505",
    "3C": "Section 3(c)",
    "3C.1": "Section 3(c)(1)",
    "3C.7": "Section 3(c)(7)",
}

_WS = re.compile(r"\s+")
_NUM = re.compile(r"^\d+(\.\d+)?$")
_DMY = re.compile(r"^(\d{1,2})-([A-Za-z]{3})-(\d{4})$")
_ISO = re.compile(r"^(\d{4})-(\d{2})-(\d{2})")


# ---------------------------------------------------------------------------
# Value parsers
# ---------------------------------------------------------------------------

def clean(s: Optional[str]) -> Optional[str]:
    """Collapse whitespace (incl. NBSP); blank -> None."""
    if s is None:
        return None
    s = _WS.sub(" ", s.replace("\xa0", " ")).strip()
    return s or None


def clean_none(s: Optional[str]) -> Optional[str]:
    """Like clean(), but the literal 'None' (RECIPIENTS) is also NULL."""
    s = clean(s)
    return None if s is None or s == "None" else s


def parse_amount(s: Optional[str]) -> Optional[int]:
    s = clean(s)
    if s is None:
        return None
    s = s.replace("$", "").replace(",", "")
    if not _NUM.match(s):
        return None  # 'Indefinite', junk
    v = int(float(s)) if "." in s else int(s)
    return v if v <= BIGINT_MAX else None


def parse_numeric(s: Optional[str]) -> Optional[Decimal]:
    """Dollar amount -> Decimal (exact), or None ('' / 'Indefinite' / junk)."""
    s = clean(s)
    if s is None:
        return None
    s = s.replace("$", "").replace(",", "")
    if not _NUM.match(s):
        return None
    try:
        return Decimal(s)
    except InvalidOperation:
        return None


def is_indefinite(s: Optional[str]) -> bool:
    return (clean(s) or "").lower() == "indefinite"


def parse_int(s: Optional[str]) -> Optional[int]:
    s = clean(s)
    if s is None or not s.isdigit():
        return None
    v = int(s)
    return v if v <= INT_MAX else None


def parse_date(s: Optional[str]) -> Optional[date]:
    s = clean(s)
    if s is None:
        return None
    try:
        m = _DMY.match(s)
        if m:
            mon = MONTHS.get(m.group(2).upper())
            return date(int(m.group(3)), mon, int(m.group(1))) if mon else None
        m = _ISO.match(s)
        if m:
            return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
    except ValueError:
        return None
    return None


def parse_bool(s: Optional[str]) -> Optional[bool]:
    s = clean(s)
    if s is None:
        return None
    s = s.lower()
    if s in ("true", "yes", "y", "1"):
        return True
    if s in ("false", "no", "n", "0"):
        return False
    return None


def split_list(s: Optional[str]) -> Optional[List[str]]:
    s = clean(s)
    if s is None:
        return None
    items = [x.strip() for x in s.split(",")]
    items = [x for x in items if x]
    return items or None


def exemption_labels(s: Optional[str]) -> Optional[List[str]]:
    codes = split_list(s)
    if codes is None:
        return None
    return [EXEMPTION_MAP.get(c, f"Rule {c}") for c in codes]


def _non_empty(values) -> Optional[List[str]]:
    out = [v for v in (clean(x) for x in values) if v]
    return out or None


# ---------------------------------------------------------------------------
# Zip streaming
# ---------------------------------------------------------------------------

def _member(zf: zipfile.ZipFile, filename: str) -> Optional[str]:
    target = filename.upper()
    for name in zf.namelist():
        if name.replace("\\", "/").rsplit("/", 1)[-1].upper() == target:
            return name
    return None


# Columns that must be in a member's header whenever the member is present
# (SPEC_129). DictReader returns None for an absent column, so header drift
# would otherwise load NULL on every row -- or, for TESTORLIVE, silently skip
# every submission as non-LIVE. Keys plus what the marts read; not every
# optional column, so a column the SEC adds or drops elsewhere never breaks a load.
REQUIRED_HEADERS: Dict[str, Tuple[str, ...]] = {
    "FORMDSUBMISSION.TSV": ("ACCESSIONNUMBER", "FILE_NUM", "FILING_DATE", "SUBMISSIONTYPE",
                            "TESTORLIVE"),
    "ISSUERS.TSV": ("ACCESSIONNUMBER", "ISSUER_SEQ_KEY", "IS_PRIMARYISSUER_FLAG", "CIK", "ENTITYNAME"),
    "OFFERING.TSV": ("ACCESSIONNUMBER", "INDUSTRYGROUPTYPE", "INVESTMENTFUNDTYPE",
                     "FEDERALEXEMPTIONS_ITEMS_LIST", "SALE_DATE", "TOTALOFFERINGAMOUNT",
                     "TOTALAMOUNTSOLD"),
    "RELATEDPERSONS.TSV": ("ACCESSIONNUMBER", "RELATEDPERSON_SEQ_KEY", "FIRSTNAME", "LASTNAME",
                           "RELATIONSHIP_1"),
    "RECIPIENTS.TSV": ("ACCESSIONNUMBER", "RECIPIENT_SEQ_KEY", "RECIPIENTNAME"),
    "SIGNATURES.TSV": ("ACCESSIONNUMBER", "SIGNATURE_SEQ_KEY", "SIGNATURENAME"),
}


def iter_tsv(zip_path: Path | str, filename: str, required: bool = True) -> Iterator[Dict[str, str]]:
    """Stream DictReader rows of one TSV member (matched by basename, any case).

    Raises ValueError when a required member is absent, or when a present
    member's header lacks a ``REQUIRED_HEADERS`` column.
    """
    with zipfile.ZipFile(zip_path) as zf:
        name = _member(zf, filename)
        if name is None:
            if required:
                raise ValueError(f"{Path(zip_path).name}: member {filename} not found")
            return
        with zf.open(name) as raw:
            text = io.TextIOWrapper(raw, encoding="utf-8-sig", errors="replace", newline="")
            reader = csv.DictReader(text, delimiter="\t", quoting=csv.QUOTE_NONE)
            # exact names: the iterators read r.get("ENTITYNAME"), so a padded
            # or re-cased header is as good as absent
            header = list(reader.fieldnames or [])
            missing = [c for c in REQUIRED_HEADERS.get(filename.upper(), ()) if c not in header]
            if missing:
                raise ValueError(
                    f"{Path(zip_path).name}: {filename} header is missing required column(s) "
                    f"{missing}; refusing to load them as NULL"
                )
            for row in reader:
                yield row


def _acc(row: Dict[str, str]) -> Optional[str]:
    return clean(row.get("ACCESSIONNUMBER"))


# ---------------------------------------------------------------------------
# Tables
# ---------------------------------------------------------------------------

SUBMISSION_COLS = ["accession_number", "file_num", "submission_type", "filed_at"]


def non_live_accessions(zip_path) -> Set[str]:
    """Accessions whose TESTORLIVE is not LIVE (their child rows are skipped)."""
    out = set()
    for r in iter_tsv(zip_path, "FORMDSUBMISSION.tsv"):
        acc = _acc(r)
        if acc and (clean(r.get("TESTORLIVE")) or "").upper() != "LIVE":
            out.add(acc)
    return out


def iter_submissions(zip_path) -> Iterator[Tuple]:
    for r in iter_tsv(zip_path, "FORMDSUBMISSION.tsv"):
        acc = _acc(r)
        if not acc or (clean(r.get("TESTORLIVE")) or "").upper() != "LIVE":
            continue
        yield (acc, clean(r.get("FILE_NUM")), clean(r.get("SUBMISSIONTYPE")), parse_date(r.get("FILING_DATE")))


ISSUER_COLS = [
    "accession_number", "issuer_seq", "is_primary", "cik", "entity_name", "street1", "street2", "city",
    "state_or_country", "state_or_country_description", "zip_code", "phone", "jurisdiction", "entity_type",
    "entity_type_other_desc", "year_of_inc_timespan", "year_of_inc_value", "issuer_previous_names",
    "edgar_previous_names",
]


def iter_issuers(zip_path, skip: Set[str] = frozenset()) -> Iterator[Tuple]:
    for r in iter_tsv(zip_path, "ISSUERS.tsv"):
        acc = _acc(r)
        seq = parse_int(r.get("ISSUER_SEQ_KEY"))
        if not acc or acc in skip or seq is None:
            continue
        yield (
            acc, seq,
            (clean(r.get("IS_PRIMARYISSUER_FLAG")) or "").upper() == "YES",
            clean(r.get("CIK")),
            clean(r.get("ENTITYNAME")),
            clean(r.get("STREET1")),
            clean(r.get("STREET2")),
            clean(r.get("CITY")),
            clean(r.get("STATEORCOUNTRY")),
            clean(r.get("STATEORCOUNTRYDESCRIPTION")),
            clean(r.get("ZIPCODE")),
            clean(r.get("ISSUERPHONENUMBER")),
            clean(r.get("JURISDICTIONOFINC")),
            clean(r.get("ENTITYTYPE")),
            clean(r.get("ENTITYTYPEOTHERDESC")),
            clean(r.get("YEAROFINC_TIMESPAN_CHOICE")),
            parse_int(r.get("YEAROFINC_VALUE_ENTERED")),
            _non_empty(r.get(f"ISSUER_PREVIOUSNAME_{i}") for i in (1, 2, 3)),
            _non_empty(r.get(f"EDGAR_PREVIOUSNAME_{i}") for i in (1, 2, 3)),
        )


OFFERING_COLS = [
    "accession_number", "industry_group_type", "investment_fund_type", "is_40_act", "revenue_range",
    "aggregate_net_asset_value_range", "federal_exemptions_items_list", "federal_exemptions", "is_amendment",
    "previous_accession_number", "date_of_first_sale", "yet_to_occur", "duration_of_offering_more_than_one_year",
    "is_equity", "is_debt", "is_option", "is_security_to_be_acquired", "is_pooled_investment_fund",
    "is_business_combination_transaction", "minimum_investment_accepted", "total_offering_amount",
    "is_indefinite", "total_amount_sold", "total_remaining", "has_non_accredited_investors",
    "non_accredited_investors", "total_number_already_invested", "sales_commission_amount",
    "finders_fees_amount", "gross_proceeds_used_amount",
]


def iter_offerings(zip_path, skip: Set[str] = frozenset()) -> Iterator[Tuple]:
    """OFFERING.tsv. `federal_exemptions_items_list` = raw codes; `federal_exemptions` = XML-path labels.
    Amounts are exact Decimals; `is_indefinite` flags TOTALOFFERINGAMOUNT == 'Indefinite'."""
    for r in iter_tsv(zip_path, "OFFERING.tsv"):
        acc = _acc(r)
        if not acc or acc in skip:
            continue
        exemptions = r.get("FEDERALEXEMPTIONS_ITEMS_LIST")
        yield (
            acc,
            clean(r.get("INDUSTRYGROUPTYPE")),
            clean(r.get("INVESTMENTFUNDTYPE")),
            parse_bool(r.get("IS40ACT")),
            clean(r.get("REVENUERANGE")),
            clean(r.get("AGGREGATENETASSETVALUERANGE")),
            split_list(exemptions),
            exemption_labels(exemptions),
            parse_bool(r.get("ISAMENDMENT")),
            clean(r.get("PREVIOUSACCESSIONNUMBER")),
            parse_date(r.get("SALE_DATE")),
            parse_bool(r.get("YETTOOCCUR")),
            parse_bool(r.get("MORETHANONEYEAR")),
            parse_bool(r.get("ISEQUITYTYPE")),
            parse_bool(r.get("ISDEBTTYPE")),
            parse_bool(r.get("ISOPTIONTOACQUIRETYPE")),
            parse_bool(r.get("ISSECURITYTOBEACQUIREDTYPE")),
            parse_bool(r.get("ISPOOLEDINVESTMENTFUNDTYPE")),
            parse_bool(r.get("ISBUSINESSCOMBINATIONTRANS")),
            parse_numeric(r.get("MINIMUMINVESTMENTACCEPTED")),
            parse_numeric(r.get("TOTALOFFERINGAMOUNT")),
            is_indefinite(r.get("TOTALOFFERINGAMOUNT")),
            parse_numeric(r.get("TOTALAMOUNTSOLD")),
            parse_numeric(r.get("TOTALREMAINING")),
            parse_bool(r.get("HASNONACCREDITEDINVESTORS")),
            parse_int(r.get("NUMBERNONACCREDITEDINVESTORS")),
            parse_int(r.get("TOTALNUMBERALREADYINVESTED")),
            parse_numeric(r.get("SALESCOMM_DOLLARAMOUNT")),
            parse_numeric(r.get("FINDERSFEE_DOLLARAMOUNT")),
            parse_numeric(r.get("GROSSPROCEEDSUSED_DOLLARAMOUNT")),
        )


RELATED_PERSON_COLS = [
    "accession_number", "related_person_seq", "first_name", "middle_name", "last_name", "street1", "street2",
    "city", "state_or_country", "state_or_country_description", "zip_code", "relationships",
    "relationship_clarification",
]


def iter_related_persons(zip_path, skip: Set[str] = frozenset()) -> Iterator[Tuple]:
    for r in iter_tsv(zip_path, "RELATEDPERSONS.tsv"):
        acc = _acc(r)
        seq = parse_int(r.get("RELATEDPERSON_SEQ_KEY"))
        if not acc or acc in skip or seq is None:
            continue
        yield (
            acc, seq,
            clean(r.get("FIRSTNAME")),
            clean(r.get("MIDDLENAME")),
            clean(r.get("LASTNAME")),
            clean(r.get("STREET1")),
            clean(r.get("STREET2")),
            clean(r.get("CITY")),
            clean(r.get("STATEORCOUNTRY")),
            clean(r.get("STATEORCOUNTRYDESCRIPTION")),
            clean(r.get("ZIPCODE")),
            _non_empty(r.get(f"RELATIONSHIP_{i}") for i in (1, 2, 3)),
            clean(r.get("RELATIONSHIPCLARIFICATION")),
        )


RECIPIENT_COLS = ["accession_number", "recipient_seq", "name", "crd_number", "states"]


def iter_recipients(zip_path, skip: Set[str] = frozenset()) -> Iterator[Tuple]:
    for r in iter_tsv(zip_path, "RECIPIENTS.tsv", required=False):
        acc = _acc(r)
        seq = parse_int(r.get("RECIPIENT_SEQ_KEY"))
        if not acc or acc in skip or seq is None:
            continue
        yield (
            acc, seq,
            clean_none(r.get("RECIPIENTNAME")),
            clean_none(r.get("RECIPIENTCRDNUMBER")),
            split_list(clean_none(r.get("STATES_OR_VALUE_LIST"))),
        )


SIGNATURE_COLS = ["accession_number", "signature_seq", "issuer_name", "signature_name", "name_of_signer",
                  "signature_title", "signature_date"]


def iter_signatures(zip_path, skip: Set[str] = frozenset()) -> Iterator[Tuple]:
    for r in iter_tsv(zip_path, "SIGNATURES.tsv", required=False):
        acc = _acc(r)
        seq = parse_int(r.get("SIGNATURE_SEQ_KEY"))
        if not acc or acc in skip or seq is None:
            continue
        yield (
            acc, seq,
            clean(r.get("ISSUERNAME")),
            clean(r.get("SIGNATURENAME")),
            clean(r.get("NAMEOFSIGNER")),
            clean(r.get("SIGNATURETITLE")),
            parse_date(r.get("SIGNATUREDATE")),
        )
