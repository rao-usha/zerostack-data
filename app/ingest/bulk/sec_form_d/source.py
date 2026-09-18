"""
SEC Form D quarterly data sets -> Postgres (SPEC_108).

Index: https://www.sec.gov/data-research/sec-markets-data/form-d-data-sets
One release per quarter (``YYYYqN``). Default window: the newest 12 quarters.

Targets:
  - public.form_d_filings           (existing table; one row per LIVE accession with a primary issuer)
  - public.form_d_offerings         (full OFFERING detail, key accession_number)
  - public.form_d_issuers           (all issuers, key accession_number + issuer_seq)
  - public.form_d_related_persons   (key accession_number + related_person_seq)
  - public.form_d_signatures        (key accession_number + signature_seq)
"""

from __future__ import annotations

import calendar
import json
import logging
import re
from datetime import date
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional, Sequence, Tuple
from urllib.parse import urljoin

from sqlalchemy import text

from app.core.copy_loader import copy_rows, create_staging, drop_staging, merge_staging
from app.ingest.bulk.base import BulkSource, Release
from app.ingest.bulk.registry import register_bulk_source
from app.ingest.bulk.sec_form_d import parse as p

logger = logging.getLogger(__name__)

INDEX_URL = "https://www.sec.gov/data-research/sec-markets-data/form-d-data-sets"
# Path moved at 2026q2 (structureddata -> datastandardsinnovation); some old files end in `_d_0.zip`.
ZIP_HREF_RE = re.compile(r"""href\s*=\s*["']([^"']*?(\d{4})q([1-4])_d(?:_\d+)?\.zip)["']""", re.I)
DEFAULT_QUARTERS = 12

# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------

# Verbatim copy of app/sources/sec_form_d/ingest.py::_ensure_tables (do not alter).
FORM_D_FILINGS_DDL = [
    """
        CREATE TABLE IF NOT EXISTS form_d_filings (
            id SERIAL PRIMARY KEY,
            accession_number VARCHAR(25) NOT NULL UNIQUE,
            cik VARCHAR(10) NOT NULL,
            submission_type VARCHAR(10) NOT NULL,
            filed_at TIMESTAMP NOT NULL,
            issuer_name VARCHAR(500) NOT NULL,
            issuer_street VARCHAR(500),
            issuer_city VARCHAR(100),
            issuer_state VARCHAR(10),
            issuer_zip VARCHAR(20),
            issuer_phone VARCHAR(50),
            entity_type VARCHAR(50),
            jurisdiction VARCHAR(100),
            year_of_incorporation INTEGER,
            industry_group VARCHAR(100),
            revenue_range VARCHAR(50),
            related_persons JSONB,
            federal_exemptions JSONB,
            date_of_first_sale DATE,
            more_than_one_year BOOLEAN,
            is_equity BOOLEAN,
            is_debt BOOLEAN,
            is_option BOOLEAN,
            is_security_to_be_acquired BOOLEAN,
            is_pooled_investment_fund BOOLEAN,
            is_business_combination BOOLEAN,
            minimum_investment BIGINT,
            total_offering_amount BIGINT,
            total_amount_sold BIGINT,
            total_remaining BIGINT,
            total_number_already_invested INTEGER,
            accredited_investors INTEGER,
            non_accredited_investors INTEGER,
            sales_compensation JSONB,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """,
    "CREATE INDEX IF NOT EXISTS idx_form_d_cik ON form_d_filings(cik)",
    "CREATE INDEX IF NOT EXISTS idx_form_d_filed_at ON form_d_filings(filed_at)",
    "CREATE INDEX IF NOT EXISTS idx_form_d_issuer_name ON form_d_filings(issuer_name)",
    "CREATE INDEX IF NOT EXISTS idx_form_d_industry ON form_d_filings(industry_group)",
    "CREATE INDEX IF NOT EXISTS idx_form_d_exemptions ON form_d_filings USING GIN(federal_exemptions)",
]

FORM_D_ISSUERS_DDL = [
    """
    CREATE TABLE IF NOT EXISTS public.form_d_issuers (
        accession_number VARCHAR(25) NOT NULL,
        issuer_seq INTEGER NOT NULL,
        is_primary BOOLEAN NOT NULL DEFAULT FALSE,
        cik VARCHAR(10),
        entity_name TEXT,
        street1 TEXT,
        street2 TEXT,
        city TEXT,
        state_or_country TEXT,
        state_or_country_description TEXT,
        zip_code TEXT,
        phone TEXT,
        jurisdiction TEXT,
        entity_type TEXT,
        entity_type_other_desc TEXT,
        year_of_inc_timespan TEXT,
        year_of_inc_value INTEGER,
        issuer_previous_names TEXT[],
        edgar_previous_names TEXT[],
        source_release_key TEXT,
        loaded_at TIMESTAMP DEFAULT NOW(),
        PRIMARY KEY (accession_number, issuer_seq)
    )
    """,
    "CREATE INDEX IF NOT EXISTS ix_form_d_issuers_cik ON public.form_d_issuers (cik)",
]

FORM_D_RELATED_PERSONS_DDL = [
    """
    CREATE TABLE IF NOT EXISTS public.form_d_related_persons (
        accession_number VARCHAR(25) NOT NULL,
        related_person_seq INTEGER NOT NULL,
        first_name TEXT,
        middle_name TEXT,
        last_name TEXT,
        street1 TEXT,
        street2 TEXT,
        city TEXT,
        state_or_country TEXT,
        state_or_country_description TEXT,
        zip_code TEXT,
        relationships TEXT[],
        relationship_clarification TEXT,
        source_release_key TEXT,
        loaded_at TIMESTAMP DEFAULT NOW(),
        PRIMARY KEY (accession_number, related_person_seq)
    )
    """,
    "CREATE INDEX IF NOT EXISTS ix_form_d_related_persons_last_name "
    "ON public.form_d_related_persons (lower(last_name))",
]

FORM_D_OFFERINGS_DDL = [
    """
    CREATE TABLE IF NOT EXISTS public.form_d_offerings (
        accession_number VARCHAR(25) PRIMARY KEY,
        file_num TEXT,
        is_amendment BOOLEAN,
        previous_accession_number VARCHAR(25),
        industry_group_type TEXT,
        investment_fund_type TEXT,
        is_40_act BOOLEAN,
        revenue_range TEXT,
        aggregate_net_asset_value_range TEXT,
        federal_exemptions_items_list TEXT[],
        is_business_combination_transaction BOOLEAN,
        duration_of_offering_more_than_one_year BOOLEAN,
        date_of_first_sale DATE,
        yet_to_occur BOOLEAN,
        total_offering_amount NUMERIC,
        is_indefinite BOOLEAN NOT NULL DEFAULT FALSE,
        total_amount_sold NUMERIC,
        total_remaining NUMERIC,
        has_non_accredited_investors BOOLEAN,
        total_number_already_invested INTEGER,
        sales_commission_amount NUMERIC,
        finders_fees_amount NUMERIC,
        gross_proceeds_used_amount NUMERIC,
        minimum_investment_accepted NUMERIC,
        source_release_key TEXT,
        loaded_at TIMESTAMP DEFAULT NOW()
    )
    """,
    "CREATE INDEX IF NOT EXISTS ix_form_d_offerings_fund_type ON public.form_d_offerings (investment_fund_type)",
    "CREATE INDEX IF NOT EXISTS ix_form_d_offerings_first_sale ON public.form_d_offerings (date_of_first_sale)",
]

FORM_D_SIGNATURES_DDL = [
    """
    CREATE TABLE IF NOT EXISTS public.form_d_signatures (
        accession_number VARCHAR(25) NOT NULL,
        signature_seq INTEGER NOT NULL,
        issuer_name TEXT,
        signature_name TEXT,
        name_of_signer TEXT,
        signature_title TEXT,
        signature_date DATE,
        source_release_key TEXT,
        loaded_at TIMESTAMP DEFAULT NOW(),
        PRIMARY KEY (accession_number, signature_seq)
    )
    """,
]

# ---------------------------------------------------------------------------
# Staging layouts
# ---------------------------------------------------------------------------

STG_SUB = "sec_form_d_sub"
STG_ISS = "sec_form_d_iss"
STG_OFF = "sec_form_d_off"
STG_RP = "sec_form_d_rp"
STG_REC = "sec_form_d_rec"
STG_FILINGS = "sec_form_d_filings"
STG_OFFERINGS = "sec_form_d_offerings"
STG_SIG = "sec_form_d_sig"

SUB_TYPES = {"accession_number": "TEXT", "submission_type": "TEXT", "filed_at": "DATE"}
ISS_TYPES = {
    "issuer_seq": "INTEGER", "is_primary": "BOOLEAN", "year_of_inc_value": "INTEGER",
    "issuer_previous_names": "TEXT[]", "edgar_previous_names": "TEXT[]",
}
OFF_TYPES = {
    "is_40_act": "BOOLEAN", "federal_exemptions_items_list": "TEXT[]", "federal_exemptions": "JSONB",
    "is_amendment": "BOOLEAN", "date_of_first_sale": "DATE", "yet_to_occur": "BOOLEAN",
    "duration_of_offering_more_than_one_year": "BOOLEAN", "is_equity": "BOOLEAN", "is_debt": "BOOLEAN",
    "is_option": "BOOLEAN", "is_security_to_be_acquired": "BOOLEAN", "is_pooled_investment_fund": "BOOLEAN",
    "is_business_combination_transaction": "BOOLEAN", "minimum_investment_accepted": "NUMERIC",
    "total_offering_amount": "NUMERIC", "is_indefinite": "BOOLEAN", "total_amount_sold": "NUMERIC",
    "total_remaining": "NUMERIC", "has_non_accredited_investors": "BOOLEAN", "non_accredited_investors": "INTEGER",
    "total_number_already_invested": "INTEGER", "sales_commission_amount": "NUMERIC",
    "finders_fees_amount": "NUMERIC", "gross_proceeds_used_amount": "NUMERIC",
}
SIG_TYPES = {"signature_seq": "INTEGER", "signature_date": "DATE"}
RP_TYPES = {"related_person_seq": "INTEGER", "relationships": "TEXT[]"}
REC_TYPES = {"recipient_seq": "INTEGER", "states": "TEXT[]"}

FILINGS_COLS: List[Tuple[str, str]] = [
    ("accession_number", "VARCHAR(25)"), ("cik", "VARCHAR(10)"), ("submission_type", "VARCHAR(10)"),
    ("filed_at", "TIMESTAMP"), ("issuer_name", "VARCHAR(500)"), ("issuer_street", "VARCHAR(500)"),
    ("issuer_city", "VARCHAR(100)"), ("issuer_state", "VARCHAR(10)"), ("issuer_zip", "VARCHAR(20)"),
    ("issuer_phone", "VARCHAR(50)"), ("entity_type", "VARCHAR(50)"), ("jurisdiction", "VARCHAR(100)"),
    ("year_of_incorporation", "INTEGER"), ("industry_group", "VARCHAR(100)"), ("revenue_range", "VARCHAR(50)"),
    ("related_persons", "JSONB"), ("federal_exemptions", "JSONB"), ("date_of_first_sale", "DATE"),
    ("more_than_one_year", "BOOLEAN"), ("is_equity", "BOOLEAN"), ("is_debt", "BOOLEAN"), ("is_option", "BOOLEAN"),
    ("is_security_to_be_acquired", "BOOLEAN"), ("is_pooled_investment_fund", "BOOLEAN"),
    ("is_business_combination", "BOOLEAN"), ("minimum_investment", "BIGINT"), ("total_offering_amount", "BIGINT"),
    ("total_amount_sold", "BIGINT"), ("total_remaining", "BIGINT"), ("total_number_already_invested", "INTEGER"),
    ("accredited_investors", "INTEGER"), ("non_accredited_investors", "INTEGER"), ("sales_compensation", "JSONB"),
    ("updated_at", "TIMESTAMP"),
]

OFFERINGS_COLS: List[Tuple[str, str]] = [
    ("accession_number", "VARCHAR(25)"), ("file_num", "TEXT"), ("is_amendment", "BOOLEAN"),
    ("previous_accession_number", "VARCHAR(25)"), ("industry_group_type", "TEXT"),
    ("investment_fund_type", "TEXT"), ("is_40_act", "BOOLEAN"), ("revenue_range", "TEXT"),
    ("aggregate_net_asset_value_range", "TEXT"), ("federal_exemptions_items_list", "TEXT[]"),
    ("is_business_combination_transaction", "BOOLEAN"), ("duration_of_offering_more_than_one_year", "BOOLEAN"),
    ("date_of_first_sale", "DATE"), ("yet_to_occur", "BOOLEAN"), ("total_offering_amount", "NUMERIC"),
    ("is_indefinite", "BOOLEAN"), ("total_amount_sold", "NUMERIC"), ("total_remaining", "NUMERIC"),
    ("has_non_accredited_investors", "BOOLEAN"), ("total_number_already_invested", "INTEGER"),
    ("sales_commission_amount", "NUMERIC"), ("finders_fees_amount", "NUMERIC"),
    ("gross_proceeds_used_amount", "NUMERIC"), ("minimum_investment_accepted", "NUMERIC"),
    ("source_release_key", "TEXT"), ("loaded_at", "TIMESTAMP"),
]

# Full OFFERING detail + FILE_NUM from the submission. Static SQL; release key is a bind param.
BUILD_OFFERINGS_SQL = f"""
INSERT INTO stg.{STG_OFFERINGS} ({", ".join(c for c, _ in OFFERINGS_COLS)})
SELECT
    LEFT(o.accession_number, 25), s.file_num, o.is_amendment, LEFT(o.previous_accession_number, 25),
    o.industry_group_type, o.investment_fund_type, o.is_40_act, o.revenue_range,
    o.aggregate_net_asset_value_range, o.federal_exemptions_items_list, o.is_business_combination_transaction,
    o.duration_of_offering_more_than_one_year, o.date_of_first_sale, o.yet_to_occur, o.total_offering_amount,
    COALESCE(o.is_indefinite, FALSE), o.total_amount_sold, o.total_remaining, o.has_non_accredited_investors,
    o.total_number_already_invested, o.sales_commission_amount, o.finders_fees_amount,
    o.gross_proceeds_used_amount, o.minimum_investment_accepted, :release_key, NOW()
FROM stg.{STG_OFF} o
LEFT JOIN (SELECT DISTINCT ON (accession_number) accession_number, file_num
           FROM stg.{STG_SUB} ORDER BY accession_number, _row_num DESC) s
       ON s.accession_number = o.accession_number
ORDER BY o._row_num
"""


def _bigint(expr: str) -> str:
    """NUMERIC staging amount -> BIGINT for the legacy table (NULL when out of range)."""
    return f"CASE WHEN {expr} BETWEEN -9223372036854775808 AND 9223372036854775807 THEN trunc({expr})::bigint END"


# Joins staged submission + primary issuer + offering + aggregated persons/recipients into the
# existing form_d_filings shape. Static SQL: only fixed stg.* identifiers, no user input.
BUILD_FILINGS_SQL = f"""
INSERT INTO stg.{STG_FILINGS} ({", ".join(c for c, _ in FILINGS_COLS)})
WITH primary_issuer AS (
    SELECT DISTINCT ON (accession_number) *
    FROM stg.{STG_ISS}
    WHERE is_primary AND cik IS NOT NULL AND entity_name IS NOT NULL
    ORDER BY accession_number, issuer_seq
),
rp AS (
    SELECT accession_number,
           jsonb_agg(jsonb_build_object(
               'first_name', first_name,
               'last_name', last_name,
               'relationship', to_jsonb(COALESCE(relationships, ARRAY[]::TEXT[]))
           ) ORDER BY related_person_seq) AS persons
    FROM (SELECT DISTINCT ON (accession_number, related_person_seq) *
          FROM stg.{STG_RP} ORDER BY accession_number, related_person_seq, _row_num DESC) d
    GROUP BY accession_number
),
rec AS (
    SELECT accession_number,
           jsonb_agg(jsonb_build_object(
               'name', name,
               'crd_number', crd_number,
               'states', to_jsonb(COALESCE(states, ARRAY[]::TEXT[]))
           ) ORDER BY recipient_seq) AS recipients
    FROM (SELECT DISTINCT ON (accession_number, recipient_seq) *
          FROM stg.{STG_REC} ORDER BY accession_number, recipient_seq, _row_num DESC) d
    GROUP BY accession_number
)
SELECT
    LEFT(s.accession_number, 25),
    LEFT(i.cik, 10),
    LEFT(s.submission_type, 10),
    s.filed_at::timestamp,
    LEFT(i.entity_name, 500),
    LEFT(i.street1, 500),
    LEFT(i.city, 100),
    LEFT(i.state_or_country, 10),
    LEFT(i.zip_code, 20),
    LEFT(i.phone, 50),
    LEFT(i.entity_type, 50),
    LEFT(i.jurisdiction, 100),
    i.year_of_inc_value,
    LEFT(o.industry_group_type, 100),
    LEFT(o.revenue_range, 50),
    COALESCE(rp.persons, '[]'::jsonb),
    COALESCE(o.federal_exemptions, '[]'::jsonb),
    o.date_of_first_sale,
    COALESCE(o.duration_of_offering_more_than_one_year, FALSE),
    COALESCE(o.is_equity, FALSE),
    COALESCE(o.is_debt, FALSE),
    COALESCE(o.is_option, FALSE),
    COALESCE(o.is_security_to_be_acquired, FALSE),
    COALESCE(o.is_pooled_investment_fund, FALSE),
    COALESCE(o.is_business_combination_transaction, FALSE),
    {_bigint("o.minimum_investment_accepted")},
    {_bigint("o.total_offering_amount")},
    {_bigint("o.total_amount_sold")},
    {_bigint("o.total_remaining")},
    o.total_number_already_invested,
    NULL::integer,
    o.non_accredited_investors,
    COALESCE(rec.recipients, '[]'::jsonb),
    NOW()
FROM (SELECT DISTINCT ON (accession_number) *
      FROM stg.{STG_SUB} ORDER BY accession_number, _row_num DESC) s
JOIN primary_issuer i ON i.accession_number = s.accession_number
LEFT JOIN (SELECT DISTINCT ON (accession_number) *
           FROM stg.{STG_OFF} ORDER BY accession_number, _row_num DESC) o
       ON o.accession_number = s.accession_number
LEFT JOIN rp ON rp.accession_number = s.accession_number
LEFT JOIN rec ON rec.accession_number = s.accession_number
WHERE s.submission_type IS NOT NULL AND s.filed_at IS NOT NULL
"""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def pg_text_array(values: Optional[Sequence[str]]) -> Optional[str]:
    """Python list -> Postgres TEXT[] literal for COPY."""
    if values is None:
        return None
    parts = []
    for v in values:
        parts.append('"' + str(v).replace("\\", "\\\\").replace('"', '\\"') + '"')
    return "{" + ",".join(parts) + "}"


def _staging_cols(cols: Sequence[str], types: Dict[str, str]) -> List[Tuple[str, str]]:
    return [(c, types.get(c, "TEXT")) for c in cols]


def _encode(rows: Iterable[Tuple], cols: Sequence[str], types: Dict[str, str],
            extra: Tuple = ()) -> Iterator[Tuple]:
    """Convert list values to TEXT[] / JSONB literals; append constant extra values."""
    kinds = [types.get(c, "TEXT") for c in cols]
    for row in rows:
        out = []
        for v, kind in zip(row, kinds):
            if v is not None and kind == "TEXT[]":
                v = pg_text_array(v)
            elif v is not None and kind == "JSONB":
                v = json.dumps(v)
            out.append(v)
        yield tuple(out) + extra


def quarter_end(year: int, quarter: int) -> date:
    month = 3 * quarter
    return date(year, month, calendar.monthrange(year, month)[1])


# ---------------------------------------------------------------------------
# Source
# ---------------------------------------------------------------------------

@register_bulk_source
class SecFormDDataSets(BulkSource):
    name = "sec_form_d"
    parser_version = "1"

    def __init__(self) -> None:
        self.last_stats: Dict[str, Tuple[int, int]] = {}

    # -- discovery --------------------------------------------------------
    def discover(self, http, since: Optional[date] = None) -> List[Release]:
        html = http.get_text(INDEX_URL)
        found: Dict[Tuple[int, int], str] = {}
        for href, year, quarter in ZIP_HREF_RE.findall(html):
            key = (int(year), int(quarter))
            url = urljoin("https://www.sec.gov/", href.strip())
            # Prefer the plain `_d.zip` name if both it and a `_d_0.zip` variant are listed.
            if key not in found or found[key].lower().endswith("_0.zip"):
                found[key] = url
        if not found:
            raise RuntimeError(f"no Form D data set zip links found on {INDEX_URL}")

        keys = sorted(found)
        if since is None:
            keys = keys[-DEFAULT_QUARTERS:]
        else:
            keys = [k for k in keys if quarter_end(*k) >= since]
        return [
            Release(
                release_key=f"{y}q{q}",
                url=found[(y, q)],
                meta={"year": y, "quarter": q, "quarter_end": quarter_end(y, q).isoformat()},
            )
            for (y, q) in keys
        ]

    # -- DDL --------------------------------------------------------------
    def ddl(self) -> List[str]:
        return (FORM_D_FILINGS_DDL + FORM_D_OFFERINGS_DDL + FORM_D_ISSUERS_DDL
                + FORM_D_RELATED_PERSONS_DDL + FORM_D_SIGNATURES_DDL)

    # -- load -------------------------------------------------------------
    def load(self, conn, release: Release, path: Path) -> Dict[str, int]:
        self.ensure_ddl(conn)
        path = Path(path)
        rk = release.release_key
        skip = p.non_live_accessions(path)
        if skip:
            logger.info(f"[bulk:sec_form_d] {rk}: skipping {len(skip)} non-LIVE submissions")

        stats: Dict[str, Tuple[int, int]] = {}
        staged = [STG_SUB, STG_ISS, STG_OFF, STG_RP, STG_REC, STG_SIG, STG_FILINGS, STG_OFFERINGS]

        # 1) stage everything
        create_staging(conn, STG_SUB, _staging_cols(p.SUBMISSION_COLS, SUB_TYPES))
        n_sub = copy_rows(conn, STG_SUB, p.SUBMISSION_COLS, p.iter_submissions(path))

        iss_cols = p.ISSUER_COLS + ["source_release_key"]
        create_staging(conn, STG_ISS, _staging_cols(p.ISSUER_COLS, ISS_TYPES)
                       + [("source_release_key", "TEXT"), ("loaded_at", "TIMESTAMP DEFAULT NOW()")])
        copy_rows(conn, STG_ISS, iss_cols,
                  _encode(p.iter_issuers(path, skip), p.ISSUER_COLS, ISS_TYPES, (rk,)))

        create_staging(conn, STG_OFF, _staging_cols(p.OFFERING_COLS, OFF_TYPES))
        copy_rows(conn, STG_OFF, p.OFFERING_COLS,
                  _encode(p.iter_offerings(path, skip), p.OFFERING_COLS, OFF_TYPES))

        rp_cols = p.RELATED_PERSON_COLS + ["source_release_key"]
        create_staging(conn, STG_RP, _staging_cols(p.RELATED_PERSON_COLS, RP_TYPES)
                       + [("source_release_key", "TEXT"), ("loaded_at", "TIMESTAMP DEFAULT NOW()")])
        copy_rows(conn, STG_RP, rp_cols,
                  _encode(p.iter_related_persons(path, skip), p.RELATED_PERSON_COLS, RP_TYPES, (rk,)))

        create_staging(conn, STG_REC, _staging_cols(p.RECIPIENT_COLS, REC_TYPES))
        copy_rows(conn, STG_REC, p.RECIPIENT_COLS,
                  _encode(p.iter_recipients(path, skip), p.RECIPIENT_COLS, REC_TYPES))

        sig_cols = p.SIGNATURE_COLS + ["source_release_key"]
        create_staging(conn, STG_SIG, _staging_cols(p.SIGNATURE_COLS, SIG_TYPES)
                       + [("source_release_key", "TEXT"), ("loaded_at", "TIMESTAMP DEFAULT NOW()")])
        copy_rows(conn, STG_SIG, sig_cols,
                  _encode(p.iter_signatures(path, skip), p.SIGNATURE_COLS, SIG_TYPES, (rk,)))

        if n_sub == 0:
            raise ValueError(f"{path.name}: FORMDSUBMISSION.tsv had no LIVE rows")

        # 2) merge child tables
        merge_iss = iss_cols + ["loaded_at"]
        stats["form_d_issuers"] = merge_staging(
            conn, STG_ISS, "public.form_d_issuers", merge_iss, ["accession_number", "issuer_seq"])
        merge_rp = rp_cols + ["loaded_at"]
        stats["form_d_related_persons"] = merge_staging(
            conn, STG_RP, "public.form_d_related_persons", merge_rp, ["accession_number", "related_person_seq"])

        stats["form_d_signatures"] = merge_staging(
            conn, STG_SIG, "public.form_d_signatures", sig_cols + ["loaded_at"],
            ["accession_number", "signature_seq"])

        create_staging(conn, STG_OFFERINGS, OFFERINGS_COLS)
        conn.execute(text(BUILD_OFFERINGS_SQL), {"release_key": rk})
        stats["form_d_offerings"] = merge_staging(
            conn, STG_OFFERINGS, "public.form_d_offerings", [c for c, _ in OFFERINGS_COLS], ["accession_number"])

        # 3) build + merge filings
        create_staging(conn, STG_FILINGS, FILINGS_COLS)
        conn.execute(text(BUILD_FILINGS_SQL))
        stats["form_d_filings"] = merge_staging(
            conn, STG_FILINGS, "public.form_d_filings", [c for c, _ in FILINGS_COLS], ["accession_number"])

        for name in staged:
            drop_staging(conn, name)

        self.last_stats = stats
        logger.info(f"[bulk:sec_form_d] {rk}: (inserted, updated) {stats}")
        return {table: ins + upd for table, (ins, upd) in stats.items()}
