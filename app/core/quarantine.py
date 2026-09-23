"""
Reversible quarantine of bad rows (PLAN_082 / SPEC_105).

Moves rows out of live ``public`` tables into the ``quarantine`` or ``demo``
schema instead of deleting them. For every rule:

1. Root ids for *all* rules are captured up front (some predicates reference
   rows that an earlier rule moves).
2. Rows referencing the root rows through a foreign key (NO ACTION / RESTRICT /
   CASCADE) are moved first, recursively. SET NULL references are backed up so
   ``revert`` can restore them.
3. Root rows are copied into ``<schema>.<table>`` with ``_q_seq``, ``_q_rule``,
   ``_q_reason`` and ``_q_at`` columns, then deleted from ``public``.

Every move is recorded in ``quarantine.manifest``; ``revert`` replays it in
reverse order. Identifiers go through ``safe_sql.qi``; values are bound params.
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from typing import Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.engine import Connection

from app.core.safe_sql import qi

logger = logging.getLogger(__name__)

TOLERANCE = 1.05
# A runaway-predicate backstop for the ad-hoc Phase 0 sweeps, counted across
# every table one apply() touches. A deliberate, measured rollback of a whole
# SEC mart is legitimately larger than this -- SEC_MART_RULES alone resolves to
# ~39k pe_funds rows today -- so apply() takes an explicit override. The guard
# stays low by default precisely so that raising it has to be a decision.
MAX_TOTAL_ROWS = 25_000
MAX_DEPTH = 6
SCHEMAS = ("quarantine", "demo")
META_COLUMNS = ("_q_seq", "_q_rule", "_q_reason", "_q_at")


class QuarantineGuardError(RuntimeError):
    """Raised when live counts exceed what was verified."""


@dataclass(frozen=True)
class QuarantineRule:
    name: str
    table: str
    predicate: str  # SQL boolean expression over the table's columns (trusted, static)
    target_schema: str
    expected: int
    reason: str


_TITLE_NAMES = (
    "'cfo','coo','investor relations','managing director','operating partner',"
    "'private equity','real estate','senior vice president','vice president'"
)

# Verified against the cloud DB on 2026-09-16 (docs/plans/PLAN_082).
RULES: List[QuarantineRule] = [
    # --- misclassified PE data -------------------------------------------------
    QuarantineRule(
        "pe_deals_8k_false_positive", "pe_deals",
        "data_source = 'SEC 8-K' AND deal_type = '8-K Event'",
        "quarantine", 740, "8-K full-text-search hit stored as a deal (D9)",
    ),
    QuarantineRule(
        "pe_companies_8k_filer_only", "pe_portfolio_companies",
        "ownership_status IS NULL AND data_source IS NULL "
        "AND id IN (SELECT company_id FROM pe_deals WHERE data_source = 'SEC 8-K') "
        "AND NOT EXISTS (SELECT 1 FROM pe_deals d WHERE d.company_id = pe_portfolio_companies.id "
        "AND d.data_source IS DISTINCT FROM 'SEC 8-K')",
        "quarantine", 341, "8-K filer saved as a portfolio company (D9)",
    ),
    QuarantineRule(
        "pe_fund_investments_13f", "pe_fund_investments",
        "investment_type = '13F Holding'",
        "quarantine", 3621, "13F public position stored as a fund investment (D10)",
    ),
    QuarantineRule(
        "pe_funds_13f_synthetic", "pe_funds",
        "strategy = '13F Reported Holdings'",
        "quarantine", 27, "synthetic '13F Holdings' fund (D10)",
    ),
    QuarantineRule(
        "pe_companies_13f", "pe_portfolio_companies",
        "ownership_status = '13F Reported'",
        "quarantine", 1069, "13F issuer stored as a portfolio company (D10)",
    ),
    QuarantineRule(
        "pe_people_job_titles", "pe_people",
        f"lower(trim(full_name)) IN ({_TITLE_NAMES})",
        "quarantine", 7, "job title stored as a person (D14)",
    ),
    # --- fabricated site_intel logistics rows (D7) ---------------------------
    QuarantineRule("motor_carrier_sample", "motor_carrier", "source = 'fmcsa'",
                   "quarantine", 10, "fabricated sample carriers (D7)"),
    QuarantineRule("carrier_safety_sample", "carrier_safety", "source = 'fmcsa'",
                   "quarantine", 60, "random sample safety scores (D7)"),
    QuarantineRule("warehouse_listing_sample", "warehouse_listing", "source = 'loopnet'",
                   "quarantine", 60, "random sample listings (D7)"),
    QuarantineRule("container_freight_index_sample", "container_freight_index",
                   "source IN ('scfi', 'freightos_fbx', 'drewry_wci')",
                   "quarantine", 982, "sample freight rates (D7)"),
    QuarantineRule("trade_gateway_stats_sample", "trade_gateway_stats", "source = 'census'",
                   "quarantine", 340, "sample trade gateway stats (D7)"),
    QuarantineRule("port_throughput_monthly_sample", "port_throughput_monthly",
                   "source = 'bts_usace'", "quarantine", 323, "sample port throughput (D7)"),
    QuarantineRule("air_cargo_stats_sample", "air_cargo_stats", "source = 'bts_t100'",
                   "quarantine", 391, "sample air cargo stats (D7)"),
    QuarantineRule("usda_truck_rate_sample", "usda_truck_rate", "source = 'usda_ams'",
                   "quarantine", 120, "sample truck rates (D7)"),
    QuarantineRule("incentive_deal_seed", "incentive_deal", "source = 'gjf_seed'",
                   "quarantine", 28, "hard-coded seed subsidy deals (D7)"),
    # --- LP / family office junk ---------------------------------------------
    QuarantineRule("lp_key_contact_regex", "lp_key_contact", "source_type = 'website'",
                   "quarantine", 3174, "regex-extracted LP contacts, ~89% junk (D12)"),
    QuarantineRule("portfolio_companies_lp_13f", "portfolio_companies",
                   "investor_type = 'lp' AND source_type = 'sec_13f'",
                   "quarantine", 4464, "LP 13F rows with wrong CIKs and x1000 values (D11)"),
    QuarantineRule("family_office_investment_headlines", "family_office_investment",
                   "source_type = 'news'", "quarantine", 26,
                   "headline fragments stored as FO investments (D13)"),
    QuarantineRule("form_adv_advisers_sample", "form_adv_advisers", "data_source = 'sample'",
                   "quarantine", 10, "fabricated sample advisers (D16)"),
    # --- demo data (D8) -> demo schema ---------------------------------------
    QuarantineRule("demo_company_financials", "pe_company_financials",
                   "data_source IN ('demo_seeder', 'Demo Seed', 'Industry estimate')",
                   "demo", 262, "demo/seeded financials (D8)"),
    QuarantineRule("demo_company_valuations", "pe_company_valuations",
                   "data_source IN ('demo_seeder', 'Demo Seed')",
                   "demo", 45, "demo/seeded valuations (D8)"),
    QuarantineRule("demo_cash_flows", "pe_cash_flows", "data_source = 'demo_seeder'",
                   "demo", 66, "demo cash flows (D8)"),
    QuarantineRule("demo_fund_performance", "pe_fund_performance", "data_source = 'demo_seeder'",
                   "demo", 9, "demo fund performance (D8)"),
    QuarantineRule("demo_funds", "pe_funds", "data_source = 'demo_seeder'",
                   "demo", 6, "demo funds (D8)"),
    QuarantineRule("demo_companies", "pe_portfolio_companies", "data_source = 'demo_seeder'",
                   "demo", 57, "demo portfolio companies (D8)"),
    QuarantineRule("demo_people", "pe_people", "CAST(data_sources AS TEXT) LIKE '%demo_seeder%'",
                   "demo", 75, "demo people (D8)"),
    QuarantineRule("demo_firms", "pe_firms", "CAST(data_sources AS TEXT) LIKE '%demo_seeder%'",
                   "demo", 3, "demo firms (D8)"),
]

# Not part of the Phase 0 sweep: these undo the SPEC_117 SEC load. They are
# applied only when passed explicitly (scripts/quarantine_dry_run.py --rules sec).
SEC_MART_RULES: List[QuarantineRule] = [
    QuarantineRule("sec_form_d_funds", "pe_funds", "data_source = 'SEC Form D'",
                   "quarantine", 40000, "funds loaded from Form D (SPEC_117)"),
    QuarantineRule("sec_adv_firms", "pe_firms", "CAST(data_sources AS TEXT) LIKE '%SEC ADV%'",
                   "quarantine", 8000, "firms loaded from Form ADV (SPEC_117)"),
    # pe_firm_people follows through the FK walk, so it needs no rule of its
    # own. Rolling this group back moves ~60k rows, well over the default
    # MAX_TOTAL_ROWS -- pass an explicit max_total, which is the point of it.
    QuarantineRule("sec_form_d_people", "pe_people", "source_key LIKE 'secformd:%'",
                   "quarantine", 12000, "people from Form D related persons (SPEC_119)"),
]


# ---------------------------------------------------------------------------
# Guards
# ---------------------------------------------------------------------------

def check_guard(rule: QuarantineRule, live_count: int) -> None:
    """Raise if the live count exceeds what was verified (plus tolerance)."""
    if live_count > rule.expected * TOLERANCE:
        raise QuarantineGuardError(
            f"{rule.name}: {live_count} live rows > expected {rule.expected} "
            f"(+{int((TOLERANCE - 1) * 100)}%). Re-verify before quarantining."
        )


# ---------------------------------------------------------------------------
# Catalog helpers
# ---------------------------------------------------------------------------

def _pk_column(conn: Connection, table: str) -> str:
    rows = conn.execute(
        text(
            """
            SELECT a.attname FROM pg_index i
            JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
            WHERE i.indrelid = to_regclass(:t) AND i.indisprimary
            """
        ),
        {"t": f"public.{table}"},
    ).fetchall()
    if len(rows) != 1:
        raise RuntimeError(f"public.{table} needs a single-column primary key, got {rows}")
    return rows[0][0]


def _fk_children(conn: Connection, table: str) -> List[Dict[str, str]]:
    """Single-column FKs in public referencing public.<table>."""
    rows = conn.execute(
        text(
            """
            SELECT cl.relname AS child, a.attname AS col, c.confdeltype AS deltype
            FROM pg_constraint c
            JOIN pg_class cl ON cl.oid = c.conrelid
            JOIN pg_namespace n ON n.oid = cl.relnamespace
            JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = c.conkey[1]
            WHERE c.contype = 'f'
              AND c.confrelid = to_regclass(:t)
              AND n.nspname = 'public'
              AND array_length(c.conkey, 1) = 1
            ORDER BY cl.relname, a.attname
            """
        ),
        {"t": f"public.{table}"},
    ).fetchall()
    return [{"child": r[0], "col": r[1], "deltype": r[2]} for r in rows]


def _ensure_meta(conn: Connection) -> None:
    for schema in SCHEMAS:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {qi(schema)}"))
    conn.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS quarantine.manifest (
                seq SERIAL PRIMARY KEY,
                rule TEXT NOT NULL,
                target_schema TEXT NOT NULL,
                table_name TEXT NOT NULL,
                row_count INTEGER NOT NULL,
                moved_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
    )
    conn.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS quarantine.set_null_backup (
                manifest_seq INTEGER NOT NULL,
                table_name TEXT NOT NULL,
                column_name TEXT NOT NULL,
                row_id BIGINT NOT NULL,
                old_value BIGINT NOT NULL
            )
            """
        )
    )


def _column_types(conn: Connection, schema: str, table: str) -> Dict[str, str]:
    """{column: sql type} for a live table, or {} when it does not exist."""
    return {
        r[0]: r[1]
        for r in conn.execute(
            text(
                "SELECT a.attname, format_type(a.atttypid, a.atttypmod) "
                "FROM pg_attribute a "
                "WHERE a.attrelid = to_regclass(:rel) AND a.attnum > 0 "
                "AND NOT a.attisdropped"
            ),
            {"rel": f"{schema}.{table}"},
        ).fetchall()
    }


def _ensure_target(conn: Connection, schema: str, table: str) -> None:
    target = f"{qi(schema)}.{qi(table)}"
    conn.execute(text(f"CREATE TABLE IF NOT EXISTS {target} (LIKE public.{qi(table)})"))

    # `CREATE TABLE IF NOT EXISTS ... (LIKE ...)` copies the shape only when it
    # actually creates the table. A shadow left behind by an earlier sweep keeps
    # the column set it was born with, so every migration that adds a column to
    # the public table since then breaks the move with "column ... does not
    # exist" -- and it breaks it at rollback time, which is exactly when the
    # rollback is needed. Reconcile instead of assuming.
    live = _column_types(conn, "public", table)
    shadow = _column_types(conn, schema, table)
    missing = [(c, t) for c, t in live.items() if c not in shadow]
    if missing:
        adds = ", ".join(f"ADD COLUMN IF NOT EXISTS {qi(c)} {t}" for c, t in missing)
        conn.execute(text(f"ALTER TABLE {target} {adds}"))
        logger.info(
            f"[quarantine] {target}: added {len(missing)} column(s) the shadow "
            f"was missing ({', '.join(c for c, _ in missing)})"
        )

    # A shadow is an archive, not a live table: it has to accept whatever the
    # public table held at the moment of the move. `LIKE` copies NOT NULL, and
    # those constraints then drift -- `quarantine.pe_funds` was born when
    # `firm_id` was NOT NULL, SPEC_117 made it nullable, and the move then died
    # on the first unattributed fund. The live table keeps its constraints;
    # restoring through `revert()` is what re-checks them.
    notnull = conn.execute(
        text(
            "SELECT a.attname FROM pg_attribute a "
            "WHERE a.attrelid = to_regclass(:rel) AND a.attnum > 0 "
            "AND NOT a.attisdropped AND a.attnotnull"
        ),
        {"rel": f"{schema}.{table}"},
    ).scalars().all()
    for column in notnull:
        conn.execute(text(f"ALTER TABLE {target} ALTER COLUMN {qi(column)} DROP NOT NULL"))

    conn.execute(
        text(
            f"ALTER TABLE {target} "
            "ADD COLUMN IF NOT EXISTS _q_seq INTEGER, "
            "ADD COLUMN IF NOT EXISTS _q_rule TEXT, "
            "ADD COLUMN IF NOT EXISTS _q_reason TEXT, "
            "ADD COLUMN IF NOT EXISTS _q_at TIMESTAMPTZ DEFAULT NOW()"
        )
    )


def _public_columns(conn: Connection, table: str) -> List[str]:
    rows = conn.execute(
        text(
            """
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = :t
            ORDER BY ordinal_position
            """
        ),
        {"t": table},
    ).fetchall()
    return [r[0] for r in rows]


# ---------------------------------------------------------------------------
# Move
# ---------------------------------------------------------------------------

class _Mover:
    def __init__(self, conn: Connection, max_total: int = MAX_TOTAL_ROWS):
        self.conn = conn
        self.max_total = max_total
        self.report: List[Dict] = []
        self.total = 0
        self._tmp = 0
        self._run = uuid.uuid4().hex[:8]  # unique per apply() within a transaction

    def _temp_ids(self, table: str, where_sql: str, params: Optional[dict] = None) -> str:
        self._tmp += 1
        name = f"_q_ids_{self._run}_{self._tmp}"
        pk = _pk_column(self.conn, table)
        self.conn.execute(
            text(
                f"CREATE TEMP TABLE {qi(name)} ON COMMIT DROP AS "
                f"SELECT {qi(pk)} AS id FROM public.{qi(table)} WHERE {where_sql}"
            ),
            params or {},
        )
        return name

    def _count(self, tmp: str) -> int:
        return self.conn.execute(text(f"SELECT COUNT(*) FROM {qi(tmp)}")).scalar() or 0

    def move(self, rule: QuarantineRule, table: str, ids_tmp: str, depth: int = 0) -> int:
        if depth > MAX_DEPTH:
            raise RuntimeError(f"FK recursion too deep at {table} for {rule.name}")
        if self._count(ids_tmp) == 0:
            return 0

        pk = _pk_column(self.conn, table)
        set_null_refs = []  # (child, col, child_pk, where) backed up under this move's seq
        # 1) children first
        for fk in _fk_children(self.conn, table):
            if fk["child"] == table:
                continue  # self-reference: rows move together
            where = f"{qi(fk['col'])} IN (SELECT id FROM {qi(ids_tmp)})"
            if fk["deltype"] == "n":  # ON DELETE SET NULL: keep the row, back up the link
                child_pk = _pk_column(self.conn, fk["child"])
                set_null_refs.append((fk["child"], fk["col"], child_pk, where))
                continue
            child_tmp = self._temp_ids(fk["child"], where)
            self.move(rule, fk["child"], child_tmp, depth + 1)

        # 2) the rows themselves
        _ensure_target(self.conn, rule.target_schema, table)
        seq = self.conn.execute(
            text(
                "INSERT INTO quarantine.manifest (rule, target_schema, table_name, row_count) "
                "VALUES (:r, :s, :t, 0) RETURNING seq"
            ),
            {"r": rule.name, "s": rule.target_schema, "t": table},
        ).scalar()

        for child, col, child_pk, where in set_null_refs:
            self.conn.execute(
                text(
                    "INSERT INTO quarantine.set_null_backup "
                    "(manifest_seq, table_name, column_name, row_id, old_value) "
                    f"SELECT :seq, :t, :c, {qi(child_pk)}, {qi(col)} "
                    f"FROM public.{qi(child)} WHERE {where}"
                ),
                {"seq": seq, "t": child, "c": col},
            )

        cols = ", ".join(qi(c) for c in _public_columns(self.conn, table))
        target = f"{qi(rule.target_schema)}.{qi(table)}"
        moved = self.conn.execute(
            text(
                f"WITH gone AS (DELETE FROM public.{qi(table)} "
                f"WHERE {qi(pk)} IN (SELECT id FROM {qi(ids_tmp)}) RETURNING *) "
                f"INSERT INTO {target} ({cols}, _q_seq, _q_rule, _q_reason) "
                f"SELECT {cols}, :seq, :rule, :reason FROM gone"
            ),
            {"seq": seq, "rule": rule.name, "reason": rule.reason},
        ).rowcount or 0
        self.conn.execute(
            text("UPDATE quarantine.manifest SET row_count = :n WHERE seq = :seq"),
            {"n": moved, "seq": seq},
        )
        self.total += moved
        if self.total > self.max_total:
            raise QuarantineGuardError(
                f"Total moved rows {self.total} exceed max_total={self.max_total}. "
                "Pass apply(..., max_total=N) if this rollback is meant to be this big."
            )
        self.report.append(
            {"rule": rule.name, "schema": rule.target_schema, "table": table,
             "depth": depth, "rows": moved}
        )
        return moved


def apply(conn: Connection, rules: List[QuarantineRule] = RULES,
          max_total: int = MAX_TOTAL_ROWS) -> Dict:
    """Quarantine rows for ``rules``. Caller owns the transaction.

    ``max_total`` caps the rows moved across all rules in this call. The
    default is deliberately smaller than a full mart, so rolling one back is
    an explicit act rather than something a stray predicate can do by accident.
    """
    _ensure_meta(conn)
    mover = _Mover(conn, max_total=max_total)

    captured = []
    roots = []
    for rule in rules:
        # A table that was never created holds no bad rows. Without this a
        # blank database (CI, a new deployment) cannot migrate past 0003,
        # because several rule tables are created by their ingestors, not
        # by create_all() (SPEC_129).
        if not conn.execute(text("SELECT to_regclass(:t)"), {"t": f"public.{rule.table}"}).scalar():
            roots.append({"rule": rule.name, "table": rule.table, "expected": rule.expected,
                          "live": 0, "absent": True})
            continue
        tmp = mover._temp_ids(rule.table, f"({rule.predicate})")
        live = mover._count(tmp)
        check_guard(rule, live)
        captured.append((rule, tmp))
        roots.append({"rule": rule.name, "table": rule.table, "expected": rule.expected,
                      "live": live})

    for rule, tmp in captured:
        mover.move(rule, rule.table, tmp)

    return {"roots": roots, "moves": mover.report, "total_rows": mover.total}


def revert(conn: Connection) -> int:
    """Restore everything recorded in quarantine.manifest (newest move first)."""
    if not conn.execute(text("SELECT to_regclass('quarantine.manifest')")).scalar():
        return 0
    entries = conn.execute(
        text("SELECT seq, target_schema, table_name FROM quarantine.manifest ORDER BY seq DESC")
    ).fetchall()
    restored = 0
    for seq, schema, table in entries:
        if schema not in SCHEMAS:
            raise RuntimeError(f"Unexpected schema in manifest: {schema}")
        cols = ", ".join(qi(c) for c in _public_columns(conn, table))
        target = f"{qi(schema)}.{qi(table)}"
        restored += conn.execute(
            text(
                f"WITH back AS (DELETE FROM {target} WHERE _q_seq = :seq RETURNING *) "
                f"INSERT INTO public.{qi(table)} ({cols}) SELECT {cols} FROM back"
            ),
            {"seq": seq},
        ).rowcount or 0
        for tbl, col in conn.execute(
            text(
                "SELECT DISTINCT table_name, column_name FROM quarantine.set_null_backup "
                "WHERE manifest_seq = :seq"
            ),
            {"seq": seq},
        ).fetchall():
            pk = _pk_column(conn, tbl)
            conn.execute(
                text(
                    f"UPDATE public.{qi(tbl)} t SET {qi(col)} = b.old_value "
                    "FROM quarantine.set_null_backup b "
                    f"WHERE b.manifest_seq = :seq AND b.table_name = :t AND b.column_name = :c "
                    f"AND t.{qi(pk)} = b.row_id"
                ),
                {"seq": seq, "t": tbl, "c": col},
            )
        conn.execute(text("DELETE FROM quarantine.set_null_backup WHERE manifest_seq = :seq"),
                     {"seq": seq})
        conn.execute(text("DELETE FROM quarantine.manifest WHERE seq = :seq"), {"seq": seq})
    return restored
