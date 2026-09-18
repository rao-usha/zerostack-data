"""
Tests for SPEC 105 — Quarantine fabricated, demo and misclassified rows.

Postgres-backed tests need TEST_PG_URL pointing at a DISPOSABLE database
(they create/drop tables in public, quarantine and demo). They are skipped
otherwise.
"""
import importlib.util
import os
import re
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

REPO = Path(__file__).resolve().parents[1]
PG_URL = os.environ.get("TEST_PG_URL")
_IDENT = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


@pytest.mark.unit
class TestRulesAndGuard:
    def test_rules_are_well_formed(self):
        """T1"""
        from app.core.quarantine import RULES, SCHEMAS

        names = [r.name for r in RULES]
        assert len(names) == len(set(names))
        for r in RULES:
            assert _IDENT.match(r.table), r
            assert r.target_schema in SCHEMAS
            assert r.expected > 0
            assert ";" not in r.predicate

    def test_guard_rejects_count_over_tolerance(self):
        """T2"""
        from app.core.quarantine import QuarantineGuardError, QuarantineRule, check_guard

        rule = QuarantineRule("r", "t", "true", "quarantine", 100, "x")
        with pytest.raises(QuarantineGuardError):
            check_guard(rule, 106)

    def test_guard_allows_zero_and_exact(self):
        """T3"""
        from app.core.quarantine import QuarantineRule, check_guard

        rule = QuarantineRule("r", "t", "true", "quarantine", 100, "x")
        check_guard(rule, 0)
        check_guard(rule, 100)
        check_guard(rule, 105)

    def test_migration_chain(self):
        """T8"""
        path = REPO / "alembic" / "versions" / "0003_quarantine_bad_data.py"
        spec = importlib.util.spec_from_file_location("mig0003", path)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        assert mod.revision == "0003_quarantine_bad_data"
        assert mod.down_revision == "0002_worker_heartbeats"

    def test_run_migrations_uses_advisory_lock(self):
        """T9"""
        from app.core import migrate

        engine = MagicMock()
        conn = engine.connect.return_value.__enter__.return_value
        with patch("alembic.command.upgrade") as upgrade:
            assert migrate.run_migrations(engine) is True
        sqls = [str(c.args[0]) for c in conn.execute.call_args_list]
        assert "pg_advisory_lock" in sqls[0]
        assert "pg_advisory_unlock" in sqls[-1]
        cfg, target = upgrade.call_args.args
        assert target == "head"
        assert cfg.attributes["configure_logger"] is False


# ---------------------------------------------------------------------------
# Postgres integration of apply()/revert()
# ---------------------------------------------------------------------------

pg = pytest.mark.skipif(not PG_URL, reason="TEST_PG_URL not set")


@pytest.fixture
def pg_conn():
    from sqlalchemy import create_engine, text

    engine = create_engine(PG_URL)
    with engine.connect() as conn:
        trans = conn.begin()
        for stmt in [
            "DROP SCHEMA IF EXISTS quarantine CASCADE",
            "DROP SCHEMA IF EXISTS demo CASCADE",
            "DROP TABLE IF EXISTS q_note, q_link, q_child, q_parent CASCADE",
            "CREATE TABLE q_parent (id SERIAL PRIMARY KEY, name TEXT, kind TEXT)",
            "CREATE TABLE q_child (id SERIAL PRIMARY KEY, parent_id INT REFERENCES q_parent(id), v TEXT)",
            "CREATE TABLE q_link (id SERIAL PRIMARY KEY, child_id INT REFERENCES q_child(id) ON DELETE CASCADE)",
            "CREATE TABLE q_note (id SERIAL PRIMARY KEY, parent_id INT REFERENCES q_parent(id) ON DELETE SET NULL, body TEXT)",
            "INSERT INTO q_parent (id, name, kind) VALUES (1,'fake A','bad'), (2,'fake B','bad'), (3,'real','good')",
            "INSERT INTO q_child (id, parent_id, v) VALUES (10,1,'a'), (11,2,'b'), (12,3,'c')",
            "INSERT INTO q_link (id, child_id) VALUES (100,10), (101,12)",
            "INSERT INTO q_note (id, parent_id, body) VALUES (200,1,'n1'), (201,3,'n2')",
        ]:
            conn.execute(text(stmt))
        yield conn
        trans.rollback()


def _rules():
    from app.core.quarantine import QuarantineRule

    return [QuarantineRule("bad_parents", "q_parent", "kind = 'bad'", "quarantine", 2, "test")]


def _snapshot(conn):
    from sqlalchemy import text

    return {
        t: conn.execute(text(f"SELECT * FROM {t} ORDER BY id")).fetchall()
        for t in ("q_parent", "q_child", "q_link", "q_note")
    }


@pg
class TestApplyRevertPostgres:
    def test_apply_moves_rows_and_children_pg(self, pg_conn):
        """T4"""
        from sqlalchemy import text
        from app.core.quarantine import apply

        result = apply(pg_conn, _rules())
        assert result["roots"][0]["live"] == 2
        assert [r[0] for r in pg_conn.execute(text("SELECT id FROM q_parent")).fetchall()] == [3]
        assert [r[0] for r in pg_conn.execute(text("SELECT id FROM q_child")).fetchall()] == [12]
        assert [r[0] for r in pg_conn.execute(text("SELECT id FROM q_link")).fetchall()] == [101]
        q = pg_conn.execute(text("SELECT id, _q_rule FROM quarantine.q_parent ORDER BY id")).fetchall()
        assert q == [(1, "bad_parents"), (2, "bad_parents")]
        assert pg_conn.execute(text("SELECT COUNT(*) FROM quarantine.q_link")).scalar() == 1
        assert result["total_rows"] == 5  # 2 parents + 2 children + 1 link

    def test_apply_backs_up_set_null_refs_pg(self, pg_conn):
        """T5"""
        from sqlalchemy import text
        from app.core.quarantine import apply, revert

        apply(pg_conn, _rules())
        assert pg_conn.execute(text("SELECT parent_id FROM q_note WHERE id = 200")).scalar() is None
        revert(pg_conn)
        assert pg_conn.execute(text("SELECT parent_id FROM q_note WHERE id = 200")).scalar() == 1

    def test_revert_restores_everything_pg(self, pg_conn):
        """T6"""
        from sqlalchemy import text
        from app.core.quarantine import apply, revert

        before = _snapshot(pg_conn)
        result = apply(pg_conn, _rules())
        restored = revert(pg_conn)
        assert restored == result["total_rows"]
        assert _snapshot(pg_conn) == before
        assert pg_conn.execute(text("SELECT COUNT(*) FROM quarantine.manifest")).scalar() == 0

    def test_apply_is_idempotent_pg(self, pg_conn):
        """T7"""
        from app.core.quarantine import apply

        apply(pg_conn, _rules())
        second = apply(pg_conn, _rules())
        assert second["total_rows"] == 0
