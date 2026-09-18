"""
Tests for SPEC 116 — Entity master and CIK↔CRD bridge.

PG-backed tests need TEST_PG_URL pointing at a DISPOSABLE database.
"""
import importlib.util
import os
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

REPO = Path(__file__).resolve().parents[1]
PG_URL = os.environ.get("TEST_PG_URL")
pg = pytest.mark.skipif(not PG_URL, reason="TEST_PG_URL not set")


# ---------------------------------------------------------------------------
# the ported pure core
# ---------------------------------------------------------------------------

@pytest.mark.unit
class TestPortedCore:
    def test_norm_selftest_passes(self, capsys):
        """T1"""
        from app.entities.norm import _selftest

        assert _selftest() == 0

    def test_resolve_selftest_passes(self, capsys):
        """T2"""
        from app.entities.resolve_core import _selftest

        assert _selftest() == 0

    def test_strong_key_canonicalization(self):
        """T3"""
        from app.entities.resolve_core import _cik, _crd, _ein

        assert _crd("000106326") == _crd("106326") == "106326"
        assert _cik("0001002784") == _cik("1002784") == "1002784"
        assert _crd("000000000") is None            # all-zero placeholder
        assert _cik("9999999999") is None           # repeated-digit placeholder
        assert _ein("000000000") is None
        assert _ein("26-1640968") == "261640968"
        assert _crd("") is None and _crd(None) is None

    def test_plan_links_records_sharing_identifier(self):
        """T4"""
        from app.entities.resolve_core import _rec, plan

        recs = [
            _rec("adv:8361", crd="8361", legal_name="ACME ADVISERS LLC", name_norm="acme advisers"),
            _rec("f13:1002784", crd="000008361", cik="0001002784", legal_name="Acme Advisers"),
            _rec("formd:1234567", cik="1234567", legal_name="Unrelated Fund"),
        ]
        result = plan(recs, [], {})
        assert [c["members"] for c in result["components"]] == [["adv:8361", "f13:1002784"]]
        assert result["components"][0]["tier"] == "exact_key"
        assert result["metrics"]["singleton_keyed"] == 1

    def test_plan_respects_veto(self):
        """T5"""
        from app.entities.resolve_core import _rec, plan

        recs = [_rec("adv:8361", crd="8361"), _rec("f13:1002784", crd="8361")]
        assert len(plan(recs, [], {})["components"]) == 1
        vetoed = plan(recs, [], {("crd", "8361", "*"): "junk"})
        assert vetoed["components"] == []
        assert vetoed["refusals"]["key_veto_global"] == 2

    def test_assign_ids_reuse_merge_split(self):
        """T6"""
        from app.entities.resolve_core import assign_ids

        comp = [{"members": ["a:1", "a:2"], "tier": "exact_key", "keys": []}]
        assert assign_ids(comp, {"a:1": 7, "a:2": 7}, {})["assigned"] == {0: 7}
        merged = assign_ids(comp, {"a:1": 12, "a:2": 4}, {})
        assert merged["assigned"] == {0: 4}  # oldest survives
        assert [(a, s) for a, s, _ in merged["merges"]] == [(12, 4)]
        split = assign_ids(
            [{"members": ["a:1"], "tier": "exact_key", "keys": []},
             {"members": ["a:2", "a:3", "a:4"], "tier": "exact_key", "keys": []}],
            {"a:1": 5, "a:2": 5, "a:3": 5, "a:4": 5}, {},
        )
        assert split["assigned"] == {1: 5} and split["new_entities"] == [0]

    def test_canonical_row_conflicts(self):
        """T7"""
        from app.entities.resolve_core import _rec, canonical_row

        by_key = {r["record_key"]: r for r in [
            _rec("adv:1", crd="8361", state="CA", legal_name="Acme LLC", name_norm="acme"),
            _rec("f13:2", crd="0008361", state="NY", legal_name="ACME", name_norm="acme"),
        ]}
        cols, conflicts = canonical_row(sorted(by_key), by_key)
        assert cols["crd"] == "8361"                 # padding is not disagreement
        assert cols["canonical_state"] is None       # real disagreement
        assert conflicts["canonical_state"] == ["CA", "NY"]


# ---------------------------------------------------------------------------
# Postgres: feeds, resolve, bridge
# ---------------------------------------------------------------------------

def _apply_migration(conn, name: str, attr: str):
    from sqlalchemy import text

    path = REPO / "alembic" / "versions" / f"{name}.py"
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    for stmt in getattr(mod, attr):
        conn.execute(text(stmt))
    return mod


@pytest.fixture
def pg_engine():
    from sqlalchemy import create_engine, text

    engine = create_engine(PG_URL)
    with engine.begin() as conn:
        conn.execute(text("DROP SCHEMA IF EXISTS core CASCADE"))
        for t in ("sec_adv_roster_snapshots", "sec_adv_feed_firm_state", "sec_13f_filings",
                  "sec_13f_other_managers", "form_d_issuers", "sec_filers", "sec_8k_index",
                  "sec_insider_owners", "sec_insider_filings"):
            conn.execute(text(f"DROP TABLE IF EXISTS public.{t} CASCADE"))
        _apply_migration(conn, "0008_entity_master", "CORE_DDL")
        # minimal source tables: only the columns the feeds read
        conn.execute(text("""
            CREATE TABLE sec_adv_roster_snapshots (crd_number TEXT, roster_date DATE,
                legal_name TEXT, business_name TEXT, main_office_state TEXT, website TEXT)"""))
        conn.execute(text("""
            CREATE TABLE sec_adv_feed_firm_state (crd_number TEXT, edition_date DATE,
                legal_name TEXT, business_name TEXT, main_office_state TEXT, website TEXT)"""))
        conn.execute(text("""
            CREATE TABLE sec_13f_filings (accession_number TEXT, cik TEXT, crd_number TEXT,
                filing_manager_name TEXT, filing_manager_state_or_country TEXT, filing_date DATE)"""))
        conn.execute(text("""
            CREATE TABLE sec_13f_other_managers (accession_number TEXT, cik TEXT, crd_number TEXT)"""))
        conn.execute(text("""
            CREATE TABLE form_d_issuers (cik TEXT, entity_name TEXT, state_or_country TEXT,
                loaded_at TIMESTAMP DEFAULT NOW())"""))
        conn.execute(text("""
            CREATE TABLE sec_filers (cik TEXT, name TEXT, ein TEXT, state_of_incorporation TEXT,
                biz_state2 TEXT, biz_state_or_country TEXT, website TEXT,
                loaded_at TIMESTAMP DEFAULT NOW())"""))
        conn.execute(text("CREATE TABLE sec_8k_index (cik TEXT)"))
        conn.execute(text("CREATE TABLE sec_insider_owners (rptowner_cik TEXT)"))
        conn.execute(text("CREATE TABLE sec_insider_filings (issuer_cik TEXT)"))
    yield engine
    engine.dispose()


def _seed(engine):
    """One adviser that also files 13F (CRD + CIK on one record) and a Form D fund."""
    from sqlalchemy import text

    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO sec_adv_roster_snapshots (crd_number, roster_date, legal_name, business_name,
                                                  main_office_state, website)
            VALUES ('8361', '2026-07-01', 'ACME ADVISERS LLC', 'ACME', 'CA', 'https://acme.com'),
                   ('8361', '2026-09-01', 'ACME ADVISERS LLC', 'ACME CAPITAL', 'CA', 'https://acme.com'),
                   ('4242', '2026-09-01', 'SOLO ADVISERS LLC', 'SOLO', 'NY', NULL),
                   ('000000000', '2026-09-01', 'PLACEHOLDER', NULL, 'NY', NULL)"""))
        conn.execute(text("""
            INSERT INTO sec_13f_filings (accession_number, cik, crd_number, filing_manager_name,
                                         filing_manager_state_or_country, filing_date)
            VALUES ('acc-1', '0001002784', '000008361', 'ACME ADVISERS LLC', 'CA', '2026-08-14')"""))
        conn.execute(text("""
            INSERT INTO form_d_issuers (cik, entity_name, state_or_country)
            VALUES ('0001002784', 'ACME ADVISERS LLC', 'CA'),
                   ('0000000099', 'ACME FUND IV LP', 'DE')"""))
        conn.execute(text("""
            INSERT INTO sec_filers (cik, name, ein, state_of_incorporation, biz_state2, website)
            VALUES ('0001002784', 'Acme Advisers LLC', '261640968', 'CA', 'CA', NULL),
                   ('0000000099', 'Acme Fund IV LP', NULL, 'DE', 'DE', NULL),
                   ('0000000777', 'Irrelevant Filer Inc', NULL, 'TX', 'TX', NULL)"""))


@pg
def test_feeds_normalize_identifiers_pg(pg_engine):
    """T8"""
    from sqlalchemy import text
    from app.entities import feeds

    _seed(pg_engine)
    with pg_engine.begin() as conn:
        counts = feeds.run_feeds(conn)
    assert counts["adv"] == 3  # 4 rows, 2 snapshots of one CRD -> latest only, placeholder kept as row
    with pg_engine.connect() as conn:
        rows = {r["record_key"]: dict(r) for r in conn.execute(text(
            "SELECT record_key, crd, cik, ein, state, legal_name FROM core.source_record")).mappings()}
    assert rows["adv:8361"]["crd"] == "8361"          # unpadded
    assert rows["f13:0001002784"]["crd"] == "8361"    # padding stripped
    assert rows["f13:0001002784"]["cik"] == "1002784"
    assert rows["adv:000000000"]["crd"] is None       # placeholder dropped, row kept keyless
    assert rows["edgar:0001002784"]["ein"] == "261640968"
    # scope: only CIKs referenced by a PE-relevant source
    assert "edgar:0000000777" not in rows


@pg
def test_resolve_end_to_end_pg(pg_engine):
    """T9"""
    from sqlalchemy import text
    from app.entities import feeds, resolve

    _seed(pg_engine)
    with pg_engine.begin() as conn:
        feeds.run_feeds(conn)
        metrics = resolve.resolve(conn)
    assert metrics["components_materialized"] >= 1

    with pg_engine.connect() as conn:
        row = conn.execute(text(
            "SELECT e.entity_id, e.cik, e.crd, e.canonical_name, e.member_count "
            "FROM core.identifier i JOIN core.entity e ON e.entity_id = i.entity_id "
            "WHERE i.id_type = 'crd' AND i.id_value = '8361'")).mappings().one()
    # adviser + 13F filer + Form D issuer + EDGAR filer collapse into one entity
    assert row["cik"] == "1002784" and row["crd"] == "8361"
    assert row["member_count"] >= 3
    with pg_engine.connect() as conn:
        members = [r[0] for r in conn.execute(text(
            "SELECT record_key FROM core.membership WHERE entity_id = :e ORDER BY record_key"),
            {"e": row["entity_id"]})]
    assert "adv:8361" in members and "f13:0001002784" in members
    # the unrelated adviser stays a singleton, not an entity
    with pg_engine.connect() as conn:
        assert conn.execute(text(
            "SELECT COUNT(*) FROM core.identifier WHERE id_type='crd' AND id_value='4242'")).scalar() == 0


@pg
def test_resolve_is_idempotent_pg(pg_engine):
    """T10"""
    from sqlalchemy import text
    from app.entities import feeds, resolve

    _seed(pg_engine)
    with pg_engine.begin() as conn:
        feeds.run_feeds(conn)
        resolve.resolve(conn)
    with pg_engine.connect() as conn:
        before = conn.execute(text(
            "SELECT entity_id, updated_at FROM core.entity ORDER BY entity_id")).fetchall()

    with pg_engine.begin() as conn:
        feeds.run_feeds(conn)
        second = resolve.resolve(conn)
    assert second["entities_new"] == 0
    assert second["entities_written"] == 0
    assert second["memberships_written"] == 0
    with pg_engine.connect() as conn:
        after = conn.execute(text(
            "SELECT entity_id, updated_at FROM core.entity ORDER BY entity_id")).fetchall()
    assert before == after  # no row versions touched


@pg
def test_bridge_tiers_and_refusals_pg(pg_engine):
    """T11"""
    from sqlalchemy import text
    from app.entities import cik_crd_bridge

    _seed(pg_engine)
    with pg_engine.begin() as conn:
        # a CIK whose cover pages disagree about the CRD -> refused
        conn.execute(text("""
            INSERT INTO sec_13f_filings (accession_number, cik, crd_number, filing_manager_name,
                                         filing_manager_state_or_country, filing_date)
            VALUES ('acc-2', '0000012345', '000001112', 'TWO FACED LLC', 'NY', '2026-08-01'),
                   ('acc-3', '0000012345', '000002223', 'TWO FACED LLC', 'NY', '2026-08-02')"""))
        stats = cik_crd_bridge.build(conn, include_name_tier=False)

    assert stats["cover_page_crd"] == 1
    with pg_engine.connect() as conn:
        bridge = conn.execute(text("SELECT cik, crd, tier FROM core.cik_crd_bridge")).mappings().all()
        refused = conn.execute(text(
            "SELECT cik, reason FROM core.cik_crd_bridge_refused")).mappings().all()
    assert [(r["cik"], r["crd"], r["tier"]) for r in bridge] == [("1002784", "8361", "cover_page_crd")]
    assert [(r["cik"], r["reason"]) for r in refused] == [("12345", "ambiguous_within_tier")]


# ---------------------------------------------------------------------------
# executor / endpoints / migration
# ---------------------------------------------------------------------------

@pytest.mark.unit
class TestExecutorAndEndpoints:
    @pytest.mark.asyncio
    async def test_executor_runs_in_thread(self):
        """T12"""
        from app.worker.executors import entity_resolve

        job = MagicMock(payload={"dry_run": True}, id=3)
        db = MagicMock()
        seen = {}

        async def fake_to_thread(fn, *args, **kwargs):
            seen["fn"] = fn
            seen["kwargs"] = kwargs
            return {"resolve": {"components_materialized": 5, "entities_new": 5,
                                "entities_written": 5}, "bridge": {"accepted": 2}}

        with patch.object(entity_resolve.asyncio, "to_thread", fake_to_thread):
            await entity_resolve.execute(job, db)
        assert seen["fn"] is entity_resolve.run_entity_master
        assert seen["kwargs"]["dry_run"] is True
        assert "5 components" in job.progress_message

    def test_resolve_endpoint_queues_job(self):
        """T12"""
        from app.api.v1 import entity_master

        db = MagicMock()
        with patch.object(entity_master, "submit_job", return_value={"job_queue_id": 9}) as sj:
            out = entity_master.queue_resolve(skip_feeds=True, skip_bridge=False,
                                              include_name_tier=True, dry_run=False, db=db)
        assert out == {"job_queue_id": 9}
        assert sj.call_args.kwargs["job_type"] == "entity_resolve"
        assert sj.call_args.kwargs["payload"]["skip_feeds"] is True

    def test_by_id_rejects_unknown_type(self):
        """T12"""
        from fastapi import HTTPException
        from app.api.v1 import entity_master

        with pytest.raises(HTTPException) as exc:
            entity_master.get_by_identifier("linkedin", "x", db=MagicMock())
        assert exc.value.status_code == 400

    def test_migration_0008_chain(self):
        """T13"""
        path = REPO / "alembic" / "versions" / "0008_entity_master.py"
        spec = importlib.util.spec_from_file_location("mig0008", path)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        assert mod.revision == "0008_entity_master"
        assert mod.down_revision == "0007_bulk_gap_fixes"
        sql = " ".join(mod.CORE_DDL).lower()
        for table in ("core.source_record", "core.entity", "core.membership", "core.identifier",
                      "core.alias", "core.key_veto", "core.entity_merge", "core.resolve_run",
                      "core.cik_crd_bridge"):
            assert table in sql
