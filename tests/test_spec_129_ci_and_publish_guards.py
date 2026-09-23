"""
Tests for SPEC 129 — CI and publish guards.

Every test here is about data vanishing, or looking wrong, without anyone
being told: a 13F zip without INFOTABLE that empties the holdings table while
the release says `loaded`, a dry run that writes, a total that ignores the
filter, a real zero reported as null.

PG-backed tests need TEST_PG_URL pointing at a DISPOSABLE database.
"""
import asyncio
import importlib.util
import io
import os
import re
import zipfile
from datetime import date
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
PG_URL = os.environ.get("TEST_PG_URL")
pg = pytest.mark.skipif(not PG_URL, reason="TEST_PG_URL not set")


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _13f_zip(path, drop=(), rename=None, acc_prefix=""):
    """The SPEC_109 synthetic data set, optionally missing members or with a
    header column renamed ({member: (old, new)})."""
    from tests.test_spec_109_sec_13f_bulk import _members

    members = _members(acc_prefix)
    for member, (old, new) in (rename or {}).items():
        head, _, rest = members[member].partition("\n")
        members[member] = "\t".join(new if h == old else h for h in head.split("\t")) + "\n" + rest
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as zf:
        for name, body in members.items():
            if name not in drop:
                zf.writestr(name, body)
    return path


def _rewrite_zip(src, dest, drop=(), rename=None):
    """Copy a zip, dropping members (by basename) or renaming one header column."""
    rename = rename or {}
    with zipfile.ZipFile(src) as zin, zipfile.ZipFile(dest, "w", zipfile.ZIP_DEFLATED) as zout:
        for name in zin.namelist():
            base = name.rsplit("/", 1)[-1]
            if base in drop:
                continue
            body = zin.read(name).decode("utf-8")
            if base in rename:
                old, new = rename[base]
                head, _, rest = body.partition("\n")
                body = "\t".join(new if h == old else h for h in head.split("\t")) + "\n" + rest
            zout.writestr(name, body)
    return dest


# ---------------------------------------------------------------------------
# T1-T3: 13F parse guards
# ---------------------------------------------------------------------------

@pytest.mark.unit
class Test13FParseGuards:
    def test_13f_missing_infotable_raises(self, tmp_path):
        """T1: the review's headline failure. No INFOTABLE must not mean 0 holdings."""
        from app.ingest.bulk.sec_13f.parse import filing_dates, iter_holdings

        with zipfile.ZipFile(_13f_zip(tmp_path / "a.zip", drop=("INFOTABLE.tsv",))) as zf:
            with pytest.raises(ValueError, match="INFOTABLE"):
                list(iter_holdings(zf, filing_dates(zf)))

    def test_13f_header_drift_raises(self, tmp_path):
        """T2: a renamed column used to load as NULL on every row."""
        from app.ingest.bulk.sec_13f.parse import filing_dates, iter_holdings

        path = _13f_zip(tmp_path / "a.zip", rename={"INFOTABLE.tsv": ("CUSIP", "CUSIP_ID")})
        with zipfile.ZipFile(path) as zf:
            with pytest.raises(ValueError, match="CUSIP"):
                list(iter_holdings(zf, filing_dates(zf)))

    def test_13f_coverpage_header_drift_raises(self, tmp_path):
        from app.ingest.bulk.sec_13f.parse import iter_filings

        path = _13f_zip(tmp_path / "a.zip",
                        rename={"COVERPAGE.tsv": ("FILINGMANAGER_NAME", "MANAGER_NAME")})
        with zipfile.ZipFile(path) as zf:
            with pytest.raises(ValueError, match="FILINGMANAGER_NAME"):
                list(iter_filings(zf))

    def test_13f_missing_coverpage_raises(self, tmp_path):
        from app.ingest.bulk.sec_13f.parse import iter_filings

        with zipfile.ZipFile(_13f_zip(tmp_path / "a.zip", drop=("COVERPAGE.tsv",))) as zf:
            with pytest.raises(ValueError, match="COVERPAGE"):
                list(iter_filings(zf))

    def test_13f_valid_zip_parses(self, tmp_path):
        """T3: the guard must not break a good data set."""
        from app.ingest.bulk.sec_13f.parse import (filing_dates, iter_filings, iter_holdings,
                                                    iter_other_managers)

        with zipfile.ZipFile(_13f_zip(tmp_path / "a.zip")) as zf:
            assert len(list(iter_filings(zf))) == 3
            assert len(list(iter_holdings(zf, filing_dates(zf)))) == 3
            assert len(list(iter_other_managers(zf))) == 2

    def test_13f_optional_member_with_drifted_header_raises(self, tmp_path):
        """An optional member may be absent, but if present its keys must be there."""
        from app.ingest.bulk.sec_13f.parse import iter_other_managers

        path = _13f_zip(tmp_path / "a.zip",
                        rename={"OTHERMANAGER2.tsv": ("SEQUENCENUMBER", "SEQ")})
        with zipfile.ZipFile(path) as zf:
            with pytest.raises(ValueError, match="SEQUENCENUMBER"):
                list(iter_other_managers(zf))


# ---------------------------------------------------------------------------
# T4-T5: Form D parse guards
# ---------------------------------------------------------------------------

@pytest.mark.unit
class TestFormDParseGuards:
    def test_form_d_header_drift_raises(self, tmp_path):
        """T4"""
        from app.ingest.bulk.sec_form_d import parse as p
        from tests.test_spec_108_sec_form_d_bulk import build_zip

        good = build_zip(tmp_path / "good.zip")
        bad = _rewrite_zip(good, tmp_path / "bad.zip",
                           rename={"ISSUERS.tsv": ("ENTITYNAME", "ENTITY_NAME")})
        with pytest.raises(ValueError, match="ENTITYNAME"):
            list(p.iter_issuers(bad))

    def test_form_d_submission_header_drift_raises(self, tmp_path):
        from app.ingest.bulk.sec_form_d import parse as p
        from tests.test_spec_108_sec_form_d_bulk import build_zip

        good = build_zip(tmp_path / "good.zip")
        bad = _rewrite_zip(good, tmp_path / "bad.zip",
                           rename={"FORMDSUBMISSION.tsv": ("TESTORLIVE", "TEST_OR_LIVE")})
        # every submission would otherwise be read as non-LIVE and skipped
        with pytest.raises(ValueError, match="TESTORLIVE"):
            p.non_live_accessions(bad)

    def test_form_d_optional_member_absent_ok(self, tmp_path):
        """T5"""
        from app.ingest.bulk.sec_form_d import parse as p
        from tests.test_spec_108_sec_form_d_bulk import build_zip

        good = build_zip(tmp_path / "good.zip")
        slim = _rewrite_zip(good, tmp_path / "slim.zip", drop=("RECIPIENTS.tsv",))
        assert list(p.iter_recipients(slim)) == []
        assert len(list(p.iter_issuers(slim))) > 0

    def test_form_d_optional_member_drifted_raises(self, tmp_path):
        from app.ingest.bulk.sec_form_d import parse as p
        from tests.test_spec_108_sec_form_d_bulk import build_zip

        good = build_zip(tmp_path / "good.zip")
        bad = _rewrite_zip(good, tmp_path / "bad.zip",
                           rename={"RECIPIENTS.tsv": ("RECIPIENT_SEQ_KEY", "SEQ")})
        with pytest.raises(ValueError, match="RECIPIENT_SEQ_KEY"):
            list(p.iter_recipients(bad))


# ---------------------------------------------------------------------------
# T6: publish guard (pure)
# ---------------------------------------------------------------------------

@pytest.mark.unit
class TestCheckPublish:
    def test_zero_rows_is_refused(self, monkeypatch):
        from app.core.copy_loader import PublishGuardError, check_publish

        monkeypatch.delenv("BULK_PUBLISH_GUARD_OVERRIDE", raising=False)
        with pytest.raises(PublishGuardError, match="0 rows"):
            check_publish("public.sec_13f_holdings", 0, 3_800_000)
        # even on a first load: an empty replacement is never a real release
        with pytest.raises(PublishGuardError):
            check_publish("public.sec_13f_holdings", 0, 0)

    def test_large_drop_is_refused(self, monkeypatch):
        from app.core.copy_loader import PublishGuardError, check_publish

        monkeypatch.delenv("BULK_PUBLISH_GUARD_OVERRIDE", raising=False)
        with pytest.raises(PublishGuardError, match="drop"):
            check_publish("public.sec_13f_holdings", 1_000_000, 3_800_000)

    def test_normal_changes_pass(self, monkeypatch):
        from app.core.copy_loader import check_publish

        monkeypatch.delenv("BULK_PUBLISH_GUARD_OVERRIDE", raising=False)
        check_publish("public.sec_13f_holdings", 3_700_000, 3_800_000)   # -3%
        check_publish("public.sec_13f_holdings", 1_900_000, 3_800_000)   # exactly -50%
        check_publish("public.sec_13f_holdings", 9_000_000, 3_800_000)   # growth
        check_publish("public.sec_13f_holdings", 5, 0)                   # first load

    def test_override_env(self, monkeypatch):
        from app.core.copy_loader import PublishGuardError, check_publish

        monkeypatch.setenv("BULK_PUBLISH_GUARD_OVERRIDE", "sec_13f_holdings")
        check_publish("public.sec_13f_holdings", 0, 3_800_000)
        with pytest.raises(PublishGuardError):  # names one table, not every table
            check_publish("public.sec_adv_private_funds", 0, 100)
        monkeypatch.setenv("BULK_PUBLISH_GUARD_OVERRIDE", "1")
        check_publish("public.sec_adv_private_funds", 0, 100)
        # explicit argument wins over env
        with pytest.raises(PublishGuardError):
            check_publish("public.sec_adv_private_funds", 0, 100, override=False)


# ---------------------------------------------------------------------------
# T12: pe_mart_build job summary
# ---------------------------------------------------------------------------

@pytest.mark.unit
class TestMartSummary:
    def test_summary_counts_all_tiers(self, monkeypatch):
        """T12: the message counted 2 of 5 tiers, so adv_* links looked missing."""
        from unittest.mock import MagicMock

        from app.worker.executors import pe_marts

        summary = {
            "firms": {"inserted": 1, "updated": 2},
            "funds": {"inserted": 3, "linked_adv_exact": 10, "linked_adv_family": 20,
                      "linked_adv_platform": 30, "linked_name_core": 1,
                      "linked_related_person": 2},
            "people": {"inserted_people": 4, "firms_covered": 5},
        }
        monkeypatch.setattr(pe_marts, "run_pe_marts", lambda **kw: summary)
        job = MagicMock(payload={})
        asyncio.run(pe_marts.execute(job, MagicMock()))
        assert "linked 63" in job.progress_message
        assert not job.progress_message.startswith("DRY RUN")

    def test_dry_run_message_says_so(self, monkeypatch):
        from unittest.mock import MagicMock

        from app.worker.executors import pe_marts

        seen = {}

        def fake(**kw):
            seen.update(kw)
            return {"dry_run": True}

        monkeypatch.setattr(pe_marts, "run_pe_marts", fake)
        job = MagicMock(payload={"dry_run": True})
        asyncio.run(pe_marts.execute(job, MagicMock()))
        assert seen["dry_run"] is True
        assert job.progress_message.startswith("DRY RUN")

    def test_dry_run_uses_one_rolled_back_transaction(self, monkeypatch):
        """Every stage, people included, runs on the connection that is rolled back."""
        from app.marts import adv_private_funds, pe_firms_sec, pe_funds_sec, pe_people_sec
        from app.worker.executors import pe_marts

        events, conns = [], []

        class Tx:
            def rollback(self):
                events.append("rollback")

            def commit(self):
                events.append("commit")

        class Conn:
            def begin(self):
                return Tx()

            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

        class Engine:
            def connect(self):
                c = Conn()
                conns.append(c)
                return c

            def begin(self):  # the non-dry path; must not be used here
                raise AssertionError("dry run must not open a committing transaction")

        monkeypatch.setattr(pe_marts, "get_engine", lambda: Engine())
        used = []
        for mod, key in ((pe_firms_sec, "firms"), (adv_private_funds, "adv"),
                         (pe_funds_sec, "funds"), (pe_people_sec, "people")):
            monkeypatch.setattr(mod, "build",
                                lambda conn, _k=key, **kw: used.append((_k, conn, kw)) or {})
        out = pe_marts.run_pe_marts(dry_run=True)
        assert [k for k, _, _ in used] == ["firms", "adv", "funds", "people"]
        assert len(conns) == 1 and all(c is conns[0] for _, c, _ in used)
        assert events == ["rollback"]
        assert out["dry_run"] is True


# ---------------------------------------------------------------------------
# T13-T15: pe_firms / zero coercion
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_no_truthy_float_coercion_left():
    """T15: `float(x) if x else None` reports a real 0 as null."""
    pattern = re.compile(r"float\(([^()]*(?:\([^()]*\))?[^()]*)\)\s+if\s+(?:[\w.\[\]]+\s+and\s+)?\1\s+else\s+None")
    offenders = []
    for path in sorted((REPO / "app" / "api" / "v1").glob("pe_*.py")):
        for i, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            if pattern.search(line):
                offenders.append(f"{path.name}:{i}: {line.strip()}")
    assert offenders == []


def _pe_firms_table(conn):
    from sqlalchemy import text

    conn.execute(text("DROP TABLE IF EXISTS public.pe_firms CASCADE"))
    conn.execute(text("""
        CREATE TABLE pe_firms (id SERIAL PRIMARY KEY, name TEXT NOT NULL, legal_name TEXT,
            website TEXT, headquarters_city TEXT, headquarters_state TEXT,
            headquarters_country TEXT, firm_type TEXT, primary_strategy TEXT,
            aum_usd_millions NUMERIC, employee_count INTEGER, founded_year INTEGER,
            status TEXT, cik TEXT, created_at TIMESTAMP DEFAULT NOW())"""))
    conn.execute(text("""
        INSERT INTO pe_firms (name, firm_type, primary_strategy, aum_usd_millions, status) VALUES
          ('Alpha Buyout', 'PE', 'Buyout', 0, 'Active'),
          ('Beta Buyout', 'PE', 'Buyout', 50, 'Active'),
          ('Gamma Ventures', 'VC', 'Venture', 10, 'Active'),
          ('Delta Ventures', 'VC', 'Venture', NULL, 'Active')"""))


@pg
def test_pe_firms_total_honours_strategy():
    """T13: `total` ignored the strategy filter, so pagination lied."""
    from sqlalchemy import create_engine
    from sqlalchemy.orm import Session

    from app.api.v1.pe_firms import list_pe_firms

    engine = create_engine(PG_URL)
    try:
        with engine.begin() as conn:
            _pe_firms_table(conn)
        with Session(engine) as db:
            out = asyncio.run(list_pe_firms(limit=1, offset=0, firm_type=None, strategy="buyout",
                                            status=None, search=None, db=db))
            assert out["total"] == 2 and out["count"] == 1
            out = asyncio.run(list_pe_firms(limit=100, offset=0, firm_type="VC", strategy="venture",
                                            status="Active", search="gamma", db=db))
            assert out["total"] == 1 and [f["name"] for f in out["firms"]] == ["Gamma Ventures"]
    finally:
        engine.dispose()


@pg
def test_zero_is_not_null():
    """T14: an AUM of 0 is a value, not a missing value."""
    from sqlalchemy import create_engine
    from sqlalchemy.orm import Session

    from app.api.v1.pe_firms import list_pe_firms

    engine = create_engine(PG_URL)
    try:
        with engine.begin() as conn:
            _pe_firms_table(conn)
        with Session(engine) as db:
            out = asyncio.run(list_pe_firms(limit=100, offset=0, firm_type=None, strategy=None,
                                            status=None, search=None, db=db))
        by = {f["name"]: f["aum_usd_millions"] for f in out["firms"]}
        assert by["Alpha Buyout"] == 0.0
        assert by["Delta Ventures"] is None
        assert out["total"] == 4
    finally:
        engine.dispose()


# ---------------------------------------------------------------------------
# T16: CI workflow contract
# ---------------------------------------------------------------------------

@pytest.mark.unit
class TestCiWorkflow:
    @pytest.fixture
    def ci(self):
        import yaml

        return yaml.safe_load((REPO / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8"))

    def test_python_matches_dockerfile(self, ci):
        docker = (REPO / "Dockerfile").read_text(encoding="utf-8")
        m = re.search(r"FROM\s+python:(\d+\.\d+)", docker)
        assert m, "Dockerfile base image not found"
        versions = {
            str(step["with"]["python-version"])
            for job in ci["jobs"].values() for step in job.get("steps", [])
            if "setup-python" in str(step.get("uses", ""))
        }
        assert versions == {m.group(1)}

    def test_pg_tests_run_in_ci(self, ci):
        test_job = ci["jobs"]["test"]
        env = dict(test_job.get("env") or {})
        for step in test_job["steps"]:
            env.update(step.get("env") or {})
        assert env.get("TEST_PG_URL", "").startswith("postgresql://")
        runs = " ".join(str(s.get("run", "")) for s in test_job["steps"])
        assert "scripts/ci/bootstrap_db.py" in runs
        # migrate before tests
        order = [i for i, s in enumerate(test_job["steps"]) if "bootstrap_db" in str(s.get("run", ""))]
        tests = [i for i, s in enumerate(test_job["steps"]) if "pytest" in str(s.get("run", ""))]
        assert order and tests and order[0] < tests[0]

    def test_secret_scan_is_blocking(self, ci):
        jobs = ci["jobs"]
        scan = [name for name, job in jobs.items()
                if any("gitleaks" in str(s.get("uses", "")) + str(s.get("run", ""))
                       for s in job.get("steps", []))]
        assert scan, "no gitleaks job"
        for name in scan:
            assert not jobs[name].get("continue-on-error"), f"{name} must block"
        assert (REPO / ".gitleaks.toml").exists()

    def test_lint_stays_non_blocking(self, ci):
        assert ci["jobs"]["lint"].get("continue-on-error") is True

    def test_smoke_is_not_just_sleep(self, ci):
        for job in ci["jobs"].values():
            for step in job.get("steps", []):
                run = str(step.get("run", ""))
                if "sleep" in run:
                    assert re.search(r"curl|python|import|exit", run), run


# ---------------------------------------------------------------------------
# PG: 13F load + publish guard (T7-T9)
# ---------------------------------------------------------------------------

@pytest.fixture
def pg13f():
    from sqlalchemy import create_engine, text

    engine = create_engine(PG_URL)
    with engine.begin() as conn:
        for t in ("sec_13f_filings", "sec_13f_holdings", "sec_13f_other_managers"):
            conn.execute(text(f"DROP TABLE IF EXISTS public.{t}"))
        conn.execute(text("DROP TABLE IF EXISTS raw.source_release"))
        path = REPO / "alembic" / "versions" / "0004_bulk_framework.py"
        spec = importlib.util.spec_from_file_location("mig0004_129", path)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        for stmt in mod.SOURCE_RELEASE_DDL:
            conn.execute(text(stmt))
    yield engine
    engine.dispose()


def _holdings_by_release(engine):
    from sqlalchemy import text

    with engine.connect() as conn:
        return dict(conn.execute(text(
            "SELECT source_release_key, COUNT(*) FROM public.sec_13f_holdings GROUP BY 1")).fetchall())


def _release(key, end):
    from app.ingest.bulk.base import Release

    return Release(key, "u", {"load_holdings": True, "holdings_keep_keys": [key], "end_date": end})


@pg
def test_13f_load_without_infotable_keeps_holdings(pg13f, tmp_path, monkeypatch):
    """T7: through run_source, so the release is marked failed, not loaded."""
    from sqlalchemy import text

    from app.ingest.bulk.base import run_source
    from app.ingest.bulk.sec_13f.source import Sec13FDataSets

    monkeypatch.delenv("BULK_13F_PRUNE_HOLDINGS", raising=False)
    monkeypatch.delenv("BULK_PUBLISH_GUARD_OVERRIDE", raising=False)
    src = Sec13FDataSets()
    with pg13f.begin() as conn:
        src.load(conn, _release("old", date(2026, 5, 31)), _13f_zip(tmp_path / "old.zip", acc_prefix="9"))
    assert _holdings_by_release(pg13f) == {"old": 3}

    broken = _13f_zip(tmp_path / "new.zip", drop=("INFOTABLE.tsv",))

    class OneRelease(Sec13FDataSets):
        def discover(self, http, since=None):
            return [_release("new", date(2026, 8, 31))]

        def fetch(self, http, release, dest_dir):
            return broken

    summary = run_source(OneRelease(), engine=pg13f, http=object(), raw_root=tmp_path / "raw")
    assert summary["failed"] == 1 and summary["loaded"] == 0
    assert "INFOTABLE" in summary["errors"][0]
    assert _holdings_by_release(pg13f) == {"old": 3}      # untouched
    with pg13f.connect() as conn:
        status = conn.execute(text(
            "SELECT status FROM raw.source_release WHERE source = 'sec_13f' AND release_key = 'new'"
        )).scalar()
    assert status == "failed"


@pg
def test_13f_prune_guard_blocks_large_drop(pg13f, tmp_path, monkeypatch):
    """T8: a release with a sliver of the holdings cannot replace a full one."""
    from app.core.copy_loader import PublishGuardError
    from app.ingest.bulk.sec_13f.source import Sec13FDataSets
    from tests.test_spec_109_sec_13f_bulk import A1, H_INFOTABLE, _members, _tsv

    monkeypatch.delenv("BULK_13F_PRUNE_HOLDINGS", raising=False)
    monkeypatch.delenv("BULK_PUBLISH_GUARD_OVERRIDE", raising=False)
    src = Sec13FDataSets()
    with pg13f.begin() as conn:
        src.load(conn, _release("old", date(2026, 5, 31)), _13f_zip(tmp_path / "old.zip", acc_prefix="9"))

    # new data set with a single holding: 3 -> 1 is a 67% drop
    members = _members()
    members["INFOTABLE.tsv"] = _tsv(H_INFOTABLE, [
        [A1, "1", "CARDINAL HEALTH INC", "COM", "14149Y108", "", "388237", "5250", "SH", "",
         "SOLE", "", "5250", "0", "0"]])
    small = tmp_path / "small.zip"
    with zipfile.ZipFile(small, "w") as zf:
        for name, body in members.items():
            zf.writestr(name, body)

    with pytest.raises(PublishGuardError, match="sec_13f_holdings"):
        with pg13f.begin() as conn:
            src.load(conn, _release("new", date(2026, 8, 31)), small)
    assert _holdings_by_release(pg13f) == {"old": 3}

    monkeypatch.setenv("BULK_PUBLISH_GUARD_OVERRIDE", "sec_13f_holdings")
    with pg13f.begin() as conn:
        src.load(conn, _release("new", date(2026, 8, 31)), small)
    assert _holdings_by_release(pg13f) == {"new": 1}


@pg
def test_13f_normal_quarter_rollover_passes(pg13f, tmp_path, monkeypatch):
    """T9: same-size replacement prunes the old quarter as before."""
    from app.ingest.bulk.sec_13f.source import Sec13FDataSets

    monkeypatch.delenv("BULK_13F_PRUNE_HOLDINGS", raising=False)
    monkeypatch.delenv("BULK_PUBLISH_GUARD_OVERRIDE", raising=False)
    src = Sec13FDataSets()
    with pg13f.begin() as conn:
        src.load(conn, _release("old", date(2026, 5, 31)), _13f_zip(tmp_path / "old.zip", acc_prefix="9"))
    with pg13f.begin() as conn:
        src.load(conn, _release("new", date(2026, 8, 31)), _13f_zip(tmp_path / "new.zip"))
    assert _holdings_by_release(pg13f) == {"new": 3}
    # reloading the same release is a no-op, not a guard failure
    with pg13f.begin() as conn:
        src.load(conn, _release("new", date(2026, 8, 31)), _13f_zip(tmp_path / "new.zip"))
    assert _holdings_by_release(pg13f) == {"new": 3}


# ---------------------------------------------------------------------------
# PG: marts (T10, T11)
# ---------------------------------------------------------------------------

def _migration_sql(name):
    path = REPO / "alembic" / "versions" / name
    spec = importlib.util.spec_from_file_location(f"mig_{path.stem}", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return list(mod.UPGRADE_SQL)


@pytest.fixture
def pgmart():
    """pe_firms / pe_funds / ADV / Form D tables as the marts read them."""
    from sqlalchemy import create_engine, text

    engine = create_engine(PG_URL)
    with engine.begin() as conn:
        conn.execute(text("DROP SCHEMA IF EXISTS stg CASCADE"))
        conn.execute(text("DROP SCHEMA IF EXISTS core CASCADE"))
        conn.execute(text("CREATE SCHEMA core"))
        conn.execute(text("CREATE TABLE core.identifier (id_type TEXT, id_value TEXT, entity_id BIGINT)"))
        for t in ("pe_firm_people", "pe_people", "pe_funds", "pe_firms", "sec_adv_roster_snapshots",
                  "form_d_offerings", "form_d_issuers", "form_d_related_persons", "sec_adv_filings",
                  "sec_adv_private_fund_filings", "sec_adv_private_funds"):
            conn.execute(text(f"DROP TABLE IF EXISTS public.{t} CASCADE"))
        conn.execute(text("""
            CREATE TABLE pe_firms (id SERIAL PRIMARY KEY, name TEXT NOT NULL, legal_name TEXT,
                website TEXT, headquarters_city TEXT, headquarters_state TEXT,
                headquarters_country TEXT, firm_type TEXT, primary_strategy TEXT,
                aum_usd_millions NUMERIC, cik TEXT, sec_file_number TEXT, crd_number TEXT,
                is_sec_registered BOOLEAN, status TEXT, data_sources JSON,
                last_verified_date DATE, updated_at TIMESTAMP)"""))
        conn.execute(text("""
            CREATE TABLE pe_funds (id SERIAL PRIMARY KEY, firm_id INTEGER, name TEXT NOT NULL,
                cik TEXT, vintage_year INTEGER, target_size_usd_millions NUMERIC,
                final_close_usd_millions NUMERIC, strategy TEXT, status TEXT,
                first_close_date DATE, sec_file_number TEXT, data_source TEXT,
                updated_at TIMESTAMP)"""))
        conn.execute(text("""
            CREATE TABLE sec_adv_roster_snapshots (crd_number TEXT, roster_date DATE,
                legal_name TEXT, business_name TEXT, sec_number TEXT, adviser_type TEXT,
                main_office_street1 TEXT, main_office_street2 TEXT,
                main_office_postal_code TEXT, main_office_city TEXT, main_office_state TEXT,
                main_office_country TEXT, website TEXT, aum_total NUMERIC, sec_status TEXT,
                raw JSONB)"""))
        conn.execute(text("""
            CREATE TABLE form_d_offerings (accession_number TEXT, investment_fund_type TEXT,
                date_of_first_sale DATE, total_offering_amount NUMERIC, total_amount_sold NUMERIC,
                is_indefinite BOOLEAN, file_num TEXT, loaded_at TIMESTAMP DEFAULT NOW())"""))
        conn.execute(text("""
            CREATE TABLE form_d_issuers (accession_number TEXT, cik TEXT, entity_name TEXT,
                is_primary BOOLEAN)"""))
        conn.execute(text("""
            CREATE TABLE form_d_related_persons (accession_number TEXT, first_name TEXT,
                middle_name TEXT, last_name TEXT)"""))
        for stmt in _migration_sql("0009_pe_mart_keys.py") + _migration_sql("0010_adv_private_funds.py"):
            conn.execute(text(stmt))
    yield engine
    engine.dispose()


def _seed_marts(engine):
    from sqlalchemy import text

    from tests.test_spec_118_adv_attribution import _filing, _fund_filing

    with engine.begin() as conn:
        # pe_firms holds a stale row for 8361; the roster has fresher facts
        conn.execute(text("""
            INSERT INTO pe_firms (name, crd_number, status) VALUES ('ALPHA OLD NAME', '8361', 'Active')"""))
        conn.execute(text("""
            INSERT INTO sec_adv_roster_snapshots (crd_number, roster_date, legal_name,
                business_name, adviser_type, main_office_city, aum_total, sec_status, raw)
            VALUES ('8361','2026-09-01','ALPHA CAPITAL LLC','ALPHA CAPITAL','ria','Boston',
                    5000000000,'APPROVED','{"Any PE Funds":"Y"}'),
                   ('9999','2026-09-01','OMEGA ADVISORS LLC','OMEGA ADVISORS','ria','NYC',
                    1000000000,'APPROVED','{"Any VC Funds":"Y"}')"""))
        conn.execute(text("""
            INSERT INTO form_d_issuers (accession_number, cik, entity_name, is_primary) VALUES
              ('acc-1','0000000101','BRIGHTWATER GROWTH FUND III, L.P.', true)"""))
        conn.execute(text("""
            INSERT INTO form_d_offerings (accession_number, investment_fund_type,
                date_of_first_sale, total_offering_amount, total_amount_sold, is_indefinite, file_num)
            VALUES ('acc-1','Private Equity Fund','2025-03-01', 500000000, 250000000, false, '021-1')"""))
        _filing(conn, 1, "8361", date(2026, 8, 15))
        _fund_filing(conn, 1, "805-1", "BRIGHTWATER GROWTH FUND III LP")


def _snapshot(engine):
    from sqlalchemy import text

    out = {}
    with engine.connect() as conn:
        for t in ("pe_firms", "pe_funds", "sec_adv_private_funds"):
            out[t] = [tuple(r) for r in conn.execute(text(
                f"SELECT * FROM public.{t} ORDER BY 1, 2"))]
    return out


@pg
def test_dry_run_leaves_tables_unchanged(pgmart, monkeypatch):
    """T11: dry_run wrote pe_firms and sec_adv_private_funds before this spec."""
    from app.worker.executors import pe_marts

    _seed_marts(pgmart)
    monkeypatch.setattr(pe_marts, "get_engine", lambda: pgmart)
    before = _snapshot(pgmart)
    assert before["sec_adv_private_funds"] == []

    summary = pe_marts.run_pe_marts(skip_people=True, dry_run=True)

    assert _snapshot(pgmart) == before
    assert summary["dry_run"] is True
    # ...and it still says what a real run would change
    assert summary["firms"]["inserted"] == 1 and summary["firms"]["updated"] == 1
    assert summary["adv_private_funds"]["inserted"] == 1
    assert summary["funds"]["candidates"] == 1 and summary["funds"]["linked_adv_exact"] == 1
    from sqlalchemy import text
    with pgmart.connect() as conn:
        assert conn.execute(text("SELECT to_regclass('stg.mart_pe_firms')")).scalar() is None

    # the real run writes what the dry run predicted
    real = pe_marts.run_pe_marts(skip_people=True)
    assert real["firms"]["inserted"] == 1 and real["firms"]["updated"] == 1
    assert real["adv_private_funds"]["inserted"] == 1
    assert real["funds"]["inserted"] == 1
    after = _snapshot(pgmart)
    assert len(after["pe_firms"]) == 2 and len(after["sec_adv_private_funds"]) == 1


@pg
def test_adv_private_funds_guard(pgmart, monkeypatch):
    """T10: filings tables emptied by a bad load must not wipe the mart."""
    from sqlalchemy import text

    from app.core.copy_loader import PublishGuardError
    from app.marts import adv_private_funds

    monkeypatch.delenv("BULK_PUBLISH_GUARD_OVERRIDE", raising=False)
    _seed_marts(pgmart)
    with pgmart.begin() as conn:
        adv_private_funds.build(conn, today=date(2026, 9, 20))
        conn.execute(text("DELETE FROM sec_adv_private_fund_filings"))
    with pytest.raises(PublishGuardError, match="sec_adv_private_funds"):
        with pgmart.begin() as conn:
            adv_private_funds.build(conn, today=date(2026, 9, 20))
    with pgmart.connect() as conn:
        assert conn.execute(text("SELECT COUNT(*) FROM sec_adv_private_funds")).scalar() == 1


# ---------------------------------------------------------------------------
# T17: blank database can migrate past 0003
# ---------------------------------------------------------------------------

@pg
def test_quarantine_skips_absent_tables():
    from sqlalchemy import create_engine, text

    from app.core.quarantine import QuarantineRule, apply

    engine = create_engine(PG_URL)
    try:
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS public.spec129_never_created"))
            rule = QuarantineRule("absent", "spec129_never_created", "true", "quarantine", 1, "x")
            result = apply(conn, rules=[rule])
        assert result["total_rows"] == 0
        assert result["roots"][0]["live"] == 0 and result["roots"][0].get("absent") is True
    finally:
        engine.dispose()
