"""
Tests for SPEC 126a — mart input assertions, build ledger, ship gates as code.

If the 13F load failed on the 9th, the 10th's entity_resolve and pe_mart_build
ran on last quarter's data and reported success; the ship gates that caught
real bugs in SPEC_118/119 were run once, by hand, from a scratchpad; and a bulk
run with one failed release out of four read as a clean success.
"""
import asyncio
import importlib.util
import json
import os
from datetime import datetime, timedelta
from pathlib import Path

import pytest

PG_URL = os.environ.get("TEST_PG_URL")
pg = pytest.mark.skipif(not PG_URL, reason="TEST_PG_URL not set")

REPO = Path(__file__).resolve().parents[1]


def _migration(name):
    path = REPO / "alembic" / "versions" / f"{name}.py"
    spec = importlib.util.spec_from_file_location(f"mig_{name}", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# A pe_marts summary that passes every gate (ratios near the 2026-09-20 run).
GOOD_PE = {
    "firms": {"candidates": 100, "inserted": 0, "updated": 0},
    "adv_private_funds": {"candidates": 1000, "inserted": 0, "updated": 0,
                          "deleted": 0, "skipped_undated": 0},
    "funds": {"candidates": 1000, "inserted": 0, "updated": 0,
              "linked_adv_exact": 400, "linked_adv_family": 20, "linked_name_core": 50,
              "linked_related_person": 20, "linked_adv_platform": 100,
              "platform_advisers": 4, "agreed": 590, "disagreed": 0, "unlinked": 410},
    "people": {"candidate_pairs": 100, "firms_covered": 30, "tier_form_d_signer": 95,
               "tier_fund_admin": 3, "tier_cross_brand": 1, "tier_platform_fund": 1,
               "collides_same_firm": 0, "link_person_missing": 0},
}

GOOD_ENTITY = {
    "feeds": {"adv_roster": 500, "form_d_issuers": 300},
    "bridge": {"accepted": 40, "refused": 3},
    "resolve": {"entities_new": 5, "entities_merged": 0, "entities_written": 700},
}


def _copy(d):
    return json.loads(json.dumps(d))


# ---------------------------------------------------------------------------
# Unit — no database
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestMigration:
    def test_migration_0013_revision(self):
        """T1"""
        mod = _migration("0013_mart_build")
        assert mod.revision == "0013_mart_build"
        assert mod.down_revision == "0012_access_lockdown"
        ddl = " ".join(mod.UPGRADE_SQL)
        assert "core.mart_build" in ddl
        for col in ("inputs", "stage_counts", "gate_results", "refusal_reason",
                    "code_version", "ingestion_job_id", "job_queue_id", "dry_run"):
            assert col in ddl
        assert "'running', 'success', 'failed', 'refused'" in ddl

    def test_single_alembic_head(self):
        """Nothing else may claim 0012 as its parent."""
        parents = []
        for path in (REPO / "alembic" / "versions").glob("*.py"):
            src = path.read_text(encoding="utf-8")
            if 'down_revision' in src and '"0012_access_lockdown"' in src.split("down_revision", 1)[1][:80]:
                parents.append(path.name)
        assert parents == ["0013_mart_build.py"]


@pytest.mark.unit
class TestGates:
    def _eval(self, mart, summary, counts=None, baseline=None, **kw):
        from app.marts import gates

        return gates.evaluate(mart, summary, counts or {}, baseline, **kw)

    def test_good_summary_passes_without_baseline(self):
        """T2: first build: tolerance gates are skipped, never failed."""
        from app.marts import gates

        res = self._eval("pe_marts", GOOD_PE)
        assert gates.failures(res) == []
        assert res["funds_reproduce_links"]["passed"] is True
        drops = [k for k in res if k.startswith("drop:")]
        assert drops and all(res[k]["skipped"] for k in drops)

    def test_platform_share_and_reproduction(self):
        """T2: the SPEC_118 gate (>=95% of existing links reproduced) and the
        AngelList platform share."""
        from app.marts import gates

        s = _copy(GOOD_PE)
        s["funds"]["agreed"], s["funds"]["disagreed"] = 90, 10
        s["funds"]["linked_adv_platform"] = 500
        res = self._eval("pe_marts", s)
        assert set(gates.failures(res)) >= {"funds_reproduce_links", "platform_link_share"}
        assert res["funds_reproduce_links"]["value"] == pytest.approx(0.9)

    def test_people_tier_shape(self):
        from app.marts import gates

        s = _copy(GOOD_PE)
        s["people"]["tier_form_d_signer"] = 80
        s["people"]["tier_fund_admin"] = 18
        s["people"]["collides_same_firm"] = 2
        fails = set(gates.failures(self._eval("pe_marts", s)))
        assert {"people_signer_share", "people_fund_admin_share",
                "people_no_silent_merge"} <= fails

    def test_skipped_stage_skips_its_gates(self):
        from app.marts import gates

        s = {"firms": GOOD_PE["firms"]}
        res = self._eval("pe_marts", s)
        assert gates.failures(res) == []
        assert res["people_signer_share"]["skipped"] is True

    def test_drop_vs_baseline(self):
        """T2: candidates fall 50% vs the previous success -> fail; 10% -> pass."""
        from app.marts import gates

        baseline = {"stages": _copy(GOOD_PE), "tables": {"pe_funds_sec": 1000}}
        s = _copy(GOOD_PE)
        s["funds"]["candidates"] = 500
        res = self._eval("pe_marts", s, {"pe_funds_sec": 1000}, baseline)
        assert "drop:funds.candidates" in gates.failures(res)

        s["funds"]["candidates"] = 900
        res = self._eval("pe_marts", s, {"pe_funds_sec": 700}, baseline)
        fails = gates.failures(res)
        assert "drop:funds.candidates" not in fails
        assert "rows:pe_funds_sec" in fails  # published rows fell 30%

    def test_max_drop_env(self, monkeypatch):
        from app.marts import gates

        monkeypatch.setenv("MART_GATE_MAX_DROP", "0.6")
        baseline = {"stages": _copy(GOOD_PE), "tables": {}}
        s = _copy(GOOD_PE)
        s["funds"]["candidates"] = 500
        assert "drop:funds.candidates" not in gates.failures(
            self._eval("pe_marts", s, {}, baseline))

    def test_entity_gates(self):
        from app.marts import gates

        assert gates.failures(self._eval("entity_resolve", GOOD_ENTITY)) == []
        baseline = {"stages": _copy(GOOD_ENTITY),
                    "tables": {"entities_live": 10_000, "source_record": 20_000}}
        s = _copy(GOOD_ENTITY)
        s["resolve"]["entities_merged"] = 900  # > max(500, 2% of 10k)
        s["feeds"] = {"adv_roster": 0, "form_d_issuers": 0}
        fails = set(gates.failures(self._eval(
            "entity_resolve", s, {"entities_live": 9_900, "source_record": 20_000}, baseline)))
        assert {"entity_mass_merge", "feeds_nonempty", "drop:feeds.total"} <= fails

    def test_override(self):
        from app.marts import gates

        s = _copy(GOOD_PE)
        s["people"]["collides_same_firm"] = 1
        res = self._eval("pe_marts", s)
        assert gates.failures(res) == ["people_no_silent_merge"]
        assert gates.failures(res, override=["people_no_silent_merge"]) == []
        assert gates.failures(res, override=True) == []
        assert gates.failures(res, override=["other"]) == ["people_no_silent_merge"]


@pytest.mark.unit
class TestHelpers:
    def test_code_version_env_then_git(self, monkeypatch):
        """T3"""
        from app.marts import build_ledger

        monkeypatch.setenv("NEXDATA_GIT_SHA", "abc1234")
        assert build_ledger.code_version() == "abc1234"
        monkeypatch.delenv("NEXDATA_GIT_SHA")
        monkeypatch.delenv("GIT_SHA", raising=False)
        v = build_ledger.code_version()
        assert v and len(v) <= 64  # a sha from .git, or the app version

    def test_partial_prefix(self):
        """T4"""
        from app.core.ingestion_job_sync import PARTIAL_PREFIX, is_partial

        assert is_partial(f"{PARTIAL_PREFIX} 1 of 3 sec_13f release(s) failed")
        assert not is_partial("All 3 sec_13f release(s) failed")
        assert not is_partial(None)

    def test_inputs_for_stages(self):
        from app.marts import inputs

        assert inputs.sources_for(inputs.PE_MART_STAGE_INPUTS, ["firms"]) == ["sec_adv_roster"]
        both = inputs.sources_for(inputs.PE_MART_STAGE_INPUTS, ["firms", "funds", "people"])
        assert both == ["sec_adv_roster", "sec_form_d"]
        ent = inputs.sources_for(inputs.ENTITY_STAGE_INPUTS, ["feeds", "bridge"])
        assert "sec_13f" in ent and len(ent) == len(set(ent))
        for src in ent + both + ["sec_adv_schedule_d"]:
            assert src in inputs.MAX_AGE_DAYS


@pytest.mark.unit
def test_builds_route_is_user_level(monkeypatch):
    """T5: reads for any signed-in user, not admin-only like the rest of pe_marts."""
    from fastapi.routing import APIRoute

    from app.core import authz
    from app.core.config import reset_settings

    monkeypatch.setenv("DATABASE_URL", PG_URL or "postgresql://nobody:nobody@localhost:1/nothing")
    reset_settings()
    try:
        from app.main import app
    finally:
        reset_settings()

    routes = [r for r in app.routes
              if isinstance(r, APIRoute) and r.path == "/api/v1/pe/marts/builds"]
    assert routes and routes[0].methods == {"GET"}
    deps = set()

    def walk(dependant):
        for d in dependant.dependencies:
            deps.add(d.call)
            walk(d)

    walk(routes[0].dependant)
    assert authz.require_admin_for_writes in deps
    assert authz.require_admin not in deps


# ---------------------------------------------------------------------------
# Postgres
# ---------------------------------------------------------------------------


@pytest.fixture
def pgl():
    """raw.source_release (0004) + core.mart_build (0013) + a marker table the
    fake builds write, so a rollback is observable."""
    from sqlalchemy import create_engine, text

    engine = create_engine(PG_URL)
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS core.mart_build"))
        conn.execute(text("DROP TABLE IF EXISTS raw.source_release"))
        conn.execute(text("DROP TABLE IF EXISTS public.t126a_marker"))
        conn.execute(text("CREATE TABLE public.t126a_marker (v TEXT)"))
        for stmt in _migration("0004_bulk_framework").SOURCE_RELEASE_DDL:
            conn.execute(text(stmt))
        for stmt in _migration("0013_mart_build").UPGRADE_SQL:
            conn.execute(text(stmt))
    yield engine
    engine.dispose()


def _release(engine, source, key, status="loaded", *, loaded_days_ago=1.0,
             discovered_days_ago=None, error=None):
    from sqlalchemy import text

    now = datetime.utcnow()
    disc = now - timedelta(days=discovered_days_ago if discovered_days_ago is not None
                           else loaded_days_ago + 0.01)
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO raw.source_release (source, release_key, url, status,
                discovered_at, loaded_at, error)
            VALUES (:s, :k, 'https://www.sec.gov/x', :st, :d, :l, :e)
        """), {"s": source, "k": key, "st": status, "d": disc,
               "l": (now - timedelta(days=loaded_days_ago)) if status == "loaded" else None,
               "e": error})


def _fresh_pe_inputs(engine):
    _release(engine, "sec_adv_roster", "ria:2026-08", loaded_days_ago=5)
    _release(engine, "sec_adv_roster", "era:2026-08", loaded_days_ago=5)
    _release(engine, "sec_adv_schedule_d", "2026-08@x", loaded_days_ago=6)
    _release(engine, "sec_form_d", "2026q2", loaded_days_ago=40)


def _builds(engine, mart=None):
    from sqlalchemy import text

    with engine.connect() as conn:
        rows = conn.execute(text(
            "SELECT * FROM core.mart_build WHERE (:m IS NULL OR mart = :m) ORDER BY id"
        ), {"m": mart}).mappings().all()
    return [dict(r) for r in rows]


def _markers(engine):
    from sqlalchemy import text

    with engine.connect() as conn:
        return [r[0] for r in conn.execute(text("SELECT v FROM public.t126a_marker"))]


def _fake_pe_stages(monkeypatch, summary=None, calls=None, raise_exc=None):
    """Replace the real stage runner: write a marker on the build connection
    and return a canned summary."""
    from sqlalchemy import text

    from app.worker.executors import pe_marts

    def fake(conn_for, skip_firms, skip_funds, skip_people, dry_run):
        with conn_for() as conn:
            conn.execute(text("INSERT INTO public.t126a_marker VALUES ('built')"))
        if calls is not None:
            calls.append(dry_run)
        if raise_exc:
            raise raise_exc
        return _copy(summary or GOOD_PE)

    monkeypatch.setattr(pe_marts, "_run_stages", fake)


@pg
def test_refused_on_failed_latest_release(pgl, monkeypatch):
    """T6: the newest Form D release failed -> refuse, nothing built."""
    from app.marts.build_ledger import MartInputRefused
    from app.worker.executors import pe_marts

    _fresh_pe_inputs(pgl)
    _release(pgl, "sec_form_d", "2026q3", status="failed", discovered_days_ago=1,
             error="BadZipFile: truncated")
    calls = []
    _fake_pe_stages(monkeypatch, calls=calls)
    monkeypatch.setattr(pe_marts, "get_engine", lambda: pgl)

    with pytest.raises(MartInputRefused, match="sec_form_d.*2026q3.*failed"):
        pe_marts.run_pe_marts(guard=True, job_queue_id=7, ingestion_job_id=3)

    assert calls == [] and _markers(pgl) == []
    [row] = _builds(pgl)
    assert row["mart"] == "pe_marts" and row["status"] == "refused"
    assert "sec_form_d" in row["refusal_reason"] and row["finished_at"] is not None
    assert row["job_queue_id"] == 7 and row["ingestion_job_id"] == 3
    by_src = {i["source"]: i for i in row["inputs"]}
    assert by_src["sec_form_d"]["ok"] is False
    assert by_src["sec_adv_roster"]["ok"] is True
    assert sorted(by_src["sec_adv_roster"]["release_keys"]) == ["era:2026-08", "ria:2026-08"]


@pg
def test_refused_on_partial_discovery_batch(pgl, monkeypatch):
    """ria loaded, era of the same month failed: half a roster is refused."""
    from app.marts.build_ledger import MartInputRefused
    from app.worker.executors import pe_marts

    _release(pgl, "sec_adv_roster", "ria:2026-09", loaded_days_ago=1)
    _release(pgl, "sec_adv_roster", "era:2026-09", status="failed", discovered_days_ago=1.01)
    _fake_pe_stages(monkeypatch)
    monkeypatch.setattr(pe_marts, "get_engine", lambda: pgl)
    with pytest.raises(MartInputRefused, match="era:2026-09"):
        pe_marts.run_pe_marts(skip_funds=True, skip_people=True, guard=True)


@pg
def test_older_failure_does_not_refuse(pgl, monkeypatch):
    """A long-dead old release is recorded, not a reason to refuse."""
    from app.worker.executors import pe_marts

    _fresh_pe_inputs(pgl)
    _release(pgl, "sec_form_d", "2023q3", status="failed", discovered_days_ago=400)
    _fake_pe_stages(monkeypatch)
    monkeypatch.setattr(pe_marts, "get_engine", lambda: pgl)
    pe_marts.run_pe_marts(guard=True)
    [row] = _builds(pgl)
    assert row["status"] == "success"
    form_d = next(i for i in row["inputs"] if i["source"] == "sec_form_d")
    assert form_d["older_unloaded"] == 1 and form_d["ok"] is True


@pg
def test_refused_on_stale_input(pgl, monkeypatch):
    """T7: the roster last loaded 200 days ago (monthly source, max 75)."""
    from app.marts.build_ledger import MartInputRefused
    from app.worker.executors import pe_marts

    _release(pgl, "sec_adv_roster", "ria:2026-02", loaded_days_ago=200)
    _fake_pe_stages(monkeypatch)
    monkeypatch.setattr(pe_marts, "get_engine", lambda: pgl)
    with pytest.raises(MartInputRefused, match="sec_adv_roster.*200"):
        pe_marts.run_pe_marts(skip_funds=True, skip_people=True, guard=True)
    assert _builds(pgl)[0]["status"] == "refused"

    # a per-input max age (admin payload) accepts it
    pe_marts.run_pe_marts(skip_funds=True, skip_people=True, guard=True,
                          input_max_age_days={"sec_adv_roster": 365})
    last = _builds(pgl)[-1]
    assert last["status"] == "success"
    assert last["overrides"]["input_max_age_days"] == {"sec_adv_roster": 365}


@pg
def test_refused_on_missing_input(pgl, monkeypatch):
    """T8: never loaded at all."""
    from app.marts.build_ledger import MartInputRefused
    from app.worker.executors import pe_marts

    _fake_pe_stages(monkeypatch)
    monkeypatch.setattr(pe_marts, "get_engine", lambda: pgl)
    with pytest.raises(MartInputRefused, match="sec_adv_roster.*no loaded release"):
        pe_marts.run_pe_marts(skip_funds=True, skip_people=True, guard=True)


@pg
def test_input_override_proceeds_and_is_recorded(pgl, monkeypatch):
    """T9"""
    from app.marts.build_ledger import MartInputRefused
    from app.worker.executors import pe_marts

    _fresh_pe_inputs(pgl)
    _release(pgl, "sec_form_d", "2026q3", status="failed", discovered_days_ago=1)
    _fake_pe_stages(monkeypatch)
    monkeypatch.setattr(pe_marts, "get_engine", lambda: pgl)

    with pytest.raises(MartInputRefused):  # overriding a different source is not enough
        pe_marts.run_pe_marts(guard=True, input_override=["sec_adv_roster"])
    out = pe_marts.run_pe_marts(guard=True, input_override=["sec_form_d"])
    row = _builds(pgl)[-1]
    assert row["status"] == "success"
    assert row["overrides"]["input_override"] == ["sec_form_d"]
    assert any("sec_form_d" in w for w in row["overrides"]["inputs_accepted"])
    assert out["mart_build"]["status"] == "success"
    assert _markers(pgl) == ["built"]


@pg
def test_gate_failure_rolls_back(pgl, monkeypatch):
    """T10: candidates fall 50% vs the previous successful build -> nothing kept."""
    from app.marts.build_ledger import MartGateFailed
    from app.worker.executors import pe_marts

    _fresh_pe_inputs(pgl)
    monkeypatch.setattr(pe_marts, "get_engine", lambda: pgl)
    _fake_pe_stages(monkeypatch)
    pe_marts.run_pe_marts(guard=True)  # baseline
    assert _markers(pgl) == ["built"]

    bad = _copy(GOOD_PE)
    bad["funds"]["candidates"] = 500
    _fake_pe_stages(monkeypatch, summary=bad)
    with pytest.raises(MartGateFailed, match=r"drop:funds\.candidates"):
        pe_marts.run_pe_marts(guard=True)

    assert _markers(pgl) == ["built"]  # the second build's write was rolled back
    rows = _builds(pgl)
    assert [r["status"] for r in rows] == ["success", "failed"]
    gate = rows[1]["gate_results"]["drop:funds.candidates"]
    assert gate["passed"] is False and gate["value"] == 500 and gate["baseline"] == 1000
    assert "drop:funds.candidates" in rows[1]["error"]
    assert rows[1]["stage_counts"]["stages"]["funds"]["candidates"] == 500


@pg
def test_within_tolerance_commits(pgl, monkeypatch):
    """T11: 10% drop passes; the ledger carries release keys, counts, version."""
    from app.worker.executors import pe_marts

    _fresh_pe_inputs(pgl)
    monkeypatch.setattr(pe_marts, "get_engine", lambda: pgl)
    monkeypatch.setenv("NEXDATA_GIT_SHA", "deadbeef")
    _fake_pe_stages(monkeypatch)
    pe_marts.run_pe_marts(guard=True)
    ok = _copy(GOOD_PE)
    ok["funds"]["candidates"] = 900
    _fake_pe_stages(monkeypatch, summary=ok)
    out = pe_marts.run_pe_marts(guard=True)

    rows = _builds(pgl)
    assert [r["status"] for r in rows] == ["success", "success"]
    r = rows[1]
    assert r["code_version"] == "deadbeef" and r["dry_run"] is False
    assert r["gate_results"]["drop:funds.candidates"]["passed"] is True
    keys = {i["source"]: i["release_keys"] for i in r["inputs"]}
    assert keys["sec_form_d"] == ["2026q2"] and keys["sec_adv_schedule_d"] == ["2026-08@x"]
    assert all(i["loaded_at"] for i in r["inputs"])
    assert "tables" in r["stage_counts"]
    assert out["mart_build"]["id"] == r["id"]
    assert _markers(pgl) == ["built", "built"]


@pg
def test_gate_override_commits(pgl, monkeypatch):
    """T12"""
    from app.worker.executors import pe_marts

    _fresh_pe_inputs(pgl)
    monkeypatch.setattr(pe_marts, "get_engine", lambda: pgl)
    bad = _copy(GOOD_PE)
    bad["people"]["collides_same_firm"] = 3
    _fake_pe_stages(monkeypatch, summary=bad)
    pe_marts.run_pe_marts(guard=True, gate_override=["people_no_silent_merge"])
    [row] = _builds(pgl)
    assert row["status"] == "success"
    assert row["gate_results"]["people_no_silent_merge"]["passed"] is False
    assert row["gate_results"]["people_no_silent_merge"]["overridden"] is True
    assert row["overrides"]["gate_override"] == ["people_no_silent_merge"]
    assert _markers(pgl) == ["built"]


@pg
def test_dry_run_is_gated_and_rolled_back(pgl, monkeypatch):
    """T13: a dry run keeps nothing, is ledgered, and is never a baseline."""
    from app.marts.build_ledger import MartGateFailed
    from app.worker.executors import pe_marts

    _fresh_pe_inputs(pgl)
    monkeypatch.setattr(pe_marts, "get_engine", lambda: pgl)
    calls = []
    _fake_pe_stages(monkeypatch, calls=calls)
    out = pe_marts.run_pe_marts(guard=True, dry_run=True)
    assert calls == [True] and out["dry_run"] is True
    assert _markers(pgl) == []
    [row] = _builds(pgl)
    assert row["dry_run"] is True and row["status"] == "success"

    # the dry run is not a baseline: this drop has nothing to compare against
    bad = _copy(GOOD_PE)
    bad["funds"]["candidates"] = 500
    _fake_pe_stages(monkeypatch, summary=bad)
    pe_marts.run_pe_marts(guard=True)
    assert _builds(pgl)[-1]["status"] == "success"

    # a failing dry run still reports failure
    worse = _copy(GOOD_PE)
    worse["people"]["collides_same_firm"] = 1
    _fake_pe_stages(monkeypatch, summary=worse)
    with pytest.raises(MartGateFailed):
        pe_marts.run_pe_marts(guard=True, dry_run=True)
    assert _builds(pgl)[-1]["status"] == "failed"


@pg
def test_build_error_marks_failed(pgl, monkeypatch):
    """T14: a stage raising rolls back every stage and leaves a failed row."""
    from app.worker.executors import pe_marts

    _fresh_pe_inputs(pgl)
    monkeypatch.setattr(pe_marts, "get_engine", lambda: pgl)
    _fake_pe_stages(monkeypatch, raise_exc=RuntimeError("people merge exploded"))
    with pytest.raises(RuntimeError, match="exploded"):
        pe_marts.run_pe_marts(guard=True)
    assert _markers(pgl) == []
    [row] = _builds(pgl)
    assert row["status"] == "failed" and "exploded" in row["error"]


@pg
def test_abandoned_running_rows_are_closed(pgl, monkeypatch):
    from sqlalchemy import text

    from app.worker.executors import pe_marts

    with pgl.begin() as conn:
        conn.execute(text(
            "INSERT INTO core.mart_build (mart, status, started_at) "
            "VALUES ('pe_marts', 'running', NOW() - INTERVAL '2 days')"))
    _fresh_pe_inputs(pgl)
    monkeypatch.setattr(pe_marts, "get_engine", lambda: pgl)
    _fake_pe_stages(monkeypatch)
    pe_marts.run_pe_marts(guard=True)
    rows = _builds(pgl)
    assert rows[0]["status"] == "failed" and "abandoned" in rows[0]["error"]
    assert rows[1]["status"] == "success"


@pg
def test_unguarded_run_keeps_direct_behaviour(pgl, monkeypatch):
    """Direct calls (tests, scripts) do not assert or ledger, but still run in
    one transaction."""
    from app.worker.executors import pe_marts

    monkeypatch.setattr(pe_marts, "get_engine", lambda: pgl)
    _fake_pe_stages(monkeypatch)
    pe_marts.run_pe_marts()
    assert _markers(pgl) == ["built"] and _builds(pgl) == []


# --- entity_resolve ----------------------------------------------------------


def _fake_entity(monkeypatch, summary=None):
    from sqlalchemy import text

    from app.entities import cik_crd_bridge, feeds, resolve

    s = _copy(summary or GOOD_ENTITY)

    def run_feeds(conn, progress=None):
        conn.execute(text("INSERT INTO public.t126a_marker VALUES ('feeds')"))
        return s["feeds"]

    monkeypatch.setattr(feeds, "run_feeds", run_feeds)
    monkeypatch.setattr(cik_crd_bridge, "build",
                        lambda conn, include_name_tier=True: s["bridge"])
    monkeypatch.setattr(resolve, "resolve", lambda conn, dry_run=False: s["resolve"])


def _fresh_entity_inputs(engine):
    _release(engine, "sec_adv_roster", "ria:2026-08", loaded_days_ago=5)
    _release(engine, "sec_iapd_feed", "edition:2026-09-22", loaded_days_ago=1)
    _release(engine, "sec_13f", "2026q2", loaded_days_ago=30)
    _release(engine, "sec_form_d", "2026q2", loaded_days_ago=40)
    _release(engine, "sec_edgar_submissions", "2026-09-22", loaded_days_ago=1)
    _release(engine, "sec_insider", "2026q2", loaded_days_ago=40)


@pg
def test_entity_resolve_guarded(pgl, monkeypatch):
    """T15"""
    from app.marts.build_ledger import MartInputRefused
    from app.worker.executors import entity_resolve

    monkeypatch.setattr(entity_resolve, "get_engine", lambda: pgl)
    _fake_entity(monkeypatch)
    _fresh_entity_inputs(pgl)
    _release(pgl, "sec_13f", "2026q3", status="fetched", discovered_days_ago=1)

    with pytest.raises(MartInputRefused, match="sec_13f.*fetched"):
        entity_resolve.run_entity_master(guard=True)
    assert _markers(pgl) == []

    # skipping the stages that read 13F removes it from the inputs
    entity_resolve.run_entity_master(guard=True, skip_feeds=True, skip_bridge=True)
    out = entity_resolve.run_entity_master(guard=True, input_override=True)
    rows = _builds(pgl, "entity_resolve")
    assert [r["status"] for r in rows] == ["refused", "success", "success"]
    assert rows[1]["inputs"] == []
    assert out["mart_build"]["status"] == "success"
    assert _markers(pgl) == ["feeds"]


@pg
def test_entity_dry_run_keeps_nothing(pgl, monkeypatch):
    """Before this spec feeds + bridge wrote even in dry run."""
    from app.worker.executors import entity_resolve

    monkeypatch.setattr(entity_resolve, "get_engine", lambda: pgl)
    _fake_entity(monkeypatch)
    _fresh_entity_inputs(pgl)
    entity_resolve.run_entity_master(dry_run=True)  # unguarded
    entity_resolve.run_entity_master(dry_run=True, guard=True)
    assert _markers(pgl) == []
    assert _builds(pgl, "entity_resolve")[0]["dry_run"] is True


# --- executors ---------------------------------------------------------------


@pg
def test_executor_pe_marts_fails_job_on_refusal(pgl, monkeypatch):
    """T16: the executor runs guarded, passes the job ids, and a refusal fails
    the job with the reason."""
    from unittest.mock import MagicMock

    from app.marts.build_ledger import MartInputRefused
    from app.worker.executors import pe_marts

    monkeypatch.setattr(pe_marts, "get_engine", lambda: pgl)
    _fake_pe_stages(monkeypatch)
    job = MagicMock(payload={"ingestion_job_id": 55}, id=77)
    with pytest.raises(MartInputRefused, match="no loaded release"):
        asyncio.run(pe_marts.execute(job, MagicMock()))
    [row] = _builds(pgl)
    assert row["status"] == "refused" and row["job_queue_id"] == 77
    assert row["ingestion_job_id"] == 55

    _fresh_pe_inputs(pgl)
    job = MagicMock(payload={"gate_override": True}, id=78)
    asyncio.run(pe_marts.execute(job, MagicMock()))
    assert "build #" in job.progress_message
    assert _builds(pgl)[-1]["overrides"]["gate_override"] is True


@pg
def test_executor_entity_resolve_guarded(pgl, monkeypatch):
    from unittest.mock import MagicMock

    from app.worker.executors import entity_resolve

    monkeypatch.setattr(entity_resolve, "get_engine", lambda: pgl)
    _fake_entity(monkeypatch)
    _fresh_entity_inputs(pgl)
    # the executor's progress callback opens its own session; keep it off the DB
    import app.core.database as database
    monkeypatch.setattr(database, "get_session_factory", lambda: MagicMock())
    job = MagicMock(payload={"input_max_age_days": {"sec_13f": 400}}, id=90)
    asyncio.run(entity_resolve.execute(job, MagicMock()))
    [row] = _builds(pgl, "entity_resolve")
    assert row["status"] == "success" and row["job_queue_id"] == 90
    assert row["overrides"]["input_max_age_days"] == {"sec_13f": 400}


# --- watchdog ----------------------------------------------------------------


@pg
def test_watchdog_mart_build_rule(pgl):
    """T18: open while the latest real build is failed/refused; dry runs and
    running rows do not count."""
    from sqlalchemy import text
    from sqlalchemy.orm import sessionmaker

    from app.services import data_watchdog as wd

    db = sessionmaker(bind=pgl)()
    try:
        assert wd.rule_mart_builds(db, datetime.utcnow()) == []
        with pgl.begin() as conn:
            conn.execute(text("""
                INSERT INTO core.mart_build (mart, status, dry_run, started_at, finished_at,
                                             refusal_reason, error)
                VALUES ('pe_marts', 'success', FALSE, NOW() - INTERVAL '40 days', NOW() - INTERVAL '40 days', NULL, NULL),
                       ('pe_marts', 'refused', FALSE, NOW() - INTERVAL '1 day', NOW() - INTERVAL '1 day',
                        'sec_form_d: latest release 2026q3 is failed', NULL),
                       ('pe_marts', 'failed', TRUE, NOW(), NOW(), NULL, 'dry'),
                       ('entity_resolve', 'success', FALSE, NOW(), NOW(), NULL, NULL),
                       ('entity_resolve', 'running', FALSE, NOW(), NULL, NULL, NULL)
            """))
        [f] = wd.rule_mart_builds(db, datetime.utcnow())
        assert f.key == "mart_build:pe_marts" and f.severity == "critical"
        assert "refused" in f.message and "sec_form_d" in f.message
        assert "mart_build" in dict(wd.RULES)

        with pgl.begin() as conn:
            conn.execute(text(
                "INSERT INTO core.mart_build (mart, status, started_at, finished_at) "
                "VALUES ('pe_marts', 'success', NOW(), NOW())"))
        assert wd.rule_mart_builds(db, datetime.utcnow()) == []
    finally:
        db.close()


# --- GET /pe/marts/builds ----------------------------------------------------


@pg
def test_builds_endpoint_lists_rows(pgl):
    """T19"""
    from sqlalchemy import text
    from sqlalchemy.orm import sessionmaker

    from app.api.v1 import mart_builds

    with pgl.begin() as conn:
        conn.execute(text("""
            INSERT INTO core.mart_build (mart, status, started_at, inputs, gate_results)
            VALUES ('pe_marts', 'success', NOW() - INTERVAL '1 hour', '[]', '{}'),
                   ('pe_marts', 'refused', NOW(), '[{"source": "sec_form_d"}]', NULL),
                   ('entity_resolve', 'success', NOW(), '[]', '{}')
        """))
    db = sessionmaker(bind=pgl)()
    try:
        out = mart_builds.list_builds(mart=None, status=None, limit=10, db=db)
        assert [b["mart"] for b in out["builds"]][:1] in (["pe_marts"], ["entity_resolve"])
        assert out["count"] == 3
        only = mart_builds.list_builds(mart="pe_marts", status="refused", limit=10, db=db)
        assert only["count"] == 1 and only["builds"][0]["inputs"] == [{"source": "sec_form_d"}]
    finally:
        db.close()


# --- bulk tri-state ------------------------------------------------------------


@pytest.fixture
def pgq(monkeypatch):
    """ingestion_jobs + job_queue, session factories pointed at them (SPEC_121 pattern)."""
    from sqlalchemy import create_engine, text
    from sqlalchemy.orm import sessionmaker

    from app.core.models import Base, IngestionJob, IngestionSchedule
    from app.core.models_queue import JobEvent, JobQueue

    engine = create_engine(PG_URL)
    tables = [IngestionSchedule.__table__, IngestionJob.__table__, JobQueue.__table__,
              JobEvent.__table__]
    with engine.begin() as conn:
        for t in reversed(tables):
            conn.execute(text(f"DROP TABLE IF EXISTS public.{t.name} CASCADE"))
    Base.metadata.create_all(engine, tables=tables)
    factory = sessionmaker(bind=engine)
    import app.worker.executors.bulk_ingest as bi
    import app.worker.main as wm

    for mod in (wm, bi):
        monkeypatch.setattr(mod, "get_session_factory", lambda: factory)
    yield engine, factory
    engine.dispose()


def _bulk_job(engine):
    from sqlalchemy import text

    with engine.begin() as conn:
        ing = conn.execute(text("""
            INSERT INTO ingestion_jobs (source, status, config, created_at, retry_count,
                                        max_retries, data_origin)
            VALUES ('bulk:sec_13f', 'pending', '{}', NOW(), 0, 3, 'real') RETURNING id
        """)).scalar()
        qid = conn.execute(text("""
            INSERT INTO job_queue (job_type, job_table_id, status, priority, payload, created_at)
            VALUES ('bulk_ingest', :i, 'pending', 0, CAST(:p AS json), NOW()) RETURNING id
        """), {"i": ing, "p": json.dumps({"bulk_source": "sec_13f",
                                          "ingestion_job_id": ing})}).scalar()
    return ing, qid


def _run_bulk_via_worker(engine, factory, qid, monkeypatch, summary):
    import app.worker.executors.bulk_ingest as bi
    import app.worker.main as wm
    from app.core.models_queue import JobQueue, QueueJobType

    monkeypatch.setattr(bi, "get_source", lambda name: object())
    monkeypatch.setattr(bi, "run_source", lambda *a, **kw: dict(summary))
    monkeypatch.setitem(wm.EXECUTORS, QueueJobType.BULK_INGEST, bi.execute)
    monkeypatch.setattr(wm, "send_job_event", lambda *a, **kw: None)
    db = factory()
    try:
        job = db.get(JobQueue, qid)
        asyncio.run(wm.execute_job(job, db))
    finally:
        db.close()


def _rows(engine, ing, qid):
    from sqlalchemy import text

    with engine.connect() as conn:
        i = conn.execute(text("SELECT status, error_message, rows_inserted FROM ingestion_jobs "
                              "WHERE id=:i"), {"i": ing}).one()
        q = conn.execute(text("SELECT status, error_message, progress_message FROM job_queue "
                              "WHERE id=:i"), {"i": qid}).one()
    return i, q


def _summary(loaded, failed, rows=10):
    return {"source": "sec_13f", "loaded": loaded, "failed": failed, "skipped": 0,
            "rows": rows, "errors": [f"2026q{n}: BadZipFile" for n in range(failed)],
            "releases": []}


@pg
def test_bulk_partial(pgq, monkeypatch):
    """T17: 1 of 3 failed -> queue success + PARTIAL, IngestionJob success + PARTIAL."""
    from app.core.ingestion_job_sync import is_partial

    engine, factory = pgq
    ing, qid = _bulk_job(engine)
    _run_bulk_via_worker(engine, factory, qid, monkeypatch, _summary(2, 1))
    (i_status, i_err, i_rows), (q_status, q_err, q_prog) = _rows(engine, ing, qid)
    assert i_status == "success" and is_partial(i_err) and "1 of 3" in i_err
    assert "BadZipFile" in i_err and i_rows == 10
    assert q_status == "success" and is_partial(q_err)
    assert q_prog == "Completed with partial failures"


@pg
def test_bulk_all_loaded_is_clean(pgq, monkeypatch):
    engine, factory = pgq
    ing, qid = _bulk_job(engine)
    _run_bulk_via_worker(engine, factory, qid, monkeypatch, _summary(3, 0))
    (i_status, i_err, _), (q_status, q_err, q_prog) = _rows(engine, ing, qid)
    assert (i_status, i_err, q_status, q_err, q_prog) == (
        "success", None, "success", None, "Completed")


@pg
def test_bulk_all_failed_still_fails(pgq, monkeypatch):
    engine, factory = pgq
    ing, qid = _bulk_job(engine)
    _run_bulk_via_worker(engine, factory, qid, monkeypatch, _summary(0, 2, rows=0))
    (i_status, i_err, _), (q_status, q_err, _) = _rows(engine, ing, qid)
    assert i_status == "failed" and "All 2" in i_err
    assert q_status == "failed" and not q_err.startswith("PARTIAL")


# --- the real stages, guarded ------------------------------------------------


from tests.test_spec_129_ci_and_publish_guards import _seed_marts, pgmart  # noqa: E402,F401


@pg
def test_real_pe_stages_guarded(pgmart, monkeypatch):  # noqa: F811
    """The real firms/ADV/funds stages run inside the guarded transaction:
    published-row counts are read on the open transaction without breaking it,
    the gates accept a real summary, and a second identical build passes the
    tolerance gates against the first."""
    from sqlalchemy import text

    from app.worker.executors import pe_marts

    engine = pgmart
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS core.mart_build"))
        for stmt in _migration("0013_mart_build").UPGRADE_SQL:
            conn.execute(text(stmt))
    _seed_marts(engine)
    monkeypatch.delenv("BULK_PUBLISH_GUARD_OVERRIDE", raising=False)
    monkeypatch.setattr(pe_marts, "get_engine", lambda: engine)

    first = pe_marts.run_pe_marts(skip_people=True, guard=True, input_override=True)
    second = pe_marts.run_pe_marts(skip_people=True, guard=True, input_override=True)
    rows = _builds(engine, "pe_marts")
    assert [r["status"] for r in rows] == ["success", "success"]
    assert first["funds"]["candidates"] == second["funds"]["candidates"] == 1
    tables = rows[1]["stage_counts"]["tables"]
    assert tables["pe_funds_sec"] == 1 and tables["sec_adv_private_funds"] == 1
    assert rows[1]["gate_results"]["rows:pe_funds_sec"]["passed"] is True
    assert rows[1]["gate_results"]["rows:pe_funds_sec"]["skipped"] is False
    assert rows[1]["overrides"]["input_override"] is True
