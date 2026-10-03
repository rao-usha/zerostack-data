"""
Tests for SPEC 153 — the ats_boards lane scheduled weekly over every active board.

Owner call 2026-10-02: schedule ats_boards WEEKLY. The refresh re-fetches only boards the lane
already verified (ats_board.status = 'active'), stored token only, through the open_web gate;
Ashby (robots.txt 401) and every non-active row stay unfetched.
"""

import asyncio
import threading
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest


def _board_rows():
    # id, ats_type, board_token, company_name, core_entity_id, cik, industrial_company_id, status
    return [
        (1, "greenhouse", "brex", "Brex", None, None, 170, "active"),
        (2, "lever", "acme", "Acme Corp", 9001, "123", None, "active"),
        (3, "ashby", "openai", "OpenAI", None, None, 232, "refused"),
        (4, "ashby", "sneaky", "Sneaky", None, None, 300, "active"),      # never offered
        (5, "greenhouse", "gone", "Gone Inc", None, None, 301, "not_found"),
        (6, "lever", "maybe", "Maybe Co", None, None, 302, "unverified"),
    ]


class _FakeDB:
    """Answers the two queries active_companies makes: boards, then aliases."""

    def __init__(self, rows, aliases=None):
        self.rows = rows
        self.aliases = aliases or {}
        self.sql = []

    def execute(self, stmt, params=None):
        sql = str(stmt)
        self.sql.append((sql, params))
        res = MagicMock()
        if "FROM ats_board" in sql:
            res.fetchall.return_value = [r[:7] for r in self.rows
                                         if r[7] == "active" and r[1] in ("greenhouse", "lever")]
        elif "core.alias" in sql:
            res.__iter__.return_value = iter([(a,) for a in self.aliases.get(params["e"], [])])
        else:
            res.fetchall.return_value = []
        return res


@pytest.mark.unit
class TestActivePreset:
    def test_active_companies_from_active_boards(self, monkeypatch):
        """T1"""
        from app.sources.ats_boards import ingest

        monkeypatch.setattr(ingest, "load_seeds", lambda path=None: [
            {"industrial_company_id": 170, "ats": "greenhouse", "token": "brex", "citation": "c",
             "aliases": ["Brex Inc."]}])
        cos = ingest.active_companies(_FakeDB(_board_rows(), {9001: ["Acme Corporation"]}))
        by = {c.name: c for c in cos}
        assert set(by) == {"Brex", "Acme Corp"}
        assert by["Brex"].tokens == [("greenhouse", "brex", "refresh", {"ats_board.id": 1})]
        assert "Brex Inc." in by["Brex"].aliases
        assert by["Acme Corp"].core_entity_id == 9001 and by["Acme Corp"].cik == "123"
        assert "Acme Corporation" in by["Acme Corp"].aliases

    def test_active_never_offers_ashby_or_refused(self, monkeypatch):
        """T2"""
        from app.sources.ats_boards import ingest

        monkeypatch.setattr(ingest, "load_seeds", lambda path=None: [
            {"industrial_company_id": 170, "ats": "greenhouse", "token": "brex", "citation": "c",
             "blocked": "robots_401"}])
        cos = ingest.active_companies(_FakeDB(_board_rows()))
        offered = {(a, t) for c in cos for a, t, _b, _e in c.tokens}
        assert all(a != "ashby" for a, _ in offered)
        assert ("greenhouse", "gone") not in offered and ("lever", "maybe") not in offered
        assert ("greenhouse", "brex") not in offered          # a blocked seed wins
        # the SQL itself filters status and ATS (a constant, no string-built input)
        from app.sources.ats_boards.ingest import ACTIVE_BOARDS_SQL
        q = str(ACTIVE_BOARDS_SQL)
        assert "status = 'active'" in q and "'greenhouse'" in q and "'lever'" in q and "ashby" not in q

    def test_active_chunks_within_cap(self, monkeypatch):
        """T3"""
        from app.sources.ats_boards import collect, discover, ingest

        cos = [collect.Company(name=f"C{i}", industrial_company_id=i,
                               tokens=[("greenhouse", f"c{i}", "refresh", {})]) for i in range(27)]
        monkeypatch.setattr(ingest, "active_companies", lambda db: cos)
        calls = []

        def fake_run(db, companies, apply=False, **kw):
            calls.append((len(companies), kw.get("slugs"), apply))
            return {"apply": apply, "companies": len(companies), "attempts": [{"outcome": "fetched"}],
                    "boards": [{"new": 1, "outcome": "fetched"} for _ in companies],
                    "boards_fetched": len(companies), "requests": 2 * len(companies)}

        monkeypatch.setattr(collect, "run", fake_run)
        out = asyncio.run(ingest.ingest_ats_boards(MagicMock(), job_id=1, preset="active", apply=True))
        assert [c[0] for c in calls] == [discover.MAX_COMPANIES, 27 - discover.MAX_COMPANIES]
        assert all(c[1] is False and c[2] is True for c in calls)
        assert out["report"]["boards_fetched"] == 27 and out["report"]["companies"] == 27
        assert out["report"]["requests"] == 54 and out["rows_inserted"] == 27

    def test_active_raises_when_nothing_fetched(self, monkeypatch):
        """T4"""
        from app.sources.ats_boards import collect, ingest

        monkeypatch.setattr(ingest, "active_companies", lambda db: [
            collect.Company(name="X", industrial_company_id=1, tokens=[("lever", "x", "refresh", {})])])
        monkeypatch.setattr(collect, "run", lambda db, companies, apply=False, **kw: {
            "apply": apply, "companies": 1, "attempts": [{"outcome": "retry_after"}], "boards": [],
            "boards_fetched": 0, "requests": 1})
        with pytest.raises(RuntimeError, match="no board fetched"):
            asyncio.run(ingest.ingest_ats_boards(MagicMock(), job_id=1, preset="active"))

    def test_collect_runs_off_event_loop(self, monkeypatch):
        """T5"""
        from app.sources.ats_boards import collect, ingest

        monkeypatch.setattr(ingest, "_companies", lambda db, **kw: [collect.Company(name="Brex")])
        seen = {}

        def fake_run(db, companies, apply=False, **kw):
            seen["thread"] = threading.current_thread()
            return {"apply": apply, "companies": 1, "attempts": [], "boards": [{"new": 0}],
                    "boards_fetched": 1, "requests": 1}

        monkeypatch.setattr(collect, "run", fake_run)
        asyncio.run(ingest.ingest_ats_boards(MagicMock(), job_id=1, preset="migrated"))
        assert seen["thread"] is not threading.main_thread()

    def test_cli_accepts_active(self, monkeypatch):
        """T10"""
        from app.sources.ats_boards import collect, ingest

        got = {}
        monkeypatch.setattr(ingest, "active_companies", lambda db: [collect.Company(name="A")])

        def fake_chunked(db, companies, apply=False, slugs=True):
            got.update(n=len(companies), apply=apply, slugs=slugs)
            return {"apply": apply, "companies": 1, "attempts": [], "boards": [], "boards_fetched": 0,
                    "requests": 0}

        monkeypatch.setattr(ingest, "run_chunked", fake_chunked)
        monkeypatch.setattr(collect, "_session", lambda: MagicMock())
        assert collect.main(["--preset", "active"]) == 0
        assert got == {"n": 1, "apply": False, "slugs": False}


@pytest.mark.unit
class TestSchedule:
    def test_weekly_template(self):
        """T6"""
        from app.core.models import ScheduleFrequency
        from app.core.scheduler_service import (ATS_BOARDS_WEEKLY, DEFAULT_SCHEDULES,
                                                validate_schedule_source)

        assert ATS_BOARDS_WEEKLY in DEFAULT_SCHEDULES
        t = ATS_BOARDS_WEEKLY
        assert t["source"] == "job:ingestion" and t["frequency"] == ScheduleFrequency.WEEKLY
        assert t["config"] == {"source": "ats_boards", "config": {"preset": "active", "apply": True}}
        assert 0 <= t["day_of_week"] <= 6 and 0 <= t["hour"] <= 23
        validate_schedule_source(t["source"])

    def test_install_idempotent(self, monkeypatch):
        """T7"""
        from app.core import scheduler_service as ss

        made = []
        monkeypatch.setattr(ss, "create_schedule", lambda db, **kw: made.append(kw) or SimpleNamespace(**kw))
        db = MagicMock()
        db.query.return_value.filter.return_value.first.return_value = None
        out = ss.install_ats_boards_weekly(db)
        assert out["created"] is True and len(made) == 1
        assert made[0]["is_active"] is True and made[0]["source"] == "job:ingestion"
        db.query.return_value.filter.return_value.first.return_value = SimpleNamespace(id=99)
        out2 = ss.install_ats_boards_weekly(db)
        assert out2 == {"created": False, "schedule_id": 99} and len(made) == 1

    def test_schedule_queues_worker_ingestion(self, monkeypatch):
        """T8"""
        from app.core import scheduler_service as ss

        sent = {}
        monkeypatch.setattr(ss, "submit_job", lambda db, job_type, payload, job_table_id=None, **kw:
                            sent.update(job_type=job_type, payload=payload) or {"id": 1})
        sched = SimpleNamespace(id=7, name="ATS", source="job:ingestion",
                                config=ss.ATS_BOARDS_WEEKLY["config"], frequency=ss.ATS_BOARDS_WEEKLY["frequency"],
                                hour=3, day_of_week=1, day_of_month=None, cron_expression=None,
                                last_job_id=None, next_run_at=None)
        db = MagicMock()
        db.refresh.side_effect = lambda job: setattr(job, "id", 42)
        asyncio.run(ss._run_job_schedule(db, sched))
        assert sent["job_type"] == "ingestion"
        assert sent["payload"]["source"] == "ats_boards"
        assert sent["payload"]["config"] == {"preset": "active", "apply": True}
        assert sent["payload"]["ingestion_job_id"] == 42


@pytest.mark.unit
def test_catalog_weekly_internal():
    """T9"""
    from app.catalog.datasets import CATALOG

    from app.catalog.datasets import SCHEDULE_DISPATCH
    from app.core.scheduler_service import DEFAULT_SCHEDULES

    ds = {d.key: d for d in CATALOG}["ats_boards"]
    assert ds.cadence == "weekly"
    assert ds.status_public == "internal"
    # the catalog's schedule set is exactly what the job:ingestion templates dispatch
    templated = {t["config"]["source"] for t in DEFAULT_SCHEDULES
                 if t["source"] == "job:ingestion" and t.get("config", {}).get("source")}
    assert SCHEDULE_DISPATCH == templated


# ---------------------------------------------------------------------------
# Review fixes (2026-10-02, same day)
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_two_active_boards_one_company_both_refreshed(monkeypatch):
    """T11: collect.run stops at a company's first fetched board, so two active boards grouped
    under one Company left the second one never re-read (postings never closed)."""
    from app.sources.ats_boards import collect, ingest

    monkeypatch.setattr(ingest, "load_seeds", lambda path=None: [])
    rows = [(1, "greenhouse", "acme", "Acme Corp", 9001, "123", None, "active"),
            (2, "lever", "acme-co", "Acme Corp", 9001, "123", None, "active")]
    cos = ingest.active_companies(_FakeDB(rows))
    offered = sorted(t for c in cos for t in c.tokens)
    assert offered == [("greenhouse", "acme", "refresh", {"ats_board.id": 1}),
                       ("lever", "acme-co", "refresh", {"ats_board.id": 2})]

    class _BF:
        def __init__(self, *a, **k):
            pass

    tried = []

    def fake_try(bf, co, ats, token, basis, ev):
        tried.append((ats, token))
        return SimpleNamespace(outcome="fetched", status="active", http_status=200, evidence={},
                               postings=[])

    monkeypatch.setattr(collect, "BoardFetcher", _BF)
    monkeypatch.setattr(collect, "try_board", fake_try)
    collect.run(None, cos, apply=False, fetcher=MagicMock(), slugs=False)
    assert sorted(tried) == [("greenhouse", "acme"), ("lever", "acme-co")]


@pytest.mark.unit
def test_scheduled_ingestion_job_records_real_source(monkeypatch):
    """T12: the ingestion_jobs row of a `job:ingestion` schedule names the dispatched source and
    its config. Recorded as source 'job:ingestion', a failed run's retry re-ran
    run_ingestion_job('job:ingestion', ...) -> 'Unknown source' (the lane never retried), and the
    job ledger never showed an ats_boards run."""
    from app.core import scheduler_service as ss

    monkeypatch.setattr(ss, "submit_job", lambda db, job_type, payload, job_table_id=None, **kw: {"id": 1})
    added = []
    db = MagicMock()
    db.add.side_effect = added.append
    db.refresh.side_effect = lambda job: setattr(job, "id", 42)
    sched = SimpleNamespace(id=7, name="ATS", source="job:ingestion", config=ss.ATS_BOARDS_WEEKLY["config"],
                            frequency=ss.ATS_BOARDS_WEEKLY["frequency"], hour=3, day_of_week=1,
                            day_of_month=None, cron_expression=None, last_job_id=None, next_run_at=None)
    asyncio.run(ss._run_job_schedule(db, sched))
    assert added[0].source == "ats_boards"
    assert added[0].config == {"preset": "active", "apply": True}
    assert added[0].schedule_id == 7 and added[0].trigger == "scheduled"

    # other job types keep the job:<type> source (entity_resolve, pe_mart_build precedents)
    added.clear()
    other = SimpleNamespace(**{**vars(sched), "source": "job:entity_resolve", "config": {}})
    asyncio.run(ss._run_job_schedule(db, other))
    assert added[0].source == "job:entity_resolve" and added[0].config == {}
