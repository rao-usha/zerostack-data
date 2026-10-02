"""
Tests for SPEC 152 — retire the old `job_postings:all` run; move its companies onto ats_boards.

Owner decisions 2026-10-02: Ashby stays blocked (robots.txt 401 = disallow-all, no override);
the old run is retired. Its Ashby fetches and its ungated website ATS detector must refuse
before any request; its stored rows stay readable; its Greenhouse / Lever companies are seeded
into the gated lane with tokens the lane verified.
"""

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

REPO = Path(__file__).resolve().parents[1]
SEEDS = REPO / "app" / "sources" / "ats_boards" / "data" / "board_seeds.json"

# The old run's company_ats_config rows (live, 2026-10-02)
OLD_ASHBY = {178, 232, 168, 233, 167, 175, 163, 228, 226, 229, 231}
# Greenhouse / Lever boards the lane verified in the 2026-10-02 dry run
MIGRATED_GH = {201, 171, 170, 191, 160, 190, 173, 172, 166, 162, 193, 164, 194, 199, 186, 192, 196,
               185, 203, 198, 161, 184}
MIGRATED_LEVER = {292, 295, 291}
NOT_VERIFIED = {169, 165, 294}          # Plaid, Netflix (not found), Ro (unverified)


@pytest.mark.unit
class TestUnscheduled:
    def test_not_in_batch_tiers_groups_or_defaults(self):
        """T1"""
        from app.catalog.datasets import BATCH_SCHEDULED_DISPATCH
        from app.core.batch_service import DEFAULT_COLLECTION_GROUPS, TIERS
        from app.core.scheduler_service import DEFAULT_SCHEDULES

        assert all(sd.key != "job_postings:all" for t in TIERS for sd in t.sources)
        assert all("job_postings:all" not in g["sources"] for g in DEFAULT_COLLECTION_GROUPS)
        assert all(not str(t["source"]).startswith("job_postings") for t in DEFAULT_SCHEDULES)
        assert "job_postings:all" not in BATCH_SCHEDULED_DISPATCH

    def test_db_override_drops_it(self, monkeypatch):
        """T2: the live DB override row removes the key even from a tier that still lists it."""
        from app.core import batch_service
        from app.core.batch_service import SourceDef, Tier

        tier = Tier(level=3, priority=5, name="t3",
                    sources=[SourceDef("job_postings:all", {"skip_recent_hours": 600}), SourceDef("fema")])
        monkeypatch.setattr(batch_service, "TIERS", [tier])
        override = SimpleNamespace(source_key="job_postings:all", enabled=False, tier_level=None,
                                   default_config={"retired": "2026-10-02"})
        db = MagicMock()

        def query(model):
            q = MagicMock()
            q.all.return_value = [override] if model.__name__ == "BatchSourceTierOverride" else []
            return q

        db.query.side_effect = query
        eff = batch_service.resolve_effective_tiers(db)
        keys = [sd.key for t in eff for sd in t.sources]
        assert keys == ["fema"]


@pytest.mark.unit
class TestOldCollectorRefuses:
    def test_ashby_client_refuses_without_request(self, monkeypatch):
        """T3"""
        import httpx

        from app.sources.job_postings.ats.ashby import AshbyClient
        from app.sources.job_postings.retired import OldLaneRetired

        monkeypatch.setattr(httpx, "AsyncClient", MagicMock(side_effect=AssertionError("client made")))
        with pytest.raises(OldLaneRetired, match="ats_boards") as ei:
            asyncio.run(AshbyClient().fetch_jobs("openai"))
        assert "robots" in str(ei.value).lower() and "401" in str(ei.value)

    def test_detector_refuses_without_request(self, monkeypatch):
        """T4"""
        import httpx

        from app.sources.job_postings.ats.detector import ATSDetector
        from app.sources.job_postings.retired import OldLaneRetired

        monkeypatch.setattr(httpx, "AsyncClient", MagicMock(side_effect=AssertionError("client made")))
        with pytest.raises(OldLaneRetired, match="ats_boards"):
            asyncio.run(ATSDetector().detect("Acme", "https://acme.example", None))
        with pytest.raises(OldLaneRetired):
            asyncio.run(ATSDetector().detect("Acme", None, "https://jobs.ashbyhq.com/acme"))

    @pytest.mark.parametrize("cached", [("ashby", "openai", None, None), None])
    def test_collector_reraises(self, cached):
        """T5: cached Ashby config -> Ashby refusal; no config -> detector refusal; never swallowed."""
        from app.sources.job_postings.collector import JobPostingCollector
        from app.sources.job_postings.retired import OldLaneRetired

        db = MagicMock()

        def execute(stmt, params=None):
            sql = str(stmt)
            r = MagicMock()
            if "FROM industrial_companies WHERE id" in sql:
                r.fetchone.return_value = (175, "OpenAI", "https://openai.com", None)
            elif "FROM company_ats_config WHERE company_id" in sql:
                r.fetchone.return_value = cached
            else:
                r.fetchone.return_value = None
            return r

        db.execute.side_effect = execute

        async def go():
            async with JobPostingCollector() as c:
                return await c.collect_company(db, 175)

        with pytest.raises(OldLaneRetired):
            asyncio.run(go())

        async def disc():
            async with JobPostingCollector() as c:
                return await c.discover_ats(db, 175)

        with pytest.raises(OldLaneRetired):
            asyncio.run(disc())

    def test_ingest_entry_points_raise(self, monkeypatch):
        """T6: every dispatch key of the old run fails loudly before the collector is built."""
        from app.sources.job_postings import ingest as jp
        from app.sources.job_postings.retired import OldLaneRetired

        monkeypatch.setattr(jp, "JobPostingCollector", MagicMock(side_effect=AssertionError("collector built")))
        db = MagicMock()
        for fn, kw in ((jp.ingest_job_postings_all, {}), (jp.ingest_job_postings_company, {"company_id": 5}),
                       (jp.ingest_job_postings_discover, {"company_id": 5})):
            with pytest.raises(OldLaneRetired, match="ats_boards"):
                asyncio.run(fn(db, 1, **kw))

    def test_trigger_routes_gone(self):
        """T7"""
        from fastapi import FastAPI
        from fastapi.testclient import TestClient

        from app.api.v1 import job_postings
        from app.core.database import get_db

        app = FastAPI()
        app.include_router(job_postings.router)
        db = MagicMock()
        app.dependency_overrides[get_db] = lambda: db
        client = TestClient(app)
        for path in ("/job-postings/collect/5", "/job-postings/collect-all", "/job-postings/discover-ats/5"):
            r = client.post(path, json={})
            assert r.status_code == 410, (path, r.status_code)
            assert "ats_boards" in r.json()["detail"]
        db.add.assert_not_called()
        db.commit.assert_not_called()


@pytest.mark.unit
class TestMigration:
    def _seeds(self):
        from app.sources.ats_boards.ingest import load_seeds

        return load_seeds()

    def test_migrated_seeds_load(self):
        """T8"""
        seeds = self._seeds()
        live = [s for s in seeds if not s.get("blocked") and s.get("industrial_company_id")]
        gh = {s["industrial_company_id"] for s in live if s["ats"] == "greenhouse"}
        lv = {s["industrial_company_id"] for s in live if s["ats"] == "lever"}
        assert gh == MIGRATED_GH and lv == MIGRATED_LEVER
        assert not ({s["industrial_company_id"] for s in live} & (OLD_ASHBY | NOT_VERIFIED))
        assert all(s["ats"] != "ashby" for s in live)
        for s in live:
            assert s["verified"]["rule"] in ("greenhouse_board_name", "name_in_postings")
            assert s["verified"]["on"] == "2026-10-02" and s["citation"]

    def test_ashby_recorded_blocked(self):
        """T9"""
        from app.sources.ats_boards import ingest

        seeds = self._seeds()
        blocked = [s for s in seeds if s.get("blocked")]
        assert {s["industrial_company_id"] for s in blocked if s.get("industrial_company_id")} == OLD_ASHBY
        assert all(s["ats"] == "ashby" and s["blocked"].startswith("robots_401") for s in blocked)
        assert not (set(ingest.migrated_industrial_ids()) & OLD_ASHBY)
        # a blocked seed is never offered to the fetcher, even for a company that is loaded
        db = MagicMock()

        def execute(stmt, params=None):
            r = MagicMock()
            sql = str(stmt)
            if "FROM industrial_companies WHERE id" in sql:
                r.fetchone.return_value = (175, "OpenAI", "https://openai.com", None)
            elif "FROM company_ats_config" in sql:
                r.fetchone.return_value = ("ashby", "openai")
            return r

        db.execute.side_effect = execute
        cos = ingest._companies(db, industrial_ids=[175])
        assert cos and all(t[0] != "ashby" for t in cos[0].tokens)
        # record_blocked: one ats_board upsert per blocked seed, status refused, no request
        db2 = MagicMock()
        n = ingest.record_blocked(db2, apply=True)
        assert n == len(blocked)
        params = [c.args[1] for c in db2.execute.call_args_list]
        assert all(p["status"] == "refused" and p["outcome"] == "robots_disallowed" for p in params)
        assert {p["token"] for p in params} == {s["token"] for s in blocked}
        db3 = MagicMock()
        assert ingest.record_blocked(db3, apply=False) == len(blocked)
        db3.execute.assert_not_called()

    def test_migrated_preset_no_slugs(self, monkeypatch):
        """T10"""
        from app.sources.ats_boards import collect, ingest

        assert set(ingest.migrated_industrial_ids()) == MIGRATED_GH | MIGRATED_LEVER
        seen = {}

        def fake_companies(db, preset=None, ciks=None, industrial_ids=None):
            seen["preset"] = preset
            return [collect.Company(name="Brex", industrial_company_id=170)]

        def fake_run(db, companies, apply=False, **kw):
            seen.update(kw)
            return {"boards_fetched": 1, "attempts": [], "boards": [{"new": 3}], "companies": len(companies)}

        monkeypatch.setattr(ingest, "_companies", fake_companies)
        monkeypatch.setattr(collect, "run", fake_run)
        out = asyncio.run(ingest.ingest_ats_boards(MagicMock(), job_id=1, preset="migrated"))
        assert seen["preset"] == "migrated" and seen["slugs"] is False
        assert out["rows_inserted"] == 3

    def test_seed_file_is_json_list(self):
        assert isinstance(json.loads(SEEDS.read_text(encoding="utf-8")), list)
