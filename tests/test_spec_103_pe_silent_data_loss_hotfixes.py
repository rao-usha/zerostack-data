"""
Tests for SPEC 103 — PE/People silent data-loss hotfixes (D1–D5, D22).
"""
import importlib.util
from datetime import date
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

REPO = Path(__file__).resolve().parents[1]


# ---------------------------------------------------------------------------
# D1 — pe_firm_people.role_type
# ---------------------------------------------------------------------------

@pytest.mark.unit
class TestD1RoleTypeColumn:
    def test_d1_alembic_migration_adds_role_type(self):
        """T1: Migration exists, chains from baseline, adds role_type."""
        path = REPO / "alembic" / "versions" / "0001_pe_firm_people_role_type.py"
        assert path.exists()
        spec = importlib.util.spec_from_file_location("mig0001", path)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        assert mod.revision == "0001_pe_firm_people_role_type"
        assert mod.down_revision == "3fb893199e22"
        executed = []
        fake_op = MagicMock()
        fake_op.execute.side_effect = lambda sql: executed.append(str(sql))
        with patch.object(mod, "op", fake_op):
            mod.upgrade()
        joined = " ".join(executed).lower()
        assert "pe_firm_people" in joined
        assert "add column if not exists role_type" in joined

    def test_d1_runtime_alter_list_has_role_type(self):
        """T2: _apply_schema_migrations includes the role_type ALTER."""
        from app.core import database

        engine = MagicMock()
        conn = engine.connect.return_value.__enter__.return_value
        database._apply_schema_migrations(engine)
        sqls = [str(c.args[0]).lower() for c in conn.execute.call_args_list]
        assert any(
            "pe_firm_people" in s and "role_type" in s for s in sqls
        ), sqls


# ---------------------------------------------------------------------------
# D2 — PEPersister savepoint isolation
# ---------------------------------------------------------------------------

def _pe_item(item_type, data):
    from app.sources.pe_collection.types import PECollectedItem, EntityType

    return PECollectedItem(
        item_type=item_type, entity_type=EntityType.FIRM, data=data,
        source_url=None, confidence="medium",
    )


def _pe_result(items):
    from app.sources.pe_collection.types import (
        PECollectionResult, PECollectionSource, EntityType,
    )

    return PECollectionResult(
        entity_id=1, entity_name="Blackstone", entity_type=EntityType.FIRM,
        source=PECollectionSource.FIRM_WEBSITE, success=True, items=items,
    )


@pytest.fixture
def persister_db(test_db):
    from app.core.pe_models import PEFirm

    test_db.add(PEFirm(id=1, name="Blackstone", status="Active"))
    test_db.commit()
    return test_db


@pytest.mark.unit
class TestD2PersisterSavepoints:
    def _run(self, db):
        from app.core.pe_models import PEPerson
        from app.sources.pe_collection.persister import PEPersister

        persister = PEPersister(db)
        original = persister._persist_team_member

        def flaky(entity_id, entity_name, item):
            if item.data.get("full_name") == "Bad Person":
                persister.stats["persisted"] += 1  # mimic handler counting early
                db.add(PEPerson(full_name="Half Written"))
                db.flush()
                raise ValueError("boom")
            return original(entity_id, entity_name, item)

        persister._persist_team_member = flaky
        good = _pe_item("team_member", {"full_name": "Good Person", "title": "Analyst"})
        bad = _pe_item("team_member", {"full_name": "Bad Person", "title": "VP"})
        stats = persister.persist_results([_pe_result([good, bad])])
        return stats

    def test_d2_failing_item_does_not_roll_back_earlier_items(self, persister_db):
        """T3: Earlier good item survives a later failing item."""
        from app.core.pe_models import PEPerson

        self._run(persister_db)
        names = {p.full_name for p in persister_db.query(PEPerson).all()}
        assert "Good Person" in names
        assert "Half Written" not in names

    def test_d2_failed_item_not_counted_as_persisted(self, persister_db):
        """T4: Stats reflect reality."""
        stats = self._run(persister_db)
        assert stats["failed"] == 1
        assert stats["persisted"] == 1


# ---------------------------------------------------------------------------
# D3 — leadership changes with string change_type
# ---------------------------------------------------------------------------

@pytest.mark.unit
class TestD3StoreChanges:
    @pytest.mark.asyncio
    async def test_d3_store_changes_with_string_change_type(self, test_db):
        """T5: A leadership_changes row is created."""
        from app.core.people_models import IndustrialCompany
        from app.core.people_models import LeadershipChange as LeadershipChangeModel
        from app.sources.people_collection.orchestrator import PeopleCollectionOrchestrator
        from app.sources.people_collection.types import ChangeType, LeadershipChange

        company = IndustrialCompany(name="Acme Industrial")
        test_db.add(company)
        test_db.commit()

        change = LeadershipChange(
            person_name="Jane Doe",
            change_type=ChangeType.HIRE,
            new_title="CFO",
            effective_date=date(2026, 9, 1),
            source_type="press_release",
        )
        assert isinstance(change.change_type, str)

        orch = PeopleCollectionOrchestrator(db_session=test_db)
        await orch._store_changes([change], company, test_db)
        test_db.commit()

        rows = test_db.query(LeadershipChangeModel).all()
        assert len(rows) == 1
        assert rows[0].change_type == "hire"


# ---------------------------------------------------------------------------
# D4 — dedup reports_to re-point + savepoint
# ---------------------------------------------------------------------------

@pytest.fixture
def real_dedup_module():
    """tests/test_dedup_service.py imports dedup_service against mocked models
    and leaves it cached; reload against the real models."""
    import importlib
    import app.core.people_models  # noqa: F401  (ensure real module is loaded)
    import app.services.dedup_service as mod

    return importlib.reload(mod)


@pytest.mark.unit
@pytest.mark.usefixtures("real_dedup_module")
class TestD4Dedup:
    def test_d4_reassign_references_repoints_reports_to(self, test_db):
        """T6: Direct report re-pointed to surviving role before delete."""
        from app.core.people_models import CompanyPerson, IndustrialCompany, Person
        from app.services.dedup_service import DedupService

        company = IndustrialCompany(name="Acme")
        test_db.add(company)
        test_db.flush()
        canonical = Person(full_name="John Smith", first_name="John", last_name="Smith")
        dup = Person(full_name="Jon Smith", first_name="Jon", last_name="Smith")
        report = Person(full_name="Amy Report", first_name="Amy", last_name="Report")
        test_db.add_all([canonical, dup, report])
        test_db.flush()
        keep_cp = CompanyPerson(company_id=company.id, person_id=canonical.id,
                                title="CEO", is_current=True)
        dup_cp = CompanyPerson(company_id=company.id, person_id=dup.id,
                               title="CEO", is_current=True)
        test_db.add_all([keep_cp, dup_cp])
        test_db.flush()
        report_cp = CompanyPerson(company_id=company.id, person_id=report.id,
                                  title="VP", is_current=True, reports_to_id=dup_cp.id)
        test_db.add(report_cp)
        test_db.commit()

        DedupService(test_db)._reassign_references(canonical.id, dup.id)
        test_db.commit()

        test_db.refresh(report_cp)
        assert report_cp.reports_to_id == keep_cp.id

    def test_d4_failed_auto_merge_keeps_session_usable(self, test_db):
        """T7: IntegrityError in _auto_merge doesn't poison the scan."""
        from app.core.people_models import (
            CompanyPerson, IndustrialCompany, PeopleMergeCandidate, Person,
        )
        from app.services.dedup_service import DedupService

        company = IndustrialCompany(name="Acme")
        test_db.add(company)
        test_db.flush()
        p1 = Person(full_name="John Smith", first_name="John", last_name="Smith")
        p2 = Person(full_name="John Smith", first_name="John", last_name="Smith")
        test_db.add_all([p1, p2])
        test_db.flush()
        for p in (p1, p2):
            test_db.add(CompanyPerson(company_id=company.id, person_id=p.id,
                                      title="CEO", is_current=True))
        test_db.commit()

        svc = DedupService(test_db)
        svc.matcher = MagicMock()
        svc.matcher.compare.return_value = MagicMock(
            matched=True, similarity=0.99, match_type="exact", notes="test"
        )
        svc.matcher.classify_match.return_value = "auto_merge"

        def broken_merge(*args, **kwargs):
            test_db.add(IndustrialCompany(name="Acme"))  # unique violation
            test_db.flush()

        svc._auto_merge = broken_merge
        stats = svc.scan_for_duplicates(company_id=company.id)

        assert stats["review_queued"] == 1
        assert test_db.query(PeopleMergeCandidate).count() == 1

    def test_d4_orchestrator_rolls_back_after_dedup_failure(self):
        """Both post-storage dedup except blocks call session.rollback()."""
        src = (REPO / "app/sources/people_collection/orchestrator.py").read_text(encoding="utf-8")
        for marker in ("Post-storage dedup scan skipped", "[DIAG] Post-storage dedup scan skipped"):
            idx = src.index(marker)
            block = src[max(0, idx - 300):idx]
            assert "rollback()" in block, marker


# ---------------------------------------------------------------------------
# D5 — no shared session across concurrent coroutines
# ---------------------------------------------------------------------------

@pytest.mark.unit
class TestD5SessionPerTask:
    @pytest.mark.asyncio
    async def test_d5_collect_batch_does_not_share_provided_session(self):
        """T8: collect_company inside collect_batch never sees the shared session."""
        from app.sources.people_collection.orchestrator import PeopleCollectionOrchestrator
        from app.sources.people_collection.types import CollectionResult

        shared = MagicMock(name="shared_session")
        orch = PeopleCollectionOrchestrator(db_session=shared)
        seen = []

        async def fake_collect(self, company_id, sources=None):
            seen.append(self._provided_session)
            return CollectionResult(company_id=company_id, company_name="x",
                                    source="website", success=True)

        with patch.object(PeopleCollectionOrchestrator, "collect_company", fake_collect):
            await orch.collect_batch([1, 2, 3], max_concurrent=3)

        assert len(seen) == 3
        assert all(s is None for s in seen)
        assert orch._provided_session is shared

    @pytest.mark.asyncio
    async def test_d5_bio_parse_all_uses_session_per_person(self):
        """T9: Each parse_person gets its own session, each closed."""
        from app.services.bio_parser_service import BioParserService

        people = [MagicMock(id=i, full_name=f"P{i}", bio="x" * 60) for i in (1, 2)]
        db = MagicMock(name="shared")
        q = db.query.return_value
        q.outerjoin.return_value.filter.return_value.order_by.return_value.all.return_value = [
            (people[0], None), (people[1], None)
        ]
        q.distinct.return_value.all.return_value = []

        made = []

        def factory():
            s = MagicMock(name=f"task_session_{len(made)}")
            made.append(s)
            return s

        svc = BioParserService()
        used = []

        async def fake_parse(**kwargs):
            used.append(kwargs["db"])
            return {"experience_created": 0, "education_created": 0}

        svc.parse_person = fake_parse
        stats = await svc.parse_all(db, session_factory=factory)

        assert stats["parsed"] == 2
        assert len(set(map(id, used))) == 2
        assert db not in used
        for s in made:
            s.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_d5_lp_runner_uses_session_per_lp(self):
        """T10: _collect_single_lp runs on a distinct session per LP, each closed."""
        from app.sources.lp_collection.runner import LpCollectionOrchestrator

        shared = MagicMock(name="shared")
        made = []

        def factory():
            s = MagicMock(name=f"lp_session_{len(made)}")
            made.append(s)
            return s

        orch = LpCollectionOrchestrator(shared, session_factory=factory)
        lps = [MagicMock(id=1, name="CalPERS"), MagicMock(id=2, name="CalSTRS")]
        orch.select_lps_for_collection = MagicMock(return_value=lps)
        used = []

        async def fake_single(self, lp, job_id):
            used.append(self.db)
            return []

        with patch.object(LpCollectionOrchestrator, "_collect_single_lp", fake_single):
            await orch.run_collection_job()

        assert len(used) == 2
        assert shared not in used
        assert len(set(map(id, used))) == 2
        for s in made:
            s.close.assert_called_once()


# ---------------------------------------------------------------------------
# D22 — SEC rate limits
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_d22_sec_rate_limits():
    """T11: Registry 480/min; limiter 8 rps."""
    from app.core.api_registry import API_REGISTRY
    from app.core.rate_limiter import DEFAULT_RATE_LIMITS

    assert API_REGISTRY["sec"].rate_limit_per_minute == 480
    assert DEFAULT_RATE_LIMITS["sec"]["requests_per_second"] == 8.0
