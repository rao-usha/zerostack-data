"""
Tests for SPEC 104 — Stop the code paths that write fake or misclassified data.
"""
import importlib
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

REPO = Path(__file__).resolve().parents[1]


def _src(rel: str) -> str:
    return (REPO / rel).read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# PE persister / collectors
# ---------------------------------------------------------------------------

def _pe_item(item_type, data):
    from app.sources.pe_collection.types import PECollectedItem, EntityType

    return PECollectedItem(item_type=item_type, entity_type=EntityType.FIRM,
                           data=data, source_url=None, confidence="high")


def _pe_result(items):
    from app.sources.pe_collection.types import (
        PECollectionResult, PECollectionSource, EntityType,
    )

    return PECollectionResult(entity_id=1, entity_name="Blackstone",
                              entity_type=EntityType.FIRM,
                              source=PECollectionSource.FIRM_WEBSITE,
                              success=True, items=items)


@pytest.fixture
def pe_db(test_db):
    from app.core.pe_models import PEFirm

    test_db.add(PEFirm(id=1, name="Blackstone", status="Active"))
    test_db.commit()
    return test_db


@pytest.mark.unit
class TestPEWriters:
    def test_persister_skips_13f_holding(self, pe_db):
        """T1"""
        from app.core.pe_models import PEFund, PEFundInvestment, PEPortfolioCompany
        from app.sources.pe_collection.persister import PEPersister

        item = _pe_item("13f_holding", {"issuer_name": "Apple Inc",
                                        "security_class": "COM", "value_usd": 5})
        stats = PEPersister(pe_db).persist_results([_pe_result([item])])
        assert stats["skipped"] == 1
        assert stats["persisted"] == 0
        assert pe_db.query(PEPortfolioCompany).count() == 0
        assert pe_db.query(PEFund).count() == 0
        assert pe_db.query(PEFundInvestment).count() == 0

    def test_persister_skips_deal_8k_filing(self, pe_db):
        """T2"""
        from app.core.pe_models import PEDeal
        from app.sources.pe_collection.persister import PEPersister

        item = _pe_item("deal_8k_filing", {"title": "8-K", "company_name": "Carlyle Group Inc."})
        stats = PEPersister(pe_db).persist_results([_pe_result([item])])
        assert stats["skipped"] == 1
        assert pe_db.query(PEDeal).count() == 0

    def test_press_release_collector_emits_no_8k_items(self):
        """T3"""
        src = _src("app/sources/pe_collection/deal_collectors/press_release_collector.py")
        assert 'item_type="deal_8k_filing"' not in src


# ---------------------------------------------------------------------------
# site_intel fabricated data
# ---------------------------------------------------------------------------

LOGISTICS = [
    ("fmcsa_collector", "FMCSACollector"),
    ("warehouse_listing_collector", None),
    ("scfi_collector", None),
    ("drewry_collector", None),
    ("freightos_collector", None),
    ("census_trade_collector", None),
    ("port_throughput_collector", None),
    ("air_cargo_collector", None),
    ("usda_truck_collector", None),
]


@pytest.mark.unit
class TestSiteIntelFabrication:
    @pytest.mark.parametrize("module", [m for m, _ in LOGISTICS])
    def test_logistics_collectors_have_no_sample_fallbacks(self, module):
        """T4"""
        src = _src(f"app/sources/site_intel/logistics/{module}.py")
        assert "_get_sample_" not in src
        assert "Using sample" not in src

    @pytest.mark.asyncio
    async def test_fmcsa_empty_api_reports_failure(self):
        """T5"""
        from app.sources.site_intel.logistics.fmcsa_collector import FMCSACollector
        from app.sources.site_intel.types import (
            CollectionConfig, CollectionStatus, SiteIntelDomain, SiteIntelSource,
        )

        collector = FMCSACollector(MagicMock())
        collector.get_client = AsyncMock(return_value=MagicMock())
        collector.apply_rate_limit = AsyncMock()
        collector._fetch_carriers_by_state = AsyncMock(return_value=[])
        collector.bulk_upsert = MagicMock(return_value=(0, 0))
        cfg = CollectionConfig(domain=SiteIntelDomain.LOGISTICS,
                               source=SiteIntelSource.FMCSA, states=["TX"])
        result = await collector.collect(cfg)

        assert result.status != CollectionStatus.SUCCESS
        assert result.inserted_items == 0
        collector.bulk_upsert.assert_not_called()

    @pytest.mark.asyncio
    async def test_goodjobs_has_no_seed_data(self):
        """T6"""
        mod = importlib.import_module("app.sources.site_intel.incentives.goodjobs_collector")
        assert not hasattr(mod, "GJF_SEED_DATA")
        from app.sources.site_intel.types import (
            CollectionConfig, CollectionStatus, SiteIntelDomain, SiteIntelSource,
        )

        collector = mod.GoodJobsFirstCollector(MagicMock())
        cfg = CollectionConfig(domain=SiteIntelDomain.INCENTIVES,
                               source=SiteIntelSource.GOOD_JOBS_FIRST)
        result = await collector.collect(cfg)
        assert result.status == CollectionStatus.FAILED


# ---------------------------------------------------------------------------
# Form ADV samples
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_form_adv_sample_removed():
    """T7"""
    from app.api.v1 import form_adv
    from app.sources.sec_form_adv import client, ingest

    assert not hasattr(client.FormADVClient, "get_sample_advisers")
    assert not hasattr(ingest.FormADVIngestionService, "ingest_sample_data")
    with pytest.raises(HTTPException) as exc:
        form_adv.ingest_data(db=MagicMock())
    assert exc.value.status_code == 501


# ---------------------------------------------------------------------------
# LP / family office
# ---------------------------------------------------------------------------

@pytest.mark.unit
class TestLpFoWriters:
    def test_lp_website_no_regex_contacts(self):
        """T8"""
        from app.sources.lp_collection.website_source import WebsiteCollector

        assert not hasattr(WebsiteCollector, "_extract_contacts_from_page")
        assert not hasattr(WebsiteCollector, "_parse_contact_blocks")

    def test_lp_runner_skips_13f_holding(self):
        """T9"""
        from app.sources.lp_collection.runner import LpCollectionOrchestrator

        db = MagicMock()
        orch = LpCollectionOrchestrator(db)
        item = MagicMock(item_type="13f_holding", data={"cik": "1"})
        orch._persist_items(1, [item])
        db.add.assert_not_called()
        assert not hasattr(LpCollectionOrchestrator, "_persist_13f_holding")

    @pytest.mark.asyncio
    async def test_fo_deals_collector_disabled(self):
        """T10"""
        from app.sources.family_office_collection.deals_source import FoDealsCollector

        collector = FoDealsCollector()
        with patch("httpx.AsyncClient") as client_cls:
            result = await collector.collect(fo_id=1, fo_name="Cascade Investment")
            client_cls.assert_not_called()
        assert result.items == []


# ---------------------------------------------------------------------------
# Demo seeders
# ---------------------------------------------------------------------------

@pytest.mark.unit
class TestDemoSeed:
    @pytest.mark.asyncio
    async def test_demo_seed_endpoints_forbidden_by_default(self, monkeypatch):
        """T11"""
        monkeypatch.delenv("ALLOW_DEMO_SEED", raising=False)
        from app.api.v1 import pe_benchmarks, pe_ecosystem

        with pytest.raises(HTTPException) as e1:
            await pe_benchmarks.seed_demo_data(db=MagicMock())
        assert e1.value.status_code == 403
        with pytest.raises(HTTPException) as e2:
            pe_ecosystem.seed_ecosystem(seed=42, db=MagicMock())
        assert e2.value.status_code == 403

    def test_ecosystem_purge_keeps_same_named_real_firm(self, test_db):
        """T12"""
        from app.core.pe_models import PEFirm
        from app.services.pe_ecosystem_seed import FIRM_TEMPLATES, PEEcosystemSeeder

        name = FIRM_TEMPLATES[0][0]
        test_db.add(PEFirm(name=name, status="Active", data_sources=["sec_adv"]))
        test_db.commit()
        PEEcosystemSeeder(test_db).purge()
        assert test_db.query(PEFirm).filter_by(name=name).count() == 1


# ---------------------------------------------------------------------------
# Batch tiers
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_prediction_markets_not_in_default_batch():
    """T13"""
    from app.core.batch_service import DEFAULT_COLLECTION_GROUPS, TIER_1

    assert "prediction_markets" not in [s.key for s in TIER_1.sources]
    critical = next(g for g in DEFAULT_COLLECTION_GROUPS if g["name"] == "critical")
    assert "prediction_markets" not in critical["sources"]
