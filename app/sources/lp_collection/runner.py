"""
LP Collection Orchestrator.

Main entry point for running LP data collection jobs:
- Selects LPs based on filters and staleness
- Coordinates multiple collectors
- Tracks progress and handles errors
- Persists results to database
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any, Type

from sqlalchemy.orm import Session

from app.core.models import (
    LpFund,
    LpKeyContact,
    LpGovernanceMember,
    LpBoardMeeting,
    LpPerformanceReturn,
    LpStrategySnapshot,
    LpCollectionRun,
    LpCollectionJob,
    LpCollectionSourceType,
    LpCollectionStatus,
    PortfolioCompany,
)
from app.sources.lp_collection.types import (
    CollectionConfig,
    CollectionResult,
    CollectedItem,
    LpCollectionSource,
    JobProgress,
)
from app.sources.lp_collection.base_collector import BaseCollector
from app.sources.lp_collection.website_source import WebsiteCollector
from app.sources.lp_collection.sec_adv_source import SecAdvCollector
from app.sources.lp_collection.sec_13f_source import Sec13fCollector
from app.sources.lp_collection.form_990_source import Form990Collector
from app.sources.lp_collection.cafr_source import CafrCollector
from app.sources.lp_collection.news_source import NewsCollector
from app.sources.lp_collection.governance_source import GovernanceCollector
from app.sources.lp_collection.performance_source import PerformanceCollector
from app.sources.lp_collection.normalizer import DataNormalizer

logger = logging.getLogger(__name__)


# Collector registry
COLLECTORS: Dict[LpCollectionSource, Type[BaseCollector]] = {
    LpCollectionSource.WEBSITE: WebsiteCollector,
    LpCollectionSource.SEC_ADV: SecAdvCollector,
    LpCollectionSource.SEC_13F: Sec13fCollector,
    LpCollectionSource.FORM_990: Form990Collector,
    LpCollectionSource.CAFR: CafrCollector,
    LpCollectionSource.NEWS: NewsCollector,
    LpCollectionSource.GOVERNANCE: GovernanceCollector,
    LpCollectionSource.PERFORMANCE: PerformanceCollector,
}


class LpCollectionOrchestrator:
    """
    Orchestrates LP data collection across multiple sources.

    Responsibilities:
    - Select LPs for collection based on filters
    - Instantiate and coordinate collectors
    - Handle rate limiting and concurrency
    - Track progress and persist results
    """

    def __init__(
        self,
        db: Session,
        config: Optional[CollectionConfig] = None,
    ):
        """
        Initialize the orchestrator.

        Args:
            db: Database session
            config: Collection configuration
        """
        self.db = db
        self.config = config or CollectionConfig()
        self.normalizer = DataNormalizer()
        self._collectors: Dict[LpCollectionSource, BaseCollector] = {}
        self._progress: Optional[JobProgress] = None

    def _get_collector(self, source: LpCollectionSource) -> BaseCollector:
        """Get or create a collector for the given source."""
        if source not in self._collectors:
            collector_class = COLLECTORS.get(source)
            if not collector_class:
                raise ValueError(f"No collector available for source: {source}")
            self._collectors[source] = collector_class(
                rate_limit_delay=self.config.rate_limit_delay,
                max_retries=self.config.max_retries,
            )
        return self._collectors[source]

    def select_lps_for_collection(self) -> List[LpFund]:
        """
        Select LPs for collection based on configuration.

        Returns:
            List of LpFund records to collect
        """
        query = self.db.query(LpFund)

        # Filter by specific LP ID(s)
        if self.config.lp_id:
            return query.filter(LpFund.id == self.config.lp_id).all()

        if self.config.lp_ids:
            return query.filter(LpFund.id.in_(self.config.lp_ids)).all()

        # Filter by LP type
        if self.config.lp_types:
            query = query.filter(LpFund.lp_type.in_(self.config.lp_types))

        # Filter by region
        if self.config.regions:
            query = query.filter(LpFund.region.in_(self.config.regions))

        # Filter by staleness (for incremental mode)
        if self.config.mode.value == "incremental":
            cutoff = datetime.utcnow() - timedelta(days=self.config.max_age_days)
            query = query.filter(
                (LpFund.last_collection_at == None)
                | (LpFund.last_collection_at < cutoff)
            )

        # Order by collection priority
        query = query.order_by(LpFund.collection_priority.asc())

        return query.all()

    async def run_collection_job(self) -> LpCollectionJob:
        """
        Run a complete collection job.

        Returns:
            LpCollectionJob record with results
        """
        # Create job record
        job = LpCollectionJob(
            job_type="batch" if not self.config.lp_id else "single_lp",
            config=self.config.to_dict(),
            lp_types=self.config.lp_types,
            regions=self.config.regions,
            sources=[s.value for s in self.config.sources],
            mode=self.config.mode.value,
            max_age_days=self.config.max_age_days,
            status="pending",
        )
        self.db.add(job)
        self.db.commit()
        self.db.refresh(job)

        logger.info(f"Created collection job {job.id}")

        # Select LPs
        lps = self.select_lps_for_collection()
        job.total_lps = len(lps)
        job.status = "running"
        job.started_at = datetime.utcnow()
        self.db.commit()

        if not lps:
            job.status = "success"
            job.completed_at = datetime.utcnow()
            self.db.commit()
            logger.info(f"No LPs to collect for job {job.id}")
            return job

        # Initialize progress tracking
        self._progress = JobProgress(
            job_id=job.id,
            total_lps=len(lps),
        )

        # Collect data
        all_results: List[CollectionResult] = []

        # Process LPs with concurrency limit
        semaphore = asyncio.Semaphore(self.config.max_concurrent_lps)

        async def collect_lp(lp: LpFund) -> None:
            async with semaphore:
                self._progress.current_lp = lp.name
                results = await self._collect_single_lp(lp, job.id)
                all_results.extend(results)

                # Track progress
                self._progress.completed_lps += 1
                if all(r.success for r in results):
                    self._progress.successful_lps += 1
                else:
                    self._progress.failed_lps += 1

                # Update job progress
                job.completed_lps = self._progress.completed_lps
                job.successful_lps = self._progress.successful_lps
                job.failed_lps = self._progress.failed_lps
                self.db.commit()

        # Run collection tasks
        tasks = [collect_lp(lp) for lp in lps]
        await asyncio.gather(*tasks, return_exceptions=True)

        # Finalize job
        job.completed_at = datetime.utcnow()
        job.status = self._determine_job_status(all_results)

        # Calculate totals
        job.total_items_found = sum(r.items_found for r in all_results)
        job.total_items_inserted = sum(r.items_new for r in all_results)
        job.total_items_updated = sum(r.items_updated for r in all_results)

        self.db.commit()

        logger.info(
            f"Completed collection job {job.id}: "
            f"{job.successful_lps}/{job.total_lps} LPs successful"
        )

        return job

    async def _collect_single_lp(
        self,
        lp: LpFund,
        job_id: int,
    ) -> List[CollectionResult]:
        """
        Collect data for a single LP from all configured sources.

        Args:
            lp: LP fund to collect
            job_id: Parent job ID

        Returns:
            List of CollectionResult (one per source)
        """
        results: List[CollectionResult] = []

        for source in self.config.sources:
            run = None
            try:
                # Create run record
                run = LpCollectionRun(
                    lp_id=lp.id,
                    source_type=LpCollectionSourceType(source.value),
                    job_id=job_id if job_id > 0 else None,
                    status=LpCollectionStatus.RUNNING,
                    started_at=datetime.utcnow(),
                )
                self.db.add(run)
                self.db.commit()
                self.db.refresh(run)

                # Get collector and run
                collector = self._get_collector(source)
                result = await collector.collect(
                    lp_id=lp.id,
                    lp_name=lp.name,
                    website_url=lp.website_url,
                    sec_crd_number=lp.sec_crd_number,
                )

                # Persist collected items
                if result.success and result.items:
                    try:
                        self._persist_items(lp.id, result.items)
                    except Exception as persist_error:
                        logger.error(
                            f"Error persisting items for {lp.name}: {persist_error}"
                        )
                        self.db.rollback()
                        result.warnings.append(
                            f"Failed to persist items: {persist_error}"
                        )

                # Update run record
                run.status = (
                    LpCollectionStatus.SUCCESS
                    if result.success
                    else LpCollectionStatus.FAILED
                )
                run.items_found = result.items_found
                run.items_inserted = result.items_new
                run.items_updated = result.items_updated
                run.error_message = result.error_message
                run.warnings = result.warnings if result.warnings else None
                run.requests_made = result.requests_made
                run.bytes_downloaded = result.bytes_downloaded
                run.completed_at = datetime.utcnow()
                run.duration_seconds = int(result.duration_seconds or 0)
                self.db.commit()

                results.append(result)

            except Exception as e:
                logger.error(f"Error collecting {source.value} for {lp.name}: {e}")
                self.db.rollback()

                if run:
                    try:
                        run.status = LpCollectionStatus.FAILED
                        run.error_message = str(e)
                        run.completed_at = datetime.utcnow()
                        self.db.commit()
                    except Exception:
                        self.db.rollback()

                results.append(
                    CollectionResult(
                        lp_id=lp.id,
                        lp_name=lp.name,
                        source=source,
                        success=False,
                        error_message=str(e),
                    )
                )

        # Update LP last collection time if any succeeded
        if any(r.success for r in results):
            lp.last_collection_at = datetime.utcnow()
            self.db.commit()

        return results

    def _persist_items(self, lp_id: int, items: List[CollectedItem]) -> None:
        """
        Persist collected items to the database.

        Args:
            lp_id: LP fund ID
            items: List of collected items
        """
        for item in items:
            try:
                if item.item_type == "contact":
                    self._persist_contact(lp_id, item)
                elif item.item_type == "strategy_info":
                    self._persist_strategy_info(lp_id, item)
                elif item.item_type == "governance_member":
                    self._persist_governance_member(lp_id, item)
                elif item.item_type == "board_meeting":
                    self._persist_board_meeting(lp_id, item)
                elif item.item_type == "performance_return":
                    self._persist_performance_return(lp_id, item)
                elif item.item_type == "13f_holding":
                    self._persist_13f_holding(lp_id, item)
                elif item.item_type in ("990_org_info", "990_financials"):
                    self._persist_990_data(lp_id, item)
                elif item.item_type == "strategy_snapshot":
                    self._persist_strategy_snapshot(lp_id, item)
                # Add handlers for other item types as needed
                self.db.flush()  # Flush after each item to catch constraint errors early
            except Exception as e:
                logger.error(f"Error persisting {item.item_type}: {e}")
                self.db.rollback()  # Rollback failed item and continue

    def _persist_contact(self, lp_id: int, item: CollectedItem) -> None:
        """Persist a contact item."""
        data = item.data
        name = data.get("full_name")

        if not name:
            return

        # Check for existing contact
        existing = (
            self.db.query(LpKeyContact)
            .filter(
                LpKeyContact.lp_id == lp_id,
                LpKeyContact.full_name == name,
            )
            .first()
        )

        if existing:
            # Update if new data has higher confidence
            confidence_order = {"high": 3, "medium": 2, "low": 1}
            new_conf = confidence_order.get(item.confidence, 1)
            existing_conf = confidence_order.get(existing.confidence_level, 1)

            if new_conf >= existing_conf:
                for field in ["email", "phone", "title", "role_category"]:
                    new_value = data.get(field)
                    if new_value and not getattr(existing, field):
                        setattr(existing, field, new_value)
                existing.updated_at = datetime.utcnow()
            item.is_new = False
        else:
            # Create new contact
            contact = LpKeyContact(
                lp_id=lp_id,
                full_name=name,
                title=data.get("title"),
                role_category=data.get("role_category"),
                email=data.get("email"),
                phone=data.get("phone"),
                source_type=data.get("source_type", "website"),
                source_url=item.source_url,
                confidence_level=item.confidence,
                collected_date=datetime.utcnow(),
            )
            self.db.add(contact)
            item.is_new = True

        self.db.commit()

    def _persist_strategy_info(self, lp_id: int, item: CollectedItem) -> None:
        """Persist strategy information (AUM, etc.)."""
        data = item.data

        # Update LP record with AUM if present
        aum = data.get("aum_usd_billions") or data.get("aum_usd_millions")
        if aum:
            lp = self.db.query(LpFund).filter(LpFund.id == lp_id).first()
            if lp:
                # Convert millions to billions if needed
                if "millions" in str(data.get("aum_usd_millions", "")):
                    aum_billions = float(aum) / 1000
                else:
                    aum_billions = float(aum)
                lp.aum_usd_billions = str(aum_billions)
                self.db.commit()

    def _persist_governance_member(self, lp_id: int, item: CollectedItem) -> None:
        """Persist a governance member (board member, trustee, etc.)."""
        data = item.data
        name = data.get("full_name")
        role = data.get("governance_role")

        if not name or not role:
            return

        # Check for existing member
        existing = (
            self.db.query(LpGovernanceMember)
            .filter(
                LpGovernanceMember.lp_id == lp_id,
                LpGovernanceMember.full_name == name,
                LpGovernanceMember.governance_role == role,
            )
            .first()
        )

        if existing:
            # Update existing record
            for field in ["title", "representing", "committee_name"]:
                new_value = data.get(field)
                if new_value and not getattr(existing, field):
                    setattr(existing, field, new_value)
            existing.is_current = data.get("is_current", 1)
            item.is_new = False
        else:
            # Create new governance member
            member = LpGovernanceMember(
                lp_id=lp_id,
                full_name=name,
                title=data.get("title"),
                governance_role=role,
                committee_name=data.get("committee_name"),
                representing=data.get("representing"),
                is_current=data.get("is_current", 1),
                source_type=data.get("source_type", "website"),
                source_url=item.source_url,
                collected_at=datetime.utcnow(),
            )
            self.db.add(member)
            item.is_new = True

        self.db.commit()

    def _persist_board_meeting(self, lp_id: int, item: CollectedItem) -> None:
        """Persist a board meeting record."""
        data = item.data
        meeting_date_str = data.get("meeting_date")
        meeting_type = data.get("meeting_type")

        if not meeting_type:
            return

        # Parse meeting date
        meeting_date = None
        if meeting_date_str:
            try:
                meeting_date = datetime.fromisoformat(meeting_date_str)
            except ValueError:
                pass

        if not meeting_date:
            meeting_date = datetime.utcnow()

        # Check for existing meeting
        existing = (
            self.db.query(LpBoardMeeting)
            .filter(
                LpBoardMeeting.lp_id == lp_id,
                LpBoardMeeting.meeting_date == meeting_date,
                LpBoardMeeting.meeting_type == meeting_type,
            )
            .first()
        )

        if existing:
            # Update document URLs if we have new ones
            for field in ["agenda_url", "minutes_url", "materials_url", "video_url"]:
                new_value = data.get(field)
                if new_value and not getattr(existing, field):
                    setattr(existing, field, new_value)
            item.is_new = False
        else:
            # Create new meeting record
            meeting = LpBoardMeeting(
                lp_id=lp_id,
                meeting_date=meeting_date,
                meeting_type=meeting_type,
                meeting_title=data.get("meeting_title"),
                agenda_url=data.get("agenda_url"),
                minutes_url=data.get("minutes_url"),
                materials_url=data.get("materials_url"),
                video_url=data.get("video_url"),
                source_url=item.source_url,
                collected_at=datetime.utcnow(),
            )
            self.db.add(meeting)
            item.is_new = True

        self.db.commit()

    def _persist_performance_return(self, lp_id: int, item: CollectedItem) -> None:
        """Persist performance return data."""
        data = item.data
        fiscal_year = data.get("fiscal_year")

        if not fiscal_year:
            return

        # Check for existing performance record
        existing = (
            self.db.query(LpPerformanceReturn)
            .filter(
                LpPerformanceReturn.lp_id == lp_id,
                LpPerformanceReturn.fiscal_year == fiscal_year,
            )
            .first()
        )

        return_fields = [
            "one_year_return_pct",
            "three_year_return_pct",
            "five_year_return_pct",
            "ten_year_return_pct",
            "twenty_year_return_pct",
            "since_inception_return_pct",
            "benchmark_name",
            "benchmark_one_year_pct",
            "benchmark_three_year_pct",
            "benchmark_five_year_pct",
            "benchmark_ten_year_pct",
            "total_fund_value_usd",
            "net_cash_flow_usd",
        ]

        if existing:
            # Update with new return data
            for field in return_fields:
                new_value = data.get(field)
                if new_value and not getattr(existing, field):
                    setattr(existing, field, new_value)
            item.is_new = False
        else:
            # Create new performance record
            perf = LpPerformanceReturn(
                lp_id=lp_id,
                fiscal_year=fiscal_year,
                source_type=data.get("source_type", "website"),
                source_url=item.source_url,
                collected_at=datetime.utcnow(),
            )
            for field in return_fields:
                value = data.get(field)
                if value:
                    setattr(perf, field, value)
            self.db.add(perf)
            item.is_new = True

        self.db.commit()

    def _persist_13f_holding(self, lp_id: int, item: CollectedItem) -> None:
        """
        Persist 13F holding data to PortfolioCompany table.

        SEC 13F filings provide authoritative data on institutional holdings
        including CUSIP, shares held, and market values.
        """
        data = item.data
        cusip = data.get("cusip")
        issuer_name = data.get("issuer_name")
        report_date_str = data.get("report_date")

        if not cusip or not issuer_name:
            return

        # Parse report date
        report_date = None
        if report_date_str:
            try:
                report_date = datetime.fromisoformat(report_date_str)
            except ValueError:
                try:
                    report_date = datetime.strptime(report_date_str, "%Y-%m-%d")
                except ValueError:
                    report_date = datetime.utcnow()

        # Check for existing holding (same LP, cusip, and report period)
        existing = (
            self.db.query(PortfolioCompany)
            .filter(
                PortfolioCompany.investor_id == lp_id,
                PortfolioCompany.investor_type == "lp",
                PortfolioCompany.company_cusip == cusip,
                PortfolioCompany.source_type == "sec_13f",
                PortfolioCompany.investment_date == report_date,
            )
            .first()
        )

        if existing:
            # Update existing holding with newer data
            if report_date and (
                not existing.investment_date or report_date > existing.investment_date
            ):
                existing.shares_held = data.get("shares")
                existing.market_value_usd = data.get("value_usd")
                existing.investment_date = report_date
                existing.updated_at = datetime.utcnow()
            item.is_new = False
        else:
            # Create new holding
            holding = PortfolioCompany(
                investor_id=lp_id,
                investor_type="lp",
                company_name=issuer_name,
                company_cusip=cusip,
                investment_type="public_equity",
                investment_date=report_date,
                shares_held=data.get("shares"),
                market_value_usd=data.get("value_usd"),
                current_holding=1,
                source_type="sec_13f",
                source_url=item.source_url,
                confidence_level="high",  # SEC filings are authoritative
                collected_date=datetime.utcnow(),
                collection_method="sec_13f_collector",
            )
            self.db.add(holding)
            item.is_new = True

        self.db.commit()

    def _persist_990_data(self, lp_id: int, item: CollectedItem) -> None:
        """
        Persist Form 990 data to LP record.

        Updates LP record with financial data from Form 990 filings.
        """
        data = item.data

        # Update LP record with Form 990 data
        lp = self.db.query(LpFund).filter(LpFund.id == lp_id).first()
        if lp:
            # Update EIN if not already set
            ein = data.get("ein")
            if ein and not getattr(lp, "ein", None):
                # Would need to add ein column to LpFund if not present
                pass

            # Update AUM from total assets
            total_assets = data.get("total_assets")
            if total_assets:
                try:
                    aum_billions = float(total_assets) / 1_000_000_000
                    if (
                        not lp.aum_usd_billions
                        or float(lp.aum_usd_billions or 0) < aum_billions
                    ):
                        lp.aum_usd_billions = f"{aum_billions:.2f}"
                except (ValueError, TypeError):
                    pass

            self.db.commit()

        # Log item as persisted (data is mostly informational)
        item.is_new = True

    def _persist_strategy_snapshot(self, lp_id: int, item: CollectedItem) -> None:
        """
        Persist strategy snapshot data.

        Creates or updates strategy snapshot with allocation data.
        """
        data = item.data
        fiscal_year = data.get("fiscal_year")

        if not fiscal_year:
            return

        # Check for existing snapshot
        existing = (
            self.db.query(LpStrategySnapshot)
            .filter(
                LpStrategySnapshot.lp_id == lp_id,
                LpStrategySnapshot.fiscal_year == fiscal_year,
                LpStrategySnapshot.program == "total_fund",
                LpStrategySnapshot.fiscal_quarter == "Q4",  # Annual data
            )
            .first()
        )

        if existing:
            # Update existing snapshot
            if data.get("total_aum_usd"):
                existing.summary_text = f"Total assets: ${data.get('total_aum_usd')}"
            item.is_new = False
        else:
            # Create new snapshot
            snapshot = LpStrategySnapshot(
                lp_id=lp_id,
                program="total_fund",
                fiscal_year=fiscal_year,
                fiscal_quarter="Q4",
                summary_text=f"Total assets: ${data.get('total_aum_usd')}"
                if data.get("total_aum_usd")
                else None,
                created_at=datetime.utcnow(),
            )
            self.db.add(snapshot)
            item.is_new = True

        self.db.commit()

    def _determine_job_status(self, results: List[CollectionResult]) -> str:
        """Determine overall job status from results."""
        if not results:
            return "success"

        success_count = sum(1 for r in results if r.success)

        if success_count == len(results):
            return "success"
        elif success_count > 0:
            return "partial"
        else:
            return "failed"

    async def collect_single_lp(self, lp_id: int) -> List[CollectionResult]:
        """
        Convenience method to collect data for a single LP.

        Args:
            lp_id: LP fund ID

        Returns:
            List of CollectionResult
        """
        lp = self.db.query(LpFund).filter(LpFund.id == lp_id).first()
        if not lp:
            raise ValueError(f"LP not found: {lp_id}")

        return await self._collect_single_lp(lp, job_id=0)

    async def collect_stale_lps(
        self,
        max_age_days: int = 90,
        limit: Optional[int] = None,
    ) -> LpCollectionJob:
        """
        Collect data for LPs that haven't been updated recently.

        Args:
            max_age_days: Consider LPs older than this as stale
            limit: Maximum number of LPs to collect

        Returns:
            LpCollectionJob record
        """
        self.config.max_age_days = max_age_days
        self.config.mode = "incremental"

        if limit:
            # Select limited number of stale LPs
            cutoff = datetime.utcnow() - timedelta(days=max_age_days)
            lps = (
                self.db.query(LpFund)
                .filter(
                    (LpFund.last_collection_at == None)
                    | (LpFund.last_collection_at < cutoff)
                )
                .order_by(LpFund.collection_priority.asc())
                .limit(limit)
                .all()
            )

            self.config.lp_ids = [lp.id for lp in lps]

        return await self.run_collection_job()

    def get_collection_status(self, job_id: int) -> Optional[Dict[str, Any]]:
        """
        Get status of a collection job.

        Args:
            job_id: Job ID

        Returns:
            Status dictionary or None if not found
        """
        job = (
            self.db.query(LpCollectionJob).filter(LpCollectionJob.id == job_id).first()
        )

        if not job:
            return None

        return {
            "job_id": job.id,
            "status": job.status,
            "total_lps": job.total_lps,
            "completed_lps": job.completed_lps,
            "successful_lps": job.successful_lps,
            "failed_lps": job.failed_lps,
            "total_items_found": job.total_items_found,
            "total_items_inserted": job.total_items_inserted,
            "total_items_updated": job.total_items_updated,
            "started_at": job.started_at.isoformat() if job.started_at else None,
            "completed_at": job.completed_at.isoformat() if job.completed_at else None,
            "progress_pct": (
                (job.completed_lps / job.total_lps * 100) if job.total_lps > 0 else 0
            ),
        }
