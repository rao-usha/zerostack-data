"""
Fund LP Tracker Agent — agentic orchestrator for LP commitment collection.

Flow:
1. Collect from all 4 sources (pension IR, CAFR, Form D, Form 990)
2. Normalize GP names via FuzzyMatcher to canonical pe_firms names
3. Persist LpGpCommitment records
4. Rebuild LpGpRelationship summaries (re-up counts, trends)
5. Trigger conviction scoring for affected PE funds
"""
import asyncio
import logging
from datetime import datetime
from typing import Optional

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, distinct

logger = logging.getLogger(__name__)


class FundLPTrackerAgent:
    """Agentic orchestrator for LP→GP commitment collection and conviction scoring."""

    def __init__(self, db: AsyncSession):
        self.db = db

    async def run(
        self,
        sources: Optional[list] = None,
        lp_ids: Optional[list] = None,
    ) -> dict:
        """
        Run the full LP tracking pipeline.

        Args:
            sources: list of sources to collect from ('pension_ir', 'form_d', 'form_990')
                     Default: all sources
            lp_ids: specific LP IDs to collect for (default: all)

        Returns: summary of what was collected and scored
        """
        sources = sources or ["pension_ir", "form_d", "form_990"]

        summary = {
            "started_at": datetime.utcnow().isoformat(),
            "sources_run": sources,
            "records_collected": 0,
            "records_persisted": 0,
            "relationships_updated": 0,
            "errors": [],
        }

        all_records = []

        # Step 1: Collect from sources
        if "pension_ir" in sources:
            try:
                from app.sources.lp_collection.pension_ir_scraper import PensionIRScraper
                scraper = PensionIRScraper()
                records = await scraper.collect_all()
                all_records.extend(records)
                logger.info(f"Pension IR: collected {len(records)} records")
            except Exception as e:
                logger.error(f"Pension IR collection failed: {e}")
                summary["errors"].append(f"pension_ir: {str(e)}")

        if "form_990" in sources:
            try:
                from app.sources.lp_collection.form_990_pe_extractor import Form990PEExtractor
                extractor = Form990PEExtractor()
                records = await extractor.collect_all()
                all_records.extend(records)
                logger.info(f"Form 990: collected {len(records)} records")
            except Exception as e:
                logger.error(f"Form 990 collection failed: {e}")
                summary["errors"].append(f"form_990: {str(e)}")

        summary["records_collected"] = len(all_records)

        # Step 2: Resolve GP names + persist
        persisted = await self._persist_commitments(all_records, lp_ids)
        summary["records_persisted"] = persisted

        # Step 3: Rebuild relationship summaries
        updated = await self._rebuild_relationships()
        summary["relationships_updated"] = updated

        summary["completed_at"] = datetime.utcnow().isoformat()
        return summary

    async def _resolve_lp_id(self, lp_name: str) -> Optional[int]:
        """Resolve LP short name to lp_fund.id via fuzzy match."""
        from app.core.models import LpFund

        stmt = select(LpFund).where(LpFund.name.ilike(f"%{lp_name.split()[0]}%"))
        result = await self.db.execute(stmt)
        matches = result.scalars().all()
        if not matches:
            return None
        if len(matches) == 1:
            return matches[0].id
        # Multiple matches — try substring match first
        for m in matches:
            if lp_name.lower() in (m.name or "").lower():
                return m.id
        return matches[0].id

    async def _resolve_gp_firm_id(self, gp_name: str) -> Optional[int]:
        """Try to link GP name to a pe_firms record via fuzzy match."""
        try:
            from app.core.pe_models import PEFirm

            # Strip common fund suffixes to get the firm name
            clean = gp_name.strip()
            for suffix in [' Fund', ' Capital', ' Partners', ' Equity', ' Ventures']:
                clean = clean.split(suffix)[0]

            stmt = select(PEFirm).where(PEFirm.name.ilike(f"%{clean[:20]}%"))
            result = await self.db.execute(stmt)
            matches = result.scalars().all()
            return matches[0].id if matches else None
        except Exception:
            return None

    async def _persist_commitments(
        self,
        records: list,
        allowed_lp_ids: Optional[list],
    ) -> int:
        """Persist LpGpCommitment records with upsert logic."""
        from app.core.models import LpGpCommitment, LpFund

        persisted = 0
        for rec in records:
            try:
                lp_name = rec.get("lp_name", "")
                lp_id = await self._resolve_lp_id(lp_name)
                if not lp_id:
                    # Create a placeholder LP if not found
                    new_lp = LpFund(name=lp_name, lp_type="public_pension")
                    self.db.add(new_lp)
                    await self.db.flush()
                    lp_id = new_lp.id

                if allowed_lp_ids and lp_id not in allowed_lp_ids:
                    continue

                gp_name = rec.get("gp_name") or rec.get("manager_name", "")
                if not gp_name:
                    continue

                gp_firm_id = await self._resolve_gp_firm_id(gp_name)

                vintage = rec.get("fund_vintage") or rec.get("vintage_year")
                amount = rec.get("commitment_amount_usd") or rec.get("fair_value_usd")
                fund_name_val = (rec.get("fund_name") or "")[:255]

                # Check for existing record
                stmt = select(LpGpCommitment).where(
                    LpGpCommitment.lp_id == lp_id,
                    LpGpCommitment.gp_name == gp_name[:255],
                    LpGpCommitment.fund_vintage == vintage,
                    LpGpCommitment.fund_name == fund_name_val,
                )
                existing = (await self.db.execute(stmt)).scalar_one_or_none()

                if existing:
                    # Update if we have new data
                    if amount and not existing.commitment_amount_usd:
                        existing.commitment_amount_usd = amount
                    if gp_firm_id and not existing.gp_firm_id:
                        existing.gp_firm_id = gp_firm_id
                else:
                    commitment = LpGpCommitment(
                        lp_id=lp_id,
                        gp_name=gp_name[:255],
                        gp_firm_id=gp_firm_id,
                        fund_name=fund_name_val,
                        fund_vintage=vintage,
                        commitment_amount_usd=amount,
                        status=rec.get("status", "active"),
                        data_source=rec.get("data_source", "unknown"),
                        source_url=rec.get("source_url"),
                        as_of_date=datetime.utcnow(),
                    )
                    self.db.add(commitment)
                    persisted += 1

            except Exception as e:
                logger.error(f"Error persisting commitment record: {e}")

        await self.db.commit()
        return persisted

    async def _rebuild_relationships(self) -> int:
        """Rebuild LpGpRelationship summaries from LpGpCommitment records."""
        from app.core.models import LpGpCommitment, LpGpRelationship

        # Get all unique (lp_id, gp_name) pairs with aggregates
        stmt = (
            select(
                LpGpCommitment.lp_id,
                LpGpCommitment.gp_name,
                LpGpCommitment.gp_firm_id,
                func.min(LpGpCommitment.fund_vintage).label("first_vintage"),
                func.max(LpGpCommitment.fund_vintage).label("last_vintage"),
                func.count(distinct(LpGpCommitment.fund_vintage)).label("vintage_count"),
                func.sum(LpGpCommitment.commitment_amount_usd).label("total_committed"),
                func.avg(LpGpCommitment.commitment_amount_usd).label("avg_committed"),
            )
            .group_by(
                LpGpCommitment.lp_id,
                LpGpCommitment.gp_name,
                LpGpCommitment.gp_firm_id,
            )
        )
        result = await self.db.execute(stmt)
        rows = result.all()

        updated = 0
        for row in rows:
            # Determine commitment trend
            trend = "new"
            if row.vintage_count >= 3:
                trend = "growing"
            elif row.vintage_count == 2:
                trend = "stable"

            # Upsert relationship
            existing_stmt = select(LpGpRelationship).where(
                LpGpRelationship.lp_id == row.lp_id,
                LpGpRelationship.gp_name == row.gp_name,
            )
            existing = (await self.db.execute(existing_stmt)).scalar_one_or_none()

            if existing:
                existing.first_vintage = row.first_vintage
                existing.last_vintage = row.last_vintage
                existing.total_vintages_committed = row.vintage_count
                existing.total_committed_usd = row.total_committed
                existing.avg_commitment_usd = row.avg_committed
                existing.commitment_trend = trend
                existing.last_updated = datetime.utcnow()
            else:
                rel = LpGpRelationship(
                    lp_id=row.lp_id,
                    gp_name=row.gp_name,
                    gp_firm_id=row.gp_firm_id,
                    first_vintage=row.first_vintage,
                    last_vintage=row.last_vintage,
                    total_vintages_committed=row.vintage_count,
                    total_committed_usd=row.total_committed,
                    avg_commitment_usd=row.avg_committed,
                    commitment_trend=trend,
                )
                self.db.add(rel)
            updated += 1

        await self.db.commit()
        return updated
