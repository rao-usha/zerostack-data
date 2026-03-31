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

from sqlalchemy.orm import Session
from sqlalchemy import select, func, distinct

logger = logging.getLogger(__name__)


class FundLPTrackerAgent:
    """Agentic orchestrator for LP→GP commitment collection and conviction scoring."""

    def __init__(self, db: Session):
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
        sources = sources or ["cafr", "form_990", "form_d"]

        summary = {
            "started_at": datetime.utcnow().isoformat(),
            "sources_run": sources,
            "records_collected": 0,
            "records_persisted": 0,
            "relationships_updated": 0,
            "funds_scored": 0,
            "errors": [],
        }

        all_records = []

        # Step 1: Collect from sources
        if "cafr" in sources:
            try:
                from app.sources.lp_collection.pension_cafr_collector import PensionCafrCollector
                collector = PensionCafrCollector()
                records = await collector.collect_all()
                all_records.extend(records)
                logger.info(f"CAFR: collected {len(records)} records")
            except Exception as e:
                logger.error(f"CAFR collection failed: {e}")
                summary["errors"].append(f"cafr: {str(e)}")

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

        if "public_seed" in sources:
            try:
                from app.sources.lp_collection.lp_public_seed import get_seed_records
                records = get_seed_records()
                all_records.extend(records)
                logger.info(f"Public seed: collected {len(records)} records")
            except Exception as e:
                logger.error(f"Public seed failed: {e}")
                summary["errors"].append(f"public_seed: {str(e)}")

        if "html_portal" in sources:
            try:
                from app.sources.lp_collection.pension_html_scraper import PensionHtmlScraper
                scraper = PensionHtmlScraper()
                records = await scraper.collect_all()
                all_records.extend(records)
                logger.info(f"HTML portal: collected {len(records)} records")
            except Exception as e:
                logger.error(f"HTML portal scraper failed: {e}")
                summary["errors"].append(f"html_portal: {str(e)}")

        summary["records_collected"] = len(all_records)

        # Step 2: Resolve GP names + persist (sync)
        persisted = self._persist_commitments(all_records, lp_ids)
        summary["records_persisted"] = persisted

        # Step 3: Rebuild relationship summaries (sync)
        updated = self._rebuild_relationships()
        summary["relationships_updated"] = updated

        # Step 4: Score affected PE funds (sync)
        if all_records:
            gp_names = list({
                rec.get("gp_name") or rec.get("manager_name", "")
                for rec in all_records
                if rec.get("gp_name") or rec.get("manager_name")
            })
            scored = self._score_affected_funds(gp_names)
            summary["funds_scored"] = scored

        summary["completed_at"] = datetime.utcnow().isoformat()
        return summary

    def _resolve_lp_id(self, lp_name: str) -> Optional[int]:
        """Resolve LP short name to lp_fund.id via fuzzy match."""
        from app.core.models import LpFund

        stmt = select(LpFund).where(LpFund.name.ilike(f"%{lp_name.split()[0]}%"))
        matches = self.db.execute(stmt).scalars().all()
        if not matches:
            return None
        if len(matches) == 1:
            return matches[0].id
        # Multiple matches — try substring match first
        for m in matches:
            if lp_name.lower() in (m.name or "").lower():
                return m.id
        return matches[0].id

    def _resolve_gp_firm_id(self, gp_name: str) -> Optional[int]:
        """Try to link GP name to a pe_firms record via fuzzy match."""
        try:
            from app.core.pe_models import PEFirm

            # Strip common fund suffixes to get the firm name
            clean = gp_name.strip()
            for suffix in [' Fund', ' Capital', ' Partners', ' Equity', ' Ventures']:
                clean = clean.split(suffix)[0]

            stmt = select(PEFirm).where(PEFirm.name.ilike(f"%{clean[:20]}%"))
            matches = self.db.execute(stmt).scalars().all()
            return matches[0].id if matches else None
        except Exception:
            return None

    def _persist_commitments(
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
                lp_id = self._resolve_lp_id(lp_name)
                if not lp_id:
                    # Create a placeholder LP if not found
                    new_lp = LpFund(name=lp_name, lp_type="public_pension")
                    self.db.add(new_lp)
                    self.db.flush()
                    lp_id = new_lp.id

                if allowed_lp_ids and lp_id not in allowed_lp_ids:
                    continue

                gp_name = rec.get("gp_name") or rec.get("manager_name", "")
                if not gp_name:
                    continue

                gp_firm_id = self._resolve_gp_firm_id(gp_name)

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
                existing = self.db.execute(stmt).scalar_one_or_none()

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

        self.db.commit()
        return persisted

    def _rebuild_relationships(self) -> int:
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
        rows = self.db.execute(stmt).all()

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
            existing = self.db.execute(existing_stmt).scalar_one_or_none()

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

        self.db.commit()
        return updated

    def _score_affected_funds(self, gp_names: list) -> int:
        """Score all PE funds whose GP has new LP relationship data."""
        from app.core.pe_models import PEFirm, PEFund, PEFundConvictionScore
        from app.core.models import LpGpRelationship
        from app.services.pe_fund_conviction_scorer import FundConvictionScorer
        from sqlalchemy import select, or_, func, distinct

        scored = 0
        scorer = FundConvictionScorer()

        for gp_name in gp_names[:20]:  # cap at 20 GPs per run
            try:
                # Find matching PE firm
                clean_name = gp_name.strip()
                for suffix in [' Fund', ' Capital', ' Partners', ' Equity', ' Ventures']:
                    clean_name = clean_name.split(suffix)[0]

                firm_stmt = select(PEFirm).where(PEFirm.name.ilike(f"%{clean_name[:20]}%"))
                firm = self.db.execute(firm_stmt).scalars().first()
                if not firm:
                    continue

                # Find their PE funds
                fund_stmt = select(PEFund).where(PEFund.firm_id == firm.id).limit(5)
                funds = self.db.execute(fund_stmt).scalars().all()

                # Get LP relationship data for this GP
                lp_stmt = select(LpGpRelationship).where(
                    LpGpRelationship.gp_firm_id == firm.id
                )
                lp_rels = self.db.execute(lp_stmt).scalars().all()

                if not lp_rels:
                    continue

                total_lps = len(lp_rels)
                repeat_lps = sum(1 for r in lp_rels if r.total_vintages_committed > 1)
                reup_rate = repeat_lps / total_lps if total_lps > 0 else None

                for fund in funds:
                    try:
                        target_usd = (
                            float(fund.target_size_usd_millions) * 1e6
                            if fund.target_size_usd_millions else None
                        )
                        final_usd = (
                            float(fund.final_close_usd_millions) * 1e6
                            if fund.final_close_usd_millions else None
                        )

                        result = scorer.score_from_data(
                            fund_id=fund.id,
                            lp_count=total_lps,
                            repeat_lp_count=repeat_lps,
                            reup_rate_pct=reup_rate,
                            target_size_usd=target_usd,
                            final_close_usd=final_usd,
                            first_close_date=fund.first_close_date,
                            final_close_date=fund.final_close_date,
                        )

                        score_record = PEFundConvictionScore(
                            fund_id=fund.id,
                            conviction_score=result.conviction_score,
                            conviction_grade=result.conviction_grade,
                            lp_quality_score=result.sub_scores.get("lp_quality"),
                            reup_rate_score=result.sub_scores.get("reup_rate"),
                            oversubscription_score=result.sub_scores.get("oversubscription"),
                            lp_diversity_score=result.sub_scores.get("lp_diversity"),
                            time_to_close_score=result.sub_scores.get("time_to_close"),
                            gp_commitment_score=result.sub_scores.get("gp_commitment"),
                            lp_count=result.lp_count,
                            repeat_lp_count=result.repeat_lp_count,
                            tier1_lp_count=result.tier1_lp_count,
                            oversubscription_ratio=result.oversubscription_ratio,
                            days_to_final_close=result.days_to_final_close,
                            reup_rate_pct=result.reup_rate_pct,
                            data_completeness=result.data_completeness,
                            scoring_notes=result.scoring_notes,
                            scored_at=datetime.utcnow(),
                        )
                        self.db.add(score_record)
                        scored += 1
                    except Exception as e:
                        logger.error(f"Error scoring fund {fund.id}: {e}")

            except Exception as e:
                logger.error(f"Error processing GP {gp_name}: {e}")

        if scored > 0:
            self.db.commit()

        return scored
