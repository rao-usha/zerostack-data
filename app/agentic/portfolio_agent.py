"""
Portfolio Research Agent - Main orchestrator for agentic portfolio discovery.

This agent:
1. Analyzes investor context to plan which strategies to try
2. Executes strategies in priority order
3. Synthesizes findings from multiple sources
4. Decides when to continue or stop based on coverage
5. Logs full reasoning trail for debugging
"""

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Type

from sqlalchemy.orm import Session

from app.agentic.strategies.base import BaseStrategy, InvestorContext, StrategyResult
from app.agentic.strategies.sec_13f_strategy import SEC13FStrategy
from app.agentic.strategies.website_strategy import WebsiteStrategy
from app.agentic.strategies.annual_report_strategy import AnnualReportStrategy
from app.agentic.strategies.news_strategy import NewsStrategy
from app.agentic.strategies.reverse_search_strategy import ReverseSearchStrategy
from app.agentic.synthesizer import DataSynthesizer

logger = logging.getLogger(__name__)


# All available strategies (5 strategies for Phase 3)
AVAILABLE_STRATEGIES: List[Type[BaseStrategy]] = [
    SEC13FStrategy,  # Strategy 1: SEC 13F filings (HIGH confidence)
    WebsiteStrategy,  # Strategy 2: Website portfolio scraping (MEDIUM confidence)
    AnnualReportStrategy,  # Strategy 3: Annual report PDF parsing (HIGH confidence)
    NewsStrategy,  # Strategy 4: News search with LLM extraction (MEDIUM confidence)
    ReverseSearchStrategy,  # Strategy 5: Reverse search for company mentions (HIGH confidence)
]


class PortfolioResearchAgent:
    """
    Agentic orchestrator for portfolio research.

    The agent:
    1. PLAN: Analyzes investor → decides which strategies to try
    2. EXECUTE: Runs strategies in priority order
    3. SYNTHESIZE: Deduplicates and merges findings
    4. VALIDATE: Scores confidence based on source quality
    5. LOG: Records full reasoning trail for debugging
    """

    # Default configuration
    DEFAULT_MAX_STRATEGIES = 5
    DEFAULT_MAX_TIME_SECONDS = 600  # 10 minutes
    DEFAULT_MIN_COMPANIES_TARGET = 5
    DEFAULT_MIN_SOURCES = 2  # For validation

    def __init__(
        self,
        db: Session,
        max_strategies: int = DEFAULT_MAX_STRATEGIES,
        max_time_seconds: int = DEFAULT_MAX_TIME_SECONDS,
        min_companies_target: int = DEFAULT_MIN_COMPANIES_TARGET,
        min_sources: int = DEFAULT_MIN_SOURCES,
    ):
        """
        Initialize the portfolio research agent.

        Args:
            db: Database session for storing results
            max_strategies: Maximum number of strategies to try
            max_time_seconds: Maximum execution time
            min_companies_target: Target number of companies to find
            min_sources: Minimum sources for validation
        """
        self.db = db
        self.max_strategies = max_strategies
        self.max_time_seconds = max_time_seconds
        self.min_companies_target = min_companies_target
        self.min_sources = min_sources

        # Initialize synthesizer
        self.synthesizer = DataSynthesizer()

        # Initialize strategies
        self.strategies: List[BaseStrategy] = [
            strategy_class() for strategy_class in AVAILABLE_STRATEGIES
        ]

        logger.info(
            f"Initialized PortfolioResearchAgent with {len(self.strategies)} strategies: "
            f"{[s.name for s in self.strategies]}"
        )

    async def collect_portfolio(
        self,
        context: InvestorContext,
        strategies_to_use: Optional[List[str]] = None,
        job_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Execute portfolio collection for an investor.

        Args:
            context: Investor context with metadata
            strategies_to_use: Optional list of strategy names to use (if None, agent decides)
            job_id: Optional job ID for tracking

        Returns:
            Collection results with companies, reasoning, and metrics
        """
        started_at = datetime.utcnow()

        # Initialize tracking
        all_findings: List[Dict[str, Any]] = []
        strategy_results: List[StrategyResult] = []
        reasoning_log: List[Dict[str, Any]] = []
        errors: List[Dict[str, Any]] = []
        warnings: List[str] = []

        total_requests = 0
        total_tokens = 0

        logger.info(
            f"Starting portfolio collection for {context.investor_name} (id={context.investor_id})"
        )

        try:
            # PHASE 1: PLAN - Decide which strategies to use
            if strategies_to_use:
                # User specified strategies
                planned_strategies = [
                    s for s in self.strategies if s.name in strategies_to_use
                ]
                reasoning_log.append(
                    {
                        "phase": "plan",
                        "decision": "user_specified",
                        "strategies": strategies_to_use,
                        "reasoning": "User specified which strategies to use",
                    }
                )
            else:
                # Agent plans strategies
                planned_strategies = self._plan_strategies(context)
                reasoning_log.append(
                    {
                        "phase": "plan",
                        "decision": "agent_planned",
                        "strategies": [
                            {
                                "name": s["strategy"].name,
                                "priority": s["priority"],
                                "reasoning": s["reasoning"],
                            }
                            for s in planned_strategies
                        ],
                        "reasoning": f"Agent planned {len(planned_strategies)} strategies based on investor context",
                    }
                )

                # Convert to list of strategy instances
                planned_strategies = [s["strategy"] for s in planned_strategies]

            if not planned_strategies:
                warnings.append("No applicable strategies found for this investor")
                return self._create_result(
                    context=context,
                    started_at=started_at,
                    all_findings=[],
                    strategy_results=[],
                    reasoning_log=reasoning_log,
                    errors=errors,
                    warnings=warnings,
                    total_requests=0,
                    total_tokens=0,
                )

            # PHASE 2: EXECUTE - Run strategies
            strategies_tried = 0

            for strategy in planned_strategies[: self.max_strategies]:
                # Check time limit
                elapsed = (datetime.utcnow() - started_at).total_seconds()
                if elapsed > self.max_time_seconds:
                    warnings.append(f"Time limit reached after {elapsed:.0f}s")
                    reasoning_log.append(
                        {
                            "phase": "execute",
                            "decision": "stop",
                            "reasoning": f"Time limit ({self.max_time_seconds}s) reached",
                        }
                    )
                    break

                # Check if we should continue
                should_continue, continue_reason = self._should_continue(
                    all_findings, strategies_tried, elapsed
                )

                if not should_continue and strategies_tried > 0:
                    reasoning_log.append(
                        {
                            "phase": "execute",
                            "decision": "stop",
                            "reasoning": continue_reason,
                        }
                    )
                    break

                # Execute strategy
                logger.info(f"Executing strategy: {strategy.name}")
                reasoning_log.append(
                    {
                        "phase": "execute",
                        "strategy": strategy.name,
                        "decision": "execute",
                        "reasoning": f"Executing {strategy.display_name}",
                    }
                )

                try:
                    result = await strategy.execute(context)
                    strategy_results.append(result)
                    strategies_tried += 1

                    total_requests += result.requests_made
                    total_tokens += result.tokens_used

                    if result.success:
                        all_findings.extend(result.companies_found)
                        logger.info(
                            f"Strategy {strategy.name} found {len(result.companies_found)} companies"
                        )
                        reasoning_log.append(
                            {
                                "phase": "execute",
                                "strategy": strategy.name,
                                "result": "success",
                                "companies_found": len(result.companies_found),
                                "reasoning": result.reasoning,
                            }
                        )
                    else:
                        logger.warning(
                            f"Strategy {strategy.name} failed: {result.error_message}"
                        )
                        errors.append(
                            {
                                "strategy": strategy.name,
                                "error": result.error_message,
                                "reasoning": result.reasoning,
                            }
                        )
                        reasoning_log.append(
                            {
                                "phase": "execute",
                                "strategy": strategy.name,
                                "result": "failed",
                                "error": result.error_message,
                                "reasoning": result.reasoning,
                            }
                        )

                except Exception as e:
                    logger.error(
                        f"Error executing strategy {strategy.name}: {e}", exc_info=True
                    )
                    errors.append({"strategy": strategy.name, "error": str(e)})
                    strategies_tried += 1

            # PHASE 3: SYNTHESIZE - Merge and deduplicate
            synthesized_companies = self.synthesizer.synthesize_findings(
                all_findings, context.investor_id, context.investor_type
            )

            reasoning_log.append(
                {
                    "phase": "synthesize",
                    "raw_findings": len(all_findings),
                    "unique_companies": len(synthesized_companies),
                    "reasoning": f"Deduplicated {len(all_findings)} findings to {len(synthesized_companies)} unique companies",
                }
            )

            # Extract co-investors
            co_investors = self.synthesizer.extract_co_investors(
                all_findings, context.investor_id, context.investor_type
            )

            # Classify themes
            themes = self.synthesizer.classify_investment_themes(
                synthesized_companies, context.investor_id, context.investor_type
            )

            # PHASE 4: STORE - Save results to database
            store_result = await self._store_results(
                context=context,
                companies=synthesized_companies,
                co_investors=co_investors,
                themes=themes,
                job_id=job_id,
            )

            reasoning_log.append(
                {
                    "phase": "store",
                    "new_companies": store_result["new_companies"],
                    "updated_companies": store_result["updated_companies"],
                    "co_investors": store_result["co_investors_stored"],
                    "themes": store_result["themes_stored"],
                    "reasoning": "Stored results to database",
                }
            )

            return self._create_result(
                context=context,
                started_at=started_at,
                all_findings=synthesized_companies,
                strategy_results=strategy_results,
                reasoning_log=reasoning_log,
                errors=errors,
                warnings=warnings,
                total_requests=total_requests,
                total_tokens=total_tokens,
                store_result=store_result,
            )

        except Exception as e:
            logger.error(f"Error in portfolio collection: {e}", exc_info=True)
            errors.append({"error": str(e), "phase": "main"})
            return self._create_result(
                context=context,
                started_at=started_at,
                all_findings=[],
                strategy_results=strategy_results,
                reasoning_log=reasoning_log,
                errors=errors,
                warnings=warnings,
                total_requests=total_requests,
                total_tokens=total_tokens,
            )

    def _plan_strategies(self, context: InvestorContext) -> List[Dict[str, Any]]:
        """
        Plan which strategies to use based on investor context.

        Returns list of strategies with priority and reasoning.
        """
        planned = []

        for strategy in self.strategies:
            applicable, reasoning = strategy.is_applicable(context)

            if applicable:
                priority = strategy.calculate_priority(context)
                planned.append(
                    {"strategy": strategy, "priority": priority, "reasoning": reasoning}
                )

        # Sort by priority (descending)
        planned.sort(key=lambda x: x["priority"], reverse=True)

        return planned

    def _should_continue(
        self,
        current_results: List[Dict[str, Any]],
        strategies_tried: int,
        time_elapsed: float,
    ) -> tuple[bool, str]:
        """
        Decide whether to try more strategies or stop.

        Args:
            current_results: Companies found so far
            strategies_tried: Number of strategies already tried
            time_elapsed: Seconds elapsed since start

        Returns:
            Tuple of (should_continue, reasoning)
        """
        # Good coverage achieved
        if len(current_results) >= self.min_companies_target:
            unique_sources = set(r.get("source_type") for r in current_results)
            if len(unique_sources) >= self.min_sources:
                return (
                    False,
                    f"Target coverage achieved: {len(current_results)} companies from {len(unique_sources)} sources",
                )

        # No results after trying all strategies
        if strategies_tried >= self.max_strategies and len(current_results) == 0:
            return False, f"No data found after {strategies_tried} strategies"

        # Time limit
        if time_elapsed > self.max_time_seconds:
            return False, f"Time limit ({self.max_time_seconds}s) reached"

        # Poor coverage, keep trying
        if len(current_results) < self.min_companies_target:
            return (
                True,
                f"Coverage below target ({len(current_results)} < {self.min_companies_target}), continuing",
            )

        # Single source only, need validation
        unique_sources = set(r.get("source_type") for r in current_results)
        if len(unique_sources) == 1:
            return (
                True,
                "Single source detected, seeking validation from additional source",
            )

        return False, "Default stop condition"

    async def _store_results(
        self,
        context: InvestorContext,
        companies: List[Dict[str, Any]],
        co_investors: List[Dict[str, Any]],
        themes: List[Dict[str, Any]],
        job_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Store results to database.

        Returns counts of stored records.
        """
        new_companies = 0
        updated_companies = 0
        co_investors_stored = 0
        themes_stored = 0

        try:
            # Store portfolio companies
            for company_data in companies:
                # Check if exists
                from sqlalchemy import text

                existing = self.db.execute(
                    text("""
                        SELECT id FROM portfolio_companies 
                        WHERE investor_id = :investor_id 
                        AND investor_type = :investor_type 
                        AND company_name = :company_name
                        LIMIT 1
                    """),
                    {
                        "investor_id": context.investor_id,
                        "investor_type": context.investor_type,
                        "company_name": company_data.get("company_name"),
                    },
                ).fetchone()

                if existing:
                    # Update existing record
                    self.db.execute(
                        text("""
                            UPDATE portfolio_companies 
                            SET 
                                company_website = COALESCE(:company_website, company_website),
                                company_industry = COALESCE(:company_industry, company_industry),
                                investment_amount_usd = COALESCE(:investment_amount_usd, investment_amount_usd),
                                market_value_usd = COALESCE(:market_value_usd, market_value_usd),
                                shares_held = COALESCE(:shares_held, shares_held),
                                last_verified_date = :last_verified_date,
                                updated_at = NOW()
                            WHERE id = :id
                        """),
                        {
                            "id": existing[0],
                            "company_website": company_data.get("company_website"),
                            "company_industry": company_data.get("company_industry"),
                            "investment_amount_usd": company_data.get(
                                "investment_amount_usd"
                            ),
                            "market_value_usd": company_data.get("market_value_usd"),
                            "shares_held": company_data.get("shares_held"),
                            "last_verified_date": datetime.utcnow(),
                        },
                    )
                    updated_companies += 1
                else:
                    # Insert new record
                    self.db.execute(
                        text("""
                            INSERT INTO portfolio_companies (
                                investor_id, investor_type, company_name, company_website,
                                company_industry, company_stage, company_location,
                                company_ticker, company_cusip, investment_type,
                                investment_date, investment_amount_usd, shares_held,
                                market_value_usd, ownership_percentage, current_holding,
                                source_type, source_url, confidence_level,
                                collection_method, agent_reasoning, collection_job_id,
                                collected_date, created_at
                            ) VALUES (
                                :investor_id, :investor_type, :company_name, :company_website,
                                :company_industry, :company_stage, :company_location,
                                :company_ticker, :company_cusip, :investment_type,
                                :investment_date, :investment_amount_usd, :shares_held,
                                :market_value_usd, :ownership_percentage, :current_holding,
                                :source_type, :source_url, :confidence_level,
                                :collection_method, :agent_reasoning, :collection_job_id,
                                CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                            )
                        """),
                        {
                            "investor_id": context.investor_id,
                            "investor_type": context.investor_type,
                            "company_name": company_data.get("company_name"),
                            "company_website": company_data.get("company_website"),
                            "company_industry": company_data.get("company_industry"),
                            "company_stage": company_data.get("company_stage"),
                            "company_location": company_data.get("company_location"),
                            "company_ticker": company_data.get("company_ticker"),
                            "company_cusip": company_data.get("company_cusip"),
                            "investment_type": company_data.get("investment_type"),
                            "investment_date": company_data.get("investment_date"),
                            "investment_amount_usd": company_data.get(
                                "investment_amount_usd"
                            ),
                            "shares_held": company_data.get("shares_held"),
                            "market_value_usd": company_data.get("market_value_usd"),
                            "ownership_percentage": company_data.get(
                                "ownership_percentage"
                            ),
                            "current_holding": company_data.get("current_holding", 1),
                            "source_type": company_data.get("source_type", "unknown"),
                            "source_url": company_data.get("source_url"),
                            "confidence_level": company_data.get(
                                "confidence_level", "medium"
                            ),
                            "collection_method": "agentic_search",
                            "agent_reasoning": company_data.get("agent_reasoning"),
                            "collection_job_id": job_id,
                        },
                    )
                    new_companies += 1

            # Store co-investors
            for co_inv_data in co_investors:
                try:
                    self.db.execute(
                        text("""
                            INSERT INTO co_investments (
                                primary_investor_id, primary_investor_type,
                                co_investor_name, co_investor_type,
                                deal_name, deal_date, deal_size_usd,
                                co_investment_count, source_type, source_url
                            ) VALUES (
                                :primary_investor_id, :primary_investor_type,
                                :co_investor_name, :co_investor_type,
                                :deal_name, :deal_date, :deal_size_usd,
                                :co_investment_count, :source_type, :source_url
                            )
                            ON CONFLICT (primary_investor_id, primary_investor_type, co_investor_name, deal_name) 
                            DO UPDATE SET 
                                co_investment_count = co_investments.co_investment_count + 1
                        """),
                        co_inv_data,
                    )
                    co_investors_stored += 1
                except Exception as e:
                    logger.warning(f"Error storing co-investor: {e}")

            # Store themes
            for theme_data in themes:
                try:
                    self.db.execute(
                        text("""
                            INSERT INTO investor_themes (
                                investor_id, investor_type,
                                theme_category, theme_value,
                                investment_count, percentage_of_portfolio,
                                confidence_level
                            ) VALUES (
                                :investor_id, :investor_type,
                                :theme_category, :theme_value,
                                :investment_count, :percentage_of_portfolio,
                                :confidence_level
                            )
                            ON CONFLICT (investor_id, investor_type, theme_category, theme_value) 
                            DO UPDATE SET 
                                investment_count = :investment_count,
                                percentage_of_portfolio = :percentage_of_portfolio
                        """),
                        theme_data,
                    )
                    themes_stored += 1
                except Exception as e:
                    logger.warning(f"Error storing theme: {e}")

            self.db.commit()

        except Exception as e:
            logger.error(f"Error storing results: {e}", exc_info=True)
            self.db.rollback()
            raise

        return {
            "new_companies": new_companies,
            "updated_companies": updated_companies,
            "co_investors_stored": co_investors_stored,
            "themes_stored": themes_stored,
        }

    def _create_result(
        self,
        context: InvestorContext,
        started_at: datetime,
        all_findings: List[Dict[str, Any]],
        strategy_results: List[StrategyResult],
        reasoning_log: List[Dict[str, Any]],
        errors: List[Dict[str, Any]],
        warnings: List[str],
        total_requests: int,
        total_tokens: int,
        store_result: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Create standardized result dictionary."""
        completed_at = datetime.utcnow()
        duration = (completed_at - started_at).total_seconds()

        # Determine status
        if errors and not all_findings:
            status = "failed"
        elif errors and all_findings:
            status = "partial_success"
        elif all_findings:
            status = "success"
        else:
            status = "success"  # No findings but no errors

        successful_strategies = [r.strategy_name for r in strategy_results if r.success]

        return {
            "status": status,
            "investor_id": context.investor_id,
            "investor_type": context.investor_type,
            "investor_name": context.investor_name,
            "companies_found": len(all_findings),
            "new_companies": store_result["new_companies"] if store_result else 0,
            "updated_companies": store_result["updated_companies"]
            if store_result
            else 0,
            "strategies_used": [r.strategy_name for r in strategy_results],
            "strategies_successful": successful_strategies,
            "sources_checked": len(strategy_results),
            "sources_successful": len(successful_strategies),
            "reasoning_log": reasoning_log,
            "errors": errors,
            "warnings": warnings,
            "requests_made": total_requests,
            "tokens_used": total_tokens,
            "duration_seconds": duration,
            "started_at": started_at.isoformat(),
            "completed_at": completed_at.isoformat(),
            "companies": all_findings[:20],  # Return first 20 for preview
        }


# Convenience function for quick portfolio lookup
async def quick_portfolio_lookup(
    db: Session,
    investor_id: int,
    investor_type: str,
    investor_name: str,
    website_url: Optional[str] = None,
    aum_usd: Optional[float] = None,
    lp_type: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Quick portfolio lookup for a single investor.

    Args:
        db: Database session
        investor_id: Investor ID
        investor_type: 'lp' or 'family_office'
        investor_name: Investor name
        website_url: Optional website URL
        aum_usd: Optional AUM in USD
        lp_type: Optional LP type

    Returns:
        Collection results
    """
    context = InvestorContext(
        investor_id=investor_id,
        investor_type=investor_type,
        investor_name=investor_name,
        website_url=website_url,
        aum_usd=aum_usd,
        lp_type=lp_type,
    )

    agent = PortfolioResearchAgent(db)
    return await agent.collect_portfolio(context)
