"""
Agentic Company Researcher.

Autonomous AI agent that researches any company across all data sources,
synthesizes findings into a structured profile, and identifies data gaps.

ENHANCED: Now actively collects data from external APIs, not just querying local DB.
When you request research on "Stripe", the agent will:
1. Call GitHub API to fetch organization data
2. Query Tranco for web traffic rankings
3. Look up SEC filings
4. Query all other available sources
5. Store all collected data persistently
6. Synthesize into a comprehensive profile
"""

import asyncio
import logging
import json
import re
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from enum import Enum
from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


# Company name to GitHub org/domain mappings for well-known companies
COMPANY_MAPPINGS = {
    "stripe": {"github": "stripe", "domain": "stripe.com"},
    "openai": {"github": "openai", "domain": "openai.com"},
    "anthropic": {"github": "anthropics", "domain": "anthropic.com"},
    "databricks": {"github": "databricks", "domain": "databricks.com"},
    "figma": {"github": "figma", "domain": "figma.com"},
    "notion": {"github": "makenotion", "domain": "notion.so"},
    "discord": {"github": "discord", "domain": "discord.com"},
    "canva": {"github": "canva", "domain": "canva.com"},
    "plaid": {"github": "plaid", "domain": "plaid.com"},
    "ramp": {"github": "ramp-development", "domain": "ramp.com"},
    "brex": {"github": "brexhq", "domain": "brex.com"},
    "vercel": {"github": "vercel", "domain": "vercel.com"},
    "supabase": {"github": "supabase", "domain": "supabase.com"},
    "linear": {"github": "linear", "domain": "linear.app"},
    "retool": {"github": "tryretool", "domain": "retool.com"},
    "hashicorp": {"github": "hashicorp", "domain": "hashicorp.com"},
    "datadog": {"github": "DataDog", "domain": "datadoghq.com"},
    "snowflake": {"github": "snowflakedb", "domain": "snowflake.com"},
    "mongodb": {"github": "mongodb", "domain": "mongodb.com"},
    "elastic": {"github": "elastic", "domain": "elastic.co"},
}


class ResearchStatus(str, Enum):
    """Research job status."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    PARTIAL = "partial"  # Some sources failed but have results


class DataSource(str, Enum):
    """Available data sources."""
    ENRICHMENT = "enrichment"
    GITHUB = "github"
    GLASSDOOR = "glassdoor"
    APP_STORE = "app_store"
    WEB_TRAFFIC = "web_traffic"
    NEWS = "news"
    SEC_FILINGS = "sec_filings"
    CORPORATE_REGISTRY = "corporate_registry"
    SCORING = "scoring"
    WEB_SCRAPE = "web_scrape"  # Scrape company website for info


class CompanyResearchAgent:
    """
    Autonomous company research agent.

    Coordinates data gathering from multiple sources, synthesizes findings,
    and produces comprehensive company profiles.
    """

    # Source weights for confidence calculation
    SOURCE_WEIGHTS = {
        DataSource.ENRICHMENT: 0.15,
        DataSource.GITHUB: 0.10,
        DataSource.GLASSDOOR: 0.12,
        DataSource.APP_STORE: 0.08,
        DataSource.WEB_TRAFFIC: 0.10,
        DataSource.NEWS: 0.10,
        DataSource.SEC_FILINGS: 0.10,
        DataSource.CORPORATE_REGISTRY: 0.05,
        DataSource.SCORING: 0.10,
        DataSource.WEB_SCRAPE: 0.10,  # Web scraping for company info
    }

    def __init__(self, db: Session):
        self.db = db
        self._ensure_tables()

    def _ensure_tables(self) -> None:
        """Ensure research job tables exist."""
        create_jobs = text("""
            CREATE TABLE IF NOT EXISTS research_jobs (
                id SERIAL PRIMARY KEY,
                job_id VARCHAR(50) UNIQUE NOT NULL,
                company_input VARCHAR(255) NOT NULL,
                company_name VARCHAR(255),
                status VARCHAR(20) DEFAULT 'pending',
                sources_requested JSONB,
                sources_completed JSONB DEFAULT '[]',
                sources_failed JSONB DEFAULT '[]',
                progress FLOAT DEFAULT 0,
                results JSONB,
                error_message TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                started_at TIMESTAMP,
                completed_at TIMESTAMP
            )
        """)

        create_cache = text("""
            CREATE TABLE IF NOT EXISTS research_cache (
                id SERIAL PRIMARY KEY,
                company_name VARCHAR(255) NOT NULL,
                profile JSONB NOT NULL,
                sources_used JSONB,
                confidence_score FLOAT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                expires_at TIMESTAMP,
                UNIQUE(company_name)
            )
        """)

        create_index = text("""
            CREATE INDEX IF NOT EXISTS idx_research_jobs_status
            ON research_jobs(status)
        """)

        try:
            self.db.execute(create_jobs)
            self.db.execute(create_cache)
            self.db.execute(create_index)
            self.db.commit()
        except Exception as e:
            logger.warning(f"Table creation warning: {e}")
            self.db.rollback()

    def _generate_job_id(self) -> str:
        """Generate unique job ID."""
        import uuid
        return f"research_{uuid.uuid4().hex[:12]}"

    def _normalize_company_name(self, input_str: str) -> str:
        """Extract company name from input (name or domain)."""
        # If it looks like a domain, extract company name
        if "." in input_str and " " not in input_str:
            # Remove common TLDs and www
            name = input_str.lower()
            name = re.sub(r'^(www\.)', '', name)
            name = re.sub(r'\.(com|io|co|org|net|ai|dev|app)$', '', name)
            # Capitalize
            return name.title()
        return input_str.strip()

    def _extract_domain(self, input_str: str) -> Optional[str]:
        """Extract domain from input if present."""
        if "." in input_str and " " not in input_str:
            domain = input_str.lower()
            if not domain.startswith(("http://", "https://")):
                domain = domain
            domain = re.sub(r'^(https?://)?(www\.)?', '', domain)
            domain = domain.split("/")[0]
            return domain
        return None

    def start_research(
        self,
        company_name: str,
        domain: Optional[str] = None,
        ticker: Optional[str] = None,
        priority_sources: Optional[List[str]] = None
    ) -> str:
        """
        Start a company research job.

        Args:
            company_name: Company name to research
            domain: Company domain for enrichment
            ticker: Stock ticker if public
            priority_sources: Specific sources to query (default: all)

        Returns:
            job_id for tracking
        """
        normalized_name = self._normalize_company_name(company_name)
        company_input = domain if domain else company_name

        # Determine sources to query
        if priority_sources:
            requested_sources = [s for s in priority_sources if s in [ds.value for ds in DataSource]]
        else:
            requested_sources = [ds.value for ds in DataSource]

        # Create job record
        job_id = self._generate_job_id()

        insert_query = text("""
            INSERT INTO research_jobs (job_id, company_input, company_name, status, sources_requested)
            VALUES (:job_id, :company_input, :company_name, 'pending', :sources)
        """)

        self.db.execute(insert_query, {
            "job_id": job_id,
            "company_input": company_input,
            "company_name": normalized_name,
            "sources": json.dumps(requested_sources),
        })
        self.db.commit()

        # Run research synchronously in a thread (since FastAPI will handle this in background)
        import threading
        thread = threading.Thread(
            target=self._run_research_sync,
            args=(job_id, normalized_name, company_input, requested_sources)
        )
        thread.daemon = True
        thread.start()

        return job_id

    def _run_research_sync(
        self,
        job_id: str,
        company_name: str,
        company_input: str,
        sources: List[str]
    ) -> None:
        """Execute research job synchronously (for threading)."""
        import asyncio
        from app.core.database import get_session_factory

        # Create a new database session for this thread
        SessionLocal = get_session_factory()
        db = SessionLocal()

        try:
            # Create a new agent instance with the thread-local session
            thread_agent = CompanyResearchAgent.__new__(CompanyResearchAgent)
            thread_agent.db = db
            thread_agent.SOURCE_WEIGHTS = self.SOURCE_WEIGHTS

            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                loop.run_until_complete(thread_agent._run_research(job_id, company_name, company_input, sources))
            finally:
                loop.close()
        except Exception as e:
            logger.error(f"Research thread failed: {e}")
            # Update job status to failed
            try:
                from sqlalchemy import text as sql_text
                db.execute(sql_text("""
                    UPDATE research_jobs
                    SET status = 'failed', error_message = :error, completed_at = NOW()
                    WHERE job_id = :job_id
                """), {"job_id": job_id, "error": str(e)})
                db.commit()
            except Exception:
                pass
        finally:
            db.close()

    async def _run_research(
        self,
        job_id: str,
        company_name: str,
        company_input: str,
        sources: List[str]
    ) -> None:
        """Execute research job asynchronously."""
        # Update status to running
        update_query = text("""
            UPDATE research_jobs
            SET status = 'running', started_at = NOW()
            WHERE job_id = :job_id
        """)
        self.db.execute(update_query, {"job_id": job_id})
        self.db.commit()

        results = {}
        completed_sources = []
        failed_sources = []
        domain = self._extract_domain(company_input)

        # Run source queries in parallel
        tasks = []
        for source in sources:
            tasks.append(self._query_source(source, company_name, domain))

        source_results = await asyncio.gather(*tasks, return_exceptions=True)

        # Process results
        for i, source in enumerate(sources):
            result = source_results[i]
            if isinstance(result, Exception):
                logger.error(f"Source {source} failed: {result}")
                failed_sources.append({"source": source, "error": str(result)})
            elif result is None or (isinstance(result, dict) and result.get("error")):
                failed_sources.append({"source": source, "error": result.get("error") if result else "No data"})
            else:
                results[source] = result
                completed_sources.append(source)

            # Update progress
            progress = (i + 1) / len(sources)
            self._update_progress(job_id, progress, completed_sources, failed_sources)

        # Synthesize profile
        profile = self._synthesize_profile(company_name, results, domain)

        # Calculate confidence
        confidence = self._calculate_confidence(completed_sources)

        # Determine final status
        if len(completed_sources) == 0:
            status = ResearchStatus.FAILED
        elif len(failed_sources) > 0:
            status = ResearchStatus.PARTIAL
        else:
            status = ResearchStatus.COMPLETED

        # Update job with results
        final_update = text("""
            UPDATE research_jobs
            SET status = :status,
                sources_completed = :completed,
                sources_failed = :failed,
                progress = 1.0,
                results = :results,
                completed_at = NOW()
            WHERE job_id = :job_id
        """)

        self.db.execute(final_update, {
            "job_id": job_id,
            "status": status.value,
            "completed": json.dumps(completed_sources),
            "failed": json.dumps(failed_sources),
            "results": json.dumps({
                "profile": profile,
                "confidence": confidence,
                "sources_used": completed_sources,
            }),
        })
        self.db.commit()

        # Cache the profile
        if status in (ResearchStatus.COMPLETED, ResearchStatus.PARTIAL):
            self._cache_profile(company_name, profile, completed_sources, confidence)

    async def _query_source(
        self,
        source: str,
        company_name: str,
        domain: Optional[str]
    ) -> Optional[Dict[str, Any]]:
        """Query a specific data source."""
        try:
            if source == DataSource.ENRICHMENT.value:
                return await self._query_enrichment(company_name)
            elif source == DataSource.GITHUB.value:
                return await self._query_github(company_name)
            elif source == DataSource.GLASSDOOR.value:
                return await self._query_glassdoor(company_name)
            elif source == DataSource.APP_STORE.value:
                return await self._query_app_store(company_name)
            elif source == DataSource.WEB_TRAFFIC.value:
                return await self._query_web_traffic(company_name, domain)
            elif source == DataSource.NEWS.value:
                return await self._query_news(company_name)
            elif source == DataSource.SEC_FILINGS.value:
                return await self._query_sec(company_name)
            elif source == DataSource.CORPORATE_REGISTRY.value:
                return await self._query_corporate_registry(company_name)
            elif source == DataSource.SCORING.value:
                return await self._query_scoring(company_name)
            elif source == DataSource.WEB_SCRAPE.value:
                return await self._query_web_scrape(company_name, domain)
            else:
                return {"error": f"Unknown source: {source}"}
        except Exception as e:
            logger.error(f"Error querying {source}: {e}")
            return {"error": str(e)}

    async def _query_enrichment(self, company_name: str) -> Optional[Dict]:
        """Query company enrichment data (T22)."""
        query = text("""
            SELECT * FROM company_enrichment
            WHERE LOWER(company_name) = LOWER(:name)
        """)
        try:
            result = self.db.execute(query, {"name": company_name})
            row = result.mappings().fetchone()
            if row:
                return {
                    "revenue": row.get("latest_revenue"),
                    "assets": row.get("latest_assets"),
                    "net_income": row.get("latest_net_income"),
                    "employees": row.get("employee_count"),
                    "employee_growth": row.get("employee_growth_yoy"),
                    "funding_total": row.get("total_funding"),
                    "last_funding": row.get("last_funding_amount"),
                    "last_funding_date": str(row["last_funding_date"]) if row.get("last_funding_date") else None,
                    "industry": row.get("industry"),
                    "sector": row.get("sector"),
                    "status": row.get("company_status"),
                    "cik": row.get("cik"),
                    "ticker": row.get("ticker"),
                }
            return None
        except Exception as e:
            logger.warning(f"Enrichment query failed: {e}")
            self.db.rollback()
            return None

    async def _query_github(self, company_name: str) -> Optional[Dict]:
        """
        Query GitHub organization data (T34).

        ENHANCED: Now actually fetches from GitHub API if data is missing or stale.
        """
        # Get GitHub org name from mappings or derive from company name
        normalized = company_name.lower().replace(" ", "").replace(",", "").replace(".", "")
        mapping = COMPANY_MAPPINGS.get(normalized, {})
        org_name = mapping.get("github", normalized)

        # First check if we have fresh data (less than 24 hours old)
        query = text("""
            SELECT * FROM github_organizations
            WHERE LOWER(login) = LOWER(:name) OR LOWER(name) LIKE LOWER(:pattern)
            ORDER BY last_fetched_at DESC NULLS LAST
            LIMIT 1
        """)
        try:
            result = self.db.execute(query, {"name": org_name, "pattern": f"%{company_name}%"})
            row = result.mappings().fetchone()

            # Check if data is fresh (fetched within last 24 hours)
            is_fresh = False
            if row and row.get("last_fetched_at"):
                age = datetime.utcnow() - row["last_fetched_at"]
                is_fresh = age < timedelta(hours=24)

            # If no data or stale data, fetch fresh from GitHub API
            if not row or not is_fresh:
                logger.info(f"Fetching fresh GitHub data for org: {org_name}")
                fresh_data = await self._fetch_github_from_api(org_name)
                if fresh_data:
                    return fresh_data

            # Return existing data if available
            if row:
                return {
                    "org_name": row.get("name"),
                    "login": row.get("login"),
                    "public_repos": row.get("public_repos"),
                    "total_stars": row.get("total_stars"),
                    "total_forks": row.get("total_forks"),
                    "total_contributors": row.get("total_contributors"),
                    "velocity_score": row.get("velocity_score"),
                    "primary_language": row.get("primary_languages")[0] if row.get("primary_languages") else None,
                    "github_url": f"https://github.com/{row.get('login')}",
                    "data_freshness": "cached",
                }
            return None
        except Exception as e:
            logger.warning(f"GitHub query failed: {e}")
            self.db.rollback()
            return None

    async def _fetch_github_from_api(self, org_name: str) -> Optional[Dict]:
        """Fetch GitHub data from API and store it."""
        try:
            from app.sources.github.ingest import GitHubAnalyticsService

            # Create service with a fresh session for this operation
            service = GitHubAnalyticsService(self.db)

            # Fetch organization (this calls GitHub API and stores in DB)
            org_data = await service.fetch_organization(org_name)

            if org_data:
                logger.info(f"Successfully fetched GitHub data for {org_name}")
                return {
                    "org_name": org_data.get("name"),
                    "login": org_data.get("login"),
                    "public_repos": org_data.get("public_repos"),
                    "total_stars": org_data.get("metrics", {}).get("total_stars", 0),
                    "total_forks": org_data.get("metrics", {}).get("total_forks", 0),
                    "total_contributors": org_data.get("metrics", {}).get("repo_count", 0),
                    "velocity_score": org_data.get("velocity_score"),
                    "primary_language": org_data.get("metrics", {}).get("primary_languages", [None])[0],
                    "github_url": f"https://github.com/{org_data.get('login')}",
                    "data_freshness": "fresh",
                }
            return None
        except Exception as e:
            logger.warning(f"GitHub API fetch failed for {org_name}: {e}")
            return None

    async def _query_glassdoor(self, company_name: str) -> Optional[Dict]:
        """Query Glassdoor data (T38)."""
        query = text("""
            SELECT * FROM glassdoor_companies
            WHERE LOWER(company_name) = LOWER(:name)
        """)
        try:
            result = self.db.execute(query, {"name": company_name})
            row = result.mappings().fetchone()
            if row:
                return {
                    "overall_rating": row.get("overall_rating"),
                    "ceo_approval": row.get("ceo_approval"),
                    "recommend_to_friend": row.get("recommend_to_friend"),
                    "work_life_balance": row.get("work_life_balance"),
                    "compensation_rating": row.get("compensation_benefits"),
                    "culture_rating": row.get("culture_values"),
                    "review_count": row.get("review_count"),
                    "salary_count": row.get("salary_count"),
                }
            return None
        except Exception as e:
            logger.warning(f"Glassdoor query failed: {e}")
            self.db.rollback()
            return None

    async def _query_app_store(self, company_name: str) -> Optional[Dict]:
        """Query App Store data (T39)."""
        query = text("""
            SELECT cap.*, asa.app_name, asa.category, asa.current_rating,
                   asa.rating_count, asa.price
            FROM company_app_portfolios cap
            JOIN app_store_apps asa ON cap.app_id = asa.app_id AND cap.store = asa.store
            WHERE LOWER(cap.company_name) = LOWER(:name)
        """)
        try:
            result = self.db.execute(query, {"name": company_name})
            rows = result.mappings().fetchall()
            if rows:
                apps = []
                for row in rows:
                    apps.append({
                        "app_name": row["app_name"],
                        "store": row["store"],
                        "category": row["category"],
                        "rating": row["current_rating"],
                        "rating_count": row["rating_count"],
                    })
                return {
                    "app_count": len(apps),
                    "apps": apps,
                    "avg_rating": sum(a["rating"] or 0 for a in apps) / len(apps) if apps else None,
                }
            return None
        except Exception as e:
            logger.warning(f"App Store query failed: {e}")
            self.db.rollback()
            return None

    async def _query_web_traffic(self, company_name: str, domain: Optional[str]) -> Optional[Dict]:
        """
        Query web traffic data (T35).

        ENHANCED: Now actually fetches from Tranco rankings.
        """
        # Get domain from mappings or use provided domain
        normalized = company_name.lower().replace(" ", "").replace(",", "").replace(".", "")
        mapping = COMPANY_MAPPINGS.get(normalized, {})

        # Determine domain to look up
        lookup_domain = domain or mapping.get("domain")
        if not lookup_domain:
            # Try to derive domain from company name
            lookup_domain = f"{normalized}.com"

        try:
            from app.sources.web_traffic.client import WebTrafficClient

            client = WebTrafficClient()
            traffic_data = client.get_domain_traffic(lookup_domain)
            client.close()

            if traffic_data and traffic_data.get("tranco_rank"):
                logger.info(f"Got web traffic data for {lookup_domain}: rank #{traffic_data['tranco_rank']}")
                return {
                    "domain": lookup_domain,
                    "tranco_rank": traffic_data.get("tranco_rank"),
                    "providers_used": traffic_data.get("providers_used", []),
                    "data_freshness": "fresh",
                }

            # Also try without the domain suffix if it didn't work
            if not traffic_data.get("tranco_rank") and "." in lookup_domain:
                base_name = lookup_domain.split(".")[0]
                for suffix in [".com", ".io", ".co", ".org", ".net"]:
                    alt_domain = base_name + suffix
                    if alt_domain != lookup_domain:
                        traffic_data = client.get_domain_traffic(alt_domain)
                        if traffic_data and traffic_data.get("tranco_rank"):
                            return {
                                "domain": alt_domain,
                                "tranco_rank": traffic_data.get("tranco_rank"),
                                "providers_used": traffic_data.get("providers_used", []),
                                "data_freshness": "fresh",
                            }

            return None
        except Exception as e:
            logger.warning(f"Web traffic query failed for {lookup_domain}: {e}")
            return None

    async def _query_news(self, company_name: str) -> Optional[Dict]:
        """
        Query news data (T24).

        ENHANCED: Now fetches from Google News RSS if no local data.
        """
        # First check local database
        query = text("""
            SELECT title, source, published_at, url
            FROM news_articles
            WHERE LOWER(title) LIKE LOWER(:pattern)
               OR LOWER(content) LIKE LOWER(:pattern)
            ORDER BY published_at DESC
            LIMIT 5
        """)
        try:
            result = self.db.execute(query, {"pattern": f"%{company_name}%"})
            rows = result.mappings().fetchall()
            if rows:
                articles = []
                for row in rows:
                    articles.append({
                        "title": row["title"],
                        "source": row["source"],
                        "date": str(row["published_at"]) if row["published_at"] else None,
                        "url": row["url"],
                    })
                return {
                    "recent_articles": articles,
                    "article_count": len(articles),
                    "data_source": "database",
                }
        except Exception as e:
            logger.warning(f"News query failed: {e}")
            self.db.rollback()

        # If no local data, fetch from Google News
        logger.info(f"Fetching news from Google News for: {company_name}")
        return await self._fetch_news_from_google(company_name)

    async def _fetch_news_from_google(self, company_name: str) -> Optional[Dict]:
        """Fetch news from Google News RSS feed and summarize with LLM."""
        try:
            from app.news.sources.google_news import GoogleNewsSource

            news_source = GoogleNewsSource()
            try:
                # Fetch news for this company
                items = await news_source.fetch(
                    queries=[f'"{company_name}" company'],
                    company_names=[company_name]
                )

                if items:
                    articles = []
                    for item in items[:5]:  # Limit to 5 articles
                        articles.append({
                            "title": item.get("title"),
                            "source": item.get("source", "Google News"),
                            "date": item.get("published_at").isoformat() if item.get("published_at") else None,
                            "url": item.get("url"),
                            "event_type": item.get("event_type"),
                        })

                    logger.info(f"Found {len(articles)} news articles for {company_name}")

                    # Generate AI summary of the news
                    summary = await self._summarize_news_with_llm(company_name, articles)

                    return {
                        "recent_articles": articles,
                        "article_count": len(articles),
                        "content_summary": summary,
                        "data_source": "google_news",
                        "data_freshness": "fresh",
                    }
            finally:
                await news_source.close()

            return None
        except Exception as e:
            logger.warning(f"Google News fetch failed for {company_name}: {e}")
            return None

    async def _summarize_news_with_llm(self, company_name: str, articles: List[Dict]) -> Optional[str]:
        """Use LLM to create a concise summary of recent news."""
        try:
            from app.agentic.llm_client import get_llm_client

            llm = get_llm_client()
            if not llm:
                logger.info("No LLM API key configured, skipping news summary")
                return None

            # Build prompt with article titles
            article_list = "\n".join([
                f"- {a.get('title', 'Untitled')} ({a.get('event_type', 'news')})"
                for a in articles
            ])

            prompt = f"""Analyze these recent news headlines about {company_name} and write a 2-3 sentence executive summary of what's happening with the company. Focus on the most important business developments.

Recent headlines:
{article_list}

Write a concise summary (2-3 sentences) for an investor audience:"""

            system_prompt = "You are a financial analyst summarizing company news. Be concise and factual. Focus on business implications."

            logger.info(f"Generating news summary for {company_name}")
            response = await llm.complete(prompt, system_prompt=system_prompt)

            summary = response.content.strip()
            logger.info(f"Generated summary ({response.total_tokens} tokens, ${response.cost_usd:.4f})")

            return summary

        except Exception as e:
            logger.warning(f"LLM summarization failed: {e}")
            return None

    async def _query_sec(self, company_name: str) -> Optional[Dict]:
        """
        Query SEC filings data.

        ENHANCED: Now searches SEC EDGAR API for company filings.
        """
        # First check local Form D filings
        query = text("""
            SELECT issuer_name, cik, total_amount_sold, total_offering_amount,
                   date_of_first_sale, industry_group
            FROM form_d_filings
            WHERE LOWER(issuer_name) LIKE LOWER(:pattern)
            ORDER BY date_of_first_sale DESC
            LIMIT 1
        """)
        try:
            result = self.db.execute(query, {"pattern": f"%{company_name}%"})
            row = result.mappings().fetchone()
            if row:
                return {
                    "issuer_name": row["issuer_name"],
                    "cik": row["cik"],
                    "total_raised": row["total_amount_sold"],
                    "offering_amount": row["total_offering_amount"],
                    "first_sale_date": str(row["date_of_first_sale"]) if row["date_of_first_sale"] else None,
                    "industry_group": row["industry_group"],
                    "data_source": "local_db",
                }
        except Exception as e:
            logger.warning(f"SEC local query failed: {e}")
            self.db.rollback()

        # If no local data, search SEC EDGAR API
        logger.info(f"Searching SEC EDGAR for: {company_name}")
        return await self._fetch_sec_from_edgar(company_name)

    async def _fetch_sec_from_edgar(self, company_name: str) -> Optional[Dict]:
        """Fetch company filings from SEC EDGAR API."""
        try:
            import httpx

            # Search SEC full-text search for company
            # SEC provides a company search endpoint
            search_url = "https://efts.sec.gov/LATEST/search-index"

            async with httpx.AsyncClient(timeout=15.0) as client:
                # Try company tickers endpoint first (for well-known companies)
                try:
                    ticker_resp = await client.get(
                        "https://www.sec.gov/cgi-bin/browse-edgar",
                        params={
                            "action": "getcompany",
                            "company": company_name,
                            "type": "",
                            "dateb": "",
                            "owner": "include",
                            "count": "10",
                            "output": "atom"
                        },
                        headers={"User-Agent": "Nexdata Research Bot (compliance@nexdata.ai)"}
                    )

                    if ticker_resp.status_code == 200:
                        # Parse the Atom feed for company info
                        import xml.etree.ElementTree as ET
                        try:
                            root = ET.fromstring(ticker_resp.text)
                            ns = {'atom': 'http://www.w3.org/2005/Atom'}

                            entries = root.findall('.//atom:entry', ns)
                            if entries:
                                # Get first matching company
                                entry = entries[0]
                                title = entry.find('atom:title', ns)
                                summary = entry.find('atom:summary', ns)
                                link = entry.find('atom:link', ns)

                                # Extract CIK from link
                                cik = None
                                if link is not None:
                                    href = link.get('href', '')
                                    import re
                                    cik_match = re.search(r'CIK=(\d+)', href)
                                    if cik_match:
                                        cik = cik_match.group(1)

                                if title is not None:
                                    logger.info(f"Found SEC filing for {company_name}: {title.text}")
                                    return {
                                        "issuer_name": title.text.split(' - ')[0] if title.text else company_name,
                                        "cik": cik,
                                        "filing_type": title.text.split(' - ')[-1] if title.text and ' - ' in title.text else None,
                                        "summary": summary.text[:200] if summary is not None and summary.text else None,
                                        "sec_url": link.get('href') if link is not None else None,
                                        "data_source": "sec_edgar",
                                        "data_freshness": "fresh",
                                    }
                        except ET.ParseError:
                            pass

                except Exception as e:
                    logger.debug(f"SEC EDGAR search failed: {e}")

            return None

        except Exception as e:
            logger.warning(f"SEC EDGAR fetch failed for {company_name}: {e}")
            return None

    async def _query_corporate_registry(self, company_name: str) -> Optional[Dict]:
        """
        Query corporate registry data (T33).

        ENHANCED: Now searches OpenCorporates API for company registrations.
        """
        logger.info(f"Searching OpenCorporates for: {company_name}")

        try:
            from app.sources.opencorporates.client import OpenCorporatesClient

            client = OpenCorporatesClient()

            # Search for company (try US jurisdictions first)
            results = client.search_companies(
                query=company_name,
                jurisdiction="us_de",  # Delaware (most US companies)
                per_page=5
            )

            if not results.get("companies"):
                # Try without jurisdiction filter
                results = client.search_companies(
                    query=company_name,
                    per_page=5
                )

            if results.get("companies"):
                company = results["companies"][0]  # Best match
                logger.info(f"Found corporate registry data for {company_name}: {company.get('name')}")

                return {
                    "registered_name": company.get("name"),
                    "company_number": company.get("company_number"),
                    "jurisdiction": company.get("jurisdiction_code"),
                    "incorporation_date": company.get("incorporation_date"),
                    "company_type": company.get("company_type"),
                    "status": company.get("current_status"),
                    "registered_address": company.get("registered_address"),
                    "registry_url": company.get("registry_url"),
                    "opencorporates_url": company.get("opencorporates_url"),
                    "data_source": "opencorporates",
                    "data_freshness": "fresh",
                }

            return None

        except Exception as e:
            logger.warning(f"OpenCorporates search failed for {company_name}: {e}")
            return None

    async def _query_scoring(self, company_name: str) -> Optional[Dict]:
        """Query company score (T36)."""
        query = text("""
            SELECT * FROM company_scores
            WHERE LOWER(company_name) = LOWER(:name)
            ORDER BY scored_at DESC
            LIMIT 1
        """)
        try:
            result = self.db.execute(query, {"name": company_name})
            row = result.mappings().fetchone()
            if row:
                return {
                    "composite_score": row["composite_score"],
                    "tier": row["tier"],
                    "growth_score": row["growth_score"],
                    "stability_score": row["stability_score"],
                    "market_score": row["market_score"],
                    "tech_score": row["tech_score"],
                    "confidence": row["confidence"],
                }
            return None
        except Exception as e:
            logger.warning(f"Scoring query failed: {e}")
            self.db.rollback()
            return None

    async def _query_web_scrape(self, company_name: str, domain: Optional[str]) -> Optional[Dict]:
        """
        Scrape company website for info like employee count, revenue, about info.

        AGENTIC: Goes to company website and extracts structured data.
        """
        # Get domain from mappings or use provided
        normalized = company_name.lower().replace(" ", "").replace(",", "").replace(".", "")
        mapping = COMPANY_MAPPINGS.get(normalized, {})
        target_domain = domain or mapping.get("domain")

        if not target_domain:
            target_domain = f"{normalized}.com"

        logger.info(f"Web scraping company info from: {target_domain}")

        try:
            import httpx
            from bs4 import BeautifulSoup

            async with httpx.AsyncClient(timeout=15.0, follow_redirects=True) as client:
                # Try to fetch About page
                urls_to_try = [
                    f"https://{target_domain}/about",
                    f"https://{target_domain}/about-us",
                    f"https://{target_domain}/company",
                    f"https://www.{target_domain}/about",
                    f"https://{target_domain}",
                ]

                page_content = None
                fetched_url = None

                for url in urls_to_try:
                    try:
                        response = await client.get(url, headers={
                            "User-Agent": "Mozilla/5.0 (compatible; NexdataBot/1.0; +https://nexdata.ai)"
                        })
                        if response.status_code == 200:
                            page_content = response.text
                            fetched_url = url
                            logger.info(f"Successfully fetched: {url}")
                            break
                    except Exception:
                        continue

                if not page_content:
                    return None

                # Parse HTML
                soup = BeautifulSoup(page_content, "lxml")

                # Extract useful information
                result = {
                    "source_url": fetched_url,
                    "data_freshness": "fresh",
                }

                # Get page title and description
                title = soup.find("title")
                if title:
                    result["page_title"] = title.get_text().strip()

                meta_desc = soup.find("meta", attrs={"name": "description"})
                if meta_desc and meta_desc.get("content"):
                    result["description"] = meta_desc["content"][:500]

                # Try to extract employee count from text
                text_content = soup.get_text().lower()

                # Look for employee patterns
                import re
                employee_patterns = [
                    r'(\d{1,3}(?:,\d{3})*)\+?\s*employees',
                    r'team of\s*(\d{1,3}(?:,\d{3})*)\+?',
                    r'(\d{1,3}(?:,\d{3})*)\+?\s*team members',
                    r'workforce of\s*(\d{1,3}(?:,\d{3})*)',
                ]

                for pattern in employee_patterns:
                    match = re.search(pattern, text_content)
                    if match:
                        emp_count = match.group(1).replace(",", "")
                        try:
                            result["estimated_employees"] = int(emp_count)
                            logger.info(f"Found employee count: {emp_count}")
                            break
                        except ValueError:
                            pass

                # Look for funding/valuation mentions
                funding_patterns = [
                    r'\$(\d+(?:\.\d+)?)\s*(billion|million|B|M)\s*(?:valuation|valued)',
                    r'raised\s*\$(\d+(?:\.\d+)?)\s*(billion|million|B|M)',
                    r'\$(\d+(?:\.\d+)?)\s*(billion|million|B|M)\s*in\s*funding',
                ]

                for pattern in funding_patterns:
                    match = re.search(pattern, text_content, re.IGNORECASE)
                    if match:
                        amount = float(match.group(1))
                        unit = match.group(2).upper()
                        if unit in ["BILLION", "B"]:
                            amount *= 1_000_000_000
                        elif unit in ["MILLION", "M"]:
                            amount *= 1_000_000
                        result["funding_mentioned"] = amount
                        logger.info(f"Found funding mention: ${amount:,.0f}")
                        break

                # Look for founded year
                founded_patterns = [
                    r'founded\s*(?:in\s*)?(\d{4})',
                    r'since\s*(\d{4})',
                    r'established\s*(?:in\s*)?(\d{4})',
                ]

                for pattern in founded_patterns:
                    match = re.search(pattern, text_content)
                    if match:
                        year = int(match.group(1))
                        if 1900 <= year <= 2026:
                            result["founded_year"] = year
                            break

                # Get social links
                social_links = {}
                for link in soup.find_all("a", href=True):
                    href = link["href"]
                    if "linkedin.com" in href:
                        social_links["linkedin"] = href
                    elif "twitter.com" in href or "x.com" in href:
                        social_links["twitter"] = href

                if social_links:
                    result["social_links"] = social_links

                if len(result) > 2:  # More than just source_url and freshness
                    return result

                return None

        except Exception as e:
            logger.warning(f"Web scrape failed for {target_domain}: {e}")
            return None

    def _update_progress(
        self,
        job_id: str,
        progress: float,
        completed: List[str],
        failed: List[Dict]
    ) -> None:
        """Update job progress."""
        update_query = text("""
            UPDATE research_jobs
            SET progress = :progress,
                sources_completed = :completed,
                sources_failed = :failed
            WHERE job_id = :job_id
        """)
        self.db.execute(update_query, {
            "job_id": job_id,
            "progress": progress,
            "completed": json.dumps(completed),
            "failed": json.dumps(failed),
        })
        self.db.commit()

    def _synthesize_profile(
        self,
        company_name: str,
        results: Dict[str, Dict],
        domain: Optional[str]
    ) -> Dict[str, Any]:
        """Synthesize data from all sources into unified profile."""
        profile = {
            "company_name": company_name,
            "domain": domain,
            "generated_at": datetime.utcnow().isoformat() + "Z",
        }

        # Basic info from enrichment
        enrichment = results.get(DataSource.ENRICHMENT.value, {})
        if enrichment:
            profile["financials"] = {
                "revenue": enrichment.get("revenue"),
                "assets": enrichment.get("assets"),
                "net_income": enrichment.get("net_income"),
                "funding_total": enrichment.get("funding_total"),
                "last_funding": enrichment.get("last_funding"),
                "last_funding_date": enrichment.get("last_funding_date"),
            }
            profile["team"] = {
                "employee_count": enrichment.get("employees"),
                "employee_growth_yoy": enrichment.get("employee_growth"),
            }
            profile["classification"] = {
                "industry": enrichment.get("industry"),
                "sector": enrichment.get("sector"),
                "status": enrichment.get("status"),
            }
            profile["identifiers"] = {
                "cik": enrichment.get("cik"),
                "ticker": enrichment.get("ticker"),
            }

        # Tech presence from GitHub
        github = results.get(DataSource.GITHUB.value, {})
        if github:
            profile["tech_presence"] = {
                "github_org": github.get("login"),
                "public_repos": github.get("public_repos"),
                "total_stars": github.get("total_stars"),
                "total_forks": github.get("total_forks"),
                "contributors": github.get("total_contributors"),
                "velocity_score": github.get("velocity_score"),
                "primary_language": github.get("primary_language"),
                "github_url": github.get("github_url"),
            }

        # Employer brand from Glassdoor
        glassdoor = results.get(DataSource.GLASSDOOR.value, {})
        if glassdoor:
            profile["employer_brand"] = {
                "overall_rating": glassdoor.get("overall_rating"),
                "ceo_approval": glassdoor.get("ceo_approval"),
                "recommend_to_friend": glassdoor.get("recommend_to_friend"),
                "work_life_balance": glassdoor.get("work_life_balance"),
                "compensation_rating": glassdoor.get("compensation_rating"),
                "culture_rating": glassdoor.get("culture_rating"),
                "review_count": glassdoor.get("review_count"),
            }

        # Mobile presence from App Store
        app_store = results.get(DataSource.APP_STORE.value, {})
        if app_store:
            profile["mobile_presence"] = {
                "app_count": app_store.get("app_count"),
                "avg_rating": app_store.get("avg_rating"),
                "apps": app_store.get("apps", []),
            }

        # SEC filings
        sec = results.get(DataSource.SEC_FILINGS.value, {})
        if sec:
            profile["sec_filings"] = {
                "issuer_name": sec.get("issuer_name"),
                "cik": sec.get("cik"),
                "form_d_total_raised": sec.get("total_raised"),
                "form_d_offering_amount": sec.get("offering_amount"),
                "industry_group": sec.get("industry_group"),
                "filing_type": sec.get("filing_type"),
                "sec_url": sec.get("sec_url"),
                "data_source": sec.get("data_source"),
            }
            # Update CIK in identifiers if found
            if sec.get("cik") and "identifiers" in profile:
                profile["identifiers"]["cik"] = sec.get("cik")

        # Corporate registry
        corp_reg = results.get(DataSource.CORPORATE_REGISTRY.value, {})
        if corp_reg:
            profile["corporate_registry"] = {
                "registered_name": corp_reg.get("registered_name"),
                "company_number": corp_reg.get("company_number"),
                "jurisdiction": corp_reg.get("jurisdiction"),
                "incorporation_date": corp_reg.get("incorporation_date"),
                "company_type": corp_reg.get("company_type"),
                "status": corp_reg.get("status"),
                "registered_address": corp_reg.get("registered_address"),
                "registry_url": corp_reg.get("registry_url"),
                "opencorporates_url": corp_reg.get("opencorporates_url"),
            }

        # Web traffic
        web_traffic = results.get(DataSource.WEB_TRAFFIC.value, {})
        if web_traffic:
            profile["web_traffic"] = {
                "domain": web_traffic.get("domain"),
                "tranco_rank": web_traffic.get("tranco_rank"),
                "providers": web_traffic.get("providers_used", []),
            }
            # Update domain in profile if we found it
            if web_traffic.get("domain") and not profile.get("domain"):
                profile["domain"] = web_traffic.get("domain")

        # Recent news
        news = results.get(DataSource.NEWS.value, {})
        if news:
            profile["news"] = {
                "recent_articles": news.get("recent_articles", []),
                "article_count": news.get("article_count", 0),
                "content_summary": news.get("content_summary"),  # AI-generated summary
            }

        # Company score
        scoring = results.get(DataSource.SCORING.value, {})
        if scoring:
            profile["health_score"] = {
                "composite": scoring.get("composite_score"),
                "tier": scoring.get("tier"),
                "growth": scoring.get("growth_score"),
                "stability": scoring.get("stability_score"),
                "market": scoring.get("market_score"),
                "tech": scoring.get("tech_score"),
            }

        # Web scrape data (fills gaps from website)
        web_scrape = results.get(DataSource.WEB_SCRAPE.value, {})
        if web_scrape:
            profile["web_scraped"] = {
                "source_url": web_scrape.get("source_url"),
                "description": web_scrape.get("description"),
                "founded_year": web_scrape.get("founded_year"),
                "social_links": web_scrape.get("social_links", {}),
            }

            # Fill in missing team data
            if web_scrape.get("estimated_employees"):
                if "team" not in profile:
                    profile["team"] = {}
                if not profile["team"].get("employee_count"):
                    profile["team"]["employee_count"] = web_scrape.get("estimated_employees")
                    profile["team"]["employee_source"] = "website_scrape"

            # Fill in missing funding data
            if web_scrape.get("funding_mentioned"):
                if "financials" not in profile:
                    profile["financials"] = {}
                if not profile["financials"].get("funding_total"):
                    profile["financials"]["funding_total"] = web_scrape.get("funding_mentioned")
                    profile["financials"]["funding_source"] = "website_mention"

        # Identify data gaps
        profile["data_gaps"] = self._identify_gaps(profile)

        return profile

    def _identify_gaps(self, profile: Dict) -> List[str]:
        """Identify missing data in profile."""
        gaps = []

        if not profile.get("financials") or not profile["financials"].get("revenue"):
            gaps.append("revenue_data")
        if not profile.get("team") or not profile["team"].get("employee_count"):
            gaps.append("employee_data")
        if not profile.get("tech_presence"):
            gaps.append("github_data")
        if not profile.get("employer_brand"):
            gaps.append("glassdoor_data")
        if not profile.get("mobile_presence"):
            gaps.append("app_store_data")
        if not profile.get("health_score"):
            gaps.append("scoring_data")
        if not profile.get("news") or profile["news"].get("article_count", 0) == 0:
            gaps.append("news_data")
        if not profile.get("web_traffic"):
            gaps.append("web_traffic_data")

        return gaps

    def _calculate_confidence(self, completed_sources: List[str]) -> float:
        """Calculate confidence score based on data coverage."""
        total_weight = 0
        for source in completed_sources:
            source_enum = DataSource(source) if source in [ds.value for ds in DataSource] else None
            if source_enum:
                total_weight += self.SOURCE_WEIGHTS.get(source_enum, 0)
        return round(min(total_weight, 1.0), 2)

    def _cache_profile(
        self,
        company_name: str,
        profile: Dict,
        sources: List[str],
        confidence: float
    ) -> None:
        """Cache research profile."""
        expires_at = datetime.utcnow() + timedelta(days=7)

        query = text("""
            INSERT INTO research_cache (company_name, profile, sources_used, confidence_score, expires_at)
            VALUES (:name, :profile, :sources, :confidence, :expires)
            ON CONFLICT (company_name) DO UPDATE SET
                profile = EXCLUDED.profile,
                sources_used = EXCLUDED.sources_used,
                confidence_score = EXCLUDED.confidence_score,
                created_at = NOW(),
                expires_at = EXCLUDED.expires_at
        """)

        self.db.execute(query, {
            "name": company_name,
            "profile": json.dumps(profile),
            "sources": json.dumps(sources),
            "confidence": confidence,
            "expires": expires_at,
        })
        self.db.commit()

    def _get_cached_profile(self, company_name: str, max_age_hours: int = 168) -> Optional[Dict]:
        """Get cached profile if not expired."""
        query = text("""
            SELECT profile, sources_used, confidence_score, created_at
            FROM research_cache
            WHERE LOWER(company_name) = LOWER(:name)
              AND created_at > NOW() - INTERVAL ':hours hours'
        """.replace(":hours", str(max_age_hours)))

        result = self.db.execute(query, {"name": company_name})
        row = result.mappings().fetchone()

        if row:
            profile = row["profile"]
            profile["_cache_info"] = {
                "cached_at": row["created_at"].isoformat() + "Z" if row["created_at"] else None,
                "sources_used": row["sources_used"],
                "confidence": row["confidence_score"],
            }
            return profile

        return None

    def get_job_status(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Get research job status and results."""
        query = text("""
            SELECT * FROM research_jobs WHERE job_id = :job_id
        """)

        result = self.db.execute(query, {"job_id": job_id})
        row = result.mappings().fetchone()

        if not row:
            return None

        response = {
            "job_id": row["job_id"],
            "company_name": row["company_name"],
            "status": row["status"],
            "progress": row["progress"],
            "sources_requested": row["sources_requested"],
            "sources_completed": row["sources_completed"],
            "sources_failed": row["sources_failed"],
            "created_at": row["created_at"].isoformat() + "Z" if row["created_at"] else None,
            "started_at": row["started_at"].isoformat() + "Z" if row["started_at"] else None,
            "completed_at": row["completed_at"].isoformat() + "Z" if row["completed_at"] else None,
        }

        if row["status"] in ("completed", "partial"):
            response["results"] = row["results"]

        if row["error_message"]:
            response["error"] = row["error_message"]

        return response

    def get_cached_research(self, company_name: str, max_age_hours: int = 168) -> Optional[Dict[str, Any]]:
        """Get cached research for a company."""
        profile = self._get_cached_profile(company_name, max_age_hours=max_age_hours)
        if profile:
            return {
                "company_name": company_name,
                "status": "cached",
                "profile": profile,
            }
        return None

    def list_jobs(
        self,
        status: Optional[str] = None,
        limit: int = 20
    ) -> List[Dict[str, Any]]:
        """List research jobs."""
        conditions = ["1=1"]
        params = {"limit": limit}

        if status:
            conditions.append("status = :status")
            params["status"] = status

        where_clause = " AND ".join(conditions)

        query = text(f"""
            SELECT job_id, company_name, status, progress, created_at, completed_at
            FROM research_jobs
            WHERE {where_clause}
            ORDER BY created_at DESC
            LIMIT :limit
        """)

        result = self.db.execute(query, params)

        return [
            {
                "job_id": row["job_id"],
                "company_name": row["company_name"],
                "status": row["status"],
                "progress": row["progress"],
                "created_at": row["created_at"].isoformat() + "Z" if row["created_at"] else None,
                "completed_at": row["completed_at"].isoformat() + "Z" if row["completed_at"] else None,
            }
            for row in result.mappings()
        ]

    def get_stats(self) -> Dict[str, Any]:
        """Get research agent statistics."""
        stats_query = text("""
            SELECT
                COUNT(*) as total_jobs,
                COUNT(*) FILTER (WHERE status = 'completed') as completed,
                COUNT(*) FILTER (WHERE status = 'partial') as partial,
                COUNT(*) FILTER (WHERE status = 'failed') as failed,
                COUNT(*) FILTER (WHERE status = 'running') as running,
                AVG(EXTRACT(EPOCH FROM (completed_at - started_at))) as avg_duration_seconds
            FROM research_jobs
            WHERE started_at IS NOT NULL
        """)

        cache_query = text("""
            SELECT COUNT(*) as cached_profiles,
                   AVG(confidence_score) as avg_confidence
            FROM research_cache
            WHERE expires_at > NOW()
        """)

        stats_result = self.db.execute(stats_query).mappings().fetchone()
        cache_result = self.db.execute(cache_query).mappings().fetchone()

        return {
            "jobs": {
                "total": stats_result["total_jobs"],
                "completed": stats_result["completed"],
                "partial": stats_result["partial"],
                "failed": stats_result["failed"],
                "running": stats_result["running"],
                "avg_duration_seconds": round(stats_result["avg_duration_seconds"], 2) if stats_result["avg_duration_seconds"] else None,
            },
            "cache": {
                "cached_profiles": cache_result["cached_profiles"],
                "avg_confidence": round(cache_result["avg_confidence"], 2) if cache_result["avg_confidence"] else None,
            },
            "available_sources": [ds.value for ds in DataSource],
        }

    def get_available_sources(self) -> Dict[str, Any]:
        """Get list of available data sources with descriptions."""
        return {
            "sources": [
                {
                    "id": DataSource.ENRICHMENT.value,
                    "name": "Company Enrichment",
                    "description": "SEC financials, funding data, employee counts",
                    "weight": self.SOURCE_WEIGHTS[DataSource.ENRICHMENT],
                },
                {
                    "id": DataSource.GITHUB.value,
                    "name": "GitHub Analytics",
                    "description": "Repository metrics, developer velocity, tech stack",
                    "weight": self.SOURCE_WEIGHTS[DataSource.GITHUB],
                },
                {
                    "id": DataSource.GLASSDOOR.value,
                    "name": "Glassdoor Data",
                    "description": "Employee reviews, ratings, salary data",
                    "weight": self.SOURCE_WEIGHTS[DataSource.GLASSDOOR],
                },
                {
                    "id": DataSource.APP_STORE.value,
                    "name": "App Store Data",
                    "description": "iOS/Android apps, ratings, rankings",
                    "weight": self.SOURCE_WEIGHTS[DataSource.APP_STORE],
                },
                {
                    "id": DataSource.WEB_TRAFFIC.value,
                    "name": "Web Traffic",
                    "description": "Tranco rankings, traffic estimates",
                    "weight": self.SOURCE_WEIGHTS[DataSource.WEB_TRAFFIC],
                },
                {
                    "id": DataSource.NEWS.value,
                    "name": "News & Media",
                    "description": "Recent news articles and press coverage",
                    "weight": self.SOURCE_WEIGHTS[DataSource.NEWS],
                },
                {
                    "id": DataSource.SEC_FILINGS.value,
                    "name": "SEC Filings",
                    "description": "Form D private placements, regulatory filings",
                    "weight": self.SOURCE_WEIGHTS[DataSource.SEC_FILINGS],
                },
                {
                    "id": DataSource.CORPORATE_REGISTRY.value,
                    "name": "Corporate Registry",
                    "description": "OpenCorporates company registrations",
                    "weight": self.SOURCE_WEIGHTS[DataSource.CORPORATE_REGISTRY],
                },
                {
                    "id": DataSource.SCORING.value,
                    "name": "Company Scoring",
                    "description": "ML-based health scores and tier ratings",
                    "weight": self.SOURCE_WEIGHTS[DataSource.SCORING],
                },
            ],
            "total_sources": len(DataSource),
        }

    def cancel_job(self, job_id: str) -> bool:
        """Cancel a pending or running research job."""
        # Check current status
        check_query = text("""
            SELECT status FROM research_jobs WHERE job_id = :job_id
        """)
        result = self.db.execute(check_query, {"job_id": job_id})
        row = result.fetchone()

        if not row:
            return False

        if row[0] not in ("pending", "running"):
            return False

        # Update to cancelled
        update_query = text("""
            UPDATE research_jobs
            SET status = 'cancelled', completed_at = NOW()
            WHERE job_id = :job_id AND status IN ('pending', 'running')
        """)
        self.db.execute(update_query, {"job_id": job_id})
        self.db.commit()
        return True
