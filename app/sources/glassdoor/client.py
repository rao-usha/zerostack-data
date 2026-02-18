"""
Glassdoor Client for company reviews, ratings, and salary data.

Provides structured storage and retrieval of Glassdoor-style data
with support for manual entry and bulk import.
"""

import logging
from typing import Dict, List, Optional, Any
from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


class GlassdoorClient:
    """
    Client for managing Glassdoor company data.

    Supports:
    - Company ratings and sentiment data
    - Salary information by role
    - Review summaries and trends
    - Bulk import capabilities
    """

    def __init__(self, db: Session):
        self.db = db
        self._ensure_tables()

    def _ensure_tables(self) -> None:
        """Ensure Glassdoor tables exist."""
        create_companies = text("""
            CREATE TABLE IF NOT EXISTS glassdoor_companies (
                id SERIAL PRIMARY KEY,
                company_name VARCHAR(255) NOT NULL,
                glassdoor_id VARCHAR(50),
                logo_url TEXT,
                website VARCHAR(255),
                headquarters VARCHAR(255),
                industry VARCHAR(100),
                company_size VARCHAR(50),
                founded_year INTEGER,
                overall_rating FLOAT,
                ceo_name VARCHAR(255),
                ceo_approval FLOAT,
                recommend_to_friend FLOAT,
                business_outlook FLOAT,
                work_life_balance FLOAT,
                compensation_benefits FLOAT,
                career_opportunities FLOAT,
                culture_values FLOAT,
                senior_management FLOAT,
                review_count INTEGER DEFAULT 0,
                salary_count INTEGER DEFAULT 0,
                interview_count INTEGER DEFAULT 0,
                data_source VARCHAR(50) DEFAULT 'manual',
                retrieved_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(company_name)
            )
        """)

        create_salaries = text("""
            CREATE TABLE IF NOT EXISTS glassdoor_salaries (
                id SERIAL PRIMARY KEY,
                company_id INTEGER REFERENCES glassdoor_companies(id) ON DELETE CASCADE,
                job_title VARCHAR(255) NOT NULL,
                location VARCHAR(255),
                base_salary_min INTEGER,
                base_salary_median INTEGER,
                base_salary_max INTEGER,
                total_comp_min INTEGER,
                total_comp_median INTEGER,
                total_comp_max INTEGER,
                sample_size INTEGER,
                experience_level VARCHAR(50),
                retrieved_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        create_reviews = text("""
            CREATE TABLE IF NOT EXISTS glassdoor_review_summaries (
                id SERIAL PRIMARY KEY,
                company_id INTEGER REFERENCES glassdoor_companies(id) ON DELETE CASCADE,
                period VARCHAR(20),
                avg_rating FLOAT,
                review_count INTEGER,
                top_pros TEXT[],
                top_cons TEXT[],
                retrieved_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        create_index = text("""
            CREATE INDEX IF NOT EXISTS idx_glassdoor_companies_rating
            ON glassdoor_companies(overall_rating DESC NULLS LAST)
        """)

        try:
            self.db.execute(create_companies)
            self.db.execute(create_salaries)
            self.db.execute(create_reviews)
            self.db.execute(create_index)
            self.db.commit()
        except Exception as e:
            logger.warning(f"Table creation warning: {e}")
            self.db.rollback()

    def get_company(self, company_name: str) -> Optional[Dict[str, Any]]:
        """
        Get Glassdoor data for a company.

        Args:
            company_name: Company name to look up

        Returns:
            Company data dict or None if not found
        """
        query = text("""
            SELECT * FROM glassdoor_companies
            WHERE LOWER(company_name) = LOWER(:name)
        """)

        result = self.db.execute(query, {"name": company_name})
        row = result.mappings().fetchone()

        if not row:
            return None

        return {
            "company_name": row["company_name"],
            "glassdoor_id": row["glassdoor_id"],
            "ratings": {
                "overall": row["overall_rating"],
                "work_life_balance": row["work_life_balance"],
                "compensation_benefits": row["compensation_benefits"],
                "career_opportunities": row["career_opportunities"],
                "culture_values": row["culture_values"],
                "senior_management": row["senior_management"],
            },
            "sentiment": {
                "ceo_approval": row["ceo_approval"],
                "recommend_to_friend": row["recommend_to_friend"],
                "business_outlook": row["business_outlook"],
            },
            "stats": {
                "review_count": row["review_count"],
                "salary_count": row["salary_count"],
                "interview_count": row["interview_count"],
            },
            "company_info": {
                "industry": row["industry"],
                "size": row["company_size"],
                "headquarters": row["headquarters"],
                "founded": row["founded_year"],
                "website": row["website"],
                "ceo_name": row["ceo_name"],
                "logo_url": row["logo_url"],
            },
            "retrieved_at": row["retrieved_at"].isoformat() + "Z"
            if row["retrieved_at"]
            else None,
            "data_source": row["data_source"],
        }

    def upsert_company(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Add or update company data.

        Args:
            data: Company data dict

        Returns:
            Upserted company data
        """
        query = text("""
            INSERT INTO glassdoor_companies (
                company_name, glassdoor_id, logo_url, website, headquarters,
                industry, company_size, founded_year, overall_rating,
                ceo_name, ceo_approval, recommend_to_friend, business_outlook,
                work_life_balance, compensation_benefits, career_opportunities,
                culture_values, senior_management, review_count, salary_count,
                interview_count, data_source, retrieved_at
            ) VALUES (
                :company_name, :glassdoor_id, :logo_url, :website, :headquarters,
                :industry, :company_size, :founded_year, :overall_rating,
                :ceo_name, :ceo_approval, :recommend_to_friend, :business_outlook,
                :work_life_balance, :compensation_benefits, :career_opportunities,
                :culture_values, :senior_management, :review_count, :salary_count,
                :interview_count, :data_source, NOW()
            )
            ON CONFLICT (company_name) DO UPDATE SET
                glassdoor_id = COALESCE(EXCLUDED.glassdoor_id, glassdoor_companies.glassdoor_id),
                logo_url = COALESCE(EXCLUDED.logo_url, glassdoor_companies.logo_url),
                website = COALESCE(EXCLUDED.website, glassdoor_companies.website),
                headquarters = COALESCE(EXCLUDED.headquarters, glassdoor_companies.headquarters),
                industry = COALESCE(EXCLUDED.industry, glassdoor_companies.industry),
                company_size = COALESCE(EXCLUDED.company_size, glassdoor_companies.company_size),
                founded_year = COALESCE(EXCLUDED.founded_year, glassdoor_companies.founded_year),
                overall_rating = COALESCE(EXCLUDED.overall_rating, glassdoor_companies.overall_rating),
                ceo_name = COALESCE(EXCLUDED.ceo_name, glassdoor_companies.ceo_name),
                ceo_approval = COALESCE(EXCLUDED.ceo_approval, glassdoor_companies.ceo_approval),
                recommend_to_friend = COALESCE(EXCLUDED.recommend_to_friend, glassdoor_companies.recommend_to_friend),
                business_outlook = COALESCE(EXCLUDED.business_outlook, glassdoor_companies.business_outlook),
                work_life_balance = COALESCE(EXCLUDED.work_life_balance, glassdoor_companies.work_life_balance),
                compensation_benefits = COALESCE(EXCLUDED.compensation_benefits, glassdoor_companies.compensation_benefits),
                career_opportunities = COALESCE(EXCLUDED.career_opportunities, glassdoor_companies.career_opportunities),
                culture_values = COALESCE(EXCLUDED.culture_values, glassdoor_companies.culture_values),
                senior_management = COALESCE(EXCLUDED.senior_management, glassdoor_companies.senior_management),
                review_count = COALESCE(EXCLUDED.review_count, glassdoor_companies.review_count),
                salary_count = COALESCE(EXCLUDED.salary_count, glassdoor_companies.salary_count),
                interview_count = COALESCE(EXCLUDED.interview_count, glassdoor_companies.interview_count),
                data_source = EXCLUDED.data_source,
                retrieved_at = NOW()
            RETURNING id
        """)

        params = {
            "company_name": data.get("company_name"),
            "glassdoor_id": data.get("glassdoor_id"),
            "logo_url": data.get("logo_url"),
            "website": data.get("website"),
            "headquarters": data.get("headquarters"),
            "industry": data.get("industry"),
            "company_size": data.get("company_size"),
            "founded_year": data.get("founded_year"),
            "overall_rating": data.get("overall_rating"),
            "ceo_name": data.get("ceo_name"),
            "ceo_approval": data.get("ceo_approval"),
            "recommend_to_friend": data.get("recommend_to_friend"),
            "business_outlook": data.get("business_outlook"),
            "work_life_balance": data.get("work_life_balance"),
            "compensation_benefits": data.get("compensation_benefits"),
            "career_opportunities": data.get("career_opportunities"),
            "culture_values": data.get("culture_values"),
            "senior_management": data.get("senior_management"),
            "review_count": data.get("review_count", 0),
            "salary_count": data.get("salary_count", 0),
            "interview_count": data.get("interview_count", 0),
            "data_source": data.get("data_source", "manual"),
        }

        self.db.execute(query, params)
        self.db.commit()

        return self.get_company(data["company_name"])

    def get_salaries(
        self,
        company_name: str,
        job_title: Optional[str] = None,
        location: Optional[str] = None,
        limit: int = 20,
    ) -> Dict[str, Any]:
        """
        Get salary data for a company.

        Args:
            company_name: Company name
            job_title: Filter by job title (partial match)
            location: Filter by location
            limit: Max results

        Returns:
            Salary data dict
        """
        # Get company ID
        company_query = text("""
            SELECT id FROM glassdoor_companies
            WHERE LOWER(company_name) = LOWER(:name)
        """)
        company_result = self.db.execute(company_query, {"name": company_name})
        company_row = company_result.fetchone()

        if not company_row:
            return {
                "company_name": company_name,
                "salaries": [],
                "total_count": 0,
                "error": "Company not found",
            }

        company_id = company_row[0]

        # Build salary query
        conditions = ["company_id = :company_id"]
        params = {"company_id": company_id, "limit": limit}

        if job_title:
            conditions.append("LOWER(job_title) LIKE LOWER(:job_title)")
            params["job_title"] = f"%{job_title}%"

        if location:
            conditions.append("LOWER(location) LIKE LOWER(:location)")
            params["location"] = f"%{location}%"

        where_clause = " AND ".join(conditions)

        query = text(f"""
            SELECT job_title, location, base_salary_min, base_salary_median,
                   base_salary_max, total_comp_min, total_comp_median,
                   total_comp_max, sample_size, experience_level
            FROM glassdoor_salaries
            WHERE {where_clause}
            ORDER BY base_salary_median DESC NULLS LAST
            LIMIT :limit
        """)

        count_query = text(f"""
            SELECT COUNT(*) FROM glassdoor_salaries
            WHERE {where_clause}
        """)

        result = self.db.execute(query, params)
        rows = result.mappings().fetchall()

        # Remove limit for count
        count_params = {k: v for k, v in params.items() if k != "limit"}
        count_result = self.db.execute(count_query, count_params)
        total_count = count_result.scalar()

        salaries = []
        for row in rows:
            salaries.append(
                {
                    "job_title": row["job_title"],
                    "location": row["location"],
                    "base_salary": {
                        "min": row["base_salary_min"],
                        "median": row["base_salary_median"],
                        "max": row["base_salary_max"],
                    },
                    "total_comp": {
                        "min": row["total_comp_min"],
                        "median": row["total_comp_median"],
                        "max": row["total_comp_max"],
                    },
                    "sample_size": row["sample_size"],
                    "experience_level": row["experience_level"],
                }
            )

        return {
            "company_name": company_name,
            "salaries": salaries,
            "total_count": total_count,
        }

    def add_salaries(self, company_name: str, salaries: List[Dict]) -> Dict[str, Any]:
        """
        Bulk add salary data for a company.

        Args:
            company_name: Company name
            salaries: List of salary data dicts

        Returns:
            Result summary
        """
        # Get or create company
        company = self.get_company(company_name)
        if not company:
            self.upsert_company({"company_name": company_name})
            company = self.get_company(company_name)

        # Get company ID
        id_query = text("""
            SELECT id FROM glassdoor_companies
            WHERE LOWER(company_name) = LOWER(:name)
        """)
        id_result = self.db.execute(id_query, {"name": company_name})
        company_id = id_result.scalar()

        insert_query = text("""
            INSERT INTO glassdoor_salaries (
                company_id, job_title, location, base_salary_min,
                base_salary_median, base_salary_max, total_comp_min,
                total_comp_median, total_comp_max, sample_size,
                experience_level, retrieved_at
            ) VALUES (
                :company_id, :job_title, :location, :base_salary_min,
                :base_salary_median, :base_salary_max, :total_comp_min,
                :total_comp_median, :total_comp_max, :sample_size,
                :experience_level, NOW()
            )
        """)

        added = 0
        for salary in salaries:
            try:
                self.db.execute(
                    insert_query,
                    {
                        "company_id": company_id,
                        "job_title": salary.get("job_title"),
                        "location": salary.get("location"),
                        "base_salary_min": salary.get("base_salary_min"),
                        "base_salary_median": salary.get("base_salary_median"),
                        "base_salary_max": salary.get("base_salary_max"),
                        "total_comp_min": salary.get("total_comp_min"),
                        "total_comp_median": salary.get("total_comp_median"),
                        "total_comp_max": salary.get("total_comp_max"),
                        "sample_size": salary.get("sample_size"),
                        "experience_level": salary.get("experience_level"),
                    },
                )
                added += 1
            except Exception as e:
                logger.warning(f"Failed to add salary: {e}")

        # Update salary count
        update_query = text("""
            UPDATE glassdoor_companies
            SET salary_count = (
                SELECT COUNT(*) FROM glassdoor_salaries WHERE company_id = :company_id
            )
            WHERE id = :company_id
        """)
        self.db.execute(update_query, {"company_id": company_id})
        self.db.commit()

        return {
            "company_name": company_name,
            "salaries_added": added,
            "total_salaries": self.get_salaries(company_name)["total_count"],
        }

    def get_reviews(self, company_name: str) -> Dict[str, Any]:
        """
        Get review summary for a company.

        Args:
            company_name: Company name

        Returns:
            Review summary dict
        """
        # Get company data
        company = self.get_company(company_name)
        if not company:
            return {
                "company_name": company_name,
                "error": "Company not found",
            }

        # Get company ID
        id_query = text("""
            SELECT id FROM glassdoor_companies
            WHERE LOWER(company_name) = LOWER(:name)
        """)
        id_result = self.db.execute(id_query, {"name": company_name})
        company_id = id_result.scalar()

        # Get review summaries by period
        reviews_query = text("""
            SELECT period, avg_rating, review_count, top_pros, top_cons
            FROM glassdoor_review_summaries
            WHERE company_id = :company_id
            ORDER BY period DESC
            LIMIT 8
        """)
        reviews_result = self.db.execute(reviews_query, {"company_id": company_id})
        reviews = reviews_result.mappings().fetchall()

        rating_trend = []
        all_pros = []
        all_cons = []

        for row in reviews:
            rating_trend.append(
                {
                    "period": row["period"],
                    "avg_rating": row["avg_rating"],
                    "count": row["review_count"],
                }
            )
            if row["top_pros"]:
                all_pros.extend(row["top_pros"])
            if row["top_cons"]:
                all_cons.extend(row["top_cons"])

        # Deduplicate pros/cons
        top_pros = list(dict.fromkeys(all_pros))[:5]
        top_cons = list(dict.fromkeys(all_cons))[:5]

        return {
            "company_name": company_name,
            "overall_rating": company["ratings"]["overall"],
            "review_count": company["stats"]["review_count"],
            "rating_trend": rating_trend,
            "top_pros": top_pros,
            "top_cons": top_cons,
        }

    def add_review_summary(
        self,
        company_name: str,
        period: str,
        avg_rating: float,
        review_count: int,
        top_pros: Optional[List[str]] = None,
        top_cons: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Add review summary for a period.

        Args:
            company_name: Company name
            period: Period string (e.g., "2025-Q4")
            avg_rating: Average rating for period
            review_count: Number of reviews
            top_pros: Top positive themes
            top_cons: Top negative themes

        Returns:
            Result dict
        """
        # Get or create company
        company = self.get_company(company_name)
        if not company:
            self.upsert_company({"company_name": company_name})

        # Get company ID
        id_query = text("""
            SELECT id FROM glassdoor_companies
            WHERE LOWER(company_name) = LOWER(:name)
        """)
        id_result = self.db.execute(id_query, {"name": company_name})
        company_id = id_result.scalar()

        insert_query = text("""
            INSERT INTO glassdoor_review_summaries (
                company_id, period, avg_rating, review_count, top_pros, top_cons
            ) VALUES (
                :company_id, :period, :avg_rating, :review_count, :top_pros, :top_cons
            )
            ON CONFLICT DO NOTHING
        """)

        self.db.execute(
            insert_query,
            {
                "company_id": company_id,
                "period": period,
                "avg_rating": avg_rating,
                "review_count": review_count,
                "top_pros": top_pros or [],
                "top_cons": top_cons or [],
            },
        )

        # Update total review count
        update_query = text("""
            UPDATE glassdoor_companies
            SET review_count = (
                SELECT COALESCE(SUM(review_count), 0)
                FROM glassdoor_review_summaries
                WHERE company_id = :company_id
            )
            WHERE id = :company_id
        """)
        self.db.execute(update_query, {"company_id": company_id})
        self.db.commit()

        return {"status": "added", "company_name": company_name, "period": period}

    def compare_companies(self, company_names: List[str]) -> Dict[str, Any]:
        """
        Compare multiple companies.

        Args:
            company_names: List of company names

        Returns:
            Comparison data
        """
        comparison = []

        for name in company_names:
            company = self.get_company(name)
            if company:
                comparison.append(
                    {
                        "company": name,
                        "overall": company["ratings"]["overall"],
                        "work_life_balance": company["ratings"]["work_life_balance"],
                        "compensation": company["ratings"]["compensation_benefits"],
                        "culture": company["ratings"]["culture_values"],
                        "career": company["ratings"]["career_opportunities"],
                        "management": company["ratings"]["senior_management"],
                        "ceo_approval": company["sentiment"]["ceo_approval"],
                        "recommend": company["sentiment"]["recommend_to_friend"],
                        "review_count": company["stats"]["review_count"],
                    }
                )
            else:
                comparison.append(
                    {
                        "company": name,
                        "error": "Not found",
                    }
                )

        # Sort by overall rating
        comparison.sort(key=lambda x: x.get("overall") or 0, reverse=True)

        return {
            "companies": company_names,
            "comparison": comparison,
        }

    def search_companies(
        self,
        query: Optional[str] = None,
        industry: Optional[str] = None,
        min_rating: Optional[float] = None,
        limit: int = 20,
        offset: int = 0,
    ) -> Dict[str, Any]:
        """
        Search companies in database.

        Args:
            query: Search query (company name)
            industry: Filter by industry
            min_rating: Minimum overall rating
            limit: Max results
            offset: Result offset

        Returns:
            Search results
        """
        conditions = ["1=1"]
        params = {"limit": limit, "offset": offset}

        if query:
            conditions.append("LOWER(company_name) LIKE LOWER(:query)")
            params["query"] = f"%{query}%"

        if industry:
            conditions.append("LOWER(industry) LIKE LOWER(:industry)")
            params["industry"] = f"%{industry}%"

        if min_rating:
            conditions.append("overall_rating >= :min_rating")
            params["min_rating"] = min_rating

        where_clause = " AND ".join(conditions)

        query_sql = text(f"""
            SELECT company_name, overall_rating, industry, company_size,
                   headquarters, review_count
            FROM glassdoor_companies
            WHERE {where_clause}
            ORDER BY overall_rating DESC NULLS LAST
            LIMIT :limit OFFSET :offset
        """)

        count_sql = text(f"""
            SELECT COUNT(*) FROM glassdoor_companies
            WHERE {where_clause}
        """)

        result = self.db.execute(query_sql, params)
        rows = result.mappings().fetchall()

        count_params = {k: v for k, v in params.items() if k not in ("limit", "offset")}
        count_result = self.db.execute(count_sql, count_params)
        total_count = count_result.scalar()

        results = []
        for row in rows:
            results.append(
                {
                    "company_name": row["company_name"],
                    "overall_rating": row["overall_rating"],
                    "industry": row["industry"],
                    "size": row["company_size"],
                    "headquarters": row["headquarters"],
                    "review_count": row["review_count"],
                }
            )

        return {
            "query": query,
            "filters": {"industry": industry, "min_rating": min_rating},
            "results": results,
            "total_count": total_count,
            "limit": limit,
            "offset": offset,
        }

    def get_rankings(
        self, metric: str = "overall", industry: Optional[str] = None, limit: int = 20
    ) -> Dict[str, Any]:
        """
        Get top-rated companies by metric.

        Args:
            metric: Rating metric (overall, compensation, culture, etc.)
            industry: Filter by industry
            limit: Number of results

        Returns:
            Rankings list
        """
        # Map metric names to columns
        metric_columns = {
            "overall": "overall_rating",
            "compensation": "compensation_benefits",
            "culture": "culture_values",
            "career": "career_opportunities",
            "work_life_balance": "work_life_balance",
            "management": "senior_management",
            "ceo_approval": "ceo_approval",
        }

        column = metric_columns.get(metric, "overall_rating")

        conditions = [f"{column} IS NOT NULL"]
        params = {"limit": limit}

        if industry:
            conditions.append("LOWER(industry) LIKE LOWER(:industry)")
            params["industry"] = f"%{industry}%"

        where_clause = " AND ".join(conditions)

        query = text(f"""
            SELECT company_name, overall_rating, {column} as metric_value,
                   industry, company_size, review_count
            FROM glassdoor_companies
            WHERE {where_clause}
            ORDER BY {column} DESC
            LIMIT :limit
        """)

        result = self.db.execute(query, params)
        rows = result.mappings().fetchall()

        rankings = []
        for i, row in enumerate(rows, 1):
            rankings.append(
                {
                    "rank": i,
                    "company_name": row["company_name"],
                    "metric_value": row["metric_value"],
                    "overall_rating": row["overall_rating"],
                    "industry": row["industry"],
                    "size": row["company_size"],
                    "review_count": row["review_count"],
                }
            )

        return {
            "metric": metric,
            "industry": industry,
            "rankings": rankings,
        }
