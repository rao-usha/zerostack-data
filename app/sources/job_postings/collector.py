"""
Job Posting Intelligence — collection orchestrator.

Coordinates ATS detection, job fetching, normalization, and storage.
"""

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, date
from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.sources.job_postings.metadata import (
    detect_seniority,
    normalize_employment_type,
    normalize_workplace_type,
    normalize_title,
)
from app.sources.job_postings.skills_extractor import extract_skills
from app.sources.job_postings.change_detector import JobPostingChangeDetector
from app.sources.job_postings.ats.detector import ATSDetector, ATSResult
from app.sources.job_postings.ats.greenhouse import GreenhouseClient
from app.sources.job_postings.ats.lever import LeverClient
from app.sources.job_postings.ats.ashby import AshbyClient
from app.sources.job_postings.ats.workday import WorkdayClient
from app.sources.job_postings.ats.generic import GenericJobScraper
from app.sources.job_postings.ats.smartrecruiters import SmartRecruitersClient

logger = logging.getLogger(__name__)


@dataclass
class CollectionResult:
    company_id: int
    company_name: str = ""
    ats_type: str = "unknown"
    total_fetched: int = 0
    new_postings: int = 0
    updated_postings: int = 0
    closed_postings: int = 0
    error: Optional[str] = None
    duration_seconds: float = 0.0


class JobPostingCollector:
    """Orchestrates job posting collection for a company."""

    def __init__(self):
        self._detector = ATSDetector()
        self._greenhouse = GreenhouseClient()
        self._lever = LeverClient()
        self._ashby = AshbyClient()
        self._workday = WorkdayClient()
        self._generic = GenericJobScraper()
        self._smartrecruiters = SmartRecruitersClient()

    async def close(self):
        await self._detector.close()
        await self._greenhouse.close()
        await self._lever.close()
        await self._ashby.close()
        await self._workday.close()
        await self._generic.close()
        await self._smartrecruiters.close()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        await self.close()

    # ------------------------------------------------------------------
    # Main entry points
    # ------------------------------------------------------------------

    async def collect_company(
        self, db: Session, company_id: int, force_rediscover: bool = False
    ) -> CollectionResult:
        """Collect job postings for a single company."""
        start = datetime.utcnow()
        result = CollectionResult(company_id=company_id)

        try:
            # Load company
            row = db.execute(
                text("SELECT id, name, website, careers_page_url FROM industrial_companies WHERE id = :id"),
                {"id": company_id},
            ).fetchone()
            if not row:
                result.error = f"Company {company_id} not found"
                return result

            company_name = row[1]
            website = row[2]
            careers_url = row[3]
            result.company_name = company_name

            # Phase 1: ATS detection (check cache or re-detect)
            ats_config = await self._get_or_detect_ats(
                db, company_id, company_name, website, careers_url, force_rediscover
            )
            result.ats_type = ats_config.ats_type

            if ats_config.ats_type == "unknown":
                result.error = ats_config.error or "Could not detect ATS"
                self._update_ats_config(db, company_id, ats_config, 0, "failed", result.error)
                return result

            # Phase 2: Fetch jobs via appropriate client
            raw_jobs = await self._fetch_jobs(ats_config)

            # If cached config returned 0 and we haven't re-detected, try fresh detection
            if not raw_jobs and not force_rediscover and ats_config.is_known:
                logger.info(f"Cached ATS config for {company_name} returned 0 jobs, re-detecting...")
                ats_config = await self._detector.detect(company_name, website, careers_url)
                result.ats_type = ats_config.ats_type
                if ats_config.ats_type not in ("unknown",):
                    raw_jobs = await self._fetch_jobs(ats_config)

            result.total_fetched = len(raw_jobs)
            logger.info(f"Fetched {len(raw_jobs)} jobs for {company_name} ({ats_config.ats_type})")

            if not raw_jobs:
                self._update_ats_config(db, company_id, ats_config, 0, "success", None)
                return result

            # Phase 3: Normalize
            normalized = self._normalize_all(raw_jobs, ats_config)

            # Phase 4: Upsert and track
            new_count, updated_count = self._upsert_postings(db, company_id, normalized, ats_config.ats_type)
            result.new_postings = new_count
            result.updated_postings = updated_count

            # Mark closed postings
            current_ids = {j["external_job_id"] for j in normalized if j.get("external_job_id")}
            closed_count = self._detect_closed_postings(db, company_id, current_ids)
            result.closed_postings = closed_count

            # Create daily snapshot
            self._create_snapshot(db, company_id)

            # Update ATS config
            self._update_ats_config(db, company_id, ats_config, len(raw_jobs), "success", None)

            db.commit()

        except Exception as e:
            logger.error(f"Collection failed for company {company_id}: {e}", exc_info=True)
            result.error = str(e)
            try:
                db.rollback()
            except Exception:
                pass

        result.duration_seconds = (datetime.utcnow() - start).total_seconds()
        return result

    async def collect_all(
        self, db: Session, limit: Optional[int] = None, skip_recent_hours: int = 24
    ) -> dict:
        """Collect for all companies with websites, skipping recently crawled."""
        rows = db.execute(
            text("""
                SELECT ic.id FROM industrial_companies ic
                WHERE ic.website IS NOT NULL
                AND NOT EXISTS (
                    SELECT 1 FROM company_ats_config cac
                    WHERE cac.company_id = ic.id
                    AND cac.last_crawled_at > NOW() - INTERVAL ':hours hours'
                )
                ORDER BY ic.id
                LIMIT :lim
            """.replace(":hours", str(int(skip_recent_hours)))),
            {"lim": limit or 10000},
        ).fetchall()

        results = []
        for (cid,) in rows:
            r = await self.collect_company(db, cid)
            results.append(r)
            logger.info(
                f"[{len(results)}/{len(rows)}] {r.company_name}: "
                f"{r.total_fetched} fetched, {r.new_postings} new, {r.error or 'OK'}"
            )

        summary = {
            "companies_processed": len(results),
            "total_fetched": sum(r.total_fetched for r in results),
            "total_new": sum(r.new_postings for r in results),
            "total_closed": sum(r.closed_postings for r in results),
            "errors": sum(1 for r in results if r.error),
        }
        return summary

    async def discover_ats(
        self, db: Session, company_id: int
    ) -> ATSResult:
        """Just discover ATS for a company (no job collection)."""
        row = db.execute(
            text("SELECT name, website, careers_page_url FROM industrial_companies WHERE id = :id"),
            {"id": company_id},
        ).fetchone()
        if not row:
            return ATSResult(ats_type="unknown", error="Company not found")

        result = await self._detector.detect(row[0], row[1], row[2])

        # Save to config table
        self._update_ats_config(
            db, company_id, result, 0,
            "pending" if result.ats_type != "unknown" else "failed",
            result.error,
        )
        db.commit()
        return result

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    async def _get_or_detect_ats(
        self,
        db: Session,
        company_id: int,
        company_name: str,
        website: Optional[str],
        careers_url: Optional[str],
        force_rediscover: bool,
    ) -> ATSResult:
        """Check cached ATS config or run detection."""
        if not force_rediscover:
            cached = db.execute(
                text("SELECT ats_type, board_token, careers_url, api_url FROM company_ats_config WHERE company_id = :cid"),
                {"cid": company_id},
            ).fetchone()
            if cached and cached[0] and cached[0] != "unknown":
                return ATSResult(
                    ats_type=cached[0],
                    board_token=cached[1],
                    careers_url=cached[2],
                    api_url=cached[3],
                )

        return await self._detector.detect(company_name, website, careers_url)

    async def _fetch_jobs(self, ats: ATSResult) -> list[dict]:
        """Dispatch to the right ATS client."""
        ats_type = ats.ats_type
        token = ats.board_token or ""

        if ats_type == "greenhouse" and token:
            return await self._greenhouse.fetch_jobs(token)
        elif ats_type == "lever" and token:
            return await self._lever.fetch_jobs(token)
        elif ats_type == "ashby" and token:
            return await self._ashby.fetch_jobs(token)
        elif ats_type == "smartrecruiters" and token:
            return await self._smartrecruiters.fetch_jobs(token)
        elif ats_type == "workday":
            # Token may be a full Workday URL if detected from HTML
            if token and "myworkdayjobs" in token:
                url = token  # Use token as URL when it's a full Workday URL
            else:
                url = ats.careers_url or ""
            return await self._workday.fetch_jobs(url, token if "myworkdayjobs" not in token else None)
        elif ats_type == "generic" and ats.careers_url:
            return await self._generic.fetch_jobs(ats.careers_url)
        else:
            logger.warning(f"No client for ATS type '{ats_type}' token='{token}'")
            return []

    def _normalize_all(self, raw_jobs: list[dict], ats: ATSResult) -> list[dict]:
        """Normalize all raw jobs through the appropriate normalizer."""
        ats_type = ats.ats_type
        token = ats.board_token or ""
        normalized = []

        for raw in raw_jobs:
            try:
                if ats_type == "greenhouse":
                    rec = self._greenhouse.normalize_job(raw, token)
                elif ats_type == "lever":
                    rec = self._lever.normalize_job(raw, token)
                elif ats_type == "ashby":
                    rec = self._ashby.normalize_job(raw, token)
                elif ats_type == "smartrecruiters":
                    rec = self._smartrecruiters.normalize_job(raw, token)
                elif ats_type == "workday":
                    rec = self._workday.normalize_job(raw, token)
                elif ats_type == "generic":
                    rec = raw  # already normalized by generic scraper
                else:
                    continue

                # Apply cross-ATS normalization
                if rec.get("title"):
                    rec["title_normalized"] = normalize_title(rec["title"])
                    rec["seniority_level"] = detect_seniority(rec["title"])
                if rec.get("employment_type"):
                    rec["employment_type"] = normalize_employment_type(rec["employment_type"])
                if rec.get("workplace_type"):
                    rec["workplace_type"] = normalize_workplace_type(rec["workplace_type"])

                # Extract skills from description
                if rec.get("description_text"):
                    reqs = extract_skills(rec["description_text"], rec.get("title", ""))
                    if reqs and reqs.get("skill_count", 0) > 0:
                        rec["requirements"] = reqs

                normalized.append(rec)
            except Exception as e:
                logger.debug(f"Failed to normalize job: {e}")
                continue

        return normalized

    def _upsert_postings(
        self, db: Session, company_id: int, jobs: list[dict], ats_type: str
    ) -> tuple[int, int]:
        """Upsert normalized postings into job_postings table. Returns (new, updated)."""
        new_count = 0
        updated_count = 0

        for job in jobs:
            ext_id = job.get("external_job_id", "")
            if not ext_id:
                continue

            # Ensure all expected keys exist
            for k in (
                "title", "title_normalized", "department", "team", "location",
                "employment_type", "workplace_type", "seniority_level",
                "salary_min", "salary_max", "salary_currency", "salary_interval",
                "description_text", "requirements", "source_url", "ats_type", "posted_date",
            ):
                job.setdefault(k, None)

            job["ats_type"] = job.get("ats_type") or ats_type

            # Check if exists
            existing = db.execute(
                text("SELECT id FROM job_postings WHERE company_id = :cid AND external_job_id = :eid"),
                {"cid": company_id, "eid": ext_id},
            ).fetchone()

            if existing:
                # Update last_seen and mutable fields
                req_json_upd = json.dumps(job.get("requirements")) if job.get("requirements") else None
                db.execute(
                    text("""
                        UPDATE job_postings SET
                            last_seen_at = NOW(),
                            title = COALESCE(:title, title),
                            title_normalized = COALESCE(:title_normalized, title_normalized),
                            department = COALESCE(:department, department),
                            team = COALESCE(:team, team),
                            location = COALESCE(:location, location),
                            employment_type = COALESCE(:employment_type, employment_type),
                            workplace_type = COALESCE(:workplace_type, workplace_type),
                            seniority_level = COALESCE(:seniority_level, seniority_level),
                            salary_min = COALESCE(:salary_min, salary_min),
                            salary_max = COALESCE(:salary_max, salary_max),
                            requirements = COALESCE(:requirements_json, requirements),
                            status = 'open',
                            closed_at = NULL
                        WHERE company_id = :cid AND external_job_id = :eid
                    """),
                    {**job, "cid": company_id, "eid": ext_id, "requirements_json": req_json_upd},
                )
                updated_count += 1
            else:
                # Insert new posting
                req_json = json.dumps(job.get("requirements")) if job.get("requirements") else None
                db.execute(
                    text("""
                        INSERT INTO job_postings (
                            company_id, external_job_id, title, title_normalized,
                            department, team, location, employment_type, workplace_type,
                            seniority_level, salary_min, salary_max, salary_currency,
                            salary_interval, description_text, requirements, source_url,
                            ats_type, status, first_seen_at, last_seen_at, posted_date
                        ) VALUES (
                            :cid, :eid, :title, :title_normalized,
                            :department, :team, :location, :employment_type, :workplace_type,
                            :seniority_level, :salary_min, :salary_max, :salary_currency,
                            :salary_interval, :description_text, :requirements, :source_url,
                            :ats_type, 'open', NOW(), NOW(), :posted_date
                        )
                    """),
                    {
                        "cid": company_id,
                        "eid": ext_id,
                        "title": job["title"],
                        "title_normalized": job.get("title_normalized"),
                        "department": job.get("department"),
                        "team": job.get("team"),
                        "location": job.get("location"),
                        "employment_type": job.get("employment_type"),
                        "workplace_type": job.get("workplace_type"),
                        "seniority_level": job.get("seniority_level"),
                        "salary_min": job.get("salary_min"),
                        "salary_max": job.get("salary_max"),
                        "salary_currency": job.get("salary_currency"),
                        "salary_interval": job.get("salary_interval"),
                        "description_text": job.get("description_text"),
                        "requirements": req_json,
                        "source_url": job.get("source_url"),
                        "ats_type": job.get("ats_type"),
                        "posted_date": job.get("posted_date"),
                    },
                )
                new_count += 1

        return new_count, updated_count

    def _detect_closed_postings(
        self, db: Session, company_id: int, current_job_ids: set[str]
    ) -> int:
        """Mark postings as closed if not in current crawl."""
        if not current_job_ids:
            return 0

        # Get all open postings for this company
        open_rows = db.execute(
            text("""
                SELECT external_job_id FROM job_postings
                WHERE company_id = :cid AND status = 'open'
            """),
            {"cid": company_id},
        ).fetchall()

        closed_count = 0
        for (eid,) in open_rows:
            if eid not in current_job_ids:
                db.execute(
                    text("""
                        UPDATE job_postings
                        SET status = 'closed', closed_at = NOW()
                        WHERE company_id = :cid AND external_job_id = :eid
                    """),
                    {"cid": company_id, "eid": eid},
                )
                closed_count += 1

        return closed_count

    def _create_snapshot(self, db: Session, company_id: int):
        """Create or update today's snapshot for a company."""
        today = date.today()

        stats = db.execute(
            text("""
                SELECT
                    COUNT(*) FILTER (WHERE status = 'open') as total_open,
                    COUNT(*) FILTER (WHERE status = 'open' AND DATE(first_seen_at) = :today) as new_today,
                    COUNT(*) FILTER (WHERE status = 'closed' AND DATE(closed_at) = :today) as closed_today
                FROM job_postings WHERE company_id = :cid
            """),
            {"cid": company_id, "today": today},
        ).fetchone()

        total_open = stats[0] or 0
        new_today = stats[1] or 0
        closed_today = stats[2] or 0

        # Department breakdown
        dept_rows = db.execute(
            text("""
                SELECT COALESCE(department, 'Unknown'), COUNT(*)
                FROM job_postings WHERE company_id = :cid AND status = 'open'
                GROUP BY department
            """),
            {"cid": company_id},
        ).fetchall()
        by_department = {r[0]: r[1] for r in dept_rows}

        # Location breakdown
        loc_rows = db.execute(
            text("""
                SELECT COALESCE(location, 'Unknown'), COUNT(*)
                FROM job_postings WHERE company_id = :cid AND status = 'open'
                GROUP BY location
            """),
            {"cid": company_id},
        ).fetchall()
        by_location = {r[0]: r[1] for r in loc_rows}

        # Seniority breakdown
        sen_rows = db.execute(
            text("""
                SELECT COALESCE(seniority_level, 'unknown'), COUNT(*)
                FROM job_postings WHERE company_id = :cid AND status = 'open'
                GROUP BY seniority_level
            """),
            {"cid": company_id},
        ).fetchall()
        by_seniority = {r[0]: r[1] for r in sen_rows}

        # Employment type breakdown
        emp_rows = db.execute(
            text("""
                SELECT COALESCE(employment_type, 'unknown'), COUNT(*)
                FROM job_postings WHERE company_id = :cid AND status = 'open'
                GROUP BY employment_type
            """),
            {"cid": company_id},
        ).fetchall()
        by_employment = {r[0]: r[1] for r in emp_rows}

        # Upsert snapshot
        db.execute(
            text("""
                INSERT INTO job_posting_snapshots (
                    company_id, snapshot_date, total_open, new_postings, closed_postings,
                    by_department, by_location, by_seniority, by_employment_type
                ) VALUES (
                    :cid, :sd, :total, :new, :closed,
                    :dept, :loc, :sen, :emp
                )
                ON CONFLICT (company_id, snapshot_date)
                DO UPDATE SET
                    total_open = EXCLUDED.total_open,
                    new_postings = EXCLUDED.new_postings,
                    closed_postings = EXCLUDED.closed_postings,
                    by_department = EXCLUDED.by_department,
                    by_location = EXCLUDED.by_location,
                    by_seniority = EXCLUDED.by_seniority,
                    by_employment_type = EXCLUDED.by_employment_type
            """),
            {
                "cid": company_id,
                "sd": today,
                "total": total_open,
                "new": new_today,
                "closed": closed_today,
                "dept": json.dumps(by_department),
                "loc": json.dumps(by_location),
                "sen": json.dumps(by_seniority),
                "emp": json.dumps(by_employment),
            },
        )

        # Run change detection to generate alerts
        try:
            detector = JobPostingChangeDetector()
            detector.detect(db, company_id, today)
        except Exception as e:
            logger.warning(f"Change detection failed for company {company_id}: {e}")

    def _update_ats_config(
        self,
        db: Session,
        company_id: int,
        ats: ATSResult,
        total_postings: int,
        crawl_status: str,
        error_message: Optional[str],
    ):
        """Upsert company_ats_config."""
        db.execute(
            text("""
                INSERT INTO company_ats_config (
                    company_id, ats_type, board_token, careers_url, api_url,
                    last_crawled_at, total_postings, crawl_status, error_message, updated_at
                ) VALUES (
                    :cid, :ats_type, :token, :careers_url, :api_url,
                    NOW(), :total, :status, :error, NOW()
                )
                ON CONFLICT (company_id)
                DO UPDATE SET
                    ats_type = EXCLUDED.ats_type,
                    board_token = COALESCE(EXCLUDED.board_token, company_ats_config.board_token),
                    careers_url = COALESCE(EXCLUDED.careers_url, company_ats_config.careers_url),
                    api_url = COALESCE(EXCLUDED.api_url, company_ats_config.api_url),
                    last_crawled_at = NOW(),
                    last_successful_crawl = CASE WHEN EXCLUDED.crawl_status = 'success' THEN NOW() ELSE company_ats_config.last_successful_crawl END,
                    total_postings = EXCLUDED.total_postings,
                    crawl_status = EXCLUDED.crawl_status,
                    error_message = EXCLUDED.error_message,
                    updated_at = NOW()
            """),
            {
                "cid": company_id,
                "ats_type": ats.ats_type,
                "token": ats.board_token,
                "careers_url": ats.careers_url,
                "api_url": ats.api_url,
                "total": total_postings,
                "status": crawl_status,
                "error": error_message,
            },
        )
