"""
Agentic Data Hunter (T45)

AI agent that autonomously finds and fills missing data in company records
by scanning for gaps, prioritizing by importance, and searching multiple sources.
"""

import logging
import uuid
import re
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
from enum import Enum

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


# =============================================================================
# CONSTANTS
# =============================================================================

class GapStatus(str, Enum):
    PENDING = "pending"
    HUNTING = "hunting"
    FILLED = "filled"
    UNFILLABLE = "unfillable"


class JobStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


# Field importance scores (0-1)
FIELD_IMPORTANCE = {
    "employee_count": 0.8,
    "total_funding": 0.9,
    "revenue": 0.85,
    "sector": 0.7,
    "industry": 0.7,
    "founding_date": 0.5,
    "headquarters": 0.4,
    "website": 0.3,
    "description": 0.3,
    "status": 0.6,
}

# Fields that can be hunted
HUNTABLE_FIELDS = [
    "employee_count",
    "total_funding",
    "revenue",
    "sector",
    "industry",
    "founding_date",
    "headquarters",
]

# Source configurations
SOURCES = {
    "glassdoor": {
        "fields": ["employee_count"],
        "base_reliability": 0.7,
    },
    "form_d": {
        "fields": ["total_funding", "sector"],
        "base_reliability": 0.8,
    },
    "sec_edgar": {
        "fields": ["revenue", "sector", "industry"],
        "base_reliability": 0.9,
    },
    "corporate_registry": {
        "fields": ["founding_date", "headquarters", "status"],
        "base_reliability": 0.75,
    },
    "news": {
        "fields": ["employee_count", "total_funding", "revenue"],
        "base_reliability": 0.5,
    },
    "enrichment": {
        "fields": ["sector", "industry", "employee_count"],
        "base_reliability": 0.6,
    },
}

# Maximum hunt attempts per gap
MAX_ATTEMPTS = 3


# =============================================================================
# DATA HUNTER AGENT
# =============================================================================

class DataHunterAgent:
    """
    AI agent that finds and fills missing data.

    Scans for incomplete records, prioritizes gaps, searches sources,
    validates data, and updates records with provenance tracking.
    """

    def __init__(self, db: Session):
        self.db = db
        self._ensure_tables()

    def _ensure_tables(self) -> None:
        """Create tables if they don't exist."""
        create_gaps = text("""
            CREATE TABLE IF NOT EXISTS data_gaps (
                id SERIAL PRIMARY KEY,
                entity_type VARCHAR(50) NOT NULL,
                entity_name VARCHAR(255) NOT NULL,
                entity_id INTEGER,
                field_name VARCHAR(100) NOT NULL,
                current_value TEXT,
                priority_score FLOAT,
                field_importance FLOAT,
                record_importance FLOAT,
                fill_likelihood FLOAT,
                status VARCHAR(20) DEFAULT 'pending',
                attempts INTEGER DEFAULT 0,
                last_attempt_at TIMESTAMP,
                filled_value TEXT,
                filled_source VARCHAR(100),
                confidence FLOAT,
                filled_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(entity_type, entity_name, field_name)
            )
        """)

        create_jobs = text("""
            CREATE TABLE IF NOT EXISTS hunt_jobs (
                id SERIAL PRIMARY KEY,
                job_id VARCHAR(50) UNIQUE NOT NULL,
                entity_type VARCHAR(50),
                field_filter TEXT[],
                limit_count INTEGER DEFAULT 100,
                status VARCHAR(20) DEFAULT 'pending',
                total_gaps INTEGER,
                processed INTEGER DEFAULT 0,
                filled INTEGER DEFAULT 0,
                failed INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                started_at TIMESTAMP,
                completed_at TIMESTAMP
            )
        """)

        create_reliability = text("""
            CREATE TABLE IF NOT EXISTS source_reliability (
                id SERIAL PRIMARY KEY,
                source_name VARCHAR(100) NOT NULL,
                field_name VARCHAR(100) NOT NULL,
                attempts INTEGER DEFAULT 0,
                successes INTEGER DEFAULT 0,
                success_rate FLOAT,
                avg_confidence FLOAT,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(source_name, field_name)
            )
        """)

        create_provenance = text("""
            CREATE TABLE IF NOT EXISTS data_provenance (
                id SERIAL PRIMARY KEY,
                entity_type VARCHAR(50) NOT NULL,
                entity_name VARCHAR(255) NOT NULL,
                field_name VARCHAR(100) NOT NULL,
                old_value TEXT,
                new_value TEXT,
                source VARCHAR(100),
                confidence FLOAT,
                hunt_job_id VARCHAR(50),
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        create_indexes = text("""
            CREATE INDEX IF NOT EXISTS idx_gaps_status ON data_gaps(status);
            CREATE INDEX IF NOT EXISTS idx_gaps_priority ON data_gaps(priority_score DESC);
            CREATE INDEX IF NOT EXISTS idx_gaps_entity ON data_gaps(entity_type, entity_name);
            CREATE INDEX IF NOT EXISTS idx_provenance_entity ON data_provenance(entity_type, entity_name);
        """)

        try:
            self.db.execute(create_gaps)
            self.db.execute(create_jobs)
            self.db.execute(create_reliability)
            self.db.execute(create_provenance)
            self.db.execute(create_indexes)
            self.db.commit()
        except Exception as e:
            logger.warning(f"Table creation warning: {e}")
            self.db.rollback()

    # -------------------------------------------------------------------------
    # GAP SCANNING
    # -------------------------------------------------------------------------

    def scan_for_gaps(self, entity_type: str = "company") -> Dict[str, Any]:
        """
        Scan database for records with missing fields.

        Returns summary of gaps found.
        """
        gaps_found = 0
        gaps_by_field = {}

        if entity_type == "company":
            gaps_found, gaps_by_field = self._scan_company_gaps()

        return {
            "scanned_records": gaps_found,
            "new_gaps_found": sum(gaps_by_field.values()),
            "gaps_by_field": gaps_by_field,
        }

    def _scan_company_gaps(self) -> Tuple[int, Dict[str, int]]:
        """Scan company_enrichment table for gaps."""
        gaps_by_field = {}
        total_scanned = 0

        try:
            # Get all enriched companies
            query = text("""
                SELECT company_name, employee_count, total_funding,
                       latest_revenue, sector, industry
                FROM company_enrichment
            """)
            result = self.db.execute(query)
            records = list(result.mappings())
            total_scanned = len(records)

            for record in records:
                company_name = record["company_name"]
                missing = self._identify_missing_fields(record)

                for field in missing:
                    created = self._create_or_update_gap(
                        entity_type="company",
                        entity_name=company_name,
                        field_name=field,
                    )
                    if created:
                        gaps_by_field[field] = gaps_by_field.get(field, 0) + 1

        except Exception as e:
            logger.error(f"Error scanning company gaps: {e}")
            self.db.rollback()

        return total_scanned, gaps_by_field

    def _identify_missing_fields(self, record: Dict) -> List[str]:
        """Identify which huntable fields are missing."""
        missing = []

        field_mapping = {
            "employee_count": "employee_count",
            "total_funding": "total_funding",
            "revenue": "latest_revenue",
            "sector": "sector",
            "industry": "industry",
        }

        for hunt_field, db_field in field_mapping.items():
            value = record.get(db_field)
            if value is None or value == "" or value == 0:
                missing.append(hunt_field)

        return missing

    def _create_or_update_gap(
        self,
        entity_type: str,
        entity_name: str,
        field_name: str,
    ) -> bool:
        """Create a gap record if it doesn't exist. Returns True if created."""
        # Check if gap already exists
        check_query = text("""
            SELECT id, status FROM data_gaps
            WHERE entity_type = :type AND entity_name = :name AND field_name = :field
        """)
        existing = self.db.execute(check_query, {
            "type": entity_type,
            "name": entity_name,
            "field": field_name,
        }).fetchone()

        if existing:
            # Don't recreate filled gaps
            if existing[1] == GapStatus.FILLED.value:
                return False
            return False  # Already exists

        # Calculate priority
        priority = self._calculate_priority(entity_name, field_name)

        # Insert new gap
        insert_query = text("""
            INSERT INTO data_gaps
            (entity_type, entity_name, field_name, priority_score,
             field_importance, record_importance, fill_likelihood, status)
            VALUES
            (:type, :name, :field, :priority,
             :field_imp, :record_imp, :fill_like, 'pending')
        """)

        try:
            self.db.execute(insert_query, {
                "type": entity_type,
                "name": entity_name,
                "field": field_name,
                "priority": priority["score"],
                "field_imp": priority["field_importance"],
                "record_imp": priority["record_importance"],
                "fill_like": priority["fill_likelihood"],
            })
            self.db.commit()
            return True
        except Exception as e:
            logger.debug(f"Gap already exists or error: {e}")
            self.db.rollback()
            return False

    def _calculate_priority(self, entity_name: str, field_name: str) -> Dict:
        """Calculate priority score for a gap."""
        # Field importance
        field_imp = FIELD_IMPORTANCE.get(field_name, 0.5)

        # Record importance (based on watchlists, recent activity)
        record_imp = self._get_record_importance(entity_name)

        # Fill likelihood (based on source availability)
        fill_like = self._get_fill_likelihood(field_name)

        score = (field_imp * 0.4) + (record_imp * 0.3) + (fill_like * 0.3)

        return {
            "score": round(score, 3),
            "field_importance": field_imp,
            "record_importance": record_imp,
            "fill_likelihood": fill_like,
        }

    def _get_record_importance(self, entity_name: str) -> float:
        """Get importance score for an entity."""
        importance = 0.5  # Base score

        try:
            # Check if in watchlists
            query = text("""
                SELECT COUNT(*) FROM watchlist_items
                WHERE LOWER(entity_name) LIKE LOWER(:pattern)
            """)
            result = self.db.execute(query, {"pattern": f"%{entity_name}%"})
            count = result.scalar() or 0
            if count > 0:
                importance += 0.2

            # Check recent activity (deals, research)
            query = text("""
                SELECT COUNT(*) FROM research_jobs
                WHERE LOWER(company_input) LIKE LOWER(:pattern)
                  AND created_at > NOW() - INTERVAL '30 days'
            """)
            result = self.db.execute(query, {"pattern": f"%{entity_name}%"})
            count = result.scalar() or 0
            if count > 0:
                importance += 0.15

        except Exception as e:
            logger.debug("Failed to check research history for %s: %s", entity_name, e)

        return min(importance, 1.0)

    def _get_fill_likelihood(self, field_name: str) -> float:
        """Get likelihood of filling a field based on source availability."""
        # Count sources that can provide this field
        available_sources = 0
        total_reliability = 0.0

        for source, config in SOURCES.items():
            if field_name in config["fields"]:
                available_sources += 1
                total_reliability += config["base_reliability"]

        if available_sources == 0:
            return 0.2

        return min(total_reliability / available_sources + 0.1 * available_sources, 1.0)

    # -------------------------------------------------------------------------
    # HUNTING
    # -------------------------------------------------------------------------

    def hunt_gap(self, gap_id: int, job_id: str = None) -> Dict[str, Any]:
        """
        Attempt to fill a single data gap.

        Returns result with status and value if found.
        """
        # Get gap details
        query = text("SELECT * FROM data_gaps WHERE id = :id")
        result = self.db.execute(query, {"id": gap_id})
        gap = result.mappings().fetchone()

        if not gap:
            return {"status": "error", "message": "Gap not found"}

        if gap["status"] == GapStatus.FILLED.value:
            return {"status": "already_filled", "value": gap["filled_value"]}

        if gap["attempts"] >= MAX_ATTEMPTS:
            self._mark_unfillable(gap_id)
            return {"status": "max_attempts", "message": "Maximum attempts reached"}

        # Mark as hunting
        self._update_gap_status(gap_id, GapStatus.HUNTING)

        entity_name = gap["entity_name"]
        field_name = gap["field_name"]

        # Get best sources for this field
        sources = self._get_best_sources(field_name)

        found_value = None
        found_source = None
        confidence = 0.0

        # Try each source
        for source in sources:
            try:
                value = self._search_source(source, entity_name, field_name)
                if value is not None:
                    # Validate the value
                    is_valid, conf = self._validate_value(value, field_name)
                    if is_valid and conf > confidence:
                        found_value = value
                        found_source = source
                        confidence = conf

                        # Update source reliability
                        self._update_source_reliability(source, field_name, True, conf)

                        if confidence >= 0.8:
                            break  # Good enough
                else:
                    self._update_source_reliability(source, field_name, False, 0)

            except Exception as e:
                logger.debug(f"Error searching {source} for {entity_name}.{field_name}: {e}")
                self._update_source_reliability(source, field_name, False, 0)

        # Update gap
        self._increment_attempts(gap_id)

        if found_value is not None:
            self._fill_gap(
                gap_id=gap_id,
                value=found_value,
                source=found_source,
                confidence=confidence,
                job_id=job_id,
            )
            return {
                "status": "filled",
                "entity": entity_name,
                "field": field_name,
                "value": found_value,
                "source": found_source,
                "confidence": confidence,
            }
        else:
            self._update_gap_status(gap_id, GapStatus.PENDING)
            return {
                "status": "not_found",
                "entity": entity_name,
                "field": field_name,
                "attempts": gap["attempts"] + 1,
            }

    def _get_best_sources(self, field_name: str) -> List[str]:
        """Get sources sorted by reliability for a field."""
        sources_for_field = []

        for source, config in SOURCES.items():
            if field_name in config["fields"]:
                # Get actual reliability if tracked
                reliability = self._get_source_reliability(source, field_name)
                if reliability is None:
                    reliability = config["base_reliability"]
                sources_for_field.append((source, reliability))

        # Sort by reliability descending
        sources_for_field.sort(key=lambda x: x[1], reverse=True)
        return [s[0] for s in sources_for_field]

    def _get_source_reliability(self, source: str, field: str) -> Optional[float]:
        """Get tracked reliability for a source/field combo."""
        try:
            query = text("""
                SELECT success_rate FROM source_reliability
                WHERE source_name = :source AND field_name = :field
            """)
            result = self.db.execute(query, {"source": source, "field": field})
            row = result.fetchone()
            return row[0] if row else None
        except Exception:
            return None

    def _search_source(
        self,
        source: str,
        entity_name: str,
        field_name: str
    ) -> Optional[Any]:
        """Search a specific source for data."""
        if source == "glassdoor":
            return self._search_glassdoor(entity_name, field_name)
        elif source == "form_d":
            return self._search_form_d(entity_name, field_name)
        elif source == "sec_edgar":
            return self._search_sec(entity_name, field_name)
        elif source == "corporate_registry":
            return self._search_registry(entity_name, field_name)
        elif source == "news":
            return self._search_news(entity_name, field_name)
        elif source == "enrichment":
            return self._search_enrichment(entity_name, field_name)
        return None

    def _search_glassdoor(self, entity: str, field: str) -> Optional[Any]:
        """Search Glassdoor for data."""
        if field != "employee_count":
            return None

        try:
            query = text("""
                SELECT review_count FROM glassdoor_ratings
                WHERE LOWER(company_name) LIKE LOWER(:pattern)
                ORDER BY fetched_at DESC
                LIMIT 1
            """)
            result = self.db.execute(query, {"pattern": f"%{entity}%"})
            row = result.fetchone()
            # Review count can be a proxy for employee size
            if row and row[0]:
                # Rough estimate: reviews ~ employees/10
                return int(row[0] * 10)
        except Exception as e:
            logger.debug(f"Glassdoor search error: {e}")
            self.db.rollback()
        return None

    def _search_form_d(self, entity: str, field: str) -> Optional[Any]:
        """Search Form D filings for data."""
        try:
            query = text("""
                SELECT total_amount_sold, industry_group
                FROM form_d_filings
                WHERE LOWER(issuer_name) LIKE LOWER(:pattern)
                ORDER BY date_of_first_sale DESC
                LIMIT 1
            """)
            result = self.db.execute(query, {"pattern": f"%{entity}%"})
            row = result.fetchone()

            if row:
                if field == "total_funding" and row[0]:
                    return float(row[0])
                if field == "sector" and row[1]:
                    return row[1]
        except Exception as e:
            logger.debug(f"Form D search error: {e}")
            self.db.rollback()
        return None

    def _search_sec(self, entity: str, field: str) -> Optional[Any]:
        """Search SEC filings for data."""
        try:
            query = text("""
                SELECT latest_revenue, sector, industry
                FROM company_enrichment
                WHERE LOWER(company_name) LIKE LOWER(:pattern)
                  AND (latest_revenue IS NOT NULL OR sector IS NOT NULL)
                LIMIT 1
            """)
            result = self.db.execute(query, {"pattern": f"%{entity}%"})
            row = result.fetchone()

            if row:
                if field == "revenue" and row[0]:
                    return float(row[0])
                if field == "sector" and row[1]:
                    return row[1]
                if field == "industry" and row[2]:
                    return row[2]
        except Exception as e:
            logger.debug(f"SEC search error: {e}")
            self.db.rollback()
        return None

    def _search_registry(self, entity: str, field: str) -> Optional[Any]:
        """Search corporate registry for data."""
        try:
            query = text("""
                SELECT incorporation_date, jurisdiction, company_status
                FROM corporate_registry
                WHERE LOWER(company_name) LIKE LOWER(:pattern)
                LIMIT 1
            """)
            result = self.db.execute(query, {"pattern": f"%{entity}%"})
            row = result.fetchone()

            if row:
                if field == "founding_date" and row[0]:
                    return row[0].isoformat() if hasattr(row[0], 'isoformat') else str(row[0])
                if field == "headquarters" and row[1]:
                    return row[1]
                if field == "status" and row[2]:
                    return row[2]
        except Exception as e:
            logger.debug(f"Registry search error: {e}")
            self.db.rollback()
        return None

    def _search_news(self, entity: str, field: str) -> Optional[Any]:
        """Search news for data mentions."""
        try:
            query = text("""
                SELECT news_title FROM news_matches
                WHERE LOWER(watch_value) LIKE LOWER(:pattern)
                   OR LOWER(news_title) LIKE LOWER(:pattern)
                ORDER BY created_at DESC
                LIMIT 10
            """)
            result = self.db.execute(query, {"pattern": f"%{entity}%"})
            rows = result.fetchall()

            for row in rows:
                title = row[0] if row else ""

                if field == "employee_count":
                    # Look for employee mentions
                    match = re.search(r'(\d+[,\d]*)\s*employees', title, re.I)
                    if match:
                        return int(match.group(1).replace(",", ""))

                if field == "total_funding":
                    # Look for funding mentions
                    match = re.search(r'\$(\d+(?:\.\d+)?)\s*(M|B|million|billion)', title, re.I)
                    if match:
                        amount = float(match.group(1))
                        unit = match.group(2).upper()
                        if unit in ["M", "MILLION"]:
                            return amount * 1_000_000
                        elif unit in ["B", "BILLION"]:
                            return amount * 1_000_000_000
                        return amount

        except Exception as e:
            logger.debug(f"News search error: {e}")
            self.db.rollback()
        return None

    def _search_enrichment(self, entity: str, field: str) -> Optional[Any]:
        """Search enrichment cache for data."""
        try:
            query = text("""
                SELECT sector, industry, employee_count
                FROM company_enrichment
                WHERE LOWER(company_name) LIKE LOWER(:pattern)
                LIMIT 1
            """)
            result = self.db.execute(query, {"pattern": f"%{entity}%"})
            row = result.fetchone()

            if row:
                if field == "sector" and row[0]:
                    return row[0]
                if field == "industry" and row[1]:
                    return row[1]
                if field == "employee_count" and row[2]:
                    return int(row[2])
        except Exception as e:
            logger.debug(f"Enrichment search error: {e}")
            self.db.rollback()
        return None

    def _validate_value(self, value: Any, field: str) -> Tuple[bool, float]:
        """Validate a found value. Returns (is_valid, confidence)."""
        if value is None:
            return False, 0.0

        confidence = 0.7  # Base confidence

        if field == "employee_count":
            if isinstance(value, (int, float)) and 1 <= value <= 10_000_000:
                confidence = 0.8 if value > 10 else 0.6
                return True, confidence
            return False, 0.0

        if field == "total_funding":
            if isinstance(value, (int, float)) and value > 0:
                confidence = 0.85
                return True, confidence
            return False, 0.0

        if field == "revenue":
            if isinstance(value, (int, float)) and value > 0:
                confidence = 0.9  # SEC data is reliable
                return True, confidence
            return False, 0.0

        if field in ["sector", "industry"]:
            if isinstance(value, str) and len(value) > 2:
                return True, 0.75
            return False, 0.0

        if field == "founding_date":
            if value:
                return True, 0.8
            return False, 0.0

        # Default validation
        if value:
            return True, 0.6

        return False, 0.0

    # -------------------------------------------------------------------------
    # GAP UPDATES
    # -------------------------------------------------------------------------

    def _update_gap_status(self, gap_id: int, status: GapStatus) -> None:
        """Update gap status."""
        query = text("UPDATE data_gaps SET status = :status WHERE id = :id")
        self.db.execute(query, {"status": status.value, "id": gap_id})
        self.db.commit()

    def _increment_attempts(self, gap_id: int) -> None:
        """Increment attempt count for a gap."""
        query = text("""
            UPDATE data_gaps
            SET attempts = attempts + 1, last_attempt_at = NOW()
            WHERE id = :id
        """)
        self.db.execute(query, {"id": gap_id})
        self.db.commit()

    def _mark_unfillable(self, gap_id: int) -> None:
        """Mark a gap as unfillable."""
        self._update_gap_status(gap_id, GapStatus.UNFILLABLE)

    def _fill_gap(
        self,
        gap_id: int,
        value: Any,
        source: str,
        confidence: float,
        job_id: str = None,
    ) -> None:
        """Fill a gap with found data."""
        # Get current gap info for provenance
        query = text("SELECT entity_type, entity_name, field_name FROM data_gaps WHERE id = :id")
        result = self.db.execute(query, {"id": gap_id})
        gap = result.fetchone()

        # Update gap
        update_query = text("""
            UPDATE data_gaps SET
                status = 'filled',
                filled_value = :value,
                filled_source = :source,
                confidence = :confidence,
                filled_at = NOW()
            WHERE id = :id
        """)
        self.db.execute(update_query, {
            "value": str(value),
            "source": source,
            "confidence": confidence,
            "id": gap_id,
        })

        # Log provenance
        if gap:
            self._log_provenance(
                entity_type=gap[0],
                entity_name=gap[1],
                field_name=gap[2],
                old_value=None,
                new_value=str(value),
                source=source,
                confidence=confidence,
                job_id=job_id,
            )

            # Update the actual record
            self._update_entity_record(
                entity_type=gap[0],
                entity_name=gap[1],
                field_name=gap[2],
                value=value,
            )

        self.db.commit()

    def _update_entity_record(
        self,
        entity_type: str,
        entity_name: str,
        field_name: str,
        value: Any,
    ) -> None:
        """Update the actual entity record with the found value."""
        if entity_type != "company":
            return

        # Map hunt field to database column
        field_mapping = {
            "employee_count": "employee_count",
            "total_funding": "total_funding",
            "revenue": "latest_revenue",
            "sector": "sector",
            "industry": "industry",
        }

        db_field = field_mapping.get(field_name)
        if not db_field:
            return

        try:
            query = text(f"""
                UPDATE company_enrichment
                SET {db_field} = :value
                WHERE LOWER(company_name) = LOWER(:name)
            """)
            self.db.execute(query, {"value": value, "name": entity_name})
        except Exception as e:
            logger.warning(f"Error updating entity record: {e}")
            self.db.rollback()

    def _log_provenance(
        self,
        entity_type: str,
        entity_name: str,
        field_name: str,
        old_value: Any,
        new_value: Any,
        source: str,
        confidence: float,
        job_id: str = None,
    ) -> None:
        """Log data provenance for audit trail."""
        query = text("""
            INSERT INTO data_provenance
            (entity_type, entity_name, field_name, old_value, new_value,
             source, confidence, hunt_job_id)
            VALUES
            (:type, :name, :field, :old, :new, :source, :conf, :job_id)
        """)
        try:
            self.db.execute(query, {
                "type": entity_type,
                "name": entity_name,
                "field": field_name,
                "old": str(old_value) if old_value else None,
                "new": str(new_value),
                "source": source,
                "conf": confidence,
                "job_id": job_id,
            })
        except Exception as e:
            logger.warning(f"Error logging provenance: {e}")

    def _update_source_reliability(
        self,
        source: str,
        field: str,
        success: bool,
        confidence: float = 0.0,
    ) -> None:
        """Update source reliability tracking."""
        try:
            # Upsert reliability record
            query = text("""
                INSERT INTO source_reliability (source_name, field_name, attempts, successes, avg_confidence)
                VALUES (:source, :field, 1, :success, :conf)
                ON CONFLICT (source_name, field_name) DO UPDATE SET
                    attempts = source_reliability.attempts + 1,
                    successes = source_reliability.successes + :success,
                    success_rate = (source_reliability.successes + :success)::float / (source_reliability.attempts + 1),
                    avg_confidence = CASE
                        WHEN :success = 1 THEN
                            (COALESCE(source_reliability.avg_confidence, 0) * source_reliability.successes + :conf) /
                            (source_reliability.successes + 1)
                        ELSE source_reliability.avg_confidence
                    END,
                    last_updated = NOW()
            """)
            self.db.execute(query, {
                "source": source,
                "field": field,
                "success": 1 if success else 0,
                "conf": confidence,
            })
            self.db.commit()
        except Exception as e:
            logger.debug(f"Error updating reliability: {e}")
            self.db.rollback()

    # -------------------------------------------------------------------------
    # HUNT JOBS
    # -------------------------------------------------------------------------

    def start_hunt_job(
        self,
        entity_type: str = None,
        fields: List[str] = None,
        limit: int = 50,
        min_priority: float = 0.0,
    ) -> Dict[str, Any]:
        """Start a new hunt job."""
        job_id = f"hunt_{uuid.uuid4().hex[:12]}"

        # Build query for gaps
        where_clauses = ["status = 'pending'", "attempts < :max_attempts"]
        params = {"max_attempts": MAX_ATTEMPTS, "limit": limit}

        if entity_type:
            where_clauses.append("entity_type = :entity_type")
            params["entity_type"] = entity_type

        if fields:
            where_clauses.append("field_name = ANY(:fields)")
            params["fields"] = fields

        if min_priority > 0:
            where_clauses.append("priority_score >= :min_priority")
            params["min_priority"] = min_priority

        where_sql = " AND ".join(where_clauses)

        # Count total gaps
        count_query = text(f"SELECT COUNT(*) FROM data_gaps WHERE {where_sql}")
        total = self.db.execute(count_query, params).scalar() or 0

        # Create job record
        insert_query = text("""
            INSERT INTO hunt_jobs
            (job_id, entity_type, field_filter, limit_count, status, total_gaps)
            VALUES
            (:job_id, :entity_type, :fields, :limit, 'pending', :total)
        """)
        self.db.execute(insert_query, {
            "job_id": job_id,
            "entity_type": entity_type,
            "fields": fields,
            "limit": limit,
            "total": min(total, limit),
        })
        self.db.commit()

        return {
            "job_id": job_id,
            "status": "pending",
            "total_gaps": min(total, limit),
        }

    def process_hunt_job(self, job_id: str) -> Dict[str, Any]:
        """Process a hunt job."""
        # Get job
        query = text("SELECT * FROM hunt_jobs WHERE job_id = :id")
        result = self.db.execute(query, {"id": job_id})
        job = result.mappings().fetchone()

        if not job:
            return {"status": "error", "message": "Job not found"}

        # Update status to running
        self.db.execute(
            text("UPDATE hunt_jobs SET status = 'running', started_at = NOW() WHERE job_id = :id"),
            {"id": job_id}
        )
        self.db.commit()

        # Build gap query
        where_clauses = ["status = 'pending'", "attempts < :max_attempts"]
        params = {"max_attempts": MAX_ATTEMPTS, "limit": job["limit_count"]}

        if job["entity_type"]:
            where_clauses.append("entity_type = :entity_type")
            params["entity_type"] = job["entity_type"]

        if job["field_filter"]:
            where_clauses.append("field_name = ANY(:fields)")
            params["fields"] = job["field_filter"]

        where_sql = " AND ".join(where_clauses)

        # Get gaps to process
        gaps_query = text(f"""
            SELECT id FROM data_gaps
            WHERE {where_sql}
            ORDER BY priority_score DESC
            LIMIT :limit
        """)
        gaps = self.db.execute(gaps_query, params).fetchall()

        results = []
        filled = 0
        failed = 0

        for gap_row in gaps:
            gap_id = gap_row[0]
            result = self.hunt_gap(gap_id, job_id)
            results.append(result)

            if result["status"] == "filled":
                filled += 1
            else:
                failed += 1

            # Update job progress
            self.db.execute(
                text("""
                    UPDATE hunt_jobs SET
                        processed = processed + 1,
                        filled = :filled,
                        failed = :failed
                    WHERE job_id = :id
                """),
                {"filled": filled, "failed": failed, "id": job_id}
            )
            self.db.commit()

        # Mark job complete
        self.db.execute(
            text("UPDATE hunt_jobs SET status = 'completed', completed_at = NOW() WHERE job_id = :id"),
            {"id": job_id}
        )
        self.db.commit()

        return {
            "job_id": job_id,
            "status": "completed",
            "total_gaps": len(gaps),
            "processed": len(gaps),
            "filled": filled,
            "failed": failed,
            "fill_rate": filled / len(gaps) if gaps else 0,
            "results": results,
        }

    def get_job_status(self, job_id: str) -> Optional[Dict]:
        """Get hunt job status."""
        query = text("SELECT * FROM hunt_jobs WHERE job_id = :id")
        result = self.db.execute(query, {"id": job_id})
        job = result.mappings().fetchone()

        if not job:
            return None

        duration = None
        if job["started_at"] and job["completed_at"]:
            duration = (job["completed_at"] - job["started_at"]).total_seconds()

        return {
            "job_id": job["job_id"],
            "status": job["status"],
            "total_gaps": job["total_gaps"],
            "processed": job["processed"],
            "filled": job["filled"],
            "failed": job["failed"],
            "fill_rate": job["filled"] / job["processed"] if job["processed"] > 0 else 0,
            "duration_seconds": duration,
            "created_at": job["created_at"].isoformat() if job["created_at"] else None,
        }

    # -------------------------------------------------------------------------
    # QUEUE & STATS
    # -------------------------------------------------------------------------

    def get_gap_queue(
        self,
        entity_type: str = None,
        field: str = None,
        status: str = None,
        min_priority: float = 0.0,
        limit: int = 50,
        offset: int = 0,
    ) -> Dict[str, Any]:
        """Get the gap queue."""
        where_clauses = []
        params = {"limit": limit, "offset": offset}

        if entity_type:
            where_clauses.append("entity_type = :entity_type")
            params["entity_type"] = entity_type

        if field:
            where_clauses.append("field_name = :field")
            params["field"] = field

        if status:
            where_clauses.append("status = :status")
            params["status"] = status

        if min_priority > 0:
            where_clauses.append("priority_score >= :min_priority")
            params["min_priority"] = min_priority

        where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"

        # Get gaps
        query = text(f"""
            SELECT id, entity_type, entity_name, field_name,
                   priority_score, status, attempts
            FROM data_gaps
            WHERE {where_sql}
            ORDER BY priority_score DESC
            LIMIT :limit OFFSET :offset
        """)
        result = self.db.execute(query, params)
        gaps = [dict(row) for row in result.mappings()]

        # Get total count
        count_query = text(f"SELECT COUNT(*) FROM data_gaps WHERE {where_sql}")
        total = self.db.execute(count_query, params).scalar() or 0

        # Get counts by field
        by_field_query = text("""
            SELECT field_name, COUNT(*) as count
            FROM data_gaps
            WHERE status = 'pending'
            GROUP BY field_name
        """)
        by_field = {row[0]: row[1] for row in self.db.execute(by_field_query)}

        # Get counts by status
        by_status_query = text("""
            SELECT status, COUNT(*) as count
            FROM data_gaps
            GROUP BY status
        """)
        by_status = {row[0]: row[1] for row in self.db.execute(by_status_query)}

        return {
            "gaps": gaps,
            "total": total,
            "by_field": by_field,
            "by_status": by_status,
        }

    def get_stats(self) -> Dict[str, Any]:
        """Get hunting statistics."""
        try:
            # Total gaps by status
            status_query = text("""
                SELECT status, COUNT(*) as count
                FROM data_gaps
                GROUP BY status
            """)
            status_counts = {row[0]: row[1] for row in self.db.execute(status_query)}

            total = sum(status_counts.values())
            filled = status_counts.get("filled", 0)
            fill_rate = filled / total if total > 0 else 0

            # By field
            field_query = text("""
                SELECT field_name,
                       COUNT(*) as total,
                       SUM(CASE WHEN status = 'filled' THEN 1 ELSE 0 END) as filled
                FROM data_gaps
                GROUP BY field_name
            """)
            by_field = {}
            for row in self.db.execute(field_query):
                by_field[row[0]] = {
                    "total": row[1],
                    "filled": row[2],
                    "rate": row[2] / row[1] if row[1] > 0 else 0,
                }

            # Source performance
            source_query = text("""
                SELECT source_name, attempts, success_rate, avg_confidence
                FROM source_reliability
                ORDER BY success_rate DESC
            """)
            sources = [
                {
                    "source": row[0],
                    "attempts": row[1],
                    "success_rate": row[2] or 0,
                    "avg_confidence": row[3] or 0,
                }
                for row in self.db.execute(source_query)
            ]

            # Recent fills
            recent_query = text("""
                SELECT entity_name, field_name, filled_value, filled_source, filled_at
                FROM data_gaps
                WHERE status = 'filled'
                ORDER BY filled_at DESC
                LIMIT 10
            """)
            recent = [
                {
                    "entity": row[0],
                    "field": row[1],
                    "value": row[2],
                    "source": row[3],
                }
                for row in self.db.execute(recent_query)
            ]

            return {
                "total_gaps": total,
                "gaps_by_status": status_counts,
                "fill_rate": round(fill_rate, 3),
                "by_field": by_field,
                "source_performance": sources,
                "recent_fills": recent,
            }

        except Exception as e:
            logger.error(f"Error getting stats: {e}")
            self.db.rollback()
            return {"error": str(e)}

    def hunt_entity(self, entity_name: str) -> Dict[str, Any]:
        """Hunt all missing data for a specific entity."""
        # First scan for gaps on this entity
        gaps_created = 0

        try:
            query = text("""
                SELECT company_name, employee_count, total_funding,
                       latest_revenue, sector, industry
                FROM company_enrichment
                WHERE LOWER(company_name) LIKE LOWER(:pattern)
                LIMIT 1
            """)
            result = self.db.execute(query, {"pattern": f"%{entity_name}%"})
            record = result.mappings().fetchone()

            if record:
                missing = self._identify_missing_fields(dict(record))
                for field in missing:
                    if self._create_or_update_gap("company", entity_name, field):
                        gaps_created += 1

        except Exception as e:
            logger.error(f"Error scanning entity: {e}")
            self.db.rollback()

        # Get gaps for this entity
        query = text("""
            SELECT id FROM data_gaps
            WHERE LOWER(entity_name) LIKE LOWER(:pattern)
              AND status = 'pending'
        """)
        gaps = self.db.execute(query, {"pattern": f"%{entity_name}%"}).fetchall()

        results = []
        filled = 0

        for gap_row in gaps:
            result = self.hunt_gap(gap_row[0])
            results.append(result)
            if result["status"] == "filled":
                filled += 1

        return {
            "entity": entity_name,
            "gaps_found": len(gaps),
            "gaps_filled": filled,
            "results": results,
        }

    def get_provenance(
        self,
        entity_name: str,
        field: str = None,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """Get data provenance (audit trail) for an entity."""
        try:
            where_clauses = ["LOWER(entity_name) LIKE LOWER(:pattern)"]
            params = {"pattern": f"%{entity_name}%", "limit": limit}

            if field:
                where_clauses.append("field_name = :field")
                params["field"] = field

            where_sql = " AND ".join(where_clauses)

            query = text(f"""
                SELECT entity_type, entity_name, field_name,
                       old_value, new_value, source, confidence,
                       hunt_job_id, updated_at
                FROM data_provenance
                WHERE {where_sql}
                ORDER BY updated_at DESC
                LIMIT :limit
            """)
            result = self.db.execute(query, params)
            return [
                {
                    "entity_type": row[0],
                    "entity_name": row[1],
                    "field_name": row[2],
                    "old_value": row[3],
                    "new_value": row[4],
                    "source": row[5],
                    "confidence": row[6],
                    "job_id": row[7],
                    "updated_at": row[8].isoformat() if row[8] else None,
                }
                for row in result
            ]
        except Exception as e:
            logger.error(f"Error getting provenance: {e}")
            self.db.rollback()
            return []

    def get_source_reliability(self) -> List[Dict[str, Any]]:
        """Get reliability scores for all data sources."""
        try:
            query = text("""
                SELECT source_name, field_name, attempts, successes,
                       success_rate, avg_confidence, last_updated
                FROM source_reliability
                ORDER BY source_name, field_name
            """)
            result = self.db.execute(query)
            return [
                {
                    "source": row[0],
                    "field": row[1],
                    "attempts": row[2],
                    "successes": row[3],
                    "success_rate": row[4] or 0,
                    "avg_confidence": row[5] or 0,
                    "last_updated": row[6].isoformat() if row[6] else None,
                }
                for row in result
            ]
        except Exception as e:
            logger.error(f"Error getting source reliability: {e}")
            self.db.rollback()
            return []
