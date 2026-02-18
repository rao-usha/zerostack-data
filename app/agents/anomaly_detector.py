"""
Agentic Anomaly Detector (T46).

AI agent that detects unusual patterns and changes in company data,
correlates anomalies across data sources, assigns severity scores,
and provides proactive alerts.
"""

import logging
import uuid
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


# =============================================================================
# ENUMS AND CONSTANTS
# =============================================================================


class SeverityLevel(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class AnomalyStatus(str, Enum):
    NEW = "new"
    ACKNOWLEDGED = "acknowledged"
    INVESTIGATING = "investigating"
    RESOLVED = "resolved"


class AnomalyType(str, Enum):
    SCORE_DROP = "score_drop"
    SCORE_SPIKE = "score_spike"
    EMPLOYEE_CHANGE = "employee_change"
    FUNDING_SPIKE = "funding_spike"
    TRAFFIC_ANOMALY = "traffic_anomaly"
    RATING_DROP = "rating_drop"
    GITHUB_STALL = "github_stall"
    STATUS_CHANGE = "status_change"


# Base severity scores for each anomaly type
BASE_SEVERITY = {
    AnomalyType.SCORE_DROP: 0.6,
    AnomalyType.SCORE_SPIKE: 0.3,
    AnomalyType.EMPLOYEE_CHANGE: 0.5,
    AnomalyType.FUNDING_SPIKE: 0.4,
    AnomalyType.TRAFFIC_ANOMALY: 0.4,
    AnomalyType.RATING_DROP: 0.6,
    AnomalyType.GITHUB_STALL: 0.3,
    AnomalyType.STATUS_CHANGE: 0.9,
}

# Thresholds for detection
THRESHOLDS = {
    "score_drop_points": 10,  # Score drop > 10 points
    "score_drop_pct": 0.12,  # Score drop > 12%
    "employee_change_pct": 0.20,  # Employee change > 20%
    "traffic_change_pct": 0.50,  # Traffic rank change > 50%
    "rating_drop": 0.5,  # Rating drop > 0.5
    "github_activity_drop_pct": 0.70,  # GitHub activity drop > 70%
}


# =============================================================================
# ANOMALY DETECTOR AGENT
# =============================================================================


class AnomalyDetectorAgent:
    """AI agent for detecting anomalies in company data."""

    def __init__(self, db: Session):
        self.db = db
        self._ensure_tables()

    # -------------------------------------------------------------------------
    # TABLE SETUP
    # -------------------------------------------------------------------------

    def _ensure_tables(self) -> None:
        """Ensure required tables exist."""
        # Anomalies table
        create_anomalies = text("""
            CREATE TABLE IF NOT EXISTS anomalies (
                id SERIAL PRIMARY KEY,
                company_name VARCHAR(255) NOT NULL,
                entity_type VARCHAR(50) DEFAULT 'company',
                anomaly_type VARCHAR(50) NOT NULL,
                description TEXT,
                previous_value TEXT,
                current_value TEXT,
                change_magnitude FLOAT,
                severity_score FLOAT,
                severity_level VARCHAR(20),
                confidence FLOAT,
                data_source VARCHAR(100),
                source_record_id INTEGER,
                status VARCHAR(20) DEFAULT 'new',
                acknowledged_at TIMESTAMP,
                resolved_at TIMESTAMP,
                resolution_notes TEXT,
                correlated_anomaly_ids INTEGER[],
                root_cause_id INTEGER,
                detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Baselines table
        create_baselines = text("""
            CREATE TABLE IF NOT EXISTS baselines (
                id SERIAL PRIMARY KEY,
                entity_type VARCHAR(50) NOT NULL,
                entity_name VARCHAR(255) NOT NULL,
                metric_name VARCHAR(100) NOT NULL,
                baseline_value FLOAT,
                mean_value FLOAT,
                std_deviation FLOAT,
                min_value FLOAT,
                max_value FLOAT,
                sample_count INTEGER DEFAULT 0,
                lower_threshold FLOAT,
                upper_threshold FLOAT,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(entity_type, entity_name, metric_name)
            )
        """)

        # Anomaly scans table
        create_scans = text("""
            CREATE TABLE IF NOT EXISTS anomaly_scans (
                id SERIAL PRIMARY KEY,
                scan_id VARCHAR(50) UNIQUE NOT NULL,
                scan_type VARCHAR(50),
                target_filter TEXT,
                status VARCHAR(20) DEFAULT 'pending',
                records_scanned INTEGER DEFAULT 0,
                anomalies_found INTEGER DEFAULT 0,
                started_at TIMESTAMP,
                completed_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Indexes
        create_indexes = [
            "CREATE INDEX IF NOT EXISTS idx_anomalies_company ON anomalies(company_name)",
            "CREATE INDEX IF NOT EXISTS idx_anomalies_severity ON anomalies(severity_score DESC)",
            "CREATE INDEX IF NOT EXISTS idx_anomalies_type ON anomalies(anomaly_type)",
            "CREATE INDEX IF NOT EXISTS idx_anomalies_detected ON anomalies(detected_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_baselines_entity ON baselines(entity_type, entity_name)",
        ]

        try:
            self.db.execute(create_anomalies)
            self.db.execute(create_baselines)
            self.db.execute(create_scans)
            for idx in create_indexes:
                self.db.execute(text(idx))
            self.db.commit()
        except Exception as e:
            logger.debug(f"Table setup: {e}")
            self.db.rollback()

    # -------------------------------------------------------------------------
    # SCANNING
    # -------------------------------------------------------------------------

    def scan_for_anomalies(
        self,
        scan_type: str = "full",
        target: str = None,
        force: bool = False,
    ) -> Dict[str, Any]:
        """Run anomaly scan across data sources."""
        scan_id = f"scan_{uuid.uuid4().hex[:12]}"

        # Create scan record
        self.db.execute(
            text("""
                INSERT INTO anomaly_scans (scan_id, scan_type, target_filter, status, started_at)
                VALUES (:scan_id, :scan_type, :target, 'running', NOW())
            """),
            {"scan_id": scan_id, "scan_type": scan_type, "target": target},
        )
        self.db.commit()

        all_anomalies = []
        records_scanned = 0

        try:
            # Detect score anomalies
            score_anomalies = self.detect_score_anomalies(target)
            all_anomalies.extend(score_anomalies)

            # Detect employee change anomalies
            employee_anomalies = self.detect_employee_anomalies(target)
            all_anomalies.extend(employee_anomalies)

            # Detect traffic anomalies
            traffic_anomalies = self.detect_traffic_anomalies(target)
            all_anomalies.extend(traffic_anomalies)

            # Detect rating anomalies
            rating_anomalies = self.detect_rating_anomalies(target)
            all_anomalies.extend(rating_anomalies)

            # Detect GitHub activity anomalies
            github_anomalies = self.detect_github_anomalies(target)
            all_anomalies.extend(github_anomalies)

            # Save anomalies
            new_count = 0
            for anomaly in all_anomalies:
                if self._save_anomaly(anomaly):
                    new_count += 1

            # Update scan record
            self.db.execute(
                text("""
                    UPDATE anomaly_scans SET
                        status = 'completed',
                        records_scanned = :scanned,
                        anomalies_found = :found,
                        completed_at = NOW()
                    WHERE scan_id = :scan_id
                """),
                {"scanned": records_scanned, "found": new_count, "scan_id": scan_id},
            )
            self.db.commit()

            return {
                "scan_id": scan_id,
                "status": "completed",
                "records_scanned": records_scanned,
                "anomalies_found": new_count,
                "anomalies": all_anomalies[:20],  # Return first 20
            }

        except Exception as e:
            logger.error(f"Scan error: {e}")
            self.db.rollback()
            self.db.execute(
                text("UPDATE anomaly_scans SET status = 'failed' WHERE scan_id = :id"),
                {"id": scan_id},
            )
            self.db.commit()
            return {"scan_id": scan_id, "status": "failed", "error": str(e)}

    def get_scan_status(self, scan_id: str) -> Optional[Dict]:
        """Get status of an anomaly scan."""
        try:
            query = text("SELECT * FROM anomaly_scans WHERE scan_id = :id")
            result = self.db.execute(query, {"id": scan_id})
            row = result.mappings().fetchone()

            if not row:
                return None

            return {
                "scan_id": row["scan_id"],
                "scan_type": row["scan_type"],
                "status": row["status"],
                "records_scanned": row["records_scanned"],
                "anomalies_found": row["anomalies_found"],
                "started_at": row["started_at"].isoformat()
                if row["started_at"]
                else None,
                "completed_at": row["completed_at"].isoformat()
                if row["completed_at"]
                else None,
            }
        except Exception as e:
            logger.error(f"Error getting scan status: {e}")
            self.db.rollback()
            return None

    # -------------------------------------------------------------------------
    # ANOMALY DETECTION BY TYPE
    # -------------------------------------------------------------------------

    def detect_score_anomalies(self, company_name: str = None) -> List[Dict]:
        """Detect anomalies in company health scores."""
        anomalies = []

        try:
            # Get recent score changes
            where_clause = ""
            params = {}

            if company_name:
                where_clause = "AND LOWER(cs.company_name) LIKE LOWER(:name)"
                params["name"] = f"%{company_name}%"

            query = text(f"""
                WITH ranked_scores AS (
                    SELECT company_name, composite_score, scored_at,
                           LAG(composite_score) OVER (
                               PARTITION BY company_name ORDER BY scored_at
                           ) as prev_score
                    FROM company_scores
                    WHERE scored_at > NOW() - INTERVAL '30 days'
                )
                SELECT company_name, composite_score, prev_score, scored_at
                FROM ranked_scores
                WHERE prev_score IS NOT NULL
                  AND composite_score != prev_score
                  {where_clause}
                ORDER BY scored_at DESC
            """)

            result = self.db.execute(query, params)

            for row in result:
                current = row[1]
                previous = row[2]
                change = current - previous
                pct_change = change / previous if previous else 0

                # Detect significant drops
                if (
                    change < -THRESHOLDS["score_drop_points"]
                    or pct_change < -THRESHOLDS["score_drop_pct"]
                ):
                    severity = self._calculate_severity(
                        AnomalyType.SCORE_DROP, abs(pct_change), row[3]
                    )
                    anomalies.append(
                        {
                            "company_name": row[0],
                            "anomaly_type": AnomalyType.SCORE_DROP.value,
                            "description": f"Health score dropped from {previous:.1f} to {current:.1f}",
                            "previous_value": str(previous),
                            "current_value": str(current),
                            "change_magnitude": pct_change,
                            "severity_score": severity,
                            "severity_level": self._get_severity_level(severity),
                            "confidence": 0.9,
                            "data_source": "company_scores",
                        }
                    )

                # Detect significant spikes (unusual improvement)
                elif change > 20 or pct_change > 0.25:
                    severity = self._calculate_severity(
                        AnomalyType.SCORE_SPIKE, abs(pct_change), row[3]
                    )
                    anomalies.append(
                        {
                            "company_name": row[0],
                            "anomaly_type": AnomalyType.SCORE_SPIKE.value,
                            "description": f"Health score spiked from {previous:.1f} to {current:.1f}",
                            "previous_value": str(previous),
                            "current_value": str(current),
                            "change_magnitude": pct_change,
                            "severity_score": severity,
                            "severity_level": self._get_severity_level(severity),
                            "confidence": 0.8,
                            "data_source": "company_scores",
                        }
                    )

        except Exception as e:
            logger.warning(f"Error detecting score anomalies: {e}")
            self.db.rollback()

        return anomalies

    def detect_employee_anomalies(self, company_name: str = None) -> List[Dict]:
        """Detect anomalies in employee count changes."""
        anomalies = []

        try:
            where_clause = ""
            params = {}

            if company_name:
                where_clause = "AND LOWER(company_name) LIKE LOWER(:name)"
                params["name"] = f"%{company_name}%"

            # Check enrichment data for employee changes
            query = text(f"""
                SELECT company_name, employee_count, previous_employee_count
                FROM company_enrichment
                WHERE employee_count IS NOT NULL
                  AND previous_employee_count IS NOT NULL
                  AND employee_count != previous_employee_count
                  {where_clause}
            """)

            result = self.db.execute(query, params)

            for row in result:
                current = row[1]
                previous = row[2]
                change = current - previous
                pct_change = change / previous if previous else 0

                if abs(pct_change) >= THRESHOLDS["employee_change_pct"]:
                    direction = "increased" if change > 0 else "decreased"
                    severity = self._calculate_severity(
                        AnomalyType.EMPLOYEE_CHANGE, abs(pct_change), datetime.now()
                    )

                    anomalies.append(
                        {
                            "company_name": row[0],
                            "anomaly_type": AnomalyType.EMPLOYEE_CHANGE.value,
                            "description": f"Employee count {direction} from {previous:,} to {current:,} ({pct_change*100:.1f}%)",
                            "previous_value": str(previous),
                            "current_value": str(current),
                            "change_magnitude": pct_change,
                            "severity_score": severity,
                            "severity_level": self._get_severity_level(severity),
                            "confidence": 0.75,
                            "data_source": "company_enrichment",
                        }
                    )

        except Exception as e:
            logger.warning(f"Error detecting employee anomalies: {e}")
            self.db.rollback()

        return anomalies

    def detect_traffic_anomalies(self, company_name: str = None) -> List[Dict]:
        """Detect anomalies in web traffic."""
        anomalies = []

        try:
            where_clause = ""
            params = {}

            if company_name:
                where_clause = "AND LOWER(company_name) LIKE LOWER(:name)"
                params["name"] = f"%{company_name}%"

            # Check web traffic changes
            query = text(f"""
                SELECT company_name, tranco_rank, previous_rank
                FROM web_traffic
                WHERE tranco_rank IS NOT NULL
                  AND previous_rank IS NOT NULL
                  AND tranco_rank != previous_rank
                  {where_clause}
            """)

            result = self.db.execute(query, params)

            for row in result:
                current = row[1]
                previous = row[2]

                # For ranks, lower is better
                change = previous - current  # Positive = improvement
                pct_change = change / previous if previous else 0

                # Detect significant rank changes
                if abs(pct_change) >= THRESHOLDS["traffic_change_pct"]:
                    direction = "improved" if change > 0 else "dropped"
                    severity = self._calculate_severity(
                        AnomalyType.TRAFFIC_ANOMALY, abs(pct_change), datetime.now()
                    )

                    anomalies.append(
                        {
                            "company_name": row[0],
                            "anomaly_type": AnomalyType.TRAFFIC_ANOMALY.value,
                            "description": f"Traffic rank {direction} from {previous:,} to {current:,}",
                            "previous_value": str(previous),
                            "current_value": str(current),
                            "change_magnitude": pct_change,
                            "severity_score": severity,
                            "severity_level": self._get_severity_level(severity),
                            "confidence": 0.7,
                            "data_source": "web_traffic",
                        }
                    )

        except Exception as e:
            logger.warning(f"Error detecting traffic anomalies: {e}")
            self.db.rollback()

        return anomalies

    def detect_rating_anomalies(self, company_name: str = None) -> List[Dict]:
        """Detect anomalies in ratings (Glassdoor, App Store)."""
        anomalies = []

        try:
            where_clause = ""
            params = {}

            if company_name:
                where_clause = "AND LOWER(company_name) LIKE LOWER(:name)"
                params["name"] = f"%{company_name}%"

            # Check Glassdoor ratings
            query = text(f"""
                SELECT company_name, overall_rating, previous_rating
                FROM glassdoor_data
                WHERE overall_rating IS NOT NULL
                  AND previous_rating IS NOT NULL
                  {where_clause}
            """)

            result = self.db.execute(query, params)

            for row in result:
                current = row[1]
                previous = row[2]
                drop = previous - current

                if drop >= THRESHOLDS["rating_drop"]:
                    severity = self._calculate_severity(
                        AnomalyType.RATING_DROP,
                        drop / 5.0,  # Normalize to 0-1
                        datetime.now(),
                    )

                    anomalies.append(
                        {
                            "company_name": row[0],
                            "anomaly_type": AnomalyType.RATING_DROP.value,
                            "description": f"Glassdoor rating dropped from {previous:.1f} to {current:.1f}",
                            "previous_value": str(previous),
                            "current_value": str(current),
                            "change_magnitude": -drop,
                            "severity_score": severity,
                            "severity_level": self._get_severity_level(severity),
                            "confidence": 0.85,
                            "data_source": "glassdoor_data",
                        }
                    )

        except Exception as e:
            logger.warning(f"Error detecting rating anomalies: {e}")
            self.db.rollback()

        return anomalies

    def detect_github_anomalies(self, company_name: str = None) -> List[Dict]:
        """Detect anomalies in GitHub activity."""
        anomalies = []

        try:
            where_clause = ""
            params = {}

            if company_name:
                where_clause = "AND LOWER(company_name) LIKE LOWER(:name)"
                params["name"] = f"%{company_name}%"

            # Check GitHub activity changes
            query = text(f"""
                SELECT company_name, total_commits_30d, previous_commits_30d
                FROM github_analytics
                WHERE total_commits_30d IS NOT NULL
                  AND previous_commits_30d IS NOT NULL
                  AND previous_commits_30d > 10
                  {where_clause}
            """)

            result = self.db.execute(query, params)

            for row in result:
                current = row[1]
                previous = row[2]
                pct_change = (current - previous) / previous if previous else 0

                # Detect activity stall
                if pct_change <= -THRESHOLDS["github_activity_drop_pct"]:
                    severity = self._calculate_severity(
                        AnomalyType.GITHUB_STALL, abs(pct_change), datetime.now()
                    )

                    anomalies.append(
                        {
                            "company_name": row[0],
                            "anomaly_type": AnomalyType.GITHUB_STALL.value,
                            "description": f"GitHub activity dropped from {previous} to {current} commits/month ({pct_change*100:.0f}%)",
                            "previous_value": str(previous),
                            "current_value": str(current),
                            "change_magnitude": pct_change,
                            "severity_score": severity,
                            "severity_level": self._get_severity_level(severity),
                            "confidence": 0.7,
                            "data_source": "github_analytics",
                        }
                    )

        except Exception as e:
            logger.warning(f"Error detecting GitHub anomalies: {e}")
            self.db.rollback()

        return anomalies

    # -------------------------------------------------------------------------
    # SEVERITY CALCULATION
    # -------------------------------------------------------------------------

    def _calculate_severity(
        self,
        anomaly_type: AnomalyType,
        magnitude: float,
        detected_at: datetime,
    ) -> float:
        """Calculate severity score for an anomaly."""
        base = BASE_SEVERITY.get(anomaly_type, 0.5)

        # Magnitude multiplier (1.0 to 2.5)
        magnitude_mult = min(1.0 + (magnitude * 1.5), 2.5)

        # Recency factor (more recent = higher weight)
        if isinstance(detected_at, datetime):
            hours_ago = (datetime.now() - detected_at).total_seconds() / 3600
            recency_factor = max(0.5, 1.5 - (hours_ago / 168))  # Decay over 1 week
        else:
            recency_factor = 1.0

        severity = base * magnitude_mult * recency_factor

        # Cap at 1.0
        return min(severity, 1.0)

    def _get_severity_level(self, score: float) -> str:
        """Convert severity score to level."""
        if score >= 0.8:
            return SeverityLevel.CRITICAL.value
        elif score >= 0.6:
            return SeverityLevel.HIGH.value
        elif score >= 0.3:
            return SeverityLevel.MEDIUM.value
        else:
            return SeverityLevel.LOW.value

    # -------------------------------------------------------------------------
    # ANOMALY STORAGE
    # -------------------------------------------------------------------------

    def _save_anomaly(self, anomaly: Dict) -> bool:
        """Save anomaly to database if not duplicate."""
        try:
            # Check for recent duplicate
            check_query = text("""
                SELECT id FROM anomalies
                WHERE company_name = :company
                  AND anomaly_type = :type
                  AND detected_at > NOW() - INTERVAL '24 hours'
                LIMIT 1
            """)
            existing = self.db.execute(
                check_query,
                {
                    "company": anomaly["company_name"],
                    "type": anomaly["anomaly_type"],
                },
            ).fetchone()

            if existing:
                return False

            # Insert new anomaly
            insert_query = text("""
                INSERT INTO anomalies (
                    company_name, anomaly_type, description,
                    previous_value, current_value, change_magnitude,
                    severity_score, severity_level, confidence,
                    data_source, status
                ) VALUES (
                    :company, :type, :description,
                    :prev, :curr, :magnitude,
                    :severity, :level, :confidence,
                    :source, 'new'
                )
            """)
            self.db.execute(
                insert_query,
                {
                    "company": anomaly["company_name"],
                    "type": anomaly["anomaly_type"],
                    "description": anomaly.get("description"),
                    "prev": anomaly.get("previous_value"),
                    "curr": anomaly.get("current_value"),
                    "magnitude": anomaly.get("change_magnitude"),
                    "severity": anomaly.get("severity_score"),
                    "level": anomaly.get("severity_level"),
                    "confidence": anomaly.get("confidence"),
                    "source": anomaly.get("data_source"),
                },
            )
            self.db.commit()
            return True

        except Exception as e:
            logger.warning(f"Error saving anomaly: {e}")
            self.db.rollback()
            return False

    # -------------------------------------------------------------------------
    # QUERIES
    # -------------------------------------------------------------------------

    def get_recent_anomalies(
        self,
        hours: int = 24,
        severity: str = None,
        anomaly_type: str = None,
        limit: int = 50,
    ) -> Dict[str, Any]:
        """Get recent anomalies."""
        try:
            params = {"hours": hours, "limit": limit}

            # Note: We need to handle interval differently
            base_where = f"detected_at > NOW() - INTERVAL '{hours} hours'"

            if severity:
                base_where += " AND severity_level = :severity"
                params["severity"] = severity

            if anomaly_type:
                base_where += " AND anomaly_type = :type"
                params["type"] = anomaly_type

            query = text(f"""
                SELECT id, company_name, anomaly_type, description,
                       previous_value, current_value, change_magnitude,
                       severity_score, severity_level, confidence,
                       data_source, status, detected_at
                FROM anomalies
                WHERE {base_where}
                ORDER BY severity_score DESC, detected_at DESC
                LIMIT :limit
            """)

            result = self.db.execute(query, params)
            anomalies = []

            for row in result:
                anomalies.append(
                    {
                        "id": row[0],
                        "company_name": row[1],
                        "anomaly_type": row[2],
                        "description": row[3],
                        "previous_value": row[4],
                        "current_value": row[5],
                        "change_magnitude": row[6],
                        "severity_score": row[7],
                        "severity_level": row[8],
                        "confidence": row[9],
                        "data_source": row[10],
                        "status": row[11],
                        "detected_at": row[12].isoformat() if row[12] else None,
                    }
                )

            # Get counts by severity and type
            severity_query = text(f"""
                SELECT severity_level, COUNT(*) FROM anomalies
                WHERE {base_where}
                GROUP BY severity_level
            """)
            by_severity = {r[0]: r[1] for r in self.db.execute(severity_query, params)}

            type_query = text(f"""
                SELECT anomaly_type, COUNT(*) FROM anomalies
                WHERE {base_where}
                GROUP BY anomaly_type
            """)
            by_type = {r[0]: r[1] for r in self.db.execute(type_query, params)}

            return {
                "anomalies": anomalies,
                "total": len(anomalies),
                "by_severity": by_severity,
                "by_type": by_type,
            }

        except Exception as e:
            logger.error(f"Error getting recent anomalies: {e}")
            self.db.rollback()
            return {"anomalies": [], "total": 0, "by_severity": {}, "by_type": {}}

    def get_company_anomalies(
        self,
        company_name: str,
        days: int = 30,
        status: str = None,
        include_resolved: bool = False,
    ) -> Dict[str, Any]:
        """Get anomalies for a specific company."""
        try:
            where_parts = [
                "LOWER(company_name) LIKE LOWER(:pattern)",
                f"detected_at > NOW() - INTERVAL '{days} days'",
            ]
            params = {"pattern": f"%{company_name}%"}

            if status:
                where_parts.append("status = :status")
                params["status"] = status

            if not include_resolved:
                where_parts.append("status != 'resolved'")

            where_sql = " AND ".join(where_parts)

            query = text(f"""
                SELECT id, company_name, anomaly_type, description,
                       previous_value, current_value, change_magnitude,
                       severity_score, severity_level, confidence,
                       data_source, status, detected_at, resolved_at
                FROM anomalies
                WHERE {where_sql}
                ORDER BY detected_at DESC
            """)

            result = self.db.execute(query, params)
            anomalies = []

            for row in result:
                anomalies.append(
                    {
                        "id": row[0],
                        "company_name": row[1],
                        "anomaly_type": row[2],
                        "description": row[3],
                        "previous_value": row[4],
                        "current_value": row[5],
                        "change_magnitude": row[6],
                        "severity_score": row[7],
                        "severity_level": row[8],
                        "confidence": row[9],
                        "data_source": row[10],
                        "status": row[11],
                        "detected_at": row[12].isoformat() if row[12] else None,
                        "resolved_at": row[13].isoformat() if row[13] else None,
                    }
                )

            # Calculate risk summary
            unresolved = [a for a in anomalies if a["status"] != "resolved"]
            critical_count = sum(
                1 for a in unresolved if a["severity_level"] == "critical"
            )
            high_count = sum(1 for a in unresolved if a["severity_level"] == "high")

            overall_risk = "low"
            if critical_count > 0:
                overall_risk = "critical"
            elif high_count > 0:
                overall_risk = "high"
            elif len(unresolved) > 3:
                overall_risk = "medium"

            return {
                "company": company_name,
                "anomalies": anomalies,
                "total": len(anomalies),
                "unresolved": len(unresolved),
                "risk_summary": {
                    "overall_risk": overall_risk,
                    "active_critical": critical_count,
                    "active_high": high_count,
                },
            }

        except Exception as e:
            logger.error(f"Error getting company anomalies: {e}")
            self.db.rollback()
            return {"company": company_name, "anomalies": [], "total": 0}

    # -------------------------------------------------------------------------
    # INVESTIGATION
    # -------------------------------------------------------------------------

    def investigate(self, anomaly_id: int, depth: str = "standard") -> Dict[str, Any]:
        """Investigate an anomaly to find causes and recommendations."""
        try:
            # Get the anomaly
            query = text("SELECT * FROM anomalies WHERE id = :id")
            result = self.db.execute(query, {"id": anomaly_id})
            row = result.mappings().fetchone()

            if not row:
                return {"error": "Anomaly not found"}

            anomaly = dict(row)

            investigation = {
                "probable_causes": self._get_probable_causes(anomaly),
                "correlated_anomalies": self._find_correlated(anomaly),
                "historical_context": self._get_historical_context(anomaly),
                "recommendations": self._get_recommendations(anomaly),
            }

            # Deep investigation adds more context
            if depth == "deep":
                investigation["company_profile"] = self._get_company_context(
                    anomaly["company_name"]
                )
                investigation["sector_comparison"] = self._get_sector_context(
                    anomaly["company_name"]
                )

            return {
                "anomaly": {
                    "id": anomaly["id"],
                    "company_name": anomaly["company_name"],
                    "anomaly_type": anomaly["anomaly_type"],
                    "description": anomaly["description"],
                    "severity_score": anomaly["severity_score"],
                    "severity_level": anomaly["severity_level"],
                },
                "investigation": investigation,
            }

        except Exception as e:
            logger.error(f"Error investigating anomaly: {e}")
            self.db.rollback()
            return {"error": str(e)}

    def _get_probable_causes(self, anomaly: Dict) -> List[Dict]:
        """Determine probable causes for an anomaly."""
        causes = []
        anomaly_type = anomaly.get("anomaly_type")

        if anomaly_type == AnomalyType.SCORE_DROP.value:
            # Check for related anomalies
            related = self._find_correlated(anomaly)
            if any(r["anomaly_type"] == "employee_change" for r in related):
                causes.append({"cause": "Recent workforce changes", "confidence": 0.8})
            if any(r["anomaly_type"] == "rating_drop" for r in related):
                causes.append(
                    {"cause": "Employee satisfaction decline", "confidence": 0.75}
                )

            # Generic causes
            causes.append(
                {"cause": "Market conditions affecting sector", "confidence": 0.5}
            )
            causes.append({"cause": "Recent negative news coverage", "confidence": 0.4})

        elif anomaly_type == AnomalyType.EMPLOYEE_CHANGE.value:
            magnitude = anomaly.get("change_magnitude", 0)
            if magnitude < 0:
                causes.append({"cause": "Layoffs or restructuring", "confidence": 0.85})
                causes.append({"cause": "Natural attrition spike", "confidence": 0.4})
            else:
                causes.append(
                    {"cause": "Rapid hiring / growth phase", "confidence": 0.8}
                )
                causes.append({"cause": "Acquisition or merger", "confidence": 0.5})

        elif anomaly_type == AnomalyType.RATING_DROP.value:
            causes.append({"cause": "Management or culture issues", "confidence": 0.7})
            causes.append(
                {"cause": "Recent layoffs affecting morale", "confidence": 0.6}
            )
            causes.append({"cause": "Compensation concerns", "confidence": 0.5})

        elif anomaly_type == AnomalyType.TRAFFIC_ANOMALY.value:
            magnitude = anomaly.get("change_magnitude", 0)
            if magnitude < 0:
                causes.append({"cause": "SEO/marketing issues", "confidence": 0.6})
                causes.append({"cause": "Product/service decline", "confidence": 0.5})
            else:
                causes.append({"cause": "Viral marketing success", "confidence": 0.7})
                causes.append({"cause": "New product launch", "confidence": 0.6})

        elif anomaly_type == AnomalyType.GITHUB_STALL.value:
            causes.append({"cause": "Team restructuring", "confidence": 0.6})
            causes.append({"cause": "Pivot to private repos", "confidence": 0.5})
            causes.append(
                {"cause": "Project completion/maintenance mode", "confidence": 0.4}
            )

        # Sort by confidence
        causes.sort(key=lambda x: x["confidence"], reverse=True)
        return causes[:5]

    def _find_correlated(self, anomaly: Dict) -> List[Dict]:
        """Find anomalies correlated with this one."""
        try:
            company = anomaly.get("company_name")
            detected = anomaly.get("detected_at")

            if isinstance(detected, str):
                detected = datetime.fromisoformat(detected)

            query = text("""
                SELECT id, anomaly_type, description, severity_score, detected_at
                FROM anomalies
                WHERE LOWER(company_name) LIKE LOWER(:company)
                  AND id != :id
                  AND detected_at > :start
                  AND detected_at < :end
                ORDER BY detected_at DESC
                LIMIT 10
            """)

            start = (
                detected - timedelta(days=7)
                if detected
                else datetime.now() - timedelta(days=14)
            )
            end = detected + timedelta(days=7) if detected else datetime.now()

            result = self.db.execute(
                query,
                {
                    "company": f"%{company}%",
                    "id": anomaly.get("id", 0),
                    "start": start,
                    "end": end,
                },
            )

            correlated = []
            for row in result:
                correlated.append(
                    {
                        "id": row[0],
                        "anomaly_type": row[1],
                        "description": row[2],
                        "severity_score": row[3],
                        "correlation": 0.7,  # Simplified correlation score
                    }
                )

            return correlated

        except Exception as e:
            logger.warning(f"Error finding correlated anomalies: {e}")
            self.db.rollback()
            return []

    def _get_historical_context(self, anomaly: Dict) -> Dict:
        """Get historical context for the anomaly."""
        try:
            company = anomaly.get("company_name")
            anomaly_type = anomaly.get("anomaly_type")

            query = text("""
                SELECT COUNT(*) as similar_count,
                       AVG(CASE WHEN status = 'resolved'
                           THEN EXTRACT(EPOCH FROM (resolved_at - detected_at))/86400
                           ELSE NULL END) as avg_resolution_days
                FROM anomalies
                WHERE LOWER(company_name) LIKE LOWER(:company)
                  AND anomaly_type = :type
                  AND detected_at < NOW() - INTERVAL '7 days'
            """)

            result = self.db.execute(
                query,
                {
                    "company": f"%{company}%",
                    "type": anomaly_type,
                },
            ).fetchone()

            similar = result[0] if result else 0
            avg_days = result[1] if result and result[1] else None

            return {
                "similar_events": similar,
                "typical_resolution_days": round(avg_days, 1) if avg_days else None,
                "company_history": f"{similar} similar anomalies in history",
            }

        except Exception as e:
            logger.warning(f"Error getting historical context: {e}")
            self.db.rollback()
            return {}

    def _get_recommendations(self, anomaly: Dict) -> List[str]:
        """Get recommendations for handling the anomaly."""
        recommendations = []
        anomaly_type = anomaly.get("anomaly_type")
        severity = anomaly.get("severity_level", "medium")

        # Severity-based recommendations
        if severity in ["critical", "high"]:
            recommendations.append("Schedule review meeting within 48 hours")
            recommendations.append("Check for related news and press releases")

        # Type-based recommendations
        if anomaly_type == AnomalyType.SCORE_DROP.value:
            recommendations.append("Review sub-component scores for root cause")
            recommendations.append("Compare against sector peers")
            recommendations.append("Monitor for continued decline over next 2 weeks")

        elif anomaly_type == AnomalyType.EMPLOYEE_CHANGE.value:
            recommendations.append("Verify data accuracy from multiple sources")
            recommendations.append("Check LinkedIn and news for announcements")
            recommendations.append("Review Glassdoor for recent reviews")

        elif anomaly_type == AnomalyType.RATING_DROP.value:
            recommendations.append("Read recent Glassdoor reviews for themes")
            recommendations.append("Compare compensation data if available")
            recommendations.append("Monitor for executive departures")

        elif anomaly_type == AnomalyType.TRAFFIC_ANOMALY.value:
            recommendations.append("Check for website outages or redirects")
            recommendations.append("Review marketing/PR activities")
            recommendations.append("Compare with competitor traffic trends")

        elif anomaly_type == AnomalyType.GITHUB_STALL.value:
            recommendations.append("Check if repos moved to private")
            recommendations.append("Review team changes")
            recommendations.append("May indicate pivot or project completion")

        return recommendations[:5]

    def _get_company_context(self, company_name: str) -> Dict:
        """Get company profile for context."""
        try:
            query = text("""
                SELECT company_name, sector, industry, employee_count, total_funding
                FROM company_enrichment
                WHERE LOWER(company_name) LIKE LOWER(:name)
                LIMIT 1
            """)
            result = self.db.execute(query, {"name": f"%{company_name}%"})
            row = result.fetchone()

            if row:
                return {
                    "name": row[0],
                    "sector": row[1],
                    "industry": row[2],
                    "employees": row[3],
                    "funding": row[4],
                }
            return {}

        except Exception as e:
            logger.warning(f"Error getting company context: {e}")
            self.db.rollback()
            return {}

    def _get_sector_context(self, company_name: str) -> Dict:
        """Get sector comparison context."""
        # Simplified sector context
        return {
            "sector_avg_score": None,
            "company_rank_in_sector": None,
            "sector_trend": "stable",
        }

    # -------------------------------------------------------------------------
    # STATUS UPDATES
    # -------------------------------------------------------------------------

    def update_anomaly_status(
        self,
        anomaly_id: int,
        status: str,
        resolution_notes: str = None,
    ) -> Optional[Dict]:
        """Update anomaly status."""
        try:
            # Validate status
            if status not in [s.value for s in AnomalyStatus]:
                return {"error": f"Invalid status: {status}"}

            updates = ["status = :status"]
            params = {"id": anomaly_id, "status": status}

            if status == AnomalyStatus.ACKNOWLEDGED.value:
                updates.append("acknowledged_at = NOW()")

            if status == AnomalyStatus.RESOLVED.value:
                updates.append("resolved_at = NOW()")
                if resolution_notes:
                    updates.append("resolution_notes = :notes")
                    params["notes"] = resolution_notes

            query = text(f"""
                UPDATE anomalies
                SET {", ".join(updates)}
                WHERE id = :id
                RETURNING id, status, acknowledged_at, resolved_at, resolution_notes
            """)

            result = self.db.execute(query, params)
            row = result.fetchone()
            self.db.commit()

            if row:
                return {
                    "id": row[0],
                    "status": row[1],
                    "acknowledged_at": row[2].isoformat() if row[2] else None,
                    "resolved_at": row[3].isoformat() if row[3] else None,
                    "resolution_notes": row[4],
                }
            return None

        except Exception as e:
            logger.error(f"Error updating anomaly status: {e}")
            self.db.rollback()
            return {"error": str(e)}

    # -------------------------------------------------------------------------
    # BASELINES / PATTERNS
    # -------------------------------------------------------------------------

    def get_patterns(
        self,
        entity_type: str = None,
        entity_name: str = None,
        metric: str = None,
        limit: int = 100,
    ) -> Dict[str, Any]:
        """Get learned baseline patterns."""
        try:
            where_parts = []
            params = {"limit": limit}

            if entity_type:
                where_parts.append("entity_type = :entity_type")
                params["entity_type"] = entity_type

            if entity_name:
                where_parts.append("LOWER(entity_name) LIKE LOWER(:entity_name)")
                params["entity_name"] = f"%{entity_name}%"

            if metric:
                where_parts.append("metric_name = :metric")
                params["metric"] = metric

            where_sql = " AND ".join(where_parts) if where_parts else "1=1"

            query = text(f"""
                SELECT entity_type, entity_name, metric_name,
                       baseline_value, mean_value, std_deviation,
                       min_value, max_value, sample_count,
                       lower_threshold, upper_threshold, last_updated
                FROM baselines
                WHERE {where_sql}
                ORDER BY entity_name, metric_name
                LIMIT :limit
            """)

            result = self.db.execute(query, params)
            patterns = []

            for row in result:
                patterns.append(
                    {
                        "entity_type": row[0],
                        "entity_name": row[1],
                        "metric": row[2],
                        "baseline": row[3],
                        "mean": row[4],
                        "std_deviation": row[5],
                        "min": row[6],
                        "max": row[7],
                        "sample_count": row[8],
                        "lower_threshold": row[9],
                        "upper_threshold": row[10],
                        "last_updated": row[11].isoformat() if row[11] else None,
                    }
                )

            return {
                "patterns": patterns,
                "total": len(patterns),
            }

        except Exception as e:
            logger.error(f"Error getting patterns: {e}")
            self.db.rollback()
            return {"patterns": [], "total": 0}

    def learn_baseline(
        self,
        entity_type: str,
        entity_name: str,
        metric: str,
        values: List[float],
    ) -> Dict:
        """Learn baseline pattern from historical values."""
        if not values:
            return {"error": "No values provided"}

        import statistics

        mean_val = statistics.mean(values)
        std_val = statistics.stdev(values) if len(values) > 1 else 0
        min_val = min(values)
        max_val = max(values)

        # Calculate thresholds (2 standard deviations)
        lower = mean_val - (2 * std_val)
        upper = mean_val + (2 * std_val)

        try:
            query = text("""
                INSERT INTO baselines (
                    entity_type, entity_name, metric_name,
                    baseline_value, mean_value, std_deviation,
                    min_value, max_value, sample_count,
                    lower_threshold, upper_threshold
                ) VALUES (
                    :type, :name, :metric,
                    :baseline, :mean, :std,
                    :min, :max, :count,
                    :lower, :upper
                )
                ON CONFLICT (entity_type, entity_name, metric_name) DO UPDATE SET
                    baseline_value = :baseline,
                    mean_value = :mean,
                    std_deviation = :std,
                    min_value = :min,
                    max_value = :max,
                    sample_count = :count,
                    lower_threshold = :lower,
                    upper_threshold = :upper,
                    last_updated = NOW()
            """)

            self.db.execute(
                query,
                {
                    "type": entity_type,
                    "name": entity_name,
                    "metric": metric,
                    "baseline": mean_val,
                    "mean": mean_val,
                    "std": std_val,
                    "min": min_val,
                    "max": max_val,
                    "count": len(values),
                    "lower": lower,
                    "upper": upper,
                },
            )
            self.db.commit()

            return {
                "entity_type": entity_type,
                "entity_name": entity_name,
                "metric": metric,
                "baseline": mean_val,
                "lower_threshold": lower,
                "upper_threshold": upper,
                "sample_count": len(values),
            }

        except Exception as e:
            logger.error(f"Error learning baseline: {e}")
            self.db.rollback()
            return {"error": str(e)}
