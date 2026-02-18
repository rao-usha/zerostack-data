"""
Deal Flow Tracker Service.

Manages investment deal pipeline from sourcing to close.
"""

import logging
from datetime import datetime
from typing import Dict, Any, List, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

# Valid pipeline stages
PIPELINE_STAGES = [
    "sourced",
    "reviewing",
    "due_diligence",
    "negotiation",
    "closed_won",
    "closed_lost",
    "passed",
]


class DealTracker:
    """Deal flow tracking service."""

    def __init__(self, db: Session):
        self.db = db
        self._ensure_tables()

    def _ensure_tables(self):
        """Create tables if they don't exist."""
        self.db.execute(
            text("""
            CREATE TABLE IF NOT EXISTS deals (
                id SERIAL PRIMARY KEY,

                -- Company info
                company_name VARCHAR(255) NOT NULL,
                company_sector VARCHAR(100),
                company_stage VARCHAR(50),
                company_location VARCHAR(100),
                company_website TEXT,

                -- Deal info
                deal_type VARCHAR(50),
                deal_size_millions FLOAT,
                valuation_millions FLOAT,

                -- Pipeline
                pipeline_stage VARCHAR(50) DEFAULT 'sourced',
                priority INTEGER DEFAULT 3,
                fit_score FLOAT,

                -- Source
                source VARCHAR(100),
                source_contact VARCHAR(255),

                -- Assignment
                assigned_to VARCHAR(255),

                -- Metadata
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                closed_at TIMESTAMP,

                -- Tags
                tags TEXT[]
            )
        """)
        )

        self.db.execute(
            text("""
            CREATE TABLE IF NOT EXISTS deal_activities (
                id SERIAL PRIMARY KEY,
                deal_id INTEGER REFERENCES deals(id) ON DELETE CASCADE,

                activity_type VARCHAR(50) NOT NULL,
                title VARCHAR(255),
                description TEXT,

                -- Meeting specific
                meeting_date TIMESTAMP,
                attendees TEXT[],

                -- User
                created_by VARCHAR(255),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        )

        # Create indexes if they don't exist
        self.db.execute(
            text("""
            CREATE INDEX IF NOT EXISTS idx_deals_stage ON deals(pipeline_stage)
        """)
        )
        self.db.execute(
            text("""
            CREATE INDEX IF NOT EXISTS idx_deals_sector ON deals(company_sector)
        """)
        )
        self.db.execute(
            text("""
            CREATE INDEX IF NOT EXISTS idx_deals_priority ON deals(priority)
        """)
        )
        self.db.execute(
            text("""
            CREATE INDEX IF NOT EXISTS idx_activities_deal ON deal_activities(deal_id)
        """)
        )

        self.db.commit()

    def create_deal(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new deal."""
        # Extract fields
        company_name = data.get("company_name")
        if not company_name:
            raise ValueError("company_name is required")

        company_sector = data.get("company_sector")
        company_stage = data.get("company_stage")
        company_location = data.get("company_location")
        company_website = data.get("company_website")
        deal_type = data.get("deal_type")
        deal_size_millions = data.get("deal_size_millions")
        valuation_millions = data.get("valuation_millions")
        pipeline_stage = data.get("pipeline_stage", "sourced")
        priority = data.get("priority", 3)
        fit_score = data.get("fit_score")
        source = data.get("source")
        source_contact = data.get("source_contact")
        assigned_to = data.get("assigned_to")
        tags = data.get("tags", [])

        # Validate pipeline stage
        if pipeline_stage not in PIPELINE_STAGES:
            raise ValueError(
                f"Invalid pipeline_stage. Must be one of: {PIPELINE_STAGES}"
            )

        # Validate priority
        if priority < 1 or priority > 5:
            raise ValueError("Priority must be between 1 and 5")

        result = self.db.execute(
            text("""
            INSERT INTO deals (
                company_name, company_sector, company_stage, company_location, company_website,
                deal_type, deal_size_millions, valuation_millions,
                pipeline_stage, priority, fit_score,
                source, source_contact, assigned_to, tags
            ) VALUES (
                :company_name, :company_sector, :company_stage, :company_location, :company_website,
                :deal_type, :deal_size_millions, :valuation_millions,
                :pipeline_stage, :priority, :fit_score,
                :source, :source_contact, :assigned_to, :tags
            )
            RETURNING id, created_at
        """),
            {
                "company_name": company_name,
                "company_sector": company_sector,
                "company_stage": company_stage,
                "company_location": company_location,
                "company_website": company_website,
                "deal_type": deal_type,
                "deal_size_millions": deal_size_millions,
                "valuation_millions": valuation_millions,
                "pipeline_stage": pipeline_stage,
                "priority": priority,
                "fit_score": fit_score,
                "source": source,
                "source_contact": source_contact,
                "assigned_to": assigned_to,
                "tags": tags,
            },
        )

        row = result.fetchone()
        self.db.commit()

        return {
            "id": row[0],
            "company_name": company_name,
            "pipeline_stage": pipeline_stage,
            "priority": priority,
            "created_at": row[1].isoformat() if row[1] else None,
            "message": "Deal created successfully",
        }

    def get_deal(self, deal_id: int) -> Optional[Dict[str, Any]]:
        """Get deal with activity count."""
        result = self.db.execute(
            text("""
            SELECT
                d.*,
                (SELECT COUNT(*) FROM deal_activities WHERE deal_id = d.id) as activity_count
            FROM deals d
            WHERE d.id = :deal_id
        """),
            {"deal_id": deal_id},
        )

        row = result.fetchone()
        if not row:
            return None

        return self._row_to_dict(row)

    def update_deal(
        self, deal_id: int, updates: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """Update deal fields."""
        # Build dynamic update
        allowed_fields = [
            "company_name",
            "company_sector",
            "company_stage",
            "company_location",
            "company_website",
            "deal_type",
            "deal_size_millions",
            "valuation_millions",
            "pipeline_stage",
            "priority",
            "fit_score",
            "source",
            "source_contact",
            "assigned_to",
            "tags",
        ]

        set_clauses = []
        params = {"deal_id": deal_id}

        for field in allowed_fields:
            if field in updates:
                value = updates[field]

                # Validate pipeline stage
                if field == "pipeline_stage" and value not in PIPELINE_STAGES:
                    raise ValueError(
                        f"Invalid pipeline_stage. Must be one of: {PIPELINE_STAGES}"
                    )

                # Validate priority
                if field == "priority" and (value < 1 or value > 5):
                    raise ValueError("Priority must be between 1 and 5")

                set_clauses.append(f"{field} = :{field}")
                params[field] = value

        if not set_clauses:
            return self.get_deal(deal_id)

        # Always update updated_at
        set_clauses.append("updated_at = CURRENT_TIMESTAMP")

        # Handle closed stages
        if updates.get("pipeline_stage") in ["closed_won", "closed_lost", "passed"]:
            set_clauses.append("closed_at = CURRENT_TIMESTAMP")

        query = f"UPDATE deals SET {', '.join(set_clauses)} WHERE id = :deal_id"
        self.db.execute(text(query), params)
        self.db.commit()

        return self.get_deal(deal_id)

    def delete_deal(self, deal_id: int) -> bool:
        """Delete a deal and its activities."""
        result = self.db.execute(
            text("""
            DELETE FROM deals WHERE id = :deal_id
        """),
            {"deal_id": deal_id},
        )
        self.db.commit()
        return result.rowcount > 0

    def list_deals(
        self,
        pipeline_stage: Optional[str] = None,
        company_sector: Optional[str] = None,
        assigned_to: Optional[str] = None,
        priority: Optional[int] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> Dict[str, Any]:
        """List deals with filters and pagination."""
        where_clauses = []
        params = {"limit": limit, "offset": offset}

        if pipeline_stage:
            where_clauses.append("pipeline_stage = :pipeline_stage")
            params["pipeline_stage"] = pipeline_stage

        if company_sector:
            where_clauses.append("company_sector ILIKE :company_sector")
            params["company_sector"] = f"%{company_sector}%"

        if assigned_to:
            where_clauses.append("assigned_to = :assigned_to")
            params["assigned_to"] = assigned_to

        if priority:
            where_clauses.append("priority = :priority")
            params["priority"] = priority

        where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

        # Get total count
        count_result = self.db.execute(
            text(f"""
            SELECT COUNT(*) FROM deals {where_sql}
        """),
            params,
        )
        total = count_result.scalar()

        # Get deals
        result = self.db.execute(
            text(f"""
            SELECT
                d.*,
                (SELECT COUNT(*) FROM deal_activities WHERE deal_id = d.id) as activity_count
            FROM deals d
            {where_sql}
            ORDER BY priority ASC, updated_at DESC
            LIMIT :limit OFFSET :offset
        """),
            params,
        )

        deals = [self._row_to_dict(row) for row in result.fetchall()]

        return {
            "deals": deals,
            "total": total,
            "limit": limit,
            "offset": offset,
        }

    def add_activity(self, deal_id: int, activity: Dict[str, Any]) -> Dict[str, Any]:
        """Add activity to a deal."""
        activity_type = activity.get("activity_type")
        if not activity_type:
            raise ValueError("activity_type is required")

        valid_types = ["note", "meeting", "call", "email", "document"]
        if activity_type not in valid_types:
            raise ValueError(f"Invalid activity_type. Must be one of: {valid_types}")

        title = activity.get("title")
        description = activity.get("description")
        meeting_date = activity.get("meeting_date")
        attendees = activity.get("attendees", [])
        created_by = activity.get("created_by")

        result = self.db.execute(
            text("""
            INSERT INTO deal_activities (
                deal_id, activity_type, title, description,
                meeting_date, attendees, created_by
            ) VALUES (
                :deal_id, :activity_type, :title, :description,
                :meeting_date, :attendees, :created_by
            )
            RETURNING id, created_at
        """),
            {
                "deal_id": deal_id,
                "activity_type": activity_type,
                "title": title,
                "description": description,
                "meeting_date": meeting_date,
                "attendees": attendees,
                "created_by": created_by,
            },
        )

        row = result.fetchone()
        self.db.commit()

        return {
            "id": row[0],
            "deal_id": deal_id,
            "activity_type": activity_type,
            "title": title,
            "created_at": row[1].isoformat() if row[1] else None,
        }

    def get_activities(self, deal_id: int, limit: int = 50) -> List[Dict[str, Any]]:
        """Get activities for a deal."""
        result = self.db.execute(
            text("""
            SELECT id, deal_id, activity_type, title, description,
                   meeting_date, attendees, created_by, created_at
            FROM deal_activities
            WHERE deal_id = :deal_id
            ORDER BY created_at DESC
            LIMIT :limit
        """),
            {"deal_id": deal_id, "limit": limit},
        )

        return [
            {
                "id": row[0],
                "deal_id": row[1],
                "activity_type": row[2],
                "title": row[3],
                "description": row[4],
                "meeting_date": row[5].isoformat() if row[5] else None,
                "attendees": row[6] or [],
                "created_by": row[7],
                "created_at": row[8].isoformat() if row[8] else None,
            }
            for row in result.fetchall()
        ]

    def get_pipeline_summary(self) -> Dict[str, Any]:
        """Get pipeline stage summary."""
        # Count by stage
        stage_result = self.db.execute(
            text("""
            SELECT pipeline_stage, COUNT(*) as count
            FROM deals
            GROUP BY pipeline_stage
        """)
        )
        by_stage = {row[0]: row[1] for row in stage_result.fetchall()}

        # Count by priority
        priority_result = self.db.execute(
            text("""
            SELECT priority, COUNT(*) as count
            FROM deals
            WHERE pipeline_stage NOT IN ('closed_won', 'closed_lost', 'passed')
            GROUP BY priority
            ORDER BY priority
        """)
        )
        by_priority = {str(row[0]): row[1] for row in priority_result.fetchall()}

        # Total deals
        total_result = self.db.execute(text("SELECT COUNT(*) FROM deals"))
        total = total_result.scalar()

        # Recent activity count (last 7 days)
        activity_result = self.db.execute(
            text("""
            SELECT COUNT(*) FROM deal_activities
            WHERE created_at > CURRENT_TIMESTAMP - INTERVAL '7 days'
        """)
        )
        recent_activity = activity_result.scalar()

        # Active deals (not closed)
        active_result = self.db.execute(
            text("""
            SELECT COUNT(*) FROM deals
            WHERE pipeline_stage NOT IN ('closed_won', 'closed_lost', 'passed')
        """)
        )
        active_deals = active_result.scalar()

        return {
            "total_deals": total,
            "active_deals": active_deals,
            "by_stage": by_stage,
            "by_priority": by_priority,
            "recent_activity": recent_activity,
        }

    def _row_to_dict(self, row) -> Dict[str, Any]:
        """Convert database row to dictionary."""
        return {
            "id": row[0],
            "company_name": row[1],
            "company_sector": row[2],
            "company_stage": row[3],
            "company_location": row[4],
            "company_website": row[5],
            "deal_type": row[6],
            "deal_size_millions": row[7],
            "valuation_millions": row[8],
            "pipeline_stage": row[9],
            "priority": row[10],
            "fit_score": row[11],
            "source": row[12],
            "source_contact": row[13],
            "assigned_to": row[14],
            "created_at": row[15].isoformat() if row[15] else None,
            "updated_at": row[16].isoformat() if row[16] else None,
            "closed_at": row[17].isoformat() if row[17] else None,
            "tags": row[18] or [],
            "activity_count": row[19] if len(row) > 19 else 0,
        }
