"""
Report Builder Service.

T25: Generates customizable PDF/Excel reports for sharing insights.
"""

import logging
from datetime import datetime
from typing import Dict, List, Any, Optional
from pathlib import Path

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.reports.templates.investor_profile import InvestorProfileTemplate
from app.reports.templates.portfolio_detail import PortfolioDetailTemplate
from app.reports.templates.data_quality import DataQualityTemplate
from app.reports.templates.data_quality_deep import DataQualityDeepTemplate
from app.reports.templates.medspa_market import MedSpaMarketTemplate

logger = logging.getLogger(__name__)

# Report storage directory
REPORTS_DIR = Path("/tmp/nexdata_reports")
REPORTS_DIR.mkdir(parents=True, exist_ok=True)


class ReportBuilder:
    """
    Report generation service.

    Manages report templates, generation, and storage.
    """

    def __init__(self, db: Session):
        self.db = db
        self.templates = {
            "investor_profile": InvestorProfileTemplate(),
            "portfolio_detail": PortfolioDetailTemplate(),
            "data_quality": DataQualityTemplate(),
            "data_quality_deep": DataQualityDeepTemplate(),
            "medspa_market": MedSpaMarketTemplate(),
        }
        self._ensure_table()

    def _ensure_table(self):
        """Ensure reports table exists."""
        create_sql = """
        CREATE TABLE IF NOT EXISTS reports (
            id SERIAL PRIMARY KEY,
            template VARCHAR(50) NOT NULL,
            title VARCHAR(255),
            format VARCHAR(10) NOT NULL,
            status VARCHAR(20) DEFAULT 'pending',
            params JSONB,
            file_path TEXT,
            file_size INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            completed_at TIMESTAMP,
            error_message TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_reports_status ON reports(status);
        CREATE INDEX IF NOT EXISTS idx_reports_created ON reports(created_at DESC);
        """
        try:
            self.db.execute(text(create_sql))
            self.db.commit()
        except Exception as e:
            logger.error(f"Error creating reports table: {e}")
            self.db.rollback()

    def get_templates(self) -> List[Dict[str, Any]]:
        """Get available report templates."""
        return [
            {
                "name": name,
                "description": template.description,
                "formats": ["html", "excel", "pdf", "pptx"],
            }
            for name, template in self.templates.items()
        ]

    def generate(
        self,
        template_name: str,
        format: str,
        params: Dict[str, Any],
        title: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Generate a report.

        Args:
            template_name: Template to use
            format: Output format (html, excel)
            params: Template parameters
            title: Optional custom title

        Returns:
            Report metadata with ID
        """
        # Validate template
        if template_name not in self.templates:
            raise ValueError(f"Unknown template: {template_name}")

        template = self.templates[template_name]

        # Validate format
        if format not in ["html", "excel", "pdf", "pptx"]:
            raise ValueError(f"Unsupported format: {format}")

        # Create report record
        import json

        insert_sql = text("""
            INSERT INTO reports (template, title, format, status, params, created_at)
            VALUES (:template, :title, :format, 'generating', :params, :created_at)
            RETURNING id
        """)

        result = self.db.execute(
            insert_sql,
            {
                "template": template_name,
                "title": title or f"{template_name} Report",
                "format": format,
                "params": json.dumps(params),
                "created_at": datetime.utcnow(),
            },
        )
        report_id = result.fetchone()[0]
        self.db.commit()

        try:
            # Gather data
            data = template.gather_data(self.db, params)
            data["report_title"] = title or f"{template_name} Report"

            # Generate file
            if format == "html":
                content = template.render_html(data)
                ext = "html"
                file_content = content.encode("utf-8")
            elif format == "pdf":
                from app.reports.pdf_renderer import render_pdf
                html_content = template.render_html(data)
                file_content = render_pdf(html_content)
                ext = "pdf"
            elif format == "pptx":
                from app.reports.pptx_renderer import render_pptx
                file_content = render_pptx(template_name, data)
                ext = "pptx"
            else:  # excel
                file_content = template.render_excel(data)
                ext = "xlsx"

            # Save file
            filename = f"report_{report_id}.{ext}"
            file_path = REPORTS_DIR / filename

            with open(file_path, "wb") as f:
                f.write(file_content)

            file_size = len(file_content)

            # Update record
            update_sql = text("""
                UPDATE reports
                SET status = 'complete',
                    file_path = :file_path,
                    file_size = :file_size,
                    completed_at = :completed_at
                WHERE id = :id
            """)
            self.db.execute(
                update_sql,
                {
                    "id": report_id,
                    "file_path": str(file_path),
                    "file_size": file_size,
                    "completed_at": datetime.utcnow(),
                },
            )
            self.db.commit()

            return self.get_report(report_id)

        except Exception as e:
            logger.error(f"Error generating report {report_id}: {e}")

            # Update with error
            error_sql = text("""
                UPDATE reports
                SET status = 'failed', error_message = :error
                WHERE id = :id
            """)
            self.db.execute(error_sql, {"id": report_id, "error": str(e)})
            self.db.commit()

            raise

    def get_report(self, report_id: int) -> Optional[Dict[str, Any]]:
        """Get report metadata."""
        result = self.db.execute(
            text("""
            SELECT id, template, title, format, status,
                   params, file_path, file_size,
                   created_at, completed_at, error_message
            FROM reports WHERE id = :id
        """),
            {"id": report_id},
        )

        row = result.fetchone()
        if not row:
            return None

        return {
            "id": row[0],
            "template": row[1],
            "title": row[2],
            "format": row[3],
            "status": row[4],
            "params": row[5],
            "file_path": row[6],
            "file_size": row[7],
            "created_at": row[8].isoformat() if row[8] else None,
            "completed_at": row[9].isoformat() if row[9] else None,
            "error_message": row[10],
            "download_url": f"/api/v1/reports/{row[0]}/download"
            if row[4] == "complete"
            else None,
        }

    def get_download_path(self, report_id: int) -> Optional[str]:
        """Get file path for download."""
        report = self.get_report(report_id)
        if report and report["status"] == "complete" and report["file_path"]:
            path = Path(report["file_path"])
            if path.exists():
                return str(path)
        return None

    def list_reports(
        self,
        limit: int = 50,
        offset: int = 0,
        template: Optional[str] = None,
        status: Optional[str] = None,
    ) -> Dict[str, Any]:
        """List generated reports."""
        conditions = []
        params = {"limit": limit, "offset": offset}

        if template:
            conditions.append("template = :template")
            params["template"] = template

        if status:
            conditions.append("status = :status")
            params["status"] = status

        where_clause = " AND ".join(conditions) if conditions else "1=1"

        # Count
        count_result = self.db.execute(
            text(f"SELECT COUNT(*) FROM reports WHERE {where_clause}"), params
        )
        total = count_result.scalar() or 0

        # Fetch
        query = text(f"""
            SELECT id, template, title, format, status, file_size,
                   created_at, completed_at
            FROM reports
            WHERE {where_clause}
            ORDER BY created_at DESC
            LIMIT :limit OFFSET :offset
        """)

        result = self.db.execute(query, params)

        reports = []
        for row in result.fetchall():
            reports.append(
                {
                    "id": row[0],
                    "template": row[1],
                    "title": row[2],
                    "format": row[3],
                    "status": row[4],
                    "file_size": row[5],
                    "created_at": row[6].isoformat() if row[6] else None,
                    "completed_at": row[7].isoformat() if row[7] else None,
                    "download_url": f"/api/v1/reports/{row[0]}/download"
                    if row[4] == "complete"
                    else None,
                }
            )

        return {
            "reports": reports,
            "total": total,
            "has_more": (offset + limit) < total,
        }

    def delete_report(self, report_id: int) -> bool:
        """Delete a report and its file."""
        report = self.get_report(report_id)
        if not report:
            return False

        # Delete file if exists
        if report.get("file_path"):
            try:
                path = Path(report["file_path"])
                if path.exists():
                    path.unlink()
            except Exception as e:
                logger.warning(f"Could not delete report file: {e}")

        # Delete record
        self.db.execute(text("DELETE FROM reports WHERE id = :id"), {"id": report_id})
        self.db.commit()

        return True
