"""
Portfolio Detail Report Template.

Generates a detailed portfolio breakdown with all holdings,
sector distribution, stage breakdown, and recent changes.
"""

import logging
from datetime import datetime
from typing import Dict, Any, List
from io import BytesIO

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


class PortfolioDetailTemplate:
    """Portfolio detail report template."""

    name = "portfolio_detail"
    description = "Detailed portfolio breakdown with holdings and analytics"

    def gather_data(self, db: Session, params: Dict[str, Any]) -> Dict[str, Any]:
        """Gather all data needed for the report."""
        investor_id = params.get("investor_id")
        investor_type = params.get("investor_type", "lp")

        data = {
            "generated_at": datetime.utcnow().isoformat(),
            "investor": self._get_investor(db, investor_id, investor_type),
            "holdings": self._get_all_holdings(db, investor_id, investor_type),
            "sector_breakdown": self._get_sector_breakdown(
                db, investor_id, investor_type
            ),
            "stage_breakdown": self._get_stage_breakdown(
                db, investor_id, investor_type
            ),
            "location_breakdown": self._get_location_breakdown(
                db, investor_id, investor_type
            ),
        }

        return data

    def _get_investor(self, db: Session, investor_id: int, investor_type: str) -> Dict:
        """Get investor details."""
        if investor_type == "lp":
            result = db.execute(
                text("""
                SELECT id, name, lp_type, jurisdiction
                FROM lp_fund WHERE id = :id
            """),
                {"id": investor_id},
            )
            row = result.fetchone()
            if row:
                return {
                    "id": row[0],
                    "name": row[1],
                    "type": row[2],
                    "jurisdiction": row[3],
                }
        else:
            result = db.execute(
                text("""
                SELECT id, name, 'family_office' as lp_type, location as jurisdiction
                FROM family_offices WHERE id = :id
            """),
                {"id": investor_id},
            )
            row = result.fetchone()
            if row:
                return {
                    "id": row[0],
                    "name": row[1],
                    "type": row[2],
                    "jurisdiction": row[3],
                }

        return {"id": investor_id, "name": "Unknown", "type": investor_type}

    def _get_all_holdings(
        self, db: Session, investor_id: int, investor_type: str
    ) -> List[Dict]:
        """Get all portfolio holdings."""
        result = db.execute(
            text("""
            SELECT
                company_name,
                company_industry,
                company_location,
                company_stage,
                current_holding,
                collected_date
            FROM portfolio_companies
            WHERE investor_id = :investor_id AND investor_type = :investor_type
            ORDER BY current_holding DESC, company_name
        """),
            {"investor_id": investor_id, "investor_type": investor_type},
        )

        return [
            {
                "name": row[0],
                "industry": row[1] or "Unknown",
                "location": row[2] or "Unknown",
                "stage": row[3] or "Unknown",
                "current": row[4] == 1,
                "collected_date": row[5].isoformat() if row[5] else None,
            }
            for row in result.fetchall()
        ]

    def _get_sector_breakdown(
        self, db: Session, investor_id: int, investor_type: str
    ) -> List[Dict]:
        """Get sector breakdown."""
        result = db.execute(
            text("""
            SELECT
                COALESCE(company_industry, 'Unknown') as sector,
                COUNT(*) as total,
                COUNT(CASE WHEN current_holding = 1 THEN 1 END) as current
            FROM portfolio_companies
            WHERE investor_id = :investor_id AND investor_type = :investor_type
            GROUP BY company_industry
            ORDER BY total DESC
        """),
            {"investor_id": investor_id, "investor_type": investor_type},
        )

        rows = result.fetchall()
        total = sum(r[1] for r in rows)

        return [
            {
                "sector": row[0],
                "total": row[1],
                "current": row[2],
                "pct": round(row[1] / total * 100, 1) if total > 0 else 0,
            }
            for row in rows
        ]

    def _get_stage_breakdown(
        self, db: Session, investor_id: int, investor_type: str
    ) -> List[Dict]:
        """Get stage breakdown."""
        result = db.execute(
            text("""
            SELECT
                COALESCE(company_stage, 'Unknown') as stage,
                COUNT(*) as total,
                COUNT(CASE WHEN current_holding = 1 THEN 1 END) as current
            FROM portfolio_companies
            WHERE investor_id = :investor_id AND investor_type = :investor_type
            GROUP BY company_stage
            ORDER BY total DESC
        """),
            {"investor_id": investor_id, "investor_type": investor_type},
        )

        rows = result.fetchall()
        total = sum(r[1] for r in rows)

        return [
            {
                "stage": row[0],
                "total": row[1],
                "current": row[2],
                "pct": round(row[1] / total * 100, 1) if total > 0 else 0,
            }
            for row in rows
        ]

    def _get_location_breakdown(
        self, db: Session, investor_id: int, investor_type: str
    ) -> List[Dict]:
        """Get location breakdown."""
        result = db.execute(
            text("""
            SELECT
                COALESCE(company_location, 'Unknown') as location,
                COUNT(*) as total
            FROM portfolio_companies
            WHERE investor_id = :investor_id AND investor_type = :investor_type
            GROUP BY company_location
            ORDER BY total DESC
            LIMIT 15
        """),
            {"investor_id": investor_id, "investor_type": investor_type},
        )

        rows = result.fetchall()
        total = sum(r[1] for r in rows)

        return [
            {
                "location": row[0],
                "total": row[1],
                "pct": round(row[1] / total * 100, 1) if total > 0 else 0,
            }
            for row in rows
        ]

    def render_html(self, data: Dict[str, Any]) -> str:
        """Render report as HTML."""
        holdings = data.get("holdings", [])
        sectors = data.get("sector_breakdown", [])
        stages = data.get("stage_breakdown", [])

        # Build holdings rows
        holdings_rows = ""
        for h in holdings:
            status = "Current" if h.get("current") else "Exited"
            holdings_rows += f"""
            <tr>
                <td>{h.get('name', 'N/A')}</td>
                <td>{h.get('industry', 'N/A')}</td>
                <td>{h.get('location', 'N/A')}</td>
                <td>{h.get('stage', 'N/A')}</td>
                <td>{status}</td>
            </tr>
            """

        # Build sector rows
        sector_rows = ""
        for s in sectors:
            sector_rows += f"""
            <tr>
                <td>{s.get('sector', 'N/A')}</td>
                <td>{s.get('current', 0)}</td>
                <td>{s.get('total', 0)}</td>
                <td>{s.get('pct', 0)}%</td>
            </tr>
            """

        # Build stage rows
        stage_rows = ""
        for s in stages:
            stage_rows += f"""
            <tr>
                <td>{s.get('stage', 'N/A')}</td>
                <td>{s.get('current', 0)}</td>
                <td>{s.get('total', 0)}</td>
                <td>{s.get('pct', 0)}%</td>
            </tr>
            """

        html = f"""
<!DOCTYPE html>
<html>
<head>
    <title>{investor.get('name', 'Portfolio')} - Detail Report</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 40px; color: #333; }}
        h1 {{ color: #2c3e50; border-bottom: 2px solid #27ae60; padding-bottom: 10px; }}
        h2 {{ color: #34495e; margin-top: 30px; }}
        table {{ width: 100%; border-collapse: collapse; margin: 15px 0; }}
        th, td {{ padding: 10px; text-align: left; border-bottom: 1px solid #ddd; }}
        th {{ background: #27ae60; color: white; }}
        tr:hover {{ background: #f5f5f5; }}
        .stats {{ display: flex; gap: 20px; margin: 20px 0; }}
        .stat-box {{ background: #f8f9fa; padding: 15px; border-radius: 8px; text-align: center; flex: 1; }}
        .stat-value {{ font-size: 1.5em; font-weight: bold; color: #27ae60; }}
        .meta {{ color: #95a5a6; font-size: 0.9em; margin-top: 40px; }}
    </style>
</head>
<body>
    <h1>{investor.get('name', 'Unknown')} - Portfolio Detail</h1>

    <div class="stats">
        <div class="stat-box">
            <div class="stat-value">{len([h for h in holdings if h.get('current')])}</div>
            <div>Current Holdings</div>
        </div>
        <div class="stat-box">
            <div class="stat-value">{len(holdings)}</div>
            <div>Total Holdings</div>
        </div>
        <div class="stat-box">
            <div class="stat-value">{len(sectors)}</div>
            <div>Sectors</div>
        </div>
    </div>

    <h2>All Holdings</h2>
    <table>
        <thead>
            <tr>
                <th>Company</th>
                <th>Industry</th>
                <th>Location</th>
                <th>Stage</th>
                <th>Status</th>
            </tr>
        </thead>
        <tbody>
            {holdings_rows if holdings_rows else '<tr><td colspan="5">No holdings</td></tr>'}
        </tbody>
    </table>

    <h2>Sector Breakdown</h2>
    <table>
        <thead>
            <tr>
                <th>Sector</th>
                <th>Current</th>
                <th>Total</th>
                <th>% of Portfolio</th>
            </tr>
        </thead>
        <tbody>
            {sector_rows if sector_rows else '<tr><td colspan="4">No data</td></tr>'}
        </tbody>
    </table>

    <h2>Stage Breakdown</h2>
    <table>
        <thead>
            <tr>
                <th>Stage</th>
                <th>Current</th>
                <th>Total</th>
                <th>% of Portfolio</th>
            </tr>
        </thead>
        <tbody>
            {stage_rows if stage_rows else '<tr><td colspan="4">No data</td></tr>'}
        </tbody>
    </table>

    <p class="meta">Generated: {data.get('generated_at', 'N/A')} | Nexdata Investment Intelligence</p>
</body>
</html>
        """
        return html

    def render_excel(self, data: Dict[str, Any]) -> bytes:
        """Render report as Excel workbook."""
        from openpyxl import Workbook
        from openpyxl.styles import Font, PatternFill

        wb = Workbook()

        header_fill = PatternFill(
            start_color="27AE60", end_color="27AE60", fill_type="solid"
        )
        header_font = Font(bold=True, color="FFFFFF")

        # Holdings sheet
        ws = wb.active
        ws.title = "Holdings"

        holdings = data.get("holdings", [])
        headers = ["Company", "Industry", "Location", "Stage", "Status"]

        for col, header in enumerate(headers, 1):
            cell = ws.cell(row=1, column=col, value=header)
            cell.font = header_font
            cell.fill = header_fill

        for row, h in enumerate(holdings, 2):
            ws.cell(row=row, column=1, value=h.get("name"))
            ws.cell(row=row, column=2, value=h.get("industry"))
            ws.cell(row=row, column=3, value=h.get("location"))
            ws.cell(row=row, column=4, value=h.get("stage"))
            ws.cell(
                row=row, column=5, value="Current" if h.get("current") else "Exited"
            )

        for col in ["A", "B", "C", "D", "E"]:
            ws.column_dimensions[col].width = 20

        # Sectors sheet
        ws_sectors = wb.create_sheet("Sectors")
        sectors = data.get("sector_breakdown", [])

        headers = ["Sector", "Current", "Total", "Percentage"]
        for col, header in enumerate(headers, 1):
            cell = ws_sectors.cell(row=1, column=col, value=header)
            cell.font = header_font
            cell.fill = header_fill

        for row, s in enumerate(sectors, 2):
            ws_sectors.cell(row=row, column=1, value=s.get("sector"))
            ws_sectors.cell(row=row, column=2, value=s.get("current"))
            ws_sectors.cell(row=row, column=3, value=s.get("total"))
            ws_sectors.cell(row=row, column=4, value=f"{s.get('pct', 0)}%")

        ws_sectors.column_dimensions["A"].width = 25

        # Stages sheet
        ws_stages = wb.create_sheet("Stages")
        stages = data.get("stage_breakdown", [])

        for col, header in enumerate(headers, 1):
            cell = ws_stages.cell(
                row=1, column=col, value=header.replace("Sector", "Stage")
            )
            cell.font = header_font
            cell.fill = header_fill

        for row, s in enumerate(stages, 2):
            ws_stages.cell(row=row, column=1, value=s.get("stage"))
            ws_stages.cell(row=row, column=2, value=s.get("current"))
            ws_stages.cell(row=row, column=3, value=s.get("total"))
            ws_stages.cell(row=row, column=4, value=f"{s.get('pct', 0)}%")

        ws_stages.column_dimensions["A"].width = 20

        # Save
        output = BytesIO()
        wb.save(output)
        return output.getvalue()
