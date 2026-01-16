"""
Investor Profile Report Template.

Generates a one-pager with investor overview, portfolio summary,
top holdings, and sector allocation.
"""

import logging
from datetime import datetime
from typing import Dict, Any, Optional
from io import BytesIO

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


class InvestorProfileTemplate:
    """Investor profile report template."""

    name = "investor_profile"
    description = "One-pager investor profile with portfolio summary"

    def gather_data(self, db: Session, params: Dict[str, Any]) -> Dict[str, Any]:
        """Gather all data needed for the report."""
        investor_id = params.get("investor_id")
        investor_type = params.get("investor_type", "lp")

        data = {
            "generated_at": datetime.utcnow().isoformat(),
            "investor": self._get_investor(db, investor_id, investor_type),
            "portfolio_summary": self._get_portfolio_summary(db, investor_id, investor_type),
            "top_holdings": self._get_top_holdings(db, investor_id, investor_type),
            "sector_allocation": self._get_sector_allocation(db, investor_id, investor_type),
            "recent_activity": self._get_recent_activity(db, investor_id, investor_type),
        }

        return data

    def _get_investor(self, db: Session, investor_id: int, investor_type: str) -> Dict:
        """Get investor details."""
        if investor_type == "lp":
            result = db.execute(text("""
                SELECT id, name, lp_type, jurisdiction, website_url
                FROM lp_fund WHERE id = :id
            """), {"id": investor_id})
            row = result.fetchone()
            if row:
                return {
                    "id": row[0],
                    "name": row[1],
                    "type": row[2],
                    "jurisdiction": row[3],
                    "website": row[4],
                    "aum_millions": None,
                }
        else:
            result = db.execute(text("""
                SELECT id, name, 'family_office' as lp_type, location as jurisdiction,
                       website, estimated_aum_millions
                FROM family_offices WHERE id = :id
            """), {"id": investor_id})
            row = result.fetchone()
            if row:
                return {
                    "id": row[0],
                    "name": row[1],
                    "type": row[2],
                    "jurisdiction": row[3],
                    "website": row[4],
                    "aum_millions": row[5],
                }

        return {"id": investor_id, "name": "Unknown", "type": investor_type}

    def _get_portfolio_summary(self, db: Session, investor_id: int, investor_type: str) -> Dict:
        """Get portfolio summary stats."""
        result = db.execute(text("""
            SELECT
                COUNT(*) as total_holdings,
                COUNT(DISTINCT company_industry) as sectors,
                COUNT(CASE WHEN current_holding = 1 THEN 1 END) as current_holdings
            FROM portfolio_companies
            WHERE investor_id = :investor_id AND investor_type = :investor_type
        """), {"investor_id": investor_id, "investor_type": investor_type})

        row = result.fetchone()
        return {
            "total_holdings": row[0] if row else 0,
            "sectors": row[1] if row else 0,
            "current_holdings": row[2] if row else 0,
        }

    def _get_top_holdings(self, db: Session, investor_id: int, investor_type: str, limit: int = 10) -> list:
        """Get top portfolio holdings."""
        result = db.execute(text("""
            SELECT company_name, company_industry, company_location, company_stage
            FROM portfolio_companies
            WHERE investor_id = :investor_id
                AND investor_type = :investor_type
                AND current_holding = 1
            ORDER BY company_name
            LIMIT :limit
        """), {"investor_id": investor_id, "investor_type": investor_type, "limit": limit})

        return [
            {
                "name": row[0],
                "industry": row[1],
                "location": row[2],
                "stage": row[3],
            }
            for row in result.fetchall()
        ]

    def _get_sector_allocation(self, db: Session, investor_id: int, investor_type: str) -> list:
        """Get sector allocation breakdown."""
        result = db.execute(text("""
            SELECT
                COALESCE(company_industry, 'Unknown') as sector,
                COUNT(*) as count
            FROM portfolio_companies
            WHERE investor_id = :investor_id
                AND investor_type = :investor_type
                AND current_holding = 1
            GROUP BY company_industry
            ORDER BY count DESC
            LIMIT 10
        """), {"investor_id": investor_id, "investor_type": investor_type})

        rows = result.fetchall()
        total = sum(r[1] for r in rows)

        return [
            {
                "sector": row[0],
                "count": row[1],
                "pct": round(row[1] / total * 100, 1) if total > 0 else 0,
            }
            for row in rows
        ]

    def _get_recent_activity(self, db: Session, investor_id: int, investor_type: str, limit: int = 5) -> list:
        """Get recent portfolio changes."""
        result = db.execute(text("""
            SELECT company_name, company_industry, collected_date
            FROM portfolio_companies
            WHERE investor_id = :investor_id
                AND investor_type = :investor_type
            ORDER BY collected_date DESC
            LIMIT :limit
        """), {"investor_id": investor_id, "investor_type": investor_type, "limit": limit})

        return [
            {
                "company": row[0],
                "industry": row[1],
                "date": row[2].isoformat() if row[2] else None,
            }
            for row in result.fetchall()
        ]

    def render_html(self, data: Dict[str, Any]) -> str:
        """Render report as HTML."""
        investor = data.get("investor", {})
        summary = data.get("portfolio_summary", {})
        holdings = data.get("top_holdings", [])
        sectors = data.get("sector_allocation", [])

        # Build holdings table rows
        holdings_rows = ""
        for h in holdings:
            holdings_rows += f"""
            <tr>
                <td>{h.get('name', 'N/A')}</td>
                <td>{h.get('industry', 'N/A')}</td>
                <td>{h.get('location', 'N/A')}</td>
                <td>{h.get('stage', 'N/A')}</td>
            </tr>
            """

        # Build sector table rows
        sector_rows = ""
        for s in sectors:
            sector_rows += f"""
            <tr>
                <td>{s.get('sector', 'N/A')}</td>
                <td>{s.get('count', 0)}</td>
                <td>{s.get('pct', 0)}%</td>
            </tr>
            """

        html = f"""
<!DOCTYPE html>
<html>
<head>
    <title>{investor.get('name', 'Investor')} - Profile Report</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 40px; color: #333; }}
        h1 {{ color: #2c3e50; border-bottom: 2px solid #3498db; padding-bottom: 10px; }}
        h2 {{ color: #34495e; margin-top: 30px; }}
        .summary-box {{ background: #f8f9fa; padding: 20px; border-radius: 8px; margin: 20px 0; }}
        .summary-grid {{ display: grid; grid-template-columns: repeat(3, 1fr); gap: 20px; }}
        .stat {{ text-align: center; }}
        .stat-value {{ font-size: 2em; font-weight: bold; color: #3498db; }}
        .stat-label {{ color: #7f8c8d; }}
        table {{ width: 100%; border-collapse: collapse; margin: 15px 0; }}
        th, td {{ padding: 12px; text-align: left; border-bottom: 1px solid #ddd; }}
        th {{ background: #3498db; color: white; }}
        tr:hover {{ background: #f5f5f5; }}
        .meta {{ color: #95a5a6; font-size: 0.9em; margin-top: 40px; }}
    </style>
</head>
<body>
    <h1>{investor.get('name', 'Unknown Investor')}</h1>

    <div class="summary-box">
        <p><strong>Type:</strong> {investor.get('type', 'N/A')}</p>
        <p><strong>Jurisdiction:</strong> {investor.get('jurisdiction', 'N/A')}</p>
        <p><strong>AUM:</strong> ${investor.get('aum_millions', 'N/A')}M</p>
        <p><strong>Website:</strong> {investor.get('website', 'N/A')}</p>
    </div>

    <h2>Portfolio Summary</h2>
    <div class="summary-box">
        <div class="summary-grid">
            <div class="stat">
                <div class="stat-value">{summary.get('current_holdings', 0)}</div>
                <div class="stat-label">Current Holdings</div>
            </div>
            <div class="stat">
                <div class="stat-value">{summary.get('sectors', 0)}</div>
                <div class="stat-label">Sectors</div>
            </div>
            <div class="stat">
                <div class="stat-value">{summary.get('total_holdings', 0)}</div>
                <div class="stat-label">Total (incl. exited)</div>
            </div>
        </div>
    </div>

    <h2>Top Holdings</h2>
    <table>
        <thead>
            <tr>
                <th>Company</th>
                <th>Industry</th>
                <th>Location</th>
                <th>Stage</th>
            </tr>
        </thead>
        <tbody>
            {holdings_rows if holdings_rows else '<tr><td colspan="4">No holdings data</td></tr>'}
        </tbody>
    </table>

    <h2>Sector Allocation</h2>
    <table>
        <thead>
            <tr>
                <th>Sector</th>
                <th>Count</th>
                <th>% of Portfolio</th>
            </tr>
        </thead>
        <tbody>
            {sector_rows if sector_rows else '<tr><td colspan="3">No sector data</td></tr>'}
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
        from openpyxl.styles import Font, PatternFill, Alignment

        wb = Workbook()

        # Summary sheet
        ws = wb.active
        ws.title = "Summary"

        investor = data.get("investor", {})
        summary = data.get("portfolio_summary", {})

        # Header styling
        header_font = Font(bold=True, size=14)
        header_fill = PatternFill(start_color="3498DB", end_color="3498DB", fill_type="solid")
        header_font_white = Font(bold=True, color="FFFFFF")

        # Title
        ws["A1"] = investor.get("name", "Investor Profile")
        ws["A1"].font = Font(bold=True, size=18)
        ws.merge_cells("A1:D1")

        # Investor details
        ws["A3"] = "Type"
        ws["B3"] = investor.get("type", "N/A")
        ws["A4"] = "Jurisdiction"
        ws["B4"] = investor.get("jurisdiction", "N/A")
        ws["A5"] = "AUM (Millions)"
        ws["B5"] = investor.get("aum_millions", "N/A")
        ws["A6"] = "Website"
        ws["B6"] = investor.get("website", "N/A")

        # Portfolio stats
        ws["A8"] = "Portfolio Summary"
        ws["A8"].font = header_font
        ws["A9"] = "Current Holdings"
        ws["B9"] = summary.get("current_holdings", 0)
        ws["A10"] = "Sectors"
        ws["B10"] = summary.get("sectors", 0)
        ws["A11"] = "Total Holdings"
        ws["B11"] = summary.get("total_holdings", 0)

        # Holdings sheet
        ws_holdings = wb.create_sheet("Holdings")
        holdings = data.get("top_holdings", [])

        # Headers
        headers = ["Company", "Industry", "Location", "Stage"]
        for col, header in enumerate(headers, 1):
            cell = ws_holdings.cell(row=1, column=col, value=header)
            cell.font = header_font_white
            cell.fill = header_fill

        # Data
        for row, h in enumerate(holdings, 2):
            ws_holdings.cell(row=row, column=1, value=h.get("name"))
            ws_holdings.cell(row=row, column=2, value=h.get("industry"))
            ws_holdings.cell(row=row, column=3, value=h.get("location"))
            ws_holdings.cell(row=row, column=4, value=h.get("stage"))

        # Adjust column widths
        ws_holdings.column_dimensions["A"].width = 30
        ws_holdings.column_dimensions["B"].width = 20
        ws_holdings.column_dimensions["C"].width = 20
        ws_holdings.column_dimensions["D"].width = 15

        # Sectors sheet
        ws_sectors = wb.create_sheet("Sectors")
        sectors = data.get("sector_allocation", [])

        headers = ["Sector", "Count", "Percentage"]
        for col, header in enumerate(headers, 1):
            cell = ws_sectors.cell(row=1, column=col, value=header)
            cell.font = header_font_white
            cell.fill = header_fill

        for row, s in enumerate(sectors, 2):
            ws_sectors.cell(row=row, column=1, value=s.get("sector"))
            ws_sectors.cell(row=row, column=2, value=s.get("count"))
            ws_sectors.cell(row=row, column=3, value=f"{s.get('pct', 0)}%")

        ws_sectors.column_dimensions["A"].width = 25
        ws_sectors.column_dimensions["B"].width = 10
        ws_sectors.column_dimensions["C"].width = 12

        # Save to bytes
        output = BytesIO()
        wb.save(output)
        return output.getvalue()
