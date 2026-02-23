"""
Investor Profile Report Template.

Generates a one-pager with investor overview, portfolio summary,
top holdings, sector allocation, and team/leadership.
"""

import logging
from datetime import datetime
from typing import Dict, Any, Optional
from io import BytesIO

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


def _format_aum(aum_millions: Optional[float]) -> str:
    """Format AUM in human-readable form (e.g. $1T, $553B, $40B)."""
    if aum_millions is None:
        return "N/A"
    if aum_millions >= 1_000_000:
        val = aum_millions / 1_000_000
        return f"${val:,.1f}T" if val != int(val) else f"${int(val)}T"
    if aum_millions >= 1_000:
        val = aum_millions / 1_000
        return f"${val:,.0f}B"
    return f"${aum_millions:,.0f}M"


class InvestorProfileTemplate:
    """Investor profile report template."""

    name = "investor_profile"
    description = "One-pager investor profile with portfolio summary"

    def gather_data(self, db: Session, params: Dict[str, Any]) -> Dict[str, Any]:
        """Gather all data needed for the report."""
        investor_id = params.get("investor_id")
        investor_type = params.get("investor_type", "lp")

        investor = self._get_investor(db, investor_id, investor_type)

        data = {
            "generated_at": datetime.utcnow().isoformat(),
            "investor": investor,
            "portfolio_summary": self._get_portfolio_summary(
                db, investor_id, investor_type, investor.get("name")
            ),
            "top_holdings": self._get_top_holdings(
                db, investor_id, investor_type, investor.get("name")
            ),
            "sector_allocation": self._get_sector_allocation(
                db, investor_id, investor_type, investor.get("name")
            ),
            "recent_activity": self._get_recent_activity(
                db, investor_id, investor_type
            ),
            "team": self._get_team(db, investor_id, investor_type),
        }

        return data

    def _get_investor(self, db: Session, investor_id: int, investor_type: str) -> Dict:
        """Get investor details."""
        if investor_type == "lp":
            result = db.execute(
                text("""
                SELECT id, name, lp_type, jurisdiction, website_url
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
                    "website": row[4],
                    "aum_millions": None,
                }
        elif investor_type == "pe_firm":
            result = db.execute(
                text("""
                SELECT id, name, firm_type, primary_strategy,
                       headquarters_city, headquarters_state, headquarters_country,
                       website, aum_usd_millions, employee_count, founded_year
                FROM pe_firms WHERE id = :id
            """),
                {"id": investor_id},
            )
            row = result.fetchone()
            if row:
                location_parts = [p for p in [row[4], row[5], row[6]] if p]
                return {
                    "id": row[0],
                    "name": row[1],
                    "type": f"{row[2]} — {row[3]}" if row[3] else row[2],
                    "jurisdiction": ", ".join(location_parts) if location_parts else None,
                    "website": row[7],
                    "aum_millions": float(row[8]) if row[8] else None,
                    "employee_count": row[9],
                    "founded_year": row[10],
                }
        else:
            result = db.execute(
                text("""
                SELECT id, name, 'family_office' as lp_type,
                       CONCAT_WS(', ', city, state_province, country) as jurisdiction,
                       website, estimated_aum_millions
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
                    "website": row[4],
                    "aum_millions": row[5],
                }

        return {"id": investor_id, "name": "Unknown", "type": investor_type}

    def _get_pe_portfolio_query(self, investor_id: int, firm_name: Optional[str]):
        """Build a UNION query that pulls portfolio companies from both linkage paths.

        Path 1: pe_funds -> pe_fund_investments -> pe_portfolio_companies (13F, SEC)
        Path 2: pe_portfolio_companies.current_pe_owner = firm name (website-scraped)
        Deduplicates by company name.
        """
        # We use a CTE to union both sources, then deduplicate preferring
        # website-scraped (source='website') over 13F
        query = """
            WITH all_companies AS (
                -- Path 1: via fund investments
                SELECT DISTINCT ON (pc.id)
                    pc.id, pc.name, pc.industry,
                    CONCAT_WS(', ', pc.headquarters_city, pc.headquarters_state) as location,
                    fi.investment_type as stage,
                    pc.website as company_website,
                    pc.ownership_status,
                    fi.investment_date,
                    CASE WHEN fi.investment_type = '13F Holding' THEN 1 ELSE 0 END as is_13f
                FROM pe_fund_investments fi
                JOIN pe_funds f ON f.id = fi.fund_id
                JOIN pe_portfolio_companies pc ON pc.id = fi.company_id
                WHERE f.firm_id = :investor_id AND fi.status = 'Active'

                UNION ALL

                -- Path 2: via current_pe_owner (website-scraped)
                SELECT
                    pc.id, pc.name, pc.industry,
                    CONCAT_WS(', ', pc.headquarters_city, pc.headquarters_state) as location,
                    'Portfolio Company' as stage,
                    pc.website as company_website,
                    pc.ownership_status,
                    pc.created_at::date as investment_date,
                    0 as is_13f
                FROM pe_portfolio_companies pc
                WHERE pc.current_pe_owner = :firm_name
                    AND pc.id NOT IN (
                        SELECT fi2.company_id FROM pe_fund_investments fi2
                        JOIN pe_funds f2 ON f2.id = fi2.fund_id
                        WHERE f2.firm_id = :investor_id
                    )
            ),
            deduped AS (
                SELECT DISTINCT ON (UPPER(name))
                    id, name, industry, location, stage, company_website,
                    ownership_status, investment_date, is_13f
                FROM all_companies
                ORDER BY UPPER(name), is_13f ASC, investment_date DESC NULLS LAST
            )
        """
        return query

    def _get_portfolio_summary(
        self, db: Session, investor_id: int, investor_type: str,
        firm_name: Optional[str] = None,
    ) -> Dict:
        """Get portfolio summary stats."""
        if investor_type == "pe_firm":
            base = self._get_pe_portfolio_query(investor_id, firm_name)
            result = db.execute(
                text(base + """
                SELECT
                    COUNT(*) as total_holdings,
                    COUNT(DISTINCT industry) FILTER (WHERE industry IS NOT NULL) as sectors,
                    COUNT(*) as current_holdings
                FROM deduped
                """),
                {"investor_id": investor_id, "firm_name": firm_name or ""},
            )
        else:
            result = db.execute(
                text("""
                SELECT
                    COUNT(*) as total_holdings,
                    COUNT(DISTINCT company_industry) as sectors,
                    COUNT(CASE WHEN current_holding = 1 THEN 1 END) as current_holdings
                FROM portfolio_companies
                WHERE investor_id = :investor_id AND investor_type = :investor_type
            """),
                {"investor_id": investor_id, "investor_type": investor_type},
            )

        row = result.fetchone()
        return {
            "total_holdings": row[0] if row else 0,
            "sectors": row[1] if row else 0,
            "current_holdings": row[2] if row else 0,
        }

    def _get_top_holdings(
        self, db: Session, investor_id: int, investor_type: str,
        firm_name: Optional[str] = None, limit: int = 10,
    ) -> list:
        """Get top portfolio holdings."""
        if investor_type == "pe_firm":
            base = self._get_pe_portfolio_query(investor_id, firm_name)
            result = db.execute(
                text(base + """
                SELECT name, industry, location, stage
                FROM deduped
                WHERE is_13f = 0
                ORDER BY name
                LIMIT :limit
                """),
                {"investor_id": investor_id, "firm_name": firm_name or "", "limit": limit},
            )
            rows = result.fetchall()
            # If no non-13F holdings, fall back to showing 13F holdings
            if not rows:
                result = db.execute(
                    text(base + """
                    SELECT name, industry, location, stage
                    FROM deduped
                    ORDER BY name
                    LIMIT :limit
                    """),
                    {"investor_id": investor_id, "firm_name": firm_name or "", "limit": limit},
                )
                rows = result.fetchall()
        else:
            result = db.execute(
                text("""
                SELECT company_name, company_industry, company_location, company_stage
                FROM portfolio_companies
                WHERE investor_id = :investor_id
                    AND investor_type = :investor_type
                    AND current_holding = 1
                ORDER BY company_name
                LIMIT :limit
            """),
                {
                    "investor_id": investor_id,
                    "investor_type": investor_type,
                    "limit": limit,
                },
            )
            rows = result.fetchall()

        return [
            {
                "name": row[0],
                "industry": row[1],
                "location": row[2],
                "stage": row[3],
            }
            for row in rows
        ]

    def _get_sector_allocation(
        self, db: Session, investor_id: int, investor_type: str,
        firm_name: Optional[str] = None,
    ) -> list:
        """Get sector allocation breakdown."""
        if investor_type == "pe_firm":
            base = self._get_pe_portfolio_query(investor_id, firm_name)
            result = db.execute(
                text(base + """
                SELECT
                    COALESCE(industry, 'Unknown') as sector,
                    COUNT(*) as count
                FROM deduped
                GROUP BY COALESCE(industry, 'Unknown')
                ORDER BY count DESC
                LIMIT 10
                """),
                {"investor_id": investor_id, "firm_name": firm_name or ""},
            )
        else:
            result = db.execute(
                text("""
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
            """),
                {"investor_id": investor_id, "investor_type": investor_type},
            )

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

    def _get_recent_activity(
        self, db: Session, investor_id: int, investor_type: str, limit: int = 5
    ) -> list:
        """Get recent portfolio changes."""
        if investor_type == "pe_firm":
            result = db.execute(
                text("""
                SELECT pc.name, pc.industry, fi.investment_date
                FROM pe_fund_investments fi
                JOIN pe_funds f ON f.id = fi.fund_id
                JOIN pe_portfolio_companies pc ON pc.id = fi.company_id
                WHERE f.firm_id = :investor_id
                ORDER BY fi.investment_date DESC NULLS LAST
                LIMIT :limit
            """),
                {"investor_id": investor_id, "limit": limit},
            )
        else:
            result = db.execute(
                text("""
                SELECT company_name, company_industry, collected_date
                FROM portfolio_companies
                WHERE investor_id = :investor_id
                    AND investor_type = :investor_type
                ORDER BY collected_date DESC
                LIMIT :limit
            """),
                {
                    "investor_id": investor_id,
                    "investor_type": investor_type,
                    "limit": limit,
                },
            )

        return [
            {
                "company": row[0],
                "industry": row[1],
                "date": row[2].isoformat() if row[2] else None,
            }
            for row in result.fetchall()
        ]

    def _get_team(
        self, db: Session, investor_id: int, investor_type: str, limit: int = 15
    ) -> list:
        """Get key team members for PE firms."""
        if investor_type != "pe_firm":
            return []

        result = db.execute(
            text("""
                SELECT p.full_name, fp.title, fp.seniority, fp.department,
                       p.linkedin_url, p.bio
                FROM pe_firm_people fp
                JOIN pe_people p ON fp.person_id = p.id
                WHERE fp.firm_id = :investor_id AND fp.is_current = true
                ORDER BY
                    CASE fp.seniority
                        WHEN 'Partner' THEN 1
                        WHEN 'Managing Director' THEN 2
                        WHEN 'Principal' THEN 3
                        WHEN 'VP' THEN 4
                        WHEN 'Associate' THEN 5
                        ELSE 6
                    END,
                    p.full_name
                LIMIT :limit
            """),
            {"investor_id": investor_id, "limit": limit},
        )

        return [
            {
                "name": row[0],
                "title": row[1],
                "seniority": row[2],
                "department": row[3],
                "linkedin": row[4],
                "bio": (row[5][:150] + "...") if row[5] and len(row[5]) > 150 else row[5],
            }
            for row in result.fetchall()
        ]

    def render_html(self, data: Dict[str, Any]) -> str:
        """Render report as HTML."""
        investor = data.get("investor", {})
        summary = data.get("portfolio_summary", {})
        holdings = data.get("top_holdings", [])
        sectors = data.get("sector_allocation", [])
        team = data.get("team", [])

        aum_display = _format_aum(investor.get("aum_millions"))

        # Build holdings table rows
        holdings_rows = ""
        for h in holdings:
            industry = h.get('industry') or '-'
            location = h.get('location') or '-'
            stage = h.get('stage') or '-'
            holdings_rows += f"""
            <tr>
                <td>{h.get('name', 'N/A')}</td>
                <td>{industry}</td>
                <td>{location}</td>
                <td>{stage}</td>
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

        # Build team table rows
        team_rows = ""
        for t in team:
            name = t.get('name', 'N/A')
            if t.get('linkedin'):
                name = f'<a href="{t["linkedin"]}" target="_blank">{name}</a>'
            bio_snippet = t.get('bio') or ''
            team_rows += f"""
            <tr>
                <td>{name}</td>
                <td>{t.get('title', '-')}</td>
                <td>{t.get('seniority') or '-'}</td>
                <td>{bio_snippet}</td>
            </tr>
            """

        # Build team section HTML
        team_section = ""
        if team:
            team_section = f"""
    <h2>Key Team Members ({len(team)})</h2>
    <table>
        <thead>
            <tr>
                <th>Name</th>
                <th>Title</th>
                <th>Seniority</th>
                <th>Bio</th>
            </tr>
        </thead>
        <tbody>
            {team_rows}
        </tbody>
    </table>
"""

        # Optional founded year / employee count line
        extra_details = ""
        if investor.get("founded_year"):
            extra_details += f'<p><strong>Founded:</strong> {investor["founded_year"]}</p>\n'
        if investor.get("employee_count"):
            extra_details += f'<p><strong>Employees:</strong> {investor["employee_count"]:,}</p>\n'

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
        td:last-child {{ max-width: 300px; font-size: 0.9em; color: #555; }}
        a {{ color: #3498db; text-decoration: none; }}
        a:hover {{ text-decoration: underline; }}
        .meta {{ color: #95a5a6; font-size: 0.9em; margin-top: 40px; }}
    </style>
</head>
<body>
    <h1>{investor.get('name', 'Unknown Investor')}</h1>

    <div class="summary-box">
        <p><strong>Type:</strong> {investor.get('type', 'N/A')}</p>
        <p><strong>Headquarters:</strong> {investor.get('jurisdiction', 'N/A')}</p>
        <p><strong>AUM:</strong> {aum_display}</p>
        <p><strong>Website:</strong> {investor.get('website', 'N/A')}</p>
        {extra_details}
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
                <th>Type</th>
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

    {team_section}

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

        # Summary sheet
        ws = wb.active
        ws.title = "Summary"

        investor = data.get("investor", {})
        summary = data.get("portfolio_summary", {})

        # Header styling
        header_font = Font(bold=True, size=14)
        header_fill = PatternFill(
            start_color="3498DB", end_color="3498DB", fill_type="solid"
        )
        header_font_white = Font(bold=True, color="FFFFFF")

        # Title
        ws["A1"] = investor.get("name", "Investor Profile")
        ws["A1"].font = Font(bold=True, size=18)
        ws.merge_cells("A1:D1")

        # Investor details
        ws["A3"] = "Type"
        ws["B3"] = investor.get("type", "N/A")
        ws["A4"] = "Headquarters"
        ws["B4"] = investor.get("jurisdiction", "N/A")
        ws["A5"] = "AUM"
        ws["B5"] = _format_aum(investor.get("aum_millions"))
        ws["A6"] = "Website"
        ws["B6"] = investor.get("website", "N/A")
        row_offset = 6
        if investor.get("founded_year"):
            row_offset += 1
            ws[f"A{row_offset}"] = "Founded"
            ws[f"B{row_offset}"] = investor["founded_year"]
        if investor.get("employee_count"):
            row_offset += 1
            ws[f"A{row_offset}"] = "Employees"
            ws[f"B{row_offset}"] = investor["employee_count"]

        # Portfolio stats
        row_offset += 2
        ws[f"A{row_offset}"] = "Portfolio Summary"
        ws[f"A{row_offset}"].font = header_font
        row_offset += 1
        ws[f"A{row_offset}"] = "Current Holdings"
        ws[f"B{row_offset}"] = summary.get("current_holdings", 0)
        row_offset += 1
        ws[f"A{row_offset}"] = "Sectors"
        ws[f"B{row_offset}"] = summary.get("sectors", 0)
        row_offset += 1
        ws[f"A{row_offset}"] = "Total Holdings"
        ws[f"B{row_offset}"] = summary.get("total_holdings", 0)

        ws.column_dimensions["A"].width = 20
        ws.column_dimensions["B"].width = 40

        # Holdings sheet
        ws_holdings = wb.create_sheet("Holdings")
        holdings = data.get("top_holdings", [])

        headers = ["Company", "Industry", "Location", "Type"]
        for col, header in enumerate(headers, 1):
            cell = ws_holdings.cell(row=1, column=col, value=header)
            cell.font = header_font_white
            cell.fill = header_fill

        for row, h in enumerate(holdings, 2):
            ws_holdings.cell(row=row, column=1, value=h.get("name"))
            ws_holdings.cell(row=row, column=2, value=h.get("industry") or "-")
            ws_holdings.cell(row=row, column=3, value=h.get("location") or "-")
            ws_holdings.cell(row=row, column=4, value=h.get("stage") or "-")

        ws_holdings.column_dimensions["A"].width = 30
        ws_holdings.column_dimensions["B"].width = 20
        ws_holdings.column_dimensions["C"].width = 20
        ws_holdings.column_dimensions["D"].width = 18

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

        # Team sheet (PE firms only)
        team = data.get("team", [])
        if team:
            ws_team = wb.create_sheet("Team")
            headers = ["Name", "Title", "Seniority", "Department", "LinkedIn", "Bio"]
            for col, header in enumerate(headers, 1):
                cell = ws_team.cell(row=1, column=col, value=header)
                cell.font = header_font_white
                cell.fill = header_fill

            for row, t in enumerate(team, 2):
                ws_team.cell(row=row, column=1, value=t.get("name"))
                ws_team.cell(row=row, column=2, value=t.get("title") or "-")
                ws_team.cell(row=row, column=3, value=t.get("seniority") or "-")
                ws_team.cell(row=row, column=4, value=t.get("department") or "-")
                ws_team.cell(row=row, column=5, value=t.get("linkedin") or "")
                ws_team.cell(row=row, column=6, value=t.get("bio") or "")

            ws_team.column_dimensions["A"].width = 25
            ws_team.column_dimensions["B"].width = 30
            ws_team.column_dimensions["C"].width = 15
            ws_team.column_dimensions["D"].width = 15
            ws_team.column_dimensions["E"].width = 35
            ws_team.column_dimensions["F"].width = 50

        # Save to bytes
        output = BytesIO()
        wb.save(output)
        return output.getvalue()
