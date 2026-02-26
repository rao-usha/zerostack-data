"""
Deep Data Quality Report Template.

Extends the basic quality report with profiling insights, anomaly alerts,
cross-source validation results, quality trending, and SLA compliance.
"""

import logging
from datetime import datetime, date, timedelta, timezone
from typing import Dict, Any, List, Optional
from io import BytesIO

from sqlalchemy import func
from sqlalchemy.orm import Session

from app.reports.design_system import (
    html_document, page_header, kpi_strip, kpi_card,
    toc, section_start, section_end,
    data_table, pill_badge, callout,
    chart_container, chart_init_js, page_footer,
    GREEN, ORANGE, RED, GRAY, BLUE,
)
from app.core.models import (
    DataProfileSnapshot,
    DQAnomalyAlert,
    DQCrossSourceValidation,
    DQCrossSourceResult,
    DQQualitySnapshot,
    DQSLATarget,
    AnomalyAlertStatus,
)

logger = logging.getLogger(__name__)

GRADE_THRESHOLDS = [
    (97, "A+"), (93, "A"), (90, "A-"),
    (87, "B+"), (83, "B"), (80, "B-"),
    (77, "C+"), (73, "C"), (70, "C-"),
    (60, "D"), (0, "F"),
]


def _letter_grade(score: float) -> str:
    for threshold, grade in GRADE_THRESHOLDS:
        if score >= threshold:
            return grade
    return "F"


def _fmt_number(n) -> str:
    if n is None:
        return "0"
    return f"{int(n):,}"


def _score_color(score: Optional[float]) -> str:
    if score is None:
        return GRAY
    if score >= 90:
        return GREEN
    if score >= 70:
        return ORANGE
    return RED


def _status_pill(status: str) -> str:
    if status in ("Good", "passed", "PASSED"):
        return pill_badge("Good", "private")
    elif status in ("Warning", "warning"):
        return pill_badge("Warning", "pe")
    elif status in ("Critical", "failed", "FAILED"):
        return pill_badge("Critical", "default")
    return pill_badge(status, "default")


class DataQualityDeepTemplate:
    """Deep data quality report with profiling, anomalies, cross-source, and trending."""

    name = "data_quality_deep"
    description = "Deep data quality analysis with profiling, anomalies, cross-source validation, and trending"

    def gather_data(self, db: Session, params: Dict[str, Any]) -> Dict[str, Any]:
        """Gather all deep quality data."""
        now = datetime.now(timezone.utc)
        today = date.today()
        window_days = params.get("window_days", 30)
        cutoff = today - timedelta(days=window_days)

        data = {
            "generated_at": now.isoformat(),
            "window_days": window_days,
        }

        # 1. Platform quality score (latest snapshots)
        quality_snapshots = (
            db.query(DQQualitySnapshot)
            .filter(DQQualitySnapshot.snapshot_date == today)
            .all()
        )
        if not quality_snapshots:
            # Try yesterday
            quality_snapshots = (
                db.query(DQQualitySnapshot)
                .filter(DQQualitySnapshot.snapshot_date == today - timedelta(days=1))
                .all()
            )

        scores = [s.quality_score for s in quality_snapshots if s.quality_score is not None]
        platform_score = sum(scores) / len(scores) if scores else None
        data["platform_score"] = round(platform_score, 1) if platform_score else None
        data["platform_grade"] = _letter_grade(platform_score) if platform_score else "N/A"
        data["tables_tracked"] = len(quality_snapshots)

        # Dimension averages
        for dim in ["completeness", "freshness", "validity", "consistency"]:
            vals = [getattr(s, f"{dim}_score") for s in quality_snapshots if getattr(s, f"{dim}_score") is not None]
            data[f"avg_{dim}"] = round(sum(vals) / len(vals), 1) if vals else None

        # 2. Domain breakdown with scores
        domains: Dict[str, List] = {}
        for s in quality_snapshots:
            domain = s.domain or "other"
            domains.setdefault(domain, []).append(s)

        data["domains"] = []
        for domain, snaps in sorted(domains.items()):
            d_scores = [s.quality_score for s in snaps if s.quality_score is not None]
            data["domains"].append({
                "name": domain,
                "tables": len(snaps),
                "avg_score": round(sum(d_scores) / len(d_scores), 1) if d_scores else None,
                "total_rows": sum(s.row_count or 0 for s in snaps),
            })

        # 3. Anomaly alerts summary
        open_anomalies = (
            db.query(DQAnomalyAlert)
            .filter(DQAnomalyAlert.status == AnomalyAlertStatus.OPEN)
            .count()
        )
        recent_anomalies = (
            db.query(DQAnomalyAlert)
            .filter(DQAnomalyAlert.detected_at >= now - timedelta(days=7))
            .order_by(DQAnomalyAlert.detected_at.desc())
            .limit(20)
            .all()
        )
        data["open_anomalies"] = open_anomalies
        data["recent_anomalies"] = [
            {
                "table": a.table_name,
                "type": a.alert_type.value if a.alert_type else "",
                "severity": a.severity.value if a.severity else "",
                "message": a.message or "",
                "detected": a.detected_at.strftime("%Y-%m-%d %H:%M") if a.detected_at else "",
                "status": a.status.value if a.status else "",
            }
            for a in recent_anomalies
        ]

        # 4. Cross-source validation results
        validations = db.query(DQCrossSourceValidation).filter(
            DQCrossSourceValidation.is_enabled == 1
        ).all()
        data["cross_source"] = []
        for v in validations:
            latest_result = (
                db.query(DQCrossSourceResult)
                .filter(DQCrossSourceResult.validation_id == v.id)
                .order_by(DQCrossSourceResult.evaluated_at.desc())
                .first()
            )
            data["cross_source"].append({
                "name": v.name,
                "type": v.validation_type,
                "pass_rate": round(latest_result.match_rate * 100, 1) if latest_result and latest_result.match_rate else None,
                "passed": bool(latest_result.passed) if latest_result else None,
                "matched": latest_result.matched_count if latest_result else None,
                "orphan_left": latest_result.orphan_left if latest_result else None,
                "orphan_right": latest_result.orphan_right if latest_result else None,
                "last_run": latest_result.evaluated_at.strftime("%Y-%m-%d %H:%M") if latest_result and latest_result.evaluated_at else "Never",
            })

        # 5. Quality trending (last N days)
        trend_data = (
            db.query(
                DQQualitySnapshot.snapshot_date,
                func.avg(DQQualitySnapshot.quality_score).label("avg_quality"),
                func.avg(DQQualitySnapshot.completeness_score).label("avg_completeness"),
                func.avg(DQQualitySnapshot.freshness_score).label("avg_freshness"),
                func.avg(DQQualitySnapshot.validity_score).label("avg_validity"),
                func.count(DQQualitySnapshot.id).label("count"),
            )
            .filter(DQQualitySnapshot.snapshot_date >= cutoff)
            .group_by(DQQualitySnapshot.snapshot_date)
            .order_by(DQQualitySnapshot.snapshot_date.asc())
            .all()
        )
        data["trend"] = [
            {
                "date": str(t.snapshot_date),
                "quality": round(t.avg_quality, 1) if t.avg_quality else None,
                "completeness": round(t.avg_completeness, 1) if t.avg_completeness else None,
                "freshness": round(t.avg_freshness, 1) if t.avg_freshness else None,
                "validity": round(t.avg_validity, 1) if t.avg_validity else None,
                "tables": t.count,
            }
            for t in trend_data
        ]

        # 6. SLA compliance
        targets = db.query(DQSLATarget).filter(DQSLATarget.is_enabled == 1).all()
        data["sla_targets"] = len(targets)
        sla_met = 0
        sla_results = []
        for target in targets:
            matching = [s for s in quality_snapshots if not target.source or s.source == target.source]
            for s in matching:
                met = (s.quality_score or 0) >= target.target_quality_score
                if met:
                    sla_met += 1
                sla_results.append({
                    "source": s.source,
                    "table": s.table_name,
                    "score": s.quality_score,
                    "target": target.target_quality_score,
                    "met": met,
                })
        data["sla_results"] = sla_results
        data["sla_met"] = sla_met
        data["sla_total"] = len(sla_results)

        # 7. Recent profiles
        recent_profiles = (
            db.query(DataProfileSnapshot)
            .order_by(DataProfileSnapshot.profiled_at.desc())
            .limit(20)
            .all()
        )
        data["recent_profiles"] = [
            {
                "table": p.table_name,
                "rows": p.row_count,
                "columns": p.column_count,
                "completeness": round(p.overall_completeness_pct, 1) if p.overall_completeness_pct else None,
                "profiled_at": p.profiled_at.strftime("%Y-%m-%d %H:%M") if p.profiled_at else "",
                "time_ms": p.execution_time_ms,
            }
            for p in recent_profiles
        ]

        return data

    def render_html(self, data: Dict[str, Any]) -> str:
        """Render the deep quality report as HTML."""
        charts_js = ""
        body = ""

        platform_score = data.get("platform_score")
        platform_grade = data.get("platform_grade", "N/A")

        # ── Page Header ──
        body += page_header(
            title="Deep Data Quality Report",
            subtitle=f"{data.get('tables_tracked', 0)} tables tracked | "
                     f"{data.get('window_days', 30)}-day analysis window",
        )

        # ── Section 1: Platform Quality Score Dashboard ──
        body += section_start("platform-quality", "1", "Platform Quality Score")

        kpis = [
            kpi_card(
                "Quality Score",
                f"{platform_score:.1f}" if platform_score else "N/A",
                delta=f"Grade: {platform_grade}",
            ),
            kpi_card(
                "Completeness",
                f"{data.get('avg_completeness', 'N/A')}",
                delta="30% weight",
            ),
            kpi_card(
                "Freshness",
                f"{data.get('avg_freshness', 'N/A')}",
                delta="20% weight",
            ),
            kpi_card(
                "Validity",
                f"{data.get('avg_validity', 'N/A')}",
                delta="30% weight",
            ),
            kpi_card(
                "Consistency",
                f"{data.get('avg_consistency', 'N/A')}",
                delta="20% weight",
            ),
        ]
        body += kpi_strip(kpis)
        body += section_end()

        # ── Section 2: Domain Breakdown ──
        body += section_start("domains", "2", "Domain Breakdown")
        domains = data.get("domains", [])
        if domains:
            headers = ["Domain", "Tables", "Avg Score", "Total Rows"]
            rows = []
            for d in sorted(domains, key=lambda x: -(x.get("avg_score") or 0)):
                score = d.get("avg_score")
                score_str = f"{score:.1f}" if score else "N/A"
                color = _score_color(score)
                rows.append([
                    f"<strong>{d['name'].title()}</strong>",
                    str(d["tables"]),
                    f'<span style="color:{color};font-weight:600">{score_str}</span>',
                    _fmt_number(d.get("total_rows")),
                ])
            body += data_table(headers, rows)
        else:
            body += callout("No quality snapshots available. Run /data-quality/trends/compute first.", "info")
        body += section_end()

        # ── Section 3: Anomaly Alerts ──
        body += section_start("anomalies", "3", "Anomaly Alerts")
        open_count = data.get("open_anomalies", 0)

        anomaly_kpis = [
            kpi_card("Open Alerts", str(open_count),
                     delta="Requires attention" if open_count > 0 else "All clear"),
            kpi_card("Last 7 Days", str(len(data.get("recent_anomalies", [])))),
        ]
        body += kpi_strip(anomaly_kpis)

        recent_anomalies = data.get("recent_anomalies", [])
        if recent_anomalies:
            headers = ["Table", "Type", "Severity", "Message", "Detected", "Status"]
            rows = []
            for a in recent_anomalies:
                sev = a.get("severity", "")
                sev_pill = pill_badge(sev.upper(), "private" if sev == "info" else ("pe" if sev == "warning" else "default"))
                rows.append([
                    a.get("table", ""),
                    a.get("type", "").replace("_", " ").title(),
                    sev_pill,
                    a.get("message", "")[:80],
                    a.get("detected", ""),
                    a.get("status", ""),
                ])
            body += data_table(headers, rows)
        else:
            body += callout("No anomalies detected in the last 7 days.", "success")
        body += section_end()

        # ── Section 4: Cross-Source Validation ──
        body += section_start("cross-source", "4", "Cross-Source Validation")
        cross_source = data.get("cross_source", [])
        if cross_source:
            headers = ["Validation", "Type", "Match Rate", "Status", "Matched", "Orphans", "Last Run"]
            rows = []
            for v in cross_source:
                rate = v.get("pass_rate")
                rate_str = f"{rate:.1f}%" if rate is not None else "N/A"
                color = _score_color(rate)
                status = _status_pill("Good" if v.get("passed") else "Critical") if v.get("passed") is not None else "N/A"
                orphans = f"L:{v.get('orphan_left', 0)} R:{v.get('orphan_right', 0)}"
                rows.append([
                    v.get("name", ""),
                    v.get("type", "").replace("_", " ").title(),
                    f'<span style="color:{color};font-weight:600">{rate_str}</span>',
                    status,
                    _fmt_number(v.get("matched")),
                    orphans,
                    v.get("last_run", "Never"),
                ])
            body += data_table(headers, rows)
        else:
            body += callout("No cross-source validations configured. Run /data-quality/cross-source/seed-defaults.", "info")
        body += section_end()

        # ── Section 5: Quality Trending ──
        body += section_start("trending", "5", "Quality Trending")
        trend = data.get("trend", [])
        if trend:
            import json as _json

            labels = [t["date"] for t in trend]
            quality_vals = [t.get("quality") for t in trend]
            completeness_vals = [t.get("completeness") for t in trend]
            freshness_vals = [t.get("freshness") for t in trend]
            validity_vals = [t.get("validity") for t in trend]

            window = data.get("window_days", 30)
            chart_config = _json.dumps({
                "type": "line",
                "data": {
                    "labels": labels,
                    "datasets": [
                        {
                            "label": "Quality Score",
                            "data": quality_vals,
                            "borderColor": BLUE,
                            "backgroundColor": BLUE + "22",
                            "fill": True,
                            "tension": 0.3,
                        },
                        {
                            "label": "Completeness",
                            "data": completeness_vals,
                            "borderColor": GREEN,
                            "tension": 0.3,
                        },
                        {
                            "label": "Freshness",
                            "data": freshness_vals,
                            "borderColor": ORANGE,
                            "tension": 0.3,
                        },
                        {
                            "label": "Validity",
                            "data": validity_vals,
                            "borderColor": "#8B5CF6",
                            "tension": 0.3,
                        },
                    ],
                },
                "options": {
                    "responsive": True,
                    "plugins": {
                        "legend": {"position": "top"},
                        "title": {"display": True, "text": f"Quality Score Trends ({window} days)"},
                    },
                    "scales": {
                        "y": {"min": 0, "max": 100, "title": {"display": True, "text": "Score"}},
                    },
                },
            })

            body += chart_container("qualityTrendChart", chart_config, title="Quality Trends")
            charts_js += chart_init_js("qualityTrendChart", chart_config)
        else:
            body += callout("No trend data available. Run /data-quality/trends/compute to generate snapshots.", "info")
        body += section_end()

        # ── Section 6: SLA Compliance ──
        body += section_start("sla", "6", "SLA Compliance")
        sla_results = data.get("sla_results", [])
        sla_met = data.get("sla_met", 0)
        sla_total = data.get("sla_total", 0)

        if sla_results:
            compliance_pct = (sla_met / sla_total * 100) if sla_total > 0 else 0
            sla_kpis = [
                kpi_card("SLA Compliance", f"{compliance_pct:.0f}%"),
                kpi_card("Targets Met", f"{sla_met}/{sla_total}"),
            ]
            body += kpi_strip(sla_kpis)

            headers = ["Source", "Table", "Score", "Target", "Status"]
            rows = []
            for sr in sla_results:
                score = sr.get("score")
                target = sr.get("target")
                met = sr.get("met", False)
                score_str = f"{score:.1f}" if score else "N/A"
                target_str = f"{target:.1f}" if target else "N/A"
                status = _status_pill("Good" if met else "Critical")
                rows.append([
                    sr.get("source", ""),
                    sr.get("table", ""),
                    score_str,
                    target_str,
                    status,
                ])
            body += data_table(headers, rows)
        else:
            body += callout("No SLA targets configured. Create targets via /data-quality/trends/sla-targets.", "info")
        body += section_end()

        # ── Section 7: Recent Profiles ──
        body += section_start("profiles", "7", "Recent Profiles")
        profiles = data.get("recent_profiles", [])
        if profiles:
            headers = ["Table", "Rows", "Columns", "Completeness", "Profiled At", "Time (ms)"]
            rows = []
            for p in profiles:
                comp = p.get("completeness")
                comp_str = f"{comp:.1f}%" if comp else "N/A"
                color = _score_color(comp)
                rows.append([
                    p.get("table", ""),
                    _fmt_number(p.get("rows")),
                    str(p.get("columns", 0)),
                    f'<span style="color:{color};font-weight:600">{comp_str}</span>',
                    p.get("profiled_at", ""),
                    _fmt_number(p.get("time_ms")),
                ])
            body += data_table(headers, rows)
        else:
            body += callout("No profiles generated yet. Run /data-quality/profile/all to profile all tables.", "info")
        body += section_end()

        # ── TOC ──
        toc_items = [
            {"number": "1", "id": "platform-quality", "title": "Platform Quality Score"},
            {"number": "2", "id": "domains", "title": "Domain Breakdown"},
            {"number": "3", "id": "anomalies", "title": "Anomaly Alerts"},
            {"number": "4", "id": "cross-source", "title": "Cross-Source Validation"},
            {"number": "5", "id": "trending", "title": "Quality Trending"},
            {"number": "6", "id": "sla", "title": "SLA Compliance"},
            {"number": "7", "id": "profiles", "title": "Recent Profiles"},
        ]
        toc_html = toc(toc_items)

        body += page_footer()

        return html_document(
            title="Deep Data Quality Report",
            body_content=toc_html + body,
            charts_js=charts_js,
        )

    def render_excel(self, data: Dict[str, Any]) -> bytes:
        """Render the deep quality report as Excel."""
        from openpyxl import Workbook
        from openpyxl.styles import Font, PatternFill

        wb = Workbook()
        header_fill = PatternFill(start_color="2B6CB0", end_color="2B6CB0", fill_type="solid")
        header_font = Font(bold=True, color="FFFFFF")
        title_font = Font(bold=True, size=14)

        # Sheet 1: Summary
        ws = wb.active
        ws.title = "Summary"
        ws["A1"] = "Deep Data Quality Report"
        ws["A1"].font = title_font
        ws["A3"] = "Platform Score"
        ws["B3"] = data.get("platform_score")
        ws["A4"] = "Grade"
        ws["B4"] = data.get("platform_grade")
        ws["A5"] = "Tables Tracked"
        ws["B5"] = data.get("tables_tracked")
        ws["A7"] = "Avg Completeness"
        ws["B7"] = data.get("avg_completeness")
        ws["A8"] = "Avg Freshness"
        ws["B8"] = data.get("avg_freshness")
        ws["A9"] = "Avg Validity"
        ws["B9"] = data.get("avg_validity")
        ws["A10"] = "Avg Consistency"
        ws["B10"] = data.get("avg_consistency")
        ws["A12"] = "Open Anomalies"
        ws["B12"] = data.get("open_anomalies")

        # Sheet 2: Domains
        ws2 = wb.create_sheet("Domains")
        headers = ["Domain", "Tables", "Avg Score", "Total Rows"]
        for i, h in enumerate(headers, 1):
            cell = ws2.cell(row=1, column=i, value=h)
            cell.fill = header_fill
            cell.font = header_font
        for row_idx, d in enumerate(data.get("domains", []), 2):
            ws2.cell(row=row_idx, column=1, value=d.get("name", ""))
            ws2.cell(row=row_idx, column=2, value=d.get("tables", 0))
            ws2.cell(row=row_idx, column=3, value=d.get("avg_score"))
            ws2.cell(row=row_idx, column=4, value=d.get("total_rows", 0))

        # Sheet 3: Anomalies
        ws3 = wb.create_sheet("Anomalies")
        headers = ["Table", "Type", "Severity", "Message", "Detected", "Status"]
        for i, h in enumerate(headers, 1):
            cell = ws3.cell(row=1, column=i, value=h)
            cell.fill = header_fill
            cell.font = header_font
        for row_idx, a in enumerate(data.get("recent_anomalies", []), 2):
            ws3.cell(row=row_idx, column=1, value=a.get("table", ""))
            ws3.cell(row=row_idx, column=2, value=a.get("type", ""))
            ws3.cell(row=row_idx, column=3, value=a.get("severity", ""))
            ws3.cell(row=row_idx, column=4, value=a.get("message", ""))
            ws3.cell(row=row_idx, column=5, value=a.get("detected", ""))
            ws3.cell(row=row_idx, column=6, value=a.get("status", ""))

        # Sheet 4: Cross-Source
        ws4 = wb.create_sheet("Cross-Source")
        headers = ["Validation", "Type", "Match Rate %", "Passed", "Matched", "Orphan Left", "Orphan Right", "Last Run"]
        for i, h in enumerate(headers, 1):
            cell = ws4.cell(row=1, column=i, value=h)
            cell.fill = header_fill
            cell.font = header_font
        for row_idx, v in enumerate(data.get("cross_source", []), 2):
            ws4.cell(row=row_idx, column=1, value=v.get("name", ""))
            ws4.cell(row=row_idx, column=2, value=v.get("type", ""))
            ws4.cell(row=row_idx, column=3, value=v.get("pass_rate"))
            ws4.cell(row=row_idx, column=4, value="Yes" if v.get("passed") else "No")
            ws4.cell(row=row_idx, column=5, value=v.get("matched"))
            ws4.cell(row=row_idx, column=6, value=v.get("orphan_left"))
            ws4.cell(row=row_idx, column=7, value=v.get("orphan_right"))
            ws4.cell(row=row_idx, column=8, value=v.get("last_run", ""))

        # Sheet 5: Trending
        ws5 = wb.create_sheet("Trending")
        headers = ["Date", "Quality", "Completeness", "Freshness", "Validity", "Tables"]
        for i, h in enumerate(headers, 1):
            cell = ws5.cell(row=1, column=i, value=h)
            cell.fill = header_fill
            cell.font = header_font
        for row_idx, t in enumerate(data.get("trend", []), 2):
            ws5.cell(row=row_idx, column=1, value=t.get("date", ""))
            ws5.cell(row=row_idx, column=2, value=t.get("quality"))
            ws5.cell(row=row_idx, column=3, value=t.get("completeness"))
            ws5.cell(row=row_idx, column=4, value=t.get("freshness"))
            ws5.cell(row=row_idx, column=5, value=t.get("validity"))
            ws5.cell(row=row_idx, column=6, value=t.get("tables"))

        # Sheet 6: SLA
        ws6 = wb.create_sheet("SLA Compliance")
        headers = ["Source", "Table", "Score", "Target", "Met"]
        for i, h in enumerate(headers, 1):
            cell = ws6.cell(row=1, column=i, value=h)
            cell.fill = header_fill
            cell.font = header_font
        for row_idx, sr in enumerate(data.get("sla_results", []), 2):
            ws6.cell(row=row_idx, column=1, value=sr.get("source", ""))
            ws6.cell(row=row_idx, column=2, value=sr.get("table", ""))
            ws6.cell(row=row_idx, column=3, value=sr.get("score"))
            ws6.cell(row=row_idx, column=4, value=sr.get("target"))
            ws6.cell(row=row_idx, column=5, value="Yes" if sr.get("met") else "No")

        buf = BytesIO()
        wb.save(buf)
        return buf.getvalue()
