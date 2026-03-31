"""
Macro Sector Brief — IC Report Template.

Generates a one-page macro briefing for any PE sector, combining the
Deal Environment Score, relevant FRED/BLS charts, and plain-language
narrative. Intended for board meeting prep and LP updates.

Parameters accepted by gather_data():
    sector (str): sector slug from SECTOR_CONFIGS (e.g. "industrials")

Sections:
  1. Deal Environment Score Card
  2. Sector-Specific Macro Indicators
  3. Watch Points — conditions that would change the score

Run via builder:
    POST /api/v1/reports/generate
    {"template": "macro_sector_brief", "sector": "industrials"}
"""

import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from sqlalchemy.orm import Session

from app.reports.design_system import (
    BLUE, BLUE_LIGHT, GRAY, GREEN, ORANGE, RED, TEAL,
    CHART_COLORS,
    build_bar_fallback, build_chart_legend,
    build_horizontal_bar_config, build_line_chart_config,
    callout, chart_container, chart_init_js,
    html_document, kpi_card, kpi_strip,
    page_footer, page_header,
    section_end, section_start,
    data_table,
)
from app.reports.templates._ic_report_base import ICReportBase
from app.services.deal_environment_scorer import (
    DealEnvironmentScore,
    DealEnvironmentScorer,
    SECTOR_CONFIGS,
    _get_fred_latest,
    _get_bls_latest,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Signal / grade color map
# ---------------------------------------------------------------------------

_SIGNAL_COLOR = {"green": GREEN, "yellow": ORANGE, "red": RED}
_GRADE_COLOR = {"A": GREEN, "B": BLUE, "C": ORANGE, "D": RED}
_IMPACT_COLOR = {"positive": GREEN, "negative": RED, "neutral": GRAY}

# Watch point templates keyed by sector sensitivity dimension
_WATCHPOINT_TEMPLATES = {
    "rates_very_high": (
        "Rate cut of ≥50bps",
        "Would materially reduce LBO financing costs and cap rate pressure; likely upgrade to next grade.",
        "Fed Funds Rate crossing below {threshold:.1f}%",
    ),
    "rates_high": (
        "Rate environment eases",
        "FFR declining toward neutral (2.5–3.5%) would lower debt cost ~150–200bps from current levels.",
        "DFF falling below 4.0%",
    ),
    "rates_medium": (
        "Credit conditions tighten",
        "Renewed rate hikes or credit spread widening would compress exit multiples and raise LBO costs.",
        "DFF rising above 5.5% or HY spread widening >100bps",
    ),
    "labor_high": (
        "Sector employment reversal",
        "12-month employment growth turning negative would signal sector contraction and reduce deal conviction.",
        "BLS sector employment 12m Δ crossing below 0%",
    ),
    "consumer_high": (
        "Consumer sentiment deterioration",
        "UMCSENT declining below 60 would indicate demand slowdown across consumer-exposed revenue lines.",
        "UMCSENT falling below 60",
    ),
    "yield_curve": (
        "Yield curve inversion deepens",
        "10Y–2Y spread widening further negative historically precedes recession by 12–18 months.",
        "10Y–2Y spread reaching –1.0pp or below",
    ),
    "cpi": (
        "Inflation re-acceleration",
        "CPI YoY returning above 4% would delay rate cuts, sustain elevated financing costs, and compress margins.",
        "CPI YoY rising above 4.0%",
    ),
}


# ---------------------------------------------------------------------------
# Sector-specific FRED table routing
# ---------------------------------------------------------------------------

_FRED_TABLE_FOR_SERIES = {
    "DFF": "fred_interest_rates",
    "DGS10": "fred_interest_rates",
    "DGS2": "fred_interest_rates",
    "MORTGAGE30US": "fred_interest_rates",
    "UNRATE": "fred_economic_indicators",
    "INDPRO": "fred_economic_indicators",
    "TCU": "fred_economic_indicators",
    "GDP": "fred_economic_indicators",
    "UMCSENT": "fred_consumer_sentiment",
    "TOTALSA": "fred_auto_sector",
    "RSXFS": "fred_economic_indicators",
    "DCOILWTICO": "fred_economic_indicators",
    "DHHNGSP": "fred_economic_indicators",
    "HOUST": "fred_housing_market",
    "CSUSHPINSA": "fred_housing_market",
}

_DEFAULT_FRED_TABLE = "fred_economic_indicators"


def _fred_table(series_id: str) -> str:
    return _FRED_TABLE_FOR_SERIES.get(series_id, _DEFAULT_FRED_TABLE)


# ---------------------------------------------------------------------------
# Template class
# ---------------------------------------------------------------------------

class MacroSectorBriefTemplate(ICReportBase):
    """
    One-page macro sector brief for any of the 9 PE sectors.
    Generates deal score card, sector indicator charts, and watch points.
    """

    name = "macro_sector_brief"
    display_name = "Macro Sector Brief"
    description = (
        "Auto-generated one-page macro briefing for any PE sector. "
        "Includes Deal Environment Score, live FRED/BLS charts, and board-ready watch points."
    )

    # ── Data gathering ────────────────────────────────────────────────────────

    def gather_data(self, db: Session, params: Dict[str, Any]) -> Dict[str, Any]:
        sector_slug = params.get("sector", "industrials")
        if sector_slug not in SECTOR_CONFIGS:
            raise ValueError(
                f"Unknown sector '{sector_slug}'. Valid: {list(SECTOR_CONFIGS.keys())}"
            )

        config = SECTOR_CONFIGS[sector_slug]

        # Deal environment score
        scorer = DealEnvironmentScorer(db)
        deal_score: DealEnvironmentScore = scorer.score_sector(sector_slug)

        # FRED history for the sector's primary series (up to 2)
        fred_histories: Dict[str, List[Dict]] = {}
        for series_id in config.get("fred_series", [])[:2]:
            table = _fred_table(series_id)
            history = self.fetch_fred_history(db, table, series_id, limit=24)
            if history:
                fred_histories[series_id] = history

        # BLS latest for the sector's primary series
        bls_data: Dict[str, Any] = {}
        for series_id in config.get("bls_series", [])[:1]:
            latest, prev = _get_bls_latest(db, series_id)
            if latest is not None:
                delta_pct = None
                if prev is not None and prev > 0:
                    delta_pct = round(((latest - prev) / prev) * 100, 1)
                bls_data[series_id] = {
                    "latest": latest,
                    "prev_12m": prev,
                    "delta_pct": delta_pct,
                }

        # Macro snapshot (FFR, 10Y, unemployment)
        macro_snapshot: Dict[str, Optional[float]] = {}
        for sid in ["DFF", "DGS10", "DGS2", "UNRATE", "UMCSENT"]:
            val, _ = _get_fred_latest(db, sid)
            macro_snapshot[sid] = val

        return {
            "generated_at": datetime.utcnow().isoformat(),
            "sector_slug": sector_slug,
            "sector_config": config,
            "deal_score": deal_score,
            "fred_histories": fred_histories,
            "bls_data": bls_data,
            "macro_snapshot": macro_snapshot,
        }

    # ── HTML rendering ────────────────────────────────────────────────────────

    def render_html(self, data: Dict[str, Any]) -> str:  # noqa: C901
        sector_slug: str = data.get("sector_slug", "industrials")
        config: Dict = data.get("sector_config", SECTOR_CONFIGS.get(sector_slug, {}))
        deal_score: DealEnvironmentScore = data["deal_score"]
        fred_histories: Dict = data.get("fred_histories", {})
        bls_data: Dict = data.get("bls_data", {})
        macro_snapshot: Dict = data.get("macro_snapshot", {})
        gen_at: str = data.get("generated_at", datetime.utcnow().isoformat())
        report_title: str = data.get(
            "report_title",
            f"Macro Sector Brief: {config.get('label', sector_slug)}",
        )

        gen_date = gen_at[:10]
        sector_label = config.get("label", sector_slug.replace("_", " ").title())
        grade_color = _GRADE_COLOR.get(deal_score.grade, GRAY)
        signal_color = _SIGNAL_COLOR.get(deal_score.signal, GRAY)

        charts_js = ""
        body = ""

        # ── Header ────────────────────────────────────────────────────────────
        body += page_header(
            title=report_title,
            subtitle=f"Macro Deal Environment Assessment | {gen_date}",
            badge=f"Nexdata Live | {sector_label}",
        )

        # ── KPI Strip ─────────────────────────────────────────────────────────
        ffr = macro_snapshot.get("DFF")
        dgs10 = macro_snapshot.get("DGS10")
        dgs2 = macro_snapshot.get("DGS2")
        unrate = macro_snapshot.get("UNRATE")

        # Sector employment delta from BLS data
        bls_series_ids = list(bls_data.keys())
        emp_delta_str = "—"
        if bls_series_ids:
            bls_entry = bls_data[bls_series_ids[0]]
            dp = bls_entry.get("delta_pct")
            if dp is not None:
                emp_delta_str = f"{dp:+.1f}%"

        # Yield spread
        spread_str = "—"
        if dgs10 is not None and dgs2 is not None:
            spread = dgs10 - dgs2
            spread_str = f"{spread:+.2f}pp"

        body += kpi_strip(
            kpi_card(
                "Deal Score",
                f"{deal_score.score}/100",
                delta=f"Grade {deal_score.grade} — {deal_score.signal.upper()}",
                delta_dir="up" if deal_score.signal == "green" else (
                    "down" if deal_score.signal == "red" else "neutral"
                ),
            )
            + kpi_card(
                "Fed Funds Rate",
                f"{ffr:.2f}%" if ffr is not None else "—",
                delta="FRED DFF · latest",
                delta_dir="down" if (ffr or 0) > 4.5 else "neutral",
            )
            + kpi_card(
                "10Y–2Y Spread",
                spread_str,
                delta="Yield curve shape",
                delta_dir="up" if (dgs10 or 0) - (dgs2 or 0) > 0.5 else (
                    "down" if (dgs10 or 0) - (dgs2 or 0) < 0 else "neutral"
                ),
            )
            + kpi_card(
                "Unemployment Rate",
                f"{unrate:.1f}%" if unrate is not None else "—",
                delta="FRED UNRATE · latest",
                delta_dir="neutral",
            )
            + kpi_card(
                "Sector Emp. 12m Δ",
                emp_delta_str,
                delta=f"BLS · {bls_series_ids[0]}" if bls_series_ids else "BLS · no data",
                delta_dir=(
                    "up" if emp_delta_str.startswith("+") and emp_delta_str != "—"
                    else ("down" if emp_delta_str.startswith("-") else "neutral")
                ),
            )
        )

        # ── Section 1: Deal Environment Score Card ────────────────────────────
        body += section_start(1, "Deal Environment Score", "deal-score")

        # Large score display
        body += f"""
<div style="display:flex;gap:32px;flex-wrap:wrap;align-items:flex-start;margin-bottom:24px">
  <div style="text-align:center;min-width:160px">
    <div style="font-size:80px;font-weight:800;color:{grade_color};line-height:1">
      {deal_score.score}
    </div>
    <div style="font-size:14px;color:var(--gray-500);text-transform:uppercase;
                letter-spacing:0.5px;margin-top:4px">Deal Score</div>
    <div style="margin-top:8px">
      <span style="background:{grade_color}20;color:{grade_color};
                   font-size:28px;font-weight:800;padding:4px 20px;
                   border-radius:8px;border:2px solid {grade_color}40">
        {deal_score.grade}
      </span>
    </div>
    <div style="margin-top:10px">
      <span style="background:{signal_color}20;color:{signal_color};
                   font-size:13px;font-weight:600;padding:4px 14px;
                   border-radius:20px;border:1px solid {signal_color}40">
        {deal_score.signal.upper()}
      </span>
    </div>
  </div>
  <div style="flex:1;min-width:260px">
    <div style="font-size:15px;font-weight:600;color:var(--gray-800);
                margin-bottom:8px">Analyst Recommendation</div>
    <div style="font-size:14px;color:var(--gray-700);line-height:1.7;
                background:var(--gray-50);padding:16px;border-radius:8px;
                border-left:4px solid {grade_color}">
      {deal_score.recommendation}
    </div>
    <div style="margin-top:16px;font-size:13px;color:var(--gray-500)">
      <strong>Sector:</strong> {sector_label} &nbsp;|&nbsp;
      <strong>As of:</strong> {gen_date} &nbsp;|&nbsp;
      <strong>Signal:</strong>
      <span style="color:{signal_color};font-weight:600">{deal_score.signal.upper()}</span>
    </div>
  </div>
</div>
"""

        # Score factor table
        if deal_score.factors:
            factor_rows = []
            for f in deal_score.factors:
                impact_color = _IMPACT_COLOR.get(f.impact, GRAY)
                contrib = f.score_contribution
                contrib_str = f"+{contrib}" if contrib > 0 else str(contrib)
                contrib_display = (
                    f'<span style="color:{GREEN};font-weight:600">{contrib_str}</span>'
                    if contrib > 0
                    else (
                        f'<span style="color:{RED};font-weight:600">{contrib_str}</span>'
                        if contrib < 0
                        else f'<span style="color:{GRAY}">{contrib_str}</span>'
                    )
                )
                impact_badge = (
                    f'<span style="background:{impact_color}20;color:{impact_color};'
                    f'font-size:11px;font-weight:600;padding:2px 10px;'
                    f'border-radius:4px">{f.impact.upper()}</span>'
                )
                factor_rows.append([
                    f.factor,
                    f.reading,
                    impact_badge,
                    contrib_display,
                    f'<span style="font-size:11px;color:var(--gray-500)">{f.data_source}</span>',
                ])

            body += data_table(
                ["Factor", "Reading", "Impact", "Score Δ", "Source"],
                factor_rows,
                numeric_columns={3},
            )
        else:
            body += callout(
                "<strong>No live macro data available.</strong> "
                "Ingest FRED and BLS data to generate factor scores. "
                "Run: <code>POST /api/v1/fred/ingest</code> and "
                "<code>POST /api/v1/bls/ingest</code>.",
                variant="warn",
            )

        body += section_end()

        # ── Section 2: Sector-Specific Macro Indicators ───────────────────────
        body += section_start(2, "Sector-Specific Macro Indicators", "macro-indicators")

        chart_count = 0

        if fred_histories:
            body += '<div class="grid-2">'
            for series_id, history in list(fred_histories.items())[:2]:
                if not history:
                    continue
                chart_id = f"chart_fred_{series_id.lower().replace('.', '_')}"
                labels = [h["date"] for h in history]
                values = [h["value"] for h in history]

                # Filter out None values for chart (keep as 0 for display)
                chart_values = [v if v is not None else 0.0 for v in values]

                line_cfg = build_line_chart_config(
                    labels=labels,
                    datasets=[{
                        "label": series_id,
                        "data": chart_values,
                        "color": CHART_COLORS[chart_count % len(CHART_COLORS)],
                        "fill": True,
                    }],
                    y_label=series_id,
                )

                # Simple fallback bars
                valid_pairs = [(l, v) for l, v in zip(labels, chart_values) if v is not None]
                fallback_labels = [p[0] for p in valid_pairs[-8:]]
                fallback_values = [p[1] for p in valid_pairs[-8:]]
                fallback_html = build_bar_fallback(fallback_labels, fallback_values)

                body += f'<div>'
                body += f'<div style="font-size:13px;font-weight:600;color:var(--gray-700);margin-bottom:8px">{series_id} (24m)</div>'
                body += chart_container(
                    chart_id=chart_id,
                    chart_config_json=json.dumps(line_cfg),
                    fallback_html=fallback_html,
                    size="medium",
                )
                body += f'</div>'
                charts_js += chart_init_js(chart_id, json.dumps(line_cfg))
                chart_count += 1

            body += '</div>'
        else:
            body += callout(
                f"<strong>No historical FRED data available for {sector_label}.</strong> "
                f"Relevant series: {', '.join(config.get('fred_series', [])[:3])}. "
                "Run FRED ingestion to populate charts.",
                variant="warn",
            )

        # BLS employment summary
        if bls_data:
            body += '<div style="margin-top:20px">'
            body += '<div style="font-size:14px;font-weight:600;color:var(--gray-700);margin-bottom:12px">Sector Employment Summary (BLS)</div>'

            bls_rows = []
            for sid, entry in bls_data.items():
                latest_val = entry.get("latest")
                prev_val = entry.get("prev_12m")
                dp = entry.get("delta_pct")
                latest_str = f"{latest_val:,.1f}K" if latest_val is not None else "—"
                prev_str = f"{prev_val:,.1f}K" if prev_val is not None else "—"
                if dp is not None:
                    dp_color = GREEN if dp > 0 else (RED if dp < 0 else GRAY)
                    dp_str = f'<span style="color:{dp_color};font-weight:600">{dp:+.1f}%</span>'
                else:
                    dp_str = "—"
                bls_rows.append([sid, latest_str, prev_str, dp_str])

            body += data_table(
                ["BLS Series", "Latest (000s)", "12m Prior", "12m Δ"],
                bls_rows,
                numeric_columns={1, 2},
            )
            body += '</div>'

        # Macro snapshot table
        snapshot_rows = []
        snapshot_items = [
            ("DFF", "Fed Funds Rate", "%"),
            ("DGS10", "10-Year Treasury", "%"),
            ("DGS2", "2-Year Treasury", "%"),
            ("UNRATE", "Unemployment Rate", "%"),
            ("UMCSENT", "Consumer Sentiment", ""),
        ]
        for sid, label, unit in snapshot_items:
            val = macro_snapshot.get(sid)
            val_str = (f"{val:.2f}{unit}" if unit else f"{val:.1f}") if val is not None else "—"
            snapshot_rows.append([label, sid, val_str])

        body += '<div style="margin-top:20px">'
        body += '<div style="font-size:14px;font-weight:600;color:var(--gray-700);margin-bottom:12px">Macro Snapshot</div>'
        body += data_table(
            ["Indicator", "Series ID", "Latest Value"],
            snapshot_rows,
        )
        body += '</div>'

        body += section_end()

        # ── Section 3: Watch Points ───────────────────────────────────────────
        body += section_start(3, "Watch Points — Score Change Triggers", "watch-points")

        body += callout(
            f"<strong>How to use:</strong> These are the three macro conditions most likely to "
            f"change the {sector_label} deal score in the next 3–6 months. Monitor monthly "
            "with each FRED/BLS data refresh.",
            variant="info",
        )
        body += '<div style="margin-top:16px"></div>'

        watch_points = self._generate_watch_points(deal_score, macro_snapshot, config)
        wp_rows = []
        for i, wp in enumerate(watch_points, 1):
            direction = wp.get("direction", "upgrade")
            dir_color = GREEN if direction == "upgrade" else RED
            dir_badge = (
                f'<span style="background:{dir_color}20;color:{dir_color};'
                f'font-size:11px;font-weight:600;padding:2px 8px;border-radius:4px">'
                f'{"▲ UPGRADE" if direction == "upgrade" else "▼ DOWNGRADE"}</span>'
            )
            wp_rows.append([
                f"WP{i}: {wp['title']}",
                wp["description"],
                wp["trigger"],
                dir_badge,
            ])

        body += data_table(
            ["Watch Point", "Why It Matters", "Trigger Condition", "Score Impact"],
            wp_rows,
        )

        # Score sensitivity summary
        body += '<div style="margin-top:20px">'
        body += callout(
            f"<strong>Current Score: {deal_score.score}/100 (Grade {deal_score.grade}).</strong> "
            f"An upgrade scenario (all three watch points improve) could add +15 to +25 points. "
            f"A downgrade scenario (all three deteriorate) could subtract –20 to –35 points.",
            variant="good" if deal_score.signal == "green" else (
                "warn" if deal_score.signal == "yellow" else "info"
            ),
        )
        body += '</div>'

        body += section_end()

        # ── Footer ────────────────────────────────────────────────────────────
        body += page_footer(
            notes=[
                "Deal scores computed from live FRED and BLS macro data at time of generation.",
                f"Sector: {sector_label} | Slug: {sector_slug}",
                f"FRED series queried: {', '.join(config.get('fred_series', []))}",
                f"BLS series queried: {', '.join(config.get('bls_series', []))}",
                "Grade scale: A (80–100) compelling deploy; B (65–79) selective; C (50–64) cautious; D (<50) avoid.",
                "This report is for internal investment committee use only. Not for distribution.",
            ],
            generated_line=f"Generated {gen_at[:19]} UTC | Nexdata Macro Intelligence | PLAN_048",
        )

        return html_document(
            title=f"{report_title} | {gen_date}",
            body_content=body,
            charts_js=charts_js,
        )

    # ── Watch point generation ────────────────────────────────────────────────

    def _generate_watch_points(
        self,
        deal_score: DealEnvironmentScore,
        macro_snapshot: Dict[str, Optional[float]],
        config: Dict,
    ) -> List[Dict[str, str]]:
        """
        Auto-generate 3 watch points based on sector sensitivities and
        current macro conditions. Each watch point describes a condition
        that would materially change the deal score.
        """
        watch_points = []
        sensitivity = config.get("sensitivity", {})
        rate_sens = sensitivity.get("rates", "medium")
        labor_sens = sensitivity.get("labor", "medium")
        consumer_sens = sensitivity.get("consumer", "medium")
        ffr = macro_snapshot.get("DFF")
        dgs10 = macro_snapshot.get("DGS10")
        dgs2 = macro_snapshot.get("DGS2")

        # Watch Point 1: Rate-driven (always relevant)
        if rate_sens in ("very_high", "high"):
            if ffr is not None and ffr > 4.5:
                threshold = 3.5
                watch_points.append({
                    "title": "Rate Environment Eases",
                    "description": (
                        f"With FFR at {ffr:.2f}%, this sector faces elevated financing costs "
                        f"due to its {rate_sens.replace('_', ' ')} rate sensitivity. "
                        "A sustained rate cut cycle would materially improve deal economics."
                    ),
                    "trigger": f"FFR declining toward {threshold:.1f}% — watch Fed dot plot and inflation prints",
                    "direction": "upgrade",
                })
            else:
                watch_points.append({
                    "title": "Rate Spike Risk",
                    "description": (
                        f"This sector has {rate_sens.replace('_', ' ')} rate sensitivity. "
                        "Any renewed inflation forcing rate hikes would compress exit multiples "
                        "and raise LBO financing costs."
                    ),
                    "trigger": "FFR rising above 5.5% or CPI re-accelerating above 4%",
                    "direction": "downgrade",
                })
        else:
            # Yield curve watch for medium/low rate sensitivity
            if dgs10 is not None and dgs2 is not None:
                spread = dgs10 - dgs2
                if spread < 0:
                    watch_points.append({
                        "title": "Yield Curve Normalization",
                        "description": (
                            f"The yield curve is currently inverted ({spread:+.2f}pp), "
                            "historically a leading recession indicator. Re-steepening would "
                            "reduce recession risk premium in deal pricing."
                        ),
                        "trigger": "10Y–2Y spread turning positive and sustaining above +0.5pp",
                        "direction": "upgrade",
                    })
                else:
                    watch_points.append({
                        "title": "Yield Curve Inversion",
                        "description": (
                            f"Yield curve currently at +{spread:.2f}pp. "
                            "Inversion would signal deteriorating credit conditions "
                            "and increasing recession probability."
                        ),
                        "trigger": "10Y–2Y spread crossing below 0 and holding negative",
                        "direction": "downgrade",
                    })

        # Watch Point 2: Labor / sector momentum
        bls_series = config.get("bls_series", [])
        if bls_series and labor_sens in ("very_high", "high"):
            sector_label = config.get("label", "sector")
            watch_points.append({
                "title": f"{sector_label} Employment Reversal",
                "description": (
                    f"This sector has {labor_sens.replace('_', ' ')} labor sensitivity. "
                    "Employment trend is a leading indicator of operating leverage — "
                    "contracting headcount signals reduced demand and margin pressure."
                ),
                "trigger": f"BLS {bls_series[0]}: 12-month employment growth turning negative",
                "direction": "downgrade",
            })
        elif bls_series:
            watch_points.append({
                "title": "Employment Momentum Accelerates",
                "description": (
                    "A step-up in sector hiring velocity would indicate strengthening "
                    "demand fundamentals and support higher exit multiples."
                ),
                "trigger": f"BLS {bls_series[0]}: 12-month growth exceeding +3%",
                "direction": "upgrade",
            })
        else:
            # Use consumer confidence as substitute for sectors with no BLS series
            umcsent = macro_snapshot.get("UMCSENT")
            if umcsent is not None:
                if umcsent < 70:
                    watch_points.append({
                        "title": "Consumer Confidence Recovery",
                        "description": (
                            f"Consumer sentiment at {umcsent:.0f} is below historical midpoint. "
                            "Recovery above 80 would signal improved demand and deal environment."
                        ),
                        "trigger": "UMCSENT recovering above 80 for two consecutive readings",
                        "direction": "upgrade",
                    })
                else:
                    watch_points.append({
                        "title": "Consumer Confidence Slippage",
                        "description": (
                            f"Consumer sentiment at {umcsent:.0f}. "
                            "Sustained decline below 65 would weaken demand outlook "
                            "and compress deal multiples in consumer-exposed sectors."
                        ),
                        "trigger": "UMCSENT falling below 65 for two consecutive readings",
                        "direction": "downgrade",
                    })
            else:
                watch_points.append({
                    "title": "Macro Data Gap",
                    "description": (
                        "No sector employment or consumer data available in database. "
                        "Ingest BLS CES and FRED UMCSENT to enable this watch point."
                    ),
                    "trigger": "Run BLS and FRED ingestion",
                    "direction": "upgrade",
                })

        # Watch Point 3: Consumer / inflation (use the dimension with highest sensitivity)
        if consumer_sens in ("very_high", "high") and len(watch_points) < 3:
            umcsent = macro_snapshot.get("UMCSENT")
            if umcsent is not None and umcsent < 75:
                watch_points.append({
                    "title": "Consumer Demand Recovery",
                    "description": (
                        f"Consumer sentiment ({umcsent:.0f}) is below the 75 threshold "
                        "that historically correlates with strong discretionary spending. "
                        "Recovery would directly lift top-line visibility for consumer-exposed companies."
                    ),
                    "trigger": "UMCSENT rising above 80 and retail sales (RSXFS) YoY turning positive",
                    "direction": "upgrade",
                })
            else:
                watch_points.append({
                    "title": "Consumer Spending Slowdown",
                    "description": (
                        "Consumer-sensitive sectors face elevated risk if spending cools. "
                        "Watch retail sales and credit card delinquency data as leading indicators."
                    ),
                    "trigger": "RSXFS YoY growth turning negative or UMCSENT falling below 65",
                    "direction": "downgrade",
                })

        # Inflation watch point (always useful as third if not already 3)
        if len(watch_points) < 3:
            watch_points.append({
                "title": "Inflation Re-acceleration",
                "description": (
                    "Input cost inflation above 4% YoY would delay Fed rate cuts, "
                    "sustain elevated financing costs, and pressure portfolio company margins. "
                    "All sectors are affected but rate-sensitive sectors most severely."
                ),
                "trigger": "CPI YoY rising above 4.0% for two consecutive months",
                "direction": "downgrade",
            })

        return watch_points[:3]
