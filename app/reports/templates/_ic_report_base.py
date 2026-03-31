"""
IC Report Base Class.

Shared database helpers and specialized table renderers for
Investment Committee report templates. Inherit from ICReportBase
to get consistent macro data fetching and styled table components.
"""

import logging
from typing import Any, Dict, List, Optional, Set

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.reports.design_system import (
    BLUE, GRAY, GREEN, ORANGE, RED,
    data_table,
)

logger = logging.getLogger(__name__)


class ICReportBase:
    """
    Base class for IC-grade report templates.

    Provides:
    - Safe database query helpers with automatic rollback on failure
    - Standardized FRED / BLS / AFDC data fetchers
    - Colored table renderers for risk badges, priority badges, watchpoints
    """

    # ── Database helpers ──────────────────────────────────────────────────────

    @staticmethod
    def _safe(db: Session, sql: str, params: dict = None) -> List[Any]:
        """Execute a parameterized query; return [] if table missing or query fails."""
        try:
            result = db.execute(text(sql), params or {})
            return result.fetchall()
        except Exception as exc:
            try:
                db.rollback()
            except Exception:
                pass
            logger.debug("IC report query skipped: %s", str(exc)[:120])
            return []

    def fetch_fred_series(self, db: Session, table: str) -> Dict[str, Dict]:
        """
        Fetch the latest value per series from a FRED table.

        Returns: {series_id: {"value": float|None, "date": str}}
        """
        rows = self._safe(db, f"""
            SELECT DISTINCT ON (series_id)
                   series_id, date, value
            FROM   {table}
            ORDER  BY series_id, date DESC
        """)
        return {
            r.series_id: {
                "value": float(r.value) if r.value is not None else None,
                "date": str(r.date),
            }
            for r in rows
        }

    def fetch_fred_history(self, db: Session, table: str, series_id: str, limit: int = 24) -> List[Dict]:
        """Fetch historical values for a single FRED series, newest-first limited."""
        rows = self._safe(db, f"""
            SELECT date, value
            FROM   {table}
            WHERE  series_id = :sid
            ORDER  BY date DESC
            LIMIT  :lim
        """, {"sid": series_id, "lim": limit})
        return [
            {"date": str(r.date)[:7], "value": float(r.value) if r.value is not None else None}
            for r in reversed(rows)
        ]

    def fetch_bls_latest(self, db: Session, table: str) -> Dict[str, Dict]:
        """
        Fetch the latest value per series from a BLS table.

        Returns: {series_id: {"title": str, "year": int, "period": str, "value": float|None}}
        """
        rows = self._safe(db, f"""
            SELECT DISTINCT ON (series_id)
                   series_id, series_title, year, period, value
            FROM   {table}
            ORDER  BY series_id, year DESC, period DESC
        """)
        return {
            r.series_id: {
                "title": r.series_title or r.series_id,
                "year": r.year,
                "period": r.period,
                "value": float(r.value) if r.value is not None else None,
            }
            for r in rows
        }

    def fetch_afdc_by_state(
        self, db: Session, footprint_states: Optional[Set[str]] = None
    ) -> Dict[str, Any]:
        """
        Fetch EV charging station counts by state from afdc_ev_stations.

        Returns:
            national_total: int
            footprint_total: int (sum over footprint_states)
            states: list of {state, total, level2, dc_fast, in_footprint}
            footprint_states: list of footprint-only entries, sorted by total desc
        """
        rows = self._safe(db, """
            SELECT DISTINCT ON (state)
                   state, total_stations, ev_level2, ev_dc_fast, as_of_date
            FROM   afdc_ev_stations
            ORDER  BY state, as_of_date DESC
        """)
        states = []
        national = 0
        footprint = 0
        fp = footprint_states or set()

        for r in rows:
            total = int(r.total_stations or 0)
            national += total
            in_fp = r.state in fp
            if in_fp:
                footprint += total
            states.append({
                "state": r.state,
                "total": total,
                "level2": int(r.ev_level2 or 0) if r.ev_level2 is not None else None,
                "dc_fast": int(r.ev_dc_fast or 0) if r.ev_dc_fast is not None else None,
                "in_footprint": in_fp,
                "as_of_date": str(r.as_of_date) if r.as_of_date else None,
            })

        states.sort(key=lambda x: x["total"], reverse=True)

        return {
            "national_total": national,
            "footprint_total": footprint,
            "states": states,
            "footprint_states": [s for s in states if s["in_footprint"]],
        }

    # ── Specialized table renderers ───────────────────────────────────────────

    @staticmethod
    def render_risk_table(
        headers: List[str], rows: List[List[str]], risk_col: int = -1
    ) -> str:
        """
        Render a data table with a color-coded risk badge column.

        Risk values recognized: "High", "Medium", "Low", "Opportunity".
        risk_col: column index (negative = from end).
        """
        _RISK_STYLE = {
            "high":        (RED,    "#fff5f5"),
            "medium":      (ORANGE, "#fffaf0"),
            "low":         (GREEN,  "#f0fff4"),
            "opportunity": (BLUE,   "#ebf8ff"),
        }
        styled_rows = []
        for row in rows:
            styled = [str(c) for c in row]
            idx = risk_col if risk_col >= 0 else max(0, len(styled) + risk_col)
            if idx < len(styled):
                raw = styled[idx].strip().lower()
                color, bg = _RISK_STYLE.get(raw, (GRAY, "#f7fafc"))
                styled[idx] = (
                    f'<span style="padding:2px 10px;border-radius:4px;'
                    f'background:{bg};color:{color};font-weight:600;font-size:12px">'
                    f'{row[idx]}</span>'
                )
            styled_rows.append(styled)
        return data_table(headers, styled_rows)

    @staticmethod
    def render_priority_table(
        headers: List[str], rows: List[List[str]], priority_col: int = -1
    ) -> str:
        """
        Render a data table with a color-coded priority badge column.

        Priority values recognized: "Priority 1", "Priority 2", "Priority 3", "Situational".
        """
        _PRIO_STYLE = {
            "priority 1":  (GREEN,  "#f0fff4"),
            "priority 2":  (BLUE,   "#ebf8ff"),
            "priority 3":  (ORANGE, "#fffaf0"),
            "situational": (GRAY,   "#f7fafc"),
        }
        styled_rows = []
        for row in rows:
            styled = [str(c) for c in row]
            idx = priority_col if priority_col >= 0 else max(0, len(styled) + priority_col)
            if idx < len(styled):
                raw = styled[idx].strip().lower()
                color, bg = _PRIO_STYLE.get(raw, (GRAY, "#f7fafc"))
                styled[idx] = (
                    f'<span style="padding:2px 10px;border-radius:4px;'
                    f'background:{bg};color:{color};font-weight:600;font-size:12px">'
                    f'{row[idx]}</span>'
                )
            styled_rows.append(styled)
        return data_table(headers, styled_rows)

    @staticmethod
    def render_watchpoints(headers: List[str], rows: List[List[str]]) -> str:
        """
        Render a watchpoint tracking table.

        Convention: col[2] = green signal (✓), col[3] = red signal (✗).
        """
        styled_rows = []
        for row in rows:
            styled = [str(c) for c in row]
            if len(styled) > 2:
                styled[2] = (
                    f'<span style="color:{GREEN};font-size:12px;font-weight:600">'
                    f'✓ {styled[2]}</span>'
                )
            if len(styled) > 3:
                styled[3] = (
                    f'<span style="color:{RED};font-size:12px;font-weight:600">'
                    f'✗ {styled[3]}</span>'
                )
            styled_rows.append(styled)
        return data_table(headers, styled_rows)

    @staticmethod
    def render_scenario_table(headers: List[str], rows: List[List[str]]) -> str:
        """
        Render a scenario table with colored scenario badges (Bull/Base/Bear).

        Convention: first cell of each row contains the scenario label.
        """
        _SCENARIO_STYLE = {
            "bull":   (GREEN,  "#f0fff4"),
            "base":   (BLUE,   "#ebf8ff"),
            "bear":   (RED,    "#fff5f5"),
        }
        styled_rows = []
        for row in rows:
            styled = [str(c) for c in row]
            if styled:
                raw = styled[0].lower()
                color, bg = next(
                    ((c, b) for k, (c, b) in _SCENARIO_STYLE.items() if k in raw),
                    (GRAY, "#f7fafc"),
                )
                styled[0] = (
                    f'<span style="padding:2px 10px;border-radius:4px;'
                    f'background:{bg};color:{color};font-weight:700;font-size:12px">'
                    f'{row[0]}</span>'
                )
            styled_rows.append(styled)
        return data_table(headers, styled_rows)
