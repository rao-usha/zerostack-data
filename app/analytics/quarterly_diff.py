"""
13F Quarterly Diff & Convergence Detector.

Compares investor holdings quarter-over-quarter and detects when
multiple institutions converge on the same security.
"""

import logging
import re
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Dict, List, Optional, Tuple

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

# Quarter-end dates (month, day)
QUARTER_ENDS = [(3, 31), (6, 30), (9, 30), (12, 31)]

# Window (days) around quarter-end to match filing dates
QUARTER_WINDOW_DAYS = 5

# Default threshold for material position change (5%)
DEFAULT_MATERIAL_THRESHOLD = 0.05


# =============================================================================
# Data classes
# =============================================================================


@dataclass
class HoldingSnapshot:
    """A single holding at a point in time."""

    key: str  # CUSIP (LP) or company_id (PE)
    company_name: str
    cusip: Optional[str] = None
    ticker: Optional[str] = None
    shares: Optional[Decimal] = None
    market_value: Optional[Decimal] = None
    investment_type: Optional[str] = None


@dataclass
class HoldingChange:
    """Classified change for a single holding between two quarters."""

    key: str
    company_name: str
    cusip: Optional[str] = None
    ticker: Optional[str] = None
    change_type: str = "unchanged"  # new, exited, increased, decreased, unchanged
    shares_prev: Optional[Decimal] = None
    shares_curr: Optional[Decimal] = None
    shares_change: Optional[Decimal] = None
    shares_change_pct: Optional[float] = None
    value_prev: Optional[Decimal] = None
    value_curr: Optional[Decimal] = None
    value_change: Optional[Decimal] = None


@dataclass
class QuarterlyDiffSummary:
    """Aggregate stats for a quarterly diff."""

    total_positions_prev: int = 0
    total_positions_curr: int = 0
    new_positions: int = 0
    exited_positions: int = 0
    increased_positions: int = 0
    decreased_positions: int = 0
    unchanged_positions: int = 0
    total_value_prev: Optional[Decimal] = None
    total_value_curr: Optional[Decimal] = None
    total_value_change: Optional[Decimal] = None
    turnover_rate: Optional[float] = None  # (new + exited) / avg positions


@dataclass
class QuarterlyDiffReport:
    """Full quarterly diff result."""

    investor_id: int
    investor_type: str
    investor_name: str
    quarter_prev: Optional[str] = None
    quarter_curr: Optional[str] = None
    summary: QuarterlyDiffSummary = field(default_factory=QuarterlyDiffSummary)
    changes: List[HoldingChange] = field(default_factory=list)


@dataclass
class ConvergenceParticipant:
    """An investor participating in a convergence signal."""

    investor_id: int
    investor_type: str
    investor_name: str
    action: str  # "new" or "increased"
    shares_change: Optional[Decimal] = None
    value_change: Optional[Decimal] = None


@dataclass
class ConvergenceSignal:
    """A security where multiple investors converged."""

    company_name: str
    cusip: Optional[str] = None
    ticker: Optional[str] = None
    signal_strength: float = 0.0  # 0-100
    participant_count: int = 0
    new_position_count: int = 0
    increased_position_count: int = 0
    total_value_added: Optional[Decimal] = None
    participants: List[ConvergenceParticipant] = field(default_factory=list)


@dataclass
class ConvergenceReport:
    """Full convergence detection result."""

    quarter: str
    quarter_prev: str
    total_investors_scanned: int = 0
    total_signals: int = 0
    signals: List[ConvergenceSignal] = field(default_factory=list)


# =============================================================================
# Helpers
# =============================================================================


def _safe_decimal(value) -> Optional[Decimal]:
    """Parse a value to Decimal, handling strings with $, commas, etc."""
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    if isinstance(value, (int, float)):
        try:
            return Decimal(str(value))
        except (InvalidOperation, ValueError):
            return None
    if isinstance(value, str):
        cleaned = re.sub(r"[$,\s]", "", value.strip())
        if not cleaned or cleaned == "":
            return None
        try:
            return Decimal(cleaned)
        except (InvalidOperation, ValueError):
            return None
    return None


def _quarter_label(d: date) -> str:
    """Convert a date to a quarter label like '2024-Q4'."""
    quarter = (d.month - 1) // 3 + 1
    return f"{d.year}-Q{quarter}"


def _previous_quarter(quarter_label: str) -> str:
    """Get the previous quarter label. e.g., '2024-Q1' -> '2023-Q4'."""
    year, q = int(quarter_label[:4]), int(quarter_label[-1])
    if q == 1:
        return f"{year - 1}-Q4"
    return f"{year}-Q{q - 1}"


def _quarter_date_range(quarter_label: str) -> Tuple[date, date]:
    """Get the date range for a quarter label (with window for filing variation)."""
    year, q = int(quarter_label[:4]), int(quarter_label[-1])
    month, day = QUARTER_ENDS[q - 1]
    end_date = date(year, month, day)
    start_date = end_date - timedelta(days=QUARTER_WINDOW_DAYS)
    end_date_padded = end_date + timedelta(days=QUARTER_WINDOW_DAYS)
    return start_date, end_date_padded


def _normalize_name(name: str) -> str:
    """Normalize company name for cross-type matching."""
    if not name:
        return ""
    n = name.upper().strip()
    # Remove common suffixes
    for suffix in [
        " INC", " INC.", " CORP", " CORP.", " LLC", " LTD", " LTD.",
        " CO", " CO.", " PLC", " LP", " L.P.", " N.V.", " S.A.",
        " GROUP", " HOLDINGS", " HOLDING", " INTERNATIONAL",
        " TECHNOLOGIES", " TECHNOLOGY", " ENTERPRISES",
        ", INC", ", INC.", ", CORP", ", CORP.",
    ]:
        if n.endswith(suffix):
            n = n[: -len(suffix)]
    # Remove punctuation
    n = re.sub(r"[^A-Z0-9\s]", "", n)
    # Collapse whitespace
    n = re.sub(r"\s+", " ", n).strip()
    return n


# =============================================================================
# Service
# =============================================================================


class QuarterlyDiffService:
    """Quarterly 13F diff analysis and convergence detection."""

    def __init__(self, db: Session):
        self.db = db

    # -----------------------------------------------------------------
    # Public: list available quarters
    # -----------------------------------------------------------------

    def get_available_quarters(
        self, investor_id: int, investor_type: str = "lp"
    ) -> List[dict]:
        """Get distinct quarters with 13F data for an investor."""
        if investor_type == "pe":
            return self._get_pe_quarters(investor_id)
        return self._get_lp_quarters(investor_id)

    def _get_lp_quarters(self, investor_id: int) -> List[dict]:
        """Get quarters from portfolio_companies for an LP."""
        result = self.db.execute(
            text("""
                SELECT DISTINCT investment_date, COUNT(*) as holdings
                FROM portfolio_companies
                WHERE investor_id = :inv_id
                  AND investor_type = 'lp'
                  AND source_type = 'sec_13f'
                  AND investment_date IS NOT NULL
                GROUP BY investment_date
                ORDER BY investment_date DESC
            """),
            {"inv_id": investor_id},
        )
        quarters = []
        for row in result.fetchall():
            inv_date = row[0]
            if isinstance(inv_date, datetime):
                inv_date = inv_date.date()
            quarters.append({
                "quarter": _quarter_label(inv_date),
                "date": str(inv_date),
                "holdings_count": row[1],
            })
        return quarters

    def _get_pe_quarters(self, investor_id: int) -> List[dict]:
        """Get quarters from pe_fund_investments for a PE firm."""
        # investor_id here is firm_id — get all fund IDs for the firm
        result = self.db.execute(
            text("""
                SELECT DISTINCT fi.investment_date, COUNT(*) as holdings
                FROM pe_fund_investments fi
                JOIN pe_funds f ON fi.fund_id = f.id
                WHERE f.firm_id = :firm_id
                  AND fi.investment_type = '13F Holding'
                  AND fi.investment_date IS NOT NULL
                GROUP BY fi.investment_date
                ORDER BY fi.investment_date DESC
            """),
            {"firm_id": investor_id},
        )
        quarters = []
        for row in result.fetchall():
            inv_date = row[0]
            if isinstance(inv_date, datetime):
                inv_date = inv_date.date()
            quarters.append({
                "quarter": _quarter_label(inv_date),
                "date": str(inv_date),
                "holdings_count": row[1],
            })
        return quarters

    # -----------------------------------------------------------------
    # Public: quarterly diff
    # -----------------------------------------------------------------

    def get_quarterly_diff(
        self,
        investor_id: int,
        investor_type: str = "lp",
        quarter_curr: Optional[str] = None,
        quarter_prev: Optional[str] = None,
        material_threshold: float = DEFAULT_MATERIAL_THRESHOLD,
    ) -> QuarterlyDiffReport:
        """
        Compute quarter-over-quarter diff for an investor's 13F holdings.

        If quarters not specified, uses the two most recent.
        """
        # Get investor name
        investor_name = self._get_investor_name(investor_id, investor_type)

        # Resolve quarters
        available = self.get_available_quarters(investor_id, investor_type)
        if not available:
            return QuarterlyDiffReport(
                investor_id=investor_id,
                investor_type=investor_type,
                investor_name=investor_name or f"Investor {investor_id}",
            )

        if not quarter_curr:
            quarter_curr = available[0]["quarter"]
        if not quarter_prev:
            # Find next most recent quarter
            for q in available:
                if q["quarter"] != quarter_curr:
                    quarter_prev = q["quarter"]
                    break

        # Get snapshots
        snap_curr = self._get_snapshot(investor_id, investor_type, quarter_curr)
        snap_prev = (
            self._get_snapshot(investor_id, investor_type, quarter_prev)
            if quarter_prev
            else {}
        )

        # Compute diff
        changes = self._compute_diff(snap_prev, snap_curr, material_threshold)

        # Build summary
        summary = self._build_summary(changes, snap_prev, snap_curr)

        return QuarterlyDiffReport(
            investor_id=investor_id,
            investor_type=investor_type,
            investor_name=investor_name or f"Investor {investor_id}",
            quarter_prev=quarter_prev,
            quarter_curr=quarter_curr,
            summary=summary,
            changes=changes,
        )

    # -----------------------------------------------------------------
    # Public: convergence detection
    # -----------------------------------------------------------------

    def detect_convergence(
        self,
        quarter: Optional[str] = None,
        min_investors: int = 3,
        min_total_value: Optional[Decimal] = None,
        include_types: Optional[List[str]] = None,
    ) -> ConvergenceReport:
        """
        Detect securities where multiple investors converged (new/increased positions).

        Scans all tracked LP and PE investors, computes diffs, and aggregates
        new/increased positions by security.
        """
        types = include_types or ["lp", "pe"]

        # Get all investors
        all_investors = self._get_all_investors(types)
        if not all_investors:
            return ConvergenceReport(
                quarter=quarter or "unknown",
                quarter_prev="unknown",
            )

        # Resolve quarter if not specified
        if not quarter:
            quarter = self._find_latest_quarter(all_investors)
            if not quarter:
                return ConvergenceReport(quarter="unknown", quarter_prev="unknown")

        quarter_prev = _previous_quarter(quarter)

        # Collect new/increased positions across all investors
        # key: normalized_name -> list of ConvergenceParticipant
        convergence_map: Dict[str, dict] = {}

        investors_scanned = 0
        for inv_id, inv_type, inv_name in all_investors:
            snap_curr = self._get_snapshot(inv_id, inv_type, quarter)
            if not snap_curr:
                continue

            snap_prev = self._get_snapshot(inv_id, inv_type, quarter_prev)
            changes = self._compute_diff(snap_prev, snap_curr)
            investors_scanned += 1

            for change in changes:
                if change.change_type not in ("new", "increased"):
                    continue

                norm_name = _normalize_name(change.company_name)
                if not norm_name:
                    continue

                if norm_name not in convergence_map:
                    convergence_map[norm_name] = {
                        "company_name": change.company_name,
                        "cusip": change.cusip,
                        "ticker": change.ticker,
                        "participants": [],
                    }

                # Update cusip/ticker if we have better data
                if change.cusip and not convergence_map[norm_name]["cusip"]:
                    convergence_map[norm_name]["cusip"] = change.cusip
                if change.ticker and not convergence_map[norm_name]["ticker"]:
                    convergence_map[norm_name]["ticker"] = change.ticker

                convergence_map[norm_name]["participants"].append(
                    ConvergenceParticipant(
                        investor_id=inv_id,
                        investor_type=inv_type,
                        investor_name=inv_name,
                        action=change.change_type,
                        shares_change=change.shares_change,
                        value_change=change.value_change,
                    )
                )

        # Filter and score
        signals = []
        for norm_name, data in convergence_map.items():
            participants = data["participants"]
            if len(participants) < min_investors:
                continue

            new_count = sum(1 for p in participants if p.action == "new")
            inc_count = sum(1 for p in participants if p.action == "increased")

            total_value = Decimal("0")
            for p in participants:
                if p.value_change is not None:
                    total_value += p.value_change

            if min_total_value and total_value < min_total_value:
                continue

            # Score: base from participant count + bonuses
            score = min(100.0, (
                (len(participants) / max(investors_scanned, 1)) * 40  # breadth
                + (new_count / max(len(participants), 1)) * 30  # new-position bonus
                + min(float(total_value) / 1_000_000_000, 1.0) * 30  # value bonus
            ))

            signals.append(ConvergenceSignal(
                company_name=data["company_name"],
                cusip=data["cusip"],
                ticker=data["ticker"],
                signal_strength=round(score, 1),
                participant_count=len(participants),
                new_position_count=new_count,
                increased_position_count=inc_count,
                total_value_added=total_value if total_value > 0 else None,
                participants=participants,
            ))

        # Sort by signal strength descending
        signals.sort(key=lambda s: s.signal_strength, reverse=True)

        return ConvergenceReport(
            quarter=quarter,
            quarter_prev=quarter_prev,
            total_investors_scanned=investors_scanned,
            total_signals=len(signals),
            signals=signals,
        )

    # -----------------------------------------------------------------
    # Internal: snapshot retrieval
    # -----------------------------------------------------------------

    def _get_snapshot(
        self, investor_id: int, investor_type: str, quarter: str
    ) -> Dict[str, HoldingSnapshot]:
        """Get holdings snapshot for a given quarter, keyed by CUSIP or company_id."""
        if investor_type == "pe":
            return self._get_pe_snapshot(investor_id, quarter)
        return self._get_lp_snapshot(investor_id, quarter)

    def _get_lp_snapshot(
        self, investor_id: int, quarter: str
    ) -> Dict[str, HoldingSnapshot]:
        """LP snapshot from portfolio_companies."""
        start_date, end_date = _quarter_date_range(quarter)
        result = self.db.execute(
            text("""
                SELECT company_name, company_cusip, company_ticker,
                       shares_held, market_value_usd, investment_type
                FROM portfolio_companies
                WHERE investor_id = :inv_id
                  AND investor_type = 'lp'
                  AND source_type = 'sec_13f'
                  AND investment_date BETWEEN :start AND :end
            """),
            {"inv_id": investor_id, "start": start_date, "end": end_date},
        )

        snapshot = {}
        for row in result.fetchall():
            name = row[0] or ""
            cusip = row[1]
            key = cusip or _normalize_name(name)
            if not key:
                continue

            snapshot[key] = HoldingSnapshot(
                key=key,
                company_name=name,
                cusip=cusip,
                ticker=row[2],
                shares=_safe_decimal(row[3]),
                market_value=_safe_decimal(row[4]),
                investment_type=row[5],
            )
        return snapshot

    def _get_pe_snapshot(
        self, firm_id: int, quarter: str
    ) -> Dict[str, HoldingSnapshot]:
        """PE snapshot from pe_fund_investments."""
        start_date, end_date = _quarter_date_range(quarter)
        result = self.db.execute(
            text("""
                SELECT pc.id, pc.name, pc.ticker,
                       fi.invested_amount_usd, fi.investment_type
                FROM pe_fund_investments fi
                JOIN pe_funds f ON fi.fund_id = f.id
                JOIN pe_portfolio_companies pc ON fi.company_id = pc.id
                WHERE f.firm_id = :firm_id
                  AND fi.investment_type = '13F Holding'
                  AND fi.investment_date BETWEEN :start AND :end
            """),
            {"firm_id": firm_id, "start": start_date, "end": end_date},
        )

        snapshot = {}
        for row in result.fetchall():
            company_id = str(row[0])
            name = row[1] or ""
            snapshot[company_id] = HoldingSnapshot(
                key=company_id,
                company_name=name,
                ticker=row[2],
                market_value=_safe_decimal(row[3]),
                investment_type=row[4],
            )
        return snapshot

    # -----------------------------------------------------------------
    # Internal: diff computation
    # -----------------------------------------------------------------

    def _compute_diff(
        self,
        prev: Dict[str, HoldingSnapshot],
        curr: Dict[str, HoldingSnapshot],
        threshold: float = DEFAULT_MATERIAL_THRESHOLD,
    ) -> List[HoldingChange]:
        """Classify each holding as new/exited/increased/decreased/unchanged."""
        changes = []
        all_keys = set(prev.keys()) | set(curr.keys())

        for key in all_keys:
            p = prev.get(key)
            c = curr.get(key)

            if c and not p:
                # New position
                changes.append(HoldingChange(
                    key=key,
                    company_name=c.company_name,
                    cusip=c.cusip,
                    ticker=c.ticker,
                    change_type="new",
                    shares_curr=c.shares,
                    value_curr=c.market_value,
                    value_change=c.market_value,
                ))
            elif p and not c:
                # Exited position
                changes.append(HoldingChange(
                    key=key,
                    company_name=p.company_name,
                    cusip=p.cusip,
                    ticker=p.ticker,
                    change_type="exited",
                    shares_prev=p.shares,
                    value_prev=p.market_value,
                    value_change=(
                        -p.market_value if p.market_value else None
                    ),
                ))
            else:
                # Both exist — compare
                shares_prev = p.shares if p else None
                shares_curr = c.shares if c else None
                value_prev = p.market_value if p else None
                value_curr = c.market_value if c else None

                shares_change = None
                shares_change_pct = None
                value_change = None
                change_type = "unchanged"

                if shares_prev is not None and shares_curr is not None:
                    shares_change = shares_curr - shares_prev
                    if shares_prev > 0:
                        shares_change_pct = float(shares_change / shares_prev)
                        if shares_change_pct > threshold:
                            change_type = "increased"
                        elif shares_change_pct < -threshold:
                            change_type = "decreased"
                elif value_prev is not None and value_curr is not None:
                    value_change = value_curr - value_prev
                    if value_prev > 0:
                        pct = float(value_change / value_prev)
                        if pct > threshold:
                            change_type = "increased"
                        elif pct < -threshold:
                            change_type = "decreased"

                if value_prev is not None and value_curr is not None:
                    value_change = value_curr - value_prev

                changes.append(HoldingChange(
                    key=key,
                    company_name=c.company_name if c else p.company_name,
                    cusip=(c.cusip if c else p.cusip) if c or p else None,
                    ticker=(c.ticker if c else p.ticker) if c or p else None,
                    change_type=change_type,
                    shares_prev=shares_prev,
                    shares_curr=shares_curr,
                    shares_change=shares_change,
                    shares_change_pct=shares_change_pct,
                    value_prev=value_prev,
                    value_curr=value_curr,
                    value_change=value_change,
                ))

        # Sort: new first, then exited, then by absolute value change
        type_order = {"new": 0, "exited": 1, "increased": 2, "decreased": 3, "unchanged": 4}
        changes.sort(key=lambda c: (
            type_order.get(c.change_type, 5),
            -(abs(float(c.value_change)) if c.value_change else 0),
        ))

        return changes

    def _build_summary(
        self,
        changes: List[HoldingChange],
        prev: Dict[str, HoldingSnapshot],
        curr: Dict[str, HoldingSnapshot],
    ) -> QuarterlyDiffSummary:
        """Build aggregate summary from classified changes."""
        summary = QuarterlyDiffSummary(
            total_positions_prev=len(prev),
            total_positions_curr=len(curr),
        )

        total_val_prev = Decimal("0")
        total_val_curr = Decimal("0")

        for snap in prev.values():
            if snap.market_value:
                total_val_prev += snap.market_value
        for snap in curr.values():
            if snap.market_value:
                total_val_curr += snap.market_value

        summary.total_value_prev = total_val_prev if total_val_prev > 0 else None
        summary.total_value_curr = total_val_curr if total_val_curr > 0 else None
        if summary.total_value_prev and summary.total_value_curr:
            summary.total_value_change = summary.total_value_curr - summary.total_value_prev

        for c in changes:
            if c.change_type == "new":
                summary.new_positions += 1
            elif c.change_type == "exited":
                summary.exited_positions += 1
            elif c.change_type == "increased":
                summary.increased_positions += 1
            elif c.change_type == "decreased":
                summary.decreased_positions += 1
            else:
                summary.unchanged_positions += 1

        # Turnover rate: (new + exited) / average positions
        avg_positions = (summary.total_positions_prev + summary.total_positions_curr) / 2
        if avg_positions > 0:
            summary.turnover_rate = round(
                (summary.new_positions + summary.exited_positions) / avg_positions, 4
            )

        return summary

    # -----------------------------------------------------------------
    # Internal: investor lookup
    # -----------------------------------------------------------------

    def _get_investor_name(self, investor_id: int, investor_type: str) -> Optional[str]:
        """Get the investor's display name."""
        if investor_type == "pe":
            result = self.db.execute(
                text("SELECT name FROM pe_firms WHERE id = :id"),
                {"id": investor_id},
            )
        else:
            result = self.db.execute(
                text("SELECT name FROM lp_fund WHERE id = :id"),
                {"id": investor_id},
            )
        row = result.fetchone()
        return row[0] if row else None

    def _get_all_investors(
        self, types: List[str]
    ) -> List[Tuple[int, str, str]]:
        """Get all investors with 13F data: (id, type, name)."""
        investors = []

        if "lp" in types:
            result = self.db.execute(text("""
                SELECT DISTINCT pc.investor_id, lf.name
                FROM portfolio_companies pc
                JOIN lp_fund lf ON pc.investor_id = lf.id
                WHERE pc.investor_type = 'lp'
                  AND pc.source_type = 'sec_13f'
            """))
            for row in result.fetchall():
                investors.append((row[0], "lp", row[1] or f"LP {row[0]}"))

        if "pe" in types:
            result = self.db.execute(text("""
                SELECT DISTINCT f.firm_id, pf.name
                FROM pe_fund_investments fi
                JOIN pe_funds f ON fi.fund_id = f.id
                JOIN pe_firms pf ON f.firm_id = pf.id
                WHERE fi.investment_type = '13F Holding'
            """))
            for row in result.fetchall():
                investors.append((row[0], "pe", row[1] or f"PE {row[0]}"))

        return investors

    def _find_latest_quarter(
        self, investors: List[Tuple[int, str, str]]
    ) -> Optional[str]:
        """Find the most recent quarter across all investors."""
        latest_date = None

        # Check LP
        result = self.db.execute(text("""
            SELECT MAX(investment_date)
            FROM portfolio_companies
            WHERE source_type = 'sec_13f' AND investment_date IS NOT NULL
        """))
        row = result.fetchone()
        if row and row[0]:
            d = row[0]
            if isinstance(d, datetime):
                d = d.date()
            latest_date = d

        # Check PE
        result = self.db.execute(text("""
            SELECT MAX(investment_date)
            FROM pe_fund_investments
            WHERE investment_type = '13F Holding' AND investment_date IS NOT NULL
        """))
        row = result.fetchone()
        if row and row[0]:
            d = row[0]
            if isinstance(d, datetime):
                d = d.date()
            if latest_date is None or d > latest_date:
                latest_date = d

        if latest_date:
            return _quarter_label(latest_date)
        return None
