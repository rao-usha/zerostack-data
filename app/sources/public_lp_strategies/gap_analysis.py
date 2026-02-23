"""
LP Allocation Gap Analysis.

Computes target vs current allocation gaps for institutional LPs,
identifying where capital must be deployed. The killer feature for
PE buyers: which LPs are underweight in private equity and need to
deploy capital?

Uses lp_strategy_snapshot + lp_asset_class_target_allocation tables.
Handles lp_fund table gracefully (LEFT JOIN, may not exist).
"""

import logging
from typing import Any, Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


def _safe_float(val: Any) -> Optional[float]:
    """Safely convert string/numeric value to float."""
    if val is None:
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _classify_gap_direction(gap_pct: float) -> str:
    """Classify gap as underweight, overweight, or on_target."""
    if gap_pct > 0.5:
        return "underweight"
    elif gap_pct < -0.5:
        return "overweight"
    return "on_target"


def _calculate_urgency(
    gap_pct: float,
    min_weight: Optional[float],
    max_weight: Optional[float],
    current_weight: Optional[float],
) -> float:
    """
    Urgency score 0-100 based on gap magnitude + range breach.

    - Base: absolute gap magnitude (0-10% → 0-50)
    - Bonus: +25 if current breaches min/max range
    - Bonus: +25 if gap > 5%
    """
    urgency = min(abs(gap_pct) * 5, 50.0)

    # Range breach bonus
    if current_weight is not None:
        if min_weight is not None and current_weight < min_weight:
            urgency += 25.0
        elif max_weight is not None and current_weight > max_weight:
            urgency += 25.0

    # Large gap bonus
    if abs(gap_pct) > 5.0:
        urgency += 25.0

    return min(urgency, 100.0)


def _ensure_lp_fund_accessible(db: Session) -> bool:
    """Check if lp_fund table exists and is joinable."""
    try:
        db.execute(text("SELECT 1 FROM lp_fund LIMIT 0"))
        return True
    except Exception:
        db.rollback()
        return False


def compute_lp_allocation_gaps(
    db: Session, lp_id: int
) -> Dict[str, Any]:
    """
    Compute allocation gaps for a single LP.

    For the latest strategy snapshot, computes gap = target - current
    per asset class, with direction, capital gap, and urgency.

    Args:
        db: Database session
        lp_id: LP fund ID (lp_strategy_snapshot.lp_id)

    Returns:
        Dict with LP info, gaps list, and summary stats
    """
    has_lp_fund = _ensure_lp_fund_accessible(db)

    # Get LP info
    lp_info = {"lp_id": lp_id, "lp_name": None, "aum_usd_billions": None}
    if has_lp_fund:
        try:
            lp_query = text("""
                SELECT name, formal_name, lp_type, jurisdiction, aum_usd_billions
                FROM lp_fund WHERE id = :lp_id
            """)
            lp_row = db.execute(lp_query, {"lp_id": lp_id}).mappings().fetchone()
            if lp_row:
                lp_info.update({
                    "lp_name": lp_row["name"],
                    "formal_name": lp_row.get("formal_name"),
                    "lp_type": lp_row.get("lp_type"),
                    "jurisdiction": lp_row.get("jurisdiction"),
                    "aum_usd_billions": lp_row.get("aum_usd_billions"),
                })
        except Exception as e:
            logger.debug(f"lp_fund lookup failed for lp_id={lp_id}: {e}")

    # Get latest strategy snapshot for this LP
    snapshot_query = text("""
        SELECT s.id AS strategy_id, s.program, s.fiscal_year, s.fiscal_quarter,
               s.strategy_date
        FROM lp_strategy_snapshot s
        WHERE s.lp_id = :lp_id
        ORDER BY s.fiscal_year DESC, s.fiscal_quarter DESC
        LIMIT 1
    """)
    try:
        snapshot = db.execute(
            snapshot_query, {"lp_id": lp_id}
        ).mappings().fetchone()
    except Exception as e:
        return {"error": f"Failed to fetch strategy snapshot: {e}", **lp_info}

    if not snapshot:
        return {"error": "No strategy snapshot found", **lp_info, "gaps": []}

    strategy_id = snapshot["strategy_id"]

    # Get allocation data
    alloc_query = text("""
        SELECT asset_class, target_weight_pct, current_weight_pct,
               min_weight_pct, max_weight_pct, benchmark_weight_pct
        FROM lp_asset_class_target_allocation
        WHERE strategy_id = :sid
        ORDER BY asset_class
    """)
    try:
        rows = db.execute(
            alloc_query, {"sid": strategy_id}
        ).mappings().fetchall()
    except Exception as e:
        return {"error": f"Failed to fetch allocations: {e}", **lp_info, "gaps": []}

    aum_billions = _safe_float(lp_info.get("aum_usd_billions"))

    gaps = []
    total_underweight_capital = 0.0
    total_overweight_capital = 0.0

    for row in rows:
        target = _safe_float(row["target_weight_pct"])
        current = _safe_float(row["current_weight_pct"])
        min_wt = _safe_float(row["min_weight_pct"])
        max_wt = _safe_float(row["max_weight_pct"])
        benchmark = _safe_float(row["benchmark_weight_pct"])

        if target is None or current is None:
            continue

        gap_pct = target - current
        direction = _classify_gap_direction(gap_pct)
        urgency = _calculate_urgency(gap_pct, min_wt, max_wt, current)

        # Capital gap in USD
        gap_capital_usd = None
        if aum_billions is not None:
            gap_capital_usd = round(gap_pct / 100 * aum_billions * 1_000_000_000, 0)
            if gap_pct > 0:
                total_underweight_capital += gap_capital_usd
            else:
                total_overweight_capital += abs(gap_capital_usd)

        # Range breach check
        in_range = True
        if min_wt is not None and current < min_wt:
            in_range = False
        if max_wt is not None and current > max_wt:
            in_range = False

        gaps.append({
            "asset_class": row["asset_class"],
            "target_weight_pct": target,
            "current_weight_pct": current,
            "gap_pct": round(gap_pct, 2),
            "gap_direction": direction,
            "gap_capital_usd": gap_capital_usd,
            "urgency_score": round(urgency, 1),
            "min_weight_pct": min_wt,
            "max_weight_pct": max_wt,
            "benchmark_weight_pct": benchmark,
            "in_range": in_range,
        })

    # Sort by urgency descending
    gaps.sort(key=lambda g: g["urgency_score"], reverse=True)

    return {
        **lp_info,
        "strategy_id": strategy_id,
        "fiscal_year": snapshot["fiscal_year"],
        "fiscal_quarter": snapshot["fiscal_quarter"],
        "strategy_date": str(snapshot["strategy_date"]) if snapshot["strategy_date"] else None,
        "asset_class_count": len(gaps),
        "total_underweight_capital_usd": round(total_underweight_capital, 0) if aum_billions else None,
        "total_overweight_capital_usd": round(total_overweight_capital, 0) if aum_billions else None,
        "gaps": gaps,
    }


def compute_all_lp_gaps(
    db: Session, asset_class_filter: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    Compute allocation gaps for all LPs.

    Args:
        db: Database session
        asset_class_filter: Optional filter to only return gaps for one asset class

    Returns:
        List of LP gap results
    """
    # Get all distinct lp_ids with strategy data
    query = text("""
        SELECT DISTINCT lp_id FROM lp_strategy_snapshot ORDER BY lp_id
    """)
    try:
        rows = db.execute(query).mappings().fetchall()
    except Exception as e:
        logger.error(f"Failed to fetch LP ids: {e}")
        return []

    results = []
    for row in rows:
        lp_id = row["lp_id"]
        lp_gaps = compute_lp_allocation_gaps(db, lp_id)

        if lp_gaps.get("error"):
            continue

        # Filter to specific asset class if requested
        if asset_class_filter:
            lp_gaps["gaps"] = [
                g for g in lp_gaps.get("gaps", [])
                if g["asset_class"] == asset_class_filter
            ]

        if lp_gaps.get("gaps"):
            results.append(lp_gaps)

    return results


def find_capital_deployment_opportunities(
    db: Session,
    asset_class: str = "private_equity",
    min_gap_pct: float = 1.0,
) -> List[Dict[str, Any]]:
    """
    Find LPs that are underweight in a given asset class — sorted by
    capital to deploy. This is the killer PE demo query.

    Args:
        db: Database session
        asset_class: Asset class to look for underweight LPs (default: private_equity)
        min_gap_pct: Minimum gap percentage to include (default: 1.0)

    Returns:
        List of opportunities sorted by deployment capital (largest first)
    """
    has_lp_fund = _ensure_lp_fund_accessible(db)

    if has_lp_fund:
        query = text("""
            SELECT
                s.lp_id,
                f.name AS lp_name,
                f.lp_type,
                f.aum_usd_billions,
                s.fiscal_year,
                s.fiscal_quarter,
                a.target_weight_pct,
                a.current_weight_pct,
                a.min_weight_pct,
                a.max_weight_pct
            FROM lp_strategy_snapshot s
            JOIN lp_asset_class_target_allocation a ON s.id = a.strategy_id
            LEFT JOIN lp_fund f ON s.lp_id = f.id
            WHERE a.asset_class = :asset_class
            AND s.id IN (
                SELECT id FROM (
                    SELECT DISTINCT ON (lp_id) id
                    FROM lp_strategy_snapshot
                    ORDER BY lp_id, fiscal_year DESC, fiscal_quarter DESC
                ) latest
            )
            ORDER BY s.lp_id
        """)
    else:
        query = text("""
            SELECT
                s.lp_id,
                NULL AS lp_name,
                NULL AS lp_type,
                NULL AS aum_usd_billions,
                s.fiscal_year,
                s.fiscal_quarter,
                a.target_weight_pct,
                a.current_weight_pct,
                a.min_weight_pct,
                a.max_weight_pct
            FROM lp_strategy_snapshot s
            JOIN lp_asset_class_target_allocation a ON s.id = a.strategy_id
            WHERE a.asset_class = :asset_class
            AND s.id IN (
                SELECT id FROM (
                    SELECT DISTINCT ON (lp_id) id
                    FROM lp_strategy_snapshot
                    ORDER BY lp_id, fiscal_year DESC, fiscal_quarter DESC
                ) latest
            )
            ORDER BY s.lp_id
        """)

    try:
        rows = db.execute(query, {"asset_class": asset_class}).mappings().fetchall()
    except Exception as e:
        logger.error(f"Deployment opportunities query failed: {e}")
        return []

    opportunities = []
    for row in rows:
        target = _safe_float(row["target_weight_pct"])
        current = _safe_float(row["current_weight_pct"])
        if target is None or current is None:
            continue

        gap_pct = target - current
        if gap_pct < min_gap_pct:
            continue

        aum_b = _safe_float(row["aum_usd_billions"])
        gap_capital_usd = None
        if aum_b is not None:
            gap_capital_usd = round(gap_pct / 100 * aum_b * 1_000_000_000, 0)

        min_wt = _safe_float(row["min_weight_pct"])
        max_wt = _safe_float(row["max_weight_pct"])
        urgency = _calculate_urgency(gap_pct, min_wt, max_wt, current)

        opportunities.append({
            "lp_id": row["lp_id"],
            "lp_name": row["lp_name"] or f"LP #{row['lp_id']}",
            "lp_type": row["lp_type"],
            "aum_usd_billions": aum_b,
            "asset_class": asset_class,
            "target_weight_pct": target,
            "current_weight_pct": current,
            "gap_pct": round(gap_pct, 2),
            "gap_capital_usd": gap_capital_usd,
            "urgency_score": round(urgency, 1),
            "fiscal_year": row["fiscal_year"],
            "fiscal_quarter": row["fiscal_quarter"],
        })

    # Sort by deployment capital (largest first), then by gap_pct
    opportunities.sort(
        key=lambda x: (x["gap_capital_usd"] or 0, x["gap_pct"]),
        reverse=True,
    )

    return opportunities


def get_allocation_summary(db: Session) -> Dict[str, Any]:
    """
    Aggregate allocation summary across all LPs.

    Returns:
        Dict with total LPs, total underweight capital by asset class, etc.
    """
    has_lp_fund = _ensure_lp_fund_accessible(db)

    # Count LPs with strategy data
    count_query = text("""
        SELECT COUNT(DISTINCT lp_id) AS lp_count
        FROM lp_strategy_snapshot
    """)
    try:
        lp_count = db.execute(count_query).mappings().fetchone()["lp_count"]
    except Exception:
        lp_count = 0

    # Aggregate gaps by asset class
    if has_lp_fund:
        agg_query = text("""
            SELECT
                a.asset_class,
                COUNT(DISTINCT s.lp_id) AS lp_count,
                AVG(CAST(NULLIF(a.target_weight_pct, '') AS NUMERIC)) AS avg_target_pct,
                AVG(CAST(NULLIF(a.current_weight_pct, '') AS NUMERIC)) AS avg_current_pct,
                SUM(
                    CASE
                        WHEN CAST(NULLIF(a.target_weight_pct, '') AS NUMERIC) >
                             CAST(NULLIF(a.current_weight_pct, '') AS NUMERIC)
                        THEN (
                            CAST(NULLIF(a.target_weight_pct, '') AS NUMERIC) -
                            CAST(NULLIF(a.current_weight_pct, '') AS NUMERIC)
                        ) / 100.0 *
                        COALESCE(CAST(NULLIF(f.aum_usd_billions, '') AS NUMERIC), 0) *
                        1000000000
                        ELSE 0
                    END
                ) AS total_underweight_capital_usd
            FROM lp_strategy_snapshot s
            JOIN lp_asset_class_target_allocation a ON s.id = a.strategy_id
            LEFT JOIN lp_fund f ON s.lp_id = f.id
            WHERE s.id IN (
                SELECT id FROM (
                    SELECT DISTINCT ON (lp_id) id
                    FROM lp_strategy_snapshot
                    ORDER BY lp_id, fiscal_year DESC, fiscal_quarter DESC
                ) latest
            )
            AND a.target_weight_pct IS NOT NULL
            AND a.current_weight_pct IS NOT NULL
            AND a.target_weight_pct != ''
            AND a.current_weight_pct != ''
            GROUP BY a.asset_class
            ORDER BY total_underweight_capital_usd DESC
        """)
    else:
        agg_query = text("""
            SELECT
                a.asset_class,
                COUNT(DISTINCT s.lp_id) AS lp_count,
                AVG(CAST(NULLIF(a.target_weight_pct, '') AS NUMERIC)) AS avg_target_pct,
                AVG(CAST(NULLIF(a.current_weight_pct, '') AS NUMERIC)) AS avg_current_pct,
                0 AS total_underweight_capital_usd
            FROM lp_strategy_snapshot s
            JOIN lp_asset_class_target_allocation a ON s.id = a.strategy_id
            WHERE s.id IN (
                SELECT id FROM (
                    SELECT DISTINCT ON (lp_id) id
                    FROM lp_strategy_snapshot
                    ORDER BY lp_id, fiscal_year DESC, fiscal_quarter DESC
                ) latest
            )
            AND a.target_weight_pct IS NOT NULL
            AND a.current_weight_pct IS NOT NULL
            AND a.target_weight_pct != ''
            AND a.current_weight_pct != ''
            GROUP BY a.asset_class
            ORDER BY a.asset_class
        """)

    try:
        rows = db.execute(agg_query).mappings().fetchall()
    except Exception as e:
        logger.error(f"Allocation summary query failed: {e}")
        return {"total_lps": lp_count, "by_asset_class": [], "error": str(e)}

    by_asset_class = []
    for row in rows:
        avg_target = _safe_float(row["avg_target_pct"])
        avg_current = _safe_float(row["avg_current_pct"])
        avg_gap = round(avg_target - avg_current, 2) if avg_target and avg_current else None

        by_asset_class.append({
            "asset_class": row["asset_class"],
            "lp_count": row["lp_count"],
            "avg_target_pct": round(avg_target, 2) if avg_target else None,
            "avg_current_pct": round(avg_current, 2) if avg_current else None,
            "avg_gap_pct": avg_gap,
            "total_underweight_capital_usd": (
                round(float(row["total_underweight_capital_usd"]), 0)
                if row["total_underweight_capital_usd"]
                else 0
            ),
        })

    return {
        "total_lps": lp_count,
        "by_asset_class": by_asset_class,
    }
