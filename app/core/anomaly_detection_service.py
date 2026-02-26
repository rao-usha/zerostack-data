"""
Anomaly Detection Service.

Compares current profile snapshots against historical baselines to detect
statistical drift, schema changes, and data quality anomalies.
"""

import logging
import re
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from sqlalchemy.orm import Session

from app.core.models import (
    DataProfileSnapshot,
    DataProfileColumn,
    DQAnomalyAlert,
    DQAnomalyThreshold,
    AnomalyAlertType,
    AnomalyAlertStatus,
    RuleSeverity,
)

logger = logging.getLogger(__name__)

# Minimum number of historical snapshots needed for statistical comparison
MIN_HISTORY_FOR_ANOMALY = 3


# =============================================================================
# Threshold resolution
# =============================================================================

def get_thresholds(
    db: Session, source: Optional[str], table_name: str
) -> DQAnomalyThreshold:
    """
    Get anomaly thresholds with cascade: table-specific -> source-specific -> global default.

    Returns a threshold object (or a synthetic default if none configured).
    """
    # 1. Table-specific match
    if source:
        all_thresholds = (
            db.query(DQAnomalyThreshold)
            .filter(
                DQAnomalyThreshold.is_enabled == 1,
                DQAnomalyThreshold.source == source,
                DQAnomalyThreshold.table_pattern.isnot(None),
            )
            .all()
        )
        for t in all_thresholds:
            if t.table_pattern and re.search(t.table_pattern, table_name):
                return t

    # 2. Source-specific (no table pattern)
    if source:
        source_threshold = (
            db.query(DQAnomalyThreshold)
            .filter(
                DQAnomalyThreshold.is_enabled == 1,
                DQAnomalyThreshold.source == source,
                DQAnomalyThreshold.table_pattern.is_(None),
            )
            .first()
        )
        if source_threshold:
            return source_threshold

    # 3. Global default (no source, no pattern)
    global_threshold = (
        db.query(DQAnomalyThreshold)
        .filter(
            DQAnomalyThreshold.is_enabled == 1,
            DQAnomalyThreshold.source.is_(None),
            DQAnomalyThreshold.table_pattern.is_(None),
        )
        .first()
    )
    if global_threshold:
        return global_threshold

    # 4. Synthetic default
    return DQAnomalyThreshold(
        row_count_sigma=2.0,
        null_rate_sigma=2.0,
        distribution_sigma=3.0,
        schema_drift_enabled=1,
    )


# =============================================================================
# Statistical helpers
# =============================================================================

def _compute_rolling_stats(values: List[float]) -> Dict[str, float]:
    """Compute mean and standard deviation for a list of values."""
    if not values:
        return {"mean": 0.0, "stddev": 0.0}
    n = len(values)
    mean = sum(values) / n
    if n < 2:
        return {"mean": mean, "stddev": 0.0}
    variance = sum((x - mean) ** 2 for x in values) / (n - 1)
    stddev = variance ** 0.5
    return {"mean": mean, "stddev": stddev}


def _check_deviation(
    current: float, mean: float, stddev: float, sigma_threshold: float
) -> Optional[float]:
    """
    Check if current value deviates from baseline by more than sigma_threshold.

    Returns deviation in sigmas if anomalous, None otherwise.
    """
    if stddev == 0:
        # If no variance in historical data but current differs, flag it
        if abs(current - mean) > 0:
            return float("inf")
        return None
    deviation = abs(current - mean) / stddev
    if deviation > sigma_threshold:
        return round(deviation, 2)
    return None


# =============================================================================
# Schema drift detection
# =============================================================================

def detect_schema_drift(
    db: Session,
    current_snapshot: DataProfileSnapshot,
    previous_snapshot: DataProfileSnapshot,
    table_name: str,
    source: Optional[str] = None,
) -> List[DQAnomalyAlert]:
    """Compare schema_snapshot JSON between two profiles to detect drift."""
    alerts = []

    current_schema = current_snapshot.schema_snapshot or []
    previous_schema = previous_snapshot.schema_snapshot or []

    current_cols = {c["name"]: c for c in current_schema}
    previous_cols = {c["name"]: c for c in previous_schema}

    current_names = set(current_cols.keys())
    previous_names = set(previous_cols.keys())

    # New columns
    for col_name in current_names - previous_names:
        alert = DQAnomalyAlert(
            table_name=table_name,
            source=source,
            column_name=col_name,
            alert_type=AnomalyAlertType.NEW_COLUMN,
            severity=RuleSeverity.INFO,
            message=f"New column '{col_name}' detected in {table_name}",
            current_value=current_cols[col_name].get("type", "unknown"),
            baseline_value=None,
            snapshot_id=current_snapshot.id,
        )
        alerts.append(alert)

    # Dropped columns
    for col_name in previous_names - current_names:
        alert = DQAnomalyAlert(
            table_name=table_name,
            source=source,
            column_name=col_name,
            alert_type=AnomalyAlertType.DROPPED_COLUMN,
            severity=RuleSeverity.WARNING,
            message=f"Column '{col_name}' dropped from {table_name}",
            current_value=None,
            baseline_value=previous_cols[col_name].get("type", "unknown"),
            snapshot_id=current_snapshot.id,
        )
        alerts.append(alert)

    # Type changes
    for col_name in current_names & previous_names:
        curr_type = current_cols[col_name].get("type")
        prev_type = previous_cols[col_name].get("type")
        if curr_type != prev_type:
            alert = DQAnomalyAlert(
                table_name=table_name,
                source=source,
                column_name=col_name,
                alert_type=AnomalyAlertType.TYPE_CHANGE,
                severity=RuleSeverity.WARNING,
                message=f"Column '{col_name}' type changed from {prev_type} to {curr_type}",
                current_value=curr_type,
                baseline_value=prev_type,
                snapshot_id=current_snapshot.id,
            )
            alerts.append(alert)

    return alerts


# =============================================================================
# Main anomaly detection
# =============================================================================

def detect_anomalies(
    db: Session,
    snapshot: DataProfileSnapshot,
    table_name: str,
) -> List[DQAnomalyAlert]:
    """
    Compare current profile against historical baseline and generate alerts.

    Tables with < MIN_HISTORY_FOR_ANOMALY historical snapshots are exempt.
    """
    # Get last 30 days of profiles
    cutoff = datetime.utcnow() - timedelta(days=30)
    history = (
        db.query(DataProfileSnapshot)
        .filter(
            DataProfileSnapshot.table_name == table_name,
            DataProfileSnapshot.profiled_at >= cutoff,
            DataProfileSnapshot.id != snapshot.id,
        )
        .order_by(DataProfileSnapshot.profiled_at.desc())
        .all()
    )

    if len(history) < MIN_HISTORY_FOR_ANOMALY:
        logger.debug(
            f"Only {len(history)} historical profiles for {table_name}, "
            f"need {MIN_HISTORY_FOR_ANOMALY} — skipping anomaly detection"
        )
        return []

    thresholds = get_thresholds(db, snapshot.source, table_name)
    alerts: List[DQAnomalyAlert] = []

    # --- Row count anomaly ---
    historical_row_counts = [h.row_count for h in history if h.row_count is not None]
    if historical_row_counts:
        stats = _compute_rolling_stats(historical_row_counts)
        deviation = _check_deviation(
            snapshot.row_count, stats["mean"], stats["stddev"], thresholds.row_count_sigma
        )
        if deviation is not None:
            direction = "increase" if snapshot.row_count > stats["mean"] else "decrease"
            alert = DQAnomalyAlert(
                table_name=table_name,
                source=snapshot.source,
                alert_type=AnomalyAlertType.ROW_COUNT_SWING,
                severity=RuleSeverity.WARNING if deviation < 4 else RuleSeverity.ERROR,
                message=(
                    f"Row count {direction}: {snapshot.row_count} vs baseline "
                    f"{stats['mean']:.0f} ({deviation:.1f}σ)"
                ),
                current_value=str(snapshot.row_count),
                baseline_value=f"{stats['mean']:.0f}",
                deviation_sigma=deviation,
                details={
                    "historical_values": historical_row_counts[-10:],
                    "mean": stats["mean"],
                    "stddev": stats["stddev"],
                    "threshold_sigma": thresholds.row_count_sigma,
                },
                snapshot_id=snapshot.id,
                job_id=snapshot.job_id,
            )
            alerts.append(alert)

    # --- Per-column null rate anomaly ---
    current_columns = (
        db.query(DataProfileColumn)
        .filter(DataProfileColumn.snapshot_id == snapshot.id)
        .all()
    )
    current_col_map = {c.column_name: c for c in current_columns}

    for col_name, current_col in current_col_map.items():
        if current_col.null_pct is None:
            continue

        # Gather historical null_pct for this column
        historical_null_pcts = []
        for h in history:
            hist_col = (
                db.query(DataProfileColumn)
                .filter(
                    DataProfileColumn.snapshot_id == h.id,
                    DataProfileColumn.column_name == col_name,
                )
                .first()
            )
            if hist_col and hist_col.null_pct is not None:
                historical_null_pcts.append(hist_col.null_pct)

        if len(historical_null_pcts) < MIN_HISTORY_FOR_ANOMALY:
            continue

        stats = _compute_rolling_stats(historical_null_pcts)
        deviation = _check_deviation(
            current_col.null_pct, stats["mean"], stats["stddev"],
            thresholds.null_rate_sigma,
        )
        if deviation is not None and current_col.null_pct > stats["mean"]:
            alert = DQAnomalyAlert(
                table_name=table_name,
                source=snapshot.source,
                column_name=col_name,
                alert_type=AnomalyAlertType.NULL_RATE_SPIKE,
                severity=RuleSeverity.WARNING if deviation < 4 else RuleSeverity.ERROR,
                message=(
                    f"Null rate spike in {col_name}: {current_col.null_pct:.1f}% "
                    f"vs baseline {stats['mean']:.1f}% ({deviation:.1f}σ)"
                ),
                current_value=f"{current_col.null_pct:.1f}%",
                baseline_value=f"{stats['mean']:.1f}%",
                deviation_sigma=deviation,
                details={
                    "historical_values": historical_null_pcts[-10:],
                    "mean": stats["mean"],
                    "stddev": stats["stddev"],
                },
                snapshot_id=snapshot.id,
                job_id=snapshot.job_id,
            )
            alerts.append(alert)

    # --- Schema drift ---
    if thresholds.schema_drift_enabled and history:
        previous = history[0]  # Most recent historical
        schema_alerts = detect_schema_drift(
            db, snapshot, previous, table_name, snapshot.source
        )
        if schema_alerts:
            # Wrap with SCHEMA_DRIFT summary if there were individual changes
            summary = DQAnomalyAlert(
                table_name=table_name,
                source=snapshot.source,
                alert_type=AnomalyAlertType.SCHEMA_DRIFT,
                severity=RuleSeverity.WARNING,
                message=f"Schema drift detected: {len(schema_alerts)} change(s) in {table_name}",
                details={
                    "changes": [
                        {"type": a.alert_type.value, "column": a.column_name, "message": a.message}
                        for a in schema_alerts
                    ]
                },
                snapshot_id=snapshot.id,
                job_id=snapshot.job_id,
            )
            alerts.append(summary)
            alerts.extend(schema_alerts)

    # Persist alerts
    for alert in alerts:
        db.add(alert)
    db.commit()

    if alerts:
        logger.warning(f"Detected {len(alerts)} anomalies for {table_name}")

    return alerts


# =============================================================================
# Alert management
# =============================================================================

def acknowledge_anomaly(
    db: Session, alert_id: int, notes: Optional[str] = None
) -> Optional[DQAnomalyAlert]:
    """Mark an anomaly alert as acknowledged."""
    alert = db.query(DQAnomalyAlert).filter(DQAnomalyAlert.id == alert_id).first()
    if not alert:
        return None
    alert.status = AnomalyAlertStatus.ACKNOWLEDGED
    if notes:
        alert.resolution_notes = notes
    db.commit()
    return alert


def resolve_anomaly(
    db: Session, alert_id: int, notes: Optional[str] = None
) -> Optional[DQAnomalyAlert]:
    """Mark an anomaly alert as resolved."""
    alert = db.query(DQAnomalyAlert).filter(DQAnomalyAlert.id == alert_id).first()
    if not alert:
        return None
    alert.status = AnomalyAlertStatus.RESOLVED
    alert.resolved_at = datetime.utcnow()
    if notes:
        alert.resolution_notes = notes
    db.commit()
    return alert


def mark_false_positive(
    db: Session, alert_id: int, notes: Optional[str] = None
) -> Optional[DQAnomalyAlert]:
    """Mark an anomaly alert as a false positive."""
    alert = db.query(DQAnomalyAlert).filter(DQAnomalyAlert.id == alert_id).first()
    if not alert:
        return None
    alert.status = AnomalyAlertStatus.FALSE_POSITIVE
    alert.resolved_at = datetime.utcnow()
    if notes:
        alert.resolution_notes = notes
    db.commit()
    return alert


def get_anomalies(
    db: Session,
    source: Optional[str] = None,
    status: Optional[str] = None,
    alert_type: Optional[str] = None,
    table_name: Optional[str] = None,
    limit: int = 100,
) -> List[DQAnomalyAlert]:
    """List anomaly alerts with optional filters."""
    query = db.query(DQAnomalyAlert)

    if source:
        query = query.filter(DQAnomalyAlert.source == source)
    if status:
        query = query.filter(DQAnomalyAlert.status == status)
    if alert_type:
        query = query.filter(DQAnomalyAlert.alert_type == alert_type)
    if table_name:
        query = query.filter(DQAnomalyAlert.table_name == table_name)

    return query.order_by(DQAnomalyAlert.detected_at.desc()).limit(limit).all()
