"""
LP Tier Classifier — PLAN_037 LP Conviction 2.0

Classifies LpFund records into conviction tiers (1–5) based on LP type and AUM.
Tiers are used downstream to weight signal confidence in the LP Conviction Score.

Tier definitions:
    1 = Sovereign Wealth / Endowment / Mega Public Pension (AUM >= $100B)
    2 = Large Public Pension (AUM >= $50B)
    3 = Mid Public Pension / Foundation (AUM < $50B, or foundation)
    4 = Insurance / Corporate Pension
    5 = Family Office / HNW / Other

Usage:
    from app.sources.lp_collection.lp_tier_classifier import classify_lp_tier, classify_all_lps

    tier = classify_lp_tier(lp)                # single record
    updated_count = classify_all_lps(db)       # bulk update null rows
"""

import logging

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Tier configuration
# ---------------------------------------------------------------------------

TIER_1_TYPES = {"sovereign_wealth", "endowment"}
TIER_2_THRESHOLD_BILLIONS = 50.0   # AUM threshold for large public pensions
TIER_1_MEGA_THRESHOLD_BILLIONS = 100.0  # Mega-pension threshold → tier 1
TIER_4_TYPES = {"corporate_pension", "insurance"}
TIER_5_TYPES = {"family_office", "hnw"}


# ---------------------------------------------------------------------------
# Classification logic
# ---------------------------------------------------------------------------

def classify_lp_tier(lp) -> int:
    """
    Classify an LpFund record into a conviction tier (1–5).

    Tier definitions:
        1 = Sovereign Wealth Fund, Endowment, or mega public pension (AUM >= $100B)
        2 = Large Public Pension (AUM >= $50B)
        3 = Mid Public Pension (AUM < $50B) or Foundation
        4 = Insurance company or Corporate Pension
        5 = Family Office, HNW individual, or unclassified

    Args:
        lp: LpFund SQLAlchemy model instance.
            Expected attributes: .lp_type (str | None), .aum_usd_billions (str | float | None)

    Returns:
        int in range [1, 5].  Defaults to 3 (mid-tier) when the type is unknown.
    """
    lp_type = (lp.lp_type or "").lower().strip()

    # aum_usd_billions is stored as String(50) in models.py — coerce safely
    try:
        aum = float(lp.aum_usd_billions or 0)
    except (ValueError, TypeError):
        aum = 0.0

    # Tier 1: Sovereign wealth funds and endowments are always tier 1
    if lp_type in TIER_1_TYPES:
        return 1

    # Tier 1: Mega public pensions (CalPERS, NY Common, etc.) treated as tier 1
    if lp_type == "public_pension" and aum >= TIER_1_MEGA_THRESHOLD_BILLIONS:
        return 1

    # Tier 2: Large public pensions
    if lp_type == "public_pension" and aum >= TIER_2_THRESHOLD_BILLIONS:
        return 2

    # Tier 3: Remaining public pensions (small/mid) and foundations
    if lp_type == "public_pension":
        return 3
    if lp_type == "foundation":
        return 3

    # Tier 4: Insurance companies and corporate pensions
    if lp_type in TIER_4_TYPES:
        return 4

    # Tier 5: Family offices, HNW, and explicitly "other" types
    if lp_type in TIER_5_TYPES:
        return 5

    # Default: mid-tier (covers empty, None, or unrecognised lp_type values)
    return 3


# ---------------------------------------------------------------------------
# Bulk classification
# ---------------------------------------------------------------------------

def classify_all_lps(db) -> int:
    """
    Run tier classification on all lp_fund rows where lp_tier is NULL and
    write the result back to the database in a single transaction.

    This function is intentionally simple — it loads all un-tiered rows into
    memory (typically a few thousand at most), classifies them in Python, and
    commits once.  For very large tables a chunked approach would be preferable,
    but LP universe size does not warrant that complexity today.

    Args:
        db: SQLAlchemy Session (synchronous).

    Returns:
        Number of rows updated (0 if all rows already had a tier set).
    """
    from app.core.models import LpFund
    from sqlalchemy import select

    # Fetch all rows with a null lp_tier.
    # NOTE: lp_tier may not yet exist as a mapped column if migrations haven't run;
    # in that case the attribute access below will raise AttributeError which the
    # caller should handle.
    stmt = select(LpFund).where(LpFund.lp_tier == None)  # noqa: E711 — SQLAlchemy requires == None
    lps = db.execute(stmt).scalars().all()

    if not lps:
        logger.info("classify_all_lps: no untiered LP rows found")
        return 0

    count = 0
    for lp in lps:
        tier = classify_lp_tier(lp)
        lp.lp_tier = tier
        count += 1

    db.commit()
    logger.info(f"classify_all_lps: updated {count} LP rows with conviction tiers")
    return count
