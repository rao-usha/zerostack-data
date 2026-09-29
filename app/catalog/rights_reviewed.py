"""
Rights sign-offs that are in force (SPEC_142, PLAN_088 decision 8).

**Code truth plus a committed hash.** A dataset's ``reviewed`` is True only
when ``REVIEWED[key]`` holds the ``rights_hash`` of its *current* rights block
(``DatasetSpec.rights_hash``: licence, licence URL, redistribution,
attribution, storage, commercial use, share-alike, PII class, origin). Change
any of those in ``rights.py`` or ``datasets.py`` and the hash no longer
matches, so the dataset silently drops back to unreviewed and re-enters the
review queue.

The ``catalog_rights_review`` table is the audit trail (who decided what,
when, against which hash); it never flips anything by itself. To bring a
sign-off into force a human runs::

    python -m app.catalog.rights_review --emit --out app/catalog/rights_reviewed.py

reviews the diff (it lists only ``confirm_current`` decisions whose hash still
matches the code) and commits it. Nothing in the application writes this file.

Format: ``{dataset_key: (rights_hash, review_id)}``.
"""

from __future__ import annotations

from dataclasses import replace
from typing import Dict, Tuple

REVIEWED: Dict[str, Tuple[str, int]] = {
    'bls_series': ('b6b34e7f5cab4436e3421a6f4b753b3aea5b18781e01847dea667a446a532bc9', 8),
    'sec_13f': ('b00bc581832f55c096481d419aa161f4fbead2bf39b0cdfc2677843e7f83aa86', 1),
    'sec_companyfacts': ('329cca3754ad15bccf91f7402f003a7666aad182089bbd6442f98ee4b3a63671', 2),
    'treasury_auctions': ('24417dee59e55bbf807f340d1d0f8b2baf0a7dd89967d3c4ac9e0d8e27bb84be', 7),
    'treasury_daily_balance': ('24417dee59e55bbf807f340d1d0f8b2baf0a7dd89967d3c4ac9e0d8e27bb84be', 3),
    'treasury_debt_outstanding': ('24417dee59e55bbf807f340d1d0f8b2baf0a7dd89967d3c4ac9e0d8e27bb84be', 4),
    'treasury_interest_rates': ('24417dee59e55bbf807f340d1d0f8b2baf0a7dd89967d3c4ac9e0d8e27bb84be', 5),
    'treasury_monthly_statement': ('24417dee59e55bbf807f340d1d0f8b2baf0a7dd89967d3c4ac9e0d8e27bb84be', 6),
}


def apply_review(spec):
    """``spec`` with ``reviewed`` set from the committed sign-offs (hash must match)."""
    entry = REVIEWED.get(spec.key)
    reviewed = bool(entry) and entry[0] == spec.rights_hash
    return spec if spec.reviewed == reviewed else replace(spec, reviewed=reviewed)
