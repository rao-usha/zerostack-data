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

REVIEWED: Dict[str, Tuple[str, int]] = {}


def apply_review(spec):
    """``spec`` with ``reviewed`` set from the committed sign-offs (hash must match)."""
    entry = REVIEWED.get(spec.key)
    reviewed = bool(entry) and entry[0] == spec.rights_hash
    return spec if spec.reviewed == reviewed else replace(spec, reviewed=reviewed)
