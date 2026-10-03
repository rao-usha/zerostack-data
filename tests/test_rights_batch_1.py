"""
Rights review batch 1 (SPEC_142 workflow) — SEC, Treasury, BLS.

The reviewer of record approved items 1-3 of batch 1 and held FDIC. This file pins
the code side: the 8 approved datasets carry a complete block (storage and commercial
use assessed as ``allowed``, each backed by a verified citation), nothing else changed
(``tests/fixtures/rights_batch_1_before.json`` is every spec's full rights block at
ecb0d56), and exactly these 8 are ``reviewed`` — via the hashes committed in
``app/catalog/rights_reviewed.py`` at 7a44563 (the sign-offs in force), never by hand.

Monotonicity note: filling a not-assessed field (``None``) with ``allowed`` is an
assessment, not a loosening, and only when the block carries a citation (URL, quote,
confidence). The SPEC_142 baseline pins redistribution / pii_class, which do not move.
"""
import json
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
BEFORE = REPO / "tests" / "fixtures" / "rights_batch_1_before.json"
SPEC142_BASELINE = REPO / "tests" / "fixtures" / "spec_142_rights_baseline.json"

SEC_URL = "https://www.sec.gov/about/privacy-information"
TREASURY_URL = "https://fiscaldata.treasury.gov/about-us/"
BLS_URL = "https://www.bls.gov/opub/copyright-information.htm"

APPROVED = {
    "sec_13f": SEC_URL,
    "sec_companyfacts": SEC_URL,
    "treasury_daily_balance": TREASURY_URL,
    "treasury_debt_outstanding": TREASURY_URL,
    "treasury_interest_rates": TREASURY_URL,
    "treasury_monthly_statement": TREASURY_URL,
    "treasury_auctions": TREASURY_URL,
    "bls_series": BLS_URL,
}
# Datasets added after the batch-1 snapshot (each carries its own cited block and spec).
ADDED_AFTER = {"ats_boards": "SPEC_151", "gleif_lei_records": "SPEC_154"}
# Blocks a later spec TIGHTENED (never loosened): pinned field by field in the test below.
TIGHTENED_AFTER = {"dunl_reference": "SPEC_154: S&P licence conflict recorded, 'loosen if CC BY' closed"}
HELD_FDIC = {"fdic_bank_financials", "fdic_institutions", "fdic_failed_banks", "fdic_summary_deposits"}

# fields that decide (hashed) plus the citation fields that document the block
DECIDING = ("license", "license_url", "redistribution", "attribution", "storage", "storage_max_age_days",
            "commercial_use", "share_alike")


def _catalog():
    from app.catalog import get_catalog

    return {s.key: s for s in get_catalog()}


def _before():
    return json.loads(BEFORE.read_text(encoding="utf-8"))["datasets"]


def _now(spec):
    return json.loads(json.dumps(spec.to_dict()["rights"], ensure_ascii=False, default=str))


@pytest.mark.unit
class TestApprovedBlocks:
    @pytest.mark.parametrize("key, url", sorted(APPROVED.items()))
    def test_complete_and_cited(self, key, url):
        s = _catalog()[key]
        assert (s.storage, s.commercial_use) == ("allowed", "allowed"), key
        assert s.storage_max_age_days is None
        assert s.citation_url == url and s.citation_quote and s.rights_confidence == "high", key
        assert s.redistribution == "open" and s.status_public not in ("ga", "beta"), key
        assert not s.rights_gate, key
        # in force through the committed hash of exactly this block
        from app.catalog.rights_reviewed import REVIEWED

        assert REVIEWED[key][0] == s.rights_hash, key
        assert s.reviewed is True and s.effective_redistribution == "open", key

    def test_citation_quotes_are_the_verified_sentences(self):
        cat = _catalog()
        for key in ("sec_13f", "sec_companyfacts"):
            assert "may be copied or further distributed by users of the web site without the SEC's " \
                   "permission" in cat[key].citation_quote
            assert "EDGARLink" in cat[key].rights_notes and "public records" in cat[key].rights_notes
        assert cat["sec_13f"].pii_class == "business_contact"
        assert "signatory" in cat["sec_13f"].rights_notes
        for key, url in APPROVED.items():
            if url == TREASURY_URL:
                assert "for non-commercial and commercial purposes" in cat[key].citation_quote, key
        assert "cite the Bureau of Labor Statistics as the source" in cat["bls_series"].citation_quote

    def test_only_deciding_terms_and_citations_changed(self):
        before, cat = _before(), _catalog()
        for key in APPROVED:
            b, n = before[key], _now(cat[key])
            assert b["storage"] is None and b["commercial_use"] is None, key  # were not assessed
            for f in ("license", "license_url", "redistribution", "attribution", "share_alike",
                      "storage_max_age_days", "proposed"):
                assert n[f] == b[f], (key, f)
            assert n["rights_hash"] != b["rights_hash"], key
            # the sign-off is the only other change: unreviewed/internal_only -> reviewed/open
            assert (b["reviewed"], b["effective_redistribution"]) == (False, "internal_only"), key
            assert (n["reviewed"], n["effective_redistribution"]) == (True, b["redistribution"]), key

    def test_reviewed_is_exactly_the_approved_eight(self):
        from app.catalog.rights_reviewed import REVIEWED

        cat = _catalog()
        assert set(REVIEWED) == set(APPROVED)
        assert {k for k, s in cat.items() if s.reviewed} == set(APPROVED)
        assert not any(cat[k].reviewed for k in HELD_FDIC)


@pytest.mark.unit
class TestNothingElseChanged:
    def test_every_other_block_is_unchanged(self):
        """Full block, ``reviewed`` and ``effective_redistribution`` included, for every
        spec outside the 8: a sign-off or edit anywhere else fails here. The 8 are allowed
        to differ; their diff is pinned field by field (sign-off included) in
        ``test_only_deciding_terms_and_citations_changed``."""
        before, cat = _before(), _catalog()
        assert set(cat) == set(before) | set(ADDED_AFTER)
        changed = sorted(k for k, s in cat.items() if k in before and _now(s) != before[k])
        assert set(changed) == set(APPROVED) | set(TIGHTENED_AFTER), changed

    def test_later_tightening_only(self):
        """SPEC_154 dunl: same redistribution / pii, proposal closed, commercial use restricted."""
        before, cat = _before(), _catalog()
        for key in TIGHTENED_AFTER:
            b, n = before[key], _now(cat[key])
            assert n["redistribution"] == b["redistribution"] == "restricted", key
            assert n["effective_redistribution"] == b["effective_redistribution"], key
            assert b["proposed"] is not None and n["proposed"] is None, key
            assert n["commercial_use"] == "restricted" and n["share_alike"] is True, key
            assert n["reviewed"] is False, key

    def test_fdic_held(self):
        before, cat = _before(), _catalog()
        base = json.loads(SPEC142_BASELINE.read_text(encoding="utf-8"))["datasets"]
        for key in HELD_FDIC:
            s = cat[key]
            assert _now(s) == before[key], key
            assert (s.storage, s.commercial_use) == (None, None)
            assert (s.redistribution, s.pii_class) == (base[key]["redistribution"], base[key]["pii_class"])

    def test_unapproved_sec_family_and_bls_collectors_not_assessed(self):
        from app.catalog.rights import COLLECTOR_RIGHTS, SOURCE_RIGHTS

        assert (SOURCE_RIGHTS["sec"].storage, SOURCE_RIGHTS["sec"].commercial_use) == (None, None)
        for c in ("bls", "bls_qcew"):
            assert (COLLECTOR_RIGHTS[c].storage, COLLECTOR_RIGHTS[c].commercial_use) == (None, None)

    def test_allowed_only_with_a_citation(self):
        """None -> allowed is an assessment only when a full citation backs it."""
        before, cat = _before(), _catalog()
        for key, s in cat.items():
            if key in ADDED_AFTER:
                assert s.citation_url and s.citation_quote and s.rights_confidence, key
                continue
            for f in ("storage", "commercial_use"):
                if before[key][f] is None and getattr(s, f) is not None:
                    assert s.citation_url and s.citation_quote and s.rights_confidence, (key, f)

