"""
Rights review batch 1 (SPEC_142 workflow) — SEC, Treasury, BLS.

The reviewer of record approved items 1-3 of batch 1 and held FDIC. This file pins
the code side: the 8 approved datasets carry a complete block (storage and commercial
use assessed as ``allowed``, each backed by a verified citation), nothing else changed
(``tests/fixtures/rights_batch_1_before.json`` is every spec's full rights block at
ecb0d56), and nothing became ``reviewed`` — that only comes from a committed hash.

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
        assert s.redistribution == "open" and s.reviewed is False and s.status_public not in ("ga", "beta")
        assert s.rights_gate == [] or not s.rights_gate

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
                      "storage_max_age_days", "proposed", "reviewed", "effective_redistribution"):
                assert n[f] == b[f], (key, f)
            assert n["rights_hash"] != b["rights_hash"], key

    def test_nothing_reviewed(self):
        from app.catalog.rights_reviewed import REVIEWED

        assert all(not s.reviewed for s in _catalog().values())
        assert not set(APPROVED) & set(REVIEWED)


@pytest.mark.unit
class TestNothingElseChanged:
    def test_every_other_block_is_unchanged(self):
        before, cat = _before(), _catalog()
        assert set(cat) == set(before)
        changed = sorted(k for k, s in cat.items() if _now(s) != before[k])
        assert set(changed) == set(APPROVED), changed

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
            for f in ("storage", "commercial_use"):
                if before[key][f] is None and getattr(s, f) is not None:
                    assert s.citation_url and s.citation_quote and s.rights_confidence, (key, f)


def test_print_new_hashes(capsys):
    cat = _catalog()
    with capsys.disabled():
        for key in sorted(APPROVED):
            print(f"\n{key} {cat[key].rights_hash}", end="")
