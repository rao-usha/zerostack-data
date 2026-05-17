"""
SPEC 060 — Diligence Pack Taxonomies + Census CBP Coverage Audit.

All tests are pure-Python — no DB, no live API. The audit script's output
itself isn't tested here (it produces a CSV reviewed by a human); these tests
cover the taxonomy module the script depends on.
"""
import json
import pytest

from app.services.diligence import taxonomies as tx
from app.services.diligence.taxonomies import (
    REF_DIR,
    load_naics,
    load_msa,
    load_naics_sic_crosswalk,
    naics_label,
    naics_parents,
    naics_children,
    msa_title,
    msa_counties,
    state_counties,
    naics_to_sic,
)


# ─────────────────────────────────────────────────────────────────────────────
# T1–T3: NAICS lookups
# ─────────────────────────────────────────────────────────────────────────────

class TestNaicsLookups:

    def test_naics_lookup_roundtrip(self):
        """T1: code → label → code roundtrips for 2/4/6-digit codes.

        We trust the dictionary file (built from Census) for the *value* of
        any specific label; the roundtrip property is what we own here —
        every code we know resolves to a label, every code is present in the
        dict.
        """
        for code in ("11", "1111", "332323"):
            label = naics_label(code)
            assert isinstance(label, str) and label, f"empty label for {code}"
            # roundtrip — the code is the key, so lookup-by-key works trivially.
            # The real assertion: lookup is total (no KeyError).
            assert load_naics()[code].code == code

    def test_naics_parents_chain(self):
        """T2: parents('332323') returns the full ancestor chain in order shortest→longest."""
        chain = naics_parents("332323")
        # 6-digit code → 5/4/3/2-digit ancestors
        assert chain == ["33", "332", "3323", "33232"]

        # 4-digit code → 3/2-digit ancestors
        assert naics_parents("3323") == ["33", "332"]

        # 2-digit code → no ancestors
        assert naics_parents("11") == []

    def test_naics_invalid_code_raises(self):
        """T3: naics_label('99999') raises ValueError, not silent None."""
        with pytest.raises(ValueError):
            naics_label("99999")
        with pytest.raises(ValueError):
            naics_parents("99999")


# ─────────────────────────────────────────────────────────────────────────────
# T4–T5: MSA dictionary
# ─────────────────────────────────────────────────────────────────────────────

class TestMsaDict:

    def test_msa_count_in_expected_range(self):
        """T4: MSA dict carries the full OMB 2023 set.

        The exact count is ~384 (US) + ~6 (PR) = ~390; we allow a small band
        because OMB occasionally republishes the delineation list.
        """
        msa = load_msa()
        assert 380 <= len(msa) <= 400, f"got {len(msa)} MSAs — outside expected band"

    def test_msa_counties_nonempty(self):
        """T5: every MSA has ≥1 constituent county FIPS, all 5-digit strings."""
        msa = load_msa()
        for cbsa_code, rec in msa.items():
            assert rec.county_fips_list, f"MSA {cbsa_code} has no counties"
            for fips in rec.county_fips_list:
                assert len(fips) == 5 and fips.isdigit(), (
                    f"MSA {cbsa_code} has malformed county FIPS {fips!r}"
                )

    def test_msa_title_lookup(self):
        """T5b: msa_title('26420') returns the Houston-area MSA title."""
        title = msa_title("26420")
        assert "Houston" in title


# ─────────────────────────────────────────────────────────────────────────────
# T6: NAICS↔SIC crosswalk
# ─────────────────────────────────────────────────────────────────────────────

class TestNaicsSicCrosswalk:

    def test_naics_to_sic_covers_sec_companies(self):
        """T6: every NAICS-4 in the crosswalk has ≥1 SIC entry (pure JSON check)."""
        xwalk = load_naics_sic_crosswalk()
        assert len(xwalk) > 0
        empty_buckets = [k for k, v in xwalk.items() if not v]
        assert empty_buckets == [], (
            f"NAICS buckets with no mapped SICs: {empty_buckets[:5]}"
        )

    def test_naics_to_sic_lookup_known_bucket(self):
        """T6b: naics_to_sic returns a list, empty for unmapped NAICS."""
        # known bucket from the crosswalk sample
        assert isinstance(naics_to_sic("1111"), list)
        # unmapped NAICS returns empty (NOT a KeyError — matches our 'lookup is total' contract for the crosswalk specifically)
        assert naics_to_sic("999999") == []


# ─────────────────────────────────────────────────────────────────────────────
# T7: Caching
# ─────────────────────────────────────────────────────────────────────────────

class TestCaching:

    def test_loaders_cached(self):
        """T7: second call to load_naics() is the SAME dict object as the first."""
        a = load_naics()
        b = load_naics()
        assert a is b, "load_naics() returned a different object on second call — not cached"

        a = load_msa()
        b = load_msa()
        assert a is b, "load_msa() not cached"

        a = load_naics_sic_crosswalk()
        b = load_naics_sic_crosswalk()
        assert a is b, "load_naics_sic_crosswalk() not cached"


# ─────────────────────────────────────────────────────────────────────────────
# T8: state_counties helper
# ─────────────────────────────────────────────────────────────────────────────

class TestStateCounties:

    def test_state_counties_texas_has_msa_counties(self):
        """T8: state_counties('48') returns the Texas counties present in our MSA dict.

        Without a full national counties dictionary we can only return counties
        that belong to at least one MSA in the state. Texas MSAs (Houston, DFW,
        Austin, San Antonio, etc.) cover ≥30 counties together — that's a
        defensible floor without being brittle.
        """
        counties = state_counties("48")
        assert len(counties) >= 30, f"got {len(counties)} TX counties — too few"
        # All should be 5-digit FIPS starting with state prefix "48"
        assert all(c.startswith("48") and len(c) == 5 for c in counties)

    def test_state_counties_invalid_state_raises(self):
        """T8b: state_counties('99') (no state with FIPS 99) raises ValueError."""
        with pytest.raises(ValueError):
            state_counties("99")
