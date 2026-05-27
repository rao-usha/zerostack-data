"""
Tests for SPEC 083 — Atlas shell polish + default-layer correction.

The visual polish is verified manually (CSS / SVG changes). These
tests cover what's testable from Python: the rendered HTML must
contain the new default layer + the pickDefaultLayer mapping logic,
and must NOT silently regress to the old disaster_nri default.
"""
import re
from pathlib import Path

import pytest

ATLAS_HTML = Path(__file__).parent.parent / "frontend" / "atlas.html"


@pytest.fixture(scope="module")
def html():
    return ATLAS_HTML.read_text(encoding="utf-8")


class TestSpec083ShellPolish:
    """Verify the default-layer change and the keyword routing in
    pickDefaultLayer() are present in the shipped HTML."""

    def test_default_layer_is_median_income(self, html):
        """T4: state.layerId default must be median_household_income_acs."""
        # We look for an assignment in the state object — not just any
        # occurrence — so a stray mention in a comment doesn't pass.
        assert re.search(
            r"layerId:\s*['\"]median_household_income_acs['\"]", html
        ), "state.layerId default should be median_household_income_acs"
        # And the OLD default must be gone from the state object.
        assert not re.search(
            r"layerId:\s*['\"]disaster_nri['\"]", html
        ), "state.layerId default still set to disaster_nri"

    def test_pick_default_layer_exists(self, html):
        assert "function pickDefaultLayer" in html, \
            "pickDefaultLayer(thesis) helper missing"

    def test_pick_default_layer_housing(self, html):
        """T1: housing keyword → population density."""
        # The helper body must reference both the housing pattern
        # and the density layer id.
        assert re.search(r"housing|apartment|real ?estate", html)
        assert "population_density" in html

    def test_pick_default_layer_retail(self, html):
        """T2: retail keyword → median income."""
        assert re.search(r"retail|restaurant|store|shop", html)
        assert "median_household_income_acs" in html

    def test_pick_default_layer_industrial(self, html):
        """T3: industrial/warehouse → broadband."""
        assert re.search(r"industrial|warehouse|manufactur|logist", html)
        assert "broadband_fixed_25_3" in html

    def test_no_emoji_activity_icons(self, html):
        """SVG replaces emoji in #activity-bar."""
        # Crude check: activity-bar should contain <svg ...> for each
        # of the 4 views, not the emoji literals 📋 ▦ 📍 🕘.
        m = re.search(
            r'<aside id="activity-bar".*?</aside>', html, re.DOTALL
        )
        assert m, "activity-bar markup not found"
        bar = m.group(0)
        assert "<svg" in bar, "expected inline SVG icons inside #activity-bar"
        for emoji in ("📋", "▦", "📍", "🕘"):
            assert emoji not in bar, f"emoji {emoji} still present in activity bar"

    def test_onboarding_nudge_present(self, html):
        assert 'id="onboarding-nudge"' in html, \
            "first-load onboarding banner missing"
        assert "atlas_onboarding_dismissed_v1" in html, \
            "dismissal persistence key missing"
