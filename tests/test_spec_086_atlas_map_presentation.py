"""
Tests for SPEC 086 — map data-presentation system + Cursor-style Pilot chat.

Frontend-only feature; verified by parsing atlas.html for the new
structures: semantic ramps, lighter opacity + zoom taper + single-owner
transition, overlay coexistence, and the Cursor-style chat (bottom
composer, thread, New chat).
"""
import re
from pathlib import Path

import pytest

ATLAS_HTML = Path(__file__).parent.parent / "frontend" / "atlas.html"


@pytest.fixture(scope="module")
def html():
    return ATLAS_HTML.read_text(encoding="utf-8")


class TestSpec086MapPresentation:

    def test_ramp_function_present(self, html):
        """T1: rampFor() + the three semantic ramps exist."""
        assert "function rampFor" in html
        assert "const RAMPS" in html
        for key in ("more:", "risk:", "pop:"):
            assert key in html, f"ramp {key} missing"

    def test_default_opacity_lighter(self, html):
        """T2: base fill opacity default dropped from 0.55."""
        m = re.search(r"opacity:\s*0\.(\d+),\s*//", html)
        assert m, "state.opacity default not found"
        val = float("0." + m.group(1))
        assert val <= 0.45, f"default opacity {val} should be ≤ 0.45"

    def test_zoom_taper_has_floor(self, html):
        """T3: high-zoom branch tapers to a floor, not a hard 0."""
        # The old code did `fillOpacity = 0;` at z>=10. That must be gone
        # from styleFeature; we use Math.max(0.15, …) instead.
        assert "Math.max(0.15" in html
        # No bare `fillOpacity = 0;` assignment in styleFeature.
        m = re.search(r"function styleFeature.*?\n  \}", html, re.DOTALL)
        assert m, "styleFeature not found"
        assert "fillOpacity = 0;" not in m.group(0)

    def test_transition_single_owner(self, html):
        """T4: the load transition no longer overwrites fillOpacity with
        state.opacity*eased — it passes an opacityScale to styleFeature."""
        assert "fl.setStyle({ fillOpacity: state.opacity * eased })" not in html
        assert "styleFeature(fl.feature, fl, eased)" in html

    def test_style_feature_takes_opacity_scale(self, html):
        assert "function styleFeature(feature, layer, opacityScale)" in html

    def test_active_ramp_set_on_apply(self, html):
        assert "ACTIVE_RAMP = rampFor(" in html


class TestSpec086CursorChat:

    def test_composer_at_bottom(self, html):
        """T5: composer exists in the right column with input + send."""
        assert 'id="pilot-composer"' in html
        m = re.search(r'<div id="pilot-composer".*?</div>\s*</div>\s*</aside>',
                      html, re.DOTALL)
        assert m, "#pilot-composer not found at the foot of #right-col"
        comp = m.group(0)
        assert 'id="pilot-input"' in comp
        assert 'id="pilot-go"' in comp

    def test_header_input_removed(self, html):
        """T6: the global header no longer hosts the Pilot input."""
        m = re.search(r'<div id="search-wrap".*?</div>', html, re.DOTALL)
        assert m, "#search-wrap not found"
        assert 'id="pilot-input"' not in m.group(0)
        assert 'id="pilot-go"' not in m.group(0)

    def test_thread_container(self, html):
        """T7: thread container + per-turn helpers present."""
        assert 'id="pilot-thread"' in html
        assert "function appendPilotTurn" in html
        assert "function scrollThreadToBottom" in html

    def test_new_chat_control(self, html):
        """T8: New chat control wired to clear the thread."""
        assert 'id="pilot-newchat"' in html
        assert "getElementById('pilot-newchat').onclick = pilotStartOver" in html

    def test_input_is_textarea(self, html):
        """Composer input is a textarea (multiline), not a single-line input."""
        assert re.search(r'<textarea id="pilot-input"', html)

    def test_enter_sends_shift_enter_newline(self, html):
        assert "!e.shiftKey" in html and "sendPilot()" in html
