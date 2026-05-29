"""
Tests for SPEC 085 — Atlas Pilot cleanup + furniture-store demo thesis.

Backend tests exercise the prompt fix, the question-aware unlinked-claim
check, and the options-block stripper. Frontend tests parse atlas.html
for the escape fix, history-key unification, chat declutter, and the
furniture demo button.
"""
import re
from pathlib import Path

import pytest

ATLAS_HTML = Path(__file__).parent.parent / "frontend" / "atlas.html"


@pytest.fixture(scope="module")
def html():
    return ATLAS_HTML.read_text(encoding="utf-8")


class TestSpec085Backend:

    def test_prompt_has_real_layer_id(self):
        """T1: SYSTEM_PROMPT example uses demo_acs_median_income, not the
        invented median_household_income_acs."""
        from app.services.atlas.pilot import SYSTEM_PROMPT
        assert "demo_acs_median_income" in SYSTEM_PROMPT
        assert "median_household_income_acs" not in SYSTEM_PROMPT

    def test_prompt_forbids_guessing_layers(self):
        """T2: prompt instructs to use list_layers / not guess ids."""
        from app.services.atlas.pilot import SYSTEM_PROMPT
        assert "NEVER guess a layer_id" in SYSTEM_PROMPT
        assert "list_layers" in SYSTEM_PROMPT

    def test_unlinked_skips_question_numbers(self):
        """T3: a $ figure echoed from the question is not flagged."""
        from app.services.atlas.pilot import find_unlinked_claims
        narration = "These tracts have median income above $150K."
        out = find_unlinked_claims(
            narration, citations=[],
            question="Highlight Austin tracts with income > $150K",
        )
        assert "$150K" not in out

    def test_unlinked_skips_expanded_question_number(self):
        """T3b: '$150K' in the question also suppresses '$150,000' when
        the model reformats it in the narration."""
        from app.services.atlas.pilot import find_unlinked_claims
        narration = "Tracts above $150,000 are highlighted."
        out = find_unlinked_claims(
            narration, citations=[],
            question="Highlight Austin tracts with income > $150K",
        )
        assert not any("150" in x for x in out), out

    def test_unlinked_still_flags_uncited(self):
        """T4: a number NOT in the question and NOT cited is still flagged."""
        from app.services.atlas.pilot import find_unlinked_claims
        narration = "The median income there is $97,169."
        out = find_unlinked_claims(
            narration, citations=[],
            question="What is the income there?",
        )
        assert any("97,169" in x or "97169" in x for x in out)

    def test_strip_options_block_from_narration(self):
        """T5: trailing 'What would you like…' + bullets removed."""
        from app.services.atlas.pilot import _strip_options_block
        text = (
            "Austin's east side is more affordable.\n\n"
            "What would you like to explore next?\n"
            "- Analyze demographics\n"
            "- Locate competition\n"
            "- Investigate migration"
        )
        out = _strip_options_block(text)
        assert "What would you like" not in out
        assert "- Analyze demographics" not in out
        assert out.startswith("Austin's east side")

    def test_strip_options_block_numbered(self):
        """Numbered option lists are stripped too."""
        from app.services.atlas.pilot import _strip_options_block
        text = ("On screen now.\n\nNext steps:\n"
                "1. Look at income\n2. Look at competition")
        out = _strip_options_block(text)
        assert "Next steps" not in out
        assert out.strip() == "On screen now."

    def test_strip_keeps_normal_narration(self):
        """T6: narration without an options block is untouched."""
        from app.services.atlas.pilot import _strip_options_block
        text = ("Austin's median income is $97,169 in Travis County, "
                "with wide east-west variation.")
        assert _strip_options_block(text) == text

    def test_thesis_region_exclude_still_supported(self):
        """region + exclude_layers from SPEC_084 still flow through."""
        from app.services.atlas.pilot import _format_thesis_block
        block = _format_thesis_block({
            "industry_label": "Furniture stores",
            "region": "Austin, TX metro",
            "exclude_layers": "disaster_nri",
        })
        assert "Austin, TX metro" in block
        assert "disaster_nri" in block


class TestSpec085Frontend:

    def test_history_key_unified(self, html):
        """T7: History view READS + CLEARS the real atlas_pilot_history
        key (the one the Pilot writes). The stale key must not appear in
        any getItem/removeItem CALL (comments mentioning it are fine)."""
        # No live read of the stale key anywhere.
        assert "getItem('nexdata.pilot.hist')" not in html
        assert 'getItem("nexdata.pilot.hist")' not in html
        # renderHistoryView reads the unified key.
        m = re.search(r"function renderHistoryView\(\).*?\n  \}", html, re.DOTALL)
        assert m, "renderHistoryView not found"
        assert "getItem('atlas_pilot_history')" in m.group(0) \
            or 'getItem("atlas_pilot_history")' in m.group(0)

    def test_tool_log_not_double_escaped(self, html):
        """T8: the appendToolLog arg-render calls no longer wrap args in
        escapeHtml (which double-escaped under textContent)."""
        assert "escapeHtml(JSON.stringify(ev.args)" not in html
        assert "escapeHtml(JSON.stringify(ev.action.args)" not in html

    def test_chat_hides_tool_log(self, html):
        """T9: CSS hides #pilot-tools/#pilot-cites/#pilot-meta in #right-col."""
        assert "#right-col #pilot-tools" in html
        assert "display:none !important" in html

    def test_demo_button_present(self, html):
        """T10: furniture-store demo button + loader present."""
        assert 'id="thesis-demo"' in html
        assert "function loadFurnitureDemo" in html
        assert "Furniture stores" in html
        assert "442110" in html

    def test_no_browser_confirm_dialogs(self, html):
        """Destructive buttons must NOT use native confirm() — they use
        the inline two-click armConfirm() pattern. Comments mentioning
        'confirm()' are fine; actual `confirm(` CALLS are not."""
        live = [
            ln.strip() for ln in html.splitlines()
            if "confirm(" in ln and not ln.strip().startswith("//")
        ]
        assert live == [], f"browser confirm() call(s) found: {live}"
        assert "function armConfirm" in html
        # All three destructive buttons routed through armConfirm
        assert html.count("armConfirm(document.getElementById") == 3
