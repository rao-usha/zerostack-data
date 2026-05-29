"""
Tests for SPEC 084 — Atlas Thesis view as Notion-style collapsible blocks.

Frontend-only feature; we test by parsing the shipped HTML for the
required markup + JS hooks. Backend assertions cover the new
sanitiser fields (region, exclude_layers) added to support the
Notion-blocks form.
"""
import re
from pathlib import Path

import pytest

ATLAS_HTML = Path(__file__).parent.parent / "frontend" / "atlas.html"


@pytest.fixture(scope="module")
def html():
    return ATLAS_HTML.read_text(encoding="utf-8")


class TestSpec084ThesisBlocks:
    """Verify the thesis pane is now 5 collapsible blocks, not a flat form."""

    def test_thesis_view_has_5_details_blocks(self, html):
        """T1: HTML contains 5 `<details class="block">` elements
        inside #view-thesis."""
        # Pull just the #view-thesis chunk to avoid counting unrelated <details>
        m = re.search(
            r'<div id="view-thesis".*?(?=<!-- View 2: Layers)',
            html, re.DOTALL,
        )
        assert m, "#view-thesis block not found"
        pane = m.group(0)
        n = len(re.findall(r'<details\s+class="block"', pane))
        assert n == 5, f"expected 5 collapsible blocks, found {n}"

    def test_industry_block_open_by_default(self, html):
        """T2: Industry block carries the `open` attribute."""
        assert re.search(
            r'<details\s+class="block"\s+data-block="industry"\s+open',
            html,
        ), "Industry block should be open by default"

    def test_demographics_block_closed_by_default(self, html):
        """T3: Demographics block has no `open` attribute."""
        m = re.search(
            r'<details\s+class="block"\s+data-block="demographics"([^>]*)>',
            html,
        )
        assert m, "Demographics block not found"
        assert "open" not in m.group(1), \
            "Demographics block should be closed by default"

    def test_save_bar_present(self, html):
        """T4: Sticky save bar markup present."""
        assert 'id="thesis-save-bar"' in html
        # Save button must live inside the save bar
        assert re.search(
            r'id="thesis-save-bar".*?id="thesis-save"',
            html, re.DOTALL,
        )

    def test_reset_button_present(self, html):
        """T5: Header reset button wired. (SPEC_085 follow-up renamed the
        worker to doResetThesis + routed the button through armConfirm to
        drop the browser confirm() dialog.)"""
        assert 'id="thesis-reset"' in html
        assert "function doResetThesis" in html
        assert "armConfirm(document.getElementById('thesis-reset')" in html

    def test_open_state_persistence_key(self, html):
        """T6: JS references atlas_thesis_open_v1."""
        assert "atlas_thesis_open_v1" in html
        assert "function initThesisBlocks" in html
        assert "function refreshThesisPreviews" in html

    def test_no_old_flat_form(self, html):
        """T7: Old <h3>Target demographics</h3> dividers are gone from
        #view-thesis (they were the old form's section headings)."""
        m = re.search(
            r'<div id="view-thesis".*?(?=<!-- View 2: Layers)',
            html, re.DOTALL,
        )
        assert m
        pane = m.group(0)
        assert "<h3>Target demographics</h3>" not in pane
        assert "<h3>Notes</h3>" not in pane

    def test_each_block_has_preview_span(self, html):
        """Every collapsible block must declare a preview span the
        JS can target via [data-preview]."""
        m = re.search(
            r'<div id="view-thesis".*?(?=<!-- View 2: Layers)',
            html, re.DOTALL,
        )
        pane = m.group(0)
        for block in ("industry", "demographics", "geography", "notes", "layers"):
            assert f'data-preview="{block}"' in pane, \
                f"missing preview span for {block}"


class TestSpec084BackendFields:
    """Backend sanitiser added two new fields."""

    def test_region_field_accepted(self):
        from app.services.atlas.pilot import _sanitize_thesis
        clean = _sanitize_thesis({"region": "Texas metros"})
        assert clean.get("region") == "Texas metros"

    def test_exclude_layers_field_accepted(self):
        from app.services.atlas.pilot import _sanitize_thesis
        clean = _sanitize_thesis({
            "exclude_layers": "disaster_nri, broadband_fixed_25_3"
        })
        assert clean["exclude_layers"].startswith("disaster_nri")

    def test_region_too_long_truncated(self):
        from app.services.atlas.pilot import _sanitize_thesis
        clean = _sanitize_thesis({"region": "x" * 1000})
        assert len(clean["region"]) <= 80

    def test_region_in_format_block(self):
        from app.services.atlas.pilot import _format_thesis_block
        block = _format_thesis_block({"region": "Texas metros"})
        assert "Region focus: Texas metros" in block

    def test_exclude_layers_in_format_block(self):
        from app.services.atlas.pilot import _format_thesis_block
        block = _format_thesis_block({"exclude_layers": "disaster_nri"})
        assert "Hide layers: disaster_nri" in block
