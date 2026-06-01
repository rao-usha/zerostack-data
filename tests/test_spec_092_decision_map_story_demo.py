"""
Tests for SPEC 092 — Decision Map storytelling demo + remove #map-empty.

Frontend-only structural tests. We parse atlas.html for the new story
banner + runner and assert the old #map-empty centered modal is gone.
"""
import re
from pathlib import Path

import pytest

ATLAS_HTML = Path(__file__).parent.parent / "frontend" / "atlas.html"


@pytest.fixture(scope="module")
def html():
    return ATLAS_HTML.read_text(encoding="utf-8")


class TestSpec092StoryDemo:

    def test_map_empty_removed(self, html):
        """T1: no live #map-empty markup, CSS, or JS calls remain.
        Comments mentioning the historical id are fine."""
        # Markup gone
        assert 'id="map-empty"' not in html
        assert 'id="map-empty-demo"' not in html
        assert 'id="map-empty-thesis"' not in html
        # No CSS selectors for #map-empty (comments are tolerated)
        non_comment = "\n".join(
            ln for ln in html.splitlines()
            if "map-empty" in ln and not ln.lstrip().startswith("/*")
            and not ln.lstrip().startswith("//")
            and "removed entirely" not in ln  # spec note comment
            and "centered modal" not in ln    # other spec notes
        )
        assert "#map-empty" not in non_comment, non_comment[:200]
        # No JS getElementById('map-empty')
        assert "getElementById('map-empty')" not in html
        assert "getElementById(\"map-empty\")" not in html

    def test_story_banner_present(self, html):
        """T2: #story-banner with kicker, message, dots, Skip button."""
        assert 'id="story-banner"' in html
        m = re.search(r'<div id="story-banner".*?</div>\s*</div>',
                      html, re.DOTALL)
        assert m, "#story-banner block not found"
        block = m.group(0)
        assert 'class="kicker"' in block
        assert 'class="msg"' in block
        assert 'class="dots"' in block
        assert 'class="skip"' in block

    def test_story_runner_exists(self, html):
        """T3: storytelling helpers + 6-beat runner declared with a
        cancellation token."""
        for fn in ("function openStoryBanner", "function closeStoryBanner",
                   "function setStoryBeat", "function storyBeat",
                   "function skipStory", "function cancelAnyRunningStory",
                   "async function runFurnitureStoryDemo"):
            assert fn in html, f"missing {fn}"
        # Cancellation guard pattern
        assert "RUNNING_STORY" in html
        assert "cancelled:" in html or "cancelled =" in html
        # 6 beats — count storyBeat( invocations inside the runner
        m = re.search(r"async function runFurnitureStoryDemo.*?\n  \}",
                      html, re.DOTALL)
        assert m, "runFurnitureStoryDemo body not found"
        body = m.group(0)
        assert body.count("await storyBeat(") == 6, body.count("await storyBeat(")

    def test_demo_button_invokes_story(self, html):
        """T4: thesis-demo onClick now wires to runFurnitureStoryDemo
        (not the silent loadFurnitureDemo path)."""
        assert "thesis-demo').onclick = runFurnitureStoryDemo" in html
        # Skip button wired
        assert "#story-banner .skip" in html
        assert "skipStory" in html

    def test_blank_init_no_modal(self, html):
        """T5: init's blank-mode branch no longer toggles a centered
        modal — it leaves the basemap empty."""
        m = re.search(r"async function init\(\).*?\n  \}", html, re.DOTALL)
        assert m, "init() body not found"
        init_body = m.group(0)
        assert "map-empty" not in init_body
