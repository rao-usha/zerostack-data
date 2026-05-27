"""
Tests for SPEC 082 — Atlas VS-Code-style workspace shell (Phase A)

The shell itself is frontend (HTML/CSS/JS); these tests cover the
backend contract added by SPEC_082: `PilotBody.thesis_context`,
sanitisation, and the `<thesis>` system-prompt injection.
"""
import pytest


class TestSpec082AtlasVscodeShell:
    """Backend contract for the thesis_context field."""

    def test_pilot_body_thesis_optional(self):
        """T4: PilotBody accepts no `thesis_context` (backwards compat)."""
        from app.api.v1.atlas import PilotBody
        b = PilotBody(question="What is X?")
        assert b.thesis_context is None
        assert b.history is None

    def test_pilot_body_thesis_present(self):
        """T1: PilotBody accepts a populated thesis_context."""
        from app.api.v1.atlas import PilotBody
        b = PilotBody(question="X?", thesis_context={
            "industry_label": "Furniture stores",
            "target_hhi_min": 75000,
        })
        assert b.thesis_context["industry_label"] == "Furniture stores"
        assert b.thesis_context["target_hhi_min"] == 75000

    def test_thesis_context_in_system_prompt(self):
        """T2: A populated thesis_context produces a <thesis> prompt block."""
        from app.services.atlas.pilot import _format_thesis_block
        block = _format_thesis_block({
            "industry_label": "Furniture stores",
            "industry_naics": "442110",
            "target_hhi_min": 75000,
            "notes": "urban TX metros",
        })
        assert "<thesis>" in block
        assert "</thesis>" in block
        assert "Furniture stores" in block
        assert "NAICS 442110" in block
        assert "$75,000+" in block

    def test_thesis_context_omitted_when_empty(self):
        """T3: Empty / None thesis_context → no <thesis> block, no token waste."""
        from app.services.atlas.pilot import _format_thesis_block
        assert _format_thesis_block(None) == ""
        assert _format_thesis_block({}) == ""
        assert _format_thesis_block({"notes": ""}) == ""
        assert _format_thesis_block({"notes": "   "}) == ""
        # Non-dict input rejected silently
        assert _format_thesis_block("just a string") == ""
        assert _format_thesis_block(42) == ""

    def test_thesis_context_sanitized(self):
        """T5: Oversized fields truncated; non-string/out-of-range rejected."""
        from app.services.atlas.pilot import _sanitize_thesis
        raw = {
            "industry_label": "X" * 5000,
            "industry_naics": 123,        # int — must be string
            "target_hhi_min": "abc",      # not numeric
            "target_hhi_max": -50,        # out of range
            "target_pop_density_min": 9999999,  # over max
            "notes": {"bad": "dict"},     # not string
            "unknown_field": "drop",      # not allow-listed
        }
        clean = _sanitize_thesis(raw)
        assert len(clean["industry_label"]) <= 200
        assert "industry_naics" not in clean
        assert "target_hhi_min" not in clean
        assert "target_hhi_max" not in clean
        assert "target_pop_density_min" not in clean
        assert "notes" not in clean
        assert "unknown_field" not in clean

    def test_thesis_hhi_range_rendering(self):
        """Both bounds rendered when present."""
        from app.services.atlas.pilot import _format_thesis_block
        block = _format_thesis_block({
            "target_hhi_min": 75000,
            "target_hhi_max": 200000,
        })
        assert "$75,000" in block
        assert "$200,000" in block

    def test_thesis_age_band_and_density(self):
        from app.services.atlas.pilot import _format_thesis_block
        block = _format_thesis_block({
            "target_age_band": "25-44",
            "target_pop_density_min": 1500,
        })
        assert "25-44" in block
        assert "1,500" in block
