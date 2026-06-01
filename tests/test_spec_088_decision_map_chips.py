"""
Tests for SPEC 088 — Decision Map constraint pill chips + candidate counter.

Backend: _apply_constraints filter, compute_fit_score with constraints,
FitScoreBody schema. Frontend: parse atlas.html for the chip strip,
chip helpers, thesis-derivation, and storage key.
"""
import re
from pathlib import Path
from unittest.mock import patch

import pytest

ATLAS_HTML = Path(__file__).parent.parent / "frontend" / "atlas.html"


@pytest.fixture(scope="module")
def html():
    return ATLAS_HTML.read_text(encoding="utf-8")


# ─── Backend stubs ───────────────────────────────────────────────────────
# We patch build_layer so constraint tests run hermetically without DB.

class _FakeLayerResult:
    def __init__(self, values):
        self.values = values


def _fake_build_layer(_db, layer_id):
    # SPEC_088 — all county-grain so the funnel intersects.
    if layer_id == "demo_acs_median_income":
        return _FakeLayerResult({
            "A": 60000, "B": 80000, "C": 120000, "D": 30000,
        })
    if layer_id == "econ_cbp_establishments_county":
        return _FakeLayerResult({
            "A": 500, "B": 2000, "C": 1200, "D": 50,
        })
    if layer_id == "infra_broadband_subscription":
        return _FakeLayerResult({
            "A": 70, "B": 90, "C": 85, "D": 40,
        })
    if layer_id == "disaster_nri":
        return _FakeLayerResult({
            "A": 20, "B": 30, "C": 65, "D": 90,
        })
    return _FakeLayerResult({})


class TestSpec088Backend:

    def test_apply_constraint_hhi_min(self):
        """T1: only geos with income ≥ threshold survive."""
        from app.services.atlas import fit_score as fs
        with patch.object(fs, "build_layer", side_effect=_fake_build_layer):
            survivors = fs._apply_constraints(
                None, [{"dimension": "hhi_min", "value": 75000}],
                {"A", "B", "C", "D"},
            )
        assert survivors == {"B", "C"}, survivors

    def test_apply_constraint_exclude_high_nri(self):
        """T2: exclude_nri keeps only places with NRI ≤ threshold."""
        from app.services.atlas import fit_score as fs
        with patch.object(fs, "build_layer", side_effect=_fake_build_layer):
            survivors = fs._apply_constraints(
                None, [{"dimension": "exclude_nri", "value": 50}],
                {"A", "B", "C", "D"},
            )
        assert survivors == {"A", "B"}, survivors

    def test_apply_constraints_intersection(self):
        """T3: multiple constraints → AND of all."""
        from app.services.atlas import fit_score as fs
        with patch.object(fs, "build_layer", side_effect=_fake_build_layer):
            survivors = fs._apply_constraints(
                None,
                [{"dimension": "hhi_min", "value": 75000},
                 {"dimension": "establishments_min", "value": 1000}],
                {"A", "B", "C", "D"},
            )
        # Income ≥ 75K → {B,C}. Density ≥ 1000 → {B,C}. AND = {B,C}.
        assert survivors == {"B", "C"}, survivors

    def test_apply_constraints_unknown_dimension_skipped(self):
        """An unrecognized dimension is silently ignored (no narrowing)."""
        from app.services.atlas import fit_score as fs
        with patch.object(fs, "build_layer", side_effect=_fake_build_layer):
            survivors = fs._apply_constraints(
                None, [{"dimension": "made_up", "value": 1}],
                {"A", "B", "C", "D"},
            )
        assert survivors == {"A", "B", "C", "D"}

    def test_compute_fit_score_with_constraints_returns_filtered(self):
        """T4 (revised): constraint chips narrow `top_n` and the
        `filtered_candidates` counter, but `scores` keeps ALL geos so
        the choropleth still shows the full fit landscape. (Earlier
        behaviour dropped non-survivors from scores; users perceived
        that as 'no colors on the map' once a chip filtered out most
        of the country.)"""
        from app.services.atlas import fit_score as fs
        with patch.object(fs, "build_layer", side_effect=_fake_build_layer):
            result = fs.compute_fit_score(
                db=None,
                thesis={"industry_label": "Furniture stores"},
                top_n=5,
                constraints=[{"dimension": "hhi_min", "value": 75000}],
            )
        assert result["total_candidates"] == 4
        assert result["filtered_candidates"] == 2
        # scores dict still contains all 4 geos (choropleth paints fully)
        assert set(result["scores"].keys()) == {"A", "B", "C", "D"}
        # top_n is drawn only from the survivor set
        survivors = {t["geo_id"] for t in result["top_n"]}
        assert survivors <= {"B", "C"}

    def test_compute_fit_score_constraints_none(self):
        """T5: no constraints = no narrowing (back-compat with SPEC_087)."""
        from app.services.atlas import fit_score as fs
        with patch.object(fs, "build_layer", side_effect=_fake_build_layer):
            r = fs.compute_fit_score(db=None, thesis=None, top_n=10)
        assert r["total_candidates"] == r["filtered_candidates"]
        assert r["filtered_candidates"] > 0

    def test_fit_score_body_accepts_constraints(self):
        """T6: POST body schema accepts the constraints list."""
        from app.api.v1.atlas import FitScoreBody
        b = FitScoreBody(constraints=[{"dimension": "hhi_min", "value": 80000}])
        assert b.constraints == [{"dimension": "hhi_min", "value": 80000}]
        b2 = FitScoreBody()  # back-compat: omitted is fine
        assert b2.constraints is None


class TestSpec088Frontend:

    def test_chip_strip_markup(self, html):
        """T7: chip strip markup present."""
        assert 'id="chip-strip"' in html
        assert 'id="chip-list"' in html
        assert 'id="chip-add"' in html
        assert 'id="chip-counter"' in html
        assert 'id="chip-menu"' in html
        # All four presets exposed
        for dim in ("hhi_min", "establishments_min", "broadband_min", "exclude_nri"):
            assert f'data-dimension="{dim}"' in html

    def test_chip_helpers(self, html):
        """T8: chip helpers declared."""
        for fn in ("function loadConstraints", "function saveConstraints",
                   "function renderChips", "function addChip",
                   "function removeChip", "function applyChipsAndRefit",
                   "function deriveChipsFromThesis"):
            assert fn in html, f"missing {fn}"

    def test_constraint_storage_key(self, html):
        """T9: uses atlas_constraints_v1 key."""
        assert "atlas_constraints_v1" in html

    def test_thesis_to_chips_derivation(self, html):
        """T10: deriveChipsFromThesis routes by thesis fields."""
        assert "deriveChipsFromThesis" in html
        # The fn body mentions the three derivation rules
        m = re.search(r"function deriveChipsFromThesis\(t\).*?\n  \}", html, re.DOTALL)
        assert m, "deriveChipsFromThesis body not found"
        body = m.group(0)
        assert "target_hhi_min" in body
        # density auto-derivation deliberately dropped — density is tract-grain
        # and the v1 county-grain proxy is "establishments" (manual chip).
        assert "disaster_nri" in body or "nri" in body.lower()
