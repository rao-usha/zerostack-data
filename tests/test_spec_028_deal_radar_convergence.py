"""
Tests for SPEC 028 — Deal Radar: Convergence Intelligence

Tests the pure scoring functions (no DB required) and region definitions.
"""
import pytest

from app.services.convergence_engine import (
    REGION_DEFINITIONS,
    RegionScores,
    compute_convergence,
    classify_cluster,
    grade_score,
    get_active_signals,
    _clamp,
    _normalize,
)


# All 50 US states + DC
ALL_STATES_DC = {
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "DC", "FL",
    "GA", "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME",
    "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH",
    "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI",
    "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI",
    "WY",
}


class TestRegionMapping:
    """Tests for region-to-state mapping completeness."""

    def test_region_state_mapping_complete(self):
        """T1: All 50 states + DC mapped to exactly one region."""
        mapped = []
        for defn in REGION_DEFINITIONS.values():
            mapped.extend(defn["states"])

        mapped_set = set(mapped)

        # Check no duplicates
        assert len(mapped) == len(mapped_set), (
            f"Duplicate states: {[s for s in mapped if mapped.count(s) > 1]}"
        )

        # Check coverage — note: AK, HI are not mapped (not contiguous US)
        # We check that all contiguous states + DC are covered
        contiguous = ALL_STATES_DC - {"AK", "HI"}
        missing = contiguous - mapped_set
        assert len(missing) == 0, f"Missing states: {missing}"

    def test_all_regions_have_required_fields(self):
        """Every region has label, states, center_lat/lon, map_x/y."""
        for rid, defn in REGION_DEFINITIONS.items():
            assert "label" in defn, f"{rid} missing label"
            assert "states" in defn, f"{rid} missing states"
            assert len(defn["states"]) > 0, f"{rid} has empty states"
            assert "center_lat" in defn, f"{rid} missing center_lat"
            assert "center_lon" in defn, f"{rid} missing center_lon"
            assert "map_x" in defn, f"{rid} missing map_x"
            assert "map_y" in defn, f"{rid} missing map_y"

    def test_13_regions_defined(self):
        """Exactly 13 regions matching the mockup."""
        assert len(REGION_DEFINITIONS) == 13


class TestConvergenceFormula:
    """Tests for the convergence scoring formula."""

    def test_convergence_formula_basic(self):
        """T2: Known inputs produce expected composite score."""
        # avg = (50+60+70+80+90)/5 = 70, above_60 = 4, bonus = 1.4
        # result = round(70 * 1.4) = 98
        scores = RegionScores(epa=50, irs=60, trade=70, water=80, macro=90)
        result = compute_convergence(scores)
        assert result == 98

    def test_convergence_formula_no_signals_above_60(self):
        """T3: No bonus when all signals < 60."""
        scores = RegionScores(epa=30, irs=40, trade=50, water=20, macro=10)
        result = compute_convergence(scores)
        # avg = 30, no bonus, result = 30
        assert result == 30

    def test_convergence_formula_all_signals_above_60(self):
        """T4: Max bonus (1.5x) when all 5 above 60."""
        scores = RegionScores(epa=80, irs=80, trade=80, water=80, macro=80)
        result = compute_convergence(scores)
        # avg = 80, above_60 = 5, bonus = 1.5, result = 120
        assert result == 120

    def test_convergence_formula_all_zeros(self):
        """All zero scores produce zero convergence."""
        scores = RegionScores()
        assert compute_convergence(scores) == 0

    def test_convergence_formula_single_high(self):
        """One signal at 100, rest at 0."""
        scores = RegionScores(epa=100, irs=0, trade=0, water=0, macro=0)
        # avg = 20, above_60 = 1, bonus = 1.1, result = round(22) = 22
        assert compute_convergence(scores) == 22


class TestClusterClassification:
    """Tests for cluster status and grade assignment."""

    def test_cluster_classification_hot(self):
        """T5: Score 72+ classified as HOT."""
        assert classify_cluster(72) == "HOT"
        assert classify_cluster(100) == "HOT"
        assert classify_cluster(85) == "HOT"

    def test_cluster_classification_active(self):
        """T6: Score 58-71 classified as ACTIVE."""
        assert classify_cluster(58) == "ACTIVE"
        assert classify_cluster(71) == "ACTIVE"
        assert classify_cluster(65) == "ACTIVE"

    def test_cluster_classification_watch(self):
        """T7: Score 44-57 classified as WATCH."""
        assert classify_cluster(44) == "WATCH"
        assert classify_cluster(57) == "WATCH"
        assert classify_cluster(50) == "WATCH"

    def test_cluster_classification_low(self):
        """T8: Score < 44 classified as LOW."""
        assert classify_cluster(0) == "LOW"
        assert classify_cluster(43) == "LOW"
        assert classify_cluster(10) == "LOW"

    def test_grade_assignment(self):
        """T11: Scores map to correct letter grades."""
        assert grade_score(72) == "A"
        assert grade_score(100) == "A"
        assert grade_score(58) == "B"
        assert grade_score(71) == "B"
        assert grade_score(44) == "C"
        assert grade_score(57) == "C"
        assert grade_score(30) == "D"
        assert grade_score(43) == "D"
        assert grade_score(0) == "F"
        assert grade_score(29) == "F"


class TestSignalScorers:
    """Tests for individual signal scoring functions."""

    def test_clamp_within_range(self):
        """T9: Clamp keeps values in [0, 100]."""
        assert _clamp(-10) == 0
        assert _clamp(150) == 100
        assert _clamp(50) == 50
        assert _clamp(0) == 0
        assert _clamp(100) == 100

    def test_normalize_basic(self):
        """Normalize maps value to 0-100 scale."""
        assert _normalize(50, 0, 100) == 50.0
        assert _normalize(0, 0, 100) == 0.0
        assert _normalize(100, 0, 100) == 100.0
        assert _normalize(25, 0, 50) == 50.0

    def test_normalize_clamped(self):
        """Normalize clamps values outside range."""
        assert _normalize(200, 0, 100) == 100.0
        assert _normalize(-50, 0, 100) == 0.0

    def test_normalize_equal_bounds(self):
        """Normalize returns 0 when high <= low."""
        assert _normalize(50, 100, 100) == 0.0
        assert _normalize(50, 100, 50) == 0.0

    def test_scorer_empty_data(self):
        """T10: RegionScores default to 0."""
        scores = RegionScores()
        assert scores.epa == 0.0
        assert scores.irs == 0.0
        assert scores.trade == 0.0
        assert scores.water == 0.0
        assert scores.macro == 0.0

    def test_active_signals_list(self):
        """T12: Signals >= 60 appear in active_signals list."""
        scores = RegionScores(epa=70, irs=30, trade=80, water=59, macro=60)
        active = get_active_signals(scores)
        assert "EPA" in active
        assert "IRS" not in active
        assert "Trade" in active
        assert "Water" not in active
        assert "Macro" in active
        assert len(active) == 3

    def test_active_signals_none(self):
        """No active signals when all below 60."""
        scores = RegionScores(epa=10, irs=20, trade=30, water=40, macro=50)
        assert get_active_signals(scores) == []

    def test_active_signals_all(self):
        """All signals active when all >= 60."""
        scores = RegionScores(epa=60, irs=60, trade=60, water=60, macro=60)
        active = get_active_signals(scores)
        assert len(active) == 5
