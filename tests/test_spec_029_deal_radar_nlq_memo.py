"""
Tests for SPEC 029 — Deal Radar: NL Query Bar + AI Deal Memo Generator
"""
import pytest

from app.services.deal_radar_nlq import (
    ALLOWED_FIELDS,
    ALLOWED_OPERATORS,
    validate_filter,
    validate_filters,
    NLQFilter,
    DealRadarNLQ,
)


class TestNLQueryValidation:
    """Tests for NLQ filter validation (no DB/LLM required)."""

    def test_nlq_parse_epa_filter(self):
        """T1: Valid EPA filter passes validation."""
        f = validate_filter({"field": "epa_score", "op": ">=", "value": 60})
        assert f is not None
        assert f.field == "epa_score"
        assert f.op == ">="
        assert f.value == 60.0

    def test_nlq_parse_multiple_filters(self):
        """T2: Multiple valid filters all pass."""
        raw = [
            {"field": "epa_score", "op": ">=", "value": 60},
            {"field": "irs_migration_score", "op": ">=", "value": 60},
        ]
        valid = validate_filters(raw)
        assert len(valid) == 2
        assert valid[0].field == "epa_score"
        assert valid[1].field == "irs_migration_score"

    def test_nlq_parse_cluster_status(self):
        """T3: cluster_status filter with string value."""
        f = validate_filter({"field": "cluster_status", "op": "=", "value": "hot"})
        assert f is not None
        assert f.field == "cluster_status"
        assert f.value == "HOT"  # auto-uppercased

    def test_nlq_invalid_field_rejected(self):
        """T4: Unknown field names are rejected."""
        f = validate_filter({"field": "secret_column", "op": ">=", "value": 50})
        assert f is None

    def test_nlq_empty_filters(self):
        """T5: Empty filter list returns empty."""
        valid = validate_filters([])
        assert valid == []

    def test_nlq_valid_operators(self):
        """T6: Only whitelisted operators pass."""
        for op in [">=", "<=", ">", "<", "="]:
            f = validate_filter({"field": "epa_score", "op": op, "value": 50})
            assert f is not None, f"Operator {op} should be valid"

        for bad_op in ["!=", "LIKE", "DROP", "OR 1=1", ";--"]:
            f = validate_filter({"field": "epa_score", "op": bad_op, "value": 50})
            assert f is None, f"Operator {bad_op} should be rejected"

    def test_nlq_invalid_value_type(self):
        """Float field rejects non-numeric values."""
        f = validate_filter({"field": "epa_score", "op": ">=", "value": "abc"})
        assert f is None

    def test_nlq_none_value_rejected(self):
        """None value is rejected."""
        f = validate_filter({"field": "epa_score", "op": ">=", "value": None})
        assert f is None

    def test_nlq_int_field_type(self):
        """Integer field coerces correctly."""
        f = validate_filter({"field": "signal_count", "op": ">=", "value": "3"})
        assert f is not None
        assert f.value == 3

    def test_nlq_keyword_fallback(self):
        """Keyword fallback produces reasonable filters."""
        nlq = DealRadarNLQ.__new__(DealRadarNLQ)  # skip __init__
        result = nlq._keyword_fallback("regions with high EPA violations and trade")
        filters = result["filters"]
        fields = [f["field"] for f in filters]
        assert "epa_score" in fields
        assert "trade_score" in fields

    def test_nlq_keyword_fallback_hot(self):
        """Keyword fallback handles 'hot' keyword."""
        nlq = DealRadarNLQ.__new__(DealRadarNLQ)
        result = nlq._keyword_fallback("show me hot regions")
        filters = result["filters"]
        assert any(f["field"] == "cluster_status" and f["value"] == "HOT" for f in filters)


class TestDealMemoValidation:
    """Tests for memo generation (structural, no DB/LLM required)."""

    def test_memo_sections_defined(self):
        """T7: All 8 sections are defined."""
        from app.services.deal_radar_memo import MEMO_SECTIONS
        assert len(MEMO_SECTIONS) == 8
        assert "executive_summary" in MEMO_SECTIONS
        assert "comparable_deals" in MEMO_SECTIONS
        assert "lp_considerations" in MEMO_SECTIONS
        assert "risk_factors" in MEMO_SECTIONS

    def test_memo_fallback_sections(self):
        """T10: Fallback sections produce all 6 keys."""
        from app.services.deal_radar_memo import DealRadarMemoGenerator
        gen = DealRadarMemoGenerator.__new__(DealRadarMemoGenerator)
        data = {
            "label": "Test Region",
            "states": ["TX"],
            "region": {
                "convergence_score": 75,
                "cluster_status": "HOT",
                "active_signals": ["EPA", "Trade"],
                "epa_score": 80,
                "irs_migration_score": 40,
                "trade_score": 70,
                "water_score": 30,
                "macro_score": 50,
            },
            "epa": {}, "migration": {}, "trade": {},
            "water": {}, "income": {"total_returns": 100000, "avg_agi": 55000},
        }
        sections = gen._fallback_sections(data)
        assert len(sections) == 8
        for key in ["executive_summary", "market_opportunity", "signal_analysis",
                     "comparable_deals", "target_profile", "risk_factors",
                     "recommended_action", "lp_considerations"]:
            assert key in sections
            assert len(sections[key]) > 10

    def test_memo_unknown_region(self):
        """T9: REGION_DEFINITIONS check catches invalid region_id."""
        from app.services.convergence_engine import REGION_DEFINITIONS
        assert "invalid_region" not in REGION_DEFINITIONS
        assert "appalachia" in REGION_DEFINITIONS
