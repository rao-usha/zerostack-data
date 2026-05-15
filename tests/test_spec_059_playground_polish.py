"""
SPEC 059 — Synthetic Playground Post-MVP Polish.

Two units under test:
  1. Per-generator normalizers in `app.api.v1.playground` —
     `_normalize_macro`, `_normalize_private_financials`, `_normalize_consumer_crowd`,
     and the `_normalize_dispatch` that picks one (or falls back).
  2. Config-driven CTA URLs in the SyntheticPlaygroundTemplate's `_render_cta_block`.

All tests are pure-Python — no DB, no FastAPI client.
"""

import pytest


# ─────────────────────────────────────────────────────────────────────────────
# Fixture data
# ─────────────────────────────────────────────────────────────────────────────

def _macro_raw():
    """Synthetic macro-generator output with 3 scenarios x 4 months over 2 series."""
    return {
        "status": "ok",
        "n_scenarios": 3,
        "horizon_months": 4,
        "training_history_months": 720,
        "series": ["DFF", "UNRATE"],
        "current_values": {"DFF": 5.3, "UNRATE": 4.1},
        "methodology": "OU random walk",
        "scenarios": [
            {"scenario_id": 0, "paths": {"DFF": [5.2, 5.1, 5.0, 4.9], "UNRATE": [4.1, 4.2, 4.3, 4.4]}},
            {"scenario_id": 1, "paths": {"DFF": [5.3, 5.4, 5.5, 5.6], "UNRATE": [4.0, 4.0, 4.0, 4.0]}},
            {"scenario_id": 2, "paths": {"DFF": [5.4, 5.5, 5.6, 5.7], "UNRATE": [4.2, 4.3, 4.4, 4.5]}},
        ],
        "summary": {
            "DFF":    {"p10_terminal": 4.9, "p50_terminal": 5.6, "p90_terminal": 5.7},
            "UNRATE": {"p10_terminal": 4.0, "p50_terminal": 4.4, "p90_terminal": 4.5},
        },
    }


def _private_financials_raw():
    return {
        "sector": "industrials",
        "peer_count": 42,
        "synthetic_count": 5,
        "methodology": "correlated sampling",
        "ratio_stats": {
            "gross_margin":  {"mean": 0.32, "std": 0.05},
            "ebitda_margin": {"mean": 0.18, "std": 0.04},
            "net_margin":    {"mean": 0.08, "std": 0.03},
        },
        "companies": [
            {"id": 1, "revenue_m": 120.0, "ebitda_margin": 0.20},
            {"id": 2, "revenue_m": 240.0, "ebitda_margin": 0.16},
        ],
    }


def _consumer_crowd_raw():
    return {
        "scenario": {
            "id": 7,
            "category": "pricing",
            "event_type": "price_hike_10pct",
            "description": "10% list-price increase across SKUs.",
        },
        "aggregate": {
            "mean_purchase_intent": 0.412,
            "share_switching":      0.187,
            "n_personas":           50,
        },
        "responses": [
            {"persona_id": 1, "purchase_intent": 0.35, "would_switch": True},
            {"persona_id": 2, "purchase_intent": 0.55, "would_switch": False},
        ],
    }


# ─────────────────────────────────────────────────────────────────────────────
# Normalizer tests
# ─────────────────────────────────────────────────────────────────────────────

class TestSpec059Normalizers:

    # ── T1 ───────────────────────────────────────────────────────────────────
    def test_normalize_macro_builds_median_path_chart(self):
        """T1: chart.series has one entry per FRED series, median computed elementwise."""
        from app.api.v1.playground import _normalize_macro

        out = _normalize_macro(_macro_raw(), {"n_scenarios": 3, "horizon_months": 4})

        chart = out["chart"]
        assert chart is not None
        assert chart["labels"] == ["M1", "M2", "M3", "M4"]

        by_label = {s["label"]: s for s in chart["series"]}
        assert set(by_label) == {"DFF", "UNRATE"}

        # median of [5.2,5.3,5.4]=5.3; [5.1,5.4,5.5]=5.4; [5.0,5.5,5.6]=5.5; [4.9,5.6,5.7]=5.6
        assert by_label["DFF"]["data"] == pytest.approx([5.3, 5.4, 5.5, 5.6])
        # median of [4.1,4.0,4.2]=4.1; [4.2,4.0,4.3]=4.2; [4.3,4.0,4.4]=4.3; [4.4,4.0,4.5]=4.4
        assert by_label["UNRATE"]["data"] == pytest.approx([4.1, 4.2, 4.3, 4.4])

    # ── T2 ───────────────────────────────────────────────────────────────────
    def test_normalize_macro_terminal_percentile_rows(self):
        """T2: rows = one row per series with series/current/p10/p50/p90 columns."""
        from app.api.v1.playground import _normalize_macro

        out = _normalize_macro(_macro_raw(), {})

        rows = out["rows"]
        assert len(rows) == 2
        by_series = {r["series"]: r for r in rows}

        assert by_series["DFF"]["current"] == 5.3
        assert by_series["DFF"]["p10"] == 4.9
        assert by_series["DFF"]["p50"] == 5.6
        assert by_series["DFF"]["p90"] == 5.7

        assert by_series["UNRATE"]["current"] == 4.1
        assert by_series["UNRATE"]["p50"] == 4.4

        # summary headline
        assert out["summary"]["Scenarios"] == 3
        assert out["summary"]["Horizon"] == "4 mo"
        assert out["summary"]["Series"] == 2

    # ── T3 ───────────────────────────────────────────────────────────────────
    def test_normalize_private_financials_summary_and_rows(self):
        """T3: summary has sector + counts + 3 ratio means; rows = companies unchanged."""
        from app.api.v1.playground import _normalize_private_financials

        out = _normalize_private_financials(
            _private_financials_raw(),
            {"sector": "industrials", "n_companies": 5},
        )

        s = out["summary"]
        assert s["Sector"] == "industrials"
        assert s["Peer Count"] == 42
        assert s["Synthetic Companies"] == 5
        assert s["Gross Margin (mean)"] == pytest.approx(0.32)
        assert s["EBITDA Margin (mean)"] == pytest.approx(0.18)
        assert s["Net Margin (mean)"] == pytest.approx(0.08)

        assert out["rows"] == _private_financials_raw()["companies"]
        # chart not meaningful for this generator — None or absent
        assert out.get("chart") in (None, {})

    # ── T4 ───────────────────────────────────────────────────────────────────
    def test_normalize_consumer_crowd_summary_includes_scenario(self):
        """T4: summary carries scenario metadata + ≥1 aggregate scalar; rows = responses."""
        from app.api.v1.playground import _normalize_consumer_crowd

        out = _normalize_consumer_crowd(
            _consumer_crowd_raw(), {"scenario_id": 7, "persona_count": 50}
        )

        s = out["summary"]
        assert s.get("Scenario") == "pricing"
        assert s.get("Event") == "price_hike_10pct"
        assert "10% list-price increase" in s.get("Description", "")
        # at least one of the aggregate scalars folded in (any of the three keys)
        agg_keys = {"Mean Purchase Intent", "Share Switching", "N Personas"}
        assert agg_keys & set(s.keys()), f"summary missed all aggregates: {s.keys()}"

        assert out["rows"] == _consumer_crowd_raw()["responses"]

    # ── T5 ───────────────────────────────────────────────────────────────────
    def test_normalize_unknown_generator_falls_back_to_generic(self):
        """T5: dispatch on an unknown generator falls back to the generic normalizer."""
        from app.api.v1.playground import _normalize_dispatch

        raw = {
            "ok": True,
            "count": 3,
            "label": "demo",
            "items": [{"a": 1}, {"a": 2}, {"a": 3}],
        }
        out = _normalize_dispatch("future-generator-xyz", raw, {"foo": "bar"})

        assert out["generator"] == "future-generator-xyz"
        # generic picks scalars into summary
        assert "Ok" in out["summary"] or "Count" in out["summary"] or "Label" in out["summary"]
        # generic picks the first list-of-dicts as rows
        assert out["rows"] == raw["items"]


# ─────────────────────────────────────────────────────────────────────────────
# CTA-URL config tests
# ─────────────────────────────────────────────────────────────────────────────

class TestSpec059CtaUrls:

    # ── T6 ───────────────────────────────────────────────────────────────────
    def test_cta_urls_use_settings_defaults_when_unset(self):
        """T6: no env override → rendered HTML contains the in-app anchor URLs."""
        from app.core.config import get_settings
        from app.reports.templates.synthetic_playground import (
            SyntheticPlaygroundTemplate,
        )

        # defaults declared in the new config fields
        s = get_settings()
        assert hasattr(s, "playground_cta_platform_url")
        assert hasattr(s, "playground_cta_run_url")

        html = SyntheticPlaygroundTemplate()._render_cta_block(
            "private-financials", "ABC123XY"
        )
        # default platform URL contains the in-app anchor
        assert s.playground_cta_platform_url in html or s.playground_cta_platform_url.split("{")[0] in html
        # CTA block anchor still present
        assert 'id="nexdata-cta"' in html
        # stable placeholder still emitted so Step 6/8 code can locate it
        assert "<!-- CTA_BLOCK_PLACEHOLDER -->" in html

    # ── T7 ───────────────────────────────────────────────────────────────────
    def test_cta_urls_honor_settings_override(self, monkeypatch):
        """T7: monkey-patched settings → rendered HTML contains override URLs verbatim."""
        from app.core import config as cfg_mod
        from app.reports.templates import synthetic_playground as tpl_mod

        class _Stub:
            playground_cta_platform_url = "https://nexdata.com/platform?gen={gen}&ref={ref}"
            playground_cta_run_url = "https://nexdata.com/try?gen={gen}&ref={ref}"

        # Patch get_settings at both the config module AND the template's import site,
        # since the template may import get_settings into its own namespace.
        monkeypatch.setattr(cfg_mod, "get_settings", lambda: _Stub())
        if hasattr(tpl_mod, "get_settings"):
            monkeypatch.setattr(tpl_mod, "get_settings", lambda: _Stub())

        html = tpl_mod.SyntheticPlaygroundTemplate()._render_cta_block(
            "macro-scenarios", "DEF456ZZ"
        )

        # Placeholders substituted with the generator + ref
        assert "https://nexdata.com/platform?gen=macro-scenarios&ref=DEF456ZZ" in html
        assert "https://nexdata.com/try?gen=macro-scenarios&ref=DEF456ZZ" in html
        # Default in-app anchor URL is gone
        assert "/playground.html?from=playground#platform" not in html
