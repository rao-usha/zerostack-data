"""
Tests for SPEC 056 — Synthetic Playground Report Template + CTA (PLAN_063 C5+C7 / Step 5).

The template is pure (gather_data reshapes params, render_html is pure) so T1-T7
need no DB. T8 (the reports schema migration) uses the real-Postgres `pg_session`
pattern and skips without DATABASE_URL.
"""
import os

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from app.reports.templates.synthetic_playground import (
    CTA_HEADLINE,
    SyntheticPlaygroundTemplate,
)


def _payload(generator="macro-scenarios", with_rows=True, with_chart=True):
    p = {
        "generator": generator,
        "generator_label": generator.replace("-", " ").title(),
        "request_params": {"n_scenarios": 100, "horizon_months": 24},
        "summary": {"Scenarios": 100, "Horizon": "24 mo", "Series": 6},
        "ref": "abc123",
    }
    if with_rows:
        p["rows"] = [
            {"month": i, "DFF": round(4.0 + i * 0.01, 2), "UNRATE": round(3.8 + i * 0.02, 2)}
            for i in range(1, 26)
        ]
    if with_chart:
        p["chart"] = {
            "labels": [f"M{i}" for i in range(1, 13)],
            "series": [{"label": "DFF", "data": [4.0 + i * 0.05 for i in range(12)]}],
            "y_label": "%",
        }
    return p


@pytest.mark.unit
class TestSpec056Template:
    """Pure template tests — no DB needed."""

    def test_template_registered(self):
        """T1: ReportBuilder.templates contains 'synthetic_playground'."""
        # ReportBuilder.__init__ touches the DB (_ensure_table); inspect the
        # templates registry without constructing it by reading the class source
        # path is brittle — instead construct against a stub db that no-ops.
        from app.reports import builder as builder_mod

        class _StubResult:
            def fetchone(self):
                return None

        class _StubDB:
            def execute(self, *a, **k):
                return _StubResult()

            def commit(self):
                pass

            def rollback(self):
                pass

        rb = builder_mod.ReportBuilder(_StubDB())
        assert "synthetic_playground" in rb.templates
        assert isinstance(
            rb.templates["synthetic_playground"], SyntheticPlaygroundTemplate
        )

    def test_gather_data_shapes_payload(self):
        """T2: gather_data passes through generator/summary/rows/chart, tolerates missing keys."""
        tpl = SyntheticPlaygroundTemplate()
        data = tpl.gather_data(None, _payload())
        assert data["generator"] == "macro-scenarios"
        assert data["summary"]["Scenarios"] == 100
        assert len(data["rows"]) == 25
        assert data["chart"]["y_label"] == "%"
        # missing keys -> safe defaults
        thin = tpl.gather_data(None, {"generator": "consumer-crowd"})
        assert thin["rows"] == [] and thin["summary"] == {} and thin["chart"] is None
        assert thin["generator_label"] == "Consumer Crowd"

    def test_render_html_is_self_contained(self):
        """T3: render_html returns a full self-contained document, no external CSS."""
        tpl = SyntheticPlaygroundTemplate()
        html = tpl.render_html(tpl.gather_data(None, _payload()))
        assert html.startswith("<!DOCTYPE html>")
        assert html.rstrip().endswith("</html>")
        assert "<style>" in html  # inline CSS
        assert "<link" not in html  # no external stylesheet

    def test_render_html_has_cta_anchor(self):
        """T4: output contains the CTA placeholder comment, id='nexdata-cta', headline."""
        tpl = SyntheticPlaygroundTemplate()
        html = tpl.render_html(tpl.gather_data(None, _payload()))
        assert "<!-- CTA_BLOCK_PLACEHOLDER -->" in html
        assert 'id="nexdata-cta"' in html
        assert CTA_HEADLINE in html
        # ref attribution flows into the CTA links
        assert "ref=abc123" in html

    def test_render_html_each_generator(self):
        """T5: renders for private-financials, macro-scenarios, consumer-crowd without raising."""
        tpl = SyntheticPlaygroundTemplate()
        for gen in ("private-financials", "macro-scenarios", "consumer-crowd"):
            html = tpl.render_html(tpl.gather_data(None, _payload(generator=gen)))
            assert html.startswith("<!DOCTYPE html>")
            assert 'id="nexdata-cta"' in html

    def test_render_html_empty_rows_graceful(self):
        """T6: empty rows / missing chart / missing summary still render a valid document."""
        tpl = SyntheticPlaygroundTemplate()
        thin = {"generator": "consumer-crowd"}  # no rows, chart, or summary
        html = tpl.render_html(tpl.gather_data(None, thin))
        assert html.startswith("<!DOCTYPE html>")
        assert html.rstrip().endswith("</html>")
        assert 'id="nexdata-cta"' in html  # CTA always renders

    def test_render_excel_not_implemented(self):
        """T7: render_excel raises NotImplementedError."""
        tpl = SyntheticPlaygroundTemplate()
        with pytest.raises(NotImplementedError):
            tpl.render_excel(tpl.gather_data(None, _payload()))


# ---------------------------------------------------------------------------
# T8 — DB-backed schema migration check
# ---------------------------------------------------------------------------

@pytest.mark.unit
class TestSpec056Schema:
    def test_reports_short_code_column_exists(self):
        """T8: after ReportBuilder(db), the reports table has short_code/is_public/view_count."""
        db_url = os.getenv("DATABASE_URL")
        if not db_url:
            pytest.skip("DATABASE_URL not set — reports schema test runs in-container only")
        try:
            engine = create_engine(db_url)
            engine.connect().close()
        except Exception as exc:  # noqa: BLE001
            pytest.skip(f"Postgres unreachable: {exc}")

        SessionLocal = sessionmaker(bind=engine)
        db = SessionLocal()
        try:
            from app.reports.builder import ReportBuilder

            ReportBuilder(db)  # runs _ensure_table() incl. the new migrations
            cols = {
                r[0]
                for r in db.execute(
                    text("SELECT column_name FROM information_schema.columns "
                         "WHERE table_name = 'reports'")
                ).fetchall()
            }
            assert {"short_code", "is_public", "view_count"}.issubset(cols)
        finally:
            db.close()
            engine.dispose()
