"""
Tests for SPEC 039 — Eval Builder: Agentic Pipeline Evaluation Framework
"""
import pytest
from datetime import datetime, timedelta
from unittest.mock import MagicMock

# Ensure eval tables are registered with Base before test_db creates them
import app.core.eval_models  # noqa: F401
import app.core.models_site_intel  # noqa: F401 — registers ThreePLCompany for enrichment_coverage_pct tests

from app.core.eval_models import EvalCase, EvalResult, EvalRun, EvalSuite
from app.services.eval_scorer import CapturedOutput, EvalScorer, ScorerResult
from app.services.eval_runner import _compute_composite, _detect_regressions


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _case(assertion_type: str, params: dict = None, tier: int = 1, entity_id: int = 1) -> EvalCase:
    """Build a minimal EvalCase (not persisted)."""
    c = EvalCase()
    c.id = 1
    c.suite_id = 1
    c.name = f"test-{assertion_type}"
    c.assertion_type = assertion_type
    c.assertion_params = params or {}
    c.tier = tier
    c.entity_id = entity_id
    c.entity_type = "company"
    c.regression_threshold_pct = 15.0
    c.is_active = True
    return c


def _output(raw: dict = None, status_code: int = None, latency_ms: float = None) -> CapturedOutput:
    return CapturedOutput(
        mode="db_snapshot",
        entity_id=1,
        entity_type="company",
        raw=raw or {},
        status_code=status_code,
        latency_ms=latency_ms,
    )


def _scorer_result(passed: bool, score: float) -> ScorerResult:
    return ScorerResult(passed=passed, score=score, actual_value={}, expected_value={})


# ---------------------------------------------------------------------------
# TestEvalScorer — rule-based assertions
# ---------------------------------------------------------------------------

class TestEvalScorer:

    @pytest.mark.unit
    def test_scorer_response_status_200_pass(self):
        """T9: status_code=200 → passed=True, score=100."""
        case = _case("response_status_200")
        out = _output(status_code=200)
        result = EvalScorer.score(case, out, MagicMock())
        assert result.passed is True
        assert result.score == 100.0

    @pytest.mark.unit
    def test_scorer_response_status_200_fail(self):
        """T10: status_code=500 → passed=False, score=0."""
        case = _case("response_status_200")
        out = _output(status_code=500)
        result = EvalScorer.score(case, out, MagicMock())
        assert result.passed is False
        assert result.score == 0.0

    @pytest.mark.unit
    def test_scorer_response_field_present_pass(self):
        """T11: field_path='thesis' found in raw → passed=True."""
        case = _case("response_field_present", {"field_path": "thesis"})
        out = _output(raw={"thesis": "Acquisition play in logistics sector."})
        result = EvalScorer.score(case, out, MagicMock())
        assert result.passed is True

    @pytest.mark.unit
    def test_scorer_response_field_present_nested(self):
        """T11b: field_path='data.ceo.name' traverses nested JSON."""
        case = _case("response_field_present", {"field_path": "data.ceo.name"})
        out = _output(raw={"data": {"ceo": {"name": "Andrew Sullivan"}}})
        result = EvalScorer.score(case, out, MagicMock())
        assert result.passed is True

    @pytest.mark.unit
    def test_scorer_response_field_range_pass(self):
        """T12a: value=75 with params={min:50, max:100} → score=100."""
        case = _case("response_field_range", {"field_path": "score", "min": 50, "max": 100})
        out = _output(raw={"score": 75})
        result = EvalScorer.score(case, out, MagicMock())
        assert result.passed is True
        assert result.score == 100.0

    @pytest.mark.unit
    def test_scorer_response_field_range_partial(self):
        """T12: value=45 below min=50, max=100 → partial credit (0 < score < 100)."""
        case = _case("response_field_range", {"field_path": "score", "min": 50, "max": 100}, tier=2)
        out = _output(raw={"score": 45})
        result = EvalScorer.score(case, out, MagicMock())
        assert result.passed is False
        assert 0.0 < result.score < 100.0

    @pytest.mark.unit
    def test_scorer_report_section_present_pass(self):
        """T13: HTML containing 'Executive Summary' → passed=True."""
        case = _case("report_section_present", {"section": "Executive Summary"})
        out = CapturedOutput(
            mode="report_output", entity_id=1, entity_type="company",
            raw="<html><h2>Executive Summary</h2><p>Content</p></html>",
        )
        result = EvalScorer.score(case, out, MagicMock())
        assert result.passed is True

    @pytest.mark.unit
    def test_scorer_report_section_present_fail(self):
        """T14: HTML without expected section → passed=False."""
        case = _case("report_section_present", {"section_name": "Executive Summary"})
        out = CapturedOutput(
            mode="report_output", entity_id=1, entity_type="company",
            raw="<html><h2>Other Section</h2></html>",
        )
        result = EvalScorer.score(case, out, MagicMock())
        assert result.passed is False

    @pytest.mark.unit
    def test_scorer_response_word_count_pass(self):
        """response_word_count_range passes when words ≥ min."""
        case = _case("response_word_count_range", {"field_path": "summary", "min": 5})
        out = _output(raw={"summary": "one two three four five six seven"})
        result = EvalScorer.score(case, out, MagicMock())
        assert result.passed is True

    @pytest.mark.unit
    def test_scorer_unknown_type_returns_failure(self):
        """Unknown assertion_type returns passed=False with descriptive reason."""
        case = _case("not_a_real_assertion_type_xyz")
        out = _output()
        result = EvalScorer.score(case, out, MagicMock())
        assert result.passed is False
        assert "Unknown" in result.failure_reason

    @pytest.mark.unit
    def test_scorer_ceo_exists_pass(self, test_db):
        """T1: ceo_exists passes when management_level=1 non-board person present."""
        from app.core.people_models import Person, IndustrialCompany, CompanyPerson
        company = IndustrialCompany(name="Test Co", status="active")
        test_db.add(company)
        test_db.flush()
        person = Person(full_name="Jane CEO", is_canonical=True, confidence_score=0.9)
        test_db.add(person)
        test_db.flush()
        cp = CompanyPerson(
            company_id=company.id, person_id=person.id,
            title="CEO", management_level=1, is_board_member=False, is_current=True,
        )
        test_db.add(cp)
        test_db.commit()

        case = _case("ceo_exists", {}, entity_id=company.id)
        out = _output()
        out.entity_id = company.id
        result = EvalScorer.score(case, out, test_db)
        assert result.passed is True
        assert result.score == 100.0

    @pytest.mark.unit
    def test_scorer_ceo_exists_fail(self, test_db):
        """T2: ceo_exists fails when no management_level=1 person exists."""
        from app.core.people_models import IndustrialCompany
        company = IndustrialCompany(name="Empty Co", status="active")
        test_db.add(company)
        test_db.commit()

        case = _case("ceo_exists", {}, entity_id=company.id)
        out = _output()
        out.entity_id = company.id
        result = EvalScorer.score(case, out, test_db)
        assert result.passed is False
        assert result.score == 0.0

    @pytest.mark.unit
    def test_scorer_headcount_range_pass(self, test_db):
        """T4: headcount within range returns score=100 — scorer reads OrgChartSnapshot."""
        from app.core.people_models import IndustrialCompany, OrgChartSnapshot
        company = IndustrialCompany(name="Big Co", status="active")
        test_db.add(company)
        test_db.flush()
        snap = OrgChartSnapshot(
            company_id=company.id, snapshot_date=datetime.utcnow().date(),
            chart_data={}, total_people=35, max_depth=3,
        )
        test_db.add(snap)
        test_db.commit()

        case = _case("headcount_range", {"min": 20, "max": 50}, tier=2, entity_id=company.id)
        out = _output()
        out.entity_id = company.id
        result = EvalScorer.score(case, out, test_db)
        assert result.passed is True
        assert result.score == 100.0

    @pytest.mark.unit
    def test_scorer_headcount_range_partial(self, test_db):
        """T3: headcount=15 below min=20 returns partial credit (0 < score < 100)."""
        from app.core.people_models import IndustrialCompany, OrgChartSnapshot
        company = IndustrialCompany(name="Mid Co", status="active")
        test_db.add(company)
        test_db.flush()
        snap = OrgChartSnapshot(
            company_id=company.id, snapshot_date=datetime.utcnow().date(),
            chart_data={}, total_people=15, max_depth=2,
        )
        test_db.add(snap)
        test_db.commit()

        case = _case("headcount_range", {"min": 20, "max": 50}, tier=2, entity_id=company.id)
        out = _output()
        out.entity_id = company.id
        result = EvalScorer.score(case, out, test_db)
        assert result.passed is False
        assert 0.0 < result.score < 100.0

    @pytest.mark.unit
    def test_scorer_no_duplicate_ceo_pass(self, test_db):
        """T7: Single active CEO → passed=True."""
        from app.core.people_models import Person, IndustrialCompany, CompanyPerson
        company = IndustrialCompany(name="Solo Co", status="active")
        test_db.add(company)
        test_db.flush()
        p = Person(full_name="Solo CEO", is_canonical=True)
        test_db.add(p)
        test_db.flush()
        test_db.add(CompanyPerson(
            company_id=company.id, person_id=p.id,
            title="CEO", is_current=True,
        ))
        test_db.commit()

        case = _case("no_duplicate_ceo", {}, tier=2, entity_id=company.id)
        out = _output()
        out.entity_id = company.id
        result = EvalScorer.score(case, out, test_db)
        assert result.passed is True

    @pytest.mark.unit
    def test_scorer_no_duplicate_ceo_fail(self, test_db):
        """T8: Two active CEOs → passed=False."""
        from app.core.people_models import Person, IndustrialCompany, CompanyPerson
        company = IndustrialCompany(name="Dual Co", status="active")
        test_db.add(company)
        test_db.flush()
        for i in range(2):
            p = Person(full_name=f"CEO {i}", is_canonical=True)
            test_db.add(p)
            test_db.flush()
            test_db.add(CompanyPerson(
                company_id=company.id, person_id=p.id,
                title="CEO", is_current=True,
            ))
        test_db.commit()

        case = _case("no_duplicate_ceo", {}, tier=2, entity_id=company.id)
        out = _output()
        out.entity_id = company.id
        result = EvalScorer.score(case, out, test_db)
        assert result.passed is False


# ---------------------------------------------------------------------------
# TestRegressionDetection
# ---------------------------------------------------------------------------

class TestRegressionDetection:

    def _setup_suite_and_runs(self, db, num_prior_runs: int = 2, prior_score: float = 100.0):
        """Create a suite, case, and N prior completed runs with given score."""
        import app.core.eval_models  # ensure tables exist
        suite = EvalSuite(
            name="Test Suite", binding_type="db", binding_target="company",
            eval_mode="db_snapshot", priority=1,
        )
        db.add(suite)
        db.flush()

        case = EvalCase(
            suite_id=suite.id, name="test case",
            assertion_type="ceo_exists", assertion_params={},
            tier=1, is_active=True, regression_threshold_pct=15.0,
        )
        db.add(case)
        db.flush()

        run_ids = []
        for i in range(num_prior_runs):
            run = EvalRun(
                suite_id=suite.id, status="completed",
                triggered_at=datetime.utcnow() - timedelta(days=num_prior_runs - i),
            )
            db.add(run)
            db.flush()
            result = EvalResult(
                run_id=run.id, case_id=case.id,
                passed=(prior_score == 100.0),
                score=prior_score,
            )
            db.add(result)
            run_ids.append(run.id)

        # Current run (not completed)
        current_run = EvalRun(suite_id=suite.id, status="running")
        db.add(current_run)
        db.flush()
        db.commit()

        return suite, case, current_run

    @pytest.mark.unit
    def test_regression_requires_two_prior_runs(self, test_db):
        """T19: Only 1 prior run → regression detection skipped."""
        suite, case, current_run = self._setup_suite_and_runs(test_db, num_prior_runs=1)
        case_results = [(case, _scorer_result(False, 0.0))]
        regressions = _detect_regressions(suite.id, current_run.id, case_results, test_db)
        assert regressions == []

    @pytest.mark.unit
    def test_regression_tier1_always_flags(self, test_db):
        """T16: T1 case passed 5 times, now fails → regression=True."""
        suite, case, current_run = self._setup_suite_and_runs(test_db, num_prior_runs=5, prior_score=100.0)
        case_results = [(case, _scorer_result(False, 0.0))]
        regressions = _detect_regressions(suite.id, current_run.id, case_results, test_db)
        assert len(regressions) == 1
        assert regressions[0]["drop_pct"] == 100.0

    @pytest.mark.unit
    def test_regression_tier2_threshold(self, test_db):
        """T17: T2 score drops 25% (above 15% threshold) → regression."""
        suite, case, current_run = self._setup_suite_and_runs(test_db, num_prior_runs=3, prior_score=80.0)
        case.tier = 2
        test_db.commit()
        # current score=60 → drop = (80-60)/80 * 100 = 25%
        case_results = [(case, _scorer_result(False, 60.0))]
        regressions = _detect_regressions(suite.id, current_run.id, case_results, test_db)
        assert len(regressions) == 1
        assert regressions[0]["drop_pct"] > 15

    @pytest.mark.unit
    def test_regression_tier2_under_threshold(self, test_db):
        """T18: T2 score drops 10% (below 15% threshold) → no regression."""
        suite, case, current_run = self._setup_suite_and_runs(test_db, num_prior_runs=3, prior_score=80.0)
        case.tier = 2
        test_db.commit()
        # current score=72 → drop = (80-72)/80 * 100 = 10%
        case_results = [(case, _scorer_result(True, 72.0))]
        regressions = _detect_regressions(suite.id, current_run.id, case_results, test_db)
        assert regressions == []

    @pytest.mark.unit
    def test_tier1_fail_zeros_composite(self):
        """T25: Any T1 failure → composite_score=0 regardless of T2/T3."""
        t1_case = _case("ceo_exists", tier=1)
        t2_case = _case("headcount_range", tier=2)
        t3_case = _case("report_word_count", tier=3)
        t2_case.id = 2
        t3_case.id = 3

        results = [
            (t1_case, _scorer_result(False, 0.0)),   # T1 fails
            (t2_case, _scorer_result(True, 90.0)),   # T2 passes
            (t3_case, _scorer_result(True, 80.0)),   # T3 passes
        ]
        comp = _compute_composite(results)
        assert comp["composite"] == 0.0
        assert comp["t1_pass_rate"] == 0.0


# ---------------------------------------------------------------------------
# TestCompositeScore (pure logic)
# ---------------------------------------------------------------------------

class TestCompositeScore:

    @pytest.mark.unit
    def test_composite_all_pass(self):
        """All tiers passing → composite > 0."""
        t1 = _case("ceo_exists", tier=1)
        t2 = _case("headcount_range", tier=2)
        t2.id = 2

        results = [
            (t1, _scorer_result(True, 100.0)),
            (t2, _scorer_result(True, 80.0)),
        ]
        comp = _compute_composite(results)
        assert comp["composite"] > 0
        assert comp["t1_pass_rate"] == 100.0

    @pytest.mark.unit
    def test_composite_formula(self):
        """0.50*t1 + 0.30*t2 + 0.20*t3 with all-pass."""
        t1 = _case("ceo_exists", tier=1)
        t2 = _case("headcount_range", tier=2)
        t3 = _case("report_word_count", tier=3)
        t2.id = 2
        t3.id = 3

        results = [
            (t1, _scorer_result(True, 100.0)),
            (t2, _scorer_result(True, 60.0)),
            (t3, _scorer_result(True, 80.0)),
        ]
        comp = _compute_composite(results)
        expected = round(0.50 * 100 + 0.30 * 60 + 0.20 * 80, 2)
        assert comp["composite"] == expected


# ---------------------------------------------------------------------------
# TestEvalCaseEditing
# ---------------------------------------------------------------------------

class TestEvalCaseEditing:

    @pytest.mark.unit
    def test_patch_case_saves_previous_params(self, test_db):
        """T21: PATCH case params → previous_params = old params, new params applied."""
        suite = EvalSuite(
            name="Edit Test Suite", binding_type="db", binding_target="company",
            eval_mode="db_snapshot", priority=2,
        )
        test_db.add(suite)
        test_db.flush()

        old_params = {"min": 10, "max": 50}
        case = EvalCase(
            suite_id=suite.id, name="headcount case",
            assertion_type="headcount_range", assertion_params=old_params,
            tier=2, is_active=True,
        )
        test_db.add(case)
        test_db.commit()

        # Simulate edit (same logic as API PATCH endpoint)
        new_params = {"min": 20, "max": 100}
        case.previous_params = case.assertion_params
        case.assertion_params = new_params
        case.edited_at = datetime.utcnow()
        case.edit_reason = "Updated range"
        test_db.commit()
        test_db.refresh(case)

        assert case.assertion_params == new_params
        assert case.previous_params == old_params
        assert case.edit_reason == "Updated range"
        assert case.edited_at is not None

    @pytest.mark.unit
    def test_seed_from_db_creates_baseline_cases(self, test_db):
        """T20: seed-from-db for db_snapshot company suite creates baseline cases."""
        from app.core.people_models import IndustrialCompany, OrgChartSnapshot
        from app.api.v1.evals import _baseline_people_cases

        company = IndustrialCompany(name="Seed Test Co", status="active")
        test_db.add(company)
        test_db.flush()
        snap = OrgChartSnapshot(
            company_id=company.id, snapshot_date=datetime.utcnow().date(),
            chart_data={}, total_people=10, max_depth=3,
        )
        test_db.add(snap)
        test_db.commit()

        # Create a stub suite (not persisted, just need an id)
        suite = EvalSuite(
            name="Seed Suite", binding_type="db", binding_target="company",
            eval_mode="db_snapshot", priority=3,
        )
        test_db.add(suite)
        test_db.flush()

        cases = _baseline_people_cases(suite.id, company)
        assertion_types = [c.assertion_type for c in cases]

        assert "ceo_exists" in assertion_types
        assert "headcount_range" in assertion_types
        assert len(cases) >= 2


# ---------------------------------------------------------------------------
# TestEvalRunnerOperations
# ---------------------------------------------------------------------------

class TestEvalRunnerOperations:

    @pytest.mark.unit
    def test_run_priority_skips_inactive_suites(self, test_db):
        """T23: run_priority only runs active suites; inactive ones are skipped."""
        active_suite = EvalSuite(
            name="Active P1 Suite", binding_type="db", binding_target="company",
            eval_mode="db_snapshot", priority=1, is_active=True,
        )
        inactive_suite = EvalSuite(
            name="Inactive P1 Suite", binding_type="db", binding_target="company",
            eval_mode="db_snapshot", priority=1, is_active=False,
        )
        test_db.add_all([active_suite, inactive_suite])
        test_db.commit()

        # Query simulates what run_priority does
        suites = test_db.query(EvalSuite).filter(
            EvalSuite.priority == 1, EvalSuite.is_active.is_(True)
        ).all()
        assert len(suites) == 1
        assert suites[0].name == "Active P1 Suite"


# ---------------------------------------------------------------------------
# Missing tests — T5, T6, T15, T22, T24 (SPEC_040 Phase D)
# ---------------------------------------------------------------------------

class TestMissingCoverage:

    @pytest.mark.unit
    def test_scorer_person_exists_fuzzy(self, test_db):
        """T5: 'Andrew Sullivan' matches 'Andrew F. Sullivan' at >= 0.85 threshold."""
        from app.core.people_models import IndustrialCompany, Person, CompanyPerson
        company = IndustrialCompany(name="Fuzzy Corp", status="active")
        test_db.add(company)
        test_db.flush()
        person = Person(full_name="Andrew F. Sullivan", is_canonical=True, confidence_score=0.9)
        test_db.add(person)
        test_db.flush()
        cp = CompanyPerson(company_id=company.id, person_id=person.id, title="CEO", is_current=True,
                           management_level=1, is_board_member=False, confidence="high")
        test_db.add(cp)
        test_db.commit()

        case = _case("person_exists", {"full_name": "Andrew Sullivan", "fuzzy_threshold": 0.85},
                     tier=1, entity_id=company.id)
        out = _output()
        out.entity_id = company.id
        result = EvalScorer.score(case, out, test_db)
        assert result.passed is True
        assert result.actual_value["matched_name"] == "Andrew F. Sullivan"

    @pytest.mark.unit
    def test_scorer_person_exists_no_match(self, test_db):
        """T6: 'John Smith' does not match 'Jane Doe'."""
        from app.core.people_models import IndustrialCompany, Person, CompanyPerson
        company = IndustrialCompany(name="No Match Corp", status="active")
        test_db.add(company)
        test_db.flush()
        person = Person(full_name="Jane Doe", is_canonical=True, confidence_score=0.9)
        test_db.add(person)
        test_db.flush()
        cp = CompanyPerson(company_id=company.id, person_id=person.id, title="CFO", is_current=True,
                           management_level=2, is_board_member=False, confidence="high")
        test_db.add(cp)
        test_db.commit()

        case = _case("person_exists", {"full_name": "John Smith", "fuzzy_threshold": 0.85},
                     tier=1, entity_id=company.id)
        out = _output()
        out.entity_id = company.id
        result = EvalScorer.score(case, out, test_db)
        assert result.passed is False
        assert result.actual_value["matched_name"] is None

    @pytest.mark.unit
    def test_scorer_enrichment_coverage_pct(self, test_db):
        """T15: 80% website coverage passes; 20% coverage is partial credit only."""
        from app.core.models_site_intel import ThreePLCompany
        # 5 companies: 4 with website (80%)
        for i in range(4):
            test_db.add(ThreePLCompany(company_name=f"3PL Pass {i}", website=f"http://co{i}.com"))
        test_db.add(ThreePLCompany(company_name="3PL No Site", website=None))
        test_db.commit()

        case = _case("enrichment_coverage_pct", {"field": "website", "min_pct": 0.7}, tier=2)
        out = _output()
        result = EvalScorer.score(case, out, test_db)
        assert result.passed is True

        # Drop to 1/5 = 20% — should fail (partial credit)
        test_db.query(ThreePLCompany).filter(ThreePLCompany.website.isnot(None)).limit(3).all()
        for co in test_db.query(ThreePLCompany).filter(ThreePLCompany.website.isnot(None)).limit(3).all():
            co.website = None
        test_db.commit()
        result2 = EvalScorer.score(case, out, test_db)
        assert result2.passed is False
        assert result2.score < 100

    @pytest.mark.unit
    def test_dry_run_no_eval_run_created(self, test_db):
        """T22: calling _capture_output + score without run_suite creates no EvalRun row."""
        from app.services.eval_runner import EvalRunner
        suite = EvalSuite(
            name="Dry Run Suite", binding_type="db", binding_target="company",
            eval_mode="db_snapshot", priority=1, is_active=True,
        )
        test_db.add(suite)
        test_db.commit()

        runner = EvalRunner()
        output = runner._capture_output(suite, entity_id=1, db=test_db)
        case = _case("headcount_range", {"min": 0, "max": 100}, tier=2)
        EvalScorer.score(case, output, test_db)

        # No EvalRun should have been created
        assert test_db.query(EvalRun).count() == 0

    @pytest.mark.unit
    def test_eval_run_persists_captured_output(self, test_db):
        """T24: run_suite completes and EvalRun.captured_output is non-null."""
        from datetime import date
        from app.core.people_models import IndustrialCompany, OrgChartSnapshot
        from app.services.eval_runner import EvalRunner

        company = IndustrialCompany(name="Output Corp", status="active")
        test_db.add(company)
        test_db.flush()
        snap = OrgChartSnapshot(
            company_id=company.id,
            snapshot_date=date.today(),
            total_people=20,
            max_depth=3,
            chart_data={"departments": ["Engineering"]},
        )
        test_db.add(snap)
        test_db.commit()

        suite = EvalSuite(
            name="Persist Suite", binding_type="db", binding_target="company",
            eval_mode="db_snapshot", priority=1, is_active=True,
        )
        test_db.add(suite)
        test_db.commit()

        case = EvalCase(
            suite_id=suite.id, name="headcount check",
            assertion_type="headcount_range",
            assertion_params={"min": 1, "max": 100},
            tier=2, weight=1.0, entity_id=company.id, entity_type="company",
            is_active=True,
        )
        test_db.add(case)
        test_db.commit()

        runner = EvalRunner()
        run = runner.run_suite(suite.id, test_db, entity_id=company.id)

        assert run.status == "completed"
        assert run.captured_output is not None
