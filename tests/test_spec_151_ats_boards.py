"""
Tests for SPEC 151 — ATS job boards into NexData (gated fetch, pay parser, hiring clock).

Pay is read from structured fields first, else from US pay-transparency text, with the
raw snippet and a confidence; traps (equity %, revenue, budgets, reimbursements, 401(k),
bonuses) never become pay. Every board request goes through app.core.open_web.
PG-backed tests need TEST_PG_URL pointing at a DISPOSABLE database.
"""

import importlib.util
import json
import os
from pathlib import Path
from unittest.mock import MagicMock

import pytest

REPO = Path(__file__).resolve().parents[1]
PG_URL = os.environ.get("TEST_PG_URL")
pg = pytest.mark.skipif(not PG_URL, reason="TEST_PG_URL not set")


def _pay(text):
    from app.sources.ats_boards import pay

    return pay.parse_text(text)


# ---------------------------------------------------------------------------
# pay parser: text
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestPayText:
    @pytest.mark.parametrize(
        "text,lo,hi,interval",
        [
            # Bombas (Greenhouse, 2026-10-02)
            ("The pay range for this position at the start of employment is expected to be between "
             "$60,000 and $70,000/year . However, the base pay offered may vary", 60000, 70000, "year"),
            ("The pay range for this position at the start of employment is expected to be between "
             "$90,000 and 110,000/year . However", 90000, 110000, "year"),
            ("The pay range for this position at the start of employment is expected to be between "
             "$45.00 and $50.00/hour. However", 45, 50, "hour"),
            # Tecovas (Greenhouse, 2026-10-02)
            ("Generous employee discounts! The hourly rate for this position is $22 to $24hr. The actual "
             "compensation will be based on", 22, 24, "hour"),
            ("The hourly rate for this position is $27 - $31. The actual compensation", 27, 31, "hour"),
            ("The hourly rate for this position is $25- $27/hr. The actual", 25, 27, "hour"),
            # common US phrasings
            ("Salary Range: $120,000 - $150,000 USD", 120000, 150000, "year"),
            ("Compensation: $150,000—$180,000 per year + equity and benefits", 150000, 180000, "year"),
            ("The base salary range for this role is $135,000 to $165,000 annually.", 135000, 165000, "year"),
            ("Pay: $18.50 - $21.00 per hour", 18.5, 21, "hour"),
        ],
    )
    def test_text_ranges_real_phrasings(self, text, lo, hi, interval):
        """T1"""
        p = _pay(text)
        assert p is not None, text
        assert (float(p.min), float(p.max), p.interval, p.currency, p.source) == (lo, hi, interval, "USD", "text")
        assert p.snippet and str(int(lo)) in p.snippet.replace(",", "")
        assert p.confidence in ("high", "medium")

    def test_k_suffix_and_unicode_dash(self):
        """T2"""
        p = _pay("Salary: $120K–$150K plus equity")
        assert (p.min, p.max, p.interval) == (120000, 150000, "year")

    def test_single_values_and_starts_at(self):
        """T3"""
        p = _pay("The hourly rate for this position is $28/hr. The actual compensation")
        assert (p.min, p.max, p.interval) == (28, 28, "hour")
        p = _pay("The hourly rate for this position starts at $25. The actual compensation")
        assert (p.min, p.max, p.interval) == (25, None, "hour")
        assert p.confidence == "medium"

    def test_ote_kind(self):
        """T4"""
        p = _pay("OTE: $200,000 - $250,000 (50/50 split)")
        assert (p.min, p.max, p.kind) == (200000, 250000, "ote")
        p = _pay("On-target earnings of $180,000 per year")
        assert (p.min, p.max, p.kind) == (180000, 180000, "ote")
        p = _pay("The base salary for this role is $140,000 - $160,000.")
        assert p.kind == "base"

    @pytest.mark.parametrize(
        "text,cur,interval",
        [
            ("Salary range: CA$100,000 - CA$120,000", "CAD", "year"),
            ("Salary range: C$100,000 - C$120,000 per year", "CAD", "year"),
            ("Salary: £45,000 - £55,000 per annum", "GBP", "year"),
            ("Salary: €60,000 - €70,000 per year", "EUR", "year"),
            ("The salary range is 90,000 - 110,000 USD per year", "USD", "year"),
            ("Base pay: $100,000 - $120,000 CAD", "CAD", "year"),
        ],
    )
    def test_currency_markers(self, text, cur, interval):
        """T5"""
        p = _pay(text)
        assert p is not None, text
        assert (p.currency, p.interval) == (cur, interval)

    @pytest.mark.parametrize(
        "text",
        [
            "We offer a $100 monthly wellbeing reimbursement to all full-time employees.",
            "Experience managing Paid Social budgets of $30mm+ is required.",
            "We have grown to $50M in annual revenue and 300 employees.",
            "We raised a $40 million Series B led by top investors.",
            "Benefits: 401(k) with 4% match, health, dental.",
            "Compensation includes equity: 0.1% - 0.25% of the company.",
            "Plus a $5,000 sign-on bonus for this position.",
            "Enjoy a $1,500 annual learning stipend.",
            "Our platform processes $2B in payments each year.",
            "You will own a P&L of $5,000,000 and a budget of $250,000 per year.",
            "Our customers saved $120,000 on average.",
            "Generous employee discounts and free boots!",
            "Salary: competitive, DOE.",
            "Pay: $3 per hour",                      # implausible
            "Salary: $9,999,999 per year",           # implausible
        ],
    )
    def test_traps_rejected(self, text):
        """T6"""
        assert _pay(text) is None, text

    def test_trap_beside_real_range(self):
        """T7"""
        text = ("We believe a healthy body equals a healthy mind, so we offer a $100 monthly wellbeing "
                "reimbursement to all full-time employees. What you'll bring: managing budgets of $30mm+. "
                "The pay range for this position at the start of employment is expected to be between "
                "$75,000 and $85,000/year . However, the base pay offered may vary.")
        p = _pay(text)
        assert (p.min, p.max, p.interval) == (75000, 85000, "year")
        assert "75,000" in p.snippet and "reimbursement" not in p.snippet

    def test_multi_zone_ranges(self):
        """T8"""
        text = ("The base salary range for this role depends on location. Zone A: $150,000 - $180,000 USD; "
                "Zone B: $135,000 - $162,000 USD; Zone C: $120,000 - $144,000 USD.")
        p = _pay(text)
        assert (p.min, p.max, p.interval) == (120000, 180000, "year")
        assert p.confidence == "medium"

    def test_review_findings_2026_10_02(self):
        """T8b: found in the 30-posting hand check (stored Greenhouse rows)"""
        # 'ranges from' is a range, not a floor: the max must survive
        p = _pay("The reasonably estimated base salary for this role ranges from $300,000 to $350,000, "
                 "plus a competitive equity package.")
        assert (p.min, p.max, p.interval) == (300000, 350000, "year")
        # 'Annually $X - $Y USD' (Amazon's per-city list) is a pay cue
        p = _pay("US, WA, Seattle - Annually $159,300 — $202,400 USD US, CA, San Francisco - Annually "
                 "$166,600 — $212,800 USD")
        assert (p.min, p.max, p.interval) == (159300, 212800, "year")
        # base, variable and OTE ranges side by side: base only, never mixed
        p = _pay("The typical starting salary range for this role is: $113,300 — $179,200 USD The typical "
                 "starting Target Variable range for this role is: $113,200 — $179,100 USD The typical "
                 "starting On-Target Earnings (OTE) range for this role is: $226,500 — $358,300 USD")
        assert (p.min, p.max, p.kind) == (113300, 179200, "unspecified")
        # a trailing ISO code other than the big five is the currency, not USD
        p = _pay("Annual base salary range (excluding equity and bonus): $212,200 — $212,200 SGD Application")
        assert (p.min, p.currency) == (212200, "SGD")
        # a bare '$' next to a country marker is that country's dollar (Affirm: "CAN base pay range")
        p = _pay("CAN base pay range per year: $90,000 - $130,000 Employees new to Affirm")
        assert (p.min, p.currency) == (90000, "CAD")
        p = _pay("Australia base salary range: $150,000 - $180,000 per year")
        assert p.currency == "AUD"
        p = _pay("USA base pay range per year: $90,000 - $130,000. We are hiring in Canada too.")
        assert p.currency == "USD"
        # disqualifier right before the amount (found: the regex had lost its \b and never matched)
        assert _pay("Pay attention: the research grant of $50,000 per year funds travel.") is None
        assert _pay("Salary aside, you manage an annual budget of $400,000.") is None
        # ... but a disqualifier fenced off by a comma or parenthesis is not one
        p = _pay("The base salary range (based on cost of labor in your area) is $120,000 - $140,000 per year.")
        assert (p.min, p.max) == (120000, 140000)
        p = _pay("The base salary range, excluding equity and bonus, is $150,000 - $170,000 per year.")
        assert (p.min, p.max) == (150000, 170000)
        # ... but 'annual' alone in front of a non-pay amount is not
        assert _pay("Our annual budget of $250,000 covers events.") is None
        assert _pay("Annual revenue: $40,000,000") is None

    def test_no_pay(self):
        """T10"""
        assert _pay("Great benefits and a fun team.") is None
        assert _pay("") is None
        assert _pay(None) is None


# ---------------------------------------------------------------------------
# pay parser: structured
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestPayStructured:
    def test_structured_first(self):
        """T9"""
        from app.sources.ats_boards import pay

        gh = {"pay_input_ranges": [{"min_cents": 12000000, "max_cents": 15000000, "currency_type": "USD",
                                    "title": "NYC"}]}
        p = pay.best("greenhouse", gh, "Salary range: $1,000,000 - $1,200,000")
        assert (p.min, p.max, p.currency, p.interval, p.source, p.confidence) == (
            120000, 150000, "USD", "year", "structured", "high")

        lever = {"salaryRange": {"currency": "USD", "interval": "per-hour-wage", "min": 30, "max": 35}}
        p = pay.best("lever", lever, "")
        assert (p.min, p.max, p.interval, p.source) == (30, 35, "hour", "structured")

        ashby = {"compensation": {"summaryComponents": [
            {"compensationType": "EquityPercentage", "interval": "NONE", "minValue": 0.1, "maxValue": 0.25},
            {"compensationType": "Salary", "interval": "1 YEAR", "currencyCode": "USD",
             "minValue": 140000, "maxValue": 170000},
        ]}}
        p = pay.best("ashby", ashby, "")
        assert (p.min, p.max, p.interval, p.currency, p.source) == (140000, 170000, "year", "USD", "structured")

        # equity only: no pay, and the text fallback still runs
        eq = {"compensation": {"summaryComponents": [
            {"compensationType": "EquityPercentage", "interval": "NONE", "minValue": 0.1, "maxValue": 0.25}]}}
        assert pay.best("ashby", eq, "") is None
        p = pay.best("ashby", eq, "Salary: $100,000 - $110,000 per year")
        assert (p.min, p.source) == (100000, "text")

        # empty structured falls back to text
        p = pay.best("greenhouse", {"pay_input_ranges": []}, "Pay: $25 - $30 per hour")
        assert (p.min, p.max, p.source) == (25, 30, "text")


# ---------------------------------------------------------------------------
# adapters
# ---------------------------------------------------------------------------

GH_JOB = {
    "id": 4011,
    "title": "Director, Insights",
    "location": {"name": "New York, New York"},
    "departments": [{"name": "Marketing"}],
    "offices": [{"name": "New York"}, {"name": "Remote - US"}],
    "content": "&lt;p&gt;Apply to jobs@bombas.com or call (212) 555-0199.&lt;/p&gt;&lt;p&gt;The pay range "
               "for this position is between $168,000 and $178,000/year .&lt;/p&gt;",
    "absolute_url": "https://job-boards.greenhouse.io/bombas/jobs/4011",
    "updated_at": "2026-09-30T10:00:00-04:00",
    "first_published": "2026-09-01T09:00:00-04:00",
    "company_name": "Bombas",
    "pay_input_ranges": [],
}

LEVER_JOB = {
    "id": "abc-123",
    "text": "Account Executive",
    "categories": {"location": "Austin, TX", "team": "Sales", "department": "Revenue",
                   "commitment": "Full-time", "allLocations": ["Austin, TX", "Remote"]},
    "workplaceType": "hybrid",
    "descriptionPlain": "Carbon Arc is hiring.",
    "lists": [{"text": "Pay", "content": "<li>OTE $200,000 - $240,000</li>"}],
    "additionalPlain": "",
    "hostedUrl": "https://jobs.lever.co/carbonarc/abc-123",
    "createdAt": 1756900000000,
    "salaryRange": None,
}

ASHBY_JOB = {
    "id": "u-1",
    "title": "Senior Engineer",
    "department": "Engineering",
    "team": "Platform",
    "employmentType": "FullTime",
    "location": "San Francisco",
    "secondaryLocations": [{"location": "Remote"}],
    "workplaceType": "Remote",
    "publishedAt": "2026-09-20T12:00:00.000+00:00",
    "descriptionPlain": "Genius builds software. Contact jane.doe@genius.ai",
    "jobUrl": "https://jobs.ashbyhq.com/geniusai/u-1",
    "compensation": {"summaryComponents": [{"compensationType": "Salary", "interval": "1 YEAR",
                                            "currencyCode": "USD", "minValue": 150000, "maxValue": 190000}]},
}


@pytest.mark.unit
class TestAdapters:
    def test_adapters_normalize(self):
        """T11"""
        from app.sources.ats_boards import adapters

        g = adapters.normalize("greenhouse", GH_JOB)
        assert g["external_id"] == "4011" and g["title"] == "Director, Insights"
        assert g["department"] == "Marketing" and g["location"] == "New York, New York"
        assert g["locations_all"] == ["New York", "Remote - US"]
        assert g["source_url"].endswith("/4011") and g["posted_at"].startswith("2026-09-01")
        assert "<p>" not in g["description_text"] and "&lt;" not in g["description_text"]
        assert (g["pay_min"], g["pay_max"], g["pay_interval"], g["pay_source"]) == (168000, 178000, "year", "text")
        assert g["pay_parser"]

        lv = adapters.normalize("lever", LEVER_JOB)
        assert (lv["external_id"], lv["title"], lv["team"], lv["employment_type"]) == (
            "abc-123", "Account Executive", "Sales", "Full-time")
        assert lv["locations_all"] == ["Austin, TX", "Remote"]
        assert (lv["pay_min"], lv["pay_max"], lv["pay_kind"]) == (200000, 240000, "ote")
        assert lv["posted_at"].startswith("2025-09-03")

        a = adapters.normalize("ashby", ASHBY_JOB)
        assert (a["external_id"], a["department"], a["team"]) == ("u-1", "Engineering", "Platform")
        assert a["locations_all"] == ["San Francisco", "Remote"]
        assert (a["pay_min"], a["pay_max"], a["pay_source"]) == (150000, 190000, "structured")

        assert adapters.board_jobs_url("greenhouse", "bombas") == (
            "https://boards-api.greenhouse.io/v1/boards/bombas/jobs?content=true&pay_transparency=true")
        assert adapters.board_jobs_url("lever", "carbonarc") == "https://api.lever.co/v0/postings/carbonarc?mode=json"
        assert adapters.board_jobs_url("ashby", "geniusai") == (
            "https://api.ashbyhq.com/posting-api/job-board/geniusai?includeCompensation=true")
        assert adapters.board_meta_url("greenhouse", "bombas") == "https://boards-api.greenhouse.io/v1/boards/bombas"
        assert adapters.board_meta_url("lever", "x") is None
        assert adapters.jobs_from_payload("lever", [LEVER_JOB]) == [LEVER_JOB]
        assert adapters.jobs_from_payload("greenhouse", {"jobs": [GH_JOB]}) == [GH_JOB]
        with pytest.raises(ValueError):
            adapters.board_jobs_url("greenhouse", "../../admin")

    def test_redaction(self):
        """T12"""
        from app.sources.ats_boards import adapters

        g = adapters.normalize("greenhouse", GH_JOB)
        assert "jobs@bombas.com" not in g["description_text"]
        assert "555-0199" not in g["description_text"]
        a = adapters.normalize("ashby", ASHBY_JOB)
        assert "jane.doe" not in a["description_text"]
        # pay figures survive redaction
        assert "$168,000" in g["description_text"]


# ---------------------------------------------------------------------------
# discovery
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestDiscovery:
    def test_slug_candidates(self):
        """T13"""
        from app.sources.ats_boards import discover

        assert discover.slug_candidates("Bombas LLC") == ["bombas"]
        assert discover.slug_candidates("TECOVAS, INC.") == ["tecovas"]
        assert discover.slug_candidates("Carbon Arc Corp") == ["carbonarc", "carbon-arc"]
        assert discover.slug_candidates("Rockfish Data, Inc.") == ["rockfishdata", "rockfish"]
        assert discover.slug_candidates("Kimball Midwest", domain="kimballmidwest.com") == [
            "kimballmidwest", "kimball-midwest"]
        assert len(discover.slug_candidates("Talent Source Solutions Corp.")) <= 2
        assert discover.slug_candidates("") == []

    def test_verify_greenhouse_name(self):
        """T14"""
        from app.sources.ats_boards import discover

        ok, ev = discover.verify("greenhouse", ["Bombas LLC"], {"name": "Bombas"}, [])
        assert ok and ev["board_name"] == "Bombas"
        ok, ev = discover.verify("greenhouse", ["Genius Ventures Founding Fund I, LP"], {"name": "Genius"}, [])
        assert not ok
        ok, _ = discover.verify("greenhouse", ["Rockfish Data, Inc."], {"name": "Rockfish"}, [])
        assert not ok

    def test_verify_lever_mentions(self):
        """T15"""
        from app.sources.ats_boards import discover

        jobs = [{"descriptionPlain": "At Carbon Arc we build data markets."},
                {"descriptionPlain": "Carbon Arc is hiring."},
                {"descriptionPlain": "Join us."}]
        ok, ev = discover.verify("lever", ["Carbon Arc Corp"], None, jobs)
        assert ok and ev["mentions"] == 2 and ev["postings"] == 3
        ok, _ = discover.verify("lever", ["Carbon Arc Corp"], None, jobs[2:])
        assert not ok
        ok, _ = discover.verify("lever", ["Carbon Arc Corp"], None, [])
        assert not ok


# ---------------------------------------------------------------------------
# gated fetch + collection
# ---------------------------------------------------------------------------


class _Clock:
    def __init__(self):
        self.t = 1000.0
        self.slept = []

    def now(self):
        return self.t

    def sleep(self, s):
        self.slept.append(round(s, 3))
        self.t += s


def _fetcher(handler, terms=None):
    import httpx

    from app.core import open_web

    clock = _Clock()
    if terms is None:
        terms = {h: open_web.Review("allowed", "test", "2026-10-02")
                 for h in ("greenhouse.io", "lever.co", "ashbyhq.com")}
    seen = []

    def wrapped(request):
        seen.append(str(request.url))
        assert request.headers["user-agent"].startswith("NexdataResearch/1.0")
        return handler(request)

    f = open_web.OpenWebFetcher(terms=terms, transport=httpx.MockTransport(wrapped),
                                resolve_host=lambda h: ["93.184.216.34"], clock=clock.now, sleep=clock.sleep)
    return f, seen


def _resp(status, body="", headers=None):
    import httpx

    if not isinstance(body, str):
        body = json.dumps(body)
    return httpx.Response(status, text=body, headers=headers or {})


def _router(routes):
    def handler(request):
        url = str(request.url)
        for key, resp in routes.items():
            if key in url:
                return resp() if callable(resp) else resp
        return _resp(404, "{}")
    return handler


ROBOTS_OK = {"boards-api.greenhouse.io/robots.txt": _resp(200, "User-agent: *\nDisallow: /embed/\n"),
             "api.lever.co/robots.txt": _resp(200, "User-agent: *\nAllow: /\nCrawl-delay: 1\n"),
             "api.ashbyhq.com/robots.txt": _resp(401, "Unauthorized")}


def _company(name, **kw):
    from app.sources.ats_boards import collect

    return collect.Company(name=name, **kw)


@pytest.mark.unit
class TestGatedCollection:
    def test_unreviewed_host_zero_requests(self):
        """T16"""
        from app.sources.ats_boards import collect

        f, seen = _fetcher(_router(ROBOTS_OK), terms={})
        rep = collect.run(None, [_company("Bombas LLC")], apply=False, fetcher=f)
        assert seen == []
        outcomes = {a["outcome"] for a in rep["attempts"]}
        assert outcomes == {"terms_unreviewed"}
        assert rep["boards_fetched"] == 0

    def test_robots_401_refuses_ashby(self):
        """T17"""
        from app.sources.ats_boards import collect

        routes = dict(ROBOTS_OK)
        routes["posting-api/job-board"] = _resp(200, {"jobs": [ASHBY_JOB]})
        f, seen = _fetcher(_router(routes))
        co = _company("GlossGenius, Inc.", aliases=["Genius"],
                      tokens=[("ashby", "geniusai", "seed", {"citation": "probe 2026-09-29"})])
        rep = collect.run(None, [co], apply=False, fetcher=f, slugs=False)
        assert not any("posting-api" in u for u in seen)
        assert [a["outcome"] for a in rep["attempts"]] == ["robots_disallowed"]

    def test_greenhouse_happy_path_dry_run(self):
        """T11b: verified board fetched, postings normalized, nothing written in a dry run"""
        from app.sources.ats_boards import collect

        routes = dict(ROBOTS_OK)
        routes["v1/boards/bombas/jobs"] = _resp(200, {"jobs": [GH_JOB]})
        routes["v1/boards/bombas"] = _resp(200, {"name": "Bombas", "content": ""})
        f, seen = _fetcher(_router(routes))
        db = MagicMock()
        rep = collect.run(db, [_company("Bombas LLC", core_entity_id=136663, cik="1617078")],
                          apply=False, fetcher=f)
        assert rep["boards_fetched"] == 1
        b = rep["boards"][0]
        assert (b["ats"], b["token"], b["status"], b["postings"], b["pay"]) == ("greenhouse", "bombas", "active", 1, 1)
        db.execute.assert_not_called()
        # lever / ashby never asked once greenhouse verified
        assert not any("lever" in u or "ashby" in u for u in seen)

    def test_truncated_or_non_json_refused(self):
        """T18"""
        from app.sources.ats_boards import collect

        routes = dict(ROBOTS_OK)
        routes["v1/boards/bombas/jobs"] = _resp(200, "<html>not json</html>")
        routes["v1/boards/bombas"] = _resp(200, {"name": "Bombas"})
        f, _ = _fetcher(_router(routes))
        rep = collect.run(None, [_company("Bombas LLC")], apply=False, fetcher=f, slugs=True,
                          ats_order=("greenhouse",))
        assert any(a["outcome"] == "not_json" for a in rep["attempts"])
        assert rep["boards_fetched"] == 0

        routes["v1/boards/bombas/jobs"] = _resp(200, json.dumps({"jobs": [GH_JOB] * 50}))
        f, _ = _fetcher(_router(routes))
        bf = collect.BoardFetcher(f, max_bytes=2000)
        outcome, payload, info = bf.fetch_json(
            "https://boards-api.greenhouse.io/v1/boards/bombas/jobs?content=true&pay_transparency=true")
        assert outcome == "truncated" and payload is None

    def test_retry_after_not_retried(self):
        """T19"""
        from app.sources.ats_boards import collect

        routes = dict(ROBOTS_OK)
        routes["v1/boards/"] = _resp(429, "slow down", {"Retry-After": "120"})
        f, seen = _fetcher(_router(routes))
        rep = collect.run(None, [_company("Bombas LLC"), _company("Tecovas, Inc.")], apply=False,
                          fetcher=f, ats_order=("greenhouse",))
        api_calls = [u for u in seen if "/v1/boards/" in u]
        assert len(api_calls) == 1
        assert [a["outcome"] for a in rep["attempts"]] == ["retry_after", "host_backed_off"]

    def test_discovery_cap(self):
        """T20"""
        from app.sources.ats_boards import collect, discover

        f, seen = _fetcher(_router(ROBOTS_OK))
        with pytest.raises(ValueError):
            collect.run(None, [_company(f"Co {i}") for i in range(discover.MAX_COMPANIES + 1)],
                        apply=False, fetcher=f)
        assert seen == []


# ---------------------------------------------------------------------------
# the old lane: generic retired, all-failed fails
# ---------------------------------------------------------------------------


def _old_db(cached):
    db = MagicMock()

    def execute(stmt, params=None):
        sql = str(stmt)
        r = MagicMock()
        if "FROM industrial_companies WHERE id" in sql:
            r.fetchone.return_value = (78, "Kimball Midwest", "https://www.kimballmidwest.com", None)
        elif "FROM company_ats_config WHERE company_id" in sql:
            r.fetchone.return_value = cached
        else:
            r.fetchone.return_value = None
            r.fetchall.return_value = []
        return r

    db.execute.side_effect = execute
    return db


@pytest.mark.unit
class TestOldLane:
    def test_generic_never_fetched(self):
        """T21"""
        import asyncio

        from app.sources.job_postings.collector import JobPostingCollector

        async def go():
            c = JobPostingCollector()
            c._generic.fetch_jobs = MagicMock(side_effect=AssertionError("generic fetched"))
            c._detector.detect = MagicMock(side_effect=AssertionError("detector ran"))
            db = _old_db(("generic", None, "https://www.kimballmidwest.com/careers", None))
            r = await c.collect_company(db, 78)
            await c.close()
            return r, db

        r, db = asyncio.run(go())
        assert r.ats_type == "generic" and r.total_fetched == 0 and r.error is None
        assert r.skipped == "retired"
        sqls = [str(c.args[0]) for c in db.execute.call_args_list]
        assert any("company_ats_config" in s and "INSERT" in s for s in sqls)
        params = [c.args[1] for c in db.execute.call_args_list if len(c.args) > 1 and c.args[1]]
        assert any(p.get("status") == "retired" for p in params)

    def test_all_failed_raises(self, monkeypatch):
        """T22"""
        import asyncio

        from app.sources.ats_boards import collect, ingest as ats_ingest
        from app.sources.job_postings import ingest as jp_ingest
        from app.sources.job_postings.collector import JobPostingCollector

        async def all_failed(self, db, limit=None, skip_recent_hours=24):
            return {"companies_processed": 5, "total_fetched": 0, "total_new": 0, "total_closed": 0,
                    "errors": 5, "skipped": 0}

        monkeypatch.setattr(JobPostingCollector, "collect_all", all_failed)
        with pytest.raises(RuntimeError, match="All 5 companies failed"):
            asyncio.run(jp_ingest.ingest_job_postings_all(MagicMock(), 1))

        monkeypatch.setattr(collect, "run", lambda db, companies, apply, **kw: {
            "boards_fetched": 0, "attempts": [{"outcome": "robots_disallowed"}], "boards": [],
            "companies": len(companies)})
        monkeypatch.setattr(ats_ingest, "_companies", lambda db, **kw: [_company("Bombas LLC")])
        with pytest.raises(RuntimeError, match="no board fetched"):
            asyncio.run(ats_ingest.ingest_ats_boards(MagicMock(), job_id=1, preset="pilot"))


# ---------------------------------------------------------------------------
# PG: merge semantics
# ---------------------------------------------------------------------------


def _migration():
    path = REPO / "alembic" / "versions" / "0018_ats_boards.py"
    spec = importlib.util.spec_from_file_location("m0018", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pg
class TestMergePg:
    def test_merge_pg(self):
        """T23"""
        from datetime import datetime

        from sqlalchemy import create_engine, text
        from sqlalchemy.orm import Session

        from app.sources.ats_boards import adapters, collect

        eng = create_engine(PG_URL)
        with eng.begin() as c:
            for t in ("ats_board_fetch", "ats_posting", "ats_board"):
                c.execute(text(f"DROP TABLE IF EXISTS {t} CASCADE"))
            for sql in _migration().UPGRADE_SQL:
                c.execute(text(sql))

        def job(i, title="Role"):
            j = dict(GH_JOB, id=i, title=title)
            return adapters.normalize("greenhouse", j)

        def result(postings, when, outcome="fetched"):
            return collect.BoardResult(
                ats="greenhouse", token="bombas", company=_company("Bombas LLC", core_entity_id=136663,
                                                                   cik="1617078"),
                basis="name_slug", evidence={"board_name": "Bombas"}, status="active", outcome=outcome,
                url="https://boards-api.greenhouse.io/v1/boards/bombas/jobs", http_status=200,
                fetched_at=when, postings=postings, bytes=10, sha256="x", terms_citation="t")

        t1, t2, t3, t4 = (datetime(2026, 10, d) for d in (1, 2, 3, 4))
        with Session(eng) as db:
            r = collect.store(db, result([job(1), job(2)], t1))
            assert (r["new"], r["seen"], r["closed"], r["reopened"]) == (2, 0, 0, 0)
            r = collect.store(db, result([job(2, "Renamed"), job(3)], t2))
            assert (r["new"], r["seen"], r["closed"], r["reopened"]) == (1, 1, 1, 0)
            r = collect.store(db, result(None, t3, outcome="retry_after"))       # failed fetch
            assert r["closed"] is None and r["new"] is None
            assert db.execute(text("SELECT count(*) FROM ats_posting WHERE closed_at = :t"),
                              {"t": t3}).scalar() == 0
            r = collect.store(db, result([job(1), job(2, "Renamed"), job(3)], t4))
            assert (r["new"], r["seen"], r["closed"], r["reopened"]) == (0, 2, 0, 1)
            db.commit()
            rows = {x[0]: x[1:] for x in db.execute(text(
                "SELECT p.external_id, p.status, p.first_seen_at, p.last_seen_at, p.title, p.pay_min, b.core_entity_id "
                "FROM ats_posting p JOIN ats_board b ON b.id = p.board_id ORDER BY 1"))}
            assert rows["1"][0] == "open" and rows["1"][1] == t1 and rows["1"][2] == t4
            assert rows["2"][3] == "Renamed" and rows["2"][1] == t1
            assert rows["3"][1] == t2 and float(rows["3"][4]) == 168000 and rows["3"][5] == 136663
            fetches = db.execute(text("SELECT outcome, open_count, new_count, closed_count "
                                      "FROM ats_board_fetch ORDER BY fetched_at")).fetchall()
            assert [tuple(x) for x in fetches] == [("fetched", 2, 2, 0), ("fetched", 2, 1, 1),
                                                   ("retry_after", None, None, None), ("fetched", 3, 0, 0)]
