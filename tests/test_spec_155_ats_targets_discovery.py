"""
Tests for SPEC 155 — ATS board discovery over the PE targets universe.

Rule R7 from the 2026-10-05 pilot (300 kept targets, 48 hits hand-checked): a board is linked
to a firm ONLY when verified; everything else is a `candidate` (no firm link) or a rejected pair
in the ledger. Every request goes through app.core.open_web. PG-backed tests need TEST_PG_URL
pointing at a DISPOSABLE database.
"""

import importlib.util
import json
import os
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
PG_URL = os.environ.get("TEST_PG_URL")
pg = pytest.mark.skipif(not PG_URL, reason="TEST_PG_URL not set")
FIXTURE = REPO / "tests" / "fixtures" / "ats_pilot_hits.json"


def _target(name, **kw):
    from app.sources.ats_boards import match

    base = dict(target_id=1, name=name, city="Austin", state="TX")
    base.update(kw)
    return match.Target(**base)


def _posting(title="Engineer", location="Austin, TX", text="", locations_all=None, url=None):
    return {"title": title, "location": location, "locations_all": locations_all or [location],
            "description_text": text, "source_url": url or "https://job-boards.greenhouse.io/x/jobs/1"}


# ---------------------------------------------------------------------------
# variants
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestVariants:
    def test_variants_greenhouse_order(self):
        """T1"""
        from app.sources.ats_boards import match

        t = _target("Rockfish Data, Inc.", aliases=["Rockfish Data Inc", "Rockfish Analytics LLC"],
                    previous_names=["Old Fish Co", "Third Name LLC"])
        v = match.variants(t, "greenhouse")
        kinds = [k for _t, k in v]
        toks = [t_ for t_, _k in v]
        assert toks[:3] == ["rockfishdata", "rockfish", "rockfishdatainc"]
        assert kinds[:3] == ["joined", "short", "inc"]
        assert kinds.count("alias") == 2                       # capped at 2
        assert "rockfishanalytics" in toks and "oldfish" in toks
        assert "first_word" not in kinds                      # "rockfish" is already the short form
        assert len(toks) == len(set(toks))                     # deduped (first word == short here)

        t2 = _target("Sigma Computing")
        v2 = dict((k, t_) for t_, k in match.variants(t2, "greenhouse"))
        assert v2["joined"] == "sigmacomputing" and v2["hyphen"] == "sigma-computing"
        assert v2["inc"] == "sigmacomputinginc" and v2["first_word"] == "sigma"

    def test_variants_lever_subset(self):
        """T2"""
        from app.sources.ats_boards import match

        t = _target("Sigma Computing", previous_names=["Sigma Labs"])
        kinds = [k for _t, k in match.variants(t, "lever")]
        assert "inc" not in kinds and "first_word" not in kinds
        assert kinds == ["joined", "hyphen", "alias"]

    def test_variants_drop_none_and_funds(self):
        """T3"""
        from app.sources.ats_boards import match

        t = _target("Netomi", previous_names=["None", "AI Fund II LP", "Netomi Series A SPV LLC", "Msg.ai Inc"])
        toks = [t_ for t_, _k in match.variants(t, "lever")]
        assert "none" not in toks
        assert not any("fund" in x or "spv" in x for x in toks)
        assert "msgai" in toks
        assert all(len(x) >= 3 for x in toks)
        assert match.variants(_target("AB"), "lever") == []        # too short to be a token


# ---------------------------------------------------------------------------
# names, location
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestNamesAndLocation:
    def test_canon_names(self):
        """T4"""
        from app.sources.ats_boards import match

        assert match.canon("The Tomorrow Companies Inc.") == "tomorrow"
        assert match.canon("Rockfish Data, Inc.") == "rockfish"
        assert match.canon("AI Liminal Labs") == "liminal"
        assert match.canon("Data") == "data"                     # never to empty
        names, canon = match.firm_names(_target("Shield AI, Inc.", previous_names=["None"]))
        assert "shield ai" in names and "shield" in canon and "none" not in names

    def test_location_rules(self):
        """T5"""
        from app.sources.ats_boards import match

        tx = _target("X", city="Austin", state="TX")
        assert match.location_hits(tx, [_posting(location="Austin, Texas")])["any"]
        assert match.location_hits(tx, [_posting(location="Dallas, TX")])["state_share"] == 1.0
        assert not match.location_hits(tx, [_posting(location="Austinburg, OH")])["any"]
        dc = _target("X", city="Washington", state="DC")
        assert not match.location_hits(dc, [_posting(location="Seattle, Washington")])["any"]
        assert match.location_hits(dc, [_posting(location="Washington, DC")])["any"]
        wa = _target("X", city="Seattle", state="WA")
        assert not match.location_hits(wa, [_posting(location="Washington, DC")])["any"]
        va = _target("X", city="Reston", state="VA")
        assert not match.location_hits(va, [_posting(location="Charleston, West Virginia")])["any"]
        assert match.location_hits(va, [_posting(location="Remote", locations_all=["Remote", "Richmond, Virginia"])])["any"]
        assert match.location_hits(_target("X", city=None, state=None), [_posting()])["any"] is False


# ---------------------------------------------------------------------------
# verdicts
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestVerdict:
    def test_verify_full_name_board(self):
        """T6"""
        from app.sources.ats_boards import match

        # SPEC_156 (R8): a one-word name verifies on the board name alone only when it is rare in
        # NexData's name corpus (document frequency < 3)
        v = match.verdict("greenhouse", _target("MinIO, Inc.", city="Redwood City", state="CA", name_df={"minio": 1}),
                          {"name": "MinIO"}, [_posting(location="Remote")])
        assert (v.verdict, v.rule) == ("verified", "R8_full_name")
        assert v.evidence["board_name"] == "MinIO" and v.evidence["short_name"] is True

    def test_verify_short_name_needs_location(self):
        """T7"""
        from app.sources.ats_boards import match

        t = _target("IRIS Inc.", city="San Francisco", state="CA")
        far = [_posting(location="Toronto", text="Join Iris today"), _posting(location="London", text="Iris")]
        v = match.verdict("lever", t, None, far)
        assert (v.verdict, v.reason) == ("candidate", "short_name_without_location")
        near = [_posting(location="San Francisco, CA", text="Join Iris today"), _posting(location="London", text="Iris")]
        assert match.verdict("lever", t, None, near).verdict == "verified"

    def test_verify_canonical_needs_location(self):
        """T8"""
        from app.sources.ats_boards import match

        t = _target("The Tomorrow Companies Inc.", city="Boston", state="MA")
        posts = [_posting(location="Remote", text="Tomorrow.io builds weather intelligence")] * 6
        v = match.verdict("greenhouse", t, {"name": "Tomorrow.io"}, posts)
        assert (v.verdict, v.reason) == ("candidate", "name_without_location")
        posts.append(_posting(location="Boston, MA", text="Tomorrow.io"))
        v = match.verdict("greenhouse", t, {"name": "Tomorrow.io"}, posts)
        assert (v.verdict, v.rule) == ("verified", "R8_canonical_location")

    def test_canonical_state_only_needs_plain_drop(self):
        """T8b (live run 2026-10-05: Luminate Health -> Luminate, the LA entertainment-data firm): a
        canonical match that DROPPED a descriptive word ("health") needs the firm's city; a state hit
        alone is enough only when the canonical name dropped nothing but the / com / io."""
        from app.sources.ats_boards import match

        lum = _target("Luminate Health Inc.", city="San Mateo", state="CA")
        posts = [_posting(location="Los Angeles, CA", text="Luminate is the entertainment data partner")] * 4
        v = match.verdict("greenhouse", lum, {"name": "Luminate"}, posts)
        assert (v.verdict, v.reason) == ("candidate", "canonical_state_only")
        assert v.evidence["canon_dropped_words"] == ["health"]
        posts2 = posts + [_posting(location="San Mateo, CA", text="Luminate")]
        assert match.verdict("greenhouse", lum, {"name": "Luminate"}, posts2).verdict == "verified"
        sr = _target("SMARTRENT.COM, INC.", city="Scottsdale", state="AZ")
        sposts = [_posting(location="Phoenix, AZ", text="SmartRent is hiring")] * 3
        v = match.verdict("greenhouse", sr, {"name": "SmartRent"}, sposts)
        assert (v.verdict, v.rule) == ("verified", "R8_canonical_location")

    def test_verify_empty_and_mismatch(self):
        """T9"""
        from app.sources.ats_boards import match

        t = _target("Arbital Health, Inc.", city="San Francisco", state="CA")
        assert (match.verdict("greenhouse", t, {"name": "Arbital Health"}, []).reason) == "empty_board"
        assert match.verdict("greenhouse", t, {"name": "Arbital Health"}, []).verdict == "candidate"
        v = match.verdict("greenhouse", t, {"name": "Purple"}, [])
        assert (v.verdict, v.reason) == ("rejected", "name_mismatch")
        v = match.verdict("greenhouse", _target("Step Function Inc", city="Austin", state="TX"),
                          {"name": "Step"}, [_posting(text="Step is a teen bank")])
        assert (v.verdict, v.reason) == ("rejected", "name_mismatch")

    def test_score_and_evidence(self):
        """T10"""
        from app.sources.ats_boards import match

        t = _target("Sigma Computing", persons=["Mike Speiser", "Jane Roe"])
        strong = match.verdict("greenhouse", t, {"name": "Sigma Computing"},
                               [_posting(text="Sigma Computing, founded by Mike Speiser")] * 10)
        weak = match.verdict("lever", _target("Togetherhood Inc", city="Miami", state="FL"), None,
                             [_posting(location="Remote", text="Togetherhood is hiring")])
        assert 0 <= weak.score < strong.score <= 1
        ev = strong.evidence
        for k in ("rule", "board_name", "names", "canon_names", "name_share", "canon_share", "city_share",
                  "state_share", "postings", "short_name", "persons_found"):
            assert k in ev
        assert ev["persons_found"] == 1
        assert "Speiser" not in json.dumps(ev)                 # persons as a count only

    def test_pilot_replay_precision(self):
        """T11: the 48 hand-labelled pilot hits -> 17 verified, 0 false"""
        from app.sources.ats_boards import match

        hits = json.loads(FIXTURE.read_text())["hits"]
        assert len(hits) == 48
        verified = [h for h in hits if match.decide(h)[0] == "verified"]
        assert sum(1 for h in verified if h["label"] == "T") == 17
        assert [h["token"] for h in verified if h["label"] != "T"] == []
        rejected_true = [h["token"] for h in hits if h["label"] == "T" and match.decide(h)[0] == "rejected"]
        assert sorted(rejected_true) == ["prophet", "prove"]   # missed by every rule in the pilot


# ---------------------------------------------------------------------------
# careers domain
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestCareersDomain:
    def test_careers_domain(self):
        """T12"""
        from app.sources.ats_boards import match

        hover = _target("Hover Inc")
        posts = [_posting(url=f"https://hover.to/careers?gh_jid={i}") for i in range(3)] + [_posting()]
        d = match.careers_domain(hover, posts, None)
        assert d["domain"] == "hover.to" and d["basis"] == "posting_url" and d["postings_with_domain"] == 3

        # own-site posting URLs: a label that is the head of the name counts ("delfina" for Delfina Care)
        posts = [_posting(url=f"https://www.delfina.com/careers/{i}") for i in range(2)]
        assert match.careers_domain(_target("Delfina Care Inc."), posts, None)["domain"] == "delfina.com"
        assert match.careers_domain(_target("Delfina Care Inc."), [_posting(text="see delfina.com")], None) is None

        liminal = _target("Liminal AI Inc")
        posts = [_posting(text="Learn more at https://www.liminal.ai/about and cypress.io"),
                 _posting(text="Visit liminal.ai"), _posting(text="See linkedin.com/company/liminal")]
        d = match.careers_domain(liminal, posts, None)
        assert d["domain"] == "liminal.ai" and d["basis"] == "posting_text"

        assert match.careers_domain(_target("Prove Identity"), [_posting(text="We use cypress.io")], None) is None
        assert match.careers_domain(_target("Lever Co"), [_posting(url="https://jobs.lever.co/lever/1")], None) is None


# ---------------------------------------------------------------------------
# the run (mock transport through the real open_web gate)
# ---------------------------------------------------------------------------


class _Clock:
    def __init__(self):
        self.t = 1000.0

    def now(self):
        return self.t

    def sleep(self, s):
        self.t += s


def _resp(status, body="", headers=None):
    import httpx

    if not isinstance(body, str):
        body = json.dumps(body)
    return httpx.Response(status, text=body, headers=headers or {})


ROBOTS = {"boards-api.greenhouse.io/robots.txt": lambda: _resp(200, "User-agent: *\nDisallow: /embed/\n"),
          "api.lever.co/robots.txt": lambda: _resp(200, "User-agent: *\nAllow: /\nCrawl-delay: 1\n")}

GH_ACME_JOB = {"id": 11, "title": "Robotics Engineer", "location": {"name": "Austin, TX"},
               "offices": [{"name": "Austin"}], "departments": [{"name": "Eng"}],
               "content": "Acme Robotics is hiring. Pay $120,000 - $150,000 per year.",
               "absolute_url": "https://acmerobotics.com/careers?gh_jid=11",
               "first_published": "2026-09-01T00:00:00Z", "updated_at": "2026-09-02T00:00:00Z"}


def _fetcher_factory(handler, seen):
    import httpx

    from app.core import open_web

    terms = {h: open_web.Review("allowed", "test", "2026-10-05") for h in ("greenhouse.io", "lever.co")}

    def make(ats):
        clock = _Clock()

        def wrapped(request):
            seen.append(str(request.url))
            assert request.headers["user-agent"].startswith("NexdataResearch/1.0")
            return handler(request)

        return open_web.OpenWebFetcher(terms=terms, transport=httpx.MockTransport(wrapped),
                                       resolve_host=lambda h: ["93.184.216.34"], clock=clock.now,
                                       sleep=clock.sleep)
    return make


def _routes(extra):
    routes = dict(ROBOTS)
    routes.update(extra)

    def handler(request):
        url = str(request.url)
        for key in sorted(routes, key=len, reverse=True):
            if url.split("?")[0].endswith(key) or key in url and key.endswith("robots.txt"):
                r = routes[key]
                return r(request) if callable(r) and r.__code__.co_argcount == 1 else (r() if callable(r) else r)
        return _resp(404, "{}")
    return handler


ACME_ROUTES = {
    "boards-api.greenhouse.io/v1/boards/acmerobotics": lambda: _resp(200, {"name": "Acme Robotics", "content": ""}),
    "boards-api.greenhouse.io/v1/boards/acmerobotics/jobs": lambda: _resp(200, {"jobs": [GH_ACME_JOB]}),
}


def _acme(tid=101, **kw):
    return _target("Acme Robotics, Inc.", target_id=tid, cik="555", core_entity_id=9001, **kw)


@pytest.mark.unit
class TestRun:
    def test_run_gated_dry_run_writes_nothing(self):
        """T13"""
        from app.sources.ats_boards import targets

        seen = []
        rep = targets.run(None, [_acme()], apply=False, fetcher_factory=_fetcher_factory(_routes(ACME_ROUTES), seen))
        assert rep["apply"] is False and rep["run_id"] is None
        v = [h for h in rep["hits"] if h["verdict"] == "verified"]
        assert len(v) == 1 and v[0]["ats"] == "greenhouse" and v[0]["token"] == "acmerobotics"
        assert v[0]["postings"] == 1 and v[0]["pay"] == 1
        assert v[0]["careers_domain"]["domain"] == "acmerobotics.com"
        # GH stopped after the verified board: no further GH variants
        gh = [u for u in seen if "greenhouse" in u and "robots" not in u]
        assert gh == ["https://boards-api.greenhouse.io/v1/boards/acmerobotics",
                      "https://boards-api.greenhouse.io/v1/boards/acmerobotics/jobs?content=true&pay_transparency=true"]
        assert any("api.lever.co/v0/postings/acmerobotics" in u for u in seen)
        assert all("ashbyhq" not in u for u in seen)
        assert rep["status"] == "done"

    def test_run_stops_host_on_429(self):
        """T14"""
        from app.sources.ats_boards import targets

        seen = []
        routes = {"boards-api.greenhouse.io/v1/boards/acmerobotics": lambda: _resp(429, "", {"Retry-After": "120"})}
        rep = targets.run(None, [_acme(), _acme(102)], apply=False,
                          fetcher_factory=_fetcher_factory(_routes(routes), seen))
        assert rep["sites"]["greenhouse"]["stopped"] == "retry_after"
        assert sum(1 for u in seen if "greenhouse.io/v1" in u) == 1           # never asked again
        assert rep["sites"]["lever"]["stopped"] is None
        assert rep["sites"]["lever"]["targets_done"] == 2
        assert rep["status"] == "paused"

    def test_run_stops_after_3_errors_and_retry_pass(self):
        """T15"""
        import httpx

        from app.sources.ats_boards import targets

        calls = {"n": 0}

        def flaky(request):
            calls["n"] += 1
            if calls["n"] == 1:
                raise httpx.ReadTimeout("timed out", request=request)
            return _resp(404, "{}")

        seen = []
        rep = targets.run(None, [_acme()], apply=False, sites=("lever",),
                          fetcher_factory=_fetcher_factory(_routes({"api.lever.co/v0/postings/acmerobotics": flaky}), seen))
        att = [a for a in rep["attempts"] if a["token"] == "acmerobotics"]
        assert [a["outcome"] for a in att] == ["error", "not_found"]           # retried once at the end
        assert rep["sites"]["lever"]["timeouts"] == 1 and rep["sites"]["lever"]["stopped"] is None

        def dead(request):
            if str(request.url).endswith("robots.txt"):
                return ROBOTS["api.lever.co/robots.txt"]()
            raise httpx.ReadTimeout("timed out", request=request)

        seen = []
        many = [_target(f"Firm Number {i}", target_id=200 + i) for i in range(5)]
        rep = targets.run(None, many, apply=False, sites=("lever",), fetcher_factory=_fetcher_factory(dead, seen))
        assert rep["sites"]["lever"]["stopped"] == "consecutive_errors"
        assert sum(1 for u in seen if "/v0/postings/" in u) == 3

    def test_run_budget(self):
        """T16"""
        from app.sources.ats_boards import targets

        seen = []
        many = [_target(f"Firm Number {i}", target_id=300 + i) for i in range(5)]
        rep = targets.run(None, many, apply=False, sites=("greenhouse",), max_requests=4,
                          fetcher_factory=_fetcher_factory(_routes({}), seen))
        assert rep["sites"]["greenhouse"]["stopped"] == "budget_exhausted"
        assert rep["sites"]["greenhouse"]["requests"] <= 4
        assert rep["status"] == "budget_exhausted"

    def test_run_skips_ledger_and_reuses_boards(self):
        """T17"""
        from app.sources.ats_boards import targets

        seen = []
        prior = targets.Prior(final_pairs={(101, "lever", "acmerobotics")},
                              not_found={"greenhouse": {"acmerobotics-inc"}, "lever": {"acme-robotics"}})
        rep = targets.run(None, [_acme(101), _acme(102, city="Boston", state="MA")], apply=False,
                          prior=prior, fetcher_factory=_fetcher_factory(_routes(ACME_ROUTES), seen))
        # board fetched once (meta + jobs) although two firms verified on it
        assert sum(1 for u in seen if u.endswith("/boards/acmerobotics")) == 1
        assert sum(1 for u in seen if "/boards/acmerobotics/jobs" in u) == 1
        # ledger-final pair never asked; the 102 lever pair is
        lever = [u for u in seen if "/v0/postings/acmerobotics" in u]
        assert len(lever) == 1
        cached = [a for a in rep["attempts"] if a["token"] == "acme-robotics"]
        assert cached and all(a["requests"] == 0 and a["outcome"] == "not_found" for a in cached)
        assert all(u.split("?")[0].split("/")[-1] != "acme-robotics" for u in seen)


# ---------------------------------------------------------------------------
# weekly refresh (SPEC_153) fixes
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestRefresh:
    def test_refresh_skips_verify(self):
        """T18"""
        from app.sources.ats_boards import collect

        seen = []
        f = _fetcher_factory(_routes(ACME_ROUTES), seen)("greenhouse")
        co = collect.Company(name="Totally Different Name Corp")
        br = collect.try_board(collect.BoardFetcher(f), co, "greenhouse", "acmerobotics", "refresh",
                               {"ats_board.id": 7})
        assert br.outcome == "fetched" and len(br.postings) == 1


@pytest.mark.unit
class TestFeedAndCli:
    def test_attach_feed_atsboard(self):
        """T21"""
        from app.entities import feeds
        from app.entities.resolve_core import ATTACH_ONLY_SOURCES, family_of

        f = next(x for x in feeds.ATTACH_FEEDS if x.name == "atsboard")
        assert f.attach_only and f.requires == "public.ats_board"
        assert "status = 'active'" in f.sql and "careers_domain" in f.sql and "verification = 'verified'" in f.sql
        assert "atsboard" in ATTACH_ONLY_SOURCES and family_of("atsboard") == "ats_board"
        assert len(ATTACH_ONLY_SOURCES) == 8

    def test_cli_args(self):
        """T22"""
        from app.sources.ats_boards import targets

        a = targets.build_parser().parse_args(["--limit", "200", "--target-ids-file", "x.txt", "--apply",
                                               "--max-requests", "5000", "--resume-run", "3", "--link-workbench"])
        assert (a.limit, a.target_ids_file, a.apply, a.max_requests, a.resume_run, a.link_workbench) == (
            200, "x.txt", True, 5000, 3, True)
        d = targets.build_parser().parse_args([])
        assert d.apply is False and d.limit is None


# ---------------------------------------------------------------------------
# PG
# ---------------------------------------------------------------------------


def _migration(name):
    path = REPO / "alembic" / "versions" / name
    spec = importlib.util.spec_from_file_location(name[:-3], path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _fresh_pg():
    from sqlalchemy import create_engine, text

    eng = create_engine(PG_URL)
    with eng.begin() as c:
        for t in ("ats_discovery_attempt", "ats_discovery_run", "ats_board_fetch", "ats_posting", "ats_board"):
            c.execute(text(f"DROP TABLE IF EXISTS {t} CASCADE"))
        for sql in (_migration("0018_ats_boards.py").UPGRADE_SQL + _migration("0020_ats_targets_discovery.py").UPGRADE_SQL
                    + _migration("0021_ats_link_precision.py").UPGRADE_SQL):      # SPEC_156: ats_board.ein
            c.execute(text(sql))
    return eng


@pg
class TestPg:
    def test_store_refresh_keeps_basis_pg(self):
        """T19"""
        from datetime import datetime

        from sqlalchemy import text
        from sqlalchemy.orm import Session

        from app.sources.ats_boards import adapters, collect

        eng = _fresh_pg()
        co = collect.Company(name="Acme Robotics", core_entity_id=9001, cik="555")
        post = [adapters.normalize("greenhouse", GH_ACME_JOB)]

        def br(basis, ev, when):
            return collect.BoardResult(ats="greenhouse", token="acmerobotics", company=co, basis=basis, evidence=ev,
                                       status="active", outcome="fetched", url="u", http_status=200, fetched_at=when,
                                       postings=post, bytes=1, sha256="s", terms_citation="t")

        with Session(eng) as db:
            r = collect.store(db, br("name_slug", {"rule": "R7_full_name"}, datetime(2026, 10, 1)))
            bid = r["board_id"]
            r2 = collect.store(db, br("refresh", {"ats_board.id": bid}, datetime(2026, 10, 6)))
            db.commit()
            assert r2["board_id"] == bid and r2["seen"] == 1
            row = db.execute(text("SELECT discovery_basis, discovery_evidence, core_entity_id, last_fetched_at "
                                  "FROM ats_board WHERE id = :i"), {"i": bid}).fetchone()
            assert row[0] == "name_slug" and row[1] == {"rule": "R7_full_name"} and row[2] == 9001
            assert row[3] == datetime(2026, 10, 6)

    def test_apply_pg_links_and_ledger(self):
        """T20"""
        from sqlalchemy import text
        from sqlalchemy.orm import Session

        from app.sources.ats_boards import targets

        eng = _fresh_pg()
        with eng.begin() as c:
            c.execute(text("INSERT INTO ats_board (ats_type, board_token, company_name, core_entity_id, cik, "
                           "discovery_basis, status) VALUES ('greenhouse', 'zeta', 'Zeta Old', 1, '1', 'seed', 'active')"))
        routes = dict(ACME_ROUTES)
        routes.update({
            "boards-api.greenhouse.io/v1/boards/beam": lambda: _resp(200, {"name": "BEAM", "content": ""}),
            "boards-api.greenhouse.io/v1/boards/beam/jobs": lambda: _resp(200, {"jobs": [
                dict(GH_ACME_JOB, id=21, content="Beam teaches maths", location={"name": "New York, NY"}, offices=[],
                     absolute_url="https://job-boards.greenhouse.io/beam/jobs/21")]}),
            "boards-api.greenhouse.io/v1/boards/zeta": lambda: _resp(200, {"name": "Zeta", "content": ""}),
            "boards-api.greenhouse.io/v1/boards/zeta/jobs": lambda: _resp(200, {"jobs": [dict(GH_ACME_JOB, id=31, content="Zeta")]}),
            "boards-api.greenhouse.io/v1/boards/stepfunction": lambda: _resp(200, {"name": "Step", "content": ""}),
            "boards-api.greenhouse.io/v1/boards/stepfunction/jobs": lambda: _resp(200, {"jobs": [dict(GH_ACME_JOB, id=41, content="Step bank")]}),
        })
        tg = [_acme(101),
              _target("Beam Health Inc", target_id=102, cik="556", city="Austin", state="TX"),
              _target("Zeta", target_id=103, cik="557", core_entity_id=2),
              _target("Step Function", target_id=104, cik="558")]

        def factory():
            return Session(eng)

        seen = []
        rep = targets.run(factory, tg, apply=True, fetcher_factory=_fetcher_factory(_routes(routes), seen))
        assert rep["run_id"] is not None
        with Session(eng) as db:
            b = {r[0]: r[1:] for r in db.execute(text(
                "SELECT board_token, status, verification, core_entity_id, cik, target_id, careers_domain, "
                "verification_score FROM ats_board"))}
            assert b["acmerobotics"][:5] == ("active", "verified", 9001, "555", 101)
            assert b["acmerobotics"][5] == "acmerobotics.com" and 0 < float(b["acmerobotics"][6]) <= 1
            assert b["beam"][:5] == ("candidate", "candidate", None, None, None)
            assert b["zeta"][:4] == ("active", None, 1, "1")                    # never relinked
            assert "stepfunction" not in b                                       # rejected: ledger only
            att = {(r[0], r[1], r[2]): r[3:] for r in db.execute(text(
                "SELECT target_id, ats_type, board_token, verdict, reason FROM ats_discovery_attempt"))}
            assert att[(102, "greenhouse", "beam")] == ("candidate", "name_without_location") or \
                att[(102, "greenhouse", "beam")][0] == "candidate"
            assert att[(103, "greenhouse", "zeta")] == ("candidate", "board_linked_to_other_firm")
            assert att[(104, "greenhouse", "stepfunction")] == ("rejected", "name_mismatch")
            n_post = db.execute(text("SELECT count(*) FROM ats_posting p JOIN ats_board b ON b.id = p.board_id "
                                     "WHERE b.board_token = 'acmerobotics'")).scalar()
            assert n_post == 1
            assert db.execute(text("SELECT count(*) FROM ats_posting p JOIN ats_board b ON b.id = p.board_id "
                                   "WHERE b.board_token IN ('beam', 'zeta')")).scalar() == 0
            run = db.execute(text("SELECT status, metrics, checkpoint FROM ats_discovery_run WHERE run_id = :r"),
                             {"r": rep["run_id"]}).fetchone()
            assert run[0] == "done"
            assert run[1]["sites"]["greenhouse"]["verdicts"]["verified"] == 1
            assert run[2]["greenhouse"] == 104

        # idempotent re-run: nothing re-requested, nothing duplicated
        seen2 = []
        rep2 = targets.run(factory, tg, apply=True, fetcher_factory=_fetcher_factory(_routes(routes), seen2))
        assert [u for u in seen2 if "robots" not in u] == []
        with Session(eng) as db:
            assert db.execute(text("SELECT count(*) FROM ats_board")).scalar() == 3
            assert db.execute(text("SELECT count(*) FROM ats_posting")).scalar() == 1
            assert rep2["status"] == "done"
            # the workbench table is absent here: linking reports it, never errors
            assert targets.link_workbench(db)["skipped"] == "workbench.targets_universe absent"


@pg
class TestReadjudicatePg:
    def test_readjudicate_demotes_under_new_rule(self):
        """T23: verified links stored under an older rule version are re-decided from their stored
        evidence; a link the current rule no longer verifies becomes an unlinked candidate."""
        from sqlalchemy import text
        from sqlalchemy.orm import Session

        from app.sources.ats_boards import targets

        eng = _fresh_pg()
        old_ev = {"rule": "R7_canonical_location", "rule_version": "R7-2026-10-05", "postings": 4,
                  "board_name_exact": False, "board_name_canon": True, "name_share": 0.0, "canon_share": 1.0,
                  "intro_names_firm": False, "intro_names_canon": False, "city_share": 0.0, "state_share": 0.75,
                  "location": True, "short_name": False, "target_id": 7, "variant": "short"}
        ok_ev = dict(old_ev, city_share=0.5)
        with eng.begin() as c:
            for tok, ev, tid in (("luminate", old_ev, 7), ("pendo", ok_ev, 8)):
                c.execute(text(
                    "INSERT INTO ats_board (ats_type, board_token, company_name, core_entity_id, cik, target_id, "
                    "discovery_basis, discovery_evidence, status, verification, verification_score) VALUES "
                    "('greenhouse', :t, :n, 1, '1', :tid, 'name_slug', CAST(:e AS JSONB), 'active', 'verified', 0.6)"),
                    {"t": tok, "n": "Luminate Health Inc." if tok == "luminate" else "Pendo.io, Inc.",
                     "e": json.dumps(ev), "tid": tid})
                c.execute(text(
                    "INSERT INTO ats_discovery_attempt (target_id, ats_type, board_token, outcome, verdict, reason, "
                    "attempted_at) VALUES (:tid, 'greenhouse', :t, 'fetched', 'verified', 'R7_canonical_location', NOW())"),
                    {"t": tok, "tid": tid})
        with Session(eng) as db:
            dry = targets.readjudicate(db, apply=False)
            assert [d["token"] for d in dry["demoted"]] == ["luminate"]
            assert db.execute(text("SELECT status FROM ats_board WHERE board_token = 'luminate'")).scalar() == "active"
            rep = targets.readjudicate(db, apply=True)
            assert rep["checked"] == 2 and len(rep["demoted"]) == 1
            row = db.execute(text("SELECT status, verification, core_entity_id, cik, target_id, "
                                  "discovery_evidence->'proposed'->0->>'reason' FROM ats_board "
                                  "WHERE board_token = 'luminate'")).fetchone()
            assert tuple(row) == ("candidate", "candidate", None, None, None, "canonical_state_only")
            att = db.execute(text("SELECT verdict, reason FROM ats_discovery_attempt WHERE board_token = 'luminate'")).fetchone()
            assert tuple(att) == ("candidate", "canonical_state_only")
            assert db.execute(text("SELECT status FROM ats_board WHERE board_token = 'pendo'")).scalar() == "active"
