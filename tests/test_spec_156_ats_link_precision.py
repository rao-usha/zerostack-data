"""
Tests for SPEC 156 — ATS board links: precision fixes (rule R8), retraction, re-verification,
same-firm seeds, EIN on careers domains, refresh 404 tolerance, pay parser v2.

Cases come from the 2026-10-06 review of discovery run 1 (Verve, Axios, Socket, One Medical,
Orchestra, Metabase Q, inKind -> GoodUnited; Wunderkind / Tenstorrent / JPY pay). PG-backed tests
need TEST_PG_URL pointing at a DISPOSABLE database.
"""

import json
from datetime import datetime

import pytest

from tests.test_spec_155_ats_targets_discovery import (
    FIXTURE, GH_ACME_JOB, PG_URL, _fetcher_factory, _migration, _posting, _resp, _routes, _target)

pg = pytest.mark.skipif(not PG_URL, reason="TEST_PG_URL not set")


# ---------------------------------------------------------------------------
# rule R8 (pure)
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestNames:
    def test_mnorm_keeps_tokens(self):
        """T1"""
        from app.sources.ats_boards import match

        assert match.mnorm("Metabase Q, Inc.") == "metabase q"
        assert match.mnorm("TIFIN AG Inc.") == "tifin ag"
        assert match.mnorm("Foo AG") == "foo"
        assert match.mnorm("IMPACT TECH, INC. DBA IMPACT") == "impact tech"
        assert match.mnorm("The Tomorrow Companies Inc.") == "tomorrow companies"
        assert match.mnorm("Acme Co., Ltd.") == "acme"
        assert match.mnorm("X") == "x" and match.mnorm("") is None
        names, cn = match.firm_names(_target("Metabase Q, Inc."))
        assert names == ["metabase q"] and cn == ["metabase q"]


@pytest.mark.unit
class TestRuleR8:
    def test_ambiguous_one_word_needs_corroboration(self):
        """T2: Verve, Inc. (Cambridge MA) vs the Verve Group board (Germany / China postings)."""
        from app.sources.ats_boards import match

        verve = _target("Verve, Inc.", city="Cambridge", state="MA", name_df={"verve": 7},
                        persons=["Conor Walsh"])
        far = [_posting(location="Berlin, Germany", text="Verve Group is an ad-tech company")] * 5
        v = match.verdict("greenhouse", verve, {"name": "Verve"}, far)
        assert (v.verdict, v.reason) == ("candidate", "ambiguous_name_without_location")
        assert v.evidence["ambiguous_full"] is True and v.evidence["name_df"] == {"verve": 7}
        near = far + [_posting(location="Cambridge, MA", text="Verve")]
        assert match.verdict("greenhouse", verve, {"name": "Verve"}, near).verdict == "verified"
        person = far + [_posting(location="Remote", text="Verve was founded by Conor Walsh")]
        assert match.verdict("greenhouse", verve, {"name": "Verve"}, person).verdict == "verified"
        # a rare one-word name (only the firm itself in NexData's corpus) needs nothing more
        fossa = _target("FOSSA, Inc.", city="San Francisco", state="CA", name_df={"fossa": 1})
        v = match.verdict("greenhouse", fossa, {"name": "FOSSA"}, [_posting(location="Remote", text="FOSSA")])
        assert (v.verdict, v.rule) == ("verified", "R8_full_name")
        # unknown corpus frequency is treated as ambiguous (precision first)
        anon = _target("FOSSA, Inc.", city="San Francisco", state="CA")
        assert match.verdict("greenhouse", anon, {"name": "FOSSA"}, [_posting(location="Remote")]).verdict == "candidate"
        # a multi-word name is never "ambiguous one-word"
        sig = _target("Sigma Computing", city="San Francisco", state="CA")
        assert match.verdict("greenhouse", sig, {"name": "Sigma Computing"},
                             [_posting(location="Remote")]).verdict == "verified"

    def test_previous_name_only_needs_location(self):
        """T3: the inKind board matched GoodUnited only through a Form D previous name."""
        from app.sources.ats_boards import match

        gu = _target("GoodUnited, Inc.", city="Charleston", state="SC", previous_names=["inKind, Inc."],
                     name_df={"goodunited": 1, "inkind": 1})
        posts = [_posting(location="Austin, TX", text="inKind helps restaurants")] * 4
        v = match.verdict("greenhouse", gu, {"name": "inKind"}, posts)
        assert (v.verdict, v.reason) == ("candidate", "previous_name_without_location")
        assert v.evidence["previous_only"] is True
        posts.append(_posting(location="Charleston, SC", text="inKind"))
        assert match.verdict("greenhouse", gu, {"name": "inKind"}, posts).verdict == "verified"
        # a previous name that IS the current name minus a descriptive word ("Prismatic LLC" ->
        # "Prismatic Software Inc.") is the current name, not a former identity
        pr = _target("Prismatic Software Inc.", city="Sioux Falls", state="SD", previous_names=["Prismatic LLC"],
                     name_df={"prismatic": 2})
        v = match.verdict("greenhouse", pr, {"name": "Prismatic"}, [_posting(location="Remote", text="Prismatic")] * 3)
        assert v.verdict == "verified" and v.evidence["previous_only"] is False

    def test_state_code_city_ignored(self):
        """T4: Orchestra Health Technologies, city stored as "Ny"."""
        from app.sources.ats_boards import match

        t = _target("Orchestra Health Technologies, Inc.", city="Ny", state="NY", name_df={"orchestra": 4})
        loc = match.location_hits(t, [_posting(location="Brooklyn, NY")])
        assert loc["city_share"] == 0 and loc["state_share"] == 1
        posts = [_posting(location="New York, NY", text="Orchestra is a communications firm")] * 5
        v = match.verdict("greenhouse", t, {"name": "Orchestra"}, posts)
        assert v.verdict == "candidate" and v.reason == "canonical_state_only"

    def test_ambiguous_canonical(self):
        """T5: AXIOS HQ (Arlington VA) vs the Axios Media board (Arlington among many cities)."""
        from app.sources.ats_boards import match

        ax = _target("AXIOS HQ", city="Arlington", state="VA", name_df={"axios": 4})
        posts = [_posting(location="Arlington, VA", text="Axios is a media company")] + \
            [_posting(location="New York, NY", text="Axios Media")] * 19
        v = match.verdict("greenhouse", ax, {"name": "Axios"}, posts)
        assert (v.verdict, v.reason) == ("candidate", "ambiguous_canonical")
        assert v.evidence["ambiguous_canon"] is True
        # the full name in one posting corroborates
        p2 = posts + [_posting(location="Remote", text="Axios HQ builds internal comms software")]
        assert match.verdict("greenhouse", ax, {"name": "Axios"}, p2).verdict == "verified"
        # or the firm's city in >= 10% of postings
        p3 = [_posting(location="Arlington, VA", text="Axios")] * 3 + posts[1:18]
        assert match.verdict("greenhouse", ax, {"name": "Axios"}, p3).verdict == "verified"
        # a rare canonical name keeps R7b behaviour
        kas = _target("Kaseya Holdings Inc.", city="Miami", state="FL", name_df={"kaseya": 1})
        kp = [_posting(location="Miami, FL", text="Kaseya")] + [_posting(location="Dublin", text="Kaseya")] * 19
        assert match.verdict("greenhouse", kas, {"name": "Kaseya Careers"}, kp).verdict == "verified"

    def test_metabase_q_not_metabase(self):
        """T6: normalisation used to drop the "Q" and merge Metabase Q with Metabase."""
        from app.sources.ats_boards import match

        mq = _target("Metabase Q, Inc.", city="San Francisco", state="CA", name_df={"metabase": 3, "q": 50})
        posts = [_posting(location="Remote", text="Metabase is the BI tool")] * 6
        assert match.verdict("lever", mq, None, posts).verdict != "verified"
        # the TIFIN group board does not verify "TIFIN AG Inc." on the board name alone
        tf = _target("TIFIN AG Inc.", city="Boulder", state="CO", name_df={"tifin": 2, "ag": 900})
        v = match.verdict("greenhouse", tf, {"name": "TIFIN"}, [_posting(location="Remote", text="TIFIN")] * 3)
        assert v.verdict != "verified"

    def test_pilot_replay_still_precise(self):
        """T7"""
        from app.sources.ats_boards import match

        hits = json.loads(FIXTURE.read_text())["hits"]
        verified = [h for h in hits if match.decide(h)[0] == "verified"]
        assert [h["token"] for h in verified if h["label"] != "T"] == []
        assert sum(1 for h in verified if h["label"] == "T") >= 15


@pytest.mark.unit
class TestSameFirmAndFeed:
    def test_same_firm_unkeyed_seed(self):
        """T8: board 18 (cockroachlabs) is an active SPEC_151 seed with no firm keys."""
        from app.sources.ats_boards import targets

        t = _target("COCKROACH LABS, INC", target_id=1473157689, cik=None)
        seed = (18, "active", None, None, None, None, {}, "Cockroach Labs")
        assert targets._same_firm(seed, t) is True
        other = (19, "active", None, None, None, None, {}, "Roach Motel LLC")
        assert targets._same_firm(other, t) is False
        keyed = (20, "active", 5, "77", None, None, {}, "Cockroach Labs")
        assert targets._same_firm(keyed, t) is False
        # the live seed carries only an industrial_companies id (190, "Cockroach Labs"): no firm key, same name
        industrial = (18, "active", None, None, None, 190, {}, "Cockroach Labs")
        assert targets._same_firm(industrial, t) is True

    def test_feed_passes_ein(self):
        """T9"""
        from app.entities import feeds

        f = next(x for x in feeds.ATTACH_FEEDS if x.name == "atsboard")
        assert "ein AS ein" in f.sql and "NULL::TEXT AS ein" not in f.sql


# ---------------------------------------------------------------------------
# pay parser v2 (pure)
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestPay:
    def test_pay_plus_bonus_and_stipend(self):
        """T10: postings 6372, 6451, 8018, 8475, 11584 parsed; Wunderkind 6295 min stays the base."""
        from app.sources.ats_boards import pay

        cases = [
            ("For this role, the estimated base is $175,000 - $210,000 + Bonus. The actual salary may vary.",
             (175000, 210000)),
            ("The role described above offers a base salary of $105,000 to $135,000 + corporate bonus "
             "eligibility. Your offer will be based on", (105000, 135000)),
            ("Employee Assistance Program (EAP) Monthly Stipend The salary for this role is $50,000-$80,000 plus "
             "sales commission, depending on experience.", (50000, 80000)),
            ("The annual compensation for this role is $336,000 -$350,000 (including a bonus), depending on",
             (336000, 350000)),
            ("Salary range: $155,000-185,000 + bonus Elation welcomes individuals", (155000, 185000)),
        ]
        for text, want in cases:
            p = pay.parse_text(text)
            assert p is not None and (p.min, p.max) == want, (text, p)
        wk = pay.parse_text("The base salary range for this role is $72,000-$90,000 + $11,500 Variable. Actual "
                            "compensation may vary.")
        assert (wk.min, wk.max, wk.interval) == (72000, 90000, "year")
        # a table row: "Variable based on ..." is the NEXT column (the stipend), not this amount (posting 11037)
        tb = pay.parse_text("Education level COMPLETED Hourly Rate Housing/Commuter Stipend Bachelors: In Process "
                            "$27.50 Variable based on permanent residence")
        assert tb is not None and (tb.min, tb.interval) == (27.5, "hour")
        assert pay.parse_text("Stipend: a $1,500 home office budget") is None
        assert pay.parse_text("New hire equity: $32,000-$48,000 Annual Refresh: $10,000") is None
        assert pay.PARSER_VERSION == "pay_v2"

    def test_pay_wide_range_and_hourly_and_jpy(self):
        """T11"""
        from app.sources.ats_boards import pay

        assert pay.parse_text("Compensation for all engineers at Tenstorrent ranges from $100k - $500k including "
                              "base and variable compensation.") is None
        h = pay.parse_text("US Pay Range $33.17 — $44.39 USD We're committed to")
        assert (h.min, h.max, h.interval, h.currency) == (33.17, 44.39, "hour", "USD")
        jp = pay.from_structured("greenhouse", {"pay_input_ranges": [
            {"min_cents": 15150000, "max_cents": 16150000, "currency_type": "JPY", "title": "Pay Range"}]})
        assert (jp.min, jp.max, jp.currency, jp.interval) == (15150000, 16150000, "JPY", "year")
        us = pay.from_structured("greenhouse", {"pay_input_ranges": [
            {"min_cents": 15000000, "max_cents": 18000000, "currency_type": "USD"}]})
        assert (us.min, us.max) == (150000, 180000)
        assert pay.from_structured("greenhouse", {"pay_input_ranges": [
            {"min_cents": 500, "max_cents": 900, "currency_type": "JPY"}]}) is None


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_cli_reverify_retract_args():
    """T17"""
    from app.sources.ats_boards import targets

    a = targets.build_parser().parse_args(["--reverify", "--board-ids", "1,2", "--apply"])
    assert a.reverify and a.board_ids == "1,2" and a.apply
    a = targets.build_parser().parse_args(["--retract", "--board-ids", "178,465", "--reason", "review_false_link"])
    assert a.retract and a.reason == "review_false_link" and not a.apply


# ---------------------------------------------------------------------------
# PG
# ---------------------------------------------------------------------------


def _fresh_pg():
    from sqlalchemy import create_engine, text

    eng = create_engine(PG_URL)
    with eng.begin() as c:
        for t in ("ats_discovery_attempt", "ats_discovery_run", "ats_board_fetch", "ats_posting", "ats_board"):
            c.execute(text(f"DROP TABLE IF EXISTS {t} CASCADE"))
        c.execute(text("DROP SCHEMA IF EXISTS workbench CASCADE"))
        for name in ("0018_ats_boards.py", "0020_ats_targets_discovery.py", "0021_ats_link_precision.py"):
            for sql in _migration(name).UPGRADE_SQL:
                c.execute(text(sql))
    return eng


def _board(c, token, tid, ev=None, status="active", verification="verified", dom=None, eid=7, cik="70", ein=None):
    from sqlalchemy import text

    return c.execute(text(
        "INSERT INTO ats_board (ats_type, board_token, company_name, core_entity_id, cik, target_id, ein, "
        "discovery_basis, discovery_evidence, status, verification, verification_score, careers_domain, "
        "careers_domain_evidence, last_outcome) VALUES ('greenhouse', :t, :t, :eid, :cik, :tid, :ein, 'name_slug', "
        "CAST(:e AS JSONB), :s, :v, 0.5, :d, CAST(:de AS JSONB), 'fetched') RETURNING id"),
        {"t": token, "tid": tid, "e": json.dumps(ev or {"rule": "R7_full_name", "rule_version": "R7b"}),
         "s": status, "v": verification, "d": dom, "de": json.dumps({"domain": dom}) if dom else None,
         "eid": eid, "cik": cik, "ein": ein}).scalar()


def _posts(c, bid, n=2, status="open"):
    from sqlalchemy import text

    for i in range(n):
        c.execute(text("INSERT INTO ats_posting (board_id, external_id, title, status, first_seen_at, last_seen_at) "
                       "VALUES (:b, :e, 'Engineer', :s, NOW(), NOW())"), {"b": bid, "e": f"{bid}-{status}-{i}", "s": status})


@pg
class TestPg:
    def test_refresh_single_404_keeps_active_pg(self):
        """T12"""
        from sqlalchemy import text
        from sqlalchemy.orm import Session

        from app.sources.ats_boards import collect

        eng = _fresh_pg()
        with eng.begin() as c:
            bid = _board(c, "acme", 1)
        co = collect.Company(name="Acme")

        def miss(when):
            return collect.BoardResult(ats="greenhouse", token="acme", company=co, basis="refresh",
                                       evidence={"ats_board.id": bid}, status="not_found", outcome="not_found",
                                       url="u", http_status=404, fetched_at=when, postings=None)

        with Session(eng) as db:
            collect.store(db, miss(datetime(2026, 10, 13)))
            db.commit()
            assert tuple(db.execute(text("SELECT status, last_outcome FROM ats_board WHERE id = :i"),
                                    {"i": bid}).fetchone()) == ("active", "not_found")
            collect.store(db, miss(datetime(2026, 10, 20)))
            db.commit()
            assert db.execute(text("SELECT status FROM ats_board WHERE id = :i"), {"i": bid}).scalar() == "not_found"

    def test_retract_pg(self):
        """T13"""
        from sqlalchemy import text
        from sqlalchemy.orm import Session

        from app.sources.ats_boards import targets

        eng = _fresh_pg()
        with eng.begin() as c:
            c.execute(text("CREATE SCHEMA workbench"))
            c.execute(text("CREATE TABLE workbench.targets_universe (target_id BIGINT, ats_board_id INT)"))
            bad = _board(c, "verve", 1808966, dom="verve.com", ein="12-3")
            good = _board(c, "fossa", 1805448)
            cand = _board(c, "luminate", None, status="candidate", verification="candidate", eid=None, cik=None)
            _posts(c, bad, 2)
            _posts(c, bad, 1, status="closed")
            _posts(c, good, 2)
            _posts(c, cand, 4)
            c.execute(text("INSERT INTO workbench.targets_universe VALUES (1808966, :b), (1805448, :g)"),
                      {"b": bad, "g": good})
            c.execute(text("INSERT INTO ats_discovery_attempt (target_id, ats_type, board_token, outcome, verdict, "
                           "reason, board_id, attempted_at) VALUES (1808966, 'greenhouse', 'verve', 'fetched', "
                           "'verified', 'R7_full_name', :b, NOW())"), {"b": bad})
        with Session(eng) as db:
            dry = targets.retract(db, [bad, cand], "review_false_link", apply=False)
            assert dry["apply"] is False and len(dry["boards"]) == 2
            assert db.execute(text("SELECT status FROM ats_board WHERE id = :i"), {"i": bad}).scalar() == "active"
            rep = targets.retract(db, [bad, cand], "review_false_link", apply=True)
            assert rep["postings_retracted"] == 7
            row = db.execute(text(
                "SELECT status, verification, core_entity_id, cik, target_id, ein, careers_domain, last_outcome, "
                "discovery_evidence->'retracted' FROM ats_board WHERE id = :i"), {"i": bad}).fetchone()
            assert row[:8] == ("rejected", "rejected", None, None, None, None, None, "retracted")
            ret = row[8]
            assert ret["reason"] == "review_false_link"
            assert ret["previous"]["target_id"] == 1808966 and ret["previous"]["careers_domain"] == "verve.com"
            assert ret["previous"]["cik"] == "70" and ret["previous"]["ein"] == "12-3"
            st = dict(db.execute(text("SELECT status, count(*) FROM ats_posting WHERE board_id IN (:a, :b) "
                                      "GROUP BY 1"), {"a": bad, "b": cand}).fetchall())
            assert st == {"retracted": 7}
            assert db.execute(text("SELECT count(*) FROM ats_posting WHERE board_id = :g AND status = 'open'"),
                              {"g": good}).scalar() == 2
            att = db.execute(text("SELECT verdict, reason FROM ats_discovery_attempt WHERE board_token = 'verve'")).fetchone()
            assert tuple(att) == ("rejected", "review_false_link")
            wb = dict(db.execute(text("SELECT target_id, ats_board_id FROM workbench.targets_universe")).fetchall())
            assert wb == {1808966: None, 1805448: good}
            # idempotent: a second retraction changes nothing and keeps the first evidence
            rep2 = targets.retract(db, [bad], "again", apply=True)
            assert rep2["postings_retracted"] == 0
            assert db.execute(text("SELECT discovery_evidence->'retracted'->>'reason' FROM ats_board WHERE id = :i"),
                              {"i": bad}).scalar() == "review_false_link"
            # a rejected board is never relinked by a later discovery
            from app.sources.ats_boards import match
            t = _target("Verve, Inc.", target_id=1808966, cik="1808966", name_df={"verve": 1})
            v = match.verdict("greenhouse", t, {"name": "Verve"}, [_posting()])
            out = targets.link_verified(db, "greenhouse", "verve", t, v, {"fetched_at": datetime(2026, 10, 6),
                                                                           "url": "u"}, None, "joined", None)
            assert out[:2] == ("candidate", "board_rejected")

    def test_reverify_pg(self):
        """T14"""
        from sqlalchemy import text
        from sqlalchemy.orm import Session

        from app.sources.ats_boards import targets

        eng = _fresh_pg()
        with eng.begin() as c:
            c.execute(text("CREATE SCHEMA workbench"))
            c.execute(text("CREATE TABLE workbench.targets_universe (target_id BIGINT, ats_board_id INT)"))
            verve = _board(c, "verve", 11)
            acme = _board(c, "acmerobotics", 12)
            gone = _board(c, "gone", 13)
            _board(c, "quiet", 14)            # verified earlier, no openings today: kept, not retracted
            _posts(c, verve, 2)
            c.execute(text("INSERT INTO workbench.targets_universe VALUES (11, :a), (12, :b), (13, :c)"),
                      {"a": verve, "b": acme, "c": gone})
        tg = {11: _target("Verve, Inc.", target_id=11, city="Cambridge", state="MA", name_df={"verve": 7}),
              12: _target("Acme Robotics, Inc.", target_id=12, cik="555"),
              13: _target("Gone Systems Inc", target_id=13),
              14: _target("Quiet Systems Inc", target_id=14)}
        routes = {
            "boards-api.greenhouse.io/v1/boards/verve": lambda: _resp(200, {"name": "Verve", "content": ""}),
            "boards-api.greenhouse.io/v1/boards/verve/jobs": lambda: _resp(200, {"jobs": [
                dict(GH_ACME_JOB, id=5, content="Verve Group ad tech", location={"name": "Berlin, Germany"}, offices=[],
                     absolute_url="https://verve.com/jobs/5")]}),
            "boards-api.greenhouse.io/v1/boards/acmerobotics": lambda: _resp(200, {"name": "Acme Robotics", "content": ""}),
            "boards-api.greenhouse.io/v1/boards/acmerobotics/jobs": lambda: _resp(200, {"jobs": [GH_ACME_JOB]}),
            "boards-api.greenhouse.io/v1/boards/gone": lambda: _resp(503, ""),
            "boards-api.greenhouse.io/v1/boards/quiet": lambda: _resp(200, {"name": "Quiet Systems", "content": ""}),
            "boards-api.greenhouse.io/v1/boards/quiet/jobs": lambda: _resp(200, {"jobs": []}),
        }
        seen = []

        def factory():
            return Session(eng)

        loader = lambda db, ids: [tg[i] for i in ids if i in tg]   # noqa: E731
        dry = targets.reverify(factory, apply=False, fetcher_factory=_fetcher_factory(_routes(routes), seen),
                               target_loader=loader, retry_other_firm=False)
        by = {r["token"]: r for r in dry["boards"]}
        assert by["verve"]["action"] == "retract" and by["verve"]["reason"] == "ambiguous_name_without_location"
        assert by["acmerobotics"]["action"] == "keep"
        assert by["gone"]["action"] == "unchanged_fetch_failed"
        assert by["quiet"]["action"] == "keep_empty"
        with Session(eng) as db:
            assert db.execute(text("SELECT count(*) FROM ats_board WHERE status = 'active'")).scalar() == 4
        rep = targets.reverify(factory, apply=True, fetcher_factory=_fetcher_factory(_routes(routes), []),
                               target_loader=loader, retry_other_firm=False)
        assert rep["counts"]["retract"] == 1 and rep["counts"]["keep"] == 1
        with Session(eng) as db:
            b = {r[0]: r[1:] for r in db.execute(text(
                "SELECT board_token, status, discovery_evidence->>'rule_version', careers_domain, "
                "discovery_evidence->'retracted'->>'reason' FROM ats_board"))}
            assert b["verve"][0] == "rejected" and b["verve"][3] == "R8:ambiguous_name_without_location"
            assert b["acmerobotics"][:3] == ("active", "R8-2026-10-06", "acmerobotics.com")
            assert b["gone"][0] == "active" and b["quiet"][0] == "active"
            assert db.execute(text("SELECT count(*) FROM ats_posting p JOIN ats_board b ON b.id = p.board_id "
                                   "WHERE b.board_token = 'verve' AND p.status = 'retracted'")).scalar() == 2
            assert db.execute(text("SELECT count(*) FROM ats_board_fetch f JOIN ats_board b ON b.id = f.board_id "
                                   "WHERE b.board_token = 'acmerobotics'")).scalar() == 1

    def test_link_stores_ein_and_claims_seed_pg(self):
        """T15"""
        from sqlalchemy import text
        from sqlalchemy.orm import Session

        from app.sources.ats_boards import match, targets

        eng = _fresh_pg()
        with eng.begin() as c:
            c.execute(text("INSERT INTO ats_board (ats_type, board_token, company_name, discovery_basis, status) "
                           "VALUES ('greenhouse', 'cockroachlabs', 'Cockroach Labs', 'seed', 'active')"))
        board = {"fetched_at": datetime(2026, 10, 6), "url": "u", "terms_citation": "t"}
        with Session(eng) as db:
            t = _target("COCKROACH LABS, INC", target_id=1473157689, ein="45-1234567", city="New York", state="NY")
            v = match.verdict("greenhouse", t, {"name": "Cockroach Labs"}, [_posting(location="New York, NY")])
            assert v.verdict == "verified"
            out = targets.link_verified(db, "greenhouse", "cockroachlabs", t, v, board, None, "short", None)
            assert out[:2] == ("verified", v.reason)
            acme = _target("Acme Robotics, Inc.", target_id=12, ein="11-2")
            va = match.verdict("greenhouse", acme, {"name": "Acme Robotics"}, [_posting()])
            targets.link_verified(db, "greenhouse", "acmerobotics", acme, va, board, None, "joined", None)
            db.commit()
            rows = {r[0]: r[1:] for r in db.execute(text(
                "SELECT board_token, target_id, ein, status, verification FROM ats_board"))}
            assert rows["cockroachlabs"] == (1473157689, "45-1234567", "active", "verified")
            assert rows["acmerobotics"] == (12, "11-2", "active", "verified")

    def test_reparse_pay_pg(self):
        """T16"""
        from sqlalchemy import text
        from sqlalchemy.orm import Session

        from app.sources.ats_boards import collect

        eng = _fresh_pg()
        with eng.begin() as c:
            bid = _board(c, "acme", 1)
            ins = text("INSERT INTO ats_posting (board_id, external_id, title, status, first_seen_at, last_seen_at, "
                       "description_text, pay_min, pay_max, pay_currency, pay_interval, pay_source, pay_parser, "
                       "pay_snippet) VALUES (:b, :e, 'x', 'open', NOW(), NOW(), :d, :lo, :hi, :cur, :iv, :src, "
                       "'pay_v1', :snip)")
            c.execute(ins, {"b": bid, "e": "1", "d": "the estimated base is $175,000 - $210,000 + Bonus. More.",
                            "lo": None, "hi": None, "cur": None, "iv": None, "src": None, "snip": None})
            c.execute(ins, {"b": bid, "e": "2", "d": "Compensation for all engineers at X ranges from $100k - $500k "
                            "including base", "lo": 100000, "hi": 500000, "cur": "USD", "iv": "year", "src": "text",
                            "snip": "x"})
            c.execute(ins, {"b": bid, "e": "3", "d": "", "lo": 151500, "hi": 161500, "cur": "JPY", "iv": "year",
                            "src": "structured", "snip": "pay_input_ranges: Pay Range 15150000-16150000 cents JPY"})
        with Session(eng) as db:
            dry = collect.reparse_pay(db, apply=False)
            assert dry["changed"] == 3 and dry["apply"] is False
            assert db.execute(text("SELECT pay_min FROM ats_posting WHERE external_id = '1'")).scalar() is None
            collect.reparse_pay(db, apply=True)
            got = {r[0]: r[1:] for r in db.execute(text(
                "SELECT external_id, pay_min, pay_max, pay_parser FROM ats_posting"))}
            assert got["1"][:2] == (175000, 210000) and got["1"][2] == "pay_v2"
            assert got["2"][:2] == (None, None)
            assert got["3"][:2] == (15150000, 16150000)
            assert collect.reparse_pay(db, apply=False)["changed"] == 0
