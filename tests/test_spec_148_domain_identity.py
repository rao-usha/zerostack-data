"""
Tests for SPEC 148 — web domains as an identity carrier.

A domain is normalized to its registrable domain (generic hosts dropped), linked
to an entity STRONG only when two independent source families agree, WEAK on
one family, and never merges anything. Redirects become aliases with their
evidence. The probe collector is robots-, terms- and Retry-After-gated.
PG-backed tests need TEST_PG_URL pointing at a DISPOSABLE database.
"""

import importlib.util
import json
import os
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
PG_URL = os.environ.get("TEST_PG_URL")
pg = pytest.mark.skipif(not PG_URL, reason="TEST_PG_URL not set")


def _rec(record_key, **kw):
    from app.entities.resolve_core import _rec as base

    return base(record_key, **kw)


# ---------------------------------------------------------------------------
# normalization
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestNormalize:
    def test_registrable_psl(self):
        """T1"""
        from app.entities import domains as d

        assert d.registrable("www.acme.co.uk") == "acme.co.uk"
        assert d.registrable("shop.acme.com.au") == "acme.com.au"
        assert d.registrable("a.b.acme.com") == "acme.com"
        assert d.registrable("city.kawasaki.jp") == "city.kawasaki.jp"  # exception rule
        assert d.registrable("foo.bar.ck") == "foo.bar.ck"  # wildcard *.ck: bar.ck is a suffix
        assert d.registrable("co.uk") is None  # a bare public suffix
        assert d.registrable("com") is None
        assert d.classify("https://bücher.de/")[0] == "xn--bcher-kva.de"  # IDN -> punycode
        assert d.psl_version() and d.psl_version().startswith("20")

    def test_normalize_strips_and_lowercases(self):
        """T2"""
        from app.entities import domains as d

        for raw in ("HTTP://WWW.Acme.COM/about?x=1", "acme.com", "www.acme.com.", "https://acme.com:8443/",
                    "info@acme.com", "https://user:pw@www.acme.com/", " 'acme.com' ", "ACME.COM/#top"):
            assert d.classify(raw) == ("acme.com", "ok"), raw

    def test_invalid_values(self):
        """T3"""
        from app.entities import domains as d

        assert d.classify(None) == (None, "empty")
        assert d.classify("   ") == (None, "empty")
        for raw in ("n/a", "none", "acme", "192.168.1.10", "http://10.0.0.1/", "acme dental.com",
                    "[::1]", "co.uk", "http://"):
            dom, reason = d.classify(raw)
            assert dom is None and reason == "invalid", (raw, reason)

    def test_generic_hosts_dropped(self):
        """T4"""
        from app.entities import domains as d

        cases = {
            "https://www.linkedin.com/company/acme": "generic:social",
            "facebook.com/acme": "generic:social",
            "https://blackstone.podbean.com/": "generic:social",
            "acme.wixsite.com/home": "generic:builder",
            "https://sites.google.com/view/acme": "generic:builder",
            "boards.greenhouse.io/acme": "generic:ats",
            "acme.myworkdayjobs.com/careers": "generic:ats",
            "jobs.lever.co/acme": "generic:ats",
            "acmeinc@gmail.com": "generic:webmail",
            "www.hugedomains.com/domain_profile.cfm?d=acme": "generic:parking",
            "bit.ly/3abc": "generic:shortener",
            "https://podcasts.apple.com/us/podcast/acme/id1": "generic:social",  # 16 ADV CRDs
        }
        for raw, reason in cases.items():
            assert d.classify(raw) == (None, reason), raw
        # a company whose name merely contains a platform word is NOT generic
        assert d.classify("linkedinsolutions-acme.com") == ("linkedinsolutions-acme.com", "ok")
        assert d.classify("https://www.apple.com/") == ("apple.com", "ok")  # Apple itself is a company


# ---------------------------------------------------------------------------
# feeds
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestFeeds:
    def test_feed_row_stores_registrable_domain(self):
        """T5"""
        from app.entities import feeds

        adv = next(f for f in feeds.FEEDS if f.name == "adv")
        row = dict(zip(feeds.COLUMN_NAMES, feeds._row(adv, {
            "native_id": "8361", "crd": "8361", "legal_name": "Acme Advisers LLC",
            "domain": "HTTPS://WWW.ACME.COM/about", "state": "NY"})))
        assert row["domain"] == "acme.com"
        row = dict(zip(feeds.COLUMN_NAMES, feeds._row(adv, {
            "native_id": "8362", "crd": "8362", "legal_name": "Beta", "domain": "linkedin.com/company/beta"})))
        assert row["domain"] is None  # generic: not stored, the record itself is still fed
        assert row["record_key"] == "adv:8362"
        # attach-only feeds: a row without a kept domain is not fed at all
        pefirm = next(f for f in feeds.ATTACH_FEEDS if f.name == "pefirm")
        assert pefirm.attach_only
        assert feeds._row(pefirm, {"native_id": "7", "legal_name": "X", "domain": "facebook.com/x"}) is None
        kept = dict(zip(feeds.COLUMN_NAMES, feeds._row(pefirm, {
            "native_id": "7", "legal_name": "Bessemer Venture Partners", "cik": "0000000042",
            "domain": "https://www.bvp.com"})))
        assert kept["record_key"] == "pefirm:7" and kept["domain"] == "bvp.com" and kept["cik"] == "42"

    def test_feed_domain_stats(self):
        """T6"""
        from app.entities import feeds

        stats = feeds.domain_stats(["acme.com", None, "", "linkedin.com/x", "acme", "bit.ly/1", "beta.io"])
        assert stats == {"raw": 7, "kept": 2, "empty": 2, "invalid": 1,
                         "generic": 2, "generic:social": 1, "generic:shortener": 1}

    def test_attach_feed_names(self):
        """T6b: the seven attach-only sources and their families"""
        from app.entities import feeds
        from app.entities.resolve_core import ATTACH_ONLY_SOURCES, family_of

        assert {f.name for f in feeds.ATTACH_FEEDS} == set(ATTACH_ONLY_SOURCES) == {
            "pefirm", "industrial", "peportco", "portco", "threepl", "famoffice", "lpfund"}
        assert family_of("adv") == family_of("iapd") == "form_adv"
        assert family_of("edgar") == "edgar"
        assert family_of("pefirm") == "pe_firm_web"
        assert len({family_of(s) for s in ATTACH_ONLY_SOURCES}) == 7


# ---------------------------------------------------------------------------
# the link rules (pure)
# ---------------------------------------------------------------------------


def _resolve(records, probes=None, cap=None):
    from app.entities import resolve_core as rc

    resolvable = [r for r in records if r["source"] not in rc.ATTACH_ONLY_SOURCES]
    p = rc.plan(resolvable, [], {})
    kw = {} if cap is None else {"cap": cap}
    out = rc.domain_links(records, p["record_keys"], p["components"], probes or {}, **kw)
    return p, out


def _row(out, domain, status=None):
    rows = [r for r in out["rows"] if r["domain"] == domain and (status is None or r["status"] == status)]
    return rows


@pytest.mark.unit
class TestLinkRules:
    def _firm(self, domain_adv="acme.com", domain_iapd="acme.com"):
        return [
            _rec("adv:100", crd="100", legal_name="Acme Capital LLC", name_norm="acme capital",
                 domain=domain_adv, state="NY"),
            _rec("iapd:100", crd="100", legal_name="Acme Capital LLC", name_norm="acme capital",
                 domain=domain_iapd, state="NY"),
            _rec("f13:42", cik="42", crd="100", legal_name="ACME CAPITAL", name_norm="acme capital"),
        ]

    def test_two_source_rule_strong(self):
        """T7"""
        recs = self._firm() + [_rec("pefirm:7", cik="0000000042", legal_name="Acme Capital",
                                    name_norm="acme capital", domain="acme.com")]
        p, out = _resolve(recs)
        rows = _row(out, "acme.com")
        assert len(rows) == 1 and rows[0]["status"] == "strong"
        assert rows[0]["families"] == ["form_adv", "pe_firm_web"]
        assert rows[0]["candidate_comp"] == 0
        vias = {c["record_key"]: c["via"] for c in rows[0]["claims"]}
        assert vias == {"adv:100": "member", "iapd:100": "member", "pefirm:7": "key"}
        assert out["entity_strong"] == {0: ["acme.com"]}

    def test_same_family_is_weak(self):
        """T8: ADV and IAPD are one filing -- one source family"""
        p, out = _resolve(self._firm())
        rows = _row(out, "acme.com")
        assert [r["status"] for r in rows] == ["weak"]
        assert rows[0]["families"] == ["form_adv"]
        assert out["entity_strong"] == {}
        assert out["metrics"]["links_by_status"] == {"weak": 1}

    def test_shared_domain_conflict(self):
        """T9: two entities on one domain are both conflicts, never strong"""
        recs = self._firm() + [
            _rec("adv:200", crd="200", legal_name="Acme Credit LLC", name_norm="acme credit", domain="acme.com"),
            _rec("iapd:200", crd="200", legal_name="Acme Credit LLC", name_norm="acme credit", domain="acme.com"),
            _rec("pefirm:7", cik="42", legal_name="Acme Capital", name_norm="acme capital", domain="acme.com"),
        ]
        p, out = _resolve(recs)
        rows = _row(out, "acme.com")
        assert len(rows) == 2 and {r["status"] for r in rows} == {"conflict"}
        for r in rows:
            assert r["conflicts"][0]["reason"] == "domain_shared"
            assert r["conflicts"][0]["other_subjects"]
        assert out["entity_strong"] == {}

    def test_shared_host_cap(self):
        """T10: a domain named by more subjects than the cap is refused, counted"""
        recs = [_rec(f"adv:{i}", crd=str(i), name_norm=f"firm {i}", domain="bighost.com") for i in range(1, 6)]
        p, out = _resolve(recs, cap=3)
        assert _row(out, "bighost.com") == []
        assert out["metrics"]["shared_host_refused"] == 1
        assert out["metrics"]["shared_host_detail"][0] == {"domain": "bighost.com", "subjects": 5}

    def test_attach_by_name(self):
        """T11: a keyless record joins the identity of the same name on the same domain"""
        recs = self._firm() + [_rec("lpfund:9", legal_name="Acme Capital, L.L.C.", name_norm="acme capital",
                                    domain="acme.com")]
        p, out = _resolve(recs)
        rows = _row(out, "acme.com")
        assert len(rows) == 1 and rows[0]["status"] == "strong"
        assert {c["record_key"]: c["via"] for c in rows[0]["claims"]}["lpfund:9"] == "name"
        # a DIFFERENT name on the same domain is its own subject -> both conflict
        recs2 = self._firm() + [_rec("lpfund:9", legal_name="Teachers Pension", name_norm="teachers pension",
                                     domain="acme.com")]
        p, out = _resolve(recs2)
        assert {r["status"] for r in _row(out, "acme.com")} == {"conflict"}
        assert {r["subject"] for r in _row(out, "acme.com")} == {"comp:adv:100", "lpfund:9"}

    def test_attach_key_disagreement(self):
        """T12: attach-only keys that point at two different identities attach to neither"""
        recs = self._firm() + [
            _rec("edgar:77", cik="77", legal_name="Other Co", name_norm="other co"),
            _rec("form:77", cik="77", name_norm="other co"),
            _rec("industrial:3", cik="42", crd="77701", legal_name="Acme", name_norm="acme",
                 domain="acme-industrial.com"),
            _rec("adv:77701", crd="77701", name_norm="other adviser", domain="other.com"),
            _rec("iapd:77701", crd="77701", name_norm="other adviser"),
        ]
        p, out = _resolve(recs)
        rows = _row(out, "acme-industrial.com")
        assert len(rows) == 1
        assert rows[0]["subject"] == "industrial:3"
        assert rows[0]["status"] == "conflict"
        assert rows[0]["conflicts"][0]["reason"] == "attach_key_disagreement"
        assert out["metrics"]["attach"]["key_disagreement"] == 1

    def test_redirect_alias(self):
        """T13: A redirects to B -> A is an alias of B for its subjects, claims count toward B"""
        recs = self._firm(domain_adv="oldacme.com", domain_iapd="oldacme.com") + [
            _rec("pefirm:7", cik="42", name_norm="acme capital", domain="acme.com")]
        evidence = {"status": 301, "location": "https://www.acme.com/", "probed_at": "2026-09-30T00:00:00"}
        p, out = _resolve(recs, probes={"oldacme.com": {"redirect_to": "acme.com", "names_found": [],
                                                        "evidence": evidence}})
        old = _row(out, "oldacme.com")
        assert len(old) == 1 and old[0]["status"] == "alias"
        assert old[0]["alias_of"] == "acme.com" and old[0]["evidence"] == evidence
        new = _row(out, "acme.com")
        assert len(new) == 1 and new[0]["status"] == "strong"
        vias = sorted(c["via"] for c in new[0]["claims"])
        assert vias == ["key", "redirect:oldacme.com", "redirect:oldacme.com"]
        assert out["entity_strong"] == {0: ["acme.com"]}
        assert out["metrics"]["aliases"] == 1
        # the redirect alone is not a family: with no second source B stays weak
        p, out = _resolve(self._firm(domain_adv="oldacme.com", domain_iapd="oldacme.com"),
                          probes={"oldacme.com": {"redirect_to": "acme.com", "names_found": [],
                                                  "evidence": evidence}})
        assert [r["status"] for r in _row(out, "acme.com")] == ["weak"]

    def test_own_site_on_redirect_target(self):
        """T14b: claims moved by a redirect still meet the target's own-site evidence
        (live 2026-09-30: gryphoninvestors.com -> gryphon-inv.com, whose homepage names
        'Gryphon Investors')."""
        recs = [_rec("pefirm:91", legal_name="Gryphon Investors", name_norm="gryphon investors",
                     domain="gryphoninvestors.com")]
        probes = {"gryphoninvestors.com": {"redirect_to": "gryphon-inv.com", "names_found": [],
                                           "evidence": {"http_status": 301}},
                  "gryphon-inv.com": {"redirect_to": None, "names_found": ["gryphon investors"],
                                      "evidence": {"http_status": 200}}}
        p, out = _resolve(recs, probes=probes)
        new = _row(out, "gryphon-inv.com")
        assert len(new) == 1 and new[0]["status"] == "strong"
        assert new[0]["families"] == ["own_site", "pe_firm_web"]
        assert new[0]["evidence"] == {"http_status": 200}
        assert _row(out, "gryphoninvestors.com")[0]["status"] == "alias"

    def test_own_site_name_is_a_family(self):
        """T14: the company's own homepage naming the legal name is a second family"""
        probes = {"acme.com": {"redirect_to": None, "names_found": ["acme capital"],
                               "evidence": {"status": 200}}}
        p, out = _resolve(self._firm(), probes=probes)
        rows = _row(out, "acme.com")
        assert rows[0]["status"] == "strong"
        assert rows[0]["families"] == ["form_adv", "own_site"]
        # a page naming someone else adds nothing
        probes["acme.com"]["names_found"] = ["zeta holdings"]
        p, out = _resolve(self._firm(), probes=probes)
        assert _row(out, "acme.com")[0]["status"] == "weak"

    def test_domains_never_merge(self):
        """T15: components are identical with and without domains / attach-only records"""
        from app.entities import resolve_core as rc

        base = [
            _rec("adv:1", crd="101", name_norm="a", domain="same.com"),
            _rec("iapd:1", crd="101", name_norm="a", domain="same.com"),
            _rec("adv:2", crd="202", name_norm="b", domain="same.com"),
            _rec("iapd:2", crd="202", name_norm="b", domain="same.com"),
        ]
        bare = [dict(r, domain=None) for r in base]
        extra = [_rec("pefirm:1", cik="901", crd="202", name_norm="a", domain="same.com")]
        comps = lambda recs: [c["members"] for c in rc.plan(  # noqa: E731
            [r for r in recs if r["source"] not in rc.ATTACH_ONLY_SOURCES], [], {})["components"]]
        assert comps(base) == comps(bare) == comps(base + extra) == [["adv:1", "iapd:1"], ["adv:2", "iapd:2"]]
        p, out = _resolve(base + extra)
        assert out["metrics"]["merged"] == 0

    def test_canonical_domain_strong_only(self):
        """T16"""
        from app.entities.resolve import entity_domain_columns

        assert entity_domain_columns(["acme.com"]) == ("acme.com", {})
        assert entity_domain_columns([]) == (None, {})
        assert entity_domain_columns(["a.com", "b.com"]) == (None, {"domain_strong": ["a.com", "b.com"]})


# ---------------------------------------------------------------------------
# the open-web fetch and the probe collector
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


def _public(host):
    return ["93.184.216.34"]


def _fetcher(handler, terms=None, resolver=_public, clock=None):
    import httpx

    from app.core import open_web

    clock = clock or _Clock()
    terms = terms if terms is not None else {"acme.com": open_web.Review("allowed", "test", "2026-09-30")}
    seen = []

    def wrapped(request):
        seen.append(str(request.url))
        assert request.headers["user-agent"].startswith("NexdataResearch/1.0")
        return handler(request)

    f = open_web.OpenWebFetcher(terms=terms, transport=httpx.MockTransport(wrapped), resolve_host=resolver,
                                clock=clock.now, sleep=clock.sleep)
    return f, seen, clock


def _resp(status, text="", headers=None):
    import httpx

    return httpx.Response(status, text=text, headers=headers or {})


@pytest.mark.unit
class TestOpenWeb:
    def test_robots_rfc9309(self):
        """T17"""
        def site(robots_status, robots_body=""):
            def h(req):
                if req.url.path == "/robots.txt":
                    return _resp(robots_status, robots_body)
                return _resp(200, "<title>Acme</title>")
            return h

        f, seen, _ = _fetcher(site(200, "User-agent: *\nDisallow: /\n"))
        assert f.get("https://acme.com/").outcome == "robots_disallowed"
        assert seen == ["https://acme.com/robots.txt"]  # the page itself is never requested
        f, seen, _ = _fetcher(site(200, "User-agent: NexdataResearch\nDisallow: /\n\nUser-agent: *\nAllow: /\n"))
        assert f.get("https://acme.com/").outcome == "robots_disallowed"  # our token is named
        f, seen, _ = _fetcher(site(404))
        assert f.get("https://acme.com/").outcome == "fetched"  # 4xx: no robots = allowed
        f, seen, _ = _fetcher(site(403))
        assert f.get("https://acme.com/").outcome == "robots_disallowed"  # 401/403: conservative
        f, seen, _ = _fetcher(site(503))
        assert f.get("https://acme.com/").outcome == "robots_disallowed"  # 5xx: disallow all

        def boom(req):
            import httpx
            raise httpx.ConnectError("down", request=req)
        f, seen, _ = _fetcher(boom)
        assert f.get("https://acme.com/").outcome == "robots_disallowed"  # unreachable

    def test_robots_redirect_followed_through_the_gates(self):
        """T17c: RFC 9309 robots redirects are followed (<= 5 hops), each hop terms-gated.
        Measured 2026-09-30: gryphoninvestors.com/robots.txt answers 301 to gryphon-inv.com."""
        from app.core import open_web

        def h(req):
            if req.url.host == "old.com" and req.url.path == "/robots.txt":
                return _resp(301, "", {"Location": "https://www.new.com/robots.txt"})
            if req.url.host == "www.new.com" and req.url.path == "/robots.txt":
                return _resp(200, "User-agent: *\nAllow: /\n")
            if req.url.host == "old.com":
                return _resp(301, "", {"Location": "https://www.new.com/"})
            raise AssertionError(f"unexpected request {req.url}")
        review = open_web.Review("allowed", "test", "2026-09-30")
        f, seen, _ = _fetcher(h, terms={"old.com": review, "new.com": review})
        res = f.get("https://old.com/")
        assert res.outcome == "redirect_offsite" and res.redirect_to == "new.com"
        assert seen == ["https://old.com/robots.txt", "https://www.new.com/robots.txt", "https://old.com/"]
        # the robots target has no terms review: it is never asked, robots = disallow all
        f, seen, _ = _fetcher(h, terms={"old.com": review})
        assert f.get("https://old.com/").outcome == "robots_disallowed"
        assert seen == ["https://old.com/robots.txt"]

    def test_crawl_delay_paces_host(self):
        """T17b: two requests to one host are paced by max(crawl-delay, MIN_INTERVAL)"""
        from app.core import open_web

        def h(req):
            if req.url.path == "/robots.txt":
                return _resp(200, "User-agent: *\nCrawl-delay: 5\nAllow: /\n")
            return _resp(200, "ok")
        f, seen, clock = _fetcher(h)
        assert f.get("https://acme.com/").outcome == "fetched"
        assert f.get("https://acme.com/about").outcome == "fetched"
        assert sum(clock.slept) >= 5.0 + open_web.MIN_INTERVAL - 0.01
        assert seen.count("https://acme.com/robots.txt") == 1  # robots cached per origin

    def test_terms_gate_refuses_unreviewed(self):
        """T18"""
        from app.core import open_web

        f, seen, _ = _fetcher(lambda req: _resp(200, "x"), terms={})
        assert f.get("https://acme.com/").outcome == "terms_unreviewed"
        f2, seen2, _ = _fetcher(lambda req: _resp(200, "x"),
                                terms={"acme.com": open_web.Review("refused", "ToS s.4 bans bots", "2026-09-30")})
        res = f2.get("https://www.acme.com/")
        assert res.outcome == "terms_refused"
        assert seen == [] and seen2 == []  # zero requests, not even robots.txt
        # the registry file loads and validates
        reg = open_web.load_terms(REPO / "app" / "entities" / "data" / "site_terms.json")
        assert isinstance(reg, dict)
        with pytest.raises(ValueError):
            open_web.parse_terms([{"host": "acme.com", "verdict": "maybe", "citation": "x", "reviewed": "2026-09-30"}])
        with pytest.raises(ValueError):
            open_web.parse_terms([{"host": "acme.com", "verdict": "allowed", "citation": "", "reviewed": "2026-09-30"}])

    def test_retry_after_backs_off_host(self):
        """T19"""
        def h(req):
            if req.url.path == "/robots.txt":
                return _resp(404)
            return _resp(429, "slow down", {"Retry-After": "120"})
        f, seen, clock = _fetcher(h)
        res = f.get("https://acme.com/")
        assert res.outcome == "retry_after" and res.retry_after == 120.0
        n = len(seen)
        assert f.get("https://acme.com/other").outcome == "host_backed_off"
        assert len(seen) == n  # nothing sent while backed off
        assert seen.count("https://acme.com/") == 1  # never retried in the run

    def test_probe_records_cross_domain_redirect(self):
        """T20"""
        def h(req):
            if req.url.path == "/robots.txt":
                return _resp(404)
            if req.url.host == "acme.com":
                return _resp(301, "", {"Location": "https://www.acme.com/"})
            if req.url.host == "www.acme.com":
                return _resp(302, "", {"Location": "https://newacme.io/home"})
            raise AssertionError("an off-site hop must never be fetched")
        f, seen, _ = _fetcher(h)
        res = f.get("https://acme.com/")
        assert res.outcome == "redirect_offsite"
        assert res.redirect_to == "newacme.io"
        assert [hop["status"] for hop in res.hops] == [301, 302]
        assert "https://newacme.io/home" not in seen

        from app.entities import domain_probe

        f, seen, _ = _fetcher(h)
        row = domain_probe.probe_domain(f, "acme.com", ["acme capital"])
        assert row["outcome"] == "redirect_offsite" and row["redirect_to"] == "newacme.io"
        assert row["user_agent"].startswith("NexdataResearch/1.0")

    def test_probe_name_match(self):
        """T20b: names found on the fetched homepage (normalized, token-bounded)"""
        from app.entities import domain_probe

        page = "<html><title>Acme Capital, L.L.C. | Home</title><p>Not Acme Capitalist</p></html>"
        f, seen, _ = _fetcher(lambda req: _resp(404) if req.url.path == "/robots.txt" else _resp(200, page))
        row = domain_probe.probe_domain(f, "acme.com", ["acme capital", "zeta holdings", "ac"])
        assert row["outcome"] == "fetched"
        assert row["names_found"] == ["acme capital"]  # 'ac' too short to count

    def test_private_address_refused(self):
        """T21"""
        for addr in ("10.0.0.5", "127.0.0.1", "169.254.169.254", "192.168.1.1", "::1"):
            f, seen, _ = _fetcher(lambda req: _resp(200, "x"), resolver=lambda host, a=addr: [a])
            assert f.get("https://acme.com/").outcome == "non_public_address", addr
            assert seen == []


# ---------------------------------------------------------------------------
# Postgres
# ---------------------------------------------------------------------------


def _apply_migration(conn, name: str, attr: str):
    from sqlalchemy import text

    path = REPO / "alembic" / "versions" / f"{name}.py"
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    for stmt in getattr(mod, attr):
        conn.execute(text(stmt))
    return mod


SOURCE_DDL = {
    "sec_adv_roster_snapshots": "crd_number TEXT, roster_date DATE, legal_name TEXT, business_name TEXT, "
                                "main_office_state TEXT, website TEXT",
    "sec_adv_feed_firm_state": "crd_number TEXT, edition_date DATE, legal_name TEXT, business_name TEXT, "
                               "main_office_state TEXT, website TEXT",
    "sec_13f_filings": "accession_number TEXT, cik TEXT, crd_number TEXT, filing_manager_name TEXT, "
                       "filing_manager_state_or_country TEXT, filing_date DATE",
    "sec_13f_other_managers": "accession_number TEXT, cik TEXT, crd_number TEXT",
    "form_d_issuers": "cik TEXT, entity_name TEXT, state_or_country TEXT, loaded_at TIMESTAMP DEFAULT NOW()",
    "sec_filers": "cik TEXT, name TEXT, ein TEXT, state_of_incorporation TEXT, biz_state2 TEXT, "
                  "biz_state_or_country TEXT, website TEXT, loaded_at TIMESTAMP DEFAULT NOW()",
    "sec_8k_index": "cik TEXT",
    "sec_insider_owners": "rptowner_cik TEXT",
    "sec_insider_filings": "issuer_cik TEXT",
    "pe_firms": "id SERIAL, name TEXT, legal_name TEXT, website TEXT, headquarters_state TEXT, cik TEXT, "
                "crd_number TEXT, data_sources JSON, updated_at TIMESTAMPTZ DEFAULT NOW()",
    "industrial_companies": "id SERIAL, name TEXT, legal_name TEXT, website TEXT, headquarters_state TEXT, "
                            "cik TEXT, updated_at TIMESTAMPTZ DEFAULT NOW()",
    "lp_fund": "id SERIAL, name TEXT, formal_name TEXT, website_url TEXT, sec_crd_number TEXT, "
               "updated_at TIMESTAMP DEFAULT NOW()",
}


@pytest.fixture
def pg_engine():
    from sqlalchemy import create_engine, text

    engine = create_engine(PG_URL)
    with engine.begin() as conn:
        conn.execute(text("DROP SCHEMA IF EXISTS core CASCADE"))
        conn.execute(text("DROP SCHEMA IF EXISTS workbench CASCADE"))
        for t in list(SOURCE_DDL) + ["pe_portfolio_companies", "portfolio_companies", "three_pl_company",
                                     "family_offices"]:
            conn.execute(text(f"DROP TABLE IF EXISTS public.{t} CASCADE"))
        _apply_migration(conn, "0008_entity_master", "CORE_DDL")
        _apply_migration(conn, "0016_entity_weak_match", "UPGRADE_SQL")
        _apply_migration(conn, "0017_domain_link", "UPGRADE_SQL")
        for t, cols in SOURCE_DDL.items():
            conn.execute(text(f"CREATE TABLE {t} ({cols})"))
        # an adviser filing 13F (CIK 42 + CRD 100) whose ADV/IAPD website is acme.com, plus a
        # web-researched pe_firms row carrying CIK 42 and the same site -> strong
        conn.execute(text("""INSERT INTO sec_adv_roster_snapshots VALUES
            ('100', '2026-09-01', 'ACME CAPITAL LLC', NULL, 'NY', 'https://www.acme.com'),
            ('200', '2026-09-01', 'BETA ADVISERS LLC', NULL, 'CA', 'https://linkedin.com/company/beta'),
            ('300', '2026-09-01', 'GAMMA PARTNERS LP', NULL, 'TX', 'gamma.com')"""))
        conn.execute(text("""INSERT INTO sec_adv_feed_firm_state VALUES
            ('100', '2026-09-01', 'ACME CAPITAL LLC', NULL, 'NY', 'acme.com'),
            ('300', '2026-09-01', 'GAMMA PARTNERS LP', NULL, 'TX', 'http://gamma.com/')"""))
        conn.execute(text("""INSERT INTO sec_13f_filings VALUES
            ('a1', '0000000042', '100', 'ACME CAPITAL LLC', 'NY', '2026-08-14')"""))
        conn.execute(text("""INSERT INTO sec_filers (cik, name, state_of_incorporation) VALUES
            ('0000000042', 'Acme Capital LLC', 'NY')"""))
        conn.execute(text("""INSERT INTO pe_firms (name, website, headquarters_state, cik, data_sources) VALUES
            ('Acme Capital', 'https://www.acme.com', 'NY', '0000000042', '["https://www.acme.com"]'),
            ('Beta Advisers', 'beta.com', 'CA', NULL, '["SEC ADV"]')"""))
        conn.execute(text("""INSERT INTO industrial_companies (name, website, headquarters_state) VALUES
            ('Kimball Widgets', 'https://kimballwidgets.com/', 'OH')"""))
        conn.execute(text("""INSERT INTO lp_fund (name, website_url) VALUES ('Lone Fund', 'facebook.com/lone')"""))
    yield engine
    engine.dispose()


def _run(engine, dry_run=False):
    from app.entities import feeds, resolve

    with engine.connect() as conn:
        tx = conn.begin()
        counts = feeds.run_feeds(conn)
        metrics = resolve.resolve(conn, dry_run=dry_run)
        if dry_run:
            tx.rollback()
        else:
            tx.commit()
    return counts, metrics


@pg
def test_resolve_pg_domains(pg_engine):
    """T22"""
    from sqlalchemy import text

    counts, m = _run(pg_engine)
    assert counts["domains"]["adv"]["generic:social"] == 1
    assert counts["pefirm"] == 1  # the SEC ADV copy is not fed
    assert counts["industrial"] == 1 and counts["lpfund"] == 0  # generic-only row not fed
    assert counts["skipped"] == ["dol5500"] or "dol5500" in counts["skipped"]
    with pg_engine.connect() as conn:
        links = {(r[0], r[1]): r for r in conn.execute(text(
            "SELECT domain, status, entity_id, families, claims FROM core.domain_link ORDER BY 1"))}
        assert links[("acme.com", "strong")][3] == ["form_adv", "pe_firm_web"]
        eid = links[("acme.com", "strong")][2]
        assert links[("gamma.com", "weak")][2] is not None
        assert links[("kimballwidgets.com", "weak")][2] is None  # a lone record, no entity
        ent = conn.execute(text("SELECT canonical_domain FROM core.entity WHERE entity_id = :e"), {"e": eid}).scalar()
        assert ent == "acme.com"
        gamma = conn.execute(text(
            "SELECT e.canonical_domain FROM core.entity e JOIN core.membership m ON m.entity_id = e.entity_id "
            "WHERE m.record_key = 'adv:300'")).scalar()
        assert gamma is None  # weak never reaches canonical_domain
        ident = conn.execute(text(
            "SELECT entity_id, sources FROM core.identifier WHERE id_type = 'domain' AND id_value = 'acme.com'"
        )).fetchone()
        assert ident[0] == eid and set(ident[1]) == {"adv", "iapd", "pefirm"}
        assert conn.execute(text("SELECT COUNT(*) FROM core.identifier WHERE id_type = 'domain'")).scalar() == 1
        # pefirm is attach-only: it is no entity's member
        assert conn.execute(text("SELECT COUNT(*) FROM core.membership WHERE record_key LIKE 'pefirm:%'")).scalar() == 0
    assert m["domains"]["links_by_status.strong"] == 1
    assert m["domains"]["merged"] == 0
    # idempotent
    _c, m2 = _run(pg_engine)
    assert m2["domains"]["rows_written"] == 0 and m2["domains"]["rows_removed"] == 0


@pg
def test_dry_run_pg_writes_nothing(pg_engine):
    """T23"""
    from sqlalchemy import text

    _c, m = _run(pg_engine, dry_run=True)
    assert m["domains"]["links_by_status.strong"] == 1
    with pg_engine.connect() as conn:
        assert conn.execute(text("SELECT COUNT(*) FROM core.domain_link")).scalar() == 0
        assert conn.execute(text("SELECT COUNT(*) FROM core.source_record")).scalar() == 0


@pg
def test_probe_evidence_pg(pg_engine):
    """T22b: a recorded redirect probe turns the old domain into an alias at resolve time"""
    from sqlalchemy import text

    with pg_engine.begin() as conn:
        conn.execute(text("""INSERT INTO core.domain_probe (domain, probed_at, url, outcome, http_status,
            redirect_to, hops, names_checked, names_found, user_agent, terms_citation)
            VALUES ('gamma.com', '2026-09-30 12:00', 'https://gamma.com/', 'redirect_offsite', 301,
                    'gammanew.com', CAST(:hops AS JSONB), '[]'::jsonb, '[]'::jsonb, 'NexdataResearch/1.0', 'test')"""),
                     {"hops": json.dumps([{"url": "https://gamma.com/", "status": 301,
                                           "location": "https://gammanew.com/"}])})
    _run(pg_engine)
    with pg_engine.connect() as conn:
        row = conn.execute(text(
            "SELECT status, alias_of, evidence FROM core.domain_link WHERE domain = 'gamma.com'")).fetchone()
        assert row[0] == "alias" and row[1] == "gammanew.com"
        assert row[2]["http_status"] == 301
        assert conn.execute(text(
            "SELECT status FROM core.domain_link WHERE domain = 'gammanew.com'")).scalar() == "weak"
