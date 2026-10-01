"""
Tests for SPEC 149 — fixes from the adversarial review of SPEC_147 / SPEC_148.

PG-backed tests need TEST_PG_URL pointing at a DISPOSABLE database.
"""

import importlib.util
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
# placeholder EINs
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestPlaceholderEins:
    def test_placeholder_eins_refused(self):
        """T1"""
        from app.entities.feeds import _ein9
        from app.entities.resolve_core import _ein

        for bad in ("123456789", "12-3456789", "987654321", "006566090", "000034578"):
            assert _ein(bad) is None, bad
            assert _ein9(bad) is None, bad
        # a 7-digit value padded to 00xxxxxxx is not an EIN either
        assert _ein9("1234567") is None
        # real EINs are kept, leading zero included
        for good in ("012345678", "261640968", "043460239", "910470860"):
            assert _ein(good) == good
            assert _ein9(good) == good

    def test_placeholder_ein_never_joins(self):
        """T2: two filers that both typed 123456789 stay two identities"""
        from app.entities.resolve_core import plan

        recs = [
            _rec("edgar:0000000001", cik="1", ein="123456789", name_norm="belmere capital fund"),
            _rec("edgar:0000000002", cik="2", ein="123456789", name_norm="parametric core"),
        ]
        p = plan(recs, [], {})
        assert p["components"] == []
        assert all(not keys or keys == [("cik", rk[-1])] for rk, keys in p["record_keys"].items())


# ---------------------------------------------------------------------------
# the SPEC_147 edgar scope expansion
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestSponsorScope:
    def test_scope_sql_requires_unique_ein(self):
        """T3: the expansion only takes an EIN carried by exactly one sec_filers CIK"""
        from app.entities import feeds

        sql = " ".join(feeds._SPONSOR_EIN_CIKS.split()).upper()
        assert "HAVING COUNT(*) = 1" in sql
        assert "WORKBENCH.DOL5500_SPONSOR" in sql


# ---------------------------------------------------------------------------
# redirects (SPEC_148 domain links)
# ---------------------------------------------------------------------------


def _links(records, probes):
    from app.entities import resolve_core as rc

    resolvable = [r for r in records if r["source"] not in rc.ATTACH_ONLY_SOURCES]
    p = rc.plan(resolvable, [], {})
    return rc.domain_links(records, p["record_keys"], p["components"], probes)


def _firm(domain):
    return [
        _rec("adv:100", crd="100", legal_name="Acme Capital LLC", name_norm="acme capital", domain=domain),
        _rec("iapd:100", crd="100", legal_name="Acme Capital LLC", name_norm="acme capital", domain=domain),
        _rec("f13:42", cik="42", crd="100", legal_name="ACME CAPITAL", name_norm="acme capital"),
        _rec("pefirm:7", cik="42", name_norm="acme capital", domain=domain),
    ]


def _redirect(target):
    return {"redirect_to": target, "names_found": [], "evidence": {"http_status": 301}}


@pytest.mark.unit
class TestRedirectRules:
    def test_redirect_to_generic_not_alias(self):
        """T4: a site that now redirects to a platform host hands that host to nobody"""
        out = _links(_firm("acme.com"), {"acme.com": _redirect("linkedin.com")})
        rows = [r for r in out["rows"] if r["domain"] == "acme.com"]
        assert len(rows) == 1 and rows[0]["status"] == "conflict"
        assert rows[0]["alias_of"] is None
        assert [c["reason"] for c in rows[0]["conflicts"]] == ["redirect_to_generic"]
        assert rows[0]["evidence"] == {"http_status": 301}
        assert not [r for r in out["rows"] if r["domain"] == "linkedin.com"]
        assert out["entity_strong"] == {}
        assert out["metrics"]["aliases"] == 0
        assert out["metrics"]["redirects_refused"] == {"redirect_to_generic": 1}

    def test_redirect_chain_resolves(self):
        """T5: A -> B -> C counts A's and B's claims on C, whatever the processing order"""
        for a, b, c in (("aaa.com", "bbb.com", "ccc.com"), ("zzz.com", "mmm.com", "ccc.com")):
            out = _links(_firm(a), {a: _redirect(b), b: _redirect(c)})
            by_dom = {r["domain"]: r for r in out["rows"]}
            assert by_dom[a]["status"] == "alias" and by_dom[a]["alias_of"] == c
            assert by_dom[c]["status"] == "strong"
            assert out["entity_strong"] == {0: [c]}
            assert b not in by_dom or by_dom[b]["status"] == "alias"

    def test_redirect_cycle_refused(self):
        """T6: A <-> B moves nothing and makes neither an alias"""
        out = _links(_firm("aaa.com"), {"aaa.com": _redirect("bbb.com"), "bbb.com": _redirect("aaa.com")})
        rows = [r for r in out["rows"] if r["domain"] == "aaa.com"]
        assert len(rows) == 1 and rows[0]["status"] == "conflict"
        assert [c["reason"] for c in rows[0]["conflicts"]] == ["redirect_cycle"]
        assert not [r for r in out["rows"] if r["domain"] == "bbb.com"]
        assert out["metrics"]["aliases"] == 0
        assert rows[0]["claim_count"] == 3


# ---------------------------------------------------------------------------
# API lookup
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_lookup_value_ein_hyphen():
    """T7"""
    from app.api.v1.entity_master import _lookup_value

    assert _lookup_value("ein", "26-1640968") == "261640968"
    assert _lookup_value("ein", "261640968") == "261640968"
    assert _lookup_value("cik", "0000000042") == "42"


# ---------------------------------------------------------------------------
# PG
# ---------------------------------------------------------------------------


def _apply_migration(conn, name: str, attr: str):
    from sqlalchemy import text

    path = REPO / "alembic" / "versions" / f"{name}.py"
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    for stmt in getattr(mod, attr):
        conn.execute(text(stmt))


SOURCE_TABLES = (
    "sec_adv_roster_snapshots", "sec_adv_feed_firm_state", "sec_13f_filings", "sec_13f_other_managers",
    "form_d_issuers", "sec_filers", "sec_8k_index", "sec_insider_owners", "sec_insider_filings",
)


@pytest.fixture
def pg_engine():
    from sqlalchemy import create_engine, text

    engine = create_engine(PG_URL)
    with engine.begin() as conn:
        conn.execute(text("DROP SCHEMA IF EXISTS core CASCADE"))
        conn.execute(text("DROP SCHEMA IF EXISTS workbench CASCADE"))
        for t in SOURCE_TABLES:
            conn.execute(text(f"DROP TABLE IF EXISTS public.{t} CASCADE"))
        _apply_migration(conn, "0008_entity_master", "CORE_DDL")
        _apply_migration(conn, "0016_entity_weak_match", "UPGRADE_SQL")
        _apply_migration(conn, "0017_domain_link", "UPGRADE_SQL")
        conn.execute(text("""CREATE TABLE sec_adv_roster_snapshots (crd_number TEXT, roster_date DATE,
            legal_name TEXT, business_name TEXT, main_office_state TEXT, website TEXT)"""))
        conn.execute(text("""CREATE TABLE sec_adv_feed_firm_state (crd_number TEXT, edition_date DATE,
            legal_name TEXT, business_name TEXT, main_office_state TEXT, website TEXT)"""))
        conn.execute(text("""CREATE TABLE sec_13f_filings (accession_number TEXT, cik TEXT,
            crd_number TEXT, filing_manager_name TEXT, filing_manager_state_or_country TEXT,
            filing_date DATE)"""))
        conn.execute(text("CREATE TABLE sec_13f_other_managers (accession_number TEXT, cik TEXT, crd_number TEXT)"))
        conn.execute(text("""CREATE TABLE form_d_issuers (cik TEXT, entity_name TEXT,
            state_or_country TEXT, loaded_at TIMESTAMP DEFAULT NOW())"""))
        conn.execute(text("""CREATE TABLE sec_filers (cik TEXT, name TEXT, ein TEXT,
            state_of_incorporation TEXT, biz_state2 TEXT, biz_state_or_country TEXT, website TEXT,
            loaded_at TIMESTAMP DEFAULT NOW())"""))
        conn.execute(text("CREATE TABLE sec_8k_index (cik TEXT)"))
        conn.execute(text("CREATE TABLE sec_insider_owners (rptowner_cik TEXT)"))
        conn.execute(text("CREATE TABLE sec_insider_filings (issuer_cik TEXT)"))
        # 0000000042: PE-relevant (Form D) and carries a SHARED sponsor EIN with 0000000043
        # (a subsidiary outside every PE table). 0000000777: unique sponsor EIN, outside scope.
        conn.execute(text("""INSERT INTO form_d_issuers (cik, entity_name, state_or_country)
            VALUES ('0000000042', 'PARENT CO', 'CO')"""))
        conn.execute(text("""INSERT INTO sec_filers (cik, name, ein, state_of_incorporation, biz_state2)
            VALUES ('0000000042', 'Parent Co', '680521411', 'DE', 'CO'),
                   ('0000000043', 'Parent Sub LLC', '680521411', 'DE', 'CO'),
                   ('0000000044', 'Parent Sub Two LLC', '680521411', 'DE', 'CO'),
                   ('0000000777', 'Zed Holdings Inc', '261640968', 'DE', 'DE')"""))
        conn.execute(text("CREATE SCHEMA workbench"))
        conn.execute(text("""CREATE TABLE workbench.dol5500_sponsor (ein TEXT, form_year INTEGER,
            sponsor_name TEXT, state TEXT, zip5 TEXT, loaded_at TIMESTAMPTZ DEFAULT NOW(),
            PRIMARY KEY (ein, form_year))"""))
        conn.execute(text("""INSERT INTO workbench.dol5500_sponsor (ein, form_year, sponsor_name, state, zip5)
            VALUES ('680521411', 2025, 'PARENT CO', 'CO', '80111'),
                   ('261640968', 2025, 'ZED HOLDINGS INC', 'DE', '19801')"""))
    yield engine
    engine.dispose()


def _keys(engine, source="edgar"):
    from sqlalchemy import text

    with engine.connect() as conn:
        return {r[0] for r in conn.execute(
            text("SELECT record_key FROM core.source_record WHERE source = :s"), {"s": source})}


@pg
def test_shared_sponsor_ein_not_expanded_pg(pg_engine):
    """T8"""
    from app.entities import feeds

    with pg_engine.begin() as conn:
        counts = feeds.run_feeds(conn)
    keys = _keys(pg_engine)
    assert "edgar:0000000042" in keys            # PE-relevant: in scope as before
    assert "edgar:0000000777" in keys            # unique sponsor EIN: expanded
    assert "edgar:0000000043" not in keys        # shared sponsor EIN: never expanded
    assert "edgar:0000000044" not in keys
    assert counts["sponsor_ein_shared"] == 1


@pg
def test_prune_expansion_only_pg(pg_engine):
    """T9: records a wider (pre-SPEC_149) scope fed are pruned; in-scope ones stay"""
    from sqlalchemy import text
    from app.entities import feeds

    wide = feeds._edgar_feed(feeds._RELEVANT_CIKS + """
        UNION SELECT sf.cik FROM sec_filers sf
        JOIN (SELECT DISTINCT ein FROM workbench.dol5500_sponsor WHERE ein IS NOT NULL) sp
          ON sp.ein = sf.ein
    """)
    with pg_engine.begin() as conn:
        feeds.run_feeds(conn, feeds=[wide])
    assert {"edgar:0000000043", "edgar:0000000044"} <= _keys(pg_engine)
    with pg_engine.begin() as conn:
        # a stale record of a PE-relevant CIK is NOT this spec's to prune
        conn.execute(text("INSERT INTO form_d_issuers (cik, entity_name) VALUES ('0000000099', 'GONE INC')"))
        conn.execute(text("""INSERT INTO core.source_record (record_key, source, native_id, legal_name)
            VALUES ('edgar:0000000099', 'edgar', '0000000099', 'GONE INC')"""))
    with pg_engine.begin() as conn:
        counts = feeds.run_feeds(conn)
    keys = _keys(pg_engine)
    assert "edgar:0000000043" not in keys and "edgar:0000000044" not in keys
    assert {"edgar:0000000042", "edgar:0000000777", "edgar:0000000099"} <= keys
    assert counts["edgar_pruned"] == 2
    with pg_engine.begin() as conn:
        assert feeds.run_feeds(conn)["edgar_pruned"] == 0
