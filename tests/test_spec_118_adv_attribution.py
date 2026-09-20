"""
Tests for SPEC 118 — ADV attribution tiers and the current-state mart.

The loader itself is covered by tests/test_spec_118_adv_schedule_d_bulk.py.
PG-backed tests need TEST_PG_URL pointing at a DISPOSABLE database.
"""
import importlib.util
import os
from datetime import date
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
PG_URL = os.environ.get("TEST_PG_URL")
pg = pytest.mark.skipif(not PG_URL, reason="TEST_PG_URL not set")

ADDR_A = "1 main st|94105"
ADDR_B = "2 oak ave|10001"


# ---------------------------------------------------------------------------
# tiers (pure)
# ---------------------------------------------------------------------------

@pytest.mark.unit
class TestAdvFundIndex:
    def test_index_drops_multi_family_keeps_siblings(self):
        """T15"""
        from app.marts.links import build_adv_fund_index

        index = build_adv_fund_index([
            ("alpha fund i", "8361", "alpha capital", ADDR_A),
            # series LLC: two CRDs, one adviser name, one address
            ("beta fund ii", "2222", "beta capital", ADDR_B),
            ("beta fund ii", "1111", "beta capital", ADDR_B),
            # the same adviser reporting the fund twice
            ("gamma fund iii", "3333", "gamma capital", "3 elm st|02110"),
            ("gamma fund iii", "3333", "gamma capital", "3 elm st|02110"),
            # two unrelated advisers claim the name
            ("delta fund iv", "4444", "delta capital", "4 pine st|60601"),
            ("delta fund iv", "5555", "omega advisors", "9 cedar rd|94105"),
            # same adviser name, different offices -> not proven siblings
            ("epsilon fund v", "6666", "epsilon capital", "6 first st|30301"),
            ("epsilon fund v", "7777", "epsilon capital", "7 second st|30301"),
            # a missing address proves nothing either
            ("zeta fund vi", "8888", "zeta capital", None),
            ("zeta fund vi", "9999", "zeta capital", None),
        ])

        assert index["alpha fund i"] == "8361"
        assert index.tiers["alpha fund i"] == "adv_exact"
        assert index["beta fund ii"] == "1111"          # stable pick within the family
        assert index.tiers["beta fund ii"] == "adv_family"
        assert index["gamma fund iii"] == "3333"
        assert index.tiers["gamma fund iii"] == "adv_family"
        for core in ("delta fund iv", "epsilon fund v", "zeta fund vi"):
            assert core not in index
            assert core in index.multi_family_cores

    def test_family_crd_is_the_main_filer(self):
        """A relying adviser filing one fund does not take the family's links."""
        from app.marts.links import build_adv_fund_index

        index = build_adv_fund_index([
            ("shared name fund", "1111", "beta capital", ADDR_B),
            ("shared name fund", "2222", "beta capital", ADDR_B),
            ("other fund one", "2222", "beta capital", ADDR_B),
            ("other fund two", "2222", "beta capital", ADDR_B),
        ])
        assert index["shared name fund"] == "2222"

    def test_tier_refusals(self):
        """T16"""
        from app.marts.links import build_adv_fund_index, match_adv_fund

        index = build_adv_fund_index([
            ("alpha fund i", "8361", "alpha capital", ADDR_A),
            ("delta fund iv", "4444", "delta capital", "4 pine st|60601"),
            ("delta fund iv", "5555", "omega advisors", "9 cedar rd|94105"),
        ])
        masters = {"master pool trust one"}

        assert match_adv_fund("Alpha Fund I, L.P.", index, masters) == ("8361", "adv_exact")
        # too generic to be a fund name
        assert match_adv_fund("Summit Capital LP", index, masters) == (None, "short_core")
        assert match_adv_fund("Zed LP", index, masters) == (None, "short_core")
        # claimed by two adviser families
        assert match_adv_fund("Delta Fund IV LP", index, masters) == (None, "multi_family")
        # only ever seen as somebody's master fund
        assert match_adv_fund("Master Pool Trust One LP", index, masters) == (
            None, "master_name_only")
        # not a refusal: nobody lists it at all
        assert match_adv_fund("Nobody Fund Ever LP", index, masters) == (None, None)
        assert match_adv_fund(None, index, masters) == (None, "short_core")

    def test_adv_tiers_rank_above_name_core(self):
        """The ADV list beats a prefix guess, and refusals are counted."""
        from app.marts.links import attribute, build_adv_fund_index, build_core_index

        core_index = build_core_index([("8361", "ALPHA CAPITAL LLC", None),
                                       ("4242", "NORTH PEAK ADVISORS LLC", None)])
        adv_index = build_adv_fund_index([
            # Alpha's own Schedule D says this fund is managed by 9999, not 8361
            ("alpha capital fund iii", "9999", "alpha managers", ADDR_A),
            ("delta fund iv", "4444", "delta capital", "4 pine st|60601"),
            ("delta fund iv", "5555", "omega advisors", "9 cedar rd|94105"),
        ])
        out = attribute(
            [("101", "ALPHA CAPITAL FUND III, L.P."),
             ("202", "DELTA FUND IV LP"),
             ("303", "NORTH PEAK ADVISORS FUND II LP"),
             ("404", "UNRELATED FUND LP")],
            {"404": ["North Peak Advisors LLC"]},
            core_index,
            adv_index,
            {"some master fund"},
        )
        assert out["101"] == ("9999", "adv_exact")        # not the name_core guess
        assert out.get("202") is None                     # refused, not guessed
        assert out["303"] == ("4242", "name_core")
        assert out["404"] == ("4242", "related_person")
        assert out.refusals["multi_family"] == 1
        assert out.tiers["adv_exact"] == 1

    def test_platform_advisers_are_identified_by_name_collisions(self):
        """T25: a filing platform's funds carry many OTHER advisers' names.

        Measured on the live data, this is the only signal that separates the
        two classes. Fund count does not (Apollo reports 880), and name
        affinity does not (KKR's own funds are named "KKR", not "Kohlberg
        Kravis Roberts", so it scores 0.000 — the same as AngelList).
        """
        from app.marts.links import build_core_index, find_platform_advisers

        core_index = build_core_index([
            ("167700", "PLATFORM ADVISOR, LLC", None),
            ("111128", "CARLYLE GROUP", None),
            ("500001", "SINGH CAPITAL PARTNERS", None),
            ("500002", "MANA VENTURES", None),
            ("500003", "NOMAD VENTURE PARTNERS LLC", None),
        ])
        rows = [
            # the platform files for three unrelated sponsors
            ("singh capital rolling fund d1", "167700", "platform advisor", ADDR_A),
            ("mana ventures lp f1", "167700", "platform advisor", ADDR_A),
            ("nomad venture partners lp c2", "167700", "platform advisor", ADDR_A),
            # a real GP files only for itself, however many funds it has
            ("carlyle partners viii", "111128", "carlyle group", ADDR_B),
            ("carlyle partners ix", "111128", "carlyle group", ADDR_B),
            ("carlyle europe v", "111128", "carlyle group", ADDR_B),
        ]
        assert find_platform_advisers(rows, core_index, min_collisions=3,
                                      min_funds=3) == {"167700"}
        # the threshold is a real cliff, not a knob: nothing sits between
        assert find_platform_advisers(rows, core_index, min_collisions=4,
                                      min_funds=3) == set()

    def test_one_fund_row_cannot_convict_an_adviser(self):
        """T29: a collision is one adviser NAME, however many entities share it.

        Big brands register many adviser entities: "goldman sachs" is one name
        but a dozen CRDs. Crediting a single fund row with all of them let one
        co-investment vehicle named after a bank partner classify a five-fund
        GP as a filing platform.
        """
        from app.marts.links import build_core_index, find_platform_advisers

        brand = build_core_index(
            [(str(900 + i), f"GOLDMAN SACHS UNIT {i} LLC", None) for i in range(30)]
        )
        # one fund, one borrowed name -> one collision, not thirty
        one_row = [("goldman sachs co invest 2025", "555", "small gp", ADDR_A)]
        assert find_platform_advisers(one_row, brand, min_collisions=2, min_funds=1) == set()

        # and a filer with too few funds is never convicted, whatever it names
        few = [(f"goldman sachs deal {i}", "555", "small gp", ADDR_A) for i in range(3)]
        assert find_platform_advisers(few, brand, min_collisions=1) == set()

    def test_displacement_that_cannot_be_written_keeps_the_platform_link(self):
        """T30: never trade a link we can write for one we cannot.

        The sponsor comes from the whole adviser roster, but only advisers
        reporting PE/VC funds get a pe_firms row. Measured on live data, 234
        of 320 displacements pointed at an adviser with no row, so the fund
        lost its manager entirely.
        """
        from app.marts.links import attribute, build_adv_fund_index, build_core_index

        core_index = build_core_index([
            ("167700", "PLATFORM ADVISOR, LLC", None),
            ("500001", "SINGH CAPITAL PARTNERS", None),
            ("500002", "OFFROSTER ADVISERS LLC", None),
        ])
        adv_index = build_adv_fund_index([
            ("singh capital rolling fund d1", "167700", "platform advisor", ADDR_A),
            ("offroster advisers fund ii", "167700", "platform advisor", ADDR_A),
        ])
        out = attribute(
            [("101", "Singh Capital Rolling Fund - D1"),
             ("202", "Offroster Advisers Fund II, L.P.")],
            {}, core_index, adv_index, platform_crds={"167700"},
            # only Singh has a pe_firms row
            can_resolve=lambda crd: crd in {"167700", "500001"},
        )
        assert out["101"] == ("500001", "name_core")
        # the sponsor is unwritable, so the true claim is kept rather than lost
        assert out["202"] == ("167700", "adv_platform")
        assert out.refusals["platform_displaced"] == 1
        assert out.refusals["platform_sponsor_unresolvable"] == 1

    def test_name_core_does_not_match_inside_a_token(self):
        """T31: 'CARMELINA CAPITAL' is not Carmel Partners.

        Measured: 89 of 1,939 live name_core links cut inside a token, and the
        wrong ones are whole different companies — "HarbourView Royalties Fund"
        matched HARBOUR GROUP INDUSTRIES, a Missouri industrial holding
        company. A trailing plural is still allowed, because "ASCENT VENTURES
        LP" really is Ascent Venture Partners.
        """
        from app.marts.links import build_core_index, match_name_core

        core_index = build_core_index([
            ("1", "CARMEL PARTNERS", None),
            ("2", "HARBOUR GROUP INDUSTRIES, INC.", None),
            ("3", "ASCENT VENTURE PARTNERS", None),
            ("4", "GENSTAR CAPITAL LLC", None),
        ])
        assert match_name_core("CARMELINA CAPITAL PARTNERS I, L.P.", core_index) is None
        assert match_name_core("HarbourView Royalties Fund I, LP", core_index) is None
        assert match_name_core("ASCENT VENTURES LP", core_index) == "3"      # plural
        assert match_name_core("GENSTAR CAPITAL PARTNERS X, L.P.", core_index) == "4"

    def test_platform_claim_yields_to_the_sponsor_the_fund_is_named_after(self):
        """T26: AngelList's adviser-of-record must not displace the sponsor.

        "Singh Capital Rolling Fund - D1" is a Singh Capital fund that
        Platform Advisor LLC files. The ADV tier is normally the strongest
        evidence, but a platform's claim says who filed, not who sponsors, so
        a name that identifies a different adviser wins.
        """
        from app.marts.links import attribute, build_adv_fund_index, build_core_index

        core_index = build_core_index([
            ("167700", "PLATFORM ADVISOR, LLC", None),
            ("500001", "SINGH CAPITAL PARTNERS", None),
        ])
        adv_index = build_adv_fund_index([
            ("singh capital rolling fund d1", "167700", "platform advisor", ADDR_A),
            # a blind SPV: nothing in the name points anywhere else
            ("project condor spv i", "167700", "platform advisor", ADDR_A),
        ])
        out = attribute(
            [("101", "Singh Capital Rolling Fund - D1"),
             ("202", "Project Condor SPV I, L.P.")],
            {}, core_index, adv_index, platform_crds={"167700"},
        )
        # the sponsor the fund is named after, not the filer
        assert out["101"] == ("500001", "name_core")
        assert out.refusals["platform_displaced"] == 1
        # no better answer exists, so the true claim stands — labelled, so a
        # consumer wanting real GPs can exclude it
        assert out["202"] == ("167700", "adv_platform")
        assert out.tiers["adv_platform"] == 1

    def test_agreeing_with_a_human_does_not_erase_that_a_human_said_it(self):
        """T32: a 'manual' link keeps its provenance when the mart agrees.

        The keep-prior branch only fired when the firms differed, so agreeing
        with a human overwrote firm_link_method with the mart's own tier and
        dropped the row from rank 0 to rank 1 for every later run.
        """
        from app.marts.pe_funds_sec import _rank

        assert _rank("manual") < _rank("adv_exact")   # a person outranks the mart
        assert _rank(None) > _rank("adv_platform")    # a legacy guess is weakest

    def test_non_platform_adv_claims_are_untouched(self):
        """T27: the rule must not disturb the ordinary case.

        StepStone Real Assets genuinely manages "StepStone NLGI
        Infrastructure Opportunities Fund" even though the name's prefix
        matches its sibling StepStone Group LP.
        """
        from app.marts.links import attribute, build_adv_fund_index, build_core_index

        core_index = build_core_index([("143635", "STEPSTONE GROUP LP", None),
                                       ("281695", "STEPSTONE GROUP REAL ASSETS LP", None)])
        adv_index = build_adv_fund_index([
            ("stepstone nlgi infrastructure opportunities fund", "281695",
             "stepstone group real assets", ADDR_A),
        ])
        out = attribute([("101", "StepStone NLGI Infrastructure Opportunities Fund, L.P.")],
                        {}, core_index, adv_index, platform_crds={"167700"})
        assert out["101"] == ("281695", "adv_exact")
        assert out.refusals["platform_displaced"] == 0

    def test_attribute_without_adv_index_is_unchanged(self):
        """SPEC_117 callers keep working before the ADV mart exists."""
        from app.marts.links import attribute, build_core_index

        core_index = build_core_index([("8361", "GENSTAR CAPITAL LLC", None)])
        out = attribute([("101", "GENSTAR CAPITAL PARTNERS X, L.P.")], {}, core_index)
        assert out["101"] == ("8361", "name_core")


# ---------------------------------------------------------------------------
# Postgres
# ---------------------------------------------------------------------------

def _adv_ddl():
    """The three ADV tables (and pe_funds.firm_link_method) from migration 0010."""
    path = REPO / "alembic" / "versions" / "0010_adv_private_funds.py"
    spec = importlib.util.spec_from_file_location("mig0010", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return list(mod.UPGRADE_SQL)


@pytest.fixture
def pg_engine():
    from sqlalchemy import create_engine, text

    engine = create_engine(PG_URL)
    with engine.begin() as conn:
        conn.execute(text("DROP SCHEMA IF EXISTS stg CASCADE"))
        for t in ("pe_funds", "pe_firms", "sec_adv_roster_snapshots", "form_d_offerings",
                  "form_d_issuers", "form_d_related_persons", "sec_adv_filings",
                  "sec_adv_private_fund_filings", "sec_adv_private_funds"):
            conn.execute(text(f"DROP TABLE IF EXISTS public.{t} CASCADE"))
        conn.execute(text("""
            CREATE TABLE pe_firms (id SERIAL PRIMARY KEY, name TEXT NOT NULL, legal_name TEXT,
                website TEXT, headquarters_city TEXT, headquarters_state TEXT,
                headquarters_country TEXT, firm_type TEXT, primary_strategy TEXT,
                aum_usd_millions NUMERIC, cik TEXT, sec_file_number TEXT, crd_number TEXT,
                is_sec_registered BOOLEAN, status TEXT, data_sources JSON,
                last_verified_date DATE, updated_at TIMESTAMP)"""))
        # firm_link_method arrives with migration 0010, applied below
        conn.execute(text("""
            CREATE TABLE pe_funds (id SERIAL PRIMARY KEY, firm_id INTEGER, name TEXT NOT NULL,
                cik TEXT, vintage_year INTEGER, target_size_usd_millions NUMERIC,
                final_close_usd_millions NUMERIC, strategy TEXT, status TEXT,
                first_close_date DATE, sec_file_number TEXT, data_source TEXT,
                updated_at TIMESTAMP)"""))
        conn.execute(text(
            "CREATE UNIQUE INDEX uq_pe_funds_cik ON pe_funds (cik) WHERE cik IS NOT NULL"))
        conn.execute(text("""
            CREATE TABLE sec_adv_roster_snapshots (crd_number TEXT, roster_date DATE,
                legal_name TEXT, business_name TEXT, sec_number TEXT, adviser_type TEXT,
                main_office_street1 TEXT, main_office_street2 TEXT,
                main_office_postal_code TEXT, main_office_city TEXT, main_office_state TEXT,
                main_office_country TEXT, website TEXT, aum_total NUMERIC, sec_status TEXT,
                raw JSONB)"""))
        conn.execute(text("""
            CREATE TABLE form_d_offerings (accession_number TEXT, investment_fund_type TEXT,
                date_of_first_sale DATE, total_offering_amount NUMERIC, total_amount_sold NUMERIC,
                is_indefinite BOOLEAN, file_num TEXT, loaded_at TIMESTAMP DEFAULT NOW())"""))
        conn.execute(text("""
            CREATE TABLE form_d_issuers (accession_number TEXT, cik TEXT, entity_name TEXT,
                is_primary BOOLEAN)"""))
        conn.execute(text("""
            CREATE TABLE form_d_related_persons (accession_number TEXT, first_name TEXT,
                middle_name TEXT, last_name TEXT)"""))
        for stmt in _adv_ddl():
            conn.execute(text(stmt))
    yield engine
    engine.dispose()


def _filing(conn, filing_id, crd, filed_at, legal_name="ALPHA CAPITAL LLC"):
    from sqlalchemy import text

    conn.execute(text("""
        INSERT INTO sec_adv_filings (filing_id, crd_number, adviser_type, sec_number,
            legal_name, filed_at)
        VALUES (:f, :crd, 'ria', '801-60166', :name, :filed)"""),
        {"f": filing_id, "crd": crd, "name": legal_name, "filed": filed_at})


def _fund_filing(conn, filing_id, fund_id, fund_name, gav=None, master_name=None):
    from sqlalchemy import text
    from app.entities import norm

    conn.execute(text("""
        INSERT INTO sec_adv_private_fund_filings (filing_id, private_fund_id, adviser_type,
            fund_name, fund_name_core, fund_type, gross_asset_value, master_fund_name)
        VALUES (:f, :pid, 'ria', :name, :core, 'Private Equity Fund', :gav, :master)"""),
        {"f": filing_id, "pid": fund_id, "name": fund_name, "core": norm.core(fund_name),
         "gav": gav, "master": master_name})


def _mart_rows(engine):
    from sqlalchemy import text

    with engine.connect() as conn:
        return [dict(r) for r in conn.execute(text(
            "SELECT crd_number, private_fund_id, filing_id, fund_name, fund_name_core, "
            "gross_asset_value, as_of_date "
            "FROM sec_adv_private_funds ORDER BY crd_number, private_fund_id")).mappings()]


@pg
def test_mart_is_idempotent_and_order_independent(pg_engine):
    """The mart is recomputed from filing grain, so release order cannot matter."""
    from sqlalchemy import text
    from app.marts import adv_private_funds

    with pg_engine.begin() as conn:
        _filing(conn, 301, "8361", date(2026, 3, 15))
        _fund_filing(conn, 301, "805-9253414470", "ALPHA FUND I LP", gav=100)
        stats = adv_private_funds.build(conn, today=date(2026, 9, 20))
    assert (stats["candidates"], stats["inserted"], stats["deleted"]) == (1, 1, 0)

    # the August release arrives second
    with pg_engine.begin() as conn:
        _filing(conn, 801, "8361", date(2026, 8, 15))
        _fund_filing(conn, 801, "805-9253414470", "ALPHA FUND I, L.P.", gav=200)
        adv_private_funds.build(conn, today=date(2026, 9, 20))
    forward = _mart_rows(pg_engine)
    assert forward[0]["filing_id"] == 801 and forward[0]["gross_asset_value"] == 200
    assert forward[0]["private_fund_id"] == "805-9253414470"   # ten digits, untouched

    with pg_engine.begin() as conn:
        again = adv_private_funds.build(conn, today=date(2026, 9, 20))
    assert (again["inserted"], again["updated"], again["deleted"]) == (0, 0, 0)
    with pg_engine.connect() as conn:
        assert conn.execute(text("SELECT to_regclass('stg.mart_adv_private_funds')")).scalar() is None

    # same releases, loaded the other way round
    with pg_engine.begin() as conn:
        conn.execute(text("DELETE FROM sec_adv_private_funds"))
        conn.execute(text("DELETE FROM sec_adv_private_fund_filings"))
        conn.execute(text("DELETE FROM sec_adv_filings"))
        _filing(conn, 801, "8361", date(2026, 8, 15))
        _fund_filing(conn, 801, "805-9253414470", "ALPHA FUND I, L.P.", gav=200)
        adv_private_funds.build(conn, today=date(2026, 9, 20))
        _filing(conn, 301, "8361", date(2026, 3, 15))
        _fund_filing(conn, 301, "805-9253414470", "ALPHA FUND I LP", gav=100)
        adv_private_funds.build(conn, today=date(2026, 9, 20))
    assert _mart_rows(pg_engine) == forward


@pg
def test_mart_drops_rows_the_filings_no_longer_support(pg_engine):
    from sqlalchemy import text
    from app.marts import adv_private_funds

    with pg_engine.begin() as conn:
        _filing(conn, 1, "8361", date(2026, 8, 15))
        _fund_filing(conn, 1, "805-1", "ALPHA FUND I LP")
        _fund_filing(conn, 1, "805-2", "ALPHA FUND II LP")
        adv_private_funds.build(conn, today=date(2026, 9, 20))
        conn.execute(text("DELETE FROM sec_adv_private_fund_filings WHERE private_fund_id = '805-2'"))
        stats = adv_private_funds.build(conn, today=date(2026, 9, 20))
    assert stats["deleted"] == 1
    assert [r["private_fund_id"] for r in _mart_rows(pg_engine)] == ["805-1"]


def _seed_funds(engine):
    from sqlalchemy import text

    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO pe_firms (name, crd_number, status) VALUES
              ('ALPHA CAPITAL', '8361', 'Active'),
              ('OMEGA ADVISORS', '9999', 'Active')"""))
        conn.execute(text("""
            INSERT INTO sec_adv_roster_snapshots (crd_number, roster_date, legal_name,
                business_name, adviser_type, main_office_street1, main_office_postal_code,
                sec_status, raw)
            VALUES ('8361','2026-09-01','ALPHA CAPITAL LLC','ALPHA CAPITAL','ria',
                    '1 Main Street','94105','APPROVED','{"Any PE Funds":"Y"}'),
                   ('9999','2026-09-01','OMEGA ADVISORS LLC','OMEGA ADVISORS','ria',
                    '9 Cedar Road','10001','APPROVED','{"Any PE Funds":"Y"}')"""))
        conn.execute(text("""
            INSERT INTO form_d_issuers (accession_number, cik, entity_name, is_primary) VALUES
              ('acc-1','0000000101','BRIGHTWATER GROWTH FUND III, L.P.', true),
              ('acc-2','0000000202','SOLO UNCLAIMED FUND LP', true)"""))
        conn.execute(text("""
            INSERT INTO form_d_offerings (accession_number, investment_fund_type,
                date_of_first_sale, total_offering_amount, total_amount_sold, is_indefinite, file_num)
            VALUES ('acc-1','Private Equity Fund','2025-03-01', 500000000, 250000000, false, '021-1'),
                   ('acc-2','Private Equity Fund','2025-04-01', 100000000, 10000000, false, '021-2')"""))
        # Alpha's Schedule D claims the Brightwater fund; nothing else matches it
        _filing(conn, 1, "8361", date(2026, 8, 15))
        _fund_filing(conn, 1, "805-1", "BRIGHTWATER GROWTH FUND III LP")
    from app.marts import adv_private_funds

    with engine.begin() as conn:
        adv_private_funds.build(conn, today=date(2026, 9, 20))


@pg
def test_pe_funds_records_link_method_and_never_downgrades(pg_engine):
    """T17"""
    from sqlalchemy import text
    from app.marts import pe_funds_sec

    _seed_funds(pg_engine)
    with pg_engine.begin() as conn:
        stats = pe_funds_sec.build(conn, today=date(2026, 9, 20))
    assert stats["linked_adv_exact"] == 1 and stats["new_links"] == 1
    assert stats["adv_index_rows"] == 1

    with pg_engine.connect() as conn:
        rows = {r["cik"]: dict(r) for r in conn.execute(text(
            "SELECT cik, firm_id, firm_link_method FROM pe_funds")).mappings()}
    alpha_id = rows["0000000101"]["firm_id"]
    assert rows["0000000101"]["firm_link_method"] == "adv_exact"
    assert rows["0000000202"] == {"cik": "0000000202", "firm_id": None, "firm_link_method": None}

    # a link to a different firm, from somewhere better than a name match
    with pg_engine.begin() as conn:
        conn.execute(text("""
            UPDATE pe_funds SET firm_id = (SELECT id FROM pe_firms WHERE crd_number = '9999'),
                   firm_link_method = 'manual' WHERE cik = '0000000101'"""))
        stats = pe_funds_sec.build(conn, today=date(2026, 9, 20))
    assert stats["disagreed"] == 1
    with pg_engine.connect() as conn:
        row = conn.execute(text(
            "SELECT firm_id, firm_link_method FROM pe_funds WHERE cik = '0000000101'")).one()
    assert row[0] != alpha_id and row[1] == "manual"   # left alone, not downgraded


@pg
def test_dry_run_reports_without_writing(pg_engine):
    """The ship gate runs this before anything lands."""
    from sqlalchemy import text
    from app.marts import pe_funds_sec

    _seed_funds(pg_engine)
    with pg_engine.begin() as conn:
        stats = pe_funds_sec.build(conn, today=date(2026, 9, 20), dry_run=True)
    assert stats["dry_run"] == 1
    assert stats["candidates"] == 2 and stats["linked_adv_exact"] == 1
    assert (stats["inserted"], stats["updated"]) == (0, 0)
    with pg_engine.connect() as conn:
        assert conn.execute(text("SELECT COUNT(*) FROM pe_funds")).scalar() == 0
        assert conn.execute(text("SELECT to_regclass('stg.mart_pe_funds')")).scalar() is None


@pg
def test_master_fund_name_alone_does_not_attribute(pg_engine):
    """T16 against real rows: a feeder named after its master stays unlinked."""
    from sqlalchemy import text
    from app.marts import adv_private_funds, pe_funds_sec

    _seed_funds(pg_engine)
    with pg_engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO form_d_issuers (accession_number, cik, entity_name, is_primary)
            VALUES ('acc-3','0000000303','HARBOR POINT MASTER FUND LP', true)"""))
        conn.execute(text("""
            INSERT INTO form_d_offerings (accession_number, investment_fund_type,
                date_of_first_sale, is_indefinite, file_num)
            VALUES ('acc-3','Private Equity Fund','2025-05-01', false, '021-3')"""))
        # Alpha reports a feeder whose MASTER is the Harbor Point fund
        _fund_filing(conn, 1, "805-2", "ALPHA FEEDER LP",
                     master_name="HARBOR POINT MASTER FUND LP")
        adv_private_funds.build(conn, today=date(2026, 9, 20))
        stats = pe_funds_sec.build(conn, today=date(2026, 9, 20))

    assert stats["refused_master_name_only"] == 1
    with pg_engine.connect() as conn:
        row = conn.execute(text(
            "SELECT firm_id, firm_link_method FROM pe_funds WHERE cik = '0000000303'")).one()
    assert row == (None, None)
