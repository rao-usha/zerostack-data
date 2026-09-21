"""
Tests for SPEC 119 — GP people from Form D related persons.

The pure name tests carry the measured constraints: each of them corresponds
to a specific number of real people that the naive version of the rule
destroys, and those numbers are in the docstrings so a future edit that
"simplifies" the rule fails loudly.

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


# ---------------------------------------------------------------------------
# name cleaning (pure)
# ---------------------------------------------------------------------------

@pytest.mark.unit
class TestNames:
    def test_entity_tokens_match_whole_words_only(self):
        """T2: a substring test costs 600 real people.

        `LIKE '%co%'` takes Michael Collins (311 rows) and Scott Voss; `%lp%`
        takes Volpert and Alpern; `%inc%` takes Vincent and Reyna.
        """
        from app.marts.names import looks_like_entity

        for first, last in [("Michael", "Collins"), ("Scott", "Voss"),
                            ("Andrew", "Volpert"), ("Dan", "Alpern"),
                            ("Vincent", "Reyna"), ("Howard", "Marks"),
                            ("Bill", "Gross"), ("Tom", "Banks"),
                            ("Jane", "Price"), ("Ed", "Young")]:
            assert looks_like_entity(first, last)[0] is False, f"{first} {last} is a person"

        for first, last in [("LLC", "Fund GP,"), ("Belltower Fund Group,", "Ltd."),
                            ("Sydecar", "LLC"), ("Forge Global Advisors", "LLC"),
                            ("", "Maples Fiduciary Services Inc")]:
            assert looks_like_entity(first, last)[0] is True, f"{first} {last} is an entity"

    def test_dotted_legal_forms_are_glued_per_field(self):
        """T3: `L.L.C.` must glue; `Brian C. O'Connor` must not."""
        from app.marts.names import looks_like_entity

        is_ent, only_glue = looks_like_entity("Acme Holdings", "L.L.C.")
        assert is_ent is True
        # 'holdings' is a plain token too, so this is not glue-only
        assert only_glue is False

        is_ent, only_glue = looks_like_entity("Riverbend", "L.P.")
        assert (is_ent, only_glue) == (True, True), "caught only by the glue rule"

        # the cross-field hazard: C. + O'Connor must never glue into 'co'
        assert looks_like_entity("Brian", "O'Connor")[0] is False

    def test_middle_name_never_refuses(self):
        """T4: every tuple flagged by a middle-field token alone is a real
        person with a corrupted middle field."""
        from app.marts.names import looks_like_entity, name_norm

        assert looks_like_entity("Anthony", "Cusano")[0] is False
        # and the middle field never reaches the identity key either
        assert name_norm("Anthony", "Cusano") == "anthony cusano"

    def test_placeholder_names_refused_and_rescued(self):
        """T5: 'N/A | Belltower…' is noise; '- | Brandon Green' is a person."""
        from app.marts.names import is_placeholder, rescue_name_in_last_field

        assert is_placeholder("N/A", "Belltower Fund Group, Ltd.") is True
        assert is_placeholder("--", "Fund GP, LLC") is True
        assert is_placeholder("Michael", "Collins") is False

        assert rescue_name_in_last_field("-", "Brandon Green") == ("brandon", "green")
        # an entity in the last field is not rescued
        assert rescue_name_in_last_field("N/A", "Fund GP") is None
        # nor is a three-token blob
        assert rescue_name_in_last_field("N/A", "David B. Singer") is None

    def test_name_norm_excludes_middle_and_does_not_strip_surnames(self):
        """T6 support: norm.core() would mangle `Ltd`-shaped surnames."""
        from app.marts.names import name_norm, token_count_ok

        assert name_norm("José", "Álvarez") == "jose alvarez"
        assert name_norm("Mary-Jane", "O'Brien") == "mary jane o brien"
        assert token_count_ok(name_norm("Mary-Jane", "O'Brien")) is True
        assert token_count_ok(name_norm("Corentin", "Du Roy De Blicquy")) is False
        assert token_count_ok("cher") is False

    def test_display_name_is_raw_cased_and_stripped(self):
        """full_name is NOT NULL and user-facing: never the normalized key."""
        from app.marts.names import display_name

        assert display_name("Michael", None, "Collins") == "Michael Collins"
        assert display_name(" John ", "M.", "Toomey\n") == "John M. Toomey"


# ---------------------------------------------------------------------------
# tiers (pure)
# ---------------------------------------------------------------------------

@pytest.mark.unit
class TestTiers:
    def test_title_is_sorted_union_not_latest(self):
        """T7: 4% of pairs change their relationship set between filings."""
        from app.marts.pe_people_sec import _title

        assert _title([["Executive Officer"], ["Director"]]) == "Director, Executive Officer"
        # order of filings must not matter
        assert _title([["Director"], ["Executive Officer"]]) == "Director, Executive Officer"
        assert _title([["Promoter", "Director"]]) == "Director, Promoter"
        assert _title([["Executive Officer"]]) == "Executive Officer"

    def test_brand_collapses_on_first_token(self):
        """T9: Blackstone's four registrations are one brand.

        A two-token collapse makes them four, which convicts John Finley and
        Christopher Striano of appearing at 4 'different' firms.
        """
        from app.marts.pe_people_sec import _brand

        blackstone = ["Blackstone Growth", "Blackstone Life Sciences",
                      "Blackstone Management Partners", "Blackstone Tactical Opportunities"]
        assert len({_brand(n) for n in blackstone}) == 1
        assert _brand("TPG Angelo Gordon") == _brand("TPG Capital Advisors")
        assert _brand("HarbourVest Partners, LLC") == "harbourvest"


# ---------------------------------------------------------------------------
# Postgres
# ---------------------------------------------------------------------------

def _upgrade_sql():
    path = REPO / "alembic" / "versions" / "0011_pe_people_sec.py"
    spec = importlib.util.spec_from_file_location("mig0011", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod.UPGRADE_SQL


@pytest.fixture
def pg_engine():
    from sqlalchemy import create_engine, text

    engine = create_engine(PG_URL)
    with engine.begin() as conn:
        for t in ("pe_firm_people", "pe_people", "pe_funds", "pe_firms",
                  "form_d_related_persons", "form_d_issuers", "form_d_filings",
                  "sec_adv_roster_snapshots"):
            conn.execute(text(f"DROP TABLE IF EXISTS public.{t} CASCADE"))
        conn.execute(text("""
            CREATE TABLE pe_firms (id SERIAL PRIMARY KEY, name TEXT NOT NULL,
                crd_number TEXT)"""))
        conn.execute(text("""
            CREATE TABLE pe_funds (id SERIAL PRIMARY KEY, firm_id INTEGER, name TEXT,
                cik TEXT, final_close_usd_millions NUMERIC, firm_link_method VARCHAR(32))"""))
        conn.execute(text("""
            CREATE TABLE pe_people (id SERIAL PRIMARY KEY, full_name VARCHAR NOT NULL,
                first_name VARCHAR, last_name VARCHAR, middle_name VARCHAR,
                city VARCHAR, state VARCHAR, country VARCHAR, is_active BOOLEAN,
                data_sources JSON, last_verified DATE, updated_at TIMESTAMP)"""))
        conn.execute(text("""
            CREATE TABLE pe_firm_people (id SERIAL PRIMARY KEY,
                firm_id INTEGER NOT NULL REFERENCES pe_firms(id),
                person_id INTEGER NOT NULL REFERENCES pe_people(id),
                title VARCHAR NOT NULL, seniority VARCHAR, department VARCHAR,
                is_current BOOLEAN, role_type VARCHAR, updated_at TIMESTAMP)"""))
        conn.execute(text("""
            CREATE TABLE form_d_related_persons (accession_number TEXT,
                related_person_seq INTEGER, first_name TEXT, middle_name TEXT,
                last_name TEXT, city TEXT, state_or_country TEXT, zip_code TEXT,
                relationships TEXT[], relationship_clarification TEXT)"""))
        conn.execute(text("""
            CREATE TABLE form_d_issuers (accession_number TEXT, cik TEXT,
                entity_name TEXT, is_primary BOOLEAN)"""))
        conn.execute(text("""
            CREATE TABLE form_d_filings (accession_number TEXT, cik TEXT,
                filed_at TIMESTAMP)"""))
        conn.execute(text("""
            CREATE TABLE sec_adv_roster_snapshots (crd_number TEXT, roster_date DATE,
                main_office_postal_code TEXT, main_office_state TEXT)"""))
        for stmt in _upgrade_sql():
            conn.execute(text(stmt))
    yield engine
    engine.dispose()


def _seed(engine):
    from sqlalchemy import text

    with engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO pe_firms (id, name, crd_number) VALUES "
            "(1,'HARBOURVEST PARTNERS, LLC','109846'), (2,'BLACKSTONE GROWTH','111111'), "
            "(3,'BLACKSTONE LIFE SCIENCES','222222')"))
        conn.execute(text("SELECT setval('pe_firms_id_seq', 100)"))
        conn.execute(text(
            "INSERT INTO sec_adv_roster_snapshots VALUES "
            "('109846','2026-06-01','02110','MA')"))
        conn.execute(text(
            "INSERT INTO pe_funds (firm_id, name, cik, firm_link_method) VALUES "
            "(1,'HarbourVest X','0000000001','adv_exact'), "
            "(2,'Blackstone Growth I','0000000002','adv_exact'), "
            "(3,'Blackstone Life I','0000000003','adv_exact')"))
        for cik in ("0000000001", "0000000002", "0000000003"):
            conn.execute(text("INSERT INTO form_d_issuers VALUES (:a,:c,'x',TRUE)"),
                         {"a": f"acc-{cik}", "c": cik})
            conn.execute(text("INSERT INTO form_d_filings VALUES (:a,:c,'2025-03-04')"),
                         {"a": f"acc-{cik}", "c": cik})
        # a real GP at HarbourVest, at the adviser's own zip
        conn.execute(text(
            "INSERT INTO form_d_related_persons VALUES "
            "('acc-0000000001',1,'John','M.','Toomey','Boston','MA','02110',"
            "ARRAY['Executive Officer'],NULL)"))
        # an entity row on the same filing -- must be refused, not written
        conn.execute(text(
            "INSERT INTO form_d_related_persons VALUES "
            "('acc-0000000001',2,'LLC',NULL,'Fund GP,','Boston','MA','02110',"
            "ARRAY['Director'],NULL)"))
        # one person at two Blackstone registrations: two rows, one brand
        for acc, seq in (("acc-0000000002", 1), ("acc-0000000003", 1)):
            conn.execute(text(
                "INSERT INTO form_d_related_persons VALUES "
                "(:a,:s,'John','G.','Finley','New York','NY','10154',"
                "ARRAY['Executive Officer','Director'],NULL)"),
                {"a": acc, "s": seq})


@pg
def test_build_writes_people_and_links_and_refuses_entities(pg_engine):
    """T1/T6/T11/T15: the ledger closes, identity is name+firm, links resolve."""
    from sqlalchemy import text

    from app.marts import pe_people_sec

    _seed(pg_engine)
    with pg_engine.begin() as conn:
        stats = pe_people_sec.build(conn, today=date(2026, 9, 20))

    assert stats["base_rows"] == 4
    assert stats["refused_entity_name"] == 1          # the 'LLC | Fund GP,' row
    assert stats["kept"] == 3
    # T1: the ledger closes exactly, or build() would have raised
    assert stats["kept"] + stats["refused_entity_name"] == stats["base_rows"]

    with pg_engine.connect() as conn:
        people = conn.execute(text(
            "SELECT full_name, source_key, state FROM pe_people ORDER BY source_key"
        )).fetchall()
        links = conn.execute(text(
            "SELECT p.full_name, f.name, l.title, l.person_link_method, "
            "       l.firm_link_method, l.address_confirmation, l.is_current, "
            "       l.fund_count, l.first_seen "
            "FROM pe_firm_people l JOIN pe_people p ON p.id = l.person_id "
            "JOIN pe_firms f ON f.id = l.firm_id ORDER BY f.name"
        )).fetchall()

    # T6: John Finley at two registrations is two rows, not one
    assert len(people) == 3
    assert [p[0] for p in people] == ["John M. Toomey", "John G. Finley", "John G. Finley"]
    assert people[0][1] == "secformd:1:john toomey"

    assert len(links) == 3
    by_firm = {r[1]: r for r in links}
    hv = by_firm["HARBOURVEST PARTNERS, LLC"]
    assert hv[2] == "Executive Officer"
    assert hv[3] == "form_d_signer"
    assert hv[4] == "adv_exact"
    assert hv[5] == "zip5"                 # T11: confirmed against the ADV office
    assert hv[6] is True                   # is_current written explicitly
    assert hv[7] == 1
    assert hv[8] == date(2025, 3, 4)       # T12: from filed_at, not NOW()

    bg = by_firm["BLACKSTONE GROWTH"]
    assert bg[2] == "Director, Executive Officer"   # T7: sorted union
    assert bg[5] == "unknown"              # no ADV address for this firm


@pg
def test_build_is_idempotent(pg_engine):
    """T13: a second run must change nothing."""
    from app.marts import pe_people_sec

    _seed(pg_engine)
    with pg_engine.begin() as conn:
        pe_people_sec.build(conn, today=date(2026, 9, 20))
    with pg_engine.begin() as conn:
        again = pe_people_sec.build(conn, today=date(2026, 9, 20))

    assert again["inserted_people"] == 0
    assert again["updated_people"] == 0
    assert again["inserted_links"] == 0
    assert again["updated_links"] == 0


@pg
def test_per_pair_picks_are_deterministic(pg_engine):
    """T17: the same person is spelled several ways across their filings.

    Taking whichever row arrived first made `full_name`, `city` and `state`
    depend on Postgres's row order, so a rerun on unchanged data reported 557
    people updated forever. The latest accession wins, not the first arrival.
    """
    from sqlalchemy import text

    from app.marts import pe_people_sec

    _seed(pg_engine)
    with pg_engine.begin() as conn:
        # the same human, spelled differently on a later filing from a later
        # accession, with a different city
        conn.execute(text("INSERT INTO form_d_issuers VALUES ('acc-zzz','0000000001','x',TRUE)"))
        conn.execute(text("INSERT INTO form_d_filings VALUES ('acc-zzz','0000000001','2026-01-09')"))
        conn.execute(text(
            "INSERT INTO form_d_related_persons VALUES "
            "('acc-zzz',1,'John',NULL,'Toomey','Cambridge','MA','02139',"
            "ARRAY['Director'],NULL)"))

    with pg_engine.begin() as conn:
        pe_people_sec.build(conn, today=date(2026, 9, 20))
    with pg_engine.connect() as conn:
        row = conn.execute(text(
            "SELECT full_name, city FROM pe_people WHERE source_key = 'secformd:1:john toomey'"
        )).one()
    # 'acc-zzz' sorts above 'acc-0000000001', so the later filing's spelling wins
    assert row == ("John Toomey", "Cambridge")

    with pg_engine.begin() as conn:
        again = pe_people_sec.build(conn, today=date(2026, 9, 20))
    assert (again["inserted_people"], again["updated_people"]) == (0, 0)
    assert (again["inserted_links"], again["updated_links"]) == (0, 0)


@pg
def test_existing_people_are_untouched(pg_engine):
    """T14: the 2,551 legacy rows carry no source_key and must not move."""
    from sqlalchemy import text

    from app.marts import pe_people_sec

    _seed(pg_engine)
    with pg_engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO pe_people (full_name, first_name, last_name) "
            "VALUES ('John Toomey','John','Toomey')"))      # same name, no source_key
    with pg_engine.begin() as conn:
        stats = pe_people_sec.build(conn, today=date(2026, 9, 20))

    assert stats["name_collides_existing_person"] == 1
    assert stats["collides_same_firm"] == 0
    with pg_engine.connect() as conn:
        legacy = conn.execute(text(
            "SELECT full_name, source_key FROM pe_people WHERE source_key IS NULL"
        )).fetchall()
    assert legacy == [("John Toomey", None)]


@pg
def test_dry_run_writes_nothing(pg_engine):
    from sqlalchemy import text

    from app.marts import pe_people_sec

    _seed(pg_engine)
    with pg_engine.begin() as conn:
        stats = pe_people_sec.build(conn, today=date(2026, 9, 20), dry_run=True)
    assert stats["candidate_pairs"] == 3
    assert stats["dry_run"] == 1
    with pg_engine.connect() as conn:
        assert conn.execute(text("SELECT count(*) FROM pe_people")).scalar() == 0


@pg
def test_spv_platform_rule_rejects_zero_size_gps(pg_engine):
    """T16: Madison Dearborn reports $0 sold and must not be flagged.

    Fund size alone classifies Madison Dearborn, Vista Equity and Andreessen
    Horowitz as platforms, because Form D reports nothing sold at launch. The
    >=60 fund floor is what stops it.
    """
    from sqlalchemy import text

    from app.marts import pe_people_sec

    with pg_engine.begin() as conn:
        conn.execute(text("INSERT INTO pe_firms (id,name) VALUES (10,'MADISON DEARBORN'),"
                          "(11,'EQUITYBEE ADVISORS'),(12,'HARBOURVEST')"))
        conn.execute(text("SELECT setval('pe_firms_id_seq', 100)"))
        # a real GP with few, zero-reported funds
        for i in range(21):
            conn.execute(text("INSERT INTO pe_funds (firm_id,cik,final_close_usd_millions,"
                              "firm_link_method) VALUES (10,:c,0,'adv_exact')"),
                         {"c": f"md{i:08d}"})
        # a platform: many funds, none large
        for i in range(70):
            conn.execute(text("INSERT INTO pe_funds (firm_id,cik,final_close_usd_millions,"
                              "firm_link_method) VALUES (11,:c,0.5,'adv_exact')"),
                         {"c": f"eb{i:08d}"})
        # a real GP with many funds and one big raise
        for i in range(70):
            conn.execute(text("INSERT INTO pe_funds (firm_id,cik,final_close_usd_millions,"
                              "firm_link_method) VALUES (12,:c,:v,'adv_exact')"),
                         {"c": f"hv{i:08d}", "v": 900 if i == 0 else 0})

    with pg_engine.connect() as conn:
        found = pe_people_sec.find_spv_platform_firms(conn)

    assert found == {11}, "only the many-funds, never-large firm is a platform"
