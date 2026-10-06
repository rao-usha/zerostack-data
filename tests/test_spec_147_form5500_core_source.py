"""
Tests for SPEC 147 — Form 5500 sponsors as an entity-resolver source.

EIN is a strong key; name+state is a WEAK tier that is recorded, never merged.
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
# feeds: EIN normalization and the dol5500 row
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestDol5500Feed:
    def test_ein9_pads_and_refuses(self):
        """T1"""
        from app.entities.feeds import _ein9

        assert _ein9("012345678") == "012345678"  # leading zero kept
        assert _ein9(12345678) == "012345678"  # int lost its zero: restored
        assert _ein9("12345678") == "012345678"  # digit string of 8: restored
        assert _ein9("01-2345678") == "012345678"  # hyphen form
        assert _ein9(" 261640968 ") == "261640968"
        assert _ein9(None) is None
        assert _ein9("") is None
        assert _ein9("000000000") is None  # sentinel
        assert _ein9("999999999") is None  # repeated digit placeholder
        assert _ein9("1234") is None  # too short to be an EIN
        assert _ein9("1234567890") is None  # too long
        assert _ein9("12-34x") is None

    def test_dol5500_row_keyed_on_ein(self):
        """T2"""
        from app.entities import feeds

        row = feeds._row(
            feeds.DOL5500,
            {
                "native_id": "12345678",
                "ein": "12345678",
                "legal_name": "ACME DENTAL, LLC",
                "state": "ca",
                "zip5": "90210",
                "observed_at": None,
            },
        )
        rec = dict(zip(feeds.COLUMN_NAMES, row))
        assert rec["record_key"] == "dol5500:012345678"
        assert rec["source"] == "dol5500"
        assert rec["native_id"] == "012345678"
        assert rec["ein"] == "012345678"
        assert rec["state"] == "CA"
        assert rec["zip5"] == "90210"
        assert rec["name_norm"] == "acme dental"
        assert rec["cik"] is None and rec["crd"] is None

    def test_dol5500_row_without_valid_ein_dropped(self):
        """T3"""
        from app.entities import feeds

        for bad in (None, "", "000000000", "abc"):
            assert feeds._row(feeds.DOL5500, {"native_id": bad, "ein": bad, "legal_name": "X", "state": "CA"}) is None
        # zip5 that is not 5 digits is not stored
        row = feeds._row(
            feeds.DOL5500, {"native_id": "261640968", "ein": "261640968", "legal_name": "X", "zip5": "9021"}
        )
        assert dict(zip(feeds.COLUMN_NAMES, row))["zip5"] is None


# ---------------------------------------------------------------------------
# the weak name+state tier (pure)
# ---------------------------------------------------------------------------


def _corpus():
    """A sponsor joined to EDGAR by EIN, an unmatched sponsor with a name twin,
    an unmatched sponsor whose twin carries a different EIN."""
    return [
        # strong: sponsor 1 shares EIN with edgar:1
        _rec("dol5500:111111112", ein="111111112", legal_name="Acme Dental LLC", name_norm="acme dental", state="CA"),
        _rec(
            "edgar:0000000001",
            cik="1001",
            ein="111111112",
            legal_name="ACME DENTAL INC",
            name_norm="acme dental",
            state="CA",
        ),
        _rec("formd:0000000001", cik="1001", legal_name="ACME DENTAL INC", name_norm="acme dental", state="CA"),
        # unmatched sponsor; Form D twin with no EIN -> clean candidate
        _rec("dol5500:222222223", ein="222222223", legal_name="Bolt Labs", name_norm="bolt labs", state="NY"),
        _rec("formd:0000000002", cik="1002", legal_name="BOLT LABS INC", name_norm="bolt labs", state="NY"),
        # unmatched sponsor; EDGAR twin carries a DIFFERENT EIN -> conflict
        _rec("dol5500:333333334", ein="333333334", legal_name="Cobalt Co", name_norm="cobalt", state="TX"),
        _rec("edgar:0000000003", cik="1003", ein="444444445", legal_name="COBALT CO", name_norm="cobalt", state="TX"),
    ]


def _weak(records):
    from app.entities.resolve_core import plan, weak_name_state

    p = plan(records, [], {})
    return p, weak_name_state(records, p["record_keys"], p["components"])


def _status(weak, rk):
    return {(r["record_key"], r["candidate_record_key"]): r for r in weak["rows"] if r["record_key"] == rk}


@pytest.mark.unit
class TestWeakTier:
    def test_weak_never_merges(self):
        """T4: name+state twins change no component."""
        from app.entities.resolve_core import plan

        recs = _corpus()
        p, weak = _weak(recs)
        keyed_only = [
            r for r in recs if r["record_key"] in {"dol5500:111111112", "edgar:0000000001", "formd:0000000001"}
        ]
        assert [c["members"] for c in p["components"]] == [c["members"] for c in plan(keyed_only, [], {})["components"]]
        assert weak["rows"]  # the tier did find candidates...
        # ...and still the unmatched sponsors are in no component
        members = {m for c in p["components"] for m in c["members"]}
        assert "dol5500:222222223" not in members
        assert "dol5500:333333334" not in members

    def test_weak_candidate_for_unmatched_sponsor(self):
        """T5"""
        _p, weak = _weak(_corpus())
        rows = _status(weak, "dol5500:222222223")
        row = rows[("dol5500:222222223", "formd:0000000002")]
        assert row["status"] == "candidate"
        assert row["conflicts"] == []
        assert row["matched_on"] == {"name_norm": "bolt labs", "state": "NY"}
        assert row["candidate_comp"] is None  # the Form D twin is a keyed singleton

    def test_weak_ein_conflict(self):
        """T6"""
        _p, weak = _weak(_corpus())
        row = _status(weak, "dol5500:333333334")[("dol5500:333333334", "edgar:0000000003")]
        assert row["status"] == "conflict"
        assert row["conflicts"] == [
            {"reason": "ein_conflict", "sponsor_ein": "333333334", "candidate_eins": ["444444445"]}
        ]

    def test_weak_corroborates_strong_match(self):
        """T7: a twin in the sponsor's own component corroborates; dol5500 records
        are never candidates for each other."""
        p, weak = _weak(_corpus())
        rows = _status(weak, "dol5500:111111112")
        assert {k[1] for k in rows} == {"edgar:0000000001", "formd:0000000001"}
        assert {r["status"] for r in rows.values()} == {"corroborates"}
        comp_idx = rows[("dol5500:111111112", "edgar:0000000001")]["candidate_comp"]
        assert "dol5500:111111112" in p["components"][comp_idx]["members"]

    def test_weak_strong_match_elsewhere(self):
        """T8"""
        recs = _corpus() + [
            _rec("adv:77", crd="7701", legal_name="Acme Dental", name_norm="acme dental", state="CA"),
            _rec("iapd:77", crd="7701", legal_name="Acme Dental", name_norm="acme dental", state="CA"),
        ]
        _p, weak = _weak(recs)
        row = _status(weak, "dol5500:111111112")[("dol5500:111111112", "adv:77")]
        assert row["status"] == "conflict"
        assert [c["reason"] for c in row["conflicts"]] == ["strong_match_elsewhere"]
        assert weak["metrics"]["sponsors_by_status"]["corroborates"] == 1  # best status wins

    def test_weak_ambiguous_and_cap(self):
        """T9"""
        from app.entities.resolve_core import WEAK_CANDIDATE_CAP

        recs = [_rec("dol5500:555555556", ein="555555556", legal_name="Delta", name_norm="delta", state="FL")]
        recs += [
            _rec(f"formd:{i:010d}", cik=str(100 + i), legal_name="DELTA", name_norm="delta", state="FL")
            for i in range(WEAK_CANDIDATE_CAP + 3)
        ]
        _p, weak = _weak(recs)
        rows = _status(weak, "dol5500:555555556")
        assert len(rows) == WEAK_CANDIDATE_CAP
        assert {r["status"] for r in rows.values()} == {"ambiguous"}
        assert weak["metrics"]["sponsors_over_cap"] == 1
        assert weak["metrics"]["candidates_over_cap"] == 3
        # two identities, under the cap: ambiguous too
        _p, weak2 = _weak(recs[:3])
        assert {r["status"] for r in weak2["rows"]} == {"ambiguous"}

    def test_weak_needs_name_and_state(self):
        """T10"""
        recs = [
            _rec("dol5500:666666667", ein="666666667", legal_name="Echo", name_norm="echo", state=None),
            _rec("formd:0000000010", cik="1010", legal_name="ECHO", name_norm="echo", state="WA"),
            _rec("dol5500:777777778", ein="777777778", legal_name="Foxtrot", name_norm="foxtrot", state="OR"),
            _rec("formd:0000000011", cik="1011", legal_name="FOXTROT", name_norm="foxtrot", state="WA"),
        ]
        _p, weak = _weak(recs)
        assert weak["rows"] == []
        assert weak["metrics"]["sponsors_considered"] == 1  # only the one with name AND state

    def test_dol5500_metrics(self):
        """T11"""
        from app.entities.resolve_core import plan, source_metrics, weak_name_state

        recs = _corpus() + [
            _rec("dol5500:888888889", ein="888888889", legal_name="Lonely", name_norm="lonely", state="ME"),
            # strong EIN match whose name and state disagree: recorded, not resolved
            _rec(
                "dol5500:999999998", ein="999999998", legal_name="Zulu Holdings", name_norm="zulu holdings", state="NM"
            ),
            _rec(
                "edgar:0000000009",
                cik="1009",
                ein="999999998",
                legal_name="ZULU CORP",
                name_norm="zulu corp",
                state="NV",
            ),
        ]
        p = plan(recs, [], {})
        weak = weak_name_state(recs, p["record_keys"], p["components"])
        m = source_metrics(recs, p["record_keys"], p["components"], weak, new_components=[0])
        assert m["records_fed"] == 5
        assert m["with_ein_key"] == 5
        assert m["matched_strong_ein"] == 2
        assert m["unmatched_singletons"] == 3
        assert m["in_new_entities"] == 1
        assert m["weak_sponsors_by_status"] == {"corroborates": 1, "candidate": 1, "conflict": 1}
        assert m["unmatched_with_weak_candidate"] == 2
        # Acme agrees on name and state; Zulu disagrees on both -> counted per field
        assert m["strong_components_with_field_conflicts"] == {"canonical_state": 1, "name_norm_distinct>1": 1}
        assert m["strong_components"] == 2


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


SOURCE_TABLES = (
    "sec_adv_roster_snapshots",
    "sec_adv_feed_firm_state",
    "sec_13f_filings",
    "sec_13f_other_managers",
    "form_d_issuers",
    "sec_filers",
    "sec_8k_index",
    "sec_insider_owners",
    "sec_insider_filings",
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
        conn.execute(
            text("""CREATE TABLE sec_adv_roster_snapshots (crd_number TEXT, roster_date DATE,
            legal_name TEXT, business_name TEXT, main_office_state TEXT, website TEXT)""")
        )
        conn.execute(
            text("""CREATE TABLE sec_adv_feed_firm_state (crd_number TEXT, edition_date DATE,
            legal_name TEXT, business_name TEXT, main_office_state TEXT, website TEXT)""")
        )
        conn.execute(
            text("""CREATE TABLE sec_13f_filings (accession_number TEXT, cik TEXT,
            crd_number TEXT, filing_manager_name TEXT, filing_manager_state_or_country TEXT,
            filing_date DATE)""")
        )
        conn.execute(text("CREATE TABLE sec_13f_other_managers (accession_number TEXT, cik TEXT, crd_number TEXT)"))
        conn.execute(
            text("""CREATE TABLE form_d_issuers (cik TEXT, entity_name TEXT,
            state_or_country TEXT, loaded_at TIMESTAMP DEFAULT NOW())""")
        )
        conn.execute(
            text("""CREATE TABLE sec_filers (cik TEXT, name TEXT, ein TEXT, lei TEXT,
            state_of_incorporation TEXT, biz_state2 TEXT, biz_state_or_country TEXT, website TEXT,
            loaded_at TIMESTAMP DEFAULT NOW())""")
        )
        conn.execute(text("CREATE TABLE sec_8k_index (cik TEXT)"))
        conn.execute(text("CREATE TABLE sec_insider_owners (rptowner_cik TEXT)"))
        conn.execute(text("CREATE TABLE sec_insider_filings (issuer_cik TEXT)"))
        # SEC side: one Form D issuer whose EDGAR row carries an EIN; one EDGAR filer that is
        # in NO PE-relevant source but carries a sponsor EIN (scope expansion)
        conn.execute(
            text("""INSERT INTO form_d_issuers (cik, entity_name, state_or_country)
            VALUES ('0000000042', 'ACME DENTAL INC', 'CA'),
                   ('0000000043', 'BOLT LABS INC', 'NY')""")
        )
        conn.execute(
            text("""INSERT INTO sec_filers (cik, name, ein, state_of_incorporation, biz_state2)
            VALUES ('0000000042', 'Acme Dental Inc', '012345678', 'CA', 'CA'),
                   ('0000000043', 'Bolt Labs Inc', NULL, 'NY', 'NY'),
                   ('0000000777', 'Zed Holdings Inc', '261640968', 'DE', 'DE'),
                   ('0000000888', 'Unrelated Inc', '999888777', 'TX', 'TX')""")
        )
    yield engine
    engine.dispose()


def _make_workbench(engine):
    from sqlalchemy import text

    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA workbench"))
        conn.execute(
            text("""CREATE TABLE workbench.dol5500_sponsor (ein TEXT, form_year INTEGER,
            sponsor_name TEXT, state TEXT, zip5 TEXT, loaded_at TIMESTAMPTZ DEFAULT NOW(),
            PRIMARY KEY (ein, form_year))""")
        )
        conn.execute(
            text("""INSERT INTO workbench.dol5500_sponsor (ein, form_year, sponsor_name, state, zip5)
            VALUES ('012345678', 2025, 'ACME DENTAL LLC', 'CA', '90210'),
                   ('261640968', 2024, 'ZED HOLDINGS OLD NAME', 'DE', '19801'),
                   ('261640968', 2025, 'ZED HOLDINGS INC', 'DE', '19801'),
                   ('555555556', 2025, 'BOLT LABS INC', 'NY', '10001'),
                   ('000000000', 2025, 'PLACEHOLDER', 'NY', NULL)""")
        )


@pg
def test_feeds_dol5500_pg(pg_engine):
    """T12"""
    from sqlalchemy import text
    from app.entities import feeds

    _make_workbench(pg_engine)
    with pg_engine.begin() as conn:
        counts = feeds.run_feeds(conn)
    assert counts["dol5500"] == 3  # placeholder EIN never fed
    # SPEC_148: attach-only website feeds whose tables this fixture lacks are skipped too
    assert "dol5500" not in counts["skipped"]
    with pg_engine.connect() as conn:
        rows = {
            r["record_key"]: dict(r)
            for r in conn.execute(
                text("SELECT record_key, source, ein, legal_name, state, zip5, observed_at FROM core.source_record")
            ).mappings()
        }
    assert rows["dol5500:012345678"]["ein"] == "012345678"  # leading zero kept
    assert rows["dol5500:012345678"]["zip5"] == "90210"
    assert rows["dol5500:261640968"]["legal_name"] == "ZED HOLDINGS INC"  # latest form_year
    assert rows["dol5500:261640968"]["observed_at"] is not None  # provenance time
    assert "dol5500:000000000" not in rows
    # scope expansion: the EDGAR filer that shares a sponsor EIN is fed, the unrelated one is not
    assert "edgar:0000000777" in rows
    assert "edgar:0000000888" not in rows


@pg
def test_feeds_without_workbench_table_pg(pg_engine):
    """T13"""
    from sqlalchemy import text
    from app.entities import feeds

    with pg_engine.begin() as conn:
        counts = feeds.run_feeds(conn)
    assert counts["dol5500"] == 0
    assert "dol5500" in counts["skipped"]
    assert set(counts["skipped"]) - {"dol5500"} <= {"pefirm", "industrial", "peportco", "portco", "threepl",
                                                    "famoffice", "lpfund",
                                                    "gleif", "usasp",  # SPEC_154 feeds
                                                    "atsboard"}  # SPEC_155
    with pg_engine.connect() as conn:
        keys = {r[0] for r in conn.execute(text("SELECT record_key FROM core.source_record"))}
    assert "edgar:0000000042" in keys and "edgar:0000000777" not in keys


@pg
def test_resolve_dol5500_end_to_end_pg(pg_engine):
    """T14"""
    from sqlalchemy import text
    from app.entities import feeds, resolve

    _make_workbench(pg_engine)
    with pg_engine.begin() as conn:
        feeds.run_feeds(conn)
        m = resolve.resolve(conn)
    d = m["dol5500"]
    assert d["records_fed"] == 3
    assert d["matched_strong_ein"] == 2  # Acme (in scope) + Zed (scope expansion)
    # Acme + Zed: twins inside their own entity; Bolt: one clean identity (Form D + EDGAR, no EIN)
    assert d["weak_sponsors_by_status.corroborates"] == 2
    assert d["weak_sponsors_by_status.candidate"] == 1
    assert "weak_sponsors_by_status.conflict" not in d
    # the job ledger keeps only scalar counters: both blocks must survive compact_summary
    from app.marts.build_ledger import compact_summary

    kept = compact_summary({"resolve": m})["resolve"]
    assert kept["dol5500"] == d
    assert kept["weak"] == m["weak"]
    assert kept["weak"]["sponsors_with_candidates"] == 3
    with pg_engine.connect() as conn:
        ent = (
            conn.execute(
                text(
                    "SELECT e.entity_id, e.ein, e.cik FROM core.identifier i "
                    "JOIN core.entity e ON e.entity_id = i.entity_id "
                    "WHERE i.id_type = 'ein' AND i.id_value = '012345678'"
                )
            )
            .mappings()
            .one()
        )
        assert ent["cik"] == "42"
        members = {
            r[0]
            for r in conn.execute(
                text("SELECT record_key FROM core.membership WHERE entity_id = :e"), {"e": ent["entity_id"]}
            )
        }
        assert {"dol5500:012345678", "edgar:0000000042", "formd:0000000042"} <= members
        weak = (
            conn.execute(
                text(
                    "SELECT record_key, candidate_record_key, candidate_entity_id, tier, status, conflicts "
                    "FROM core.weak_match ORDER BY 1, 2"
                )
            )
            .mappings()
            .all()
        )
    bolt = [w for w in weak if w["record_key"] == "dol5500:555555556"]
    assert {w["candidate_record_key"] for w in bolt} == {"formd:0000000043", "edgar:0000000043"}
    assert all(w["tier"] == "name_state" for w in weak)
    # Bolt's twins are one entity (formd + edgar share CIK 43) -> a single identity, and the
    # Bolt sponsor is NOT a member of it: weak never merges
    assert {w["candidate_entity_id"] for w in bolt} != {None}
    with pg_engine.connect() as conn:
        assert (
            conn.execute(text("SELECT COUNT(*) FROM core.membership WHERE record_key = 'dol5500:555555556'")).scalar()
            == 0
        )
    # Acme's twins corroborate (same entity)
    acme = [w for w in weak if w["record_key"] == "dol5500:012345678"]
    assert acme and {w["status"] for w in acme} == {"corroborates"}


@pg
def test_resolve_dol5500_idempotent_and_dry_run_pg(pg_engine):
    """T15"""
    from sqlalchemy import text
    from app.entities import feeds, resolve

    _make_workbench(pg_engine)
    with pg_engine.begin() as conn:
        feeds.run_feeds(conn)
        dry = resolve.resolve(conn, dry_run=True)
    with pg_engine.connect() as conn:
        assert conn.execute(text("SELECT COUNT(*) FROM core.weak_match")).scalar() == 0
        assert conn.execute(text("SELECT COUNT(*) FROM core.entity")).scalar() == 0
    with pg_engine.begin() as conn:
        first = resolve.resolve(conn)
    assert first["dol5500"] == dry["dol5500"]  # dry run reports what a real run does
    assert first["weak"]["rows_written"] > 0
    with pg_engine.begin() as conn:
        feeds.run_feeds(conn)
        second = resolve.resolve(conn)
    assert second["weak"]["rows_written"] == 0
    assert second["weak"]["rows_removed"] == 0
    assert second["entities_written"] == 0


@pg
def test_migration_0016_idempotent_pg(pg_engine):
    """T16"""
    from sqlalchemy import text

    with pg_engine.begin() as conn:
        _apply_migration(conn, "0016_entity_weak_match", "UPGRADE_SQL")
        cols = {
            r[0]
            for r in conn.execute(
                text(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_schema = 'core' AND table_name = 'weak_match'"
                )
            )
        }
    assert {
        "record_key",
        "candidate_record_key",
        "candidate_entity_id",
        "tier",
        "status",
        "conflicts",
        "matched_on",
        "resolver_version",
        "updated_at",
    } <= cols


@pg
def test_merge_dissolves_absorbed_entity_pg(pg_engine):
    """T17: a later observation joining two live entities merges them. resolve.py
    called an undefined `_dissolve` on that path (latent since SPEC_116: no merge had
    ever happened), so the first real merge would have failed the whole run."""
    from sqlalchemy import text
    from app.entities import feeds, resolve

    _make_workbench(pg_engine)
    with pg_engine.begin() as conn:
        # two entities: Acme (EIN 012345678 via edgar 42 + sponsor) and Bolt (CIK 43 formd+edgar)
        feeds.run_feeds(conn)
        resolve.resolve(conn)
        ids = dict(
            conn.execute(
                text(
                    "SELECT record_key, entity_id FROM core.membership "
                    "WHERE record_key IN ('edgar:0000000042', 'edgar:0000000043')"
                )
            ).fetchall()
        )
        assert ids["edgar:0000000042"] != ids["edgar:0000000043"]
        # a new record asserting both CIKs' keys would be odd; an EIN shared by both is the
        # realistic join. SPEC_150: a shared EIN joins two CIKs only when corroborated, so
        # CIK 43's EDGAR row becomes Acme's '/ADV' duplicate filer account (rule R1)
        conn.execute(text("UPDATE sec_filers SET ein = '012345678', name = 'Acme Dental Inc /ADV' "
                          "WHERE cik = '0000000043'"))
    with pg_engine.begin() as conn:
        feeds.run_feeds(conn)
        m = resolve.resolve(conn)
    assert m["entities_merged"] == 1
    survivor, absorbed = sorted(ids.values())
    with pg_engine.connect() as conn:
        row = (
            conn.execute(
                text("SELECT dissolved_at, superseded_by FROM core.entity WHERE entity_id = :a"), {"a": absorbed}
            )
            .mappings()
            .one()
        )
        assert row["dissolved_at"] is not None and row["superseded_by"] == survivor
        assert (
            conn.execute(
                text(
                    "SELECT COUNT(*) FROM core.entity_merge WHERE absorbed_entity_id = :a AND survivor_entity_id = :s"
                ),
                {"a": absorbed, "s": survivor},
            ).scalar()
            == 1
        )
