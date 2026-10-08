"""
SPEC_162 — healthcare data readiness + gold set (PLAN_100 step 1).

Unit tests run offline. PG tests need TEST_PG_URL pointing at a DISPOSABLE database
(they drop and recreate cms_medicare_utilization / nppes_providers in public, and use a
throwaway schema for the gold fixtures).
"""
import asyncio
import importlib.util
import json
import os
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import httpx
import pytest

PG_URL = os.environ.get("TEST_PG_URL")
pg = pytest.mark.skipif(not PG_URL, reason="TEST_PG_URL not set")

ROOT = Path(__file__).resolve().parents[1]
MIGRATION = ROOT / "alembic" / "versions" / "0022_cms_utilization_year_key.py"
STATE_COL = "Rndrng_Prvdr_State_Abrvtn"
KEY = ("data_year", "rndrng_npi", "hcpcs_cd", "place_of_srvc")

# live column lists (information_schema, 2026-10-08); data_year is added by 0022
LIVE_COLUMNS = {
    "nppes_providers": (
        "npi entity_type legal_name first_name last_name credential dba_name gender practice_address_line1 "
        "practice_address_line2 practice_city practice_state practice_zip practice_phone practice_fax "
        "mailing_address_line1 mailing_address_line2 mailing_city mailing_state mailing_zip taxonomy_code "
        "taxonomy_description taxonomy_license taxonomy_state enumeration_date last_updated status "
        "sole_proprietor organization_subpart ingestion_timestamp").split(),
    "cms_medicare_utilization": (
        "id ingestion_timestamp data_year rndrng_npi rndrng_prvdr_last_org_name rndrng_prvdr_first_name "
        "rndrng_prvdr_mi rndrng_prvdr_crdntls rndrng_prvdr_gndr rndrng_prvdr_ent_cd rndrng_prvdr_st1 "
        "rndrng_prvdr_st2 rndrng_prvdr_city rndrng_prvdr_state_abrvtn rndrng_prvdr_state_fips "
        "rndrng_prvdr_zip5 rndrng_prvdr_ruca rndrng_prvdr_ruca_desc rndrng_prvdr_cntry rndrng_prvdr_type "
        "rndrng_prvdr_mdcr_prtcptg_ind hcpcs_cd hcpcs_desc hcpcs_drug_ind place_of_srvc tot_benes tot_srvcs "
        "tot_bene_day_srvcs avg_sbmtd_chrg avg_mdcr_alowd_amt avg_mdcr_pymt_amt avg_mdcr_stdzd_amt").split(),
    "cms_hospitals": (
        "id facility_id facility_name address city state zip_code county hospital_type ownership "
        "emergency_services overall_rating mortality_rating readmission_rating patient_experience_rating "
        "effectiveness_rating timeliness_rating imaging_rating ingested_at").split(),
}


def _migration():
    spec = importlib.util.spec_from_file_location("m0022", MIGRATION)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# ---------------------------------------------------------------------------
# 1. data_year + unique key (metadata, ingest helpers, migration text)
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestDataYear:
    def test_pinned_versions_and_default_year(self):
        from app.sources.cms import metadata

        meta = metadata.get_dataset_metadata("medicare_utilization")
        assert meta["dkan_series_id"] == "92396110-2aed-4d63-a6a2-5d6207d46a29"
        assert meta["dkan_series_id"] not in meta["dkan_versions"].values()
        assert metadata.dkan_version_for_year("medicare_utilization", None) == (
            2024, "335e5f35-eca6-482d-87b3-f99883e213e3")
        assert metadata.dkan_version_for_year("medicare_utilization", 2023) == (
            2023, "0e9f2f2b-7bf9-451a-912c-e02e654dd725")
        with pytest.raises(ValueError, match="known years"):
            metadata.dkan_version_for_year("medicare_utilization", 2031)

    def test_fresh_table_ddl_has_year_and_key(self):
        from app.sources.cms import metadata

        sql = metadata.generate_create_table_sql("medicare_utilization")
        assert "data_year INTEGER NOT NULL" in sql
        assert ("CONSTRAINT uq_cms_medicare_utilization_year_key UNIQUE "
                "(data_year, rndrng_npi, hcpcs_cd, place_of_srvc)") in sql
        # the other CMS tables are unchanged
        assert "CONSTRAINT" not in metadata.generate_create_table_sql("drug_pricing")

    def test_static_ddl_matches_the_column_map(self):
        """The literal DDL (read by the column dictionary) and the ingest's column map agree."""
        from app.catalog.dictionary_build import parse_create_tables
        from app.sources.cms import metadata

        cols = parse_create_tables(metadata.UTILIZATION_TABLE_DDL)["cms_medicare_utilization"]
        meta = metadata.get_dataset_metadata("medicare_utilization")["columns"]
        assert list(cols) == ["id", "ingestion_timestamp"] + list(meta)
        for name, info in meta.items():
            assert cols[name]["pg_type"] == info["type"].lower(), name
        assert cols["data_year"]["nullable"] is False

    def test_dedupe_on_key_last_wins_and_nulls_equal(self):
        from app.sources.cms.ingest import _dedupe_on_key

        rows = [
            {"data_year": 2024, "rndrng_npi": "1", "hcpcs_cd": "A", "place_of_srvc": "O", "v": 1},
            {"data_year": 2024, "rndrng_npi": "1", "hcpcs_cd": "A", "place_of_srvc": "O", "v": 2},
            {"data_year": 2024, "rndrng_npi": "1", "hcpcs_cd": "A", "place_of_srvc": None, "v": 3},
            {"data_year": 2024, "rndrng_npi": "1", "hcpcs_cd": "A", "place_of_srvc": None, "v": 4},
            {"data_year": 2023, "rndrng_npi": "1", "hcpcs_cd": "A", "place_of_srvc": "O", "v": 5},
        ]
        out = _dedupe_on_key(rows, KEY)
        assert sorted(r["v"] for r in out) == [2, 4, 5]

    def test_migration_revision_chain_and_guards(self):
        m = _migration()
        assert m.revision == "0022_cms_utilization_year_key"
        assert m.down_revision == "0021_ats_link_precision"
        sql = m.upgrade_sql()
        assert "to_regclass('public.cms_medicare_utilization') IS NULL" in sql
        assert "lock_timeout" in sql and "lock_not_available" in sql
        assert "uq_cms_medicare_utilization_year_key" in sql
        assert "DATE '2026-05-21'" in sql
        assert "row_number() OVER" in sql  # one pass, not a 40k x 40k self-join

    def test_single_alembic_head(self):
        heads = set()
        downs = set()
        for p in (ROOT / "alembic" / "versions").glob("*.py"):
            text = p.read_text(encoding="utf-8")
            import re
            rev = re.search(r'^revision(?::\s*str)?\s*=\s*"([^"]+)"', text, re.M)
            down = re.search(r'^down_revision(?:[^=]*)=\s*"([^"]+)"', text, re.M)
            if rev:
                heads.add(rev.group(1))
            if down:
                downs.add(down.group(1))
        assert heads - downs == {"0022_cms_utilization_year_key"}


# ---------------------------------------------------------------------------
# 2. catalog
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestCatalog:
    def test_utilization_text_states_years_and_grain(self):
        from app.catalog import get_spec

        s = get_spec("cms_medicare_utilization")
        assert "2023" in s.description and "2024" in s.description
        assert s.primary_key == KEY
        assert "data year" in s.grain
        assert s.coverage_sql and "data_year" in s.coverage_sql
        assert not any(lim.startswith("BUG-CMS-IDEMPOTENT: no data_year") for lim in s.limitations)

    def test_cms_hospitals_catalogued_with_family_rights(self):
        from app.catalog import get_spec
        from app.catalog.rights import SOURCE_RIGHTS, rights_for

        s = get_spec("cms_hospitals")
        assert s.tables == ("cms_hospitals",)
        assert s.primary_key == ("facility_id",)
        assert s.producer == "api:cms_hospitals"
        assert s.kind == "reference" and s.data_state == "ok" and s.verified_at == "2026-10-08"
        assert any("BUG-CMS-HOSP-GEO" in lim for lim in s.limitations)
        # no rights.py edit (SPEC_163 owns it): the cms family default applies
        assert rights_for("cms_hospitals", "cms") is SOURCE_RIGHTS["cms"]
        assert s.license == SOURCE_RIGHTS["cms"].license


# ---------------------------------------------------------------------------
# 3. semantic types + dictionary
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestSemanticTypes:
    def test_new_types_registered_as_join_keys(self):
        from app.catalog.identifiers import join_types, normalize_expr

        for t in ("ccn", "pac_id", "nucc_taxonomy", "hcpcs", "icd10cm", "npi"):
            assert t in join_types(), t
        assert normalize_expr("ccn", "h.facility_id") == "lpad(upper(trim(h.facility_id::text)),6,'0')"
        assert normalize_expr("hcpcs", "u.hcpcs_cd") == "upper(trim(u.hcpcs_cd::text))"
        assert normalize_expr("icd10cm", "d") == "upper(replace(trim(d::text),'.',''))"
        assert normalize_expr("pac_id", "x").startswith("lpad(regexp_replace(x::text")

    def test_dictionary_tags_healthcare_columns(self):
        body = json.loads((ROOT / "app" / "catalog" / "columns.generated.json").read_text(encoding="utf-8"))
        cols = {t: {c["name"]: c for c in body["tables"][t]["columns"]}
                for t in ("nppes_providers", "cms_medicare_utilization", "cms_hospitals")}
        assert cols["nppes_providers"]["npi"]["semantic_type"] == "npi"
        assert cols["nppes_providers"]["taxonomy_code"]["semantic_type"] == "nucc_taxonomy"
        assert cols["nppes_providers"]["gender"]["pii"] == "personal"
        assert cols["cms_medicare_utilization"]["rndrng_npi"]["semantic_type"] == "npi"
        assert cols["cms_medicare_utilization"]["hcpcs_cd"]["semantic_type"] == "hcpcs"
        assert "data_year" in cols["cms_medicare_utilization"]
        assert cols["cms_hospitals"]["facility_id"]["semantic_type"] == "ccn"
        for t, live in LIVE_COLUMNS.items():
            assert set(cols[t]) == set(live), t
            assert all(c["description"] for c in cols[t].values()), t


# ---------------------------------------------------------------------------
# 4. NPPES parser + backfill (offline parts)
# ---------------------------------------------------------------------------


def _registry_result(npi, kind="NPI-2", **basic):
    b = {"enumeration_date": "2010-01-01", "last_updated": "2024-01-01", "status": "A"}
    if kind == "NPI-2":
        b.update(organization_name=f"TEST ORG {npi}", organizational_subpart="YES")
    else:
        b.update(first_name="TESTX", last_name="PERSON", credential="MD", sex="F", sole_proprietor="NO")
    b.update(basic)
    return {
        "number": npi, "enumeration_type": kind, "basic": b,
        "other_names": [{"code": "3", "type": "Doing Business As", "organization_name": f"DBA {npi}"}]
        if kind == "NPI-2" else [{"code": "1", "type": "Former Name", "first_name": "X"}],
        "addresses": [{"address_purpose": "LOCATION", "address_1": "1 TEST WAY", "city": "TESTVILLE",
                       "state": "WY", "postal_code": "826010000", "telephone_number": "307-555-0100"}],
        "taxonomies": [{"code": "225X00000X", "desc": "Occupational Therapist", "primary": True,
                        "state": "WY", "license": "OT-1"}],
    }


@pytest.mark.unit
class TestNppesParse:
    def test_parser_reads_registry_v21_keys(self):
        from app.sources.nppes.metadata import parse_provider_record

        org = parse_provider_record(_registry_result("1234567893"))
        assert org["organization_subpart"] == "YES"
        assert org["dba_name"] == "DBA 1234567893"
        ind = parse_provider_record(_registry_result("1234567894", kind="NPI-1"))
        assert ind["gender"] == "F" and ind["dba_name"] is None  # a former name is not a DBA
        assert ind["entity_type"] == "1" and ind["taxonomy_code"] == "225X00000X"

    def test_backfill_cli_defaults_to_dry_run(self):
        from app.sources.nppes.backfill import build_parser

        a = build_parser().parse_args([])
        assert a.dry_run and not a.apply and a.sample == 3 and a.rps == 1.0
        a = build_parser().parse_args(["--apply", "--limit", "5", "--rps", "9"])
        assert a.apply and a.limit == 5
        with pytest.raises(SystemExit):
            build_parser().parse_args(["--apply", "--dry-run"])

    def test_upsert_is_null_preserving(self):
        from app.sources.nppes import metadata
        from app.sources.nppes.backfill import upsert_sql

        sql = upsert_sql()
        assert "ON CONFLICT (npi) DO UPDATE" in sql
        for c in metadata.COLUMN_NAMES:
            if c != "npi":
                assert f"{c} = COALESCE(EXCLUDED.{c}, nppes_providers.{c})" in sql


# ---------------------------------------------------------------------------
# 5. gold set (offline)
# ---------------------------------------------------------------------------

GROUNDABLE_13 = ["CQ01", "CQ02", "CQ04", "CQ05", "CQ08", "CQ11", "CQ12", "CQ14", "CQ15", "CQ18",
                 "CQ19", "CQ23", "CQ25"]


@pytest.mark.unit
class TestGold:
    def test_manifest_pins_every_file(self):
        from app.ontology import gold

        assert gold.verify() == []
        m = gold.read_manifest()
        assert set(m["files"]) == {"column_map.json", "cq_gold.sql", "cq_labels.json", "cq_probes.sql",
                                   "cq_fixtures/seed.sql", "cq_fixtures/expected.json"}
        assert len(m["gold_sha256"]) == 64

    def test_hash_detects_a_change(self, tmp_path, monkeypatch):
        import shutil

        from app.ontology import gold

        dst = tmp_path / "healthcare_provider"
        shutil.copytree(gold.domain_dir(), dst)
        monkeypatch.setattr(gold, "GOLD_ROOT", tmp_path)
        assert gold.verify() == []
        p = dst / "cq_gold.sql"
        p.write_text(p.read_text(encoding="utf-8") + "\n-- edited\n", encoding="utf-8")
        problems = gold.verify()
        assert "changed file: cq_gold.sql" in problems and any("gold_sha256" in x for x in problems)
        # CRLF checkouts hash like LF
        q = dst / "column_map.json"
        q.write_bytes(q.read_bytes().replace(b"\r\n", b"\n").replace(b"\n", b"\r\n"))
        assert "changed file: column_map.json" not in gold.verify()

    def test_gold_sql_covers_the_13_and_binds_only(self):
        import re

        from app.ontology import gold

        sql = gold.load_gold_sql()
        assert sorted(sql) == GROUNDABLE_13
        exp = gold.load_expected()["cases"]
        assert sorted(exp) == GROUNDABLE_13
        for cq, q in sql.items():
            q = " ".join(line for line in q.splitlines() if not line.lstrip().startswith("--"))
            assert "{" not in q and "%" not in q, cq
            assert not re.search(r"\b(insert|update|delete|drop|alter|create|truncate)\b", q, re.I), cq
            binds = set(re.findall(r"(?<!:):([a-z_]+)", q))
            for case in exp[cq]:
                assert set(case["params"]) == binds, cq

    def test_column_map_covers_every_live_column(self):
        from app.ontology import gold

        cmap = gold.load_column_map()
        for t, live in LIVE_COLUMNS.items():
            assert set(cmap["tables"][t]["columns"]) == set(live), t
        uni = gold.column_universe()
        per = {t: sum(1 for u in uni if u.startswith(t + ".")) for t in LIVE_COLUMNS}
        assert per == {"nppes_providers": 29, "cms_medicare_utilization": 30, "cms_hospitals": 17}
        assert len(gold.column_universe(include_schema_only=True)) == 76 + 22
        for t, spec in cmap["tables"].items():
            for c, v in spec["columns"].items():
                if not v.get("in_universe"):
                    assert v.get("reason"), (t, c)
                    continue
                assert v["match"] in ("exact", "close", "broad", "narrow", "related", "none"), (t, c)
                assert v["transform"] in ("none", "trim", "upper", "lower", "lpad_10", "zip5", "cast_date",
                                          "cast_int", "cast_numeric", "yn_to_bool"), (t, c)
                for target in v["fhir"]:
                    assert target[0].isupper() and " " not in target, (t, c, target)
                for target in v["omop"]:
                    assert target.split(".")[0].isupper(), (t, c, target)
                assert (v["match"] == "none") == (not v["fhir"] and not v["omop"]), (t, c)

    def test_labels_match_probe_rules(self):
        from app.ontology import gold
        from app.ontology.gold import probes

        doc = gold.load_labels()
        assert len(doc["results"]) == 30 and len(probes.load_probes()) == 30 and len(probes.RULES) == 30
        labels = {}
        for r in doc["results"]:
            lab, _ = probes.label(r["cq"], r["metrics"])
            assert lab == r["label"], r["cq"]
            labels[r["cq"]] = lab
        assert probes.tally(labels) == doc["tally"]
        assert doc["tally"] == {"HOLD": 3, "PARTIAL": 7, "NEED": 17, "SCHEMA": 3, "groundable": 10}
        assert doc["gold_cqs"] == GROUNDABLE_13

    def test_probe_rules_on_post_apply_metrics(self):
        from app.ontology.gold import probes

        lab, why = probes.label("CQ18", {"n": 29189, "npis": 3216, "has_allowed": 29189, "has_paid": 29189,
                                          "has_year_key": 1})
        assert lab == "PARTIAL" and "0022" not in why
        assert probes.label("CQ08", {"n_org": 10, "has_subpart_flag": 9, "parent_columns": 0})[0] == "PARTIAL"
        assert probes.label("CQ16", {"network_tables": 0})[0] == "NEED"
        assert probes.label("CQ28", {"patient_tables": 1})[1].count("WARNING") == 1


# ---------------------------------------------------------------------------
# PG — migration 0022 on a populated table with duplicates
# ---------------------------------------------------------------------------


def _engine():
    from sqlalchemy import create_engine

    return create_engine(PG_URL)


def _old_table_sql():
    """cms_medicare_utilization as it is live before 0022: no data_year, no key."""
    from app.sources.cms import metadata

    cols = metadata.get_dataset_metadata("medicare_utilization")["columns"]
    defs = ["id SERIAL PRIMARY KEY", "ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP"]
    defs += [f"{c} {v['type']}" for c, v in cols.items() if c != "data_year"]
    return "CREATE TABLE cms_medicare_utilization (" + ", ".join(defs) + ")"


def _seed_live_like(conn):
    from sqlalchemy import text

    rows = [
        # 2023 data (loaded 2026-02-20): every key loaded twice, the later copy differs
        ("2026-02-20 20:24:46", "1003000126", "99221", "F", 12),
        ("2026-02-20 20:25:00", "1003000126", "99221", "F", 13),
        ("2026-02-20 20:24:46", "1003000126", "99222", "F", 22),
        ("2026-02-20 20:24:47", "1003000126", "99222", "F", 22),
        ("2026-02-25 02:50:46", "1003000126", "99222", "F", 23),   # the 02-25 rerun: latest wins
        ("2026-02-20 20:24:46", "1003000134", "G0438", None, 5),   # NULL place of service, twice
        ("2026-02-20 20:24:48", "1003000134", "G0438", None, 6),
        # 2024 data (loaded 2026-09-25), shares a key with 2023: kept, different year
        ("2026-09-25 18:39:59", "1003000126", "99221", "F", 36),
        ("2026-09-25 18:39:59", "1003029653", "97110", "O", 662),
    ]
    for ts, npi, hcpcs, pos, srv in rows:
        conn.execute(text(
            "INSERT INTO cms_medicare_utilization (ingestion_timestamp, rndrng_npi, hcpcs_cd, place_of_srvc, "
            "tot_srvcs, rndrng_prvdr_state_abrvtn) VALUES (:ts, :n, :h, :p, :s, 'WY')"),
            {"ts": ts, "n": npi, "h": hcpcs, "p": pos, "s": srv})


@pg
def test_m1_migration_dedupes_backfills_and_adds_key():
    from sqlalchemy import text

    m = _migration()
    eng = _engine()
    with eng.begin() as c:
        c.execute(text("DROP TABLE IF EXISTS cms_medicare_utilization"))
        c.execute(text(_old_table_sql()))
        _seed_live_like(c)
    with eng.begin() as c:
        c.execute(text(m.upgrade_sql()))
    with eng.begin() as c:
        got = c.execute(text(
            "SELECT data_year, rndrng_npi, hcpcs_cd, place_of_srvc, tot_srvcs FROM cms_medicare_utilization "
            "ORDER BY 1, 2, 3, 4")).fetchall()
        assert [tuple(r[:4]) + (float(r[4]),) for r in got] == [
            (2023, "1003000126", "99221", "F", 13.0),
            (2023, "1003000126", "99222", "F", 23.0),
            (2023, "1003000134", "G0438", None, 6.0),
            (2024, "1003000126", "99221", "F", 36.0),
            (2024, "1003029653", "97110", "O", 662.0),
        ]
        nullable = c.execute(text(
            "SELECT is_nullable FROM information_schema.columns WHERE table_name = 'cms_medicare_utilization' "
            "AND column_name = 'data_year'")).scalar()
        assert nullable == "NO"
        assert c.execute(text("SELECT count(*) FROM pg_constraint WHERE conname = "
                              "'uq_cms_medicare_utilization_year_key'")).scalar() == 1
    # idempotent: a second run changes nothing and does not fail
    with eng.begin() as c:
        c.execute(text(m.upgrade_sql()))
        assert c.execute(text("SELECT count(*) FROM cms_medicare_utilization")).scalar() == 5
    # the key now refuses a duplicate
    with pytest.raises(Exception):
        with eng.begin() as c:
            c.execute(text("INSERT INTO cms_medicare_utilization (data_year, rndrng_npi, hcpcs_cd, place_of_srvc) "
                           "VALUES (2024, '1003029653', '97110', 'O')"))
    with eng.begin() as c:
        c.execute(text(m.downgrade_sql()))
        assert c.execute(text(
            "SELECT count(*) FROM information_schema.columns WHERE table_name = 'cms_medicare_utilization' "
            "AND column_name = 'data_year'")).scalar() == 0
        c.execute(text("DROP TABLE cms_medicare_utilization"))
        # no table (fresh database): a no-op, not an error
        c.execute(text(m.upgrade_sql()))
        c.execute(text(m.downgrade_sql()))
    eng.dispose()


@pg
def test_m2_lock_timeout_gives_up_instead_of_queueing():
    from sqlalchemy import text

    m = _migration()
    eng = _engine()
    with eng.begin() as c:
        c.execute(text("DROP TABLE IF EXISTS cms_medicare_utilization"))
        c.execute(text(_old_table_sql()))
    holder = eng.connect()
    tx = holder.begin()
    holder.execute(text("LOCK TABLE cms_medicare_utilization IN ACCESS EXCLUSIVE MODE"))
    try:
        t0 = time.monotonic()
        with pytest.raises(Exception, match="lock"):
            with eng.begin() as c:
                c.execute(text(m.upgrade_sql(lock_timeout="100ms", attempts=2, sleep_s=0)))
        assert time.monotonic() - t0 < 10
    finally:
        tx.rollback()
        holder.close()
    with eng.begin() as c:
        c.execute(text("DROP TABLE IF EXISTS cms_medicare_utilization"))
    eng.dispose()


# ---------------------------------------------------------------------------
# PG — ingest writes data_year and stays idempotent on the key
# ---------------------------------------------------------------------------


def _dkan_handler(rows_by_state, seen_ids=None):
    def handler(request: httpx.Request) -> httpx.Response:
        if seen_ids is not None:
            seen_ids.append(urlparse(str(request.url)).path)
        q = parse_qs(urlparse(str(request.url)).query)
        state = (q.get(f"filter[{STATE_COL}]") or [None])[0]
        size, offset = int(q["size"][0]), int(q["offset"][0])
        return httpx.Response(200, json=rows_by_state.get(state, [])[offset: offset + size])
    return handler


def _util_rows(state, n, dup_every=0):
    rows = []
    for i in range(n):
        rows.append({"Rndrng_NPI": f"10000{i:05d}", STATE_COL: state, "HCPCS_Cd": "G0438",
                     "Place_Of_Srvc": "O", "Tot_Srvcs": str(i)})
        if dup_every and i % dup_every == 0:  # the API repeats a key (overlapping pages)
            rows.append(dict(rows[-1], Tot_Srvcs=str(i + 1000)))
    return rows


@pytest.fixture
def util_db():
    from sqlalchemy import text
    from sqlalchemy.orm import sessionmaker

    from app.core.models import DatasetRegistry, IngestionJob

    eng = _engine()
    with eng.begin() as c:
        c.execute(text("DROP TABLE IF EXISTS cms_medicare_utilization"))
    for model in (IngestionJob, DatasetRegistry):
        model.__table__.create(eng, checkfirst=True)
    db = sessionmaker(bind=eng)()
    yield db
    db.close()
    with eng.begin() as c:
        c.execute(text("DROP TABLE IF EXISTS cms_medicare_utilization"))
    eng.dispose()


def _ingest(db, data, year=None, states=("WY",), seen=None):
    from app.sources.cms.client import CMSClient
    from app.sources.cms.ingest import ingest_medicare_utilization

    async def run():
        c = CMSClient(transport=httpx.MockTransport(_dkan_handler(data, seen)), requests_per_second=0)
        try:
            return await ingest_medicare_utilization(db=db, job_id=0, year=year, states=list(states),
                                                     client=c, page_size=10)
        finally:
            await c.close()

    return asyncio.run(run())


@pg
def test_i1_ingest_writes_year_and_never_duplicates(util_db):
    from sqlalchemy import text

    data = {"WY": _util_rows("WY", 23, dup_every=5)}  # 23 keys, 5 repeated
    seen = []
    r = _ingest(util_db, data, seen=seen)
    assert r["data_year"] == 2024 and r["dataset_id"] == "335e5f35-eca6-482d-87b3-f99883e213e3"
    assert all("335e5f35-eca6-482d-87b3-f99883e213e3" in p for p in seen)  # never the series id

    def counts():
        return dict(util_db.execute(text(
            "SELECT data_year, count(*) FROM cms_medicare_utilization GROUP BY 1")).fetchall())

    assert counts() == {2024: 23}
    # the repeated key kept the later value (last wins)
    assert float(util_db.execute(text(
        "SELECT tot_srvcs FROM cms_medicare_utilization WHERE rndrng_npi = '1000000000'")).scalar()) == 1000.0
    _ingest(util_db, data)                       # rerun: replaced, not duplicated
    assert counts() == {2024: 23}
    _ingest(util_db, {"WY": _util_rows("WY", 4)}, year=2023)   # another year: both kept
    assert counts() == {2023: 4, 2024: 23}
    _ingest(util_db, {"WY": _util_rows("WY", 2)}, year=2023)   # 2023 rerun leaves 2024 alone
    assert counts() == {2023: 2, 2024: 23}


@pg
def test_i2_unknown_year_and_unmigrated_table_fail_before_writing(util_db):
    from sqlalchemy import text

    with pytest.raises(ValueError, match="known years"):
        _ingest(util_db, {"WY": _util_rows("WY", 3)}, year=2031)
    # a pre-0022 table (no key): refuse, do not delete or insert anything
    with util_db.get_bind().begin() as c:
        c.execute(text("DROP TABLE IF EXISTS cms_medicare_utilization"))
        c.execute(text(_old_table_sql()))
        c.execute(text("INSERT INTO cms_medicare_utilization (rndrng_npi, rndrng_prvdr_state_abrvtn) "
                       "VALUES ('1', 'WY')"))
    with pytest.raises(RuntimeError, match="0022"):
        _ingest(util_db, {"WY": _util_rows("WY", 3)})
    assert util_db.execute(text("SELECT count(*) FROM cms_medicare_utilization")).scalar() == 1


# ---------------------------------------------------------------------------
# PG — NPI backfill dry run / apply through an httpx MockTransport
# ---------------------------------------------------------------------------


@pytest.fixture
def backfill_db(monkeypatch):
    from sqlalchemy import text
    from sqlalchemy.orm import sessionmaker

    from app.sources.nppes import metadata

    monkeypatch.delenv("WORKER_MODE", raising=False)  # no shared bucket in tests
    eng = _engine()
    with eng.begin() as c:
        c.execute(text("DROP TABLE IF EXISTS cms_medicare_utilization"))
        c.execute(text("DROP TABLE IF EXISTS nppes_providers"))
        c.execute(text(metadata.CREATE_TABLE_SQL))
        c.execute(text("CREATE TABLE cms_medicare_utilization (id SERIAL PRIMARY KEY, rndrng_npi TEXT)"))
        for npi in ("1000000001", "1000000002", "1000000003", "1000000004", "1000000004", "BADNPI"):
            c.execute(text("INSERT INTO cms_medicare_utilization (rndrng_npi) VALUES (:n)"), {"n": npi})
        c.execute(text("INSERT INTO nppes_providers (npi, entity_type, taxonomy_license) "
                       "VALUES ('1000000001', '1', 'KEEP-ME')"))
    db = sessionmaker(bind=eng)()
    yield db
    db.close()
    with eng.begin() as c:
        c.execute(text("DROP TABLE IF EXISTS cms_medicare_utilization"))
        c.execute(text("DROP TABLE IF EXISTS nppes_providers"))
    eng.dispose()


def _registry_client(log):
    from app.sources.nppes.client import NPPESClient

    known = {"1000000002": _registry_result("1000000002"),
             "1000000004": _registry_result("1000000004", kind="NPI-1")}

    def handler(request):
        q = parse_qs(urlparse(str(request.url)).query)
        npi = q["number"][0]
        log.append((npi, time.monotonic(), request.headers.get("user-agent", "")))
        assert q["version"] == ["2.1"]
        hit = known.get(npi)
        return httpx.Response(200, json={"result_count": 1 if hit else 0, "results": [hit] if hit else []})

    c = NPPESClient(max_concurrency=1, max_retries=1)
    c._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return c


@pg
def test_b1_backfill_dry_run_writes_nothing(backfill_db):
    from sqlalchemy import text

    from app.sources.nppes.backfill import run_backfill

    log = []
    client = _registry_client(log)
    report = asyncio.run(run_backfill(backfill_db, sample=2, rps=2.0, client=client))
    asyncio.run(client.close())
    assert report["mode"] == "dry_run"
    assert report["utilization_npis"] == 5 and report["missing_before"] == 3   # BADNPI ignored
    assert [n for n, _, _ in log] == ["1000000002", "1000000003"]               # sample only
    assert report["fetched"] == 1 and report["not_found"] == 1 and report["upserted"] == 0
    assert report["sample"][0]["npi"] == "1000000002"
    assert "NexdataResearch" in log[0][2]
    assert backfill_db.execute(text("SELECT count(*) FROM nppes_providers")).scalar() == 1


@pg
def test_b2_backfill_apply_paced_null_preserving_resumable(backfill_db):
    from sqlalchemy import text

    from app.sources.nppes.backfill import null_preserving_upsert, run_backfill

    log = []
    client = _registry_client(log)
    report = asyncio.run(run_backfill(backfill_db, apply=True, batch=1, rps=2.0, client=client))
    assert report["requested"] == 3 and report["upserted"] == 2 and report["not_found_npis"] == ["1000000003"]
    assert report["missing_after"] == 1
    gaps = [b - a for (_, a, _), (_, b, _) in zip(log, log[1:])]
    assert all(g >= 0.45 for g in gaps), gaps                                    # <= 2 req/s
    row = backfill_db.execute(text(
        "SELECT organization_subpart, dba_name FROM nppes_providers WHERE npi = '1000000002'")).one()
    assert tuple(row) == ("YES", "DBA 1000000002")
    # re-run: only the NPI the Registry does not know is asked again
    log.clear()
    asyncio.run(run_backfill(backfill_db, apply=True, rps=2.0, client=client))
    asyncio.run(client.close())
    assert [n for n, _, _ in log] == ["1000000003"]
    # null-preserving: a sparse answer never blanks a held column
    null_preserving_upsert(backfill_db, [{"npi": "1000000001", "entity_type": "1", "credential": "MD"}])
    backfill_db.commit()
    row = backfill_db.execute(text(
        "SELECT taxonomy_license, credential FROM nppes_providers WHERE npi = '1000000001'")).one()
    assert tuple(row) == ("KEEP-ME", "MD")


# ---------------------------------------------------------------------------
# PG — gold SQL on the seed fixtures, probes on the seed
# ---------------------------------------------------------------------------


def _norm(v):
    from datetime import date
    from decimal import Decimal

    if isinstance(v, (date, datetime)):
        return v.isoformat()
    if isinstance(v, Decimal):
        return round(float(v), 6)
    if isinstance(v, float):
        return round(v, 6)
    return v


@pytest.fixture
def gold_conn():
    from sqlalchemy import text

    from app.ontology import gold

    eng = _engine()
    seed = (gold.domain_dir() / "cq_fixtures" / "seed.sql").read_text(encoding="utf-8")
    with eng.begin() as c:
        c.execute(text("DROP SCHEMA IF EXISTS gold_fx CASCADE"))
        c.execute(text("CREATE SCHEMA gold_fx"))
        c.exec_driver_sql("SET search_path TO gold_fx")
        c.exec_driver_sql(seed)
    conn = eng.connect()
    conn.exec_driver_sql("SET search_path TO gold_fx")
    conn.commit()
    yield conn
    conn.close()
    with eng.begin() as c:
        c.execute(text("DROP SCHEMA IF EXISTS gold_fx CASCADE"))
    eng.dispose()


@pg
def test_g1_gold_sql_answers_match_fixtures(gold_conn):
    from sqlalchemy import text

    from app.ontology import gold

    sql = gold.load_gold_sql()
    for cq, cases in gold.load_expected()["cases"].items():
        for case in cases:
            rows = gold_conn.execute(text(sql[cq]), case["params"]).mappings().all()
            got = [{k: _norm(v) for k, v in r.items()} for r in rows]
            want = [{k: _norm(v) for k, v in r.items()} for r in case["rows"]]
            assert got == want, (cq, case["params"])
    gold_conn.rollback()


@pg
def test_g2_probes_run_read_only_on_the_seed(gold_conn):
    from sqlalchemy import text

    from app.ontology.gold import probes

    results = {r["cq"]: r for r in probes.run_probes(gold_conn)}
    assert len(results) == 30
    assert results["CQ05"]["label"] == "HOLD" and results["CQ05"]["metrics"]["sharing_individuals"] == 3
    assert results["CQ08"]["label"] == "PARTIAL"   # flags set, no parent column
    assert results["CQ23"]["label"] == "PARTIAL"   # one exact name match
    assert results["CQ25"]["label"] == "HOLD"
    assert results["CQ18"]["label"] == "PARTIAL"   # key present, but a sample
    with pytest.raises(Exception, match="read-only"):
        gold_conn.execute(text("INSERT INTO nucc_taxonomy (code) VALUES ('X')"))
    gold_conn.rollback()
