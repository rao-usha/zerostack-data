"""
Tests for SPEC 137 — the column dictionary.

Column truth was split across seven sources and nothing gathered it; CIKs
were stored four ways and a naive join matched 0 of 1,000 rows; column PII
was invisible (sec_13f was pii_class='none' with signature names and phones).
The dictionary is generated offline into a checked-in JSON file (drift
gate), merged with live facts at request time, and drives /schema, the
masked /sample, the join-key index and COMMENT ON COLUMN.
"""
import copy
import os
import re
from pathlib import Path

import pytest

PG_URL = os.environ.get("TEST_PG_URL")
pg = pytest.mark.skipif(not PG_URL, reason="TEST_PG_URL not set")

REPO = Path(__file__).resolve().parents[1]

# dataset pii_class below its max column PII, being raised by SPEC_141 (sec_13f)
# or newly found here and reported (si_public_water_systems.admin_contact_phone,
# glassdoor_companies.ceo_name). The live offender set must stay a subset: it can only shrink.
PENDING_PII_RAISES: set = set()  # all applied after the wave A merge

# DDL section headers the old metadata regex attributed to the previous column (review finding)
SECTION_HEADERS = {"Company identifiers", "Filing metadata", "Sub-scores (each 0-100)", "Metadata",
                   "Acquisition prospect score", "Size distribution", "Derived", "Composite", "Rankings",
                   "Raw metrics", "Yelp business data (denormalized)", "Discovery metadata"}


class _Proxy:
    """A DatasetSpec with extra attributes (SPEC_141/142 fields this branch lacks)."""

    def __init__(self, spec, **extra):
        self._spec, self._extra = spec, extra

    def __getattr__(self, name):
        if name in self._extra:
            return self._extra[name]
        return getattr(self._spec, name)


@pytest.fixture(scope="module")
def built():
    from app.catalog.dictionary_build import build

    return build()


def _dict():
    from app.catalog.dictionary import load_dictionary

    return load_dictionary()


# ---------------------------------------------------------------------------
# T1 generation / drift
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestGeneration:
    def test_checked_in_file_is_current(self, built):
        """Drift gate: python -m app.catalog.dictionary_build regenerates this file."""
        from app.catalog.dictionary_build import OUT_PATH, render

        assert OUT_PATH.read_text(encoding="utf-8") == render(built), (
            "columns.generated.json is stale: run python -m app.catalog.dictionary_build")

    def test_hash_covers_content(self, built):
        from app.catalog.dictionary_build import dictionary_hash

        assert built["dictionary_hash"] == dictionary_hash(built)
        other = copy.deepcopy(built)
        other["tables"]["sec_13f_holdings"]["columns"][0]["description"] = "changed"
        assert dictionary_hash(other) != built["dictionary_hash"]

    def test_every_catalog_table_with_static_ddl_is_present(self, built):
        for t in ("sec_13f_filings", "form_d_filings", "core.identifier", "pe_firms", "sec_filers"):
            assert built["tables"][t]["origin"] != "none", t
            assert built["tables"][t]["columns"], t

    def test_precedence_curated_over_model_over_glossary(self, built):
        cols = {c["name"]: c for c in built["tables"]["sec_13f_holdings"]["columns"]}
        assert cols["value"]["source"] == "upstream" and cols["value"]["unit"] == "USD"
        assert cols["value"]["upstream_url"].startswith("https://www.sec.gov/")
        assert cols["cusip"]["source"] == "glossary" and cols["cusip"]["semantic_type"] == "cusip"
        firms = {c["name"]: c for c in built["tables"]["pe_firms"]["columns"]}
        assert firms["firm_type"]["source"] == "model"  # inline `# PE, VC, ...` comment
        # bulk COMMENT ON COLUMN inside ddl()
        f13 = {c["name"]: c for c in built["tables"]["sec_13f_filings"]["columns"]}
        assert f13["table_value_total"]["source"] == "bulk"

    def test_model_eg_comment_becomes_example(self, built):
        cols = {c["name"]: c for c in built["tables"]["pe_firms"]["columns"]}
        assert cols["sec_file_number"]["description"].startswith("SEC File Number")
        # a comment that *starts* with "e.g." is an example, not a description
        examples = [c for e in built["tables"].values() for c in e["columns"]
                    if c["example"] and c["source"] != "curated" and c["source"] != "upstream"]
        assert examples and all(not (c["description"] or "").lower().startswith("e.g") for c in examples)

    def test_metadata_description_dicts_are_harvested(self, built):
        pc = built["package_columns"]
        assert "cert" in pc["fdic"] and "npi" in pc["nppes"]
        assert "index_nsa" in pc["realestate"]  # DDL `-- comment`

    def test_ddl_section_headers_are_not_column_text(self, built):
        """A `-- header` line of its own is not the previous column's description."""
        bad = [f"{t}.{c['name']}={c['description']!r}" for t, e in built["tables"].items() for c in e["columns"]
               if c["description"] in SECTION_HEADERS]
        assert not bad, bad
        for p, cols in built["package_columns"].items():
            assert not (set(cols.values()) & SECTION_HEADERS), (p, set(cols.values()) & SECTION_HEADERS)
        ids = {t: {c["name"]: c for c in built["tables"][t]["columns"]}["id"]
               for t in ("sec_income_statement", "form_d_filings")}
        assert all(c["source"] == "glossary" and c["description"].startswith("Surrogate") for c in ids.values())

    def test_ddl_comment_line_is_single_line(self):
        from app.catalog.dictionary_build import _DDL_COMMENT_LINE

        assert _DDL_COMMENT_LINE.match("    cik VARCHAR(10), -- SEC CIK").groups() == ("cik", "SEC CIK")
        assert _DDL_COMMENT_LINE.match("    id SERIAL PRIMARY KEY,") is None
        assert _DDL_COMMENT_LINE.match("    -- Company identifiers") is None

    def test_dataset_level_dicts_are_not_column_text(self, built):
        """{"cpi": {"description", "series"}} / {"job_postings": {"table_name", ...}} describe datasets."""
        from app.catalog.dictionary_build import _description_dicts

        found = {}
        _description_dicts({"cpi": {"description": "Consumer prices", "series": {}},
                            "ces": {"description": "Employment", "series": {}}}, found)
        _description_dicts({"jobs": {"description": "Jobs", "table_name": "jobs"},
                            "snap": {"description": "Snapshots", "table_name": "snap"}}, found)
        assert found == {}
        _description_dicts({"npi": {"type": "TEXT", "description": "NPI"}, "name": {"description": "Name"}}, found)
        assert found == {"npi": "NPI", "name": "Name"}
        pc = built["package_columns"]
        assert "bls" not in pc and "job_postings" not in pc and "prediction_markets" not in pc
        assert "medicare_utilization" not in pc.get("cms", {})

    def test_curated_rows_name_real_columns(self, built):
        from app.catalog.columns_curated import CURATED

        bad = []
        for key in CURATED:
            table, _, col = key.rpartition(".")
            entry = built["tables"].get(table)
            cols = {c["name"]: c for c in (entry or {}).get("columns", [])}
            if entry is None or entry["origin"] == "none" or col not in cols or cols[col]["pg_type"] is None:
                bad.append(key)
        assert not bad, f"curated rows for unknown columns: {bad}"


# ---------------------------------------------------------------------------
# T2 vocabulary
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestVocabulary:
    @pytest.mark.parametrize("field, value", [
        ("semantic_type", "social_security"), ("pii", "secret"), ("source", "llm"), ("confidence", "certain")])
    def test_column_spec_rejects_unknown_values(self, field, value):
        from app.catalog.columns import ColumnSpec

        with pytest.raises(ValueError):
            ColumnSpec(table="t", name="c", **{field: value})

    def test_glossary_types_are_closed(self):
        from app.catalog.columns import GLOSSARY
        from app.catalog.identifiers import SEMANTIC_TYPES
        from app.catalog.spec import PII_CLASSES

        for rx, desc, st, pii, unit in GLOSSARY:
            re.compile(rx)
            assert st is None or st in SEMANTIC_TYPES, rx
            assert pii in PII_CLASSES and desc.strip(), rx

    @pytest.mark.parametrize("name, st, pii", [
        ("cik", "cik", "none"), ("issuer_cik", "cik", "none"), ("rptowner_cik", "cik", "none"),
        ("crd_number", "crd", "none"), ("naics_code", "naics", "none"), ("zip_code", "zip5", "none"),
        ("latitude", "latitude", "none"), ("lon", "longitude", "none"), ("county_fips", "fips_county", "none"),
        ("accession_number", "accession_number", "none"), ("lei", "lei", "none"),
        ("email", None, "business_contact"), ("signature_phone", None, "business_contact"),
        ("first_name", None, "business_contact"), ("filing_manager_street1", None, "business_contact"),
        ("date_of_birth", None, "personal"), ("address", None, "none"),
    ])
    def test_glossary_classification(self, name, st, pii):
        from app.catalog.columns import classify

        g = classify(name)
        assert g is not None and g["semantic_type"] == st and g["pii"] == pii

    def test_normalize_sql_templates(self):
        from app.catalog.identifiers import SEMANTIC_TYPES, join_types, normalize_expr

        assert normalize_expr("cik", "x.cik") == "lpad(ltrim(x.cik::text,'0'),10,'0')"
        assert normalize_expr("crd", "c") == "ltrim(c::text,'0')"
        assert normalize_expr("fips_county", "f") == "lpad(f::text,5,'0')"
        assert normalize_expr("naics", "n").startswith("left(regexp_replace(n::text")
        assert normalize_expr("latitude", "l") is None
        assert "us_state" not in join_types() and "cik" in join_types()
        for t in join_types().values():
            assert "{col}" in t.normalize_sql and t.specificity > 0
        with pytest.raises(ValueError):
            normalize_expr("ssn", "x")
        assert len(SEMANTIC_TYPES) == 29  # SPEC_162: ccn, pac_id, nucc_taxonomy, hcpcs, icd10cm


# ---------------------------------------------------------------------------
# T3 PII
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestPii:
    def test_pii_named_columns_are_not_none(self):
        from app.catalog.columns import pii_name_lint_applies

        body = _dict()
        bad = [f"{t}.{c['name']}" for t, e in body["tables"].items() for c in e["columns"]
               if pii_name_lint_applies(c["name"], c.get("pg_type")) and c["pii"] == "none"]
        assert not bad, f"PII-named columns tagged pii='none': {bad}"

    @pytest.mark.parametrize("table, column, pii", [
        ("people", "linkedin_url", "business_contact"),
        ("people", "twitter_url", "business_contact"),
        ("people", "linkedin_id", "business_contact"),
        ("people", "photo_url", "personal"),
        ("pe_people", "linkedin_url", "business_contact"),
        ("pe_people", "twitter_url", "business_contact"),
        ("family_offices", "principal_name", "business_contact"),
        ("family_offices", "principal_family", "business_contact"),
        ("oc_officers", "name", "business_contact"),
        ("leadership_changes", "person_name", "business_contact"),
        ("glassdoor_companies", "ceo_name", "business_contact"),
        ("public_water_system", "admin_contact_name", "business_contact"),
        ("uspto_inventors", "name_first", "business_contact"),
        ("uspto_inventors", "name_last", "business_contact"),
    ])
    def test_person_columns_the_old_rules_missed(self, table, column, pii):
        cols = {c["name"]: c for c in _dict()["tables"][table]["columns"]}
        assert cols[column]["pii"] == pii

    def test_lint_covers_person_names_and_profile_links(self):
        from app.catalog.columns import pii_name_lint_applies

        for n in ("name_first", "name_last", "linkedin_url", "twitter_url", "photo_url", "principal_name",
                  "person_name", "admin_contact_name", "ceo_name", "officer_name"):
            assert pii_name_lint_applies(n, "text"), n
        assert not pii_name_lint_applies("website", "text")

    def test_personal_dataset_denies_by_default(self):
        """In a 'personal' dataset a column the rules miss is masked; identifiers, numbers,
        dates and clearly non-person columns are not."""
        from app.catalog import get_catalog, get_spec
        from app.catalog.schema_live import effective_pii

        w = [s for s in get_catalog() if s.pii_class == "personal"][:1]
        assert w

        def eff(name, pg="text", st=None, pii="none", table=None):
            return effective_pii({"name": name, "pg_type": pg, "semantic_type": st, "pii": pii}, w, table)

        assert eff("nickname") == "personal" and eff("name") == "personal" and eff("bio") == "personal"
        assert eff("email", pii="business_contact") == "personal"
        assert eff("id", "integer") == "none" and eff("joined", "date") == "none"
        assert eff("cik", st="cik") == "none" and eff("company_name") == "none" and eff("title") == "none"
        assert eff("status") == "none" and eff("seniority_level") == "none"
        assert eff("email_confidence") == "none"
        # a non-personal dataset keeps the column tag
        assert effective_pii({"name": "nickname", "pg_type": "text", "pii": "none"},
                             [get_spec("sec_13f")]) == "none"

    def test_curated_pii_none_opts_out(self, monkeypatch):
        from app.catalog import columns_curated, get_catalog
        from app.catalog.schema_live import effective_pii

        w = [s for s in get_catalog() if s.pii_class == "personal"][:1]
        monkeypatch.setitem(columns_curated.CURATED, "t137_x.nickname", {"pii": "none", "source": "curated"})
        col = {"name": "nickname", "pg_type": "text", "pii": "none"}
        assert effective_pii(col, w, "t137_x") == "none" and effective_pii(col, w, "t137_y") == "personal"

    def test_storage_forbidden_gate(self):
        from app.catalog import get_spec
        from app.catalog.schema_live import storage_forbidden

        s = get_spec("sec_13f")
        assert not storage_forbidden([s])
        assert storage_forbidden([s, _Proxy(s, storage="forbidden")])
        assert storage_forbidden([_Proxy(s, commercial_use="forbidden")])
        assert not storage_forbidden([_Proxy(s, storage="allowed", commercial_use="allowed")])

    def test_natural_key_needs_an_index(self):
        from app.catalog import get_spec
        from app.catalog.schema_live import _natural_key

        s = _Proxy(get_spec("sec_13f"), primary_key=("accession_number",))
        cols = [{"name": n} for n in ("accession_number", "x")]
        t = s.tables[0]
        assert _natural_key(s, t, {"columns": cols, "unique_keys": []}) == []
        assert _natural_key(s, t, {"columns": cols, "unique_keys": [
            {"columns": ["x"], "primary": False}, {"columns": ["accession_number"], "primary": True}]}) == [
            "accession_number"]
        assert _natural_key(s, "other", {"columns": cols,
                                         "unique_keys": [{"columns": ["x"], "primary": True}]}) == ["x"]

    def test_lint_ignores_non_contact_types(self):
        from app.catalog.columns import pii_name_lint_applies

        assert pii_name_lint_applies("signature_name", "text")
        assert not pii_name_lint_applies("signature_date", "date")
        assert not pii_name_lint_applies("has_email", "boolean")
        assert not pii_name_lint_applies("email_confidence", "varchar(20)")

    def test_dataset_pii_class_covers_its_columns(self):
        """PLAN_088: dataset pii_class >= max column PII (would have caught sec_13f)."""
        from app.catalog import get_catalog
        from app.catalog.columns import PII_RANK
        from app.catalog.dictionary import dataset_column_pii

        offenders = {}
        for s in get_catalog():
            top, cols = dataset_column_pii(s)
            if PII_RANK[top] > PII_RANK[s.pii_class]:
                offenders[s.key] = (s.pii_class, top, cols[:3])
        assert set(offenders) <= PENDING_PII_RAISES, offenders

    def test_sec_13f_signature_is_business_contact(self):
        from app.catalog import get_spec
        from app.catalog.dictionary import dataset_column_pii

        top, cols = dataset_column_pii(get_spec("sec_13f"))
        assert top == "business_contact" and "sec_13f_filings.signature_name" in cols

    def test_people_email_is_personal(self):
        cols = {c["name"]: c for c in _dict()["tables"]["people"]["columns"]}
        assert cols["email"]["pii"] == "personal"

    @pytest.mark.parametrize("value, name, pii, expected", [
        ("jane.doe@acme.com", "email", "business_contact", "j***@acme.com"),
        ("(212) 555-1234", "signature_phone", "business_contact", "***1234"),
        ("Jane", "first_name", "business_contact", "J***"),
        ("Jane", "first_name", "personal", None),
        ("Acme", "name", "none", "Acme"),
        (None, "email", "business_contact", None),
    ])
    def test_mask_value(self, value, name, pii, expected):
        from app.catalog.columns import mask_value

        assert mask_value(value, name, pii) == expected


# ---------------------------------------------------------------------------
# T4 joins, coverage, route order
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestJoinsAndCoverage:
    def test_join_index_has_cik_across_sec_datasets(self):
        from app.catalog.dictionary import join_index

        cik = join_index("cik")["cik"]
        ds = {r["dataset"] for r in cik}
        assert {"sec_13f", "sec_form_d", "sec_edgar_submissions", "entity_master"} <= ds
        assert all(r["normalize_sql"].startswith("lpad(ltrim(") for r in cik)

    def test_related_datasets_ranked_by_specificity(self):
        from app.catalog.dictionary import related_datasets

        rel = related_datasets("sec_13f")
        spec = [r["specificity"] for r in rel]
        assert spec == sorted(spec, reverse=True)
        cik = next(r for r in rel if r["semantic_type"] == "cik" and r["dataset"] == "sec_form_d")
        assert cik["specificity"] == 10
        assert cik["join_sql"].count("lpad(ltrim(") == 2 and " JOIN " in cik["join_sql"]
        assert all(r["dataset"] != "sec_13f" for r in rel)
        assert not any(r["semantic_type"] == "us_state" for r in rel)

    def test_search_columns(self):
        from app.catalog.dictionary import search_columns

        total, hits = search_columns(q="signature", pii="business_contact")
        assert total >= 3 and all(h["pii"] == "business_contact" for h in hits)
        total, hits = search_columns(semantic_type="crd", limit=5)
        assert total > 5 and len(hits) == 5

    def test_description_coverage(self):
        """PLAN_088: >= 80% for the PE/entity pack (gate), >= 56% overall (tracked floor)."""
        from app.catalog.dictionary import coverage_report

        r = coverage_report()
        assert r["pe_entity_pack"]["pct"] >= 80.0, r["pe_entity_pack"]
        assert r["overall"]["pct"] >= 56.0, r["overall"]
        print(f"\ncolumn description coverage: overall {r['overall']}, pack {r['pe_entity_pack']}")

    def test_router_registered_before_catalog(self):
        main = (REPO / "app" / "main.py").read_text(encoding="utf-8")
        mine = 'app.include_router(catalog_schema.router, prefix="/api/v1", dependencies=_auth)'
        theirs = 'app.include_router(catalog.router, prefix="/api/v1", dependencies=_auth)'
        assert mine in main and main.index(mine) < main.index(theirs)

    def test_static_routes_not_swallowed_by_key_route(self):
        from fastapi import FastAPI
        from fastapi.testclient import TestClient

        from app.api.v1 import catalog as catalog_api
        from app.api.v1 import catalog_schema

        app = FastAPI()
        app.include_router(catalog_schema.router, prefix="/api/v1")
        app.include_router(catalog_api.router, prefix="/api/v1")
        c = TestClient(app)
        joins = c.get("/api/v1/catalog/joins", params={"semantic_type": "crd"})
        assert joins.status_code == 200 and "crd" in joins.json()["join_keys"]
        assert c.get("/api/v1/catalog/joins", params={"semantic_type": "us_state"}).status_code == 422
        cols = c.get("/api/v1/catalog/columns", params={"q": "cusip"})
        assert cols.status_code == 200 and cols.json()["count"] > 0
        assert c.get("/api/v1/catalog/columns", params={"pii": "secret"}).status_code == 422
        cov = c.get("/api/v1/catalog/columns/coverage").json()
        assert cov["pe_entity_pack"]["pct"] >= 80
        rel = c.get("/api/v1/catalog/sec_form_d/joins").json()
        assert rel["dataset"] == "sec_form_d" and rel["count"] > 0
        assert c.get("/api/v1/catalog/nope/joins").status_code == 404

    def test_sync_endpoint_is_admin_only(self):
        from fastapi import FastAPI
        from fastapi.testclient import TestClient

        from app.api.v1 import catalog_schema
        from app.core.authz import current_principal

        app = FastAPI()
        app.include_router(catalog_schema.router, prefix="/api/v1")
        app.dependency_overrides[current_principal] = lambda: {"role": "user"}
        r = TestClient(app).post("/api/v1/catalog/columns/comments/sync")
        assert r.status_code == 403


# ---------------------------------------------------------------------------
# PostgreSQL-backed: schema, sample, CIK join, comment sync
# ---------------------------------------------------------------------------

_T = ("t137_people", "t137_secret", "t137_acs", "t137_identifier", "t137_filers", "t137_filers_int")


def _t137_spec(key, tables, **kw):
    from app.catalog.spec import DatasetSpec

    base = dict(
        key=key, source="t137", display_name=key, description="A SPEC 137 test dataset for the column dictionary, schema and sample endpoints.",
        kind="reference", grain="one row per person", producer=f"api:{key}", cadence="ad_hoc",
        rerun="idempotent", license="public domain", redistribution="open", pii_class="business_contact",
        origin="official", status_public="internal", tables=tuple(tables), primary_key=("id",),
    )
    base.update(kw)
    return DatasetSpec(**base)


@pytest.fixture
def pg137(monkeypatch):
    from sqlalchemy import create_engine, text

    import app.catalog.dictionary as dictionary
    import app.catalog.registry as registry
    from app.api.v1 import catalog_schema
    from app.catalog.schema_live import clear_sample_cache
    from app.core.models import Base, CensusVariableMetadata, DataProfileColumn, DataProfileSnapshot

    engine = create_engine(PG_URL)

    def _drop(conn):
        for t in _T:
            conn.execute(text(f"DROP TABLE IF EXISTS public.{t} CASCADE"))

    Base.metadata.create_all(engine, tables=[CensusVariableMetadata.__table__, DataProfileSnapshot.__table__,
                                             DataProfileColumn.__table__])
    with engine.begin() as conn:
        _drop(conn)
        conn.execute(text("DELETE FROM census_variable_metadata WHERE dataset_id LIKE 't137%'"))
        conn.execute(text("DELETE FROM data_profile_snapshots WHERE table_name LIKE 't137%'"))
        conn.execute(text(
            "CREATE TABLE t137_people (id INTEGER PRIMARY KEY, full_name TEXT, first_name TEXT, email TEXT, "
            "email_confidence TEXT, phone TEXT, cik TEXT, mystery_blob TEXT, extra JSONB, nickname TEXT)"))
        conn.execute(text("CREATE UNIQUE INDEX ux_t137_people_cik ON t137_people (cik)"))
        conn.execute(text(
            "INSERT INTO t137_people SELECT g, 'Person ' || g, 'Jane' || g, 'jane' || g || '@acme.com', "
            "CASE WHEN g % 3 = 0 THEN 'inferred' ELSE 'verified' END, '(212) 555-' || lpad(g::text, 4, '0'), "
            "lpad(g::text, 10, '0'), 'blob', '{\"a\": 1}'::jsonb, 'Nick' || g FROM generate_series(1, 25) g"))
        conn.execute(text("CREATE TABLE t137_secret (id INTEGER PRIMARY KEY, api_key TEXT)"))
        conn.execute(text("INSERT INTO t137_secret VALUES (1, 'k')"))
        conn.execute(text("CREATE TABLE t137_acs (geo_id TEXT PRIMARY KEY, b01001_001e INTEGER)"))
        conn.execute(text("INSERT INTO t137_acs VALUES ('06075', 870000)"))
        acs_sid = conn.execute(text(
            "INSERT INTO data_profile_snapshots (table_name, row_count, column_count, total_null_count, "
            "profiled_at) VALUES ('t137_acs', 1, 2, 0, now()) RETURNING id")).scalar()
        conn.execute(text(
            "INSERT INTO data_profile_columns (snapshot_id, column_name, null_count, null_pct, distinct_count, "
            "stats) VALUES (:s, 'geo_id', 0, 0.0, 1, CAST(:st AS json))"),
            {"s": acs_sid, "st": '{"top_values": [{"value": "06075", "count": 1}]}'})
        conn.execute(text(
            "INSERT INTO census_variable_metadata (dataset_id, variable_name, column_name, label, concept, "
            "created_at) VALUES ('t137_acs', 'B01001_001E', 'b01001_001e', 'Estimate!!Total:', 'SEX BY AGE', "
            "now())"))
        sid = conn.execute(text(
            "INSERT INTO data_profile_snapshots (table_name, row_count, column_count, total_null_count, "
            "profiled_at) VALUES ('t137_people', 25, 9, 0, now()) RETURNING id")).scalar()
        conn.execute(text(
            "INSERT INTO data_profile_columns (snapshot_id, column_name, null_count, null_pct, distinct_count, "
            "stats) VALUES (:s, 'cik', 0, 0.0, 25, CAST(:st AS json)), "
            "(:s, 'email', 0, 4.0, 25, CAST(:em AS json))"),
            {"s": sid, "st": '{"top_values": [{"value": "0000000001", "count": 1}]}',
             "em": '{"top_values": [{"value": "jane1@acme.com", "count": 1}]}'})

    specs = {
        "t137_people_ds": _t137_spec("t137_people_ds", ["t137_people"]),
        "t137_personal_ds": _t137_spec("t137_personal_ds", ["t137_people"], pii_class="personal",
                                       producer="api:t137_personal_ds"),
        "t137_restricted_ds": _t137_spec("t137_restricted_ds", ["t137_acs"], redistribution="restricted",
                                         producer="api:t137_restricted_ds", attribution="Source: T137 Bureau"),
        "t137_secret_ds": _t137_spec("t137_secret_ds", ["t137_secret", "t137_missing"],
                                     producer="api:t137_secret_ds"),
        "t137_acs_ds": _t137_spec("t137_acs_ds", ["t137_acs"], producer="api:t137_acs_ds",
                                  primary_key=("geo_id",)),
    }
    real = registry.get_catalog()
    # the personal variant is reachable by key but is not a catalog writer of t137_people
    writers = tuple(v for k, v in specs.items() if k != "t137_personal_ds")
    monkeypatch.setattr(registry, "get_catalog", lambda: tuple(real) + writers)
    monkeypatch.setattr(catalog_schema, "get_spec", lambda k: specs.get(k) or registry.get_spec(k))

    body = copy.deepcopy(dictionary.load_dictionary())
    body["tables"]["t137_people"] = {"datasets": ["t137_people_ds"], "origin": "model", "packages": [],
                                     "columns": [
        {"name": "full_name", "description": "Full name, 100% as filed.", "source": "curated", "pii": "business_contact",
         "semantic_type": None, "unit": None, "example": None, "pg_type": "text", "nullable": True,
         "confidence": "high"},
        {"name": "extra", "description": "Extra JSON.", "source": "model", "pii": "none", "semantic_type": None,
         "unit": None, "example": None, "pg_type": "jsonb", "nullable": True, "confidence": "medium"},
        {"name": "nickname", "description": "What friends call them.", "source": "model", "pii": "none",
         "semantic_type": None, "unit": None, "example": None, "pg_type": "text", "nullable": True,
         "confidence": "medium"},
    ]}
    monkeypatch.setattr(dictionary, "load_dictionary", lambda: body)
    clear_sample_cache()
    yield engine, specs
    clear_sample_cache()
    with engine.begin() as conn:
        _drop(conn)
        conn.execute(text("DELETE FROM census_variable_metadata WHERE dataset_id LIKE 't137%'"))
        conn.execute(text("DELETE FROM data_profile_snapshots WHERE table_name LIKE 't137%'"))
    engine.dispose()


def _client(engine, role="user"):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from sqlalchemy.orm import sessionmaker

    from app.api.v1 import catalog_schema
    from app.core.authz import current_principal
    from app.core.database import get_db

    factory = sessionmaker(bind=engine)

    def _db():
        db = factory()
        try:
            yield db
        finally:
            db.close()

    app = FastAPI()
    app.include_router(catalog_schema.router, prefix="/api/v1")
    app.dependency_overrides[get_db] = _db
    app.dependency_overrides[current_principal] = lambda: {"role": role}
    return TestClient(app)


@pg
class TestSchemaPg:
    def test_schema_merges_live_facts(self, pg137):
        engine, _ = pg137
        r = _client(engine).get("/api/v1/catalog/t137_people_ds/schema")
        assert r.status_code == 200
        assert r.headers["ETag"].startswith('W/"')
        body = r.json()
        t = body["tables"][0]
        assert t["exists"] and t["table"] == "t137_people" and t["primary_key"] == ["id"]
        assert {"columns": ["cik"], "primary": False} in t["unique_keys"]
        cols = {c["name"]: c for c in t["columns"]}
        assert cols["email"]["pg_type"] == "text" and cols["email"]["pii"] == "business_contact"
        assert cols["email"]["null_pct"] == 4.0 and cols["email"]["example"] is None  # PII: no example
        assert cols["cik"]["semantic_type"] == "cik" and cols["cik"]["example"] == "0000000001"
        assert cols["cik"]["normalize_sql"] == "lpad(ltrim(\"cik\"::text,'0'),10,'0')"
        assert cols["full_name"]["source"] == "curated"
        assert cols["mystery_blob"]["description"] is None
        assert body["coverage"]["columns"] == 10 and body["coverage"]["described"] == 8
        assert body["flags"]["status_public"] == "internal" and body["flags"]["origin"] == "official"
        assert t["rights"]["origins"] == ["official"] and t["row_filter"] is None

    def test_schema_missing_table_and_census_label(self, pg137):
        engine, _ = pg137
        c = _client(engine)
        body = c.get("/api/v1/catalog/t137_secret_ds/schema").json()
        missing = next(t for t in body["tables"] if t["table"] == "t137_missing")
        assert missing["exists"] is False and missing["row_estimate"] is None
        acs = c.get("/api/v1/catalog/t137_acs_ds/schema", params={"table": "t137_acs"}).json()
        col = next(x for x in acs["tables"][0]["columns"] if x["name"] == "b01001_001e")
        assert col["source"] == "upstream" and "Total" in col["description"] and "Sex by age" in col["description"]
        assert c.get("/api/v1/catalog/t137_acs_ds/schema", params={"table": "nope"}).status_code == 404
        assert c.get("/api/v1/catalog/nope/schema").status_code == 404

    def test_restricted_profile_examples_are_admin_only(self, pg137):
        """Review finding: /schema leaked profile top_values of restricted tables to anyone."""
        engine, _ = pg137

        def geo(role):
            body = _client(engine, role).get("/api/v1/catalog/t137_restricted_ds/schema").json()
            return next(c for c in body["tables"][0]["columns"] if c["name"] == "geo_id")

        assert geo("user")["example"] is None
        assert geo("admin")["example"] == "06075"

    def test_storage_forbidden_examples_withheld_from_admins(self, pg137, monkeypatch):
        from app.catalog import schema_live

        engine, _ = pg137
        real = schema_live.table_writers
        monkeypatch.setattr(schema_live, "table_writers",
                            lambda t, s: [_Proxy(w, storage="forbidden") for w in real(t, s)])
        body = _client(engine, "admin").get("/api/v1/catalog/t137_people_ds/schema").json()
        cik = next(c for c in body["tables"][0]["columns"] if c["name"] == "cik")
        assert cik["example"] is None

    def test_schema_is_fast_for_sec_13f(self, pg137):
        import time

        engine, _ = pg137
        c = _client(engine)
        c.get("/api/v1/catalog/sec_13f/schema")  # warm imports
        t0 = time.monotonic()
        r = c.get("/api/v1/catalog/sec_13f/schema")
        assert r.status_code == 200 and time.monotonic() - t0 < 1.0
        assert {t["table"] for t in r.json()["tables"]} == {"sec_13f_filings", "sec_13f_holdings",
                                                            "sec_13f_other_managers"}


@pg
class TestSamplePg:
    def test_non_admin_masking(self, pg137):
        engine, _ = pg137
        r = _client(engine).get("/api/v1/catalog/t137_people_ds/sample", params={"limit": 20})
        assert r.status_code == 200
        assert r.headers["X-Dataset-Attribution"] == "License: public domain"
        body = r.json()
        assert len(body["rows"]) == 20 and body["masked"] and body["order_by"] == ["id"]
        names = [c["name"] for c in body["columns"]]
        assert "mystery_blob" not in names and "email_confidence" not in names
        assert body["hidden_columns"] == 2
        row1, row3 = body["rows"][0], body["rows"][2]
        assert row1["id"] == 1 and row1["email"] == "j***@acme.com"
        assert row3["email"] is None  # inferred
        assert row1["phone"] == "***0001" and row1["first_name"] == "J***" and row1["full_name"] == "P***"
        assert row1["cik"] == "0000000001"
        assert row1["extra"] is None  # model-described JSON: opaque to non-admins
        meta = {c["name"]: c for c in body["columns"]}
        assert meta["email"]["masked"] and not meta["cik"]["masked"]

    def test_admin_unmasked_but_guessed_email_null(self, pg137):
        engine, _ = pg137
        body = _client(engine, role="admin").get("/api/v1/catalog/t137_people_ds/sample").json()
        assert len(body["rows"]) == 10 and not body["masked"]
        assert body["rows"][0]["email"] == "jane1@acme.com" and body["rows"][0]["first_name"] == "Jane1"
        assert body["rows"][2]["email"] is None
        assert body["rows"][0]["extra"] == {"a": 1}

    def test_personal_dataset_nulls_contact_columns(self, pg137):
        engine, _ = pg137
        row = _client(engine).get("/api/v1/catalog/t137_personal_ds/sample").json()["rows"][0]
        assert row["first_name"] is None and row["email"] is None and row["cik"] == "0000000001"

    def test_personal_dataset_masks_columns_the_rules_miss(self, pg137):
        """Review finding: a person column tagged pii='none' was served in clear (deny by default)."""
        engine, _ = pg137
        body = _client(engine).get("/api/v1/catalog/t137_personal_ds/sample").json()
        meta = {c["name"]: c for c in body["columns"]}
        assert meta["nickname"]["pii"] == "personal" and meta["nickname"]["masked"]
        assert body["rows"][0]["nickname"] is None and body["rows"][0]["id"] == 1
        # the same column in a business_contact dataset is shown
        assert _client(engine).get("/api/v1/catalog/t137_people_ds/sample").json()["rows"][0]["nickname"] == "Nick1"
        schema = _client(engine).get("/api/v1/catalog/t137_personal_ds/schema").json()
        assert {c["name"]: c for c in schema["tables"][0]["columns"]}["nickname"]["pii"] == "personal"

    def test_sample_carries_origin_and_rights(self, pg137):
        engine, _ = pg137
        r = _client(engine).get("/api/v1/catalog/t137_people_ds/sample")
        body = r.json()
        assert r.headers["X-Dataset-Origin"] == "official"
        assert body["flags"]["origin"] == "official" and body["flags"]["reviewed"] is False
        assert body["rights"]["status_public"] == "internal" and body["row_filter"] is None

    def test_storage_forbidden_refuses_non_admins_and_flags_admins(self, pg137, monkeypatch):
        """SPEC_142 replaced item 5 (refuse admins too): gated data is refused to non-admins and
        served to admins with the gate in the body and the X-Dataset-Rights-Gate header."""
        from app.catalog import schema_live

        engine, _ = pg137
        real = schema_live.table_writers
        monkeypatch.setattr(schema_live, "table_writers",
                            lambda t, s: [_Proxy(w, commercial_use="forbidden") for w in real(t, s)])
        assert _client(engine).get("/api/v1/catalog/t137_people_ds/sample").status_code == 403
        r = _client(engine, "admin").get("/api/v1/catalog/t137_people_ds/sample")
        assert r.status_code == 200
        assert r.headers["X-Dataset-Rights-Gate"] == "commercial_use_forbidden"
        assert r.json()["flags"]["rights_gate"] == ["commercial_use_forbidden"]

    def test_row_filter_scopes_shared_table(self, pg137):
        from app.catalog.schema_live import build_sample

        engine, specs = pg137
        spec = _Proxy(specs["t137_people_ds"], row_filters=(("t137_people", "id % 5 = 0"),))
        body = build_sample(engine, spec, "t137_people", 20, admin=True, use_cache=False)
        assert [r["id"] for r in body["rows"]] == [5, 10, 15, 20, 25]
        assert body["row_filter"] == "id % 5 = 0"

    def test_restricted_is_admin_only(self, pg137):
        engine, _ = pg137
        assert _client(engine).get("/api/v1/catalog/t137_restricted_ds/sample").status_code == 403
        r = _client(engine, role="admin").get("/api/v1/catalog/t137_restricted_ds/sample")
        assert r.status_code == 200 and r.headers["X-Dataset-Attribution"] == "Source: T137 Bureau"

    def test_export_policy_denies_everyone(self, pg137):
        engine, _ = pg137
        for role in ("user", "admin"):
            r = _client(engine, role).get("/api/v1/catalog/t137_secret_ds/sample",
                                          params={"table": "t137_secret"})
            assert r.status_code == 403

    def test_limits_and_unknown_tables(self, pg137):
        engine, _ = pg137
        c = _client(engine)
        assert c.get("/api/v1/catalog/t137_people_ds/sample", params={"limit": 21}).status_code == 422
        assert c.get("/api/v1/catalog/t137_people_ds/sample", params={"table": "users"}).status_code == 404
        assert c.get("/api/v1/catalog/t137_secret_ds/sample",
                     params={"table": "t137_missing"}).status_code == 404


@pg
class TestCikJoinPg:
    def test_normalize_sql_joins_unpadded_ciks(self, pg137):
        """PLAN_088 §1.9: naive join 0/1000, normalize_sql 1000/1000 (text, padded text and bigint)."""
        from sqlalchemy import text

        from app.catalog.identifiers import normalize_expr

        engine, _ = pg137
        with engine.begin() as conn:
            conn.execute(text("CREATE TABLE t137_identifier (id_type TEXT, id_value TEXT)"))
            conn.execute(text("CREATE TABLE t137_filers (cik TEXT PRIMARY KEY)"))
            conn.execute(text("CREATE TABLE t137_filers_int (cik BIGINT PRIMARY KEY)"))
            conn.execute(text("INSERT INTO t137_identifier SELECT 'cik', (1000 + g * 7)::text "
                              "FROM generate_series(1, 1000) g"))
            conn.execute(text("INSERT INTO t137_filers SELECT lpad((1000 + g * 7)::text, 10, '0') "
                              "FROM generate_series(1, 1500) g"))
            conn.execute(text("INSERT INTO t137_filers_int SELECT 1000 + g * 7 FROM generate_series(1, 1500) g"))
            naive = conn.execute(text(
                "SELECT count(*) FROM t137_identifier i JOIN t137_filers f ON i.id_value = f.cik")).scalar()
            joined = conn.execute(text(
                f"SELECT count(*) FROM t137_identifier i JOIN t137_filers f "
                f"ON {normalize_expr('cik', 'i.id_value')} = {normalize_expr('cik', 'f.cik')}")).scalar()
            joined_int = conn.execute(text(
                f"SELECT count(*) FROM t137_identifier i JOIN t137_filers_int f "
                f"ON {normalize_expr('cik', 'i.id_value')} = {normalize_expr('cik', 'f.cik')}")).scalar()
        assert naive == 0 and joined == 1000 and joined_int == 1000

    @pytest.mark.parametrize("st, values, expected", [
        ("cik", ["25743", "0000025743", 25743], "0000025743"),
        ("crd", ["00123", "123"], "123"),
        ("zip5", ["02139-1234", "2139", "02139"], "02139"),
        ("ein", ["12-3456789", "123456789"], "123456789"),
        ("naics", ["541511", "5415-11"], "541511"),
        ("accession_number", ["0001234567-24-000001", "000123456724000001"], "000123456724000001"),
    ])
    def test_normalize_sql_values(self, pg137, st, values, expected):
        from sqlalchemy import text

        from app.catalog.identifiers import normalize_expr

        engine, _ = pg137
        with engine.connect() as conn:
            for v in values:
                assert conn.execute(text(f"SELECT {normalize_expr(st, 'CAST(:v AS text)')}"), {"v": v}).scalar() == expected


@pg
class TestCommentSyncPg:
    def test_sync_writes_adopts_and_is_idempotent(self, pg137):
        from sqlalchemy import text

        from app.catalog.mirror import sync_column_comments

        engine, specs = pg137
        spec = [specs["t137_people_ds"]]
        with engine.begin() as conn:
            conn.execute(text("COMMENT ON COLUMN t137_people.extra IS 'hand written'"))

        def comments():
            with engine.connect() as conn:
                return dict(conn.execute(text(
                    "SELECT a.attname, col_description(a.attrelid, a.attnum) FROM pg_attribute a "
                    "WHERE a.attrelid = 't137_people'::regclass AND a.attnum > 0")).fetchall())

        dry = sync_column_comments(engine, specs=spec, dry_run=True)
        assert dry["would_write"] == 2 and dry["written"] == 0 and comments()["full_name"] is None
        first = sync_column_comments(engine, specs=spec)
        assert first["written"] == 2 and first["adopted"] == 1 and not first["skipped_tables"]
        got = comments()
        assert got["full_name"] == "Full name, 100% as filed."
        assert got["extra"] == "hand written"  # adopted, never overwritten
        assert got["email"] is None  # glossary text is not written back
        again = sync_column_comments(engine, specs=spec)
        assert again["written"] == 0 and again["unchanged"] == 2 and again["adopted"] == 1

    def test_lock_timeout_skips_table(self, pg137):
        from sqlalchemy import text

        from app.catalog.mirror import sync_column_comments

        engine, specs = pg137
        blocker = engine.connect()
        tx = blocker.begin()
        blocker.execute(text("LOCK TABLE t137_people IN ACCESS EXCLUSIVE MODE"))
        try:
            stats = sync_column_comments(engine, specs=[specs["t137_people_ds"]], lock_timeout_ms=200)
        finally:
            tx.rollback()
            blocker.close()
        assert stats["written"] == 0
        assert stats["tables"] == 0 or stats["skipped_tables"] == [{"table": "t137_people",
                                                                     "error": "OperationalError"}]
