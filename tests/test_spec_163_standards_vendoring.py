"""
Tests for SPEC 163 -- reference standards vendored (FHIR R4, US Core 9.0.0, Plan-Net 1.2.0,
OMOP CDM 5.4, NUCC 26.1, ICD-10-CM FY2027, HCPCS L2, CMS POS) + licence guard + rights.

All offline: the vendored files are read from app/ontology/standards/.
"""

import csv
import hashlib
import json

import pytest

pytestmark = pytest.mark.unit

STANDARDS = ("fhir_r4", "us_core", "plan_net", "omop_cdm", "nucc_taxonomy", "icd10cm", "hcpcs_l2", "cms_pos")


def _S():
    import app.ontology.standards as S

    return S


def _G():
    from app.ontology.standards import licence_guard as G

    return G


# ---------------------------------------------------------------------------
# T1/T2 manifest
# ---------------------------------------------------------------------------

def test_manifest_hashes_match_files():
    S = _S()
    m = S.manifest()
    assert len(m["files"]) == 10
    for name, e in m["files"].items():
        blob = (S.HERE / name).read_bytes()
        assert hashlib.sha256(blob).hexdigest() == e["sha256"], name
        assert len(blob) == e["bytes"], name
    assert m["total_bytes"] == sum(e["bytes"] for e in m["files"].values())
    assert m["total_bytes"] < 1_500_000          # compact extracts, not the 35 MB bundles
    assert set(S.verify()) == set(m["files"])


def test_verify_detects_tamper(monkeypatch):
    S = _S()
    m = json.loads(json.dumps(S.manifest()))
    m["files"]["cms_pos.json"]["sha256"] = "0" * 64
    monkeypatch.setattr(S, "manifest", lambda: m)
    with pytest.raises(S.StandardsIntegrityError):
        S.verify()


def test_manifest_entries_complete():
    m = _S().manifest()
    assert {e["standard"] for e in m["files"].values()} == set(STANDARDS)
    for name, e in m["files"].items():
        for k in ("standard", "title", "version", "url", "licence", "licence_url", "use", "retrieved", "extract"):
            assert e.get(k), (name, k)
        assert e["url"].startswith("http") and e["licence_url"].startswith("http")
        assert e["use"] in ("open", "internal_only")
        assert e.get("licence_quote") or e.get("licence_quote_note"), name
    assert "D3" in m["decisions"] and "5.4" in m["decisions"]["D3"]
    assert "INTERNAL" in m["decisions"]["D9"] and "before any external release" in m["decisions"]["D9"].lower()
    nucc = m["files"]["nucc_taxonomy_26_1.csv"]
    assert nucc["use"] == "internal_only" and "D9" in nucc["licence_action"]
    assert "American Medical Association" in nucc["licence_quote"]
    assert "commercial use" in nucc["licence_quote"]
    assert m["files"]["icd10cm_2027_chapters.json"]["use"] == "internal_only"
    assert "Apache License 2.0" in m["files"]["omop_cdm_v5_4_fields.csv"]["licence_quote"]
    assert "CC0" in m["files"]["fhir_r4_resources.json"]["licence_quote"]


def test_no_unlisted_files():
    S = _S()
    listed = set(S.manifest()["files"]) | {"manifest.json"}
    for p in S.HERE.iterdir():
        if p.is_file() and p.suffix in (".json", ".csv", ".txt", ".zip", ".tgz", ".xml"):
            assert p.name in listed, p.name


# ---------------------------------------------------------------------------
# T3 FHIR R4
# ---------------------------------------------------------------------------

def test_fhir_concepts_and_practitioner_elements():
    S = _S()
    assert len(S.fhir_concepts()) == 13
    raw = json.loads((S.HERE / "fhir_r4_resources.json").read_text(encoding="utf-8"))
    assert raw["fhir_version"] == "4.0.1"
    assert len(raw["resources"]["Practitioner"]["elements"]) == 26      # R4 4.0.1 snapshot
    attrs = {a["path"]: a for a in S.fhir_attributes("Practitioner")}
    assert len(attrs) == 14                                            # without id/meta/text/extension...
    assert attrs["Practitioner.name"]["types"] == ["HumanName"]
    assert attrs["Practitioner.qualification"]["max"] == "*"
    assert "Practitioner.meta" not in attrs
    assert "Practitioner.meta" in {a["path"] for a in S.fhir_attributes("Practitioner", include_infra=True)}
    with pytest.raises(KeyError):
        S.fhir_attributes("Observation")


def test_fhir_relations():
    rel = {(r["source"], r["path"]): r["targets"] for r in _S().fhir_relations()}
    assert rel[("PractitionerRole", "PractitionerRole.practitioner")] == ["Practitioner"]
    assert rel[("PractitionerRole", "PractitionerRole.organization")] == ["Organization"]
    assert rel[("PractitionerRole", "PractitionerRole.location")] == ["Location"]
    assert rel[("PractitionerRole", "PractitionerRole.healthcareService")] == ["HealthcareService"]
    assert rel[("PractitionerRole", "PractitionerRole.endpoint")] == ["Endpoint"]
    assert rel[("Organization", "Organization.partOf")] == ["Organization"]
    assert "Practitioner" in rel[("Encounter", "Encounter.participant.individual")]


@pytest.mark.parametrize("path,ok", [
    ("Practitioner.name.family", True), ("Practitioner.identifier.value", True),
    ("Patient.multipleBirthBoolean", True), ("Patient.deceased[x]", True),
    ("Claim.item.detail.subDetail.net.value", True), ("Coverage.class.value", True),
    ("Encounter.participant.individual.reference", True),
    ("Practitioner.specialty", False), ("Practitioner.name.surname", False),
    ("Patient.name.family.x", False), ("Observation.code", False), ("", False),
])
def test_fhir_path_exists(path, ok):
    assert _S().fhir_path_exists(path) is ok


# ---------------------------------------------------------------------------
# T4/T5 US Core + Plan-Net
# ---------------------------------------------------------------------------

def test_us_core_must_support():
    S = _S()
    prac = S.us_core_must_support("Practitioner")
    assert {"Practitioner.identifier", "Practitioner.name", "Practitioner.identifier:NPI"} <= set(prac)
    assert "Patient.birthDate" in S.us_core_must_support("Patient")
    assert "PractitionerRole.specialty" in S.us_core_must_support("PractitionerRole")
    assert S.us_core_must_support("Claim") == []
    attrs = {a["path"]: a for a in S.fhir_attributes("Practitioner")}
    assert attrs["Practitioner.name"]["must_support_us_core"] is True
    assert attrs["Practitioner.gender"]["must_support_us_core"] is False
    uc = json.loads((S.HERE / "us_core_9_0_0.json").read_text(encoding="utf-8"))
    assert uc["version"] == "9.0.0" and uc["license"] == "CC0-1.0"


def test_plan_net_profiles():
    S = _S()
    profs = S.plan_net_profiles()
    assert {"PractitionerRole", "OrganizationAffiliation", "HealthcareService", "Endpoint", "Network"} <= set(profs)
    assert profs["Network"]["type"] == "Organization"
    assert profs["OrganizationAffiliation"]["type"] == "OrganizationAffiliation"
    pn = json.loads((S.HERE / "plan_net_1_2_0.json").read_text(encoding="utf-8"))
    assert pn["version"] == "1.2.0" and pn["license"] == "CC0-1.0"


def test_plan_net_relations():
    rel = {(r["source"], r["path"]): r["targets"] for r in _S().plan_net_relations()}
    assert rel[("OrganizationAffiliation", "OrganizationAffiliation.network")] == ["Network"]
    assert rel[("OrganizationAffiliation", "OrganizationAffiliation.participatingOrganization")] == ["Organization"]
    assert rel[("PractitionerRole", "PractitionerRole.practitioner")] == ["Practitioner"]
    assert rel[("HealthcareService", "HealthcareService.providedBy")] == ["Organization"]
    assert rel[("Endpoint", "Endpoint.managingOrganization")] == ["Organization"]
    assert rel[("InsurancePlan", "InsurancePlan.network")] == ["Network"]
    assert not any(".extension" in p for _, p in rel)


# ---------------------------------------------------------------------------
# T6 OMOP
# ---------------------------------------------------------------------------

def test_omop_tables_and_fields():
    S = _S()
    tables = {t["table"] for t in S.omop_tables()}
    assert len(tables) == 39
    assert {"person", "provider", "care_site", "location", "visit_occurrence", "payer_plan_period",
            "cost", "condition_occurrence", "procedure_occurrence", "visit_detail"} <= tables
    assert len(S.omop_fields("person")) == 18
    assert len(S.omop_fields("provider")) == 13
    assert len(S.omop_fields("care_site")) == 6
    person = {f["field"]: f for f in S.omop_fields("PERSON")}
    assert person["person_id"]["primary_key"] and person["person_id"]["required"]
    assert person["year_of_birth"]["datatype"] == "integer"
    assert S.omop_field_exists("provider", "npi") and not S.omop_field_exists("provider", "taxonomy")
    assert S.omop_field_exists("care_site") and not S.omop_field_exists("hospital")


def test_omop_relations():
    fk = {(r["table"], r["field"]): r for r in _S().omop_relations()}
    assert (fk[("provider", "care_site_id")]["fk_table"], fk[("provider", "care_site_id")]["fk_field"]) == \
        ("care_site", "care_site_id")
    assert fk[("care_site", "location_id")]["fk_table"] == "location"
    assert fk[("person", "gender_concept_id")]["fk_domain"] == "Gender"
    assert fk[("visit_occurrence", "provider_id")]["fk_table"] == "provider"


def test_omop_no_prose_columns():
    S = _S()
    for name in ("omop_cdm_v5_4_fields.csv", "omop_cdm_v5_4_tables.csv"):
        header = (S.HERE / name).read_text(encoding="utf-8").splitlines()[0].split(",")
        assert not {"userGuidance", "etlConventions", "tableDescription"} & set(header), name


# ---------------------------------------------------------------------------
# T7 NUCC
# ---------------------------------------------------------------------------

def test_nucc_codes_and_tree():
    S = _S()
    codes = S.nucc_codes()
    assert len(codes) == 883 and all(len(c) == 10 and c.endswith("X") for c in codes)
    tree = S.nucc_tree()
    depth = max(3 if node["specializations"] else 2 for g in tree.values() for node in g.values())
    assert depth == 3
    assert all(S.nucc_lookup(c)["grouping"] and S.nucc_lookup(c)["classification"] for c in codes)
    im = S.nucc_lookup("207r00000x")
    assert (im["grouping"], im["classification"], im["specialization"]) == \
        ("Allopathic & Osteopathic Physicians", "Internal Medicine", None)
    assert tree["Allopathic & Osteopathic Physicians"]["Internal Medicine"]["specializations"]["Cardiovascular Disease"] \
        == "207RC0000X"
    assert S.nucc_lookup("207RC0000X")["specialization"] == "Cardiovascular Disease"
    assert S.nucc_lookup("999999999X") is None
    header = (S.HERE / "nucc_taxonomy_26_1.csv").read_text(encoding="utf-8").splitlines()[0]
    assert "Definition" not in header and "Notes" not in header


# ---------------------------------------------------------------------------
# T8 ICD-10-CM, HCPCS L2, POS
# ---------------------------------------------------------------------------

def test_icd10cm():
    S = _S()
    ch = S.icd10cm_chapters()
    assert len(ch) == 22
    assert (ch[0]["first"], ch[0]["last"]) == ("A00", "B99")
    assert ch[3]["title"].startswith("Endocrine")
    blocks = S.icd10cm_blocks()
    assert len(blocks) > 250 and all(b["first"] <= b["last"] for b in blocks)
    assert S.icd10cm_category_exists("E11.9") and S.icd10cm_category_exists("e11")
    assert not S.icd10cm_category_exists("E99")
    assert S.icd10cm_chapter_of("I10") == 9
    data = json.loads((S.HERE / "icd10cm_2027_chapters.json").read_text(encoding="utf-8"))
    assert data["version"] == "FY2027"


def test_hcpcs_l2():
    S = _S()
    codes = S.hcpcs_l2_codes()
    assert len(codes) > 8000
    assert "A0427" in codes and "J1100" in codes
    assert not any(c.startswith("D") for c in codes), "CDT (ADA) D-series must never be vendored"
    assert not any(c.isdigit() for c in codes), "no CPT Level I codes"
    assert not any(m.isdigit() for m in S.hcpcs_l2_modifiers())
    assert {"LT", "RT"} <= set(S.hcpcs_l2_modifiers())
    assert S.code_exists("hcpcs_l2", "a0427") and not S.code_exists("hcpcs_l2", "99213")
    with open(S.HERE / "hcpcs_l2_2026_oct.csv", encoding="utf-8") as fh:
        assert next(csv.reader(fh)) == ["kind", "code", "short_description", "added", "terminated"]


def test_pos():
    S = _S()
    pos = S.pos_codes()
    assert pos["11"] == "Office" and pos["21"] == "Inpatient Hospital" and pos["23"].startswith("Emergency Room")
    assert S.code_exists("pos", "11") and S.code_exists("pos", "2")       # zero-padded
    assert not S.code_exists("pos", "98")
    assert S.medicare_facility_indicator() == {"F": "Facility", "O": "Non-facility (office / other)"}


def test_code_exists_dispatch():
    S = _S()
    assert S.code_exists("nucc", "207R00000X")
    assert S.code_exists("icd10cm", "Z00.00")
    with pytest.raises(ValueError):
        S.code_exists("cpt", "99213")
    assert S.versions()["omop_cdm"].startswith("5.4")


# ---------------------------------------------------------------------------
# T9 licence guard
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("value,ok", [
    ("99213", True), ("00100", True), ("0001F", True), ("0042T", True), ("0001U", True), ("99607", True),
    ("00099", False), ("A0427", False), ("9921", False), ("992130", False), ("1234567890", False),
    ("D0120", False), (None, False), (99213, True),
])
def test_is_cpt_code(value, ok):
    assert _G().is_cpt_code(value) is ok


def test_find_cpt_codes_in_text():
    G = _G()
    text = "Billed 99213 and 0042T; NPI 1234567893; date 20260101; HCPCS A0427; ZIP 10001-1234; $12345.67"
    assert G.find_cpt_codes(text) == ["99213", "0042T"]
    assert G.find_cpt_codes(text, allow_codes={"99213"}) == ["0042T"]
    assert G.find_cpt_codes("99213-25 modifier") == ["99213"]
    # precision limit (documented): a bare ZIP has the CPT shape
    assert G.find_cpt_codes("ZIP 10001") == ["10001"]


def test_cdt_codes():
    G = _G()
    assert G.find_cdt_codes("dental D0120 exam") == ["D0120"]
    assert G.find_cdt_codes("ICD D68.51 and HCPCS A0427") == []


def test_cpt_descriptor_index():
    G = _G()
    idx = G.CptDescriptorIndex(["Established patient office or other outpatient visit, 20-29 minutes",
                                "Office visit", None, ""])
    assert len(idx) == 1                                    # short descriptors ignored
    exact = idx.match("Row: ESTABLISHED patient office or other outpatient visit 20-29 minutes.")
    assert exact and exact[0].detail == "exact"
    fuzzy = idx.match("established patient ofice or other outpatient visit, 20-29 minute")
    assert fuzzy and fuzzy[0].detail.startswith("fuzzy")
    assert idx.match("Practitioner who bills Medicare for outpatient services") == []
    assert idx.match("") == []


def test_snomed_detection():
    G = _G()
    assert G.is_sctid("22298006") and G.is_sctid("38341003")      # real SCTIDs (check digits valid)
    assert not G.is_sctid("22298007") and not G.is_sctid("012345") and not G.is_sctid("12345")
    kinds = [f.kind for f in G.find_snomed("system http://snomed.info/sct code 22298006")]
    assert "snomed_uri" in kinds
    assert [f.value for f in G.find_snomed("SNOMED CT: 22298006")] == ["22298006"]
    assert G.find_snomed("NPI 22298006 in a list") == []             # unanchored: not flagged by default
    assert [f.value for f in G.find_snomed("NPI 22298006", bare=True)] == ["22298006"]
    assert G.find_snomed("SNOMED 22298007") == []                    # Verhoeff fails
    assert G.find_snomed("urn:oid:2.16.840.1.113883.6.96")[0].kind == "snomed_uri"


def test_scan_json_paths_and_skip():
    G = _G()
    doc = {"classes": [{"id": "Visit", "label": "Office visit 99213",
                        "mappings": [{"system": "http://snomed.info/sct", "code": "185349003"}]}],
           "bindings": [{"table": "cms_medicare_utilization", "column": "hcpcs_cd", "example": "99214"}],
           "count": 3, "flag": True, "none": None}
    found = {(f.kind, f.location) for f in G.scan_json(doc)}
    assert ("cpt_code", "$.classes[0].label") in found
    assert ("snomed_id", "$.classes[0].mappings[0].code") in found
    assert ("snomed_uri", "$.classes[0].mappings[0].system") in found
    assert ("cpt_code", "$.bindings[0].example") in found
    skipped = G.scan_json(doc, skip_path=lambda p: p.startswith("$.bindings"))
    assert not any(f.location.startswith("$.bindings") for f in skipped)
    assert G.scan_json({"zip": "10001"}, allow_codes={"10001"}) == []
    with pytest.raises(G.LicenceViolation):
        G.assert_clean(doc)
    G.assert_clean({"class": "Practitioner", "nucc": "207R00000X", "hcpcs": "A0427", "npi": "1234567893"})


def test_load_cpt_descriptors_sql_is_read_only():
    G = _G()
    sql = G.CPT_DESCRIPTOR_SQL.upper()
    assert sql.startswith("SELECT DISTINCT HCPCS_DESC FROM CMS_MEDICARE_UTILIZATION")
    assert not any(w in sql for w in ("INSERT", "UPDATE", "DELETE", "DROP", ";"))

    class _Conn:
        def execute(self, stmt):
            assert str(stmt) == G.CPT_DESCRIPTOR_SQL
            return [("Established patient office visit, 20-29 minutes",)]

    assert G.load_cpt_descriptors(_Conn()) == ["Established patient office visit, 20-29 minutes"]


# ---------------------------------------------------------------------------
# T10 vendored files carry no CPT / SNOMED / CDT
# ---------------------------------------------------------------------------

def test_vendored_files_clean():
    S, G = _S(), _G()
    for name in S.manifest()["files"]:
        text = (S.HERE / name).read_text(encoding="utf-8")
        assert G.scan_text(text) == [], name
        assert G.find_snomed(text) == [] and "snomed" not in text.lower(), name
    with open(S.HERE / "hcpcs_l2_2026_oct.csv", encoding="utf-8") as fh:
        codes = [r["code"] for r in csv.DictReader(fh) if r["kind"] == "code"]
    assert not [c for c in codes if G.is_cpt_code(c) or c.startswith("D")]


# ---------------------------------------------------------------------------
# T11 rights
# ---------------------------------------------------------------------------

def test_reference_standard_rights():
    from app.catalog.rights import REFERENCE_STANDARD_RIGHTS as R

    assert set(R) == set(STANDARDS)
    for name, r in R.items():
        assert r.origin == "official" and r.pii_class == "none", name
        assert r.citation_url and r.license_url, name
    nucc = R["nucc_taxonomy"]
    assert nucc.redistribution == "internal_only" and nucc.commercial_use == "agreement_required"
    assert "American Medical Association" in nucc.license and "D9" in nucc.notes
    assert nucc.citation_quote and "commercial use" in nucc.citation_quote
    assert R["icd10cm"].redistribution == "internal_only"
    assert R["omop_cdm"].redistribution == "attribution" and "Apache" in R["omop_cdm"].license
    assert R["fhir_r4"].redistribution == "open" and R["fhir_r4"].commercial_use == "allowed"
    assert "CDT" in R["hcpcs_l2"].notes
    # use in the manifest agrees with the rights block
    use = {e["standard"]: e["use"] for e in _S().manifest()["files"].values()}
    for std, u in use.items():
        assert (R[std].redistribution == "internal_only") == (u == "internal_only"), std


def test_nppes_rights_note_records_nucc_terms():
    from app.catalog.datasets import CATALOG
    from app.catalog.rights import SOURCE_RIGHTS

    notes = SOURCE_RIGHTS["nppes"].notes
    assert "NUCC" in notes and "American Medical Association" in notes and "D9" in notes
    assert "licence" in notes.lower() or "license" in notes.lower()
    spec = next(s for s in CATALOG if s.key == "nppes_providers")
    assert "NUCC" in spec.rights_notes


def test_cms_utilization_cpt_note():
    from app.catalog.datasets import CATALOG

    spec = next(s for s in CATALOG if s.key == "cms_medicare_utilization")
    assert "CPT" in spec.rights_notes and "AMA" in spec.rights_notes
    assert "licence_guard" in spec.rights_notes
    assert spec.commercial_use == "agreement_required" and spec.redistribution == "restricted"


def test_reviewed_signoffs_untouched():
    from app.catalog.rights_reviewed import REVIEWED

    assert "nppes_providers" not in REVIEWED and "cms_medicare_utilization" not in REVIEWED
