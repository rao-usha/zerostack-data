# SPEC 163 — Reference standards vendored + licence guard

**Status:** Active
**Task type:** service
**Date:** 2026-10-08
**Plan:** `docs/plans/PLAN_100_industry_ontologies.md` (§3, §5.4 guard 5, §6 G4/R3, §8.4 row SPEC_163, §10, §12 D3/D9)
**Test file:** tests/test_spec_163_standards_vendoring.py

## Why

The ontology bake-off (PLAN_100) scores every model output deterministically against public
standards (G4 hallucination lookups, R3-lite alignment, R1(b) NUCC value coverage). Those answer
keys must be pinned, hashed, licence-checked files in the repo — not live downloads at scoring
time — and the CPT / SNOMED exclusion (§2, §5.4 guard 5) needs a defined detector before any
brief or prompt exists.

Owner decisions applied:
- **D3 = OMOP CDM 5.4.** Files come from the OHDSI/CommonDataModel GitHub repo at tag `v5.4.3`
  (Apache-2.0, repo `DESCRIPTION`). Only the structural columns of the CSVs are vendored; the
  prose columns (`userGuidance`, `etlConventions`, `tableDescription`) are dropped, and nothing
  from the CC BY-SA docs site is copied.
- **D9 = NUCC vendored for INTERNAL use now; the commercial licence is requested from NUCC
  before any external release.** Recorded in the manifest (`internal_only`, `d9`), in
  `rights.REFERENCE_STANDARD_RIGHTS["nucc_taxonomy"]` and on the `nppes` rights notes.

## Scope

1. **Vendored files** in `app/ontology/standards/` (compact extracts, each with sha256, source
   URL, version, retrieved date, licence, licence URL and a verbatim licence quote in
   `manifest.json`):
   - `fhir_r4_resources.json` — 13 FHIR R4 (4.0.1) StructureDefinitions (Patient, Practitioner,
     PractitionerRole, Organization, OrganizationAffiliation, Location, HealthcareService,
     Endpoint, InsurancePlan, Coverage, Encounter, Claim, ExplanationOfBenefit) reduced to
     snapshot `path/min/max/type(code, targetProfile)/short/binding/contentReference`, extracted
     from `definitions.json.zip` (the 35 MB bundle is not vendored).
   - `fhir_r4_datatypes.json` — the complex datatypes those resources use (HumanName, Address,
     Identifier, CodeableConcept ...), so `Practitioner.name.family` resolves.
   - `us_core_9_0_0.json` — mustSupport elements of the US Core 9.0.0 profiles on those
     resources (Patient, Practitioner, PractitionerRole, Organization, Location, Encounter,
     Coverage).
   - `plan_net_1_2_0.json` — Da Vinci PDex Plan-Net 1.2.0 profiles (PractitionerRole,
     OrganizationAffiliation, HealthcareService, Endpoint, Network, plus Practitioner,
     Organization, Location, InsurancePlan): mustSupport elements and typed references.
   - `omop_cdm_v5_4_fields.csv`, `omop_cdm_v5_4_tables.csv` — structural columns only.
   - `nucc_taxonomy_26_1.csv` — code, grouping, classification, specialization, display name,
     section (no definitions / notes prose).
   - `icd10cm_2027_chapters.json` — FY2027 ICD-10-CM chapters, blocks (sections) and the
     3-character category codes (codes only, no category titles).
   - `hcpcs_l2_2026_oct.csv` — HCPCS Level II codes + short descriptors + Level II modifiers from
     the CMS October 2026 alpha-numeric file. **The D-series (CDT, ADA copyright) is excluded**,
     and the file has no Level I (CPT) rows.
   - `cms_pos.json` — CMS Place of Service codes (code, name; no long descriptions) plus the
     Medicare utilization F/O facility indicator.
   - `build.py` regenerates every file from the pinned sources (network, NexdataResearch UA,
     1 req/s); tests never touch the network.
2. **Loader** `app/ontology/standards/__init__.py`: `manifest()`, `verify()`, and the answer keys
   `fhir_concepts()`, `fhir_attributes(resource)`, `fhir_relations()`, `fhir_path_exists(path)`,
   `us_core_must_support(resource)`, `plan_net_profiles()`, `plan_net_relations()`,
   `omop_tables()`, `omop_fields(table)`, `omop_relations()`, `omop_field_exists()`,
   `nucc_codes()`, `nucc_tree()`, `nucc_lookup(code)`, `icd10cm_chapters()`,
   `icd10cm_blocks()`, `icd10cm_category_exists()`, `hcpcs_l2_codes()`, `pos_codes()`,
   `code_exists(system, code)`.
3. **Licence guard** `app/ontology/standards/licence_guard.py`: detectors for CPT Level I codes
   (plus Category II/III/PLA shapes), CPT descriptors (exact + fuzzy against a caller-supplied
   descriptor set, e.g. `cms_medicare_utilization.hcpcs_desc` for Level I rows, loaded read-only
   in-process and never sent anywhere), SNOMED CT (system URIs, keyword-anchored SCTIDs with
   Verhoeff + partition check, optional bare mode) and the CDT D-series; scans strings and nested
   JSON; documents its precision limits in the module docstring.
4. **Rights**: `rights.REFERENCE_STANDARD_RIGHTS` (one `SourceRights` per vendored standard,
   cited); `nppes` notes record the NUCC terms (AMA copyright, commercial licence needed, D9);
   `cms_medicare_utilization` CPT/AMA note verified (already present since SPEC_142) and extended
   with the guard pointer. Neither dataset is in `rights_reviewed.REVIEWED`; `notes` is not a
   hashed field anyway.

## Non-goals

- No CPT, no SNOMED CT, no CDT, no LOINC content vendored. No Athena vocabularies.
- No DB writes, no table, no endpoint, no scheduler entry. No `app/ontology/__init__.py`
  (SPEC_164/165 own the parent package; `app.ontology.standards` imports as a namespace
  sub-package until then).

## Acceptance Criteria

- [x] Every manifest entry's sha256 and byte size match the committed file; every file in the
      directory (except code) is in the manifest; each entry has url, version, retrieved,
      licence, licence_url, licence_quote, use.
- [x] FHIR: 13 resources; Practitioner has 26 snapshot elements (14 non-infrastructure, R4 4.0.1); PractitionerRole
      references Practitioner/Organization/Location/HealthcareService/Endpoint; datatype paths
      resolve (`Practitioner.name.family`); invented paths do not.
- [x] US Core 9.0.0 mustSupport: Practitioner includes `Practitioner.identifier` and
      `Practitioner.name`; Patient includes `Patient.birthDate`.
- [x] Plan-Net 1.2.0: the 5 named profiles exist; Network is an Organization profile;
      OrganizationAffiliation references Network.
- [x] OMOP 5.4: 39 tables; person / provider / care_site field counts match the CSV;
      `provider.care_site_id` is an FK to `care_site.care_site_id`; no prose columns.
- [x] NUCC 26.1: 883 codes; every code has grouping + classification; tree depth is 3
      (Grouping > Classification > Specialization); `207R00000X` resolves to Allopathic &
      Osteopathic Physicians > Internal Medicine.
- [x] ICD-10-CM FY2027: 22 chapters; blocks parse as ranges; `E11` category exists.
- [x] HCPCS L2: no D-series, no 5-digit numeric codes; `A0427` present.
- [x] POS: `11` = Office, `21` = Inpatient Hospital; F/O indicator present.
- [x] Licence guard: CPT codes `99213`, `0001F`, `0042T`, `0001U` flagged; HCPCS L2 `A0427`,
      NPI-like 10-digit numbers and dates not flagged as CPT; descriptor exact + fuzzy match;
      SNOMED URI and anchored SCTID flagged, bare Verhoeff-invalid IDs not; nested JSON scan
      returns JSON paths; join-value allow-list honoured.
- [x] No vendored file contains CPT codes/descriptors or SNOMED content (guard run over every
      file; HCPCS file has no D-series).
- [x] Rights: one `REFERENCE_STANDARD_RIGHTS` entry per manifest standard; NUCC is
      `internal_only` with commercial_use `agreement_required` and the D9 note; `nppes` notes
      carry the NUCC/AMA terms; `cms_medicare_utilization` keeps the CPT/AMA note; no REVIEWED
      hash changes.

## Test Cases

| ID | Test | What it verifies |
|----|------|------------------|
| T1 | test_manifest_hashes_match_files | sha256 + size of every vendored file |
| T2 | test_manifest_entries_complete / test_no_unlisted_files | provenance fields; no stray file |
| T3 | test_fhir_* | resource count, Practitioner elements, references, datatype path walk |
| T4 | test_us_core_must_support | mustSupport subsets |
| T5 | test_plan_net_* | profiles, Network base, relations |
| T6 | test_omop_* | tables, field counts, FKs, prose columns absent |
| T7 | test_nucc_* | code count, tree depth, lookup |
| T8 | test_icd10cm_*, test_hcpcs_*, test_pos_* | code systems |
| T9 | test_guard_* | CPT / descriptor / SNOMED / CDT true and false positives, JSON scan |
| T10 | test_vendored_files_clean | no CPT / SNOMED / CDT in any vendored file |
| T11 | test_rights_* | reference-standard rights, nppes NUCC note, cms CPT note, REVIEWED untouched |

## Licence findings (verified live 2026-10-08)

See `app/ontology/standards/manifest.json` for each verbatim quote. Summary:
- FHIR R4: CC0 ("This document is licensed under Creative Commons \"No Rights Reserved\" (CC0)");
  third-party terminologies are not covered ("Acceptance of these License Terms does not grant
  any rights with respect to Third Party IP").
- US Core 9.0.0 / Plan-Net 1.2.0: `package.json` and the ImplementationGuide resource both say
  `"license": "CC0-1.0"`.
- OMOP CDM: repo `DESCRIPTION` at tag v5.4.3: "License: Apache License 2.0".
- NUCC 26.1: "For commercial use, including sales or licensing, a license must be obtained from
  this web site." / "Copyright 2026 American Medical Association" → internal only (D9).
- ICD-10-CM: WHO "is the copyright holder of ICD-10"; CDC: WHO "authorized NCHS to develop
  ICD-10-CM". No public-domain statement found → internal only, codes and chapter/block titles
  only; confirm with NCHS before external release.
- HCPCS L2: CMS record layout: Level I "Codes and descriptors copyrighted by the American Medical
  Association"; Level II "Includes codes and descriptors copyrighted by the American Dental
  Association's current dental terminology ... comprising the d series" → D-series excluded.
- CMS POS: US Government work (CMS web page); no reuse restriction found.

## Follow-ups

- SPEC_164/165: create `app/ontology/__init__.py` (the parent package) and have `guards.py`
  import `app.ontology.standards.licence_guard`.
- D9: request the NUCC commercial licence before any external release (owner).
- D10: confirm ICD-10-CM redistribution terms with NCHS before any external release.
- Load CPT descriptor set for guard (c) from `cms_medicare_utilization` read-only at run time
  (`licence_guard.load_cpt_descriptors(conn)`).
- `tests/test_nrel_resource_collector.py` fails to import at HEAD (`COUNTY_CENTROIDS`); not
  touched here.

## Vendored sizes (retrieved 2026-10-08; total 1,025,591 bytes)

| File | Bytes | Content |
|---|---:|---|
| `fhir_r4_resources.json` | 229,585 | 13 resources, snapshot elements |
| `fhir_r4_datatypes.json` | 22,445 | 12 complex datatypes |
| `us_core_9_0_0.json` | 36,937 | 7 profiles, mustSupport + references |
| `plan_net_1_2_0.json` | 65,951 | 9 profiles, mustSupport + references |
| `omop_cdm_v5_4_fields.csv` | 32,618 | 432 fields |
| `omop_cdm_v5_4_tables.csv` | 1,136 | 39 tables |
| `nucc_taxonomy_26_1.csv` | 117,931 | 883 codes |
| `icd10cm_2027_chapters.json` | 77,364 | 22 chapters, 297 blocks, 3-character categories |
| `hcpcs_l2_2026_oct.csv` | 437,694 | 8,770 Level II codes (D-series excluded) + 384 modifiers |
| `cms_pos.json` | 3,930 | 52 POS codes + F/O indicator |

## Test-pin changes

`tests/test_rights_batch_1.py` / `test_rights_batch_2.py` pin every dataset's full rights block
(notes included). `NOTES_ONLY_AFTER` (batch 2, imported by batch 1) lists `nppes_providers` and
`cms_medicare_utilization`; `test_later_notes_only` asserts only `notes` changed (appended).
No hashed field and no `REVIEWED` entry changed.
