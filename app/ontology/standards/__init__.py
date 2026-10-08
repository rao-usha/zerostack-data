"""
Vendored reference standards: the ontology scorer's answer keys (SPEC_163, PLAN_100 §3, §6).

Every data file next to this module is a compact extract listed in ``manifest.json``
(source URL, version, retrieved date, sha256, licence, licence URL, verbatim licence quote,
``use``). ``verify()`` re-hashes them; ``build.py`` regenerates them from the pinned sources.

Answer keys (all pure, cached, offline):

- **concepts**: ``fhir_concepts()`` (13 FHIR R4 resources), ``omop_tables()``.
- **attributes**: ``fhir_attributes(resource)`` (element path, types, min/max, short,
  mustSupport per US Core / Plan-Net), ``omop_fields(table)`` (field, datatype, required, PK).
- **typed relations**: ``fhir_relations()`` (Reference targetProfile -> target resource),
  ``plan_net_relations()``, ``omop_relations()`` (FK field -> table.field).
- **code systems**: ``nucc_tree()`` / ``nucc_lookup()`` (Grouping > Classification >
  Specialization), ``icd10cm_chapters()`` / ``icd10cm_blocks()``, ``hcpcs_l2_codes()``,
  ``pos_codes()``, and ``code_exists(system, code)`` for G4 lookups.
- **existence checks** for G4 / guard 3: ``fhir_path_exists(path)`` walks datatypes
  (``Practitioner.name.family``), ``omop_field_exists(table, field)``.

Licence notes: NUCC and ICD-10-CM extracts are ``use: internal_only`` (manifest
``licence_action``: D9 NUCC licence, D10 NCHS terms) -- never ship them, or anything that
carries their text, outside Nexdata until those actions are done. CPT / SNOMED / CDT are
never vendored; ``licence_guard`` detects them in text.

This is a sub-package of ``app.ontology`` without an ``app/ontology/__init__.py`` (SPEC_164/165
own the parent package); it imports as a namespace sub-package until then.
"""

from __future__ import annotations

import csv
import hashlib
import json
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

HERE = Path(__file__).resolve().parent

FHIR_RESOURCES = ("Patient", "Practitioner", "PractitionerRole", "Organization",
                  "OrganizationAffiliation", "Location", "HealthcareService", "Endpoint",
                  "InsurancePlan", "Coverage", "Encounter", "Claim", "ExplanationOfBenefit")
CODE_SYSTEMS = ("nucc", "icd10cm", "hcpcs_l2", "pos")


class StandardsIntegrityError(RuntimeError):
    """A vendored file does not match its manifest entry."""


# ---------------------------------------------------------------------------
# manifest + integrity
# ---------------------------------------------------------------------------

@lru_cache(maxsize=1)
def manifest() -> Dict[str, Any]:
    return json.loads((HERE / "manifest.json").read_text(encoding="utf-8"))


def verify() -> Dict[str, str]:
    """Re-hash every manifest file; raises StandardsIntegrityError on any mismatch."""
    out = {}
    for name, entry in manifest()["files"].items():
        blob = (HERE / name).read_bytes()
        digest = hashlib.sha256(blob).hexdigest()
        if digest != entry["sha256"] or len(blob) != entry["bytes"]:
            raise StandardsIntegrityError(f"{name}: sha256/size differ from manifest.json")
        out[name] = digest
    return out


def versions() -> Dict[str, str]:
    """``{standard: version}`` -- recorded on every brief (``onto.brief.standards_versions``)."""
    return {e["standard"]: e["version"] for e in manifest()["files"].values()}


def _json(name: str) -> Any:
    return json.loads((HERE / name).read_text(encoding="utf-8"))


def _csv(name: str) -> List[Dict[str, str]]:
    with open(HERE / name, encoding="utf-8", newline="") as fh:
        return list(csv.DictReader(fh))


# ---------------------------------------------------------------------------
# FHIR R4 + US Core + Plan-Net
# ---------------------------------------------------------------------------

@lru_cache(maxsize=1)
def _fhir() -> Dict[str, Any]:
    return _json("fhir_r4_resources.json")["resources"]


@lru_cache(maxsize=1)
def _fhir_types() -> Dict[str, Any]:
    return _json("fhir_r4_datatypes.json")["datatypes"]


@lru_cache(maxsize=1)
def _us_core() -> Dict[str, Any]:
    return _json("us_core_9_0_0.json")


@lru_cache(maxsize=1)
def _plan_net() -> Dict[str, Any]:
    return _json("plan_net_1_2_0.json")


def fhir_concepts() -> List[str]:
    """The FHIR R4 resources in the answer key (concepts)."""
    return list(FHIR_RESOURCES)


def _target_name(url: str) -> Tuple[str, str]:
    """``(resource_type, profile_name)`` for a targetProfile URL (core, US Core or Plan-Net)."""
    tail = url.rstrip("/").rsplit("/", 1)[-1]
    if tail in FHIR_RESOURCES or url.startswith("http://hl7.org/fhir/StructureDefinition/"):
        return tail, tail
    for bundle in (_plan_net(), _us_core()):
        prof = bundle["profiles"].get(tail)
        if prof:
            name = tail.split("-", 1)[1] if tail.startswith("plannet-") else prof["type"]
            return prof["type"], name
    return tail, tail


def us_core_must_support(resource: str) -> List[str]:
    """US Core 9.0.0 mustSupport element ids (slices included, e.g. ``Practitioner.identifier:NPI``)."""
    for prof in _us_core()["profiles"].values():
        if prof["type"] == resource:
            return [e["id"] for e in prof["mustSupport"]]
    return []


def plan_net_profiles() -> Dict[str, Dict[str, Any]]:
    """Plan-Net 1.2.0 profiles keyed by short name (``Network``, ``PractitionerRole`` ...)."""
    out = {}
    for pid, prof in _plan_net()["profiles"].items():
        out[pid.split("-", 1)[1]] = {"id": pid, "url": prof["url"], "type": prof["type"],
                                     "baseDefinition": prof["baseDefinition"],
                                     "mustSupport": [e["id"] for e in prof["mustSupport"]]}
    return out


def plan_net_must_support(resource: str) -> List[str]:
    """Plan-Net mustSupport element ids of the profile on ``resource`` (Network excluded)."""
    prof = plan_net_profiles().get(resource)
    return list(prof["mustSupport"]) if prof else []


def fhir_attributes(resource: str, include_infra: bool = False) -> List[Dict[str, Any]]:
    """Element answer key for one resource: path, min, max, types, short, and whether US Core
    9.0.0 / Plan-Net 1.2.0 flag it mustSupport (by path; slices roll up to their path)."""
    if resource not in _fhir():
        raise KeyError(f"{resource!r} is not in the vendored FHIR resources")
    us = {i.split(":")[0] for i in us_core_must_support(resource)}
    pn = {i.split(":")[0] for i in plan_net_must_support(resource)}
    out = []
    for e in _fhir()[resource]["elements"][1:]:
        if e.get("infra") and not include_infra:
            continue
        out.append({"path": e["path"], "min": e["min"], "max": e["max"],
                    "types": [t["code"] for t in e.get("type", [])],
                    "short": e.get("short"), "contentReference": e.get("contentReference"),
                    "must_support_us_core": e["path"] in us, "must_support_plan_net": e["path"] in pn})
    return out


def fhir_relations() -> List[Dict[str, Any]]:
    """Typed relations of the core spec: every Reference element with its target resources."""
    out = []
    for res in FHIR_RESOURCES:
        for e in _fhir()[res]["elements"]:
            if e.get("infra"):
                continue
            for t in e.get("type", []):
                if t["code"] == "Reference":
                    targets = sorted({_target_name(u)[0] for u in t.get("targetProfile", [])})
                    out.append({"source": res, "path": e["path"], "targets": targets,
                                "min": e["min"], "max": e["max"]})
    return out


def plan_net_relations() -> List[Dict[str, Any]]:
    """Plan-Net gold relation set: profile -> path -> target profile names (``Network`` kept
    distinct from ``Organization``)."""
    out = []
    for name, prof in sorted(plan_net_profiles().items()):
        for r in _plan_net()["profiles"][prof["id"]]["references"]:
            if ".extension" in r["path"] or r["path"].endswith(".assigner"):
                continue
            out.append({"source": name, "source_type": prof["type"], "path": r["path"],
                        "targets": sorted({_target_name(u)[1] for u in r["targetProfile"]}),
                        "must_support": r["mustSupport"]})
    return out


def _elements_by_path(owner: str) -> Dict[str, Dict[str, Any]]:
    src = _fhir().get(owner) or _fhir_types().get(owner)
    return {e["path"]: e for e in src["elements"]} if src else {}


def fhir_path_exists(path: str) -> bool:
    """True when ``path`` is a real element: a resource path, descending through backbone
    elements, contentReference and complex datatypes (``Practitioner.name.family``).
    Choice elements accept the ``[x]`` form and typed variants (``multipleBirthBoolean``)."""
    parts = (path or "").split(".")
    if parts[0] not in _fhir():
        return False
    owner = base = parts[0]
    for i, leaf in enumerate(parts[1:], start=1):
        elements = _elements_by_path(owner)
        el, chosen = elements.get(f"{base}.{leaf}"), None
        if el is None:
            el, chosen = _choice_match(elements, base, leaf)
        if el is None:
            return False
        if i == len(parts) - 1:
            return True
        if el.get("contentReference"):
            base = el["contentReference"].lstrip("#")
            continue
        types = [chosen] if chosen else [t["code"] for t in el.get("type", [])]
        if not types or types[0] in ("BackboneElement", "Element"):
            base = el["path"]
            continue
        if len(types) == 1 and types[0] in _fhir_types():
            owner = base = types[0]
            continue
        return False
    return True


def _choice_match(elements: Dict[str, Any], base: str, leaf: str) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    for p, e in elements.items():
        if not p.endswith("[x]") or p.rsplit(".", 1)[0] != base:
            continue
        stem = p.rsplit(".", 1)[1][:-3]
        if leaf == stem + "[x]":
            return e, None
        for t in e.get("type", []):
            if leaf == stem + t["code"][0].upper() + t["code"][1:]:
                return e, t["code"]
    return None, None


# ---------------------------------------------------------------------------
# OMOP CDM v5.4
# ---------------------------------------------------------------------------

@lru_cache(maxsize=1)
def _omop_fields() -> List[Dict[str, str]]:
    return _csv("omop_cdm_v5_4_fields.csv")


def omop_tables() -> List[Dict[str, Any]]:
    return [{"table": r["cdmTableName"].lower(), "schema": r["schema"],
             "required": r["isRequired"].lower() == "true"} for r in _csv("omop_cdm_v5_4_tables.csv")]


def omop_fields(table: str) -> List[Dict[str, Any]]:
    t = table.lower()
    return [{"field": r["cdmFieldName"].lower(), "datatype": r["cdmDatatype"],
             "required": r["isRequired"].upper() == "TRUE", "primary_key": r["isPrimaryKey"].upper() == "TRUE",
             "foreign_key": r["isForeignKey"].upper() == "TRUE"}
            for r in _omop_fields() if r["cdmTableName"].lower() == t]


def omop_relations() -> List[Dict[str, Any]]:
    """FK answer key: (table.field) -> (fk_table.fk_field), with the vocabulary domain if any."""
    out = []
    for r in _omop_fields():
        if r["isForeignKey"].upper() != "TRUE":
            continue
        out.append({"table": r["cdmTableName"].lower(), "field": r["cdmFieldName"].lower(),
                    "fk_table": r["fkTableName"].lower(), "fk_field": r["fkFieldName"].lower(),
                    "fk_domain": None if r["fkDomain"] in ("", "NA") else r["fkDomain"]})
    return out


def omop_field_exists(table: str, field: Optional[str] = None) -> bool:
    t = table.lower()
    if field is None:
        return any(x["table"] == t for x in omop_tables())
    return any(r["cdmTableName"].lower() == t and r["cdmFieldName"].lower() == field.lower()
               for r in _omop_fields())


# ---------------------------------------------------------------------------
# code systems
# ---------------------------------------------------------------------------

@lru_cache(maxsize=1)
def _nucc() -> Dict[str, Dict[str, str]]:
    return {r["Code"]: {"code": r["Code"], "grouping": r["Grouping"], "classification": r["Classification"],
                        "specialization": r["Specialization"] or None, "display_name": r["Display Name"],
                        "section": r["Section"]}
            for r in _csv("nucc_taxonomy_26_1.csv")}


def nucc_codes() -> List[str]:
    return sorted(_nucc())


def nucc_lookup(code: str) -> Optional[Dict[str, str]]:
    """Grouping / classification / specialization for a taxonomy code (CQ04), or None."""
    return _nucc().get((code or "").strip().upper())


def nucc_tree() -> Dict[str, Dict[str, Dict[str, Any]]]:
    """``{grouping: {classification: {"codes": [...], "specializations": {name: code}}}}``.
    Depth is 3: Grouping > Classification > Specialization."""
    tree: Dict[str, Dict[str, Dict[str, Any]]] = {}
    for r in _nucc().values():
        node = tree.setdefault(r["grouping"], {}).setdefault(
            r["classification"], {"codes": [], "specializations": {}})
        if r["specialization"]:
            node["specializations"][r["specialization"]] = r["code"]
        else:
            node["codes"].append(r["code"])
    return tree


@lru_cache(maxsize=1)
def _icd() -> Dict[str, Any]:
    return _json("icd10cm_2027_chapters.json")


def icd10cm_chapters() -> List[Dict[str, Any]]:
    return [{k: c[k] for k in ("chapter", "first", "last", "title")} for c in _icd()["chapters"]]


def icd10cm_blocks() -> List[Dict[str, Any]]:
    return [{"chapter": c["chapter"], **{k: b[k] for k in ("id", "first", "last", "title")},
             "categories": list(b["categories"])}
            for c in _icd()["chapters"] for b in c["blocks"]]


@lru_cache(maxsize=1)
def _icd_categories() -> Dict[str, Tuple[int, str]]:
    out = {}
    for c in _icd()["chapters"]:
        for b in c["blocks"]:
            for cat in b["categories"]:
                out[cat] = (c["chapter"], b["id"])
    return out


def icd10cm_category_exists(code: str) -> bool:
    """True when the first 3 characters of ``code`` (dot optional) are a FY2027 category.
    Only the category level is vendored: full-code validity is not checked."""
    c = (code or "").replace(".", "").strip().upper()[:3]
    return c in _icd_categories()


def icd10cm_chapter_of(code: str) -> Optional[int]:
    hit = _icd_categories().get((code or "").replace(".", "").strip().upper()[:3])
    return hit[0] if hit else None


@lru_cache(maxsize=1)
def _hcpcs() -> Dict[Tuple[str, str], Dict[str, str]]:
    return {(r["kind"], r["code"]): r for r in _csv("hcpcs_l2_2026_oct.csv")}


def hcpcs_l2_codes(include_terminated: bool = True) -> List[str]:
    return sorted(c for (k, c), r in _hcpcs().items()
                  if k == "code" and (include_terminated or not r["terminated"]))


def hcpcs_l2_modifiers() -> List[str]:
    return sorted(c for (k, c) in _hcpcs() if k == "modifier")


def hcpcs_l2_lookup(code: str) -> Optional[Dict[str, str]]:
    return _hcpcs().get(("code", (code or "").strip().upper()))


@lru_cache(maxsize=1)
def _pos() -> Dict[str, Any]:
    return _json("cms_pos.json")


def pos_codes() -> Dict[str, str]:
    return {c["code"]: c["name"] for c in _pos()["codes"]}


def medicare_facility_indicator() -> Dict[str, str]:
    return {k: v for k, v in _pos()["medicare_facility_indicator"].items() if not k.startswith("_")}


def code_exists(system: str, code: str) -> bool:
    """G4 lookup. ``system`` in CODE_SYSTEMS. CPT / SNOMED are never answerable here."""
    code = (code or "").strip()
    if system == "nucc":
        return nucc_lookup(code) is not None
    if system == "icd10cm":
        return icd10cm_category_exists(code)
    if system == "hcpcs_l2":
        return hcpcs_l2_lookup(code) is not None
    if system == "pos":
        return code.zfill(2) in pos_codes()
    raise ValueError(f"unknown code system {system!r}; expected one of {CODE_SYSTEMS}")
