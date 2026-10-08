"""
Rebuild the vendored reference standards (SPEC_163). Operator-run only; tests never call it.

    python -m app.ontology.standards.build --raw <scratch dir> [--retrieved YYYY-MM-DD]

Downloads each pinned source into ``--raw`` (skipped when the file is already there),
extracts the compact answer-key files next to this module and rewrites ``manifest.json``
(sha256, size, source URL, version, retrieved date, licence, licence URL, licence quote).

Fetching follows the project rules: honest NexdataResearch User-Agent, one request at a
time, >= 1 s between requests. Nothing here touches the database.

Licence rules applied in the extraction (PLAN_100 §3, §10):
- OMOP: structural CSV columns only; the prose columns (userGuidance, etlConventions,
  tableDescription ...) are dropped, so no documentation text is vendored.
- NUCC: code / grouping / classification / specialization / display name / section only
  (no definitions or notes); internal use only until the D9 licence is obtained.
- HCPCS: Level II only (the CMS alpha-numeric file has no CPT rows); the D-series (CDT,
  ADA copyright) is excluded; short descriptors only.
- ICD-10-CM: chapters, blocks (sections) and 3-character category codes; no category titles,
  no inclusion / exclusion notes.
- FHIR: snapshot path / cardinality / types / short / binding only; no mappings, examples,
  comments or definitions text (those are where third-party terminology content sits).
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import html
import io
import json
import re
import tarfile
import time
import xml.etree.ElementTree as ET
import zipfile
from datetime import date
from pathlib import Path
from typing import Any, Dict, List

HERE = Path(__file__).resolve().parent
UA = "NexdataResearch/1.0 (research@nexdata.com; respectful research bot)"

OMOP_TAG = "v5.4.3"
OMOP_SHA = "746a15e0fb36a95ba6cc0993737f1273bbad92f2"
_OMOP_RAW = f"https://raw.githubusercontent.com/OHDSI/CommonDataModel/{OMOP_SHA}"

RAW_SOURCES = {
    "fhir-r4-definitions.json.zip": "https://hl7.org/fhir/R4/definitions.json.zip",
    "uscore-9.0.0.tgz": "https://hl7.org/fhir/us/core/STU9/package.tgz",
    "plannet-1.2.0.tgz": "http://hl7.org/fhir/us/davinci-pdex-plan-net/STU1.2/package.tgz",
    "omop-field.csv": f"{_OMOP_RAW}/inst/csv/OMOP_CDMv5.4_Field_Level.csv",
    "omop-table.csv": f"{_OMOP_RAW}/inst/csv/OMOP_CDMv5.4_Table_Level.csv",
    "nucc_taxonomy_261.csv": "https://www.nucc.org/images/stories/CSV/nucc_taxonomy_261.csv",
    "icd10cm-table-and-index-2027.zip":
        "https://ftp.cdc.gov/pub/Health_Statistics/NCHS/Publications/ICD10CM/2027/icd10cm-table-and-index-2027.zip",
    "hcpcs-oct2026.zip": "https://www.cms.gov/files/zip/october-2026-alpha-numeric-hcpcs-file.zip",
    "pos.html": "https://www.cms.gov/medicare/coding-billing/place-of-service-codes/code-sets",
}

FHIR_RESOURCES = ("Patient", "Practitioner", "PractitionerRole", "Organization",
                  "OrganizationAffiliation", "Location", "HealthcareService", "Endpoint",
                  "InsurancePlan", "Coverage", "Encounter", "Claim", "ExplanationOfBenefit")
US_CORE_PROFILES = ("us-core-patient", "us-core-practitioner", "us-core-practitionerrole",
                    "us-core-organization", "us-core-location", "us-core-encounter",
                    "us-core-coverage")
PLAN_NET_PROFILES = ("plannet-PractitionerRole", "plannet-OrganizationAffiliation",
                     "plannet-HealthcareService", "plannet-Endpoint", "plannet-Network",
                     "plannet-Practitioner", "plannet-Organization", "plannet-Location",
                     "plannet-InsurancePlan")
OMOP_FIELD_COLUMNS = ("cdmTableName", "cdmFieldName", "isRequired", "cdmDatatype", "isPrimaryKey",
                      "isForeignKey", "fkTableName", "fkFieldName", "fkDomain", "fkClass")
OMOP_TABLE_COLUMNS = ("cdmTableName", "schema", "isRequired", "conceptPrefix")
NUCC_COLUMNS = ("Code", "Grouping", "Classification", "Specialization", "Display Name", "Section")
# Infrastructure elements every resource carries; kept, but flagged so answer keys can skip them.
_INFRA_LEAVES = {"id", "meta", "implicitRules", "language", "text", "contained", "extension",
                 "modifierExtension"}


# ---------------------------------------------------------------------------
# fetch
# ---------------------------------------------------------------------------

def fetch(raw: Path) -> None:
    import httpx

    raw.mkdir(parents=True, exist_ok=True)
    with httpx.Client(headers={"User-Agent": UA}, follow_redirects=True, timeout=180) as client:
        for name, url in RAW_SOURCES.items():
            if (raw / name).exists():
                continue
            resp = client.get(url)
            resp.raise_for_status()
            (raw / name).write_bytes(resp.content)
            time.sleep(1.0)


# ---------------------------------------------------------------------------
# FHIR
# ---------------------------------------------------------------------------

def _types(el: Dict[str, Any]) -> List[Dict[str, Any]]:
    out = []
    for t in el.get("type") or []:
        item: Dict[str, Any] = {"code": t["code"]}
        if t.get("targetProfile"):
            item["targetProfile"] = sorted(t["targetProfile"])
        if t.get("profile"):
            item["profile"] = sorted(t["profile"])
        out.append(item)
    return out


def _element(el: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {"path": el["path"], "min": el.get("min", 0), "max": el.get("max", "*")}
    types = _types(el)
    if types:
        out["type"] = types
    if el.get("contentReference"):
        out["contentReference"] = el["contentReference"]
    if el.get("short"):
        out["short"] = el["short"]
    b = el.get("binding")
    if b and b.get("valueSet"):
        out["binding"] = {"strength": b.get("strength"), "valueSet": b["valueSet"]}
    leaf = el["path"].rsplit(".", 1)[-1]
    if "." in el["path"] and leaf in _INFRA_LEAVES:
        out["infra"] = True
    return out


def _bundle_sds(zf: zipfile.ZipFile, member: str) -> Dict[str, Dict[str, Any]]:
    bundle = json.loads(zf.read(member))
    sds = {}
    for e in bundle["entry"]:
        r = e["resource"]
        if r.get("resourceType") == "StructureDefinition":
            sds[r["id"]] = r
    return sds


def build_fhir(raw: Path) -> Dict[str, Any]:
    zf = zipfile.ZipFile(raw / "fhir-r4-definitions.json.zip")
    version_info = zf.read("version.info").decode()
    resources = _bundle_sds(zf, "profiles-resources.json")
    types = _bundle_sds(zf, "profiles-types.json")

    res_out = {}
    for name in FHIR_RESOURCES:
        sd = resources[name]
        res_out[name] = {"url": sd["url"], "elements": [_element(e) for e in sd["snapshot"]["element"]]}

    # complex datatypes reachable from those resources (transitively)
    needed, queue = set(), []
    for r in res_out.values():
        for e in r["elements"]:
            for t in e.get("type", []):
                queue.append(t["code"])
    while queue:
        code = queue.pop()
        if code in needed or code not in types:
            continue
        sd = types[code]
        if sd.get("kind") != "complex-type" or code in ("Extension", "Element", "BackboneElement",
                                                        "Narrative", "Meta"):
            continue
        needed.add(code)
        for e in sd["snapshot"]["element"][1:]:
            for t in e.get("type") or []:
                queue.append(t["code"])
    dt_out = {}
    for code in sorted(needed):
        sd = types[code]
        dt_out[code] = {"url": sd["url"], "elements": [_element(e) for e in sd["snapshot"]["element"]]}

    fhir_version = re.search(r"version=([\d.]+)", version_info).group(1)
    return {"resources": {"fhir_version": fhir_version, "resources": res_out},
            "datatypes": {"fhir_version": fhir_version, "datatypes": dt_out}}


def _ig_profiles(tgz: Path, ids) -> Dict[str, Any]:
    tf = tarfile.open(tgz)
    pkg = json.load(tf.extractfile("package/package.json"))
    out = {}
    for pid in ids:
        sd = json.load(tf.extractfile(f"package/StructureDefinition-{pid}.json"))
        ms, refs = [], []
        for el in sd["snapshot"]["element"]:
            entry = {"id": el["id"], "path": el["path"], "min": el.get("min", 0), "max": el.get("max", "*")}
            types = _types(el)
            if types:
                entry["type"] = types
            if el.get("mustSupport"):
                ms.append(entry)
            for t in types:
                if t["code"] == "Reference" and t.get("targetProfile"):
                    refs.append({"path": el["path"], "id": el["id"], "targetProfile": t["targetProfile"],
                                 "mustSupport": bool(el.get("mustSupport"))})
        out[pid] = {"url": sd["url"], "type": sd["type"], "baseDefinition": sd.get("baseDefinition"),
                    "title": sd.get("title"), "mustSupport": ms, "references": refs}
    return {"package": pkg["name"], "version": pkg["version"], "fhirVersions": pkg.get("fhirVersions"),
            "license": pkg.get("license"), "profiles": out}


# ---------------------------------------------------------------------------
# OMOP / NUCC
# ---------------------------------------------------------------------------

def _csv_subset(src: bytes, columns, encoding="utf-8-sig") -> str:
    rows = list(csv.DictReader(io.StringIO(src.decode(encoding))))
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    w.writerow(columns)
    for r in rows:
        w.writerow([(r.get(c) or "").strip() for c in columns])
    return buf.getvalue()


# ---------------------------------------------------------------------------
# ICD-10-CM
# ---------------------------------------------------------------------------

def build_icd(raw: Path) -> Dict[str, Any]:
    zf = zipfile.ZipFile(raw / "icd10cm-table-and-index-2027.zip")
    member = next(n for n in zf.namelist() if re.search(r"icd10cm-tabular.*\.xml$", n))
    root = ET.fromstring(zf.read(member))
    chapters = []
    for ch in root.findall("chapter"):
        desc = " ".join(ch.find("desc").text.split())
        m = re.search(r"\(([A-Z][0-9A-Z]{2})-([A-Z][0-9A-Z]{2})\)\s*$", desc)
        blocks = []
        for sec in ch.findall("section"):
            sdesc = " ".join(sec.find("desc").text.split())
            cats = sorted({d.find("name").text.strip() for d in sec.findall("diag")})
            first, _, last = sec.get("id").partition("-")
            blocks.append({"id": sec.get("id"), "first": first, "last": last or first,
                           "title": re.sub(r"\s*\([A-Z0-9.\-]+\)\s*$", "", sdesc), "categories": cats})
        chapters.append({"chapter": int(ch.find("name").text), "first": m.group(1), "last": m.group(2),
                         "title": desc[:m.start()].strip(), "blocks": blocks})
    return {"code_system": "ICD-10-CM", "version": f"FY{root.find('version').text}",
            "level": "chapter / block / 3-character category code (no category titles)",
            "chapters": chapters}


# ---------------------------------------------------------------------------
# HCPCS Level II
# ---------------------------------------------------------------------------

def build_hcpcs(raw: Path) -> str:
    zf = zipfile.ZipFile(raw / "hcpcs-oct2026.zip")
    member = next(n for n in zf.namelist() if re.search(r"ANWEB_\d+\.txt$", n))
    lines = zf.read(member).decode("latin-1").splitlines()
    rows: Dict[tuple, Dict[str, str]] = {}
    for ln in lines:
        ln = ln.ljust(320)
        rec_id = ln[10]
        if rec_id not in ("3", "7"):          # 3 = procedure first line, 7 = modifier first line
            continue
        code, modifier = ln[0:5].strip(), ln[3:5].strip()
        if rec_id == "3":
            kind, key = "code", code
        else:
            kind, key = "modifier", modifier
        if not key:
            continue
        if kind == "code" and (key.startswith("D") or key.isdigit()):
            continue                           # CDT (ADA) and any Level I shape: never vendored
        if kind == "modifier" and key.isdigit():
            continue                           # numeric modifiers are CPT Level I
        rows[(kind, key)] = {"kind": kind, "code": key, "short_description": ln[91:119].strip(),
                             "added": ln[268:276].strip(), "terminated": ln[284:292].strip()}
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    w.writerow(["kind", "code", "short_description", "added", "terminated"])
    for k in sorted(rows):
        r = rows[k]
        w.writerow([r["kind"], r["code"], r["short_description"], r["added"], r["terminated"]])
    return buf.getvalue()


# ---------------------------------------------------------------------------
# CMS Place of Service
# ---------------------------------------------------------------------------

def build_pos(raw: Path) -> Dict[str, Any]:
    data = (raw / "pos.html").read_bytes()
    try:
        page = data.decode("utf-8")
    except UnicodeDecodeError:
        page = data.decode("cp1252")
    i = page.find("<table")
    table = page[i:page.find("</table>", i)]
    codes, unassigned = [], []
    for tr in re.findall(r"<tr.*?</tr>", table, flags=re.S)[1:]:
        cells = [" ".join(html.unescape(re.sub(r"<[^>]+>", " ", c)).split())
                 for c in re.findall(r"<t[dh].*?</t[dh]>", tr, flags=re.S)]
        if len(cells) < 2:
            continue
        code, name = cells[0], cells[1].replace("’", "'")
        if name.lower() == "unassigned" or "-" in code:
            unassigned.append(code)
            continue
        codes.append({"code": code.zfill(2), "name": name})
    return {"code_system": "CMS Place of Service", "codes": codes, "unassigned": unassigned,
            "medicare_facility_indicator": {
                "F": "Facility", "O": "Non-facility (office / other)",
                "_note": "place_of_srvc in cms_medicare_utilization (CMS Physician & Other Practitioners "
                         "PUF data dictionary); not a POS code."}}


# ---------------------------------------------------------------------------
# write + manifest
# ---------------------------------------------------------------------------

def _dump_json(obj: Any) -> bytes:
    return (json.dumps(obj, indent=1, sort_keys=True, ensure_ascii=False) + "\n").encode("utf-8")


def build(raw: Path, retrieved: str) -> Dict[str, int]:
    fhir = build_fhir(raw)
    outputs: Dict[str, bytes] = {
        "fhir_r4_resources.json": _dump_json(fhir["resources"]),
        "fhir_r4_datatypes.json": _dump_json(fhir["datatypes"]),
        "us_core_9_0_0.json": _dump_json(_ig_profiles(raw / "uscore-9.0.0.tgz", US_CORE_PROFILES)),
        "plan_net_1_2_0.json": _dump_json(_ig_profiles(raw / "plannet-1.2.0.tgz", PLAN_NET_PROFILES)),
        "omop_cdm_v5_4_fields.csv": _csv_subset((raw / "omop-field.csv").read_bytes(),
                                                OMOP_FIELD_COLUMNS).encode("utf-8"),
        "omop_cdm_v5_4_tables.csv": _csv_subset((raw / "omop-table.csv").read_bytes(),
                                                OMOP_TABLE_COLUMNS).encode("utf-8"),
        "nucc_taxonomy_26_1.csv": _csv_subset((raw / "nucc_taxonomy_261.csv").read_bytes(), NUCC_COLUMNS
                                              ).encode("utf-8"),
        "icd10cm_2027_chapters.json": _dump_json(build_icd(raw)),
        "hcpcs_l2_2026_oct.csv": build_hcpcs(raw).encode("utf-8"),
        "cms_pos.json": _dump_json(build_pos(raw)),
    }
    manifest = json.loads((HERE / "manifest.json").read_text(encoding="utf-8"))
    sizes = {}
    for name, blob in outputs.items():
        (HERE / name).write_bytes(blob)
        entry = manifest["files"][name]
        entry["sha256"] = hashlib.sha256(blob).hexdigest()
        entry["bytes"] = len(blob)
        entry["retrieved"] = retrieved
        sizes[name] = len(blob)
    manifest["total_bytes"] = sum(e["bytes"] for e in manifest["files"].values())
    (HERE / "manifest.json").write_bytes(_dump_json(manifest))
    return sizes


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--raw", required=True, type=Path, help="scratch dir for the raw downloads")
    ap.add_argument("--retrieved", default=date.today().isoformat())
    ap.add_argument("--no-fetch", action="store_true", help="use the raw files already in --raw")
    args = ap.parse_args()
    if not args.no_fetch:
        fetch(args.raw)
    for name, size in build(args.raw, args.retrieved).items():
        print(f"{size:>9}  {name}")


if __name__ == "__main__":
    main()
