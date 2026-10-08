"""
CQ label probes for the healthcare gold set (SPEC_162, PLAN_100 Appendix B).

    python -m app.ontology.gold.probes            # run every probe read-only, print labels + tally
    python -m app.ontology.gold.probes --json     # machine-readable

Each competency question has one SQL probe (``healthcare_provider/cq_probes.sql``) that returns
one row of counts, and one fixed rule below that turns the row into a label:

- HOLD: the data to answer it is loaded and populated;
- PARTIAL: some of it (a sample, a missing column, a weak derivation);
- NEED: new data is required;
- SCHEMA: patient-level questions answered by schema only (CQ28-30; no patient data, ever).

The session is forced read-only with a statement timeout; nothing is ever written. The module
imports nothing from ``app.ontology`` so it can be copied next to the SQL and run on its own.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any, Callable, Dict, List, Mapping, Optional, Tuple

DOMAIN_DIR = Path(__file__).resolve().parent / "healthcare_provider"
PROBES_FILE = DOMAIN_DIR / "cq_probes.sql"
LABELS = ("HOLD", "PARTIAL", "NEED", "SCHEMA")
GROUNDABLE = ("HOLD", "PARTIAL")

# A provider dataset this size or larger is treated as the full registry / national file, not
# a sample (NPPES ~8.9M NPIs; the utilization file ~1.2M rendering NPIs per year).
FULL_NPPES = 7_000_000
FULL_UTILIZATION_NPIS = 1_000_000

_NAME_RE = re.compile(r"^--\s*name:\s*(CQ\d{2})\s*$", re.M)


def load_probes(path: Path = PROBES_FILE) -> Dict[str, str]:
    """``-- name: CQnn`` blocks -> SQL (comments outside blocks dropped, trailing ';' removed)."""
    text = path.read_text(encoding="utf-8")
    parts = _NAME_RE.split(text)
    out: Dict[str, str] = {}
    for i in range(1, len(parts), 2):
        sql = parts[i + 1].strip().rstrip(";").strip()
        out[parts[i]] = sql
    return out


def _share(num: Any, den: Any) -> float:
    return float(num or 0) / float(den) if den else 0.0


def _exists(key: str, present: str = "PARTIAL") -> Callable[[Mapping[str, Any]], Tuple[str, str]]:
    """NEED unless a candidate table appeared (then re-check by hand: PARTIAL)."""
    def rule(m: Mapping[str, Any]) -> Tuple[str, str]:
        if m[key]:
            return present, f"{m[key]} candidate table(s) present: re-check the label by hand"
        return "NEED", "no source table loaded"
    return rule


def _cq01(m):
    core = min(_share(m["has_type"], m["n"]), _share(m["has_enum_date"], m["n"]),
               _share(m["has_last_updated"], m["n"]))
    if not m["n"] or core < 0.95:
        return "NEED", "type / enumeration / update dates not populated"
    if m["non_active"] and m["has_dba"]:
        return "HOLD", "type, dates, deactivations and DBA present"
    missing = []
    if not m["non_active"]:
        missing.append("every row is status 'A' (no deactivations)")
    if not m["has_dba"]:
        missing.append("dba_name NULL on every row")
    return "PARTIAL", "; ".join(missing)


def _cq02(m):
    tax = _share(m["has_taxonomy"], m["n_individual"])
    if not m["n_individual"] or tax < 0.95:
        return "NEED", "primary taxonomy not populated"
    if _share(m["has_credential"], m["n_individual"]) >= 0.5 and _share(m["has_license_state"], m["n_individual"]) >= 0.5:
        return "HOLD", "credential, primary taxonomy and licence state (primary only)"
    return "PARTIAL", "taxonomy present, credential or licence state sparse"


def _cq03(m):
    return ("HOLD", "multi-taxonomy table present") if m["multi_taxonomy_tables"] else \
        ("NEED", "primary taxonomy only; no table of all 15 taxonomy slots")


def _cq04(m):
    return ("HOLD", "NUCC hierarchy table present") if m["nucc_tables"] else \
        ("NEED", "no NUCC grouping/classification/specialization table (SPEC_163 vendors it)")


def _cq05(m):
    if m["sharing_individuals"]:
        return "HOLD", f"{m['sharing_individuals']} individuals share an address+ZIP or phone with an org (derived, low confidence)"
    return "NEED", "no shared address / phone found"


def _cq08(m):
    if m["has_subpart_flag"] and m["parent_columns"]:
        return "HOLD", "subpart flag and parent present"
    if _share(m["has_subpart_flag"], m["n_org"]) >= 0.5:
        return "PARTIAL", "subpart flag only; parent organization not loaded"
    return "NEED", f"organization_subpart populated on {m['has_subpart_flag']} of {m['n_org']} type-2 rows; no parent column"


def _cq09(m):
    return ("PARTIAL", "a TIN/EIN column sits beside an NPI") if m["npi_tables_with_tin"] else \
        ("NEED", "no EIN/TIN for any NPI (NPPES publishes none)")


def _cq10(m):
    if m["crosswalk_rows"] >= 1000:
        return "HOLD", f"{m['crosswalk_rows']} CCN-NPI rows"
    if m["crosswalk_rows"]:
        return "PARTIAL", f"{m['crosswalk_rows']} CCN-NPI rows"
    return "NEED", "no CCN-NPI pair (cost-report npi / prvdr_num all NULL; cms_hospitals has no NPI)"


def _cq11(m):
    if not m["n"] or _share(m["has_zip"], m["n"]) < 0.95:
        return "NEED", "practice ZIP not populated"
    if m["crosswalk_tables"] or _share(m["has_ruca"], m["n"]) >= 0.95:
        return "HOLD", "address, ZIP, state and county/RUCA"
    return "PARTIAL", (f"address/ZIP/state held; county/RUCA only for {m['has_ruca']} of {m['n']} NPIs "
                       "(utilization), no ZIP-county crosswalk")


def _cq12(m):
    if not m["n"]:
        return "NEED", "no providers"
    if m["n"] >= FULL_NPPES:
        return "HOLD", "full registry"
    return "PARTIAL", f"{m['n']} NPIs: a targeted sample, so counts per ZIP are biased; no per-capita join"


def _cq14(m):
    if not m["n"]:
        return "NEED", "no hospitals"
    typ = min(_share(m["has_type"], m["n"]), _share(m["has_emergency"], m["n"]))
    if typ >= 0.95 and _share(m["has_county"], m["n"]) >= 0.95:
        return "HOLD", "type, emergency services and county"
    if typ >= 0.95:
        return "PARTIAL", f"type and emergency held; county populated on {m['has_county']} of {m['n']} rows"
    return "NEED", "type / emergency not populated"


def _cq15(m):
    if m["npis"] and _share(m["has_assignment_flag"], m["npis"]) >= 0.95:
        return "HOLD", f"assignment flag for {m['npis']} sampled NPIs"
    return "NEED", "no assignment flag"


def _cq18(m):
    if not m["n"] or not m["has_allowed"]:
        return "NEED", "no utilization rows"
    issues = []
    if not m["has_year_key"]:
        issues.append("no data_year / unique key yet (duplicates; alembic 0022 not applied)")
    if m["npis"] < FULL_UTILIZATION_NPIS:
        issues.append(f"sample of {m['npis']} NPIs")
    return ("PARTIAL", "; ".join(issues)) if issues else ("HOLD", "full national file, keyed by year")


def _cq19(m):
    if not m["npis"] or not m["provider_types"] or not m["places"]:
        return "NEED", "no utilization rows"
    if m["npis"] >= FULL_UTILIZATION_NPIS:
        return "HOLD", "full national file"
    return "PARTIAL", f"{m['provider_types']} provider types, {m['states']} states, sample of {m['npis']} NPIs"


def _cq20(m):
    if m["n"] >= 1000 and m["has_beds"] and m["has_costs"] and m["has_net_income"]:
        return "HOLD", "cost reports populated"
    if m["has_beds"] or m["has_costs"]:
        return "PARTIAL", "some cost-report values"
    return "NEED", f"{m['n']} cost-report rows, every data column NULL"


def _cq23(m):
    if m["npi_link_columns"]:
        return "HOLD", "an NPI <-> portfolio link exists"
    if m["name_matched_companies"]:
        return "PARTIAL", (f"{m['name_matched_companies']} of {m['portfolio_companies']} portfolio companies "
                           "match a type-2 NPI legal name exactly (unreviewed, low confidence)")
    return "NEED", "no NPI <-> portfolio link and no exact name match"


def _cq25(m):
    if m["n"] and m["has_overall"] and m["has_domain"]:
        return "HOLD", f"overall rating on {m['has_overall']} of {m['n']} hospitals ('Not Available' otherwise), domain comparisons"
    if m["n"] and m["has_overall"]:
        return "PARTIAL", (f"overall rating on {m['has_overall']} of {m['n']} hospitals; the domain comparison "
                           f"columns are NULL on {m['n'] - m['has_domain']} rows")
    return "NEED", "no ratings"


def _schema(key):
    def rule(m):
        note = "patient-level schema question; no patient data is ever loaded"
        if m[key]:
            note += f" (WARNING: {m[key]} patient-like table(s) found, check for PHI)"
        return "SCHEMA", note
    return rule


RULES: Dict[str, Callable[[Mapping[str, Any]], Tuple[str, str]]] = {
    "CQ01": _cq01, "CQ02": _cq02, "CQ03": _cq03, "CQ04": _cq04, "CQ05": _cq05,
    "CQ06": _exists("reassignment_tables"), "CQ07": _exists("affiliation_tables"),
    "CQ08": _cq08, "CQ09": _cq09, "CQ10": _cq10, "CQ11": _cq11, "CQ12": _cq12,
    "CQ13": _exists("practice_location_tables"), "CQ14": _cq14, "CQ15": _cq15,
    "CQ16": _exists("network_tables"), "CQ17": _exists("medicaid_tables"),
    "CQ18": _cq18, "CQ19": _cq19, "CQ20": _cq20, "CQ21": _exists("prescriber_tables"),
    "CQ22": _exists("owner_tables"), "CQ23": _cq23, "CQ24": _exists("change_history_tables"),
    "CQ25": _cq25, "CQ26": _exists("exclusion_tables"), "CQ27": _exists("payment_tables"),
    "CQ28": _schema("patient_tables"), "CQ29": _schema("encounter_tables"),
    "CQ30": _schema("coverage_tables"),
}


def label(cq: str, metrics: Mapping[str, Any]) -> Tuple[str, str]:
    lab, why = RULES[cq](metrics)
    assert lab in LABELS, (cq, lab)
    return lab, why


def tally(labels: Mapping[str, str]) -> Dict[str, int]:
    out = {k: 0 for k in LABELS}
    for v in labels.values():
        out[v] += 1
    out["groundable"] = out["HOLD"] + out["PARTIAL"]
    return out


def _jsonable(v: Any) -> Any:
    if isinstance(v, (int, str)) or v is None:
        return v
    try:
        return int(v)
    except (TypeError, ValueError):
        return str(v)


def run_probes(conn, probes: Optional[Dict[str, str]] = None, timeout_ms: int = 30000) -> List[Dict[str, Any]]:
    """Run every probe on a SQLAlchemy connection/session, read-only. Returns one dict per CQ."""
    from sqlalchemy import text

    conn.execute(text("SET TRANSACTION READ ONLY"))
    conn.execute(text(f"SET LOCAL statement_timeout = {int(timeout_ms)}"))
    out = []
    for cq, sql in sorted((probes or load_probes()).items()):
        row = conn.execute(text(sql)).mappings().one()
        metrics = {k: _jsonable(v) for k, v in row.items()}
        lab, why = label(cq, metrics)
        out.append({"cq": cq, "label": lab, "why": why, "metrics": metrics})
    return out


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--json", action="store_true")
    ap.add_argument("--timeout-ms", type=int, default=30000)
    args = ap.parse_args(argv)
    from app.core.database import get_engine

    with get_engine().connect() as conn:
        try:
            results = run_probes(conn, timeout_ms=args.timeout_ms)
        finally:
            conn.rollback()  # read-only transaction: nothing to keep
    labels = {r["cq"]: r["label"] for r in results}
    if args.json:
        print(json.dumps({"results": results, "tally": tally(labels)}, indent=1))
    else:
        for r in results:
            print(f"{r['cq']}  {r['label']:<8} {r['why']}")
        print(json.dumps(tally(labels)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
