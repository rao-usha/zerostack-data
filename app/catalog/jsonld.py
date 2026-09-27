"""
schema.org ``Dataset`` JSON-LD for a catalog spec (SPEC_145, PLAN_088 §3 "SPEC_139").

``to_jsonld(spec)`` is pure: the spec plus the committed column dictionary, no
database. A few W3C DCAT / Dublin Core terms ride along where they come free
(``dct:accrualPeriodicity``, ``dcat:landingPage``); the federal-only DCAT-US
fields (bureauCode, programCode) are skipped.

Rights come first:

- a dataset the rights gate holds back (storage forbidden, commercial use
  forbidden, an agreement required) or a retired one gets no JSON-LD at all
  (``JsonLdRefused``);
- ``license`` names the upstream licence only when the rights block is
  reviewed and the effective redistribution is ``open`` or ``attribution``.
  Otherwise it is a ``CreativeWork`` naming what may actually be done today —
  restricted or unreviewed rights are never emitted as open;
- ``publishable(spec)`` is True only for a reviewed ga/beta dataset: the only
  JSON-LD that may ever leave the building. The route stays behind auth.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from app.catalog.spec import MIN_DESCRIPTION, DatasetSpec

API_PREFIX = "/api/v1"
MAX_VARIABLES = 250
PUBLISHER = {"@type": "Organization", "name": "Nexdata"}
CONTEXT = {
    "@vocab": "https://schema.org/",
    "dcat": "http://www.w3.org/ns/dcat#",
    "dct": "http://purl.org/dc/terms/",
}
# DCAT accrualPeriodicity: Dublin Core frequency vocabulary
_FREQUENCY = {
    "daily": "http://purl.org/cld/freq/daily",
    "weekly": "http://purl.org/cld/freq/weekly",
    "monthly": "http://purl.org/cld/freq/monthly",
    "quarterly": "http://purl.org/cld/freq/quarterly",
    "annual": "http://purl.org/cld/freq/annual",
    "ad_hoc": "http://purl.org/cld/freq/irregular",
}
_SPATIAL_LEVEL = {"state": "state level", "county": "county level", "tract": "census tract level",
                  "zip": "ZIP code level", "msa": "metro area level", "country": "country level",
                  "point": "point locations", "facility": "facility locations"}
OPEN_EFFECTIVE = ("open", "attribution")
# what the rights allow today, in words (conditionsOfAccess)
_REDISTRIBUTION_TEXT = {
    "open": "Redistribution permitted.",
    "attribution": "Redistribution permitted with the attribution below.",
    "internal_only": "Internal use only; not for redistribution.",
    "restricted": "Restricted: not for redistribution; access is limited.",
}


class JsonLdRefused(Exception):
    def __init__(self, reasons: Tuple[str, ...]):
        super().__init__(", ".join(reasons))
        self.reasons = reasons


def refusal_reasons(spec: DatasetSpec) -> Tuple[str, ...]:
    reasons = tuple(spec.rights_gate)
    if spec.status_public == "retired":
        reasons += ("retired",)
    return reasons


def publishable(spec: DatasetSpec) -> bool:
    """May this JSON-LD be published outside (a search engine, a data marketplace)?"""
    return (spec.status_public in ("ga", "beta") and spec.reviewed
            and not refusal_reasons(spec) and spec.effective_redistribution in OPEN_EFFECTIVE)


def spatial_text(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    parts = value.split(":")
    where = "United States" if parts[0] == "US" else "Global"
    levels = [_SPATIAL_LEVEL.get(p, p.replace("_", " ")) for p in parts[1:]]
    return f"{where} ({', '.join(levels)})" if levels else where


def temporal_coverage(spec: DatasetSpec) -> Optional[str]:
    """ISO 8601 interval: ``<from>/..`` (open-ended, loaded through today)."""
    if spec.coverage_from:
        return f"{spec.coverage_from}/.."
    return None


def license_node(spec: DatasetSpec) -> Any:
    if spec.reviewed and spec.effective_redistribution in OPEN_EFFECTIVE:
        if spec.license_url:
            return spec.license_url
        return {"@type": "CreativeWork", "name": spec.license}
    if spec.effective_redistribution == "restricted" or spec.redistribution == "restricted":
        name = "Restricted (not for redistribution)"
    elif not spec.reviewed:
        name = "Internal use only (rights review pending)"
    else:
        name = "Internal use only"
    return {"@type": "CreativeWork", "name": name,
            "description": f"Upstream terms: {spec.license}. Nexdata may not pass these rights on "
                          f"until they are reviewed and published."}


def conditions_of_access(spec: DatasetSpec) -> str:
    parts = ["Requires a Nexdata account (API key or sign-in)."]
    parts.append(_REDISTRIBUTION_TEXT.get(spec.effective_redistribution, "Internal use only."))
    if not spec.reviewed:
        parts.append("Rights not yet reviewed.")
    if spec.commercial_use == "restricted":
        parts.append("Commercial use is restricted by the upstream terms.")
    if spec.storage == "time_limited" and spec.storage_max_age_days:
        parts.append(f"Upstream terms limit storage to {spec.storage_max_age_days} days.")
    if spec.share_alike:
        parts.append("Share-alike: derived data must carry the same licence.")
    if spec.pii_class != "none":
        parts.append(f"Contains {spec.pii_class.replace('_', ' ')} information; PII is masked for "
                     f"non-admin users.")
    if spec.attribution and spec.effective_redistribution in OPEN_EFFECTIVE:
        parts.append(f"Attribution: {spec.attribution}")
    return " ".join(parts)


def variables(spec: DatasetSpec, body: Optional[Dict[str, Any]] = None,
              limit: int = MAX_VARIABLES) -> List[Dict[str, Any]]:
    from app.catalog.dictionary import dataset_tables_offline, load_dictionary

    body = body if body is not None else load_dictionary()
    out: List[Dict[str, Any]] = []
    seen = set()
    for table in dataset_tables_offline(spec, body):
        for c in body["tables"][table]["columns"]:
            if c["name"] in seen:
                continue
            seen.add(c["name"])
            v: Dict[str, Any] = {"@type": "PropertyValue", "name": c["name"]}
            if c.get("description"):
                v["description"] = c["description"]
            if c.get("unit"):
                v["unitText"] = c["unit"]
            if c.get("semantic_type"):
                v["propertyID"] = c["semantic_type"]
            out.append(v)
            if len(out) >= limit:
                return out
    return out


def distributions(spec: DatasetSpec, base_url: str = "") -> List[Dict[str, Any]]:
    base = f"{base_url.rstrip('/')}{API_PREFIX}/catalog/{spec.key}"
    items = [
        ("catalog entry", base, "The catalog entry with live row counts, coverage and quality."),
        ("schema", f"{base}/schema", "Columns, types, descriptions and PII classes."),
        ("sample", f"{base}/sample", "Up to 20 rows; PII masked for non-admin users."),
    ]
    return [{"@type": "DataDownload", "name": f"{spec.display_name} — {name}", "contentUrl": url,
             "encodingFormat": "application/json", "description": desc}
            for name, url, desc in items]


def to_jsonld(spec: DatasetSpec, base_url: str = "", body: Optional[Dict[str, Any]] = None
              ) -> Dict[str, Any]:
    """The schema.org Dataset for ``spec``. Raises ``JsonLdRefused`` for a gated or
    retired dataset and ``ValueError`` for a description under 50 characters."""
    reasons = refusal_reasons(spec)
    if reasons:
        raise JsonLdRefused(reasons)
    if len((spec.description or "").strip()) < MIN_DESCRIPTION:
        raise ValueError(f"{spec.key}: description under {MIN_DESCRIPTION} characters")
    doc: Dict[str, Any] = {
        "@context": CONTEXT,
        "@type": "Dataset",
        "@id": f"{base_url.rstrip('/')}{API_PREFIX}/catalog/{spec.key}",
        "identifier": spec.key,
        "name": spec.display_name,
        "description": spec.description,
        "keywords": list(spec.keywords) + [spec.kind, spec.source],
        "creator": {"@type": "Organization", "name": spec.source.replace("_", " ").upper()
                    if len(spec.source) <= 5 else spec.source.replace("_", " ").title()},
        "publisher": PUBLISHER,
        "isAccessibleForFree": False,
        "conditionsOfAccess": conditions_of_access(spec),
        "license": license_node(spec),
        "distribution": distributions(spec, base_url),
        "variableMeasured": variables(spec, body),
        "measurementTechnique": spec.origin,
        "dct:accrualPeriodicity": _FREQUENCY.get(spec.cadence, spec.cadence),
    }
    if spec.subtitle:
        doc["alternateName"] = spec.subtitle
    tc = temporal_coverage(spec)
    if tc:
        doc["temporalCoverage"] = tc
    sp = spatial_text(spec.spatial_coverage)
    if sp:
        doc["spatialCoverage"] = {"@type": "Place", "name": sp}
    if spec.upstream_url:
        doc["isBasedOn"] = spec.upstream_url
        doc["dcat:landingPage"] = spec.upstream_url
    if spec.citation_url:
        doc["usageInfo"] = spec.citation_url
    if spec.verified_at:
        doc["dateModified"] = spec.verified_at
    return doc


REQUIRED_FIELDS = ("@context", "@type", "name", "description", "license", "creator", "publisher",
                   "isAccessibleForFree", "distribution", "keywords")


__all__ = ["JsonLdRefused", "REQUIRED_FIELDS", "publishable", "refusal_reasons", "to_jsonld"]
