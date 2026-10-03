"""
GLEIF LEI API -- URLs and record parsing (SPEC_154). Pure: no I/O.

The connector pages ``https://api.gleif.org/api/v1/lei-records`` filtered to one legal-address
country, with cursor pagination (page-number paging stops at 10,000 results) and a sparse
fieldset of ``lei,entity,registration``: the third-party mapping fields GLEIF also serves
(``spglobal`` -- S&P CIQ ids --, ``ocid``, ``bic``, ``mic``, ``qcc``, ``gem``) are never requested,
and ``parse_record`` never stores them even if a response carries them.

GLEIF's ``links.next`` drops the sparse fieldset, so ``next_url`` rebuilds the next request from
the cursor alone.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qs, urlencode, urlsplit

API_BASE = "https://api.gleif.org/api/v1/lei-records"
PAGE_SIZE = 200
FIELDS = "lei,entity,registration"
COUNTRY_FILTER = "filter[entity.legalAddress.country]"

_LEI = re.compile(r"^[A-Z0-9]{18}[0-9]{2}$")
_REGION = re.compile(r"^US-([A-Z]{2})$")


def _url(country: str, cursor: str, size: int = PAGE_SIZE) -> str:
    q = {COUNTRY_FILTER: country, "fields[lei-records]": FIELDS,
         "page[size]": str(size), "page[cursor]": cursor}
    return f"{API_BASE}?{urlencode(q)}"


def first_url(country: str = "US", size: int = PAGE_SIZE) -> str:
    return _url(country, "*", size)


def next_url(links_next: Optional[str], country: str = "US", size: int = PAGE_SIZE) -> Optional[str]:
    """The next page's URL from GLEIF's ``links.next`` (its cursor only), or None at the end."""
    if not links_next:
        return None
    cursor = (parse_qs(urlsplit(links_next).query).get("page[cursor]") or [None])[0]
    return _url(country, cursor, size) if cursor else None


def lei(raw: Any) -> Optional[str]:
    v = re.sub(r"[^A-Z0-9]", "", str(raw or "").upper())
    return v if _LEI.match(v) else None


def region_state(region: Optional[str]) -> Optional[str]:
    """'US-DE' -> 'DE'; anything that is not a US subdivision -> None."""
    m = _REGION.match(str(region or "").strip().upper())
    return m.group(1) if m else None


def _s(v: Any, cap: int = 500) -> Optional[str]:
    if v is None:
        return None
    v = str(v).strip()
    return v[:cap] if v else None


def _date(v: Any) -> Optional[str]:
    v = _s(v, 40)
    return v if v and re.match(r"^\d{4}-\d{2}-\d{2}", v) else None


def _addr(a: Optional[Dict[str, Any]]) -> Dict[str, Optional[str]]:
    a = a or {}
    return {"city": _s(a.get("city"), 200), "region": _s(a.get("region"), 16),
            "postal": _s(a.get("postalCode"), 32), "country": _s(a.get("country"), 2)}


# the columns parse_record returns, in gleif_lei_record order (ingest upserts exactly these)
ROW_COLUMNS = (
    "lei", "legal_name", "jurisdiction", "state",
    "legal_city", "legal_region", "legal_state", "legal_postal", "legal_country",
    "hq_city", "hq_region", "hq_state", "hq_postal", "hq_country",
    "registered_at", "registered_as", "legal_form_id", "category", "entity_status",
    "registration_status", "initial_registration_date", "last_update_date", "next_renewal_date",
    "managing_lou", "corroboration_level",
)


def parse_record(rec: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """One API ``lei-records`` item -> a gleif_lei_record row, or None without a valid LEI.

    ``state`` is the US state of the entity's jurisdiction (``US-DE``), else of its legal address:
    the closest analogue of EDGAR's state of incorporation, which the resolver's other records use.
    """
    attrs = (rec or {}).get("attributes") or {}
    code = lei(attrs.get("lei"))
    if not code:
        return None
    ent = attrs.get("entity") or {}
    reg = attrs.get("registration") or {}
    legal, hq = _addr(ent.get("legalAddress")), _addr(ent.get("headquartersAddress"))
    juris = _s(ent.get("jurisdiction"), 16)
    legal_state, hq_state = region_state(legal["region"]), region_state(hq["region"])
    return {
        "lei": code,
        "legal_name": _s((ent.get("legalName") or {}).get("name")),
        "jurisdiction": juris,
        "state": region_state(juris) or legal_state,
        "legal_city": legal["city"], "legal_region": legal["region"], "legal_state": legal_state,
        "legal_postal": legal["postal"], "legal_country": legal["country"],
        "hq_city": hq["city"], "hq_region": hq["region"], "hq_state": hq_state,
        "hq_postal": hq["postal"], "hq_country": hq["country"],
        "registered_at": _s((ent.get("registeredAt") or {}).get("id"), 16),
        "registered_as": _s(ent.get("registeredAs"), 100),
        "legal_form_id": _s((ent.get("legalForm") or {}).get("id"), 16),
        "category": _s(ent.get("category"), 32),
        "entity_status": _s(ent.get("status"), 16),
        "registration_status": _s(reg.get("status"), 32),
        "initial_registration_date": _date(reg.get("initialRegistrationDate")),
        "last_update_date": _date(reg.get("lastUpdateDate")),
        "next_renewal_date": _date(reg.get("nextRenewalDate")),
        "managing_lou": lei(reg.get("managingLou")),
        "corroboration_level": _s(reg.get("corroborationLevel"), 32),
    }


def parse_page(doc: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], int, Optional[str], Optional[str], Optional[int]]:
    """-> (rows, records_without_lei, links.next, golden copy publish date, total)."""
    rows, bad = [], 0
    for item in doc.get("data") or []:
        row = parse_record(item)
        if row is None:
            bad += 1
        else:
            rows.append(row)
    meta = doc.get("meta") or {}
    publish = (meta.get("goldenCopy") or {}).get("publishDate")
    total = (meta.get("pagination") or {}).get("total")
    return rows, bad, (doc.get("links") or {}).get("next"), publish, total
