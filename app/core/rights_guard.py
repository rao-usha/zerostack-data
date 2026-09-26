"""
Rights gate on source read APIs (SPEC_142 fix round).

``require_rights_clear(*tables)`` is a FastAPI dependency for endpoints that serve
rows of catalog tables whose source terms forbid storing them, forbid commercial
use, or need an agreement first (``export_policy.catalog_rights_gate``):

- non-admins get **403** with the gate (reasons + datasets);
- admins get the response with an ``X-Dataset-Rights-Gate`` header (flag only,
  PLAN_088 decision 3).

``ENFORCED_PATHS`` lists the paths guarded this way; ``UNENFORCED_PATHS`` lists the
known paths that still read gated tables without the guard (derived scorers and
dashboards). The rights report publishes both so nobody reads "sample refused" as
"refused everywhere".
"""

from typing import Any, Callable, Dict, List, Optional

from fastapi import Depends, HTTPException, Response

from app.core.authz import ROLE_ADMIN, current_principal

GATE_HEADER = "X-Dataset-Rights-Gate"


def rights_gate_for(*tables: str) -> Optional[Dict[str, Any]]:
    """Union of ``catalog_rights_gate`` over ``tables`` (None when none is gated)."""
    from app.core.export_policy import catalog_rights_gate

    reasons: List[str] = []
    datasets: List[str] = []
    for t in tables:
        g = catalog_rights_gate(t)
        if not g:
            continue
        reasons += [r for r in g["reasons"] if r not in reasons]
        datasets += [d for d in g["datasets"] if d not in datasets]
    return {"reasons": reasons, "datasets": datasets} if (reasons or datasets) else None


def require_rights_clear(*tables: str) -> Callable[..., None]:
    """Dependency: refuse non-admins when any of ``tables`` is rights-gated; flag admins."""
    if not tables:
        raise ValueError("require_rights_clear needs at least one table")

    def _guard(response: Response, principal: Dict[str, Any] = Depends(current_principal)) -> None:
        gate = rights_gate_for(*tables)
        if not gate:
            return
        if principal.get("role") != ROLE_ADMIN:
            raise HTTPException(status_code=403, detail={
                "error": "rights_gated",
                "message": "the source's terms forbid serving these rows (storage / commercial use / "
                           "agreement required); see GET /api/v1/catalog/rights/report (admin)",
                "reasons": gate["reasons"],
                "datasets": gate["datasets"],
            })
        response.headers[GATE_HEADER] = ",".join(gate["reasons"])

    _guard.rights_tables = tuple(tables)  # type: ignore[attr-defined]
    return _guard


# Paths guarded with require_rights_clear (kept in step by a test that walks the routes).
ENFORCED_PATHS: Dict[str, List[str]] = {
    "prediction_markets": ["/api/v1/prediction-markets/* (whole router)"],
    "medspa_prospects": ["/api/v1/medspa-discovery/* (whole router)"],
    "vertical_prospects": ["/api/v1/vertical-discovery/* (whole router)"],
    "courtlistener_dockets": ["/api/v1/courtlistener/search", "/api/v1/courtlistener/stats"],
    "si_internet_exchanges": ["/api/v1/site-intel/telecom/ix", "/api/v1/site-intel/telecom/ix/nearby",
                              "/api/v1/site-intel/telecom/data-centers",
                              "/api/v1/site-intel/telecom/data-centers/nearby"],
    "si_warehouse_listings": ["/api/v1/site-intel/logistics/warehouse-listings",
                              "/api/v1/site-intel/logistics/warehouse-listings/market-summary"],
    "si_zoning_districts": ["/api/v1/realestate/zoning/districts", "/api/v1/realestate/zoning/summary",
                            "/api/v1/realestate/zoning/dc-eligible"],
}

# Known paths that still read gated tables for any signed-in user (derived scores,
# dashboards, aggregates). Not guarded here: gating them is a product decision.
UNENFORCED_PATHS: List[Dict[str, str]] = [
    {"paths": "/api/v1/econ-snapshot/*, /api/v1/macro/*",
     "reads": "FRED tables (fred_series: storage forbidden as retrieved)"},
    {"paths": "/api/v1/datacenter-sites/*, app/services/unified_site_scorer.py, app/services/atlas/*",
     "reads": "data_center_facility / internet_exchange (PeeringDB) and zoning_district (NZA) in scores and map layers"},
    {"paths": "app/services/healthcare_practice_scorer.py, app/services/deal_engine.py, "
              "app/services/company_diligence_scorer.py, app/reports/templates/medspa_market.py",
     "reads": "medspa_prospects / yelp_businesses (Yelp) in scores and reports"},
    {"paths": "CMS-backed scorers and reports (the /api/v1/cms router serves metadata only)",
     "reads": "cms_medicare_utilization (CPT, agreement required) aggregates"},
    {"paths": "/api/v1/international/*", "reads": "IMF series (agreement required) if loaded (0 rows today)"},
]
