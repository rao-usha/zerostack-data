"""
Diligence Pack Intake API — PLAN_065 / SPEC_062.

Public router (no auth) exposing three endpoints that drive the front door
of the paid Sector × Market Intelligence Pack product:

  POST /api/v1/diligence-pack/request      — submit an order, get a Stripe link
  GET  /api/v1/diligence-pack/skus         — pricing ladder for the intake page
  GET  /api/v1/diligence-pack/taxonomies   — NAICS sectors + MSA list for the pickers

Anonymous-friendly: buyer provides name + email + NAICS-4 + geography; server
returns a payment URL. Manual delivery for v1.

Lives at `/diligence-pack/*` rather than `/diligence/*` because the existing
`app/api/v1/diligence.py` (DueDiligenceAgent legacy router) already owns the
`/diligence` prefix and is JWT-gated. Keeping these distinct avoids mixing
auth modes in one router.
"""

import asyncio
import logging
from typing import Any, Dict, List, Literal, Optional

from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import BaseModel, EmailStr, Field
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.core.database import get_db
from app.services.diligence.orders import (
    DiligenceOrderService,
    SKU_CATALOG,
)
from app.services.diligence.taxonomies import (
    load_msa,
    load_naics,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/diligence-pack",
                   tags=["Diligence Pack (Sector × Market Intelligence)"])


# ─────────────────────────────────────────────────────────────────────────────
# Pydantic schemas
# ─────────────────────────────────────────────────────────────────────────────

class RequestBody(BaseModel):
    contact_name: str = Field(..., min_length=1, max_length=200)
    contact_email: EmailStr
    naics_code: str = Field(..., min_length=2, max_length=6)
    sku: Literal["single_map", "pilot_3_maps", "retainer"]
    geography_mode: Literal["msa", "state", "multi_county"]
    msa_code: Optional[str] = None
    state_fips: Optional[str] = None
    county_fips_list: Optional[List[str]] = None
    contact_org: Optional[str] = Field(None, max_length=200)
    client_note: Optional[str] = Field(None, max_length=2000)
    geography_note: Optional[str] = Field(None, max_length=500)
    source: Optional[str] = Field(None, max_length=100)


# ─────────────────────────────────────────────────────────────────────────────
# Endpoints
# ─────────────────────────────────────────────────────────────────────────────

@router.post("/request")
def request_order(body: RequestBody, request: Request,
                  db: Session = Depends(get_db)):
    """Submit a new diligence-pack order. Anonymous-friendly. Returns the
    matching Stripe Payment Link URL when one is configured for that SKU.
    On validation failure returns HTTP 422 with the message in `detail`."""
    try:
        result = DiligenceOrderService(db).create_order(
            contact_name=body.contact_name,
            contact_email=str(body.contact_email),
            naics_code=body.naics_code,
            sku=body.sku,
            geography_mode=body.geography_mode,
            msa_code=body.msa_code,
            state_fips=body.state_fips,
            county_fips_list=body.county_fips_list,
            contact_org=body.contact_org,
            client_note=body.client_note,
            geography_note=body.geography_note,
            source=body.source,
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=str(exc),
        )

    # Best-effort notify email — failure NEVER blocks the response (T8).
    _fire_and_forget_notify(result, body)
    return result


@router.get("/skus")
def get_skus():
    """SKU pricing ladder. Does NOT leak Stripe URLs — those only come back
    after a successful POST /request."""
    settings = get_settings()
    out = []
    for sku, meta in SKU_CATALOG.items():
        has_url = False
        if meta["settings_field"]:
            has_url = bool(getattr(settings, meta["settings_field"], None))
        out.append({
            "sku": sku,
            "label": meta["label"],
            "price_display": meta["price_display"],
            "price_cents": meta["price_cents"],
            "copy": meta["copy"],
            "has_payment_url": has_url,
        })
    return {"skus": out}


@router.get("/taxonomies")
def get_taxonomies():
    """NAICS sectors → industries tree + MSA flat list, for the intake pickers."""
    naics = load_naics()
    msa = load_msa()

    sectors: Dict[str, Dict[str, Any]] = {}
    industries_by_sector: Dict[str, List[Dict[str, Any]]] = {}
    for code, node in naics.items():
        if node.digits == 2:
            sectors[code] = {"code": code, "label": node.label}
            industries_by_sector.setdefault(code, [])
    for code, node in naics.items():
        if node.digits == 4:
            sector = code[:2]
            industries_by_sector.setdefault(sector, []).append({
                "code": code,
                "label": node.label,
            })
    for sector in industries_by_sector:
        industries_by_sector[sector].sort(key=lambda x: x["code"])

    naics_tree = [
        {
            "sector_code": sec_code,
            "sector_label": sectors[sec_code]["label"],
            "industries": industries_by_sector.get(sec_code, []),
        }
        for sec_code in sorted(sectors)
    ]

    msa_list = [
        {
            "cbsa_code": rec.cbsa_code,
            "title": rec.title,
            "state_abbrs": list(rec.state_abbrs),
            "county_count": len(rec.county_fips_list),
        }
        for rec in msa.values()
    ]
    msa_list.sort(key=lambda x: x["title"])

    return {"naics": naics_tree, "msa": msa_list}


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _fire_and_forget_notify(result: Dict[str, Any], body: RequestBody) -> None:
    """Best-effort notify email. Never raises — failure logged at WARNING."""
    try:
        settings = get_settings()
        if not settings.diligence_notify_email:
            return
        from app.services.email import get_email_service

        subject = (
            f"[Nexdata] New diligence order #{result['order_id']} — "
            f"{result['sku_label']}"
        )
        text_body = (
            f"Order ID: {result['order_id']}\n"
            f"SKU: {result['sku']}  ({result['price_display']})\n"
            f"Status: {result['status']}\n\n"
            f"Contact: {body.contact_name} <{body.contact_email}>\n"
            f"Org: {body.contact_org or '—'}\n\n"
            f"NAICS: {body.naics_code}\n"
            f"Geography: {body.geography_mode} ("
            f"{body.msa_code or body.state_fips or body.county_fips_list})\n\n"
            f"Client note:\n{body.client_note or '(none)'}\n"
        )
        service = get_email_service()
        coro = service.send(
            to=settings.diligence_notify_email,
            subject=subject,
            text=text_body,
        )
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(coro)
        except RuntimeError:
            asyncio.run(coro)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Diligence notify-email failed: %s", exc)
