"""
Diligence Orders service — SPEC_062 / PLAN_065.

Persists requests for Market Intelligence Pack reports (SPEC_061 deliverable)
and routes them to a Stripe Payment Link by SKU. Anonymous-friendly: the
intake API requires no auth — the buyer just provides a name + email + a
NAICS-4 + a geography, and we hand back a payment URL.

Manual delivery for v1 — an operator (founder) reads the admin queue,
generates the report against cloud, sends the file to the buyer. SPEC_065
will wrap this in an admin UI; until then the table is reviewed via SQL.

Stripe webhook integration (auto-flip status='paid') is deferred to a future
spec; v1 marks status='requested' on intake and an operator PATCHes it
forward through 'scoped' → 'paid' → 'generating' → 'delivered' → 'closed'.

Schema lives only on whichever DB this service is instantiated against —
local for tests, cloud for prod. Migration is idempotent via
`CREATE TABLE IF NOT EXISTS`.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Literal, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.services.diligence.taxonomies import (
    load_msa,
    load_naics,
    msa_title,
    naics_label,
)

logger = logging.getLogger(__name__)

VALID_SKUS = ("single_map", "pilot_3_maps", "retainer")
VALID_GEO_MODES = ("msa", "state", "multi_county")

# Authoritative SKU catalog — surfaced via /skus and the intake response.
SKU_CATALOG: Dict[str, Dict[str, Any]] = {
    "single_map": {
        "label": "Single Sector × MSA Intelligence Pack",
        "price_display": "$2,500",
        "price_cents": 250_000,
        "copy": "One sector × MSA intelligence pack delivered in 24–48 hours.",
        "settings_field": "stripe_payment_link_url_2500",
        "next_step_when_paid": (
            "We'll start the map within 4 working hours and email it to you in "
            "24–48 hours."
        ),
    },
    "pilot_3_maps": {
        "label": "3-Map Pilot",
        "price_display": "$7,500",
        "price_cents": 750_000,
        "copy": "Three maps + a comparable view. 5-day delivery.",
        "settings_field": "stripe_payment_link_url_7500",
        "next_step_when_paid": (
            "We'll confirm the 3 (NAICS, MSA) pairs over email within one "
            "working day, then deliver in 5 business days."
        ),
    },
    "retainer": {
        "label": "Sector-Thesis Retainer",
        "price_display": "$10K–$15K / month",
        "price_cents": None,
        "copy": (
            "Weekly sourcing + map refreshes for a portfolio of sectors. "
            "Invoice-billed."
        ),
        "settings_field": None,  # no Payment Link — sales-led
        "next_step_when_paid": None,
    },
}


class DiligenceOrderService:
    """Order intake + persistence for paid Market Intelligence Pack requests."""

    def __init__(self, db: Session):
        self.db = db
        self._ensure_tables()

    # ── Schema migration (idempotent) ────────────────────────────────────────

    def _ensure_tables(self) -> None:
        ddl = """
            CREATE TABLE IF NOT EXISTS diligence_orders (
                id              SERIAL PRIMARY KEY,
                requested_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                contact_name    TEXT NOT NULL,
                contact_email   TEXT NOT NULL,
                contact_org     TEXT,
                naics_code      TEXT NOT NULL,
                naics_label     TEXT,
                msa_code        TEXT,
                msa_title       TEXT,
                state_fips      TEXT,
                geography_mode  TEXT NOT NULL,
                geography_note  TEXT,
                client_note     TEXT,
                sku             TEXT NOT NULL,
                status          TEXT NOT NULL DEFAULT 'requested',
                stripe_session_id TEXT,
                paid_at         TIMESTAMP,
                report_ids      INTEGER[],
                delivered_at    TIMESTAMP,
                notes           TEXT,
                source          TEXT
            );
            CREATE INDEX IF NOT EXISTS diligence_orders_status_idx        ON diligence_orders(status);
            CREATE INDEX IF NOT EXISTS diligence_orders_email_idx         ON diligence_orders(contact_email);
            CREATE INDEX IF NOT EXISTS diligence_orders_requested_at_idx  ON diligence_orders(requested_at DESC);
        """
        # `text()` with multiple statements works on Postgres via `execute`.
        for stmt in [s for s in ddl.strip().split(";") if s.strip()]:
            self.db.execute(text(stmt + ";"))
        self.db.commit()

    # ── Public API ───────────────────────────────────────────────────────────

    def create_order(
        self,
        *,
        contact_name: str,
        contact_email: str,
        naics_code: str,
        sku: str,
        geography_mode: str,
        msa_code: Optional[str] = None,
        state_fips: Optional[str] = None,
        county_fips_list: Optional[List[str]] = None,
        contact_org: Optional[str] = None,
        client_note: Optional[str] = None,
        geography_note: Optional[str] = None,
        source: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Validate, persist, return {order_id, payment_url, sku, status, next_step}.

        Raises ValueError on any validation failure — the API layer turns that
        into HTTP 422 with the message exposed in the `detail` field.
        """
        # ── 1. validate SKU + geo mode ───────────────────────────────────────
        if sku not in VALID_SKUS:
            raise ValueError(
                f"Invalid sku {sku!r}. Allowed: {', '.join(VALID_SKUS)}."
            )
        if geography_mode not in VALID_GEO_MODES:
            raise ValueError(
                f"Invalid geography_mode {geography_mode!r}. "
                f"Allowed: {', '.join(VALID_GEO_MODES)}."
            )

        # ── 2. validate NAICS — must be 4-digit and known ────────────────────
        naics = load_naics()
        if naics_code not in naics:
            raise ValueError(f"Unknown NAICS code: {naics_code!r}.")
        if naics[naics_code].digits != 4:
            raise ValueError(
                f"naics_code must be 4-digit; got {naics[naics_code].digits}-digit "
                f"{naics_code!r}. Use the 4-digit industry-group code from the "
                f"intake picker."
            )
        denorm_naics_label = naics_label(naics_code)

        # ── 3. validate geography for the chosen mode ────────────────────────
        denorm_msa_title: Optional[str] = None
        if geography_mode == "msa":
            if not msa_code:
                raise ValueError("geography_mode='msa' requires msa_code.")
            if msa_code not in load_msa():
                raise ValueError(f"Unknown MSA CBSA code: {msa_code!r}.")
            denorm_msa_title = msa_title(msa_code)
        elif geography_mode == "state":
            if not state_fips or len(state_fips) != 2 or not state_fips.isdigit():
                raise ValueError(
                    "geography_mode='state' requires 2-digit numeric state_fips."
                )
        elif geography_mode == "multi_county":
            if not county_fips_list:
                raise ValueError(
                    "geography_mode='multi_county' requires non-empty county_fips_list."
                )

        # ── 4. persist ───────────────────────────────────────────────────────
        result = self.db.execute(text("""
            INSERT INTO diligence_orders (
                contact_name, contact_email, contact_org,
                naics_code, naics_label, msa_code, msa_title, state_fips,
                geography_mode, geography_note, client_note, sku, source
            ) VALUES (
                :contact_name, :contact_email, :contact_org,
                :naics_code, :naics_label, :msa_code, :msa_title, :state_fips,
                :geography_mode, :geography_note, :client_note, :sku, :source
            )
            RETURNING id, status
        """), {
            "contact_name": contact_name,
            "contact_email": contact_email,
            "contact_org": contact_org,
            "naics_code": naics_code,
            "naics_label": denorm_naics_label,
            "msa_code": msa_code,
            "msa_title": denorm_msa_title,
            "state_fips": state_fips,
            "geography_mode": geography_mode,
            "geography_note": geography_note,
            "client_note": client_note,
            "sku": sku,
            "source": source or "direct",
        })
        row = result.fetchone()
        self.db.commit()
        order_id, status = int(row[0]), str(row[1])

        # ── 5. resolve Stripe Payment Link by SKU ────────────────────────────
        catalog = SKU_CATALOG[sku]
        payment_url: Optional[str] = None
        if catalog["settings_field"]:
            payment_url = getattr(get_settings(), catalog["settings_field"], None)

        next_step = (
            catalog["next_step_when_paid"]
            if payment_url
            else (
                "We'll reach out within one business day to scope the "
                "retainer and confirm pricing."
                if sku == "retainer"
                else "We'll be in touch within one business day with payment instructions."
            )
        )

        logger.info(
            "Diligence order %s created: sku=%s naics=%s mode=%s msa=%s state=%s",
            order_id, sku, naics_code, geography_mode, msa_code, state_fips,
        )

        return {
            "order_id": order_id,
            "status": status,
            "sku": sku,
            "sku_label": catalog["label"],
            "price_display": catalog["price_display"],
            "payment_url": payment_url,
            "next_step": next_step,
        }

    # ── Lookup helpers (admin queue uses these in SPEC_065) ──────────────────

    def get_order(self, order_id: int) -> Optional[Dict[str, Any]]:
        row = self.db.execute(text("""
            SELECT * FROM diligence_orders WHERE id = :id
        """), {"id": order_id}).mappings().first()
        return dict(row) if row else None

    def list_orders(
        self,
        status: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        sql = "SELECT * FROM diligence_orders"
        params: Dict[str, Any] = {"limit": limit, "offset": offset}
        if status:
            sql += " WHERE status = :status"
            params["status"] = status
        sql += " ORDER BY requested_at DESC LIMIT :limit OFFSET :offset"
        rows = self.db.execute(text(sql), params).mappings().all()
        return [dict(r) for r in rows]
