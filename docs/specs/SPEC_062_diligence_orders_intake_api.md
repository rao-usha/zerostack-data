# SPEC 062 — Diligence Orders Model + Intake API

**Status:** Draft
**Task type:** api_endpoint
**Date:** 2026-05-18
**Plan:** PLAN_065 (Sector × Market Intelligence Pack)
**Test file:** tests/test_spec_062_diligence_orders_intake_api.py
**Builds on:** SPEC_060 (taxonomies), SPEC_061 (report template, committed `6c43119`)
**Note on numbering:** PLAN_065 v2 originally called this SPEC_063; the
named-operators curator (PLAN_065's SPEC_062) is deferred since the
SPEC_061 stub already produces useful EPA-ECHO output. The two are swapping
positions in the sequence; the plan doc is updated to reflect.

## Goal

Make the Market Intelligence Pack (SPEC_061) commercially orderable. Three
public, no-auth endpoints land an order request, surface live pricing, and
expose the NAICS + MSA taxonomies so the intake page (SPEC_064 next) can
populate its pickers. Orders are persisted to a new `diligence_orders` table;
the response payload carries the matching Stripe Payment Link URL so the
buyer can pay immediately. Order delivery is manual for v1 — a best-effort
notification email fires to the operator on every new request.

## Acceptance Criteria

- [ ] Three settings fields added to `app/core/config.py`:
  - `stripe_payment_link_url_2500: Optional[str]` (Stripe Payment Link for the $2,500 single map SKU)
  - `stripe_payment_link_url_7500: Optional[str]` (Stripe Payment Link for the $7,500 pilot SKU)
  - `diligence_notify_email: Optional[str]` (where new-order notifications go)
- [ ] `diligence_orders` table created idempotently on `DiligenceOrderService` instantiation. Schema matches §Design Notes exactly.
- [ ] `POST /api/v1/diligence/request` — public, no auth.
  - Validates `naics_code` against `taxonomies.load_naics`; rejects unknown / non-4-digit codes with 422.
  - Validates `msa_code` (mode='msa') against `taxonomies.load_msa`; rejects unknown with 422.
  - Accepts `state_fips` (mode='state') and `county_fips_list` (mode='multi_county').
  - Validates `sku ∈ {single_map, pilot_3_maps, retainer}`; 422 otherwise.
  - Persists the order with `status='requested'`.
  - Returns `{order_id, payment_url, sku, status}`.
  - Best-effort email notification to `diligence_notify_email` via EmailService (failure logged, never blocks response).
- [ ] `GET /api/v1/diligence/skus` — public.
  - Returns the price ladder, payment-URL availability flag (NOT the URL itself — clients get the URL only after a successful POST), and a short copy block per SKU.
- [ ] `GET /api/v1/diligence/taxonomies` — public.
  - Returns `{naics: {...top-level sectors...}, msa: [{cbsa_code, title, state_abbrs}]}`. NAICS is trimmed to 2-digit sectors + 4-digit industries (the picker's two-level shape); full hierarchy under each.
  - Cached at process startup (taxonomies are immutable per-deploy).
- [ ] Notify-email failure does NOT fail the intake (T8 specifically tests this).
- [ ] All SQL parameterized.
- [ ] No regression: full SPEC_052-061 + config test suite stays green.

## Test Cases

| ID  | Test Name                                              | What It Verifies                                                                      |
|-----|--------------------------------------------------------|----------------------------------------------------------------------------------------|
| T1  | test_request_happy_path_creates_order_row              | Valid POST → 200, row exists with status='requested' + correct fields                  |
| T2  | test_request_returns_correct_stripe_link_per_sku       | sku='single_map' → 2500 URL; sku='pilot_3_maps' → 7500 URL                             |
| T3  | test_request_unknown_naics_returns_422                 | `naics_code='99999'` → 422                                                             |
| T4  | test_request_non_4digit_naics_returns_422              | `naics_code='332'` (3-digit) → 422                                                     |
| T5  | test_request_unknown_msa_returns_422                   | `msa_code='00000'` with mode='msa' → 422                                               |
| T6  | test_request_missing_required_fields_returns_422       | missing contact_email / naics_code / sku → 422 with field names                        |
| T7  | test_request_invalid_sku_returns_422                   | `sku='gold_plan'` → 422                                                                |
| T8  | test_request_notify_email_failure_does_not_block       | Mock email service raises → request still returns 200                                  |
| T9  | test_skus_endpoint_returns_pricing                     | GET /skus → all 3 SKUs with prices + copy + has_payment_url flag                       |
| T10 | test_taxonomies_endpoint_returns_naics_and_msa         | GET /taxonomies → NAICS sectors + MSA list, expected key shapes                        |
| T11 | test_table_migration_idempotent                        | Calling DiligenceOrderService() twice does not error                                   |
| T12 | test_state_mode_intake_works                           | `geography_mode='state'`, `state_fips='48'` → order persisted with state_fips column   |

## Rubric Checklist

_(No `api_endpoint.md` rubric — generic checklist.)_

- [ ] Public router; no `Depends(get_current_user)` on the intake endpoints — anonymous orders allowed.
- [ ] All input validated server-side (do NOT trust the client to send a valid NAICS).
- [ ] SQL parameterized via `text(...)` + `:param`.
- [ ] Idempotent `_ensure_tables()` — `CREATE TABLE IF NOT EXISTS` + `CREATE INDEX IF NOT EXISTS`.
- [ ] No PII collected beyond what the user provides voluntarily (contact_name, contact_email, contact_org).
- [ ] No secrets in code: Stripe URLs from settings; treated as optional (router still works when unset — returns `payment_url=None` with a hint).
- [ ] Logger used; no `print()`.
- [ ] Defensive: notify-email failure wrapped in try/except.

## Design Notes

### Schema

```sql
CREATE TABLE IF NOT EXISTS diligence_orders (
    id              SERIAL PRIMARY KEY,
    requested_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    contact_name    TEXT NOT NULL,
    contact_email   TEXT NOT NULL,
    contact_org     TEXT,
    naics_code      TEXT NOT NULL,
    naics_label     TEXT,                                       -- denormalized for admin UI
    msa_code        TEXT,
    msa_title       TEXT,                                       -- denormalized
    state_fips      TEXT,
    geography_mode  TEXT NOT NULL,
    geography_note  TEXT,
    client_note     TEXT,
    sku             TEXT NOT NULL,                              -- 'single_map'|'pilot_3_maps'|'retainer'
    status          TEXT NOT NULL DEFAULT 'requested',
    -- 'requested' → 'scoped' → 'paid' → 'generating' → 'delivered' → 'closed'
    stripe_session_id TEXT,                                     -- filled in later by webhook (deferred)
    paid_at         TIMESTAMP,
    report_ids      INTEGER[],                                  -- linked reports.id values
    delivered_at    TIMESTAMP,
    notes           TEXT,
    source          TEXT                                        -- 'landing'|'outreach'|'referral'|'direct'
);
CREATE INDEX IF NOT EXISTS diligence_orders_status_idx ON diligence_orders(status);
CREATE INDEX IF NOT EXISTS diligence_orders_email_idx  ON diligence_orders(contact_email);
CREATE INDEX IF NOT EXISTS diligence_orders_requested_at_idx ON diligence_orders(requested_at DESC);
```

### SKU table

| `sku`              | Price | Copy |
|--------------------|-------|------|
| `single_map`       | $2,500 | "One sector × MSA intelligence pack delivered in 24–48 hours." |
| `pilot_3_maps`     | $7,500 | "Three maps + a comparable view. 5-day delivery." |
| `retainer`         | $10K–$15K/mo | "Weekly sourcing + map refreshes for a portfolio of sectors. Invoice-billed." |

`retainer` has no Stripe Payment Link in v1 — response returns `payment_url=None` and `next_step="We'll reach out within one business day to scope the retainer."`

### Service shape

```python
# app/services/diligence/orders.py
class DiligenceOrderService:
    def __init__(self, db: Session):
        self.db = db
        self._ensure_tables()

    def _ensure_tables(self) -> None: ...

    def create_order(
        self,
        contact_name: str,
        contact_email: str,
        naics_code: str,
        sku: str,
        geography_mode: str,
        msa_code: Optional[str] = None,
        state_fips: Optional[str] = None,
        contact_org: Optional[str] = None,
        client_note: Optional[str] = None,
        geography_note: Optional[str] = None,
        source: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Validate inputs, persist row, return {order_id, payment_url, sku, status}.
        Raises ValueError on any validation failure — caller turns to HTTP 422."""
```

### Router shape

```python
# app/api/v1/diligence.py
router = APIRouter(prefix="/diligence", tags=["Diligence (Sector × Market Intelligence)"])

class RequestBody(BaseModel):
    contact_name: str
    contact_email: EmailStr
    naics_code: str
    sku: Literal["single_map", "pilot_3_maps", "retainer"]
    geography_mode: Literal["msa", "state", "multi_county"]
    msa_code: Optional[str] = None
    state_fips: Optional[str] = None
    county_fips_list: Optional[List[str]] = None
    contact_org: Optional[str] = None
    client_note: Optional[str] = None
    geography_note: Optional[str] = None
    source: Optional[str] = None

@router.post("/request") def request_order(...): ...
@router.get("/skus")      def get_skus(): ...
@router.get("/taxonomies")def get_taxonomies(): ...
```

### Best-effort notify email

In the request handler, after persistence:

```python
try:
    settings = get_settings()
    if settings.diligence_notify_email:
        from app.services.email import get_email_service
        service = get_email_service()
        asyncio.create_task(service.send(...))  # fire and forget
except Exception as exc:
    logger.warning("diligence notify-email failed: %s", exc)
```

The pattern matches the existing `_notify_lead_event` in `app/users/auth.py` (guarded import + try/except + best-effort).

### Test plan

- T1-T2, T11-T12 use a real Postgres `pg_session` fixture (same fixture style as SPEC_053/054/055 in this codebase — connect to docker postgres, clean rows with the test-marker prefix on setup/teardown).
- T3-T7, T9-T10 use `fastapi.testclient.TestClient` against an in-memory FastAPI app with only the diligence router mounted.
- T8 uses `monkeypatch` to inject a fake `get_email_service` that raises.

## Files to Create/Modify

| File                                                          | Action  | Description                                                       |
|---------------------------------------------------------------|---------|-------------------------------------------------------------------|
| `docs/specs/SPEC_062_diligence_orders_intake_api.md`          | Create  | This file                                                         |
| `tests/test_spec_062_diligence_orders_intake_api.py`          | Create  | Skeleton + T1-T12                                                 |
| `docs/specs/.active_spec`                                     | Modify  | → `SPEC_062_diligence_orders_intake_api`                          |
| `app/core/config.py`                                          | Modify  | +3 settings fields                                                |
| `app/services/diligence/orders.py`                            | Create  | `DiligenceOrderService` class                                     |
| `app/api/v1/diligence.py`                                     | Create  | Public router with 3 endpoints                                    |
| `app/main.py`                                                 | Modify  | Register router (one line) + import (one line)                    |
| `docs/plans/PLAN_065_industrial_diligence_pack.md`            | Modify  | Renumber: this is SPEC_062 (was 063); named-operators is 063 (was 062) |

## Feedback History

_No corrections yet._
