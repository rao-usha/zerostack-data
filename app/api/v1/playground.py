"""
Synthetic Data Playground — public, quota-metered API.  PLAN_063 / SPEC_057.

The free self-serve front door: anonymous visitors (and authenticated free-tier
users) run Nexdata's synthetic generators, get a shareable self-contained HTML
report, and become intent-scored leads. This router is registered WITHOUT
`dependencies=_auth` — it self-gates via the `PlaygroundQuota` dependency.

Endpoints:
  GET  /playground/generators        — generator metadata (drives the UI)
  GET  /playground/quota             — remaining runs for the caller (peek)
  POST /playground/run               — run a generator -> report + lead capture
  GET  /playground/report/{code}     — serve a shared report by short_code (un-gated)
"""

import logging
import secrets
import string
import time
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, Header, HTTPException, Request, status
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.core.database import get_db

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/playground", tags=["Synthetic Data Playground"])


# ---------------------------------------------------------------------------
# Generator metadata — also the dispatch allow-list
# ---------------------------------------------------------------------------

GENERATORS: Dict[str, Dict[str, Any]] = {
    "private-financials": {
        "name": "private-financials",
        "label": "Synthetic Private-Company Financials",
        "description": (
            "Correlated revenue + margin profiles for synthetic private companies, "
            "fitted from EDGAR peer data."
        ),
        "high_value": True,
        "param_schema": {
            "sector": {"type": "string", "default": "industrials"},
            "revenue_min_millions": {"type": "number", "default": 10},
            "revenue_max_millions": {"type": "number", "default": 500},
            "n_companies": {"type": "integer", "default": 20, "min": 1, "max": 100},
            "seed": {"type": "integer", "default": None},
        },
    },
    "macro-scenarios": {
        "name": "macro-scenarios",
        "label": "Macro Scenario Paths",
        "description": (
            "Monte-Carlo macro scenario paths (rates, unemployment, CPI, sentiment) "
            "calibrated from 60 years of FRED history."
        ),
        "high_value": False,
        "param_schema": {
            "n_scenarios": {"type": "integer", "default": 100, "min": 1, "max": 1000},
            "horizon_months": {"type": "integer", "default": 24, "min": 1, "max": 120},
            "series": {
                "type": "array",
                "default": ["DFF", "DGS10", "DGS2", "UNRATE", "CPIAUCSL", "UMCSENT"],
            },
            "seed": {"type": "integer", "default": None},
        },
    },
    "consumer-crowd": {
        "name": "consumer-crowd",
        "label": "Consumer-Crowd Response",
        "description": (
            "Predicted response of a 50-persona synthetic consumer crowd to a "
            "pricing / product / brand scenario."
        ),
        "high_value": True,
        "param_schema": {
            "scenario_id": {"type": "integer", "default": 1, "min": 1, "max": 40},
            "persona_count": {"type": "integer", "default": 50, "min": 1, "max": 50},
        },
    },
}

# free-tier per-run caps so a report build stays fast
_FREE_TIER_N_CAP = {
    "private-financials": 100,
    "macro-scenarios": 500,
    "consumer-crowd": 50,
}


# ---------------------------------------------------------------------------
# Quota dependency — self-gates the public router
# ---------------------------------------------------------------------------

class PlaygroundQuota:
    """Per-request quota gate. Anonymous visitors are metered by IP, authed
    users by user id + tier limit. Consumes one token on a successful check."""

    async def __call__(
        self,
        request: Request,
        authorization: Optional[str] = Header(None),
        db: Session = Depends(get_db),
    ) -> Dict[str, Any]:
        from app.services.quota.quota_service import QuotaService

        settings = get_settings()
        user_id: Optional[int] = None
        email: Optional[str] = None
        tier = "anonymous"

        # Resolve an authenticated user, if a valid Bearer token is present.
        if authorization and authorization.startswith("Bearer "):
            token = authorization[7:]
            try:
                from app.users.auth import AUD_APP, AUD_PLAYGROUND, AuthService

                info = AuthService(db).verify_token(token, audiences=(AUD_APP, AUD_PLAYGROUND))
                user_id = info["user_id"]
                email = info.get("email")
                row = db.execute(
                    text("SELECT tier FROM users WHERE id = :id"), {"id": user_id}
                ).fetchone()
                tier = (row[0] if row and row[0] else "free")
            except Exception:  # noqa: BLE001 — bad token falls through to anon
                user_id = None

        if user_id is not None:
            subject_type = "user"
            subject_key = str(user_id)
            if tier == "enterprise":
                limit = 10_000_000
            elif tier == "pro":
                limit = settings.playground_pro_runs_per_day
            else:
                limit = settings.playground_free_runs_per_day
        else:
            subject_type = "ip"
            subject_key = request.client.host if request.client else "unknown"
            limit = settings.playground_anon_runs_per_ip

        quota = QuotaService(db)
        result = quota.check_and_consume(subject_type, subject_key, limit)

        if not result.allowed:
            action = "upgrade" if subject_type == "user" else "signup_required"
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail={
                    "detail": (
                        "Free run limit reached. Sign up to keep going."
                        if action == "signup_required"
                        else "Daily free-tier limit reached."
                    ),
                    "action": action,
                    "reset_ts": result.reset_ts,
                },
                headers={
                    "X-RateLimit-Limit": str(result.limit),
                    "X-RateLimit-Remaining": "0",
                    "Retry-After": str(
                        max(1, int(
                            __import__("datetime").datetime.fromisoformat(result.reset_ts)
                            .timestamp() - time.time()
                        ))
                    ),
                },
            )

        state = {
            "subject_type": subject_type,
            "subject_key": subject_key,
            "user_id": user_id,
            "email": email,
            "tier": tier,
            "remaining": result.remaining,
            "reset_ts": result.reset_ts,
        }
        request.state.playground_quota = state
        return state


# ---------------------------------------------------------------------------
# Request model
# ---------------------------------------------------------------------------

class RunRequest(BaseModel):
    generator: str = Field(..., description="One of the keys in GENERATORS")
    params: Dict[str, Any] = Field(default_factory=dict)


# ---------------------------------------------------------------------------
# Generator dispatch + output normalization
# ---------------------------------------------------------------------------

def _run_generator(db: Session, generator: str, params: Dict[str, Any]) -> Dict[str, Any]:
    """Call the existing generator service. Never reimplements generation."""
    if generator == "private-financials":
        from app.services.synthetic.private_company_financials import (
            PrivateCompanyFinancialGenerator,
        )

        n = min(int(params.get("n_companies", 20)), _FREE_TIER_N_CAP[generator])
        return PrivateCompanyFinancialGenerator(db).generate(
            n_companies=n,
            sector=params.get("sector", "industrials"),
            revenue_min_millions=float(params.get("revenue_min_millions", 10)),
            revenue_max_millions=float(params.get("revenue_max_millions", 500)),
            seed=params.get("seed"),
        )

    if generator == "macro-scenarios":
        from app.services.synthetic.macro_scenarios import MacroScenarioGenerator

        n = min(int(params.get("n_scenarios", 100)), _FREE_TIER_N_CAP[generator])
        return MacroScenarioGenerator(db).generate(
            n_scenarios=n,
            horizon_months=int(params.get("horizon_months", 24)),
            series=params.get("series")
            or ["DFF", "DGS10", "DGS2", "UNRATE", "CPIAUCSL", "UMCSENT"],
            seed=params.get("seed"),
        )

    if generator == "consumer-crowd":
        from app.api.v1.synthetic_crowd import _get_model
        from app.services.synthetic.consumer_crowd.personas import PERSONAS
        from app.services.synthetic.consumer_crowd.scenarios import get_scenario_by_id

        model = _get_model()
        if model is None:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Consumer-crowd model artifact not loaded.",
            )
        scenario = get_scenario_by_id(int(params.get("scenario_id", 1)))
        count = min(int(params.get("persona_count", 50)), _FREE_TIER_N_CAP[generator])
        personas = list(PERSONAS)[:count]
        responses = model.predict_crowd(scenario, personas=personas)
        aggregate = model.aggregate(responses)
        return {
            "scenario": {
                "id": scenario.id,
                "category": scenario.category,
                "event_type": scenario.event_type,
                "description": scenario.description,
            },
            "aggregate": aggregate.to_dict(),
            "responses": [r.to_dict() for r in responses],
        }

    raise HTTPException(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        detail=f"Unknown generator '{generator}'. Valid: {', '.join(GENERATORS)}",
    )


def _normalize(generator: str, raw: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
    """Generic fallback normalizer for any generator without a dedicated one.

    Defensive — extracts a `summary` from top-level scalar fields and `rows`
    from the first list-of-dicts in the result, so it tolerates shape
    variation across generators without raising.
    """
    summary: Dict[str, Any] = {}
    rows: List[Dict[str, Any]] = []

    if isinstance(raw, dict):
        # summary: top-level scalars (skip noisy/internal keys)
        for k, v in raw.items():
            if k.startswith("_"):
                continue
            if isinstance(v, bool) or isinstance(v, (int, float, str)):
                summary[_titleize(k)] = v
            if len(summary) >= 6:
                break
        # rows: first list-of-dicts found
        for v in raw.values():
            if isinstance(v, list) and v and isinstance(v[0], dict):
                rows = v
                break
        # nested aggregate dict (consumer-crowd) -> fold into summary
        agg = raw.get("aggregate")
        if isinstance(agg, dict):
            for k, v in agg.items():
                if isinstance(v, (int, float)) and len(summary) < 6:
                    summary[_titleize(k)] = round(v, 3) if isinstance(v, float) else v

    meta = GENERATORS.get(generator, {})
    return {
        "generator": generator,
        "generator_label": meta.get("label", generator.replace("-", " ").title()),
        "request_params": {k: v for k, v in params.items() if v is not None},
        "summary": summary,
        "rows": rows,
    }


# ── Per-generator normalizers (SPEC_059) ──────────────────────────────────
#
# Each takes (raw, params) — the raw dict the generator returns plus the
# inbound request params — and produces the canonical playground-report
# payload: `summary` (KPI strip), `rows` (sample table, <=20 shown), and an
# optional `chart` ({labels, series:[{label,data}], y_label}). The generic
# `_normalize()` above stays as a fallback for future generators that haven't
# been given a dedicated normalizer yet.

def _normalize_macro(raw: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
    """Flatten macro-scenario output into a median-path chart + percentile rows."""
    series_list: List[str] = list(raw.get("series") or [])
    scenarios = raw.get("scenarios") or []
    summary_in = raw.get("summary") or {}
    current = raw.get("current_values") or {}
    horizon = int(raw.get("horizon_months") or 0)

    # Median path per series, computed elementwise across scenarios. Median (not
    # mean) preserves heavy tails — UNRATE / UMCSENT / DCOILWTICO shouldn't be
    # visually compressed by extreme draws.
    chart_series: List[Dict[str, Any]] = []
    for s in series_list:
        paths = [
            sc["paths"][s]
            for sc in scenarios
            if isinstance(sc.get("paths"), dict) and s in sc["paths"]
        ]
        if not paths:
            continue
        length = min(len(p) for p in paths)
        median = [
            round(float(sorted(p[t] for p in paths)[len(paths) // 2]), 4)
            for t in range(length)
        ]
        chart_series.append({"label": s, "data": median})

    labels = [f"M{t + 1}" for t in range(horizon)]

    rows = [
        {
            "series": s,
            "current": current.get(s),
            "p10": (summary_in.get(s) or {}).get("p10_terminal"),
            "p50": (summary_in.get(s) or {}).get("p50_terminal"),
            "p90": (summary_in.get(s) or {}).get("p90_terminal"),
        }
        for s in series_list
    ]

    return {
        "generator": "macro-scenarios",
        "generator_label": GENERATORS["macro-scenarios"]["label"],
        "request_params": {k: v for k, v in params.items() if v is not None},
        "summary": {
            "Scenarios": raw.get("n_scenarios"),
            "Horizon": f"{horizon} mo",
            "Series": len(series_list),
            "History (mo)": raw.get("training_history_months"),
        },
        "rows": rows,
        "chart": {"labels": labels, "series": chart_series, "y_label": "Level"},
    }


def _normalize_private_financials(
    raw: Dict[str, Any], params: Dict[str, Any]
) -> Dict[str, Any]:
    """Sector + ratio means in the KPI strip; companies straight into the table."""
    rs = raw.get("ratio_stats") or {}

    def _mean(stat_key: str) -> Optional[float]:
        block = rs.get(stat_key)
        if not isinstance(block, dict):
            return None
        return round(float(block.get("mean", 0.0)), 4)

    return {
        "generator": "private-financials",
        "generator_label": GENERATORS["private-financials"]["label"],
        "request_params": {k: v for k, v in params.items() if v is not None},
        "summary": {
            "Sector": raw.get("sector"),
            "Peer Count": raw.get("peer_count"),
            "Synthetic Companies": raw.get("synthetic_count"),
            "Gross Margin (mean)": _mean("gross_margin"),
            "EBITDA Margin (mean)": _mean("ebitda_margin"),
            "Net Margin (mean)": _mean("net_margin"),
        },
        "rows": raw.get("companies") or [],
        "chart": None,
    }


def _normalize_consumer_crowd(
    raw: Dict[str, Any], params: Dict[str, Any]
) -> Dict[str, Any]:
    """Scenario context + aggregate scalars in the KPI strip; responses → rows."""
    scen = raw.get("scenario") or {}
    agg = raw.get("aggregate") or {}

    summary: Dict[str, Any] = {
        "Scenario": scen.get("category"),
        "Event": scen.get("event_type"),
        "Description": scen.get("description"),
    }
    for k, v in agg.items():
        if isinstance(v, (int, float)) and len(summary) < 6:
            summary[_titleize(k)] = round(v, 3) if isinstance(v, float) else v

    return {
        "generator": "consumer-crowd",
        "generator_label": GENERATORS["consumer-crowd"]["label"],
        "request_params": {k: v for k, v in params.items() if v is not None},
        "summary": summary,
        "rows": raw.get("responses") or [],
        "chart": None,
    }


_NORMALIZERS = {
    "private-financials": _normalize_private_financials,
    "macro-scenarios": _normalize_macro,
    "consumer-crowd": _normalize_consumer_crowd,
}


def _normalize_dispatch(
    generator: str, raw: Dict[str, Any], params: Dict[str, Any]
) -> Dict[str, Any]:
    """Pick the dedicated normalizer for `generator` or fall back to generic."""
    fn = _NORMALIZERS.get(generator)
    return fn(raw, params) if fn else _normalize(generator, raw, params)


def _titleize(key: str) -> str:
    return str(key).replace("_", " ").title()


def _short_code(n: int = 10) -> str:
    alphabet = string.ascii_letters + string.digits
    return "".join(secrets.choice(alphabet) for _ in range(n))


def _n_requested(generator: str, params: Dict[str, Any]) -> int:
    if generator == "private-financials":
        return int(params.get("n_companies", 20))
    if generator == "macro-scenarios":
        return int(params.get("n_scenarios", 100))
    if generator == "consumer-crowd":
        return int(params.get("persona_count", 50))
    return 0


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get("/generators")
def list_generators():
    """Generator metadata + parameter schemas — drives the playground UI."""
    return {"generators": list(GENERATORS.values())}


@router.get("/quota")
def get_quota(
    request: Request,
    authorization: Optional[str] = Header(None),
    db: Session = Depends(get_db),
):
    """Remaining playground runs for the caller — read-only, never consumes."""
    from app.services.quota.quota_service import QuotaService

    settings = get_settings()
    user_id: Optional[int] = None
    tier = "anonymous"
    if authorization and authorization.startswith("Bearer "):
        try:
            from app.users.auth import AUD_APP, AUD_PLAYGROUND, AuthService

            info = AuthService(db).verify_token(
                authorization[7:], audiences=(AUD_APP, AUD_PLAYGROUND)
            )
            user_id = info["user_id"]
            row = db.execute(
                text("SELECT tier FROM users WHERE id = :id"), {"id": user_id}
            ).fetchone()
            tier = row[0] if row and row[0] else "free"
        except Exception:  # noqa: BLE001
            user_id = None

    if user_id is not None:
        subject_type, subject_key = "user", str(user_id)
        limit = (
            10_000_000 if tier == "enterprise"
            else settings.playground_pro_runs_per_day if tier == "pro"
            else settings.playground_free_runs_per_day
        )
    else:
        subject_type = "ip"
        subject_key = request.client.host if request.client else "unknown"
        limit = settings.playground_anon_runs_per_ip

    result = QuotaService(db).peek(subject_type, subject_key, limit)
    return {
        "tier": tier,
        "limit": result.limit,
        "remaining": result.remaining,
        "used": result.used,
        "reset_ts": result.reset_ts,
        "authenticated": user_id is not None,
    }


@router.post("/run")
def run_generator(
    req: RunRequest,
    request: Request,
    quota: Dict[str, Any] = Depends(PlaygroundQuota()),
    db: Session = Depends(get_db),
):
    """Run a synthetic generator, build a shareable report, capture the lead."""
    if req.generator not in GENERATORS:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Unknown generator '{req.generator}'. Valid: {', '.join(GENERATORS)}",
        )

    # 1. Run the generator (quota token already consumed by the dependency).
    try:
        raw = _run_generator(db, req.generator, req.params)
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        logger.error("Playground generator %s failed: %s", req.generator, exc)
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Generator '{req.generator}' failed to produce a result.",
        )

    # 2. Normalize -> build the shareable report. Per-generator normalizers
    #    (SPEC_059) produce richer summary/rows/chart payloads; unknown
    #    generators still go through the generic fallback inside _normalize_dispatch.
    payload = _normalize_dispatch(req.generator, raw, req.params)
    short_code = _short_code()
    payload["ref"] = short_code  # CTA attribution

    from app.reports.builder import ReportBuilder

    report = ReportBuilder(db).generate(
        template_name="synthetic_playground",
        format="html",
        params=payload,
        title=f"Synthetic Sample — {payload['generator_label']}",
    )
    report_id = report["id"]

    # 3. Mark the report public + addressable by short_code.
    db.execute(
        text("UPDATE reports SET short_code = :sc, is_public = TRUE WHERE id = :id"),
        {"sc": short_code, "id": report_id},
    )
    db.commit()

    # 4. Capture the lead (best-effort — never fail the run on a lead-hook error).
    try:
        from app.services.leads.lead_service import LeadService

        LeadService(db).record_run(
            email=quota.get("email"),
            user_id=quota.get("user_id"),
            generator=req.generator,
            n_requested=_n_requested(req.generator, req.params),
            anon_ip=quota.get("subject_key") if quota.get("subject_type") == "ip" else None,
            report_id=report_id,
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("Playground lead capture failed: %s", exc)

    return {
        "generator": req.generator,
        "result": payload["summary"],
        "preview_rows": payload["rows"][:10],
        "report": {"short_code": short_code, "url_path": f"/p/{short_code}"},
        "quota": {"remaining": quota.get("remaining"), "reset_ts": quota.get("reset_ts")},
    }


@router.get("/report/{short_code}")
def serve_report(short_code: str, db: Session = Depends(get_db)):
    """Serve a shared playground report by short_code — un-gated, renders inline."""
    row = db.execute(
        text("""
        SELECT file_path, status, is_public
        FROM reports WHERE short_code = :sc
    """),
        {"sc": short_code},
    ).fetchone()

    if not row or not row[2] or row[1] != "complete":
        raise HTTPException(status_code=404, detail="Report not found")

    file_path = row[0]
    if not file_path:
        raise HTTPException(status_code=404, detail="Report file not found")

    import os

    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="Report file not found")

    # Best-effort view counter for viral-loop analytics.
    try:
        db.execute(
            text("UPDATE reports SET view_count = COALESCE(view_count, 0) + 1 "
                 "WHERE short_code = :sc"),
            {"sc": short_code},
        )
        db.commit()
    except Exception:  # noqa: BLE001
        db.rollback()

    return FileResponse(path=file_path, media_type="text/html")
