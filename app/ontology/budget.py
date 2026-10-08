"""Hard budget guard for ontology model calls (SPEC_164, PLAN_100 §9.3).

Before every call the guard reserves the call's **worst-case** cost
(``in_tok × p_in + max_output_tokens × p_out`` at the long-context tier the
prompt selects) against three scopes:

* **call** — ``ONTOLOGY_CALL_CAP_USD`` (default $10), checked in process;
* **round/pilot** — a budget row the run was started with;
* **month** — a budget row per calendar month (``YYYY-MM``).

The round and month reservation happens in one transaction with both rows
locked (``SELECT ... FOR UPDATE``, ordered by id), so two concurrent workers
cannot both pass a cap. A reservation that would exceed any cap raises
``BudgetExceeded`` **before** anything is sent. After the call the
reservation is settled to actual usage; it is released when the call failed
in a way that cannot have been billed. A timeout (billing unknown) is settled
at the worst case, conservatively.

Storage sits behind ``BudgetStore``: ``MemoryBudgetStore`` for tests and
local use, ``PostgresBudgetStore`` for workers. This spec ships no migration;
``BUDGET_DDL`` is the DDL SPEC_165 puts into its Alembic revision.

D2 defaults: pilot $20, round $100, month $150, ``allow_premium=False``.
"""
from __future__ import annotations

import asyncio
import itertools
import json
import logging
import os
import threading
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Any, Awaitable, Callable, Dict, List, Mapping, Optional, Sequence

from app.ontology.pricing import (
    DEFAULT_PREMIUM_OUTPUT_PER_M,
    DEFAULT_PRICE_MAX_AGE_DAYS,
    PriceEntry,
    PriceGateError,
    actual_cost,
    check_price_gate,
    expected_cost,
    worst_case_cost,
)
from app.ontology.providers import (
    CallResult,
    ProviderConnectionError,
    ProviderSpec,
    ProviderTimeout,
    RosterError,
)
from app.ontology.tokens import count_messages

logger = logging.getLogger(__name__)

SCOPES = ("pilot", "round", "month")
DEFAULT_EXPECTED_OUT_TOKENS = 40_000  # PLAN_100 §9.1


class BudgetExceeded(RuntimeError):
    """A reservation would exceed a cap. Nothing was sent."""

    def __init__(self, scope: str, cap_usd: float, needed_usd: float, available_usd: float, detail: str = ""):
        self.scope = scope
        self.cap_usd = cap_usd
        self.needed_usd = needed_usd
        self.available_usd = available_usd
        super().__init__(
            f"budget {scope}: need ${needed_usd:.4f}, ${available_usd:.4f} of ${cap_usd:.2f} left"
            + (f" ({detail})" if detail else "")
        )


class ReasoningUnbounded(PriceGateError):
    """The adapter cannot bound reasoning tokens and no reservation ceiling is set."""


class WorstCaseNotConfirmed(RuntimeError):
    """Whole-round worst case exceeds the remaining budget; pass confirm_worst_case=True."""

    def __init__(self, estimate: Dict[str, Any]):
        self.estimate = estimate
        super().__init__(
            f"worst case ${estimate['worst_case_usd']:.2f} exceeds remaining ${estimate['remaining_usd']:.2f}; "
            "per-call reservation still enforces the cap; pass confirm_worst_case=True to proceed"
        )


def _r6(x: float) -> float:
    return round(float(x), 6)


# ---------------------------------------------------------------- config


@dataclass
class BudgetConfig:
    pilot_cap_usd: float = 20.0
    round_cap_usd: float = 100.0
    month_cap_usd: float = 150.0
    call_cap_usd: float = 10.0
    price_max_age_days: int = DEFAULT_PRICE_MAX_AGE_DAYS
    premium_output_per_m: float = DEFAULT_PREMIUM_OUTPUT_PER_M

    @classmethod
    def from_env(cls, env: Optional[Mapping[str, str]] = None) -> "BudgetConfig":
        env = os.environ if env is None else env
        d = cls()

        def f(key, default):
            v = env.get(key)
            return float(v) if v not in (None, "") else default

        return cls(
            pilot_cap_usd=f("ONTOLOGY_PILOT_BUDGET_USD", d.pilot_cap_usd),
            round_cap_usd=f("ONTOLOGY_ROUND_BUDGET_USD", d.round_cap_usd),
            month_cap_usd=f("ONTOLOGY_MONTHLY_BUDGET_USD", d.month_cap_usd),
            call_cap_usd=f("ONTOLOGY_CALL_CAP_USD", d.call_cap_usd),
            price_max_age_days=int(f("ONTOLOGY_PRICE_MAX_AGE_DAYS", d.price_max_age_days)),
            premium_output_per_m=f("ONTOLOGY_PREMIUM_OUTPUT_PER_M", d.premium_output_per_m),
        )

    def default_cap(self, scope: str) -> float:
        return {"pilot": self.pilot_cap_usd, "round": self.round_cap_usd, "month": self.month_cap_usd}[scope]


# ---------------------------------------------------------------- storage


@dataclass
class Budget:
    id: int
    scope: str
    cap_usd: float
    reserved_usd: float = 0.0
    spent_usd: float = 0.0
    status: str = "open"
    period_key: Optional[str] = None
    name: Optional[str] = None
    created_by: Optional[str] = None

    @property
    def available_usd(self) -> float:
        return self.cap_usd - self.spent_usd - self.reserved_usd


AfterLockHook = Optional[Callable[[List[Budget]], None]]


class BudgetStore:
    """Interface. ``reserve`` must be atomic across all ``budget_ids``."""

    def create_budget(self, scope: str, cap_usd: float, period_key: Optional[str] = None,
                      name: Optional[str] = None, created_by: Optional[str] = None) -> int:
        raise NotImplementedError

    def get_or_create_budget(self, scope: str, period_key: str, cap_usd: float) -> int:
        raise NotImplementedError

    def get(self, budget_id: int) -> Budget:
        raise NotImplementedError

    def reserve(self, budget_ids: Sequence[int], amount_usd: float, meta: Optional[Dict[str, Any]] = None,
                _after_lock: AfterLockHook = None) -> int:
        raise NotImplementedError

    def settle(self, reservation_id: int, actual_usd: float, cost_basis: str) -> None:
        raise NotImplementedError

    def release(self, reservation_id: int) -> None:
        raise NotImplementedError


def _check_caps(budgets: List[Budget], amount: float) -> None:
    for b in budgets:
        if b.status != "open":
            raise BudgetExceeded(b.scope, b.cap_usd, amount, 0.0, f"budget {b.id} is {b.status}")
        if b.spent_usd + b.reserved_usd + amount > b.cap_usd + 1e-9:
            raise BudgetExceeded(b.scope, b.cap_usd, amount, max(b.available_usd, 0.0), f"budget {b.id}")


class MemoryBudgetStore(BudgetStore):
    """Process-local store (tests, laptop dry runs). Atomic under one lock."""

    def __init__(self):
        self._lock = threading.Lock()
        self._budgets: Dict[int, Budget] = {}
        self._res: Dict[int, Dict[str, Any]] = {}
        self._ids = itertools.count(1)
        self._res_ids = itertools.count(1)

    def create_budget(self, scope, cap_usd, period_key=None, name=None, created_by=None):
        if scope not in SCOPES:
            raise ValueError(f"scope must be one of {SCOPES}")
        with self._lock:
            bid = next(self._ids)
            self._budgets[bid] = Budget(bid, scope, _r6(cap_usd), period_key=period_key, name=name,
                                        created_by=created_by)
            return bid

    def get_or_create_budget(self, scope, period_key, cap_usd):
        with self._lock:
            for b in self._budgets.values():
                if b.scope == scope and b.period_key == period_key:
                    return b.id
        return self.create_budget(scope, cap_usd, period_key=period_key)

    def get(self, budget_id):
        with self._lock:
            b = self._budgets[budget_id]
            return Budget(**b.__dict__)

    def reserve(self, budget_ids, amount_usd, meta=None, _after_lock=None):
        amount = _r6(amount_usd)
        with self._lock:
            budgets = [self._budgets[i] for i in sorted(set(budget_ids))]
            if _after_lock:
                _after_lock(budgets)
            _check_caps(budgets, amount)
            for b in budgets:
                b.reserved_usd = _r6(b.reserved_usd + amount)
            rid = next(self._res_ids)
            self._res[rid] = {"id": rid, "budget_ids": [b.id for b in budgets], "amount_usd": amount,
                              "actual_usd": None, "status": "reserved", "cost_basis": None,
                              "meta": dict(meta or {})}
            return rid

    def _close(self, reservation_id, actual, cost_basis, status):
        with self._lock:
            r = self._res[reservation_id]
            if r["status"] != "reserved":
                return
            for bid in r["budget_ids"]:
                b = self._budgets[bid]
                b.reserved_usd = _r6(max(b.reserved_usd - r["amount_usd"], 0.0))
                b.spent_usd = _r6(b.spent_usd + actual)
            r.update(actual_usd=actual, status=status, cost_basis=cost_basis)

    def settle(self, reservation_id, actual_usd, cost_basis):
        self._close(reservation_id, _r6(actual_usd), cost_basis, "settled")

    def release(self, reservation_id):
        self._close(reservation_id, 0.0, "released", "released")

    def reservations(self) -> List[Dict[str, Any]]:
        with self._lock:
            return [dict(r) for r in self._res.values()]


# DDL for SPEC_165's Alembic revision (schema ``onto``). Idempotent.
BUDGET_DDL = """
CREATE SCHEMA IF NOT EXISTS onto;

CREATE TABLE IF NOT EXISTS onto.budget (
    id            BIGSERIAL PRIMARY KEY,
    scope         TEXT NOT NULL CHECK (scope IN ('pilot', 'round', 'month')),
    name          TEXT,
    period_key    TEXT,
    cap_usd       NUMERIC(14, 6) NOT NULL CHECK (cap_usd >= 0),
    reserved_usd  NUMERIC(14, 6) NOT NULL DEFAULT 0 CHECK (reserved_usd >= 0),
    spent_usd     NUMERIC(14, 6) NOT NULL DEFAULT 0 CHECK (spent_usd >= 0),
    status        TEXT NOT NULL DEFAULT 'open' CHECK (status IN ('open', 'closed', 'exhausted')),
    created_by    TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_onto_budget_month
    ON onto.budget (scope, period_key) WHERE scope = 'month';

CREATE TABLE IF NOT EXISTS onto.budget_reservation (
    id            BIGSERIAL PRIMARY KEY,
    budget_ids    BIGINT[] NOT NULL,
    amount_usd    NUMERIC(14, 6) NOT NULL CHECK (amount_usd >= 0),
    actual_usd    NUMERIC(14, 6),
    status        TEXT NOT NULL DEFAULT 'reserved' CHECK (status IN ('reserved', 'settled', 'released')),
    cost_basis    TEXT,
    provider      TEXT,
    model         TEXT,
    label         TEXT,
    meta          JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    closed_at     TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS ix_onto_budget_reservation_open
    ON onto.budget_reservation (created_at) WHERE status = 'reserved';
"""


class PostgresBudgetStore(BudgetStore):
    """Ledger in ``onto.budget`` / ``onto.budget_reservation`` (DDL: ``BUDGET_DDL``)."""

    def __init__(self, engine: Any, schema: str = "onto"):
        if not schema.isidentifier():
            raise ValueError("schema must be a plain identifier")
        self.engine = engine
        self.schema = schema

    def _t(self, table: str) -> str:
        return f"{self.schema}.{table}"

    @staticmethod
    def _row(r) -> Budget:
        return Budget(id=int(r.id), scope=r.scope, cap_usd=float(r.cap_usd), reserved_usd=float(r.reserved_usd),
                      spent_usd=float(r.spent_usd), status=r.status, period_key=r.period_key, name=r.name,
                      created_by=r.created_by)

    _COLS = "id, scope, name, period_key, cap_usd, reserved_usd, spent_usd, status, created_by"

    def create_budget(self, scope, cap_usd, period_key=None, name=None, created_by=None):
        from sqlalchemy import text

        if scope not in SCOPES:
            raise ValueError(f"scope must be one of {SCOPES}")
        with self.engine.begin() as c:
            return int(c.execute(
                text(f"INSERT INTO {self._t('budget')} (scope, cap_usd, period_key, name, created_by) "
                     "VALUES (:s, :cap, :pk, :n, :cb) RETURNING id"),
                {"s": scope, "cap": _r6(cap_usd), "pk": period_key, "n": name, "cb": created_by},
            ).scalar_one())

    def get_or_create_budget(self, scope, period_key, cap_usd):
        from sqlalchemy import text

        if scope != "month":
            return self.create_budget(scope, cap_usd, period_key=period_key)
        with self.engine.begin() as c:
            c.execute(
                text(f"INSERT INTO {self._t('budget')} (scope, cap_usd, period_key) VALUES ('month', :cap, :pk) "
                     "ON CONFLICT (scope, period_key) WHERE scope = 'month' DO NOTHING"),
                {"cap": _r6(cap_usd), "pk": period_key},
            )
            return int(c.execute(
                text(f"SELECT id FROM {self._t('budget')} WHERE scope = 'month' AND period_key = :pk"),
                {"pk": period_key},
            ).scalar_one())

    def get(self, budget_id):
        from sqlalchemy import text

        with self.engine.connect() as c:
            r = c.execute(text(f"SELECT {self._COLS} FROM {self._t('budget')} WHERE id = :id"),
                          {"id": budget_id}).one()
            return self._row(r)

    def _lock_budgets(self, c, ids: List[int]) -> List[Budget]:
        from sqlalchemy import text

        rows = c.execute(
            text(f"SELECT {self._COLS} FROM {self._t('budget')} WHERE id = ANY(:ids) ORDER BY id FOR UPDATE"),
            {"ids": ids},
        ).all()
        if len(rows) != len(ids):
            raise KeyError(f"unknown budget ids in {ids}")
        return [self._row(r) for r in rows]

    def reserve(self, budget_ids, amount_usd, meta=None, _after_lock=None):
        from sqlalchemy import text

        ids = sorted({int(i) for i in budget_ids})
        amount = _r6(amount_usd)
        meta = dict(meta or {})
        with self.engine.begin() as c:
            budgets = self._lock_budgets(c, ids)
            if _after_lock:
                _after_lock(budgets)
            _check_caps(budgets, amount)
            c.execute(
                text(f"UPDATE {self._t('budget')} SET reserved_usd = reserved_usd + :r, updated_at = now() "
                     "WHERE id = ANY(:ids)"),
                {"r": amount, "ids": ids},
            )
            return int(c.execute(
                text(f"INSERT INTO {self._t('budget_reservation')} "
                     "(budget_ids, amount_usd, provider, model, label, meta) "
                     "VALUES (:ids, :r, :p, :m, :l, CAST(:meta AS JSONB)) RETURNING id"),
                {"ids": ids, "r": amount, "p": meta.get("provider"), "m": meta.get("model"),
                 "l": meta.get("label"), "meta": json.dumps(meta, default=str)},
            ).scalar_one())

    def _close(self, reservation_id, actual, cost_basis, status):
        from sqlalchemy import text

        with self.engine.begin() as c:
            r = c.execute(
                text(f"SELECT budget_ids, amount_usd, status FROM {self._t('budget_reservation')} "
                     "WHERE id = :id FOR UPDATE"),
                {"id": reservation_id},
            ).one()
            if r.status != "reserved":
                return
            ids = sorted(int(i) for i in r.budget_ids)
            self._lock_budgets(c, ids)
            c.execute(
                text(f"UPDATE {self._t('budget')} SET reserved_usd = GREATEST(reserved_usd - :r, 0), "
                     "spent_usd = spent_usd + :a, updated_at = now() WHERE id = ANY(:ids)"),
                {"r": float(r.amount_usd), "a": actual, "ids": ids},
            )
            c.execute(
                text(f"UPDATE {self._t('budget_reservation')} SET status = :s, actual_usd = :a, "
                     "cost_basis = :cb, closed_at = now() WHERE id = :id"),
                {"s": status, "a": actual, "cb": cost_basis, "id": reservation_id},
            )

    def settle(self, reservation_id, actual_usd, cost_basis):
        self._close(reservation_id, _r6(actual_usd), cost_basis, "settled")

    def release(self, reservation_id):
        self._close(reservation_id, 0.0, "released", "released")


# ---------------------------------------------------------------- guard


@dataclass
class Reservation:
    id: int
    name: str
    amount_usd: float
    in_tok: int
    in_tok_estimated: bool
    max_out: int
    budget_ids: List[int]
    spec: ProviderSpec
    entry: Optional[PriceEntry]


@dataclass
class PlannedCall:
    """One call in a round preflight (model × sample × variant × split part)."""

    name: str
    in_tok: Optional[int] = None
    expected_out_tok: Optional[int] = None
    text: Optional[str] = None
    count: int = 1


Recorder = Callable[..., Awaitable[None]]


async def _default_recorder(**kw: Any) -> None:
    from app.core.llm_cost_tracker import get_cost_tracker

    await get_cost_tracker().record(**kw)


@dataclass
class BudgetGuard:
    store: BudgetStore
    registry: Mapping[str, ProviderSpec]
    pricing: Mapping[str, PriceEntry]
    config: BudgetConfig = field(default_factory=BudgetConfig.from_env)
    recorder: Optional[Recorder] = None
    today_fn: Callable[[], date] = field(default=lambda: datetime.now(timezone.utc).date())

    # ---- gates

    def gate(self, name: str, allow_premium: bool = False):
        """Price gate + reasoning-bound gate. Returns (spec, entry or None for local)."""
        spec = self.registry.get(name)
        if spec is None:
            raise RosterError(f"{name}: not in registry")
        entry = check_price_gate(spec, self.pricing, today=self.today_fn(),
                                 max_age_days=self.config.price_max_age_days, allow_premium=allow_premium)
        rc = spec.reasoning_control
        if rc.get("bounded") == "no" and not rc.get("reservation_ceiling_tokens"):
            raise ReasoningUnbounded(
                f"{name}: reasoning tokens are not bounded by the output cap and no reservation ceiling is set"
            )
        return spec, entry

    @staticmethod
    def max_out(spec: ProviderSpec) -> int:
        ceiling = int(spec.reasoning_control.get("reservation_ceiling_tokens") or 0)
        return max(int(spec.max_output_tokens), ceiling)

    def month_key(self) -> str:
        return self.today_fn().strftime("%Y-%m")

    def month_budget_id(self) -> int:
        return self.store.get_or_create_budget("month", self.month_key(), self.config.month_cap_usd)

    # ---- reservation

    def reserve(self, name: str, round_budget_id: int, in_tok: Optional[int] = None,
                messages: Optional[List[Dict[str, Any]]] = None, allow_premium: bool = False,
                label: str = "") -> Reservation:
        spec, entry = self.gate(name, allow_premium)
        estimated = False
        if in_tok is None:
            tc = count_messages(messages or [], spec)
            in_tok, estimated = tc.tokens, not tc.exact
        max_out = self.max_out(spec)
        amount = 0.0 if entry is None else _r6(worst_case_cost(entry, in_tok, max_out, band=estimated))
        if amount > self.config.call_cap_usd + 1e-9:
            raise BudgetExceeded("call", self.config.call_cap_usd, amount, self.config.call_cap_usd,
                                 f"{name} worst case")
        ids = [int(round_budget_id), self.month_budget_id()]
        rid = self.store.reserve(ids, amount, {"provider": spec.provider, "model": spec.model_id,
                                               "registry_name": name, "label": label,
                                               "in_tok": in_tok, "in_tok_estimated": estimated,
                                               "max_out": max_out})
        return Reservation(rid, name, amount, in_tok, estimated, max_out, ids, spec, entry)

    async def guarded_call(self, adapter: Any, round_budget_id: int, messages: List[Dict[str, Any]],
                           schema: Optional[dict] = None, schema_name: str = "output",
                           in_tok: Optional[int] = None, allow_premium: bool = False, label: str = "",
                           temperature: Optional[float] = None, seed: Optional[int] = None) -> CallResult:
        """Reserve → call → settle (or release). Raises before sending when a cap would be exceeded."""
        spec: ProviderSpec = adapter.spec
        res = await asyncio.to_thread(self.reserve, spec.name, round_budget_id, in_tok, messages,
                                      allow_premium, label)
        try:
            result: CallResult = await adapter.complete(messages, schema=schema, schema_name=schema_name,
                                                        temperature=temperature, seed=seed)
        except ProviderTimeout:
            # Billing unknown: keep the worst case as spent (the ledger is the primary control).
            await asyncio.to_thread(self.store.settle, res.id, res.amount_usd, "timeout_worst_case")
            logger.warning("[ontology] %s timed out; settled at worst case $%.4f", spec.name, res.amount_usd)
            raise
        except ProviderConnectionError as e:
            if e.sent:
                await asyncio.to_thread(self.store.settle, res.id, res.amount_usd, "connection_error_worst_case")
            else:
                await asyncio.to_thread(self.store.release, res.id)
            raise
        except BaseException:
            await asyncio.to_thread(self.store.release, res.id)
            raise
        if res.entry is None:
            cost, basis = 0.0, "local"
        else:
            cost, basis = _r6(actual_cost(res.entry, result.usage, spec.reasoning_in_output)), "priced"
            if cost > res.amount_usd + 1e-9:
                logger.warning("[ontology] %s actual $%.4f exceeded reservation $%.4f",
                               spec.name, cost, res.amount_usd)
        await asyncio.to_thread(self.store.settle, res.id, cost, basis)
        result.cost_usd, result.reserved_usd, result.cost_basis = cost, res.amount_usd, basis
        recorder = self.recorder or _default_recorder
        try:
            await recorder(model=spec.model_id, input_tokens=result.usage.input_tokens,
                           output_tokens=result.usage.output_tokens, source=f"ontology:{label or 'call'}",
                           provider=spec.provider, cost_usd=cost,
                           prompt_chars=sum(len(str(m.get("content", ""))) for m in messages))
        except Exception as e:  # never lose the result because logging failed
            logger.warning("[ontology] failed to record usage for %s: %s", spec.name, e)
        return result

    # ---- preflight

    def remaining_usd(self, round_budget_id: int) -> float:
        budgets = [self.store.get(round_budget_id), self.store.get(self.month_budget_id())]
        return min(b.available_usd for b in budgets)

    def preflight(self, planned: Sequence[PlannedCall], round_budget_id: int, confirm_worst_case: bool = False,
                  allow_premium: bool = False) -> Dict[str, Any]:
        """Expected and whole-round worst-case cost; makes no model calls and reserves nothing."""
        per_model: Dict[str, Dict[str, Any]] = {}
        refused: List[Dict[str, str]] = []
        warnings: List[str] = []
        total_exp = total_worst = 0.0
        for p in planned:
            try:
                spec, entry = self.gate(p.name, allow_premium)
            except (PriceGateError, RosterError) as e:
                if not any(r["name"] == p.name for r in refused):
                    refused.append({"name": p.name, "reason": str(e)})
                continue
            estimated = False
            in_tok = p.in_tok
            if in_tok is None:
                from app.ontology.tokens import count_tokens

                tc = count_tokens(p.text or "", spec)
                in_tok, estimated = tc.tokens, not tc.exact
            max_out = self.max_out(spec)
            out = min(p.expected_out_tok or DEFAULT_EXPECTED_OUT_TOKENS, max_out)
            try:
                exp = 0.0 if entry is None else expected_cost(entry, in_tok, out)
                worst = 0.0 if entry is None else worst_case_cost(entry, in_tok, max_out, band=estimated)
            except PriceGateError as e:
                refused.append({"name": p.name, "reason": str(e)})
                continue
            if worst > self.config.call_cap_usd:
                warnings.append(f"{p.name}: worst case per call ${worst:.2f} exceeds call cap "
                                f"${self.config.call_cap_usd:.2f}; those calls will be refused")
            if spec.reasoning_control.get("bounded") == "unverified":
                msg = f"{p.name}: reasoning bound unverified for this adapter (pilot verifies)"
                if msg not in warnings:
                    warnings.append(msg)
            m = per_model.setdefault(p.name, {"calls": 0, "expected_usd": 0.0, "worst_case_usd": 0.0,
                                              "in_tok_estimated": estimated, "class": spec.klass})
            m["calls"] += p.count
            m["expected_usd"] = _r6(m["expected_usd"] + exp * p.count)
            m["worst_case_usd"] = _r6(m["worst_case_usd"] + worst * p.count)
            total_exp += exp * p.count
            total_worst += worst * p.count
        remaining = self.remaining_usd(round_budget_id)
        estimate = {
            "per_model": per_model,
            "expected_usd": _r6(total_exp),
            "worst_case_usd": _r6(total_worst),
            "remaining_usd": _r6(remaining),
            "refused": refused,
            "warnings": warnings,
        }
        if total_worst > remaining + 1e-9:
            warnings.append(f"whole-round worst case ${total_worst:.2f} exceeds remaining ${remaining:.2f}")
            if not confirm_worst_case:
                raise WorstCaseNotConfirmed(estimate)
        return estimate
