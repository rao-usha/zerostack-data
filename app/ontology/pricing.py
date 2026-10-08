"""Model prices, the price gate and cost maths for the ontology bake-off (SPEC_164).

``pricing.json`` holds one entry per hosted registry name. PLAN_100 §4 makes a
verified, dated price a hard prerequisite of running a model, so
``check_price_gate`` refuses (``PriceGateError``) a hosted model whose entry is
missing, unverified, older than ``ONTOLOGY_PRICE_MAX_AGE_DAYS``, past its
``promo_end``, or premium without ``allow_premium``. Unpriced models are never
recorded as $0. Local models (class ``open_local``) cost $0 and need no entry.
"""
from __future__ import annotations

import json
import math
import os
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Tuple

PRICING_PATH = Path(__file__).with_name("pricing.json")
DEFAULT_PRICE_MAX_AGE_DAYS = 1
DEFAULT_PREMIUM_OUTPUT_PER_M = 40.0
TOKENIZER_BAND = 0.30  # PLAN_100 §4.1/§9.1: uniform ±30% until counts are measured


class PriceGateError(RuntimeError):
    """The model may not run on its current price entry."""


class UnpricedTier(PriceGateError):
    """The prompt crosses a long-context threshold whose price is unknown."""


@dataclass(frozen=True)
class PriceEntry:
    name: str
    input_per_m: float
    output_per_m: float
    cached_input_per_m: Optional[float] = None
    reasoning_per_m: Optional[float] = None
    long_context: List[Dict[str, Any]] = field(default_factory=list)
    promo_end: Optional[str] = None
    premium_flag: Optional[bool] = None
    as_of: Optional[str] = None
    source_url: Optional[str] = None
    verified: bool = False
    notes: Optional[str] = None
    premium_threshold: float = DEFAULT_PREMIUM_OUTPUT_PER_M

    @classmethod
    def from_dict(cls, name: str, d: Mapping[str, Any],
                  premium_threshold: float = DEFAULT_PREMIUM_OUTPUT_PER_M) -> "PriceEntry":
        tiers = sorted((dict(t) for t in d.get("long_context") or []), key=lambda t: t["above_input_tokens"])
        return cls(
            name=name,
            input_per_m=float(d["input_per_m"]),
            output_per_m=float(d["output_per_m"]),
            cached_input_per_m=_opt_float(d.get("cached_input_per_m")),
            reasoning_per_m=_opt_float(d.get("reasoning_per_m")),
            long_context=tiers,
            promo_end=d.get("promo_end"),
            premium_flag=d.get("premium"),
            as_of=d.get("as_of"),
            source_url=d.get("source_url"),
            verified=bool(d.get("verified", False)),
            notes=d.get("notes"),
            premium_threshold=premium_threshold,
        )

    @property
    def premium(self) -> bool:
        """Vendor-neutral rule (§9.4): output price >= threshold, unless set explicitly."""
        if self.premium_flag is not None:
            return bool(self.premium_flag)
        return self.output_per_m >= self.premium_threshold


def _opt_float(v: Any) -> Optional[float]:
    return None if v is None else float(v)


def load_pricing(path: Optional[os.PathLike] = None,
                 premium_threshold: Optional[float] = None) -> Dict[str, PriceEntry]:
    if premium_threshold is None:
        premium_threshold = float(os.environ.get("ONTOLOGY_PREMIUM_OUTPUT_PER_M", DEFAULT_PREMIUM_OUTPUT_PER_M))
    raw = json.loads(Path(path or PRICING_PATH).read_text(encoding="utf-8"))
    models = raw.get("models", raw)
    return {
        name: PriceEntry.from_dict(name, d, premium_threshold)
        for name, d in models.items()
        if not name.startswith("_")
    }


def _parse_date(s: Optional[str]) -> Optional[date]:
    return date.fromisoformat(s) if s else None


def check_price_gate(
    spec: Any,
    pricing: Mapping[str, PriceEntry],
    today: Optional[date] = None,
    max_age_days: int = DEFAULT_PRICE_MAX_AGE_DAYS,
    allow_premium: bool = False,
) -> Optional[PriceEntry]:
    """Return the entry a hosted model may run on, None for a local model, else raise."""
    if getattr(spec, "is_local", False):
        return None
    today = today or date.today()
    name = spec.name
    entry = pricing.get(name)
    if entry is None:
        raise PriceGateError(f"{name}: no price entry in pricing.json; unpriced models are refused")
    if not entry.verified:
        raise PriceGateError(f"{name}: price entry is unverified; verify it at first party and set verified=true")
    if not entry.source_url:
        raise PriceGateError(f"{name}: price entry has no source_url")
    as_of = _parse_date(entry.as_of)
    if as_of is None:
        raise PriceGateError(f"{name}: price entry has no as_of date")
    age = (today - as_of).days
    if age > max_age_days:
        raise PriceGateError(
            f"{name}: price as_of {entry.as_of} is {age} days older than allowed ({max_age_days}); re-enter it"
        )
    promo = _parse_date(entry.promo_end)
    if promo is not None and today > promo:
        raise PriceGateError(f"{name}: promo_end {entry.promo_end} has passed; re-price before running")
    if entry.premium and not allow_premium:
        raise PriceGateError(
            f"{name}: premium tier (output ${entry.output_per_m}/M >= ${entry.premium_threshold}/M) "
            "needs allow_premium=True"
        )
    return entry


def price_status(spec: Any, pricing: Mapping[str, PriceEntry], today: Optional[date] = None,
                 max_age_days: Optional[int] = None) -> str:
    """Short label for the --check listing: local, missing, unverified, stale, promo_ended, ok."""
    if getattr(spec, "is_local", False):
        return "local"
    entry = pricing.get(spec.name)
    if entry is None:
        return "missing"
    if not entry.verified:
        return "unverified"
    today = today or date.today()
    if max_age_days is None:
        max_age_days = int(os.environ.get("ONTOLOGY_PRICE_MAX_AGE_DAYS", DEFAULT_PRICE_MAX_AGE_DAYS))
    as_of = _parse_date(entry.as_of)
    if as_of is None or (today - as_of).days > max_age_days:
        return "stale"
    promo = _parse_date(entry.promo_end)
    if promo is not None and today > promo:
        return "promo_ended"
    return "ok"


def tier_prices(entry: PriceEntry, in_tok: int) -> Tuple[float, float, float, float]:
    """(p_in, p_out, p_cached, p_reasoning) per 1M tokens for a prompt of ``in_tok``.

    The highest long-context tier whose ``above_input_tokens`` the prompt
    exceeds applies to the whole call. A tier with null prices raises
    ``UnpricedTier``.
    """
    p_in, p_out = entry.input_per_m, entry.output_per_m
    p_cached, p_reason = entry.cached_input_per_m, entry.reasoning_per_m
    for tier in entry.long_context:
        if in_tok > int(tier["above_input_tokens"]):
            if tier.get("input_per_m") is None or tier.get("output_per_m") is None:
                raise UnpricedTier(
                    f"{entry.name}: prompt of {in_tok} tokens crosses {tier['above_input_tokens']} and that "
                    "tier is unpriced"
                )
            p_in, p_out = float(tier["input_per_m"]), float(tier["output_per_m"])
            p_cached = _opt_float(tier.get("cached_input_per_m"))
            p_reason = _opt_float(tier.get("reasoning_per_m"))
    return p_in, p_out, (p_in if p_cached is None else p_cached), (p_out if p_reason is None else p_reason)


def banded(in_tok: int, band: bool) -> int:
    return int(math.ceil(in_tok * (1 + TOKENIZER_BAND))) if band else int(in_tok)


def worst_case_cost(entry: PriceEntry, in_tok: int, max_out: int, band: bool = False) -> float:
    """``in_tok × p_in + max_out × max(p_out, p_reasoning)`` at the tier the prompt selects.

    ``band=True`` (token count from the fallback estimator) inflates ``in_tok``
    by 30% first, which may also push the prompt into a higher tier.
    """
    n_in = banded(in_tok, band)
    p_in, p_out, _, p_reason = tier_prices(entry, n_in)
    return (n_in * p_in + int(max_out) * max(p_out, p_reason)) / 1_000_000


def expected_cost(entry: PriceEntry, in_tok: int, out_tok: int) -> float:
    p_in, p_out, _, _ = tier_prices(entry, in_tok)
    return (in_tok * p_in + out_tok * p_out) / 1_000_000


def actual_cost(entry: PriceEntry, usage: Any, reasoning_in_output: bool = True) -> float:
    """Cost of a finished call from its normalised usage.

    ``usage.input_tokens`` includes cached tokens; ``usage.output_tokens``
    includes reasoning tokens when ``reasoning_in_output`` (OpenAI-style
    ``completion_tokens``), otherwise reasoning is billed on top.
    """
    n_in = int(usage.input_tokens or 0)
    cached = min(int(usage.cached_input_tokens or 0), n_in)
    reasoning = int(usage.reasoning_tokens or 0)
    out = int(usage.output_tokens or 0)
    visible = max(out - reasoning, 0) if reasoning_in_output else out
    p_in, p_out, p_cached, p_reason = tier_prices(entry, n_in)
    return ((n_in - cached) * p_in + cached * p_cached + visible * p_out + reasoning * p_reason) / 1_000_000
