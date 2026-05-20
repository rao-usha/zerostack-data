"""
Atlas card-connection builder — SPEC_064.

Given the cards that actually rendered for an exploration, emit the
relationship edges between them. Deterministic rule table for v1 — no
learned weights yet (telemetry will inform a future ranking pass).

A connection is only emitted when BOTH endpoint cards are present in the
exploration (T6: every connection references existing cards).
"""

from __future__ import annotations

from typing import List

from app.services.atlas.types import Card, Connection


# (source_id, target_id, relationship, strength). Order within a pair is
# stable so output is deterministic.
_CONNECTION_RULES = [
    ("industry_footprint", "named_operators",
     "Operators within this industry footprint", 0.85),
    ("industry_footprint", "local_wealth_demand",
     "Demand pool the industry sells into", 0.70),
    ("industry_footprint", "macro_rates",
     "Rate + production cycle the sector underwrites against", 0.55),
    ("industry_footprint", "logistics_trade",
     "Trade exposure of the industry's goods", 0.50),
    ("industry_footprint", "risk_context",
     "Hazard exposure of the operating base", 0.55),
    ("risk_context", "infrastructure",
     "Natural-hazard exposure of physical infrastructure", 0.65),
    ("connectivity", "infrastructure",
     "Connectivity is part of the infrastructure picture", 0.60),
    ("local_wealth_demand", "macro_rates",
     "Macro backdrop for local demand", 0.45),
    ("logistics_trade", "infrastructure",
     "Trade flows depend on rail / air / port infrastructure", 0.55),
    ("named_operators", "risk_context",
     "Risk the named operators are exposed to", 0.40),
]


def build_connections(cards: List[Card]) -> List[Connection]:
    """Emit relationship edges between cards that are both present."""
    present = {c.id for c in cards}
    out: List[Connection] = []
    for src, tgt, rel, strength in _CONNECTION_RULES:
        if src in present and tgt in present:
            out.append(Connection(
                source_card=src,
                target_card=tgt,
                relationship=rel,
                strength=strength,
            ))
    return out
