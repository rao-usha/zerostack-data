"""
Nexdata Atlas — interactive public-data exploration engine. SPEC_064 / PLAN_065 rev_01.

Atlas is the primary product surface: a user queries a market / sector /
geography and gets back resolved entities, insight cards, cross-dataset
connections, a provenance trail, related queries, and a shareable URL —
plus telemetry that teaches the platform which insights matter.

This replaces the report-first monetization of PLAN_065 rev_00. The
`market_intelligence_pack` report, `/diligence-pack/*` orders API, and the
Synthetic Playground all remain functional but move downstream of Atlas
(export / concierge-fallback / demo respectively).

Public entry point: `AtlasService.explore(...)`.
"""

from app.services.atlas.service import AtlasService

__all__ = ["AtlasService"]
