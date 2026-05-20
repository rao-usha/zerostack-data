"""
Atlas telemetry + persistence — SPEC_064.

Owns the five `atlas_*` tables (idempotent migration), exploration
save/load, and the event/feedback recording that will train future card
ranking. V1 only *records* — no ranking logic reads this yet.

All writes are best-effort from the caller's perspective: telemetry failure
must never break an exploration. The API layer wraps these in try/except.
"""

from __future__ import annotations

import json
import logging
import secrets
import string
from typing import Any, Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

# Telemetry event types — see SPEC_064 §Telemetry. Used for validation.
EVENT_TYPES = {
    "query_submitted", "entity_resolved", "card_viewed", "card_expanded",
    "connection_clicked", "related_query_clicked", "source_opened",
    "share_created", "export_clicked", "thumbs_up", "thumbs_down",
    "followup_question",
}

_SHARE_ALPHABET = string.ascii_lowercase + string.digits


class AtlasTelemetry:
    """Persistence + telemetry for Atlas. Instantiate per-request with a db Session."""

    def __init__(self, db: Session):
        self.db = db
        self._ensure_tables()

    # ── Schema ───────────────────────────────────────────────────────────────

    def _ensure_tables(self) -> None:
        ddl = """
        CREATE TABLE IF NOT EXISTS atlas_explorations (
            id SERIAL PRIMARY KEY,
            slug TEXT UNIQUE NOT NULL,
            query TEXT NOT NULL,
            resolved_msa_code TEXT,
            resolved_msa_title TEXT,
            resolved_naics_code TEXT,
            resolved_naics_label TEXT,
            summary JSONB NOT NULL DEFAULT '{}',
            cards JSONB NOT NULL DEFAULT '[]',
            connections JSONB NOT NULL DEFAULT '[]',
            related_queries JSONB NOT NULL DEFAULT '[]',
            coverage_notes JSONB NOT NULL DEFAULT '[]',
            is_public BOOLEAN NOT NULL DEFAULT TRUE,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS atlas_queries (
            id SERIAL PRIMARY KEY,
            exploration_id INTEGER REFERENCES atlas_explorations(id),
            query TEXT NOT NULL,
            session_id TEXT,
            anon_ip TEXT,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS atlas_events (
            id SERIAL PRIMARY KEY,
            exploration_id INTEGER REFERENCES atlas_explorations(id),
            session_id TEXT,
            event_type TEXT NOT NULL,
            card_id TEXT,
            payload JSONB NOT NULL DEFAULT '{}',
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS atlas_card_feedback (
            id SERIAL PRIMARY KEY,
            exploration_id INTEGER REFERENCES atlas_explorations(id),
            card_id TEXT NOT NULL,
            session_id TEXT,
            feedback TEXT NOT NULL,
            comment TEXT,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS atlas_shared_links (
            id SERIAL PRIMARY KEY,
            exploration_id INTEGER REFERENCES atlas_explorations(id),
            share_code TEXT UNIQUE NOT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            view_count INTEGER NOT NULL DEFAULT 0
        );
        CREATE INDEX IF NOT EXISTS atlas_events_exploration_idx ON atlas_events(exploration_id);
        CREATE INDEX IF NOT EXISTS atlas_events_type_idx ON atlas_events(event_type);
        CREATE INDEX IF NOT EXISTS atlas_explorations_slug_idx ON atlas_explorations(slug);
        """
        for stmt in [s for s in ddl.strip().split(";") if s.strip()]:
            self.db.execute(text(stmt + ";"))
        self.db.commit()

    # ── Exploration save / load ──────────────────────────────────────────────

    def save_exploration(self, exploration_dict: Dict[str, Any]) -> int:
        """Upsert an exploration by slug. Returns the row id.

        Slugs are deterministic (resolver-derived), so re-running the same
        query refreshes the stored exploration rather than duplicating it.
        """
        re = exploration_dict.get("resolved_entities") or {}
        msa = re.get("msa") or {}
        naics = re.get("naics") or {}
        coverage_notes = (exploration_dict.get("summary") or {}).get("coverage_notes") or []

        row = self.db.execute(text("""
            INSERT INTO atlas_explorations (
                slug, query, resolved_msa_code, resolved_msa_title,
                resolved_naics_code, resolved_naics_label,
                summary, cards, connections, related_queries, coverage_notes
            ) VALUES (
                :slug, :query, :msa_code, :msa_title,
                :naics_code, :naics_label,
                CAST(:summary AS JSONB), CAST(:cards AS JSONB),
                CAST(:connections AS JSONB), CAST(:related AS JSONB),
                CAST(:coverage AS JSONB)
            )
            ON CONFLICT (slug) DO UPDATE SET
                query = EXCLUDED.query,
                resolved_msa_code = EXCLUDED.resolved_msa_code,
                resolved_msa_title = EXCLUDED.resolved_msa_title,
                resolved_naics_code = EXCLUDED.resolved_naics_code,
                resolved_naics_label = EXCLUDED.resolved_naics_label,
                summary = EXCLUDED.summary,
                cards = EXCLUDED.cards,
                connections = EXCLUDED.connections,
                related_queries = EXCLUDED.related_queries,
                coverage_notes = EXCLUDED.coverage_notes,
                updated_at = CURRENT_TIMESTAMP
            RETURNING id
        """), {
            "slug": exploration_dict["slug"],
            "query": exploration_dict["query"],
            "msa_code": msa.get("code"),
            "msa_title": msa.get("title"),
            "naics_code": naics.get("code"),
            "naics_label": naics.get("label"),
            "summary": json.dumps(exploration_dict.get("summary") or {}),
            "cards": json.dumps(exploration_dict.get("cards") or []),
            "connections": json.dumps(exploration_dict.get("connections") or []),
            "related": json.dumps(exploration_dict.get("related_queries") or []),
            "coverage": json.dumps(coverage_notes),
        }).fetchone()
        self.db.commit()
        return int(row[0])

    def get_exploration(self, slug_or_id: str) -> Optional[Dict[str, Any]]:
        """Load a stored exploration by slug or numeric id."""
        if str(slug_or_id).isdigit():
            where, param = "id = :v", int(slug_or_id)
        else:
            where, param = "slug = :v", str(slug_or_id)
        row = self.db.execute(
            text(f"SELECT * FROM atlas_explorations WHERE {where}"),
            {"v": param},
        ).mappings().first()
        if not row:
            return None
        d = dict(row)
        return {
            "id": d["id"],
            "slug": d["slug"],
            "query": d["query"],
            "resolved_entities": {
                "msa": ({"code": d["resolved_msa_code"], "title": d["resolved_msa_title"]}
                        if d["resolved_msa_code"] else None),
                "naics": ({"code": d["resolved_naics_code"], "label": d["resolved_naics_label"]}
                          if d["resolved_naics_code"] else None),
                "geographies": [],
                "datasets": [],
            },
            "summary": d["summary"],
            "cards": d["cards"],
            "connections": d["connections"],
            "related_queries": d["related_queries"],
            "share_url": f"/atlas/{d['slug']}",
        }

    # ── Telemetry recording ──────────────────────────────────────────────────

    def record_query(self, query: str, exploration_id: Optional[int],
                      session_id: Optional[str], anon_ip: Optional[str]) -> None:
        self.db.execute(text("""
            INSERT INTO atlas_queries (exploration_id, query, session_id, anon_ip)
            VALUES (:eid, :q, :sid, :ip)
        """), {"eid": exploration_id, "q": query, "sid": session_id, "ip": anon_ip})
        self.db.commit()

    def record_event(self, event_type: str, exploration_id: Optional[int] = None,
                     session_id: Optional[str] = None, card_id: Optional[str] = None,
                     payload: Optional[Dict[str, Any]] = None) -> int:
        """Record one telemetry event. Unknown event_type is allowed but
        logged — we don't want telemetry to reject a client's new event."""
        if event_type not in EVENT_TYPES:
            logger.info("Atlas: non-standard event_type %r recorded", event_type)
        row = self.db.execute(text("""
            INSERT INTO atlas_events (exploration_id, session_id, event_type, card_id, payload)
            VALUES (:eid, :sid, :etype, :cid, CAST(:payload AS JSONB))
            RETURNING id
        """), {
            "eid": exploration_id, "sid": session_id, "etype": event_type,
            "cid": card_id, "payload": json.dumps(payload or {}),
        }).fetchone()
        self.db.commit()
        return int(row[0])

    def record_feedback(self, card_id: str, feedback: str,
                        exploration_id: Optional[int] = None,
                        session_id: Optional[str] = None,
                        comment: Optional[str] = None) -> int:
        """Record a thumbs up/down (+ optional comment) on a card."""
        row = self.db.execute(text("""
            INSERT INTO atlas_card_feedback (exploration_id, card_id, session_id, feedback, comment)
            VALUES (:eid, :cid, :sid, :fb, :comment)
            RETURNING id
        """), {
            "eid": exploration_id, "cid": card_id, "sid": session_id,
            "fb": feedback, "comment": comment,
        }).fetchone()
        self.db.commit()
        return int(row[0])

    # ── Share links ──────────────────────────────────────────────────────────

    def create_share_link(self, exploration_id: int) -> str:
        """Mint an unguessable share code for an exploration."""
        code = "".join(secrets.choice(_SHARE_ALPHABET) for _ in range(10))
        self.db.execute(text("""
            INSERT INTO atlas_shared_links (exploration_id, share_code)
            VALUES (:eid, :code)
        """), {"eid": exploration_id, "code": code})
        self.db.commit()
        return code
