# Corrections Log

Append-only. One entry per correction / lesson. Fed back into rubrics.

---

## 2026-04-15 — Migration failures must not be logged at DEBUG

**Context:** PLAN_053/056/057 added `ingestion_jobs.data_origin`. The SQLAlchemy model knew about it; Cloud SQL did not. The `ALTER TABLE` in `_apply_schema_migrations` failed every startup with `InsufficientPrivilege` (table owned by `postgres`, app connects as `nexdata`), but the error was caught at `logger.debug` so nobody saw it. Result: every `/api/v1/jobs` read for a synthetic source 500ed for hours.

**Correction:** Schema-migration failures at startup are *always* significant — they mean model ↔ DB drift. They must be logged at `ERROR` level. Changed in `app/core/database.py:94`.

**Rubric additions:**
- When adding a `_apply_schema_migrations` entry, also verify the `nexdata` role owns (or has ALTER on) the target table. If the table was created long ago by `postgres`, a one-time `ALTER TABLE <t> OWNER TO nexdata` is required before the auto-migration can take effect.
- Never use `logger.debug` for "this might fail and that's fine" patterns at startup. If a failure is benign, say so explicitly in the message and still log at WARN or higher.

---
