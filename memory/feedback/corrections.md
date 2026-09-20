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

## 2026-05-20 — Report-first monetization was the wrong commercial shape

**Context:** PLAN_065 built a paid public-data report product end-to-end —
`market_intelligence_pack` report template (SPEC_061), `/diligence-pack/*`
orders API + Stripe routing (SPEC_062), and a `$2,500 / $7,500` intake
landing page (SPEC_063). The engineering was sound and shipped clean (104
tests passing). But the commercial framing — "pay for an AI-generated
report" as the first user experience — was rejected. Users increasingly
expect generated reports to be free; a static AI report competes with
frontier LLMs on prose, which is a losing wedge.

**Correction:** Pivot to **Nexdata Atlas** (SPEC_064) — an interactive
public-data exploration product. The report/orders/intake engineering is
all KEPT but DEMOTED: report → export/deep-dive renderer, orders → concierge
fallback, intake page → "request custom work after exploring". The
defensible moat is the governed cross-dataset joins + entity resolution +
provenance + usage telemetry, not the prose.

**Rubric additions:**
- For any plan whose deliverable is "a paid artifact", explicitly answer two
  *separate* questions before building the funnel: (a) "is this artifact
  good?" and (b) "should this artifact be *sold*, or *used*?" A good
  artifact can be a free proof / export / deep-dive without being the thing
  the funnel sells. PLAN_065 revised the artifact twice but never
  questioned (b).
- When the core technology cost (here: LLM prose generation) is collapsing
  toward zero, do not monetize that technology directly. Monetize what
  isn't collapsing — governed data, provenance, the compounding telemetry.
- Build the telemetry/event spine as a v1 feature, not a later addition. A
  product that can't observe which outputs users value can't improve
  ranking, ingestion priorities, or monetization.
- Engineering sunk cost ≠ commercial sunk cost. When a commercial framing is
  wrong, keep 100% of the working engineering and discard 100% of the
  framing — they are separable. Don't defend a funnel just because code
  exists for it.

---

## 2026-09-20 — SPEC_118 filing-platform rule (self-caught by adversarial review)

**Correction:** A new heuristic was validated only by the ranked table it
produced. `find_platform_advisers` counted *CRDs sharing a name stem*, not
*distinct advisers named*, so its docstring described a different quantity
than its code. Separately, the rule re-pointed 320 funds at advisers without
checking they resolve to a `pe_firms` row — 234 of them ended with no link,
counted as successes.

**Why it matters:** Both were found by an adversarial review and confirmed by
direct measurement, before anything was written. The ranked table looked right
because it *was* right for the four advisers it listed; it said nothing about
the mechanism.

**How to apply next time:**
- When a new metric decides which rows get re-pointed, write a test that
  pins the metric's *definition* (what one input contributes), not only its
  output on the happy case.
- Run the validation script through the same code path the mart uses. The
  "cliff" table here was built from a different index over a different table
  and ranked a different population than the shipped function.
- Any rule that overrides an existing link must check the replacement is
  writable at the point it decides, and count the case where it is not.
