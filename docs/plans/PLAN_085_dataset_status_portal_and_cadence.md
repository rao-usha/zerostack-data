# PLAN 085 — Dataset status portal and collection cadence

**Status:** Draft, for review
**Date:** 2026-09-22
**Depends on:** SPEC_120 (schedules), PLAN_082–084 (the SEC pipeline)
**Research:** four parallel investigations — frontend, API surface, dataset
inventory, cadence vs upstream. All numbers below are measured, not estimated.

---

## 1. The finding that reframes this request

You asked for a portal showing the status of every dataset. Building it
surfaced the reason it is needed:

> **One of roughly 81 source families is still running.** The entire API
> ingestor fleet has been dead since **2026-04-16** — 159 days.

Successful `ingestion_jobs` by month:

| month | jobs | successes |
|---|---|---|
| 2026-01 | 439 | 388 |
| 2026-02 | 563 | 391 |
| 2026-03 | 755 | 135 |
| 2026-04 | 46 | **2** |
| 2026-05 → 09 | 1,795 | **0** |

2,000 of 2,705 failures carry one identical error:
`"zombie: no worker since 2026-04-16 (PLAN_082)"`. Of those, 1,794 are since
2026-05-01 — **100% of every failure in that window**. The batch scheduler
kept enqueueing into a queue nothing was draining until it too stopped on
2026-08-04.

What is actually alive: the 8 SEC bulk loaders, `entity_resolve`, and
`pe_mart_build`. That is it.

**So the portal's first and most valuable output is not a freshness number.
It is the sentence "51 sources have not succeeded in 159 days."** Nobody
could see that, which is why it went unnoticed for five months.

---

## 2. What already exists

Not greenfield. Five overlapping status surfaces already ship:

| Surface | Gives | Grain |
|---|---|---|
| `GET /sources/health-summary` | status, staleness, SLA, row trend, next run, quality, anomalies | source |
| `GET /datasets/freshness` | `last_success_at`, `age_hours`, `expected_cadence_hours`, `is_stale` | source |
| `GET /source-health` | scored tiers (freshness/reliability/coverage/consistency) | source |
| `GET /jobs/monitoring/dashboard` | 24h+1h metrics, per-source health, alerts | source |
| `GET /bulk/releases` | per-release `rows_loaded`, `bytes`, status | **release** ✅ |

`/sources/health-summary` is ~80% of a source-grain dashboard and is already
batched correctly, with a comment recording why: *51 sources × 4 helpers = 204
queries ≈ 17s observed*.

### What is wrong with it

1. **Everything is keyed by source, never dataset.** The discriminator lives
   in `job.config["dataset"]` and is never projected into a column, indexed or
   grouped on. `eia` fans out to five datasets, `fdic` to four. **"One row per
   dataset" is not derivable from job history today.**
2. **Row counts are two different wrong things.** `pg_stat_user_tables.n_live_tup`
   (reads 0 until ANALYZE, prefix-matched), falling back to the last job's
   `rows_inserted` — *rows written by that run*, a different quantity.
   Measured proof it lies: `fbi_crime` has **18 recorded successes and 0 rows**;
   `sec_adv_schedule_d` reports `rows_loaded` of 0 against **561,175 actual**.
3. **`status == "running"` is computed over unbounded history**, so a job that
   died without a status reset pins its source to "running" forever.
4. **`/datasets/freshness` silently omits never-run sources** — the ones most
   worth seeing.
5. **DB `next_run_at` and live APScheduler `next_run` diverge**, and nothing
   reconciles them. Observed: the column read 19 Sep while the live scheduler
   correctly held 23 Sep. It is only written *after* a schedule first runs.
6. **A whole panel in `index.html` is dead.** It reads `avg_coverage_score`,
   `coverage_depth`, `total_records` — zero hits anywhere in the backend. It
   falls back to a local log-scale guess while **discarding the real
   `is_stale` / `age_hours` / `expected_cadence_hours` it does receive.**

### The frontend

27 hand-written static HTML files on an nginx bind mount. **No package.json,
no build step, no framework.** A new page is one file, live on save at
`localhost:3001/status.html`. `frontend/node_modules/` is uncommitted,
abandoned SPA wreckage — ignore it. There is a public no-auth SSE feed at
`/api/v1/job-queue/stream` already driving live job updates, and a reusable
dark palette (`.card`, `.stat-card`, `.job-status-badge`, `.ds-table`,
`.health-bar-*`) to copy verbatim.

---

## 3. The core idea: three clocks

A dataset has three timestamps, and every current surface conflates the first
with the others:

| Clock | Question | Where it lives |
|---|---|---|
| **Run** | when did we last collect? | `job_queue` / `ingestion_jobs` |
| **Publish** | when did upstream last release? | `raw.source_release` (bulk only) |
| **Coverage** | what period does the data cover? | **nowhere** |

A job that succeeded an hour ago on a source whose newest data is Q2 is green
everywhere and three months stale. Conversely a source last run in July is
*perfectly current* if upstream published nothing since — flagging it red
trains people to ignore the dashboard.

**The portal's primary metric is coverage lag, not run recency.**

```
lag = expected_through - coverage_through
```

### Status taxonomy

| Status | Meaning |
|---|---|
| `current` | coverage meets expectation |
| `awaiting_upstream` | we hold everything published; next release not due — **not a warning** |
| `behind` | upstream published; we have not loaded it |
| `stalled` | schedule should have fired and did not |
| `failing` | recent runs errored |
| `blocked` | missing API key, dead dependency, no worker |
| `dormant` | no schedule |
| `never_run` | configured, never produced data |

`awaiting_upstream` and `stalled` do not exist today and are the two that
matter. On current data, `blocked` would immediately explain most of the
fleet: `NOAA_API_TOKEN`, `SAM_GOV_API_KEY`, `USPTO_PATENTSVIEW_API_KEY`,
`SIMILARWEB`, `OPENCORPORATES` and the foot-traffic keys are **absent from
`.env`**, which maps 1:1 onto those sources sitting at zero successes forever.

---

## 4. Part A — closing the grain gap

**A "dataset" has no single definition here.** Four incompatible concepts
coexist: a `dataset_registry` row (table-level), a `SOURCE_DISPATCH` key
(104 of them, `source:sub-dataset`), a `BulkSource` (file-release level), and
a site-intel `COLLECTOR_REGISTRY` entry (domain level). One must be chosen and
the others mapped onto it.

**`dataset_registry` cannot be the spine.** Measured: 116 rows, `max(last_updated_at)`
= **2026-04-16, 159 days stale**. It covers 114 tables against **352 it misses** —
**66% of all rows and 68% of all bytes are absent**, including every large table
(`sec_13f_holdings`, `sec_filers`, all `form_d_*`). The bulk loaders register
nothing at all (0 hits for `DatasetRegistry` under `app/ingest/bulk/`). And
`_update_dataset_registry` is called from `prepare_table()` — *before* the
fetch — so its timestamp means "an attempt started", not "data landed".

**Recommendation: a declared `DatasetSpec` catalogue in code**, with
`dataset_registry` left as the runtime mirror it already is, plus
`dataset_key` projected onto `ingestion_jobs` so history becomes groupable.

A catalogue derived from history can only show what has already run —
precisely blind to the 51 dormant sources most worth seeing. It is also the
only home for the two facts nothing records: **what kind of data this is**,
and **how to compute its coverage**.

```python
@dataclass(frozen=True)
class DatasetSpec:
    key: str                  # "sec_adv_schedule_d", "eia:electricity_retail"
    source: str
    display_name: str
    kind: DatasetKind         # reference | filings | timeseries | holdings | derived_mart
    grain: str                # "one row per filing per fund"
    tables: tuple[str, ...]   # BulkSource has NO tables attribute today
    cadence: CadenceProfile
    coverage_sql: str | None  # "SELECT max(filed_at) FROM sec_adv_filings"
    rerun: RerunPolicy        # idempotent | append_only | destructive | currency_only
```

`kind` and `grain` answer *"what kind of data"*. `rerun` answers *"can it be
run again"* — and the vocabulary matters. The research corrected my
assumption here: `bulk:sec_iapd_feed`'s schedule says *"a missed day cannot be
recovered"*, but its `load()` runs
`DELETE FROM sec_adv_feed_firm_state WHERE edition_date < :edition` — it keeps
only the newest edition anyway. **A missed day costs currency, not history.**
That is `currency_only`, not `unrecoverable`.

---

## 5. Part B — one status endpoint

`GET /api/v1/datasets/status`. Dataset grain, one batched call.

```jsonc
{
  "summary": {"current": 9, "behind": 2, "stalled": 3, "failing": 0,
              "dormant": 20, "never_run": 20, "blocked": 11},
  "datasets": [{
    "key": "sec_adv_schedule_d",
    "display_name": "Form ADV Schedule D — private funds",
    "kind": "filings",
    "grain": "one row per filing per private fund",
    "status": "current",
    "tables": [{"name": "sec_adv_private_fund_filings",
                "rows": 561175, "rows_exact": true, "bytes": 216006656}],
    "clocks": {
      "last_run_at": "2026-09-20T23:01:34Z", "last_run_status": "success",
      "last_run_duration_s": 141,
      "last_publish_at": "2026-09-01T15:31:43Z",
      "coverage_through": "2026-08-31", "expected_through": "2026-08-31",
      "lag_days": 0
    },
    "schedule": {"cron": "0 8 4 * *", "next_run_at": "2026-10-04T08:00:00Z",
                 "next_run_source": "apscheduler", "missed_runs_30d": 0},
    "can_run": {"allowed": true, "policy": "idempotent", "blockers": [],
                "warnings": ["one manifest GET; downloads only if a new release exists"],
                "estimated_duration_s": 141}
  }]
}
```

Constraints:

- **One call renders the page.** Reuse the `*_all(db)` batched helpers
  (`sources.py:379-477`). Explicitly do **not** reuse `get_all_source_health()`
  — it loops sources at 3 queries each, ~150 for 51 sources.
- **Row counts from `count(*)` on declared target tables**, never from
  `rows_inserted` or `rows_loaded`. `rows_exact: false` when estimated.
- **"Last run" is a UNION of three stores**, because there is no single
  trustworthy one (§8.1): `job_queue` ∪ `raw.source_release` ∪
  `ingestion_jobs WHERE status='success'`.
- **`next_run_source`** records whether the time came from live APScheduler or
  the DB column, making the divergence visible rather than silent.
- **`can_run` is a verdict with reasons.** Composed from
  `_check_api_key_preflight()` (today an internal function with no endpoint),
  `has_live_worker()` (today only a 409 *after* you try), currently-running,
  and dependency freshness.
- **Filter `job_queue.status` case-insensitively** — one row is `'FAILED'`
  against 2,379 `'failed'`.

Plus `POST /api/v1/datasets/{key}/run`, returning the same verdict on refusal
instead of a bare 409.

---

## 6. Part C — the portal page

`frontend/status.html`. Standalone, no build step, live on save.

Not a tab in `index.html`: that file is 1 MB / 16k lines, untouched since
April, and its tab highlighting is positional
(`tabs[TAB_ROUTES.indexOf(tab)]`), so button order and array order must stay
in lockstep. High friction, no benefit.

1. **Banner** — counts by status. The headline number is *dormant + never_run*,
   because that is the finding.
2. **Lag strip** — every dataset as a bar of coverage lag against its cadence.
3. **Table**, one row per dataset, grouped by kind:
   `dataset · kind · coverage through · lag · last run · next run · rows · [Run]`.
   Run is disabled with the blocker as tooltip when `can_run.allowed` is false.
4. **Row expansion** — recent runs, tables with counts, cadence profile,
   release history from `/bulk/releases`.
5. **Live** — subscribe to the existing `/job-queue/stream` SSE. No polling.

Also in scope: **delete or repair the dead panel** in `index.html`. Leaving
invented numbers on screen beside a new honest dashboard is worse than either
alone.

---

## 7. Part D — cadence design

### D1. The two loader classes, and why it matters

`run_source` skips any release already `loaded` without touching the network.
So the cost of a no-op run is exactly the cost of `discover()` — **unless
`discover()` mints a new key every time.** That splits the eight sources:

**Class A — manifest-keyed. A no-op run is one HTTP GET. Over-polling is free.**
`sec_13f`, `sec_form_d`, `sec_insider`, `sec_adv_roster`, `sec_adv_schedule_d`,
`sec_iapd_feed`.

**Class B — date-keyed snapshots. Every run is a full download, unconditionally.**
`sec_edgar_submissions` and `sec_companyfacts` both do
`return [Release(f"snapshot:{today.isoformat()}", ...)]`. A fresh key each UTC
day is never `loaded`, so it always re-fetches. `SecHttp.stream_to_file`
*captures* `ETag`/`Last-Modified` into the result and **never sends
`If-None-Match` or `If-Modified-Since`**. The `etag` column is written and
never read.

### D2. The cost nobody budgeted — and a correction

| source | download | duration | rows actually changed per day |
|---|---|---|---|
| `sec_edgar_submissions` | **1.564 GB** | 473 s | **8,886** (0.41% of `sec_filers`) |
| `sec_companyfacts` | **1.409 GB** | 369 s | **1,053** (0.52% of statement rows) |

There is **no retention logic anywhere** in `app/ingest/bulk/` — the only
`prune` calls are row-level. Two days of snapshots are already on disk:

```
2.7G  data/raw/sec_companyfacts
3.0G  data/raw/sec_edgar_submissions
6.9G  data/raw total
```

At the configured cadence that is **~570 GB/yr from EDGAR daily and ~73 GB/yr
from companyfacts weekly, permanently.**

> **Correction to what I told you earlier.** When you asked what the schedules
> would cost, I measured *database* growth (~60–90 MB/month, cents) and said
> turning them on was effectively free. That was right about Cloud SQL and
> wrong about the machine they run on: local disk grows ~640 GB/yr under the
> current cron, for a measured 0.4–0.5% daily change rate. The bandwidth is
> not the problem; the accumulating snapshots are.

Three fixes, in order of value:

1. **Send `If-None-Match` / `If-Modified-Since`** in `stream_to_file` and skip
   on 304. The ETag is already captured — it just has to be replayed. This
   alone converts both Class B sources to near-zero-cost polls.
2. **Retention**: keep the newest N snapshots per source on disk.
3. **Reconsider the cadence itself.** `sec_companyfacts` feeds **nothing** —
   its four tables appear nowhere in `app/marts/` or `app/entities/`, and its
   only consumer is a generic read endpoint. The second most expensive loader
   in the system has no downstream consumer.

### D3. Where cadence is actually wrong

Measured publication intervals from release keys (the publisher's real
cadence, since `discovered_at` only records our backfill bursts):

| source | releases | median gap | cron | verdict |
|---|---|---|---|---|
| `sec_13f` | 13 | 92 d | monthly check | correct — ~8 wasted GETs/yr |
| `sec_form_d` | 12 | 91 d | monthly check | correct |
| `sec_insider` | 8 | 91 d | monthly check | correct |
| `sec_adv_schedule_d` | 20 | 31 d | 4th monthly | **too rare** — see below |
| `sec_adv_roster` | 3 | 31 d | 5th monthly | window-capped, can't measure |
| `sec_iapd_feed` | 1 | — | daily | correct; one JSON GET |

**`sec_adv_schedule_d` is the sharpest miss.** Decoding the `uploadedOn` stamp
in each release key: SEC publishes on **day 1–3** of the following month
(lags of 0, 1, 0, 2, 0 days). The cron waits until the 4th. Worse — on
**2026-05-04 the SEC re-uploaded all twelve 2025 monthly files at once**.
Because the upload stamp is part of the release key, a restatement mints new
keys and correctly triggers a reload — but a monthly poll means a restatement
is picked up **up to 33 days late**. A daily manifest check costs one JSON GET
and cuts that to under 24 hours.

### D4. Missed runs are dropped, not caught

`add_job()` never sets `misfire_grace_time`, so APScheduler's default of
**1 second** applies. The current log window already holds 9 skipped runs,
missed by 1.1–2.6 seconds — ordinary event-loop latency:

```
Run time of job "People Collection Job Processor" was missed by 0:00:01.311453
Run time of job "Freshness Auto-Refresh Checker"  was missed by 0:00:02.644924
```

Three changes: per-dataset `misfire_grace_time` from the cadence profile;
`coalesce=True` so an outage produces one catch-up rather than a herd; and a
**reconciliation pass** asking "should this have run since it last did?",
which is what makes `stalled` detectable and survives the API being down —
which `misfire_grace_time` alone does not.

### D5. A failed release does not fail the job

`run_source` is documented *"Never raises for per-release errors"* — its inner
loop catches everything, writes `status='failed'` to `raw.source_release`, and
continues. **The job still completes `success`.**

So if the 13F load fails on the 9th, `entity_resolve` runs on the 10th against
last quarter's data, succeeds, and `pe_mart_build` produces a clean-looking
mart on stale inputs. Nothing checks: there is no freshness assertion and no
`source_release.status` gate in either executor. This is the silent-staleness
path SPEC_120's own description hand-waved past.

**Fix:** the mart executors assert their inputs' `source_release` status and
coverage before building, and record the input generations they consumed.

### D6. Backfill list (concrete)

- **`sec_insider` — 4 missing quarters**: `2023q3`, `2023q4`, `2024q1`,
  `2024q2`. It holds 8 quarters (`DEFAULT_QUARTERS = 8`); its sibling
  `sec_form_d` holds 12.
- **`sec_adv_roster` — 14 missing release keys**: `ria:` and `era:` for
  2025-12 through 2026-06. `DEFAULT_WINDOW = 3` months caps discovery.
- **`sec_adv_schedule_d`** — coverage starts 2025-01; the FOIA manifest likely
  lists earlier years. Needs a `discover(since=…)` probe.
- **Two releases stuck in `fetched`** (`sec_companyfacts`,
  `sec_edgar_submissions`) — downloaded but never merged. They will re-load on
  the next run; the portal should show them as "downloaded, not merged".

**Rolling-window hazard.** From October 2026, `sec_13f`'s
`DEFAULT_WINDOW_YEARS = 3` cutoff passes 2023-10, so `2023q3`/`2023q4` drop out
of `discover()` while their rows stay in the database — never refreshed or
re-verified again. Same mechanic in `sec_form_d` (`keys[-12:]`) and
`sec_insider` (`keys[-8:]`).

**13F holdings cliff.** `HOLDINGS_RELEASES = 1`, so `sec_13f_holdings` holds
exactly one quarter (3,830,274 rows). When the next window publishes, the
loader prunes the old one — there is no fallback and the table is a full
quarter behind the moment it rolls.

---

## 8. Reliability defects found along the way

### 8.1 Seven competing "last run" stores

| store | records | trust |
|---|---|---|
| `job_queue` | worker executions, all job types | ✅ since 2026-02-22 |
| `raw.source_release` | bulk data actually landed | ✅ the best-designed store here |
| `ingestion_jobs` | API-ingestor history | ✅ `status='success'` only |
| `ingestion_schedules.last_run_at` | **an incremental watermark, not a run time** | ❌ up to 6 months divergent |
| `dataset_registry.last_updated_at` | attempt *started* | ❌ 159 days stale |
| `source_watermarks` | 16 sources | ✅ but partial (16/60+) |
| `collection_watermarks` | site-intel only | ✅ scoped |

`ingestion_schedules.last_run_at` is the trap: **all 10 active schedules have
it NULL**, so a dashboard reading it would report every live pipeline as
"never run" — including `bulk:sec_adv_schedule_d`, which loaded 20 releases
four days ago.

### 8.2 A bug in SPEC_120, shipped this morning

`ingestion_jobs.id=3891` (`job:pe_mart_build`) is **`pending` since 15:26**,
while `job_queue.id=2668` for the same `job_table_id` **succeeded at 15:30**.

`_run_job_schedule` creates an `IngestionJob` row, but `pe_marts.py` and
`entity_resolve.py` never write back to it — only `bulk_ingest.py` has
`_start_ingestion_job` / `_finish_ingestion_job`. So **every scheduled mart run
leaves a zombie `pending` row**, which `cancel_stale_pending_jobs` will mark
**failed** after 4 hours. The mart will look like it failed every month while
succeeding. Mine, from this morning; it goes in SPEC_121.

### 8.3 `api` and `worker` are `restart: no`

A reboot silently stops all collection. The proxy already has
`unless-stopped`; these two do not.

---

## 9. Sequencing

Ordered so the cheapest fix to a live problem ships first.

| Spec | Scope | Size | Value |
|---|---|---|---|
| **SPEC_121** | misfire grace + coalesce + reconciliation; `IngestionJob` write-back for `job:` executors (§8.2); `restart: unless-stopped`; write `next_run_at` at registration | small | **stops silent loss happening now** |
| **SPEC_122** | conditional GET (`If-None-Match`) + snapshot retention | small | **~640 GB/yr → near zero** |
| **SPEC_123** | `DatasetSpec` catalogue, `dataset_key` on jobs, coverage SQL | medium | the grain gap |
| **SPEC_124** | `GET /datasets/status`, `POST /{key}/run`, `can_run` verdict | medium | one honest surface |
| **SPEC_125** | `frontend/status.html`; delete/repair the dead panel | small | the thing you asked for |
| **SPEC_126** | measured cadence profiles, mart input assertions (§D5), backfill of the named gaps | medium | cadence correctness |

SPEC_121 and SPEC_122 are independent of the portal and worth doing whatever
you decide about the rest.

---

## 10. Open questions for you

1. **Scope.** The portal can show ~10 live datasets, or all ~81 source
   families with 51 marked dormant and 11 blocked on missing API keys. The
   second is more honest and considerably bleaker. My recommendation is all of
   them — the dormancy *is* the finding — with a default filter to what is
   live.
2. **The dead fleet: revive or retire?** 51 sources have not succeeded in 159
   days, 20 have never succeeded once, and 11 are blocked on API keys that are
   simply absent from `.env`. That is a separate decision from this plan, but
   the portal will make it unavoidable. Do you want a recommendation on which
   to revive and which to delete?
3. **`sec_companyfacts` earns nothing.** 1.4 GB per run, no downstream
   consumer anywhere in the codebase. Keep polling it weekly, drop to monthly,
   or pause until something reads it?
4. **"Run now" from the browser.** Genuinely useful, and also a button that can
   start a 1.5 GB download. Gate behind a confirmation showing estimated cost,
   or leave out of v1?
5. **Should the portal write?** Pause/resume a schedule from the page is a
   small addition to the same endpoints, but it turns a read-only dashboard
   into an admin tool with different risk.

---

## Revisions

- **Rev 01 (2026-09-23)** — `PLAN_085_dataset_status_portal_and_cadence_rev_01.md`:
  §8.2 consequence corrected (schedules freeze, not fail), OQ3 premise refuted,
  access control + alerting added ahead of the portal (PLAN_086).
