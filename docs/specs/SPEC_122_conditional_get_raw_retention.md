# SPEC 122 — Conditional GET + raw snapshot retention

**Status:** Implemented (branch spec-122)
**Task type:** service
**Date:** 2026-09-23
**Plan:** PLAN_087 wave 1, PLAN_085 §D1–D2, review `docs/reviews/2026-09-23_daas_platform_review.md` ("ETag is captured but never sent back", "No file retention")
**Test file:** tests/test_spec_122_conditional_get_raw_retention.py

## Goal

A no-op run of a date-keyed snapshot source (`sec_edgar_submissions`,
`sec_companyfacts`) must cost one small HTTP request, not a 1.4–1.6 GB download
plus a full merge; and raw files must not accumulate on disk forever
(~640 GB/yr at the configured cadence).

## Facts (verified in code at de3152d)

- Both snapshot sources `discover()` a fresh `snapshot:{utc today}` key each
  day (`sec_edgar_submissions/source.py:76`, `sec_companyfacts/source.py:153`).
  A fresh key is never `loaded`, so `run_source` always fetches it.
- `SecHttp.stream_to_file` (`app/core/sec_http.py`) captures `ETag` /
  `Last-Modified` into `FetchedFile`; `_ensure_file` (`bulk/base.py`) writes
  them to `raw.source_release.etag/last_modified`. Nothing reads them back and
  no request carries `If-None-Match` / `If-Modified-Since`.
- No file retention exists in `app/ingest/bulk/`. Files live at
  `<bulk_raw_dir>/<source>/<safe release key>/<file>` (default `data/raw`).
- `raw.source_release.status` has a CHECK constraint
  (`discovered|fetched|staged|loaded|failed`, alembic 0004). This spec makes
  no schema changes.
- SEC's `www.sec.gov/Archives/...` static files are served through a CDN that
  returns `ETag` and `Last-Modified`. Whether it answers conditional requests
  with 304 for these two zips is not verified from code; the design handles
  both a 304 and a server that ignores the headers (200 + identical ETag or
  identical sha256).

## Acceptance Criteria

- [x] `SecHttp.stream_to_file(url, dest, etag=None, last_modified=None)` sends
      `If-None-Match` / `If-Modified-Since` when given; on 304 it writes no
      file (no `.part` either) and returns a `FetchedFile` with
      `not_modified=True`, `bytes=0`.
- [x] `BulkSource.snapshot = True` on the two snapshot sources opts them into
      conditional fetch. For such a source, `run_source` looks up the most
      recent **loaded** release with the same URL and replays its
      ETag/Last-Modified (via `release.meta`, so the default `fetch()` passes
      them through).
- [x] On 304 the new release is **not minted**: its `raw.source_release` row
      (created by discovery, nothing fetched) is deleted, the load is skipped,
      the prior loaded row's `updated_at` is touched ("confirmed current"), and
      the summary records `unchanged += 1`, the key in `unchanged_releases`, and
      the prior file size in `bytes_saved`.
- [x] If the server ignores the conditional headers and returns 200, the
      returned ETag (exact match) or the file's sha256 is compared with the
      prior loaded release **before** the merge; if equal, the duplicate file
      is deleted, the row is deleted, and the release counts as `unchanged`
      (download not saved, merge skipped). No row is left in `fetched`.
- [x] Non-snapshot sources behave exactly as before (no conditional headers).
- [x] Summary gains `unchanged`, `unchanged_releases`, `bytes_downloaded`
      (HTTP bytes during this run), `bytes_saved`, `retention`. The worker job
      progress message reports unchanged releases and MB downloaded/saved.
      `raw.source_release.bytes` continues to record the size of each file
      actually downloaded.
- [x] Retention (`app/ingest/bulk/retention.py`): `apply_retention(engine,
      source, raw_root, keep, dry_run)` keeps the newest `keep` loaded files per
      source (ordered by `loaded_at` desc, then `release_key` desc; `keep` is
      clamped to ≥1 so the newest loaded file is always kept). It never deletes
      the file of a release that is `discovered|fetched|staged|failed` unless
      the release is marked superseded (below). It deletes only files inside
      `<raw_root>/<source>/`, removes the emptied release directory, and never
      touches files not referenced by a release row (reported as `orphans`;
      they may be an in-flight `.part`). Dry-run lists every file it would
      delete with bytes and `bytes_to_free`, changing nothing.
- [x] `BULK_RAW_RETENTION` (settings `bulk_raw_retention`, default 2).
      `run_source` applies retention automatically after each run of a
      `snapshot` source (best-effort; errors are logged, never fail the run).
      Other sources are pruned only on explicit request (see Decisions).
- [x] One-time cleanup: `python -m app.ingest.bulk.retention [--source S]...
      [--keep N] [--apply]` (dry-run by default; default sources = snapshot
      sources) and admin `POST /api/v1/bulk/raw/cleanup?source=&keep=&dry_run=true`
      (bulk router is already admin-only).
- [x] Stale snapshot rows: for a snapshot source, any non-loaded row whose key
      is older than the current snapshot key is marked superseded —
      `status='failed'`, `error='superseded: ...'` (prefix
      `SUPERSEDED_PREFIX`). This happens at discovery in `run_source` and in
      the cleanup (dry-run only reports it). Superseded files are eligible for
      retention. This resolves the two releases stuck in `fetched` from
      PLAN_085 §D6.

## Design

### `app/core/sec_http.py`
- `_send(url, stream, headers=None)`; 304 is `< 400` so it is returned.
- `FetchedFile.not_modified: bool = False`.
- `stream_to_file(..., etag=None, last_modified=None)`: on 304 close the
  response and return `FetchedFile(path=dest, bytes=0, sha256="",
  etag=<304 ETag or the one sent>, last_modified=..., not_modified=True)`.

### `app/ingest/bulk/base.py`
- `BulkSource.snapshot = False`; default `fetch()` passes
  `release.meta["if_none_match"/"if_modified_since"]`.
- `ReleaseUnchanged(Exception)` raised by `_ensure_file` (304 or equal
  ETag/sha256); caught in the release loop → `_drop_unminted()` +
  `_touch()` of the prior row + summary.
- `_prior_loaded(conn, source, url, exclude_key)`.
- `supersede_stale(engine, source, current_key, dry_run)`.
- After the loop: `apply_retention` for snapshot sources.

### Why the stuck rows are superseded, not re-merged
A snapshot older than the newest *discovered* one will never be loaded
(`discover()` only returns today's key), and merging it after a newer one would
regress data. Today's run fetches the latest content anyway; if content
changed since the last loaded snapshot the conditional GET returns 200 and
it loads. So the old row only needs an honest terminal state and its file
freed. `failed` + `superseded:` is the only terminal non-loaded state
the CHECK constraint allows without a migration.

## Decisions

- Automatic retention only for snapshot sources. Quarterly/monthly archives
  are window-capped (bounded disk) and operators reload them from local zips
  after parser changes (2026-09-20 13F reload). The cleanup CLI/endpoint can
  prune any source explicitly with `--source`.
- Unchanged detection uses exact ETag or sha256, never Last-Modified alone.
- Skipping an unchanged day means the 3-year cutoff prune in
  `sec_edgar_submissions` advances on the next changed day instead (≤1 day).

## Test plan

| # | Test | Kind |
|---|---|---|
| T1 | 304 → no file, no `.part`, `not_modified`, headers sent | unit (MockTransport) |
| T2 | no validators → no conditional headers; 200 path unchanged | unit |
| T3 | snapshot run, 304: no release minted, row deleted, summary unchanged/bytes_saved, load not called | PG |
| T4 | snapshot run, server ignores headers, same ETag → merge skipped, file deleted, no `fetched` row | PG |
| T5 | snapshot run, content changed → loads normally, etag stored | PG |
| T6 | non-snapshot source never sends conditional headers | PG |
| T7 | retention keeps newest N loaded, protects unloaded, deletes superseded | PG |
| T8 | dry-run lists files + bytes, deletes nothing; keep=0 clamps to 1 | PG |
| T9 | stale fetched snapshot rows superseded at discovery | PG |
| T10 | executor progress message shows unchanged/bytes | unit |
| T11 | cleanup endpoint delegates with dry_run default True | unit |
| T12 | `bulk_raw_retention` setting default 2, env override | unit |
| F1 | watchdog `failed_release` ignores `superseded:` rows | PG |
| F2 | unchanged run bumps the prior row's `loaded_at` | PG |
| F3 | parser_version bump / `force=True` reload an unchanged snapshot | PG |
| F4 | failed snapshot's file kept while no newer load; failed-superseded kept until `purge_failed`; stuck-superseded newer than every load kept | PG |
| F5 | run_source / cleanup skip while another holder has the source lock; lock released after | PG |
| F6 | executor passes `force`, reports `locked`; endpoints pass `force` / `purge_failed` | unit |

## Review fixes (spec-122-fix)

1. **Watchdog noise.** `rule_failed_releases` excludes rows whose `error`
   starts `superseded:` (literal in the SQL; the watchdog does not import the
   bulk registry). A real failure alerted when it happened.
2. **One freshness signal: `loaded_at`.** An unchanged run (304 / equal ETag /
   equal sha256) sets the prior loaded row's `loaded_at` (and `updated_at`)
   to now. `loaded_at` now means "content confirmed current as of", so
   SPEC_126a's `check_source` (`MAX(loaded_at)`) and SPEC_124 need no change.
3. **Parser changes reload.** `_prior_loaded` only matches a release loaded
   with the current `parser_version`, so a bump re-downloads and re-merges.
   `run_source(force=True)` (worker payload `force`, `POST /bulk/{source}/run?force=true`)
   skips the conditional headers and the ETag/sha256 comparison.
4. **Superseded files are not deleted early.** A superseded file is eligible
   only once newer content has loaded: a loaded release with a later key, or
   a loaded release whose `loaded_at` is after the file's `fetched_at` (an
   unchanged re-verification counts). A superseded release that had
   **failed** (not just stuck `fetched`/`discovered`/`staged`) is kept for
   diagnosis until an operator passes `purge_failed`
   (`--purge-failed` / `POST /bulk/raw/cleanup?purge_failed=true`).
   Protected entries carry a `reason`.
5. **Per-source lock.** `run_source` holds `pg_try_advisory_lock(122,
   hashtext(source))` on an AUTOCOMMIT connection (no long open transaction)
   for the whole run; a second run returns `locked=True` without
   discovering, superseding, pruning or merging. The executor reports
   `skipped: another <source> run is in progress`. `cleanup` takes the same
   lock per source and reports `skipped` for a busy source.

Not changed: automatic retention stays snapshot-only; archive sources are
pruned only by an explicit `--source` cleanup, where "newest N" orders by
`loaded_at` (most recently loaded), not by release date. Unchanged days are
not persisted as rows (the minted row is deleted); the durable trace is the
prior row's bumped `loaded_at`, plus job progress.

## Test environment note

Docker Desktop hung (`docker ps` timed out at 60 s) while this spec was built,
so the suite ran on a host Python 3.11 venv with an embedded PostgreSQL
(`pgserver`) as `TEST_PG_URL`. The test module sets a placeholder
`DATABASE_URL` when none is set (the api image always sets one).

## Live follow-ups (operator)

1. After deploy, dry-run then apply the one-time cleanup:
   `python -m app.ingest.bulk.retention` (lists files + `bytes_to_free`), then
   `--apply`. This also supersedes the two stuck `fetched` snapshot rows;
   their files are freed once a newer snapshot has loaded or been re-verified
   (otherwise listed under `protected` with a reason; the next scheduled run
   frees them). Superseded releases that had failed need `--purge-failed`.
2. The first run of each snapshot source after deploy compares against the
   newest loaded release's stored ETag; check the job progress message for
   `unchanged upstream` / `MB saved` to confirm SEC answers 304.
