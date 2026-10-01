# PLAN 092 — Web domains as an identity carrier (B2)

**Status:** Approved by task (workbench orchestrator, task B2, 2026-09-30) · **Spec:** `docs/specs/SPEC_148_domain_identity.md`

## Why (measured 2026-09-30, read-only)

- `core.entity.canonical_domain` 18,479 / 140,963 filled, all from ADV/IAPD (one filing, one family);
  the workbench never exposes it (linkedin.com 3,544, facebook.com 752).
- ADV roster websites: 6,730 of 24,017 latest-per-CRD rows are social links, not company sites.
- EDGAR `sec_filers.website` / `investor_website`: 0 of 986,698 filled, so EDGAR cannot be a second
  source today.
- Public tables with company domains not wired to core: pe_firms (web-researched 102), industrial 226,
  pe_portfolio 269, portfolio 7, three_pl 6, family_offices 85, lp_fund 563.
- Owner rules in play: NexData-only data; domains never a merge key; open-web fetches only after a
  per-site terms review (workbench `ingest/hosts.py` precedent: undeclared host = zero requests).

## Steps

- [x] Spec + failing tests (T1–T23)
- [x] `domains.py` + vendored PSL (ICANN, VERSION 2026-09-24, COMMIT a179a48c)
- [x] `feeds.py`: classify domains + stats; attach-only feeds for the 7 public tables
- [x] `resolve_core.py`: `domain_links()`; `resolve.py`: split records, write domain_link / identifier / canonical_domain
- [x] Migration `0017_domain_link` (core.domain_link, core.domain_probe)
- [x] `open_web.py` + `domain_probe.py` + `site_terms.json` (empty by default)
- [x] ruff + unit + PG tests (throwaway postgres:14-alpine)
- [x] Small live probe sample only on hosts with a recorded terms review
- [x] DRY RUN via the job path (one-off worker container, pre-claimed job row) — strong/weak/conflict/generic counts
- [x] LIVE run, same path; before/after counts
- [x] Workbench: bridge refresh dry then --apply; eval_core_identity.py, eval_bridge_core_view.py
- [x] Session log

## Owner calls left open

- Attach-only sources never resolve (industrial/pe_firms CIKs are not merge evidence).
- Which hosts get a terms review (the probe refuses every other host).
