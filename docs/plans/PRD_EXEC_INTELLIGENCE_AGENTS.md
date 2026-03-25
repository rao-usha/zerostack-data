# PRD — Executive Intelligence Agents
**Version:** 1.0 | **Date:** 2026-03-23 | **Status:** Draft
**Implementation plan:** PLAN_033_exec_intelligence_agents.md

---

## Problem

PE firms make multi-hundred-million-dollar bets on management teams but have no systematic way to assess leadership quality at acquisition, no early warning system for retention risk before an exit, and no view into the board relationships that govern who gets hired, fired, or backed next. The data exists in public filings — nobody has aggregated and scored it automatically.

---

## What We're Building

Three intelligence layers on top of the existing people collection pipeline, each independently useful but most powerful in combination.

---

## Agent 1 — Career Pedigree Scorer

### What it does
Scores every executive in the DB on career quality using existing work history and education data. No new HTTP collection — pure computation.

### Output
A `person_pedigree_scores` table with a 0–100 composite score and boolean flags:

| Signal | What it captures |
|---|---|
| `overall_pedigree_score` | Weighted composite (0–100) |
| `employer_quality_score` | McKinsey / Goldman / FAANG background vs. regional employers |
| `career_velocity_score` | Years from first job to C-suite vs. benchmark |
| `education_score` | Elite MBA / Ivy undergrad |
| `pe_experience` | Has operated inside a PE-backed company |
| `exit_experience` | Was at a company through an M&A close |
| `tier1_employer` | At least one Tier 1 employer (consulting, banking, or tech) |

### API surface
| Endpoint | Use case |
|---|---|
| `POST /people-analytics/companies/{id}/score-pedigree` | Score all current execs at a company |
| `GET /people-analytics/companies/{id}/pedigree-report` | Team quality summary with per-exec breakdown |
| `GET /people/{id}/pedigree` | Individual score, with `?recompute=true` to refresh |

### Buyer value
PE deal team can answer in 30 seconds: *"Does this management team have the pedigree to execute a $500M growth thesis? Have any of them done this before?"*

### Acceptance criteria
- [ ] `POST /score-pedigree` returns team avg score and per-exec flags
- [ ] `employer_quality_score` correctly identifies McKinsey, Goldman, Google backgrounds
- [ ] `pe_experience` and `exit_experience` flags are set when experience data supports it
- [ ] Scores are cached in `person_pedigree_scores` with `scored_at` timestamp
- [ ] `GET /pedigree-report` returns sorted list (highest pedigree first)

---

## Agent 2 — Board Interlock Agent

### What it does
Maps every board seat held by every person in the DB — current and historical — then computes the co-director graph: who sits on boards together, what companies connect them, and how central each director is to the broader network.

### Data sources
1. **SEC DEF 14A** — "Other Public Company Directorships" section (already fetched by SECAgent infrastructure)
2. **Company board pages** — "Board of Directors" pages on company websites (distinct from leadership pages)

### Output
Two new tables:

`board_seats` — every board seat per person
- company, role, committee (Audit, Compensation, etc.), is_chair, tenure dates

`board_interlocks` — computed co-director pairs
- person A + person B + shared company — tells you who traveled together from deal to deal

### API surface
| Endpoint | Use case |
|---|---|
| `POST /board-interlocks/collect/{company_id}` | Scrape DEF 14A for a company's board + other directorships |
| `GET /board-interlocks/person/{id}/seats` | All boards a person sits on |
| `GET /board-interlocks/person/{id}/co-directors` | Who else sits on boards with this person |
| `GET /board-interlocks/company/{id}/network` | Board network graph (nodes + edges) for visualization |
| `POST /board-interlocks/compute/{company_id}` | Trigger interlock computation from existing seat data |

### Buyer value
Three distinct use cases:
1. **Pre-acquisition:** *"Who are the board members, what else do they control, and who are they connected to?"* — answers whether the board will be constructive or obstructive post-close
2. **Relationship mapping:** *"Our GP has co-invested with this director before. Use that relationship to get a warm intro."*
3. **Network scoring:** Boards where every member is also connected to KKR or Warburg portfolio companies are a signal of a well-networked, PE-experienced board

### Acceptance criteria
- [ ] `POST /collect/{company_id}` successfully parses DEF 14A "Other Directorships" section
- [ ] `board_seats` rows created for each director + their external board memberships
- [ ] `POST /compute/{company_id}` creates `board_interlocks` rows for all director pairs sharing a board
- [ ] `GET /network` returns valid nodes/edges structure (0 edges is valid if no interlocks exist)
- [ ] No duplicate `board_seats` rows on re-collection (upsert behavior)

---

## Agent 3 — SEC Proxy Compensation Agent

### What it does
Parses the **Summary Compensation Table** from DEF 14A proxy filings and populates the compensation columns that already exist in `company_people` but have never been filled:
- `base_salary_usd`
- `total_compensation_usd`
- `equity_awards_usd`
- `compensation_year`

Also parses **Form 4** insider transaction filings and stores buy/sell activity in a new `insider_transactions` table.

### Why compensation data matters
Three strategic uses:
1. **Comp benchmarking:** Is the CFO paid in line with peers? Under-compensation = flight risk before exit. Over-compensation = EBITDA drag the buyer will discover in diligence.
2. **Equity alignment:** What fraction of total comp is equity? High equity % = better aligned with exit outcome.
3. **Insider selling signal:** An executive liquidating 30%+ of holdings 12–18 months before a planned exit is a significant red flag — they may know something, or they're not committed to the outcome.

### API surface
| Endpoint | Use case |
|---|---|
| `POST /people/companies/{id}/collect-comp` | Trigger DEF 14A + Form 4 collection for a company |
| `GET /people/companies/{id}/executive-comp` | Current team comp table |
| `GET /people/{id}/compensation-history` | Multi-year comp across all public company roles |
| `GET /people/{id}/insider-transactions` | Form 4 history with net buy/sell summary |

### Acceptance criteria
- [ ] `POST /collect-comp` populates `base_salary_usd` + `total_comp_usd` for at least 3 execs when DEF 14A exists
- [ ] Compensation matched to existing `company_people` rows by last-name fuzzy match
- [ ] `GET /executive-comp` returns sorted list (highest total comp first)
- [ ] Form 4 transactions stored with correct `transaction_type` (buy / sell / option_exercise)
- [ ] `is_10b5_plan` flag correctly set when proxy transaction plan is indicated
- [ ] Re-running collect does not create duplicate `insider_transactions` rows

---

## Compound use case: Management Quality Report

When all three agents have run on a portfolio company, a single call can produce:

```json
{
  "company": "PortCo X",
  "team_pedigree_avg": 72,
  "pe_experienced_pct": 60,
  "comp_alignment": "3 of 5 execs equity-heavy (>40% total comp)",
  "insider_signal": "CFO sold $1.2M in past 6 months — flag for retention discussion",
  "board_network": "2 board members also sit on KKR portfolio company boards",
  "succession_gaps": ["No internal successor identified for CFO", "CTO has no VP Engineering reporting to them"]
}
```

This replaces 2–3 weeks of consultant work for a PE ops team preparing for an exit process.

---

## Dependencies

| Agent | Requires | Notes |
|---|---|---|
| Pedigree Scorer | Existing `people_experience` + `people_education` data | Quality degrades if work history not populated — run deep-collect first |
| Board Interlock | Company CIK set in `industrial_companies` | DEF 14A collection only works for public companies |
| Proxy Comp | Company CIK set in `industrial_companies` | DEF 14A + Form 4 only for public companies; private co comp must be manual |

All three agents add new tables to `people_models.py` — tables are created at API startup via `Base.metadata.create_all()`. No migration needed.

---

## Out of scope (v1)

- LinkedIn scraping for career signals (ToS risk)
- Real-time comp benchmarking API (Radford, Mercer) — requires paid license
- Private company comp data (requires direct negotiation or disclosure events)
- Litigation / regulatory flag agent (CourtListener integration — Phase 2)
- Reputation / conference speaker agent (Phase 2)
