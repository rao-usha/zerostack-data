# Nexdata Atlas — Launch Readiness

**Date:** 2026-05-24
**Audience:** you (Alex), so you can decide *if*, *when*, and *how* to launch
**Length:** ~10 min read
**Status:** the Atlas is functionally complete enough to ship a public v1; everything below is the honest gap list + deployment path.

---

## 1 · What we actually built

A **public-data explorer** — map-first, no login, no query required, all
20 data layers reachable in one click.

**Live now (local + cloud-backed):**

| Category | What's there |
|---|---|
| **Data** | **22 layers** across 10 domains, county-grain where possible, state/point where honest; 3,143 counties, ~1.07M EPA facilities, 51,093 FEMA declarations. PLAN_067 backfills complete: ACS county wealth (SPEC_070), USAspending federal $ (SPEC_071), ACS broadband subscription (SPEC_072), SEC active filers + recent filings lane (SPEC_074), multi-year CBP (SPEC_075). |
| **Frontend** | Single self-contained `atlas.html` — Leaflet map, layer panel, place panel, header chrome. ~2,200 lines of HTML/CSS/JS. CDN deps only (Leaflet, D3, Observable Plot, Leaflet.heat). No build step. |
| **Dynamic UI** | Bivariate compare, scatter pair, brushed histogram, sparklines, animated migration arcs, **generalized time-cascade scrubber (FEMA + CBP, SPEC_066e)**, calendar heatmap per place, kernel-density heat toggle, **Recent Activity feed (FEMA + SEC, SPEC_067/074)**, smooth color transitions, animated counters, opacity slider |
| **API** | FastAPI; **14 atlas endpoints** including `/layers`, `/layer/{id}`, `/boundaries`, `/place/{geo_id}`, `/place/{geo_id}/series`, `/place/{geo_id}/events`, `/explore`, `/events`, `/feedback`, `/migration`, `/recent`, two cascade endpoints (FEMA + CBP). |
| **Telemetry** | Every interaction fires `POST /atlas/events` to `atlas_events` table (5 tables in the SPEC_064 schema) |
| **Quality** | **40/40 headless-Chrome smoke scenarios** pass in ~90s; **71/71 pytest pass**; smoke harness opt-in via `ATLAS_SMOKE=1` |
| **Tour** | `docs/LAUNCH_GUIDE.md` — comprehensive click-through showcase (the one to read) · `docs/ATLAS_TOUR.md` — developer/QA runbook |
| **Tour** | `docs/ATLAS_TOUR.md` — runbook for clicking through every feature with deep-link URLs |

**What's not built** is on the gap list (§5).

---

## 2 · Who would actually use this

**Honest answer: we don't know yet.** PLAN_066 v3 was rewritten
deliberately as "general explorer" because the earlier persona-anchored
framing got rejected — *let telemetry name the wedge*. The 30-day
measurement window (Phase 2) is designed precisely to surface this.

That said, the realistic audience hypotheses worth testing:

| Audience | Why they show up | Why they'd return |
|---|---|---|
| **Curious citizens / hobbyists** | Map of America with rich data is inherently interesting | Sharable URLs; "what's happening near me" |
| **Journalists** | County-level statistics for story research | Already-cited provenance per layer (FEMA, FRED, Census, BLS); cross-dataset joins they can't get from a single source |
| **Local economic development officers (EDOs)** | Want to benchmark their county vs neighbors / national | Cross-domain view; ability to see federal $ + workforce + risk in one place |
| **Real estate / site selection** | Where to expand / where to invest | Federal contract spending + permits + risk + demographics overlay |
| **Researchers / academics** | Need cross-domain county joins for papers | Atlas is the only place that joins NRI × ACS × FDIC × IRS × USAspending in one click |
| **Policy / think-tank staff** | Same as researchers, more applied | Recent Activity feed (when SPEC_067 ships) for time-shaped stories |
| **Government staff (federal / state)** | Quick "what does the data say about district X" | Deep-link URLs to share in briefings |

**The honest move:** ship to all of them, instrument heavily, let the
30-day read tell you which one keeps coming back. Don't market to one
before you know.

---

## 3 · What they do when they arrive

**Zero-query landing (no signup, no friction):**

1. They land on `/atlas.html`. The map renders in ~2 seconds.
2. **Default layer is FEMA disaster risk** — every US county is colored.
   The map alone is interesting; most people will hover counties for ~30s
   before clicking anything.
3. **The layer panel on the left** shows 20 layers grouped by domain.
   They scan, see one that interests them (Federal Dollars, Bank Deposits,
   Migration Flows), click it.
4. The map recolors with a smooth 400ms fade.
5. **They click a county.** The right panel opens with a multi-layer
   aggregate for that place: every value they have, plus a distribution
   histogram showing where this county sits in the national distribution
   plus a percentile readout ("78th percentile for AGI per return").
6. **They notice a sparkline** under FDIC Bank Deposits showing 168
   quarters since 1984 → "huh, this county's deposits really took off
   in 2008."
7. **They try "Compare layers"** → pick a B-axis (wealth vs disaster
   risk) → map recolors with a 3×3 bivariate scale; the legend changes.
   They scan for "high wealth × high risk" counties.
8. **They drag the FEMA scrubber** through years 1999→2026. Watch
   Hurricane Katrina light up the Gulf in 2005, Sandy in NY/NJ in 2012,
   Hurricane Beryl in TX in 2024.
9. **They share the URL** with a friend. ("Look at this — Arlington
   County gets $13.5B in federal contracts a year.") The friend opens
   it; same exact view loads.

**Stickier behaviors (Recent Activity, when SPEC_067 ships):**

10. **They check back tomorrow** to see what's new — was a fire declared
    in their county this week, did the SEC log a new IPO from their
    metro, etc.

The whole loop happens **without ever filling in a form.** That's the bet.

---

## 4 · Why they'd like it

**The value-prop, in order of distinctiveness:**

1. **Cross-domain joins in one click.** No other public-data tool puts
   FEMA risk × IRS wealth × FDIC deposits × USAspending dollars × ACS
   broadband on the same map keyed by county. ResearchData.gov has the
   raw files; nobody has bothered to join them. This is the one moat
   that doesn't decay.

2. **Honest data per layer.** Every layer card carries vintage,
   coverage_note, grain. The legend says "2024 ACS 5-year" not "latest";
   "3,222 counties via ZCTA→county" not "all counties." Atlas refuses
   to lie. (Most public-data tools are either too proud to say "we
   don't have this for your county" or hide it behind interpolation.)

3. **Map-first, zero-friction.** No login. No form. No "select your
   parameters" tutorial. The default view is meaningful (NRI risk over
   the entire US) within 2 seconds.

4. **Fun to play with.** Bivariate, scatter, scrubber, brushed
   histogram, migration arcs. These aren't decorative — every animation
   reveals a real pattern. The PLAN_070 §2 Pillar 2 hard rule: "animate
   to teach, not to decorate."

5. **Sharable URLs.** Every state of the explorer round-trips through
   the URL bar. "Share view" copies the link. This is the
   distribution mechanism for content reposting (journalists, EDOs).

6. **Free, no ads, no tracking beyond first-party telemetry** (which is
   product-honest and disclosed).

**What they DON'T get yet** (set expectations honestly):
- No CSV export of the underlying values (Phase 4 monetize)
- No saved-region / follow-this-place feature (deferred to SPEC_068)
- No SQL or API for custom queries (Phase 4 monetize)
- No automated alerts ("tell me when X changes") — Recent Activity is
  the manual version
- No mobile-optimized layout (works on tablet+; phone is squeezed)

---

## 5 · What's missing for launch (the gap list)

Stack-ranked by "absolutely must" → "nice to have."

### Tier 1 — can't launch without
| Item | Effort | Why |
|---|---|---|
| **A public hostname + HTTPS** | 1 hour | `https://atlas.nexdata.io` or similar; needs domain + SSL cert |
| **Public hosting for the API** | 2-4 hours | Cloud Run or Fly.io; container is already built — just push |
| **Public hosting for `atlas.html`** | 30 min | Cloudflare Pages / Netlify / Vercel — static file, free tier handles it |
| **A landing page or splash** | 2-4 hours | `/` should not 404. Either redirect to `/atlas.html` or a one-screen "what is this" page |
| **About / data-honesty page** | 1-2 hours | `/about` linked from header — what is this, who runs it, where does the data come from, why is it free |
| **Privacy + terms (minimal)** | 1 hour | Generic template; we collect telemetry (be specific), we use cookies for nothing (mention), data sources are public |
| **Robots.txt + sitemap** | 30 min | Don't be invisible to search; deep-link URLs should be indexable |
| **Health check + uptime monitor** | 1 hour | UptimeRobot or Better Stack — free tier, alerts on /health 5xx |

**Tier 1 total: ~1 working day.**

### Tier 2 — should have within a week of launch
| Item | Effort | Why |
|---|---|---|
| **Recent Activity feed (SPEC_067)** | 1-3 days | The single feature most likely to drive return visits |
| **OpenGraph / Twitter card** | 1 hour | When the share URL gets pasted to Twitter/LinkedIn it should render an image + title, not a blank link |
| **Server-side rendered preview snapshot** | half day | Hard mode: generate a PNG per share URL so the social cards show the actual view they're sharing |
| **Error page / 404 / 500** | 1-2 hours | Currently the user sees FastAPI's JSON error pages, which is ugly |
| **Email capture (zero-coupling)** | 2-4 hours | "Get the weekly digest" — single email field, no signup, no password; later you turn this on for newsletter / re-engagement |
| **First "story" article** | half day | One actual analysis with screenshots ("the federal-dollars map: who really gets the contracts") to seed search + social traffic |
| **Sentry / GCP error tracking** | 1 hour | When something throws in production, you want to know |
| **CDN in front (Cloudflare)** | 1 hour | Free, gives you DDoS protection + caching + edge SSL automatically |

### Tier 3 — only after Phase-2 measurement says the loop works
| Item | When |
|---|---|
| Stripe paywall / pro tier | When GATE 2 passes (PLAN_069) |
| Saved-region / follow-a-place | When wedge audience is identified (PLAN_066 §6) |
| API key + rate-limited public API | When pro tier ships (PLAN_069) |
| Embeddable widget for news sites | Only after stories generate inbound (PLAN_068) |
| Mobile-optimized layout | When telemetry shows ≥20% mobile traffic |

### Tier 4 — defer indefinitely until justified
- Mobile native app
- Real-time alerts (the data isn't real-time anyway)
- 3D / globe view (anti-our-honesty principle)
- AI chat over the data (commoditizing fast, weak moat)
- Sub-county granularity (block-group, tract) — PLAN_067-followon territory

---

## 6 · How to deploy (concrete)

The cheapest, sanest path. Total cost ~$20-50/month for the first year of low-traffic life.

### Architecture (proposed)

```
                                  Cloudflare (free)
                                   ↓ TLS + DDoS + CDN
   atlas.nexdata.io  → Cloudflare Pages (static, free)
                         serves frontend/atlas.html + ATLAS_TOUR.md

   api.nexdata.io    → Cloud Run (autoscale to 0)
                         runs the FastAPI Docker image
                         ↓ SQL connector
                       Cloud SQL postgres
                         (already exists — nexdata-cloud-sql)
```

### Step-by-step

1. **Register a domain** (~$12/yr): `nexdata.io` ideally; Namecheap or Cloudflare Registrar.
2. **Cloudflare account + add domain.** Free plan. Set DNS:
   - `atlas` CNAME → Cloudflare Pages
   - `api` CNAME → Cloud Run (after step 4)
3. **Cloudflare Pages**: connect to this GitHub repo; build settings = none, publish directory = `frontend/`. Push to main → auto-deploy.
4. **Cloud Run for the API**:
   ```bash
   gcloud builds submit --tag us-central1-docker.pkg.dev/PROJECT/atlas/api:latest
   gcloud run deploy atlas-api \
     --image us-central1-docker.pkg.dev/PROJECT/atlas/api:latest \
     --region us-central1 \
     --allow-unauthenticated \
     --min-instances 0 --max-instances 3 \
     --memory 1Gi --cpu 1 \
     --add-cloudsql-instances PROJECT:us-central1:nexdata-cloud-sql \
     --set-env-vars DATABASE_URL=postgresql+psycopg2://nexdata:${DB_PASSWORD}@/nexdata?host=/cloudsql/...
   ```
5. **Update `atlas.html`** — change `const API = '/api/v1/atlas'` to
   `'https://api.nexdata.io/api/v1/atlas'` (or use a relative `/api`
   path and rely on Cloudflare to route — cleaner long-term).
6. **HTTPS** is automatic via Cloudflare for both subdomains.
7. **Smoke test** the production URL via the existing harness:
   `python scripts/smoke_atlas.py --base https://atlas.nexdata.io --api https://api.nexdata.io`.
8. **Set up uptime monitor** on `https://api.nexdata.io/health` —
   UptimeRobot free tier, 5-minute interval, alerts your email on 5xx.
9. **Add `robots.txt` + sitemap** in `frontend/`. Sitemap should
   include all 20 layer URLs and the 8 featured place URLs (let Google
   index the deep-links).

### Monthly cost estimate (low-traffic)

| Item | Plan | Cost |
|---|---|---|
| Domain | annual | ~$1/mo |
| Cloudflare Pages | free | $0 |
| Cloud Run (autoscale 0) | pay-per-request | ~$5-15/mo at first |
| Cloud SQL (db-f1-micro, current) | shared core | ~$10/mo |
| Cloudflare CDN | free | $0 |
| UptimeRobot | free | $0 |
| Sentry | free tier (5k events/mo) | $0 |
| **TOTAL** | | **~$15-30/mo** |

If traffic grows past ~50k page views/mo, upgrade Cloud SQL tier
(~$25-50/mo) and add `--min-instances 1` to Cloud Run (~$15/mo).
That's still under $100/mo for a meaningful audience.

### What you'd run yourself

After it's deployed, the only ongoing operational work is:
- **Refresh data periodically.** The PLAN_067 ingest scripts
  (`scripts/ingest_*.py`) are one-shot; schedule them monthly via
  Cloud Scheduler → Cloud Run jobs. ~1 hour to wire up.
- **Watch the telemetry.** Phase-2 measurement uses the existing
  `atlas_events` table. Query it monthly to read activation, return,
  breadth, share rates (PLAN_066 §9).
- **Respond to Sentry + uptime alerts.** Maybe 10 min/week if quiet.

Total ops burden: ~1 hour/month after initial deploy.

---

## 7 · What to measure once live (Phase 2)

Per the `ATLAS_PROGRAM_ROADMAP.md` Phase-2 gate. **30-day measurement
window** before any monetization decision.

The five metrics — all queryable from `atlas_events`:

| Metric | Query | Decision rule |
|---|---|---|
| **Activation** | sessions that fired both `layer_toggled` AND `place_clicked` | ≥40% of sessions → loop is alive |
| **Breadth** | distinct domains per session | median ≥3 → users explore beyond first interest |
| **Return** | 7-day repeat-visit rate (cookie-based session_id) | ≥15% → reason to revisit exists |
| **Share** | `share_created` events per session | ≥5% → content is reposting itself |
| **Wedge** | top 3 domains/layers/places by interaction count | identifies which audience is converting |

**GATE 2 → 4** (per the roadmap): monetization (PLAN_069) does **not** start
unless return + share clear their floors AND a wedge audience is named.

---

## 8 · What you should genuinely worry about

Honest unknowns I can't answer from here:

| Concern | Why it matters | What I'd do |
|---|---|---|
| **Does anyone actually want this?** | The whole bet is "general curiosity-driven exploration is a category." If it's not, the loop never holds | Phase 2 measurement; if return rate < 5% after 30 days, the framing is wrong, not the product |
| **Distribution.** No one will find atlas.nexdata.io organically | Even great content needs a first push | PLAN_068 §3 — first story article + Twitter / LinkedIn / Hacker News post + EDO outreach |
| **Data staleness.** Most layers are 2023-2024 vintage; FEMA last refresh ~3 months stale | Users will notice "why does 2024 say…" | Schedule monthly refresh jobs; surface "vintage" prominently per layer (already done in the UI, but make it more visible) |
| **PostGIS absent on cloud DB** | Boundary geometry uses Python fallback (1.2MB instead of optimized vector tiles) | Acceptable for v1; SPEC for Vector Tiles + PostGIS is a follow-on if zoom-in performance is an issue |
| **Single point of failure on Cloud SQL `db-f1-micro`** | One shared core, no automatic failover | Acceptable for v1; upgrade tier + add read replica only when traffic justifies (>50k MAU) |
| **No abuse protection** | Public unauthenticated API; someone could hammer `/atlas/layer/{id}` | Cloudflare's free-tier rate limiting handles ~95% of this; add proper API-key gating only when an abuser shows up |

---

## 9 · The honest "should I launch?" call

**Yes, launch.** Specifically, launch a **soft public beta** at
`atlas.nexdata.io` with no announcement. Here's the reasoning:

- The product is functionally complete enough that a curious visitor
  won't think it's a prototype.
- The whole point of the Phase-2 measurement window is to read real
  usage telemetry. You cannot get that without real visitors.
- A soft launch (no announcement, no PR push) lets you find the
  rough edges with the lowest-stakes audience — your own visits,
  early friends/colleagues, slow organic discovery.
- If the soft launch reveals serious UX bugs or framings that don't
  work, you fix them in private before any "real" launch.
- Cost is ~$20/mo to keep it live. The downside of "don't launch yet"
  is delaying the only measurement that matters.

**Don't launch with a big PR push yet.** Per PLAN_068, heavy growth
investment (paid story cadence, EDO outreach) gates on Phase-2 signal
clearing the activation floor. Don't drive Danas to a map that doesn't
activate them.

---

## 10 · Concrete sequencing — the path to launched

If you say "go," here's the order I'd ship in:

1. **Today / tomorrow** *(½ day work)*: register domain; Cloudflare account; Cloudflare Pages deploy of the frontend; smoke-test against the local API.
2. **This week** *(1 day work)*: Cloud Run deploy of the API; switch frontend `API` constant to production; smoke-test cloud→cloud.
3. **This week** *(½ day)*: add /about page + privacy/terms + robots.txt + sitemap; wire OpenGraph card; set up UptimeRobot.
4. **Soft launch** *(no announcement)*: send the URL to 5 people you trust; collect first-impression feedback; fix top 3 things.
5. **Week 2** *(1-3 days)*: ship SPEC_067 (Recent Activity) — the one feature most likely to drive return visits.
6. **Week 3** *(½ day)*: write the first story article (e.g. "The Federal-Dollars Map"). Post to Hacker News. See what happens.
7. **Day 30 from soft launch**: query `atlas_events`, compute the five Phase-2 metrics, decide whether to start PLAN_068 heavy growth.

**Total time from "go" to "publicly readable URL": ~2 working days.**

---

## 11 · What's deferred *until* Phase-2 says it works

Hard discipline, per the roadmap:

- No paywall, no Stripe, no "pro" tier until GATE 2 passes
- No paid acquisition (Google Ads, sponsorships) until GATE 2 passes
- No outbound sales motion (EDO calls, partner pitches) until early Phase 2 signal
- No team hires for this product line until the loop holds
- No native app, no mobile rewrite, no enterprise dashboard until there's a paying audience

**Why so strict:** because PLAN_064 and PLAN_065 both shipped real
engineering into commercial framings that turned out to be wrong. The
lesson logged in `memory/feedback/corrections.md`: *don't build ahead
of proof.* The Atlas exists precisely because we learned that lesson —
keep it.

---

## Where to read more

- **What's built layer-by-layer:** `docs/ATLAS_TOUR.md` — clickable runbook
- **Why we're shipping this shape:** `docs/plans/ATLAS_PROGRAM_ROADMAP.md` — phase + gate logic
- **The general-explorer thesis:** `docs/plans/PLAN_066_atlas_map_v2.md` §1-3
- **Coming-up data backfills:** `docs/plans/PLAN_067_atlas_data_coverage_expansion.md`
- **The compounding-compellingness plan:** `docs/plans/PLAN_072_atlas_loop_compelling.md`
- **Growth thinking (when it's time):** `docs/plans/PLAN_068_atlas_growth_distribution.md`
- **Monetization thinking (gated):** `docs/plans/PLAN_069_atlas_monetization.md`

---

## tl;dr

Atlas is launch-ready as a soft public beta. ~2 working days to live,
~$20/mo to keep up. The whole bet is that curiosity-driven exploration
of governed cross-dataset public data is a category — Phase-2
measurement tells us whether that's true. Don't push for scale until
the loop reads green.
