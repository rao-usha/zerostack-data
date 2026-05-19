# SPEC 063 — `frontend/diligence.html` Intake Page

**Status:** Draft
**Task type:** report (UI surface; closest fit — no static-page rubric exists)
**Date:** 2026-05-18
**Plan:** PLAN_065
**Test file:** _none_ — static HTML page, e2e verification per SPEC_058 convention.
**Builds on:** SPEC_062 (orders + intake API, this commit), SPEC_061 (report template), SPEC_060 (taxonomies).
**Numbering note:** PLAN_065 v2 originally called this SPEC_064. SPEC_062 (named-operators curator) is deferred per session decision; the intake page comes up by one number.

## Goal

Ship the customer-facing front door of the paid Sector × Market Intelligence
Pack product. Single self-contained HTML file (matches the existing
`playground.html` convention) — loads pricing + NAICS + MSA taxonomies from
the `/api/v1/diligence-pack` endpoints, presents a tight hero + pricing
block + form, and on submit shows a success card carrying the Stripe link
(when configured) or a "we'll be in touch" message (retainer / pre-Stripe).

## Acceptance Criteria

- [ ] `frontend/diligence.html` exists as a single self-contained HTML file —
      no external CSS framework, no build step.
- [ ] Visual language matches `frontend/playground.html` — dark theme, indigo
      primary, cyan accent, same font stack + spacing primitives.
- [ ] Sections in order:
  1. Hero (title + positioning copy + sample-link button)
  2. "What's in the pack" — bullet list of the 14 sections
  3. Pricing block — rendered live from `GET /api/v1/diligence-pack/skus`
  4. Intake form — NAICS picker (sector → industry drill-in), MSA picker
     (searchable), client_note textarea, contact_name + contact_email +
     contact_org inputs, source dropdown, SKU selector
  5. Submit → success card: order ID, payment-link button (when present),
     "what happens next" timeline; or error callout
- [ ] Pickers populate from `GET /api/v1/diligence-pack/taxonomies` on load —
      no hardcoded NAICS / MSA lists.
- [ ] NAICS picker is two-level: sector select → industry select; industry
      select repopulates when sector changes.
- [ ] MSA picker is a searchable `<input list="...">` datalist (no JS framework).
- [ ] Form validation client-side (required fields, email shape) — but the
      server is the source of truth (422 surfaces as error callout).
- [ ] On 422 response, the form preserves user input and shows the server
      `detail` in a red callout above the submit button.
- [ ] No external CSS / JS dependencies beyond what `playground.html` already uses.
- [ ] Page loads + renders correctly when served at `/diligence.html` (the
      frontend nginx already serves all `*.html` from the root — no nginx change).

## Acceptance verification (manual / e2e)

1. Open `http://localhost:3001/diligence.html` in a real browser
2. NAICS picker populates with 24 sectors; selecting "23 — Construction" populates the industry select with NAICS-4 codes like 2382
3. MSA picker autocompletes — typing "Houston" surfaces CBSA 26420
4. Pricing block shows 3 SKUs from the live `/skus` endpoint
5. Submit form → success card with "next step" message
6. Order row appears in `diligence_orders` table on local DB
7. Invalid NAICS / MSA returns a clean error callout, form values preserved
8. No console errors in DevTools

## Design Notes

### Page skeleton (rough — final markup may diverge)

```html
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Nexdata — Sector × Market Intelligence Packs</title>
  <style>
    /* Reuse the same CSS-variable palette as playground.html */
    :root { --bg:#0a0e1a; --card:#141b2d; --primary:#6366f1; --accent:#22d3ee;
            --text:#e4e6eb; --muted:#9aa3b8; --border:#2a3145; }
    /* …a few section helpers (.hero, .pricing-card, .form-row, etc.) */
  </style>
</head>
<body>
  <header class="hero">
    <h1>The market before the company.</h1>
    <p>Public-data sector × geography intelligence maps. 24-48 hours. From $2,500.</p>
    <a href="/p/<SAMPLE_REPORT_CODE>" class="btn-secondary">See a sample</a>
  </header>

  <section class="what-you-get">
    <h2>What's in the pack</h2>
    <ul><li>Structural density (Census CBP)</li>… (14 items)</ul>
  </section>

  <section class="pricing" id="pricing">
    <h2>Pricing</h2>
    <div id="pricing-cards" role="list"></div>
  </section>

  <section class="intake" id="intake">
    <h2>Request a map</h2>
    <form id="intake-form">
      <label>Sector  <select id="naics-sector" required></select></label>
      <label>Industry <select id="naics-industry" required></select></label>
      <label>MSA      <input  id="msa-search" list="msa-options" required></label>
      <datalist id="msa-options"></datalist>
      <label>SKU     <select id="sku" required>
                       <option value="single_map">$2,500 — Single map</option>
                       <option value="pilot_3_maps">$7,500 — 3-map pilot</option>
                       <option value="retainer">$10K-15K/mo — Retainer</option>
                     </select></label>
      <label>Name    <input id="contact-name" required></label>
      <label>Email   <input id="contact-email" type="email" required></label>
      <label>Org     <input id="contact-org"></label>
      <label>Notes   <textarea id="client-note" rows="4"></textarea></label>
      <label>How'd you hear?
                     <select id="source">
                       <option value="direct">Direct</option>
                       <option value="referral">Referral</option>
                       <option value="outreach">Outreach reply</option>
                       <option value="search">Search</option>
                     </select></label>
      <div id="form-error" class="callout-warn" hidden></div>
      <button type="submit">Request map</button>
    </form>
    <div id="success-card" hidden></div>
  </section>

  <script>
    const API = '/api/v1/diligence-pack';
    let TAX = null;

    async function bootstrap() {
      TAX = await (await fetch(API + '/taxonomies')).json();
      populateNaicsSectors(TAX.naics);
      populateMsa(TAX.msa);
      const skus = (await (await fetch(API + '/skus')).json()).skus;
      renderPricing(skus);
    }

    function populateNaicsSectors(naics) { /* sector select; on change, repopulate industry */ }
    function populateMsa(msa)             { /* datalist options + map "title" → cbsa_code */ }
    function renderPricing(skus)          { /* 3 cards */ }

    document.getElementById('intake-form').addEventListener('submit', async (e) => {
      e.preventDefault();
      const body = collectForm();
      const resp = await fetch(API + '/request', {
        method: 'POST',
        headers: {'Content-Type':'application/json'},
        body: JSON.stringify(body),
      });
      const data = await resp.json();
      if (resp.status === 200) showSuccess(data);
      else                     showError(data.detail || 'Request failed.');
    });

    bootstrap();
  </script>
</body>
</html>
```

### Sample-link target

Hero CTA "See a sample" links to the SPEC_061 smoke-test HTML for now —
once a public sample report is published via the report system, this becomes
the canonical sample URL. For v1 the file is at
`/data/reference/mip_smoke_sample.html` (NOT publicly served — frontend
serves `frontend/` only). Either:
  (a) copy the smoke sample into `frontend/diligence-sample.html` so nginx
      serves it; OR
  (b) skip the sample button until a real shareable report exists.

Going with (a) for v1: copy the sample once, commit it under
`frontend/diligence-sample.html`.

### MSA picker UX

393 MSAs is too many for a plain `<select>`. Using a native `<input list>`
+ `<datalist>` lets the browser handle autocomplete with no JS framework.
On submit we map the typed title back to a CBSA code via the `TAX.msa`
dict we already fetched.

### What's deliberately NOT in this page

- No client-side payment processing (Stripe Payment Links handle that
  off-page).
- No JavaScript bundler / framework. Vanilla JS only.
- No tracking pixels / analytics for v1 (manual outreach is the funnel).
- No auth — completely anonymous-friendly.

## Files to Create/Modify

| File                                                      | Action  | Description                                                                       |
|-----------------------------------------------------------|---------|-----------------------------------------------------------------------------------|
| `docs/specs/SPEC_063_diligence_pack_intake_page.md`       | Create  | This file                                                                         |
| `docs/specs/.active_spec`                                 | Modify  | → `SPEC_063_diligence_pack_intake_page`                                           |
| `frontend/diligence.html`                                 | Create  | Single self-contained intake page (~500 LOC est.)                                 |
| `frontend/diligence-sample.html`                          | Create  | Copy of `data/reference/mip_smoke_sample.html` for the hero "See a sample" CTA   |

## Feedback History

_No corrections yet._
