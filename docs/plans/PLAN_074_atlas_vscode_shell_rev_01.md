# PLAN 074 rev_01 — Atlas shell polish + default-layer correction

**Date:** 2026-05-27
**Parent:** PLAN_074_atlas_vscode_shell.md (Phase A shipped at 2669e95)
**Active spec:** SPEC_083_atlas_shell_polish (created with this revision)

## Revision 01

Two corrections required after shipping Phase A of the VS-Code-style
workspace shell:

1. **Visual polish.** Activity-bar icons (emoji 📋 ▦ 📍 🕘) and
   panel borders look "like 1990s Windows." User expects a modern,
   premium feel — closer to Linear / Cursor / Vercel — with refined
   typography, soft shadows, subtle gradients, micro-interactions,
   and a real SVG icon set instead of emoji.

2. **Default layer correction.** The Atlas opens with
   `disaster_nri` (Natural Hazard Risk) as the active layer. This
   was a leftover from when NRI was the demo darling, but it
   doesn't match the current product positioning: we built the
   thesis-driven site-selection workflow, so the landing layer
   should reflect what the *primary customer* (someone vetting a
   place to invest / open a business) wants first — typically
   median household income or population. Opening on NRI sends the
   wrong message about what the tool is for.

## What Was Wrong

### Visual
- Emoji icons render inconsistently across OSes (📋 is Apple
  clipboard on Mac, generic clipboard on Windows, etc.) and look
  amateur next to a real product like VS Code which uses crisp
  SVG glyphs.
- Borders use a single solid line at full opacity → harsh and
  flat. Modern UIs layer subtle shadow + soft border at lower
  opacity for depth.
- Hover/active states for the activity bar are flat color blocks.
- No transitions on view-switch — instant DOM swap feels janky.
- Form inputs in the Thesis view use the dark monospace IDE look
  but without the polish (no focus rings, no padding hierarchy).
- The status bar at the bottom is a single un-styled flex row.
- Save buttons are flat indigo blocks — no hover lift, no haptic
  feedback (subtle scale on press).
- Color palette is the original Wave-1 palette — fine for an MVP
  but reads as utilitarian, not premium.

### Default layer
- `state.layerId: 'disaster_nri'` in atlas.html (the JS state
  default) hardcodes NRI as what loads first.
- The same value is used when no `?layer=` query param is set.
- This contradicts the SPEC_082 thesis-driven framing where the
  user's stated industry should drive the default visual.
- Earlier feedback (PLAN_073 §6, captured in
  `memory/feedback_nri_vs_operational_risk.md` if it exists)
  already noted NRI is actuarial expected-annual-loss and is
  often a misleading first impression for non-insurance users.

## What Was Fixed

### Visual polish (SPEC_083)
- Replace emoji icons with **inline SVG glyphs** (24×24) drawn
  from a small in-house set: thesis (briefcase), layers
  (stack), places (location pin), history (clock). Use
  `stroke="currentColor"` + 1.75-px stroke for a Lucide-like
  weight.
- New **palette** — bump primary to a richer indigo
  `--primary:#7c83ff`, accent to a softer cyan `--accent:#67e8f9`,
  panel backgrounds slightly warmer, borders dropped to 40-50%
  opacity so they read as separators not walls.
- **Soft shadows** under floating elements (`box-shadow:
  0 1px 3px rgba(0,0,0,.2), 0 4px 12px rgba(0,0,0,.3)`).
- **Glass-morphism touch** on the bottom panel & status bar —
  `backdrop-filter: blur(8px)` + 80% bg opacity over the map.
- **Activity bar icons**: hover lifts color, active state shows a
  2-px gradient accent bar on the left + soft glow.
- **Form inputs**: focus ring (2-px primary at 30% opacity outset),
  rounded 6 → 7-px, slightly larger touch targets.
- **Save button**: gradient background (`linear-gradient(135deg,
  --primary, --accent)`), subtle 1-px translate on press.
- **View transitions**: opacity + 4-px y-translate on switch,
  120 ms eased.
- **Typography**: bump font to `Inter` w/ system-font fallback,
  tighter letter-spacing for headings, looser for labels.
- **Bottom-panel tabs**: underline indicator slides between tabs
  with a 180-ms ease.

### Default layer
- Change `state.layerId` default from `'disaster_nri'` to
  `'median_household_income_acs'` — universal, immediately
  meaningful, the most-asked-about layer by far.
- Add a tiny banner on first load:
  *"Set your investment thesis on the left → I'll re-pick the
   default layer for you."*
- When a thesis is saved with industry filled in, the next page
  load (or thesis-save action) sets the default layer based on
  industry heuristics (housing-heavy → density; retail → income;
  industrial → broadband). Implementation: `pickDefaultLayer(thesis)`
  helper.

## Lessons Learned

- **Don't ship emoji as icons in a product surface.** Either go
  bare (text-only buttons) or use SVG from day one. Emoji always
  read as placeholders and date the product instantly.
- **Default UI state speaks louder than copy.** What loads first
  tells the user what the tool is *about*. Picking NRI by default
  contradicts every other surface that says "site-selection".
- **Polish ≠ paint.** It's typography + shadow + transitions +
  consistent icon weight. One of those alone won't fix the feel.
- **The "set your thesis on the left" affordance was invisible.**
  A first-load banner / cursor hint is the fix — premium products
  use onboarding nudges, they're not optional.
