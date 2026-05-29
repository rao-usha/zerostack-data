# PLAN 074 rev_03 — Atlas Pilot funkiness fixes + furniture-store demo thesis

**Date:** 2026-05-28
**Parent:** PLAN_074_atlas_vscode_shell.md
**Prior revisions:** rev_01 (SPEC_083 polish), rev_02 (SPEC_084 thesis blocks)
**Active spec:** SPEC_085_atlas_pilot_cleanup

## Revision 03

User ran the guided tour and hit a pile of rough edges:

> "the atlas pilot is very funky. It got hung up here [Highlight Austin
> tracts with income > $150K]… ⚠ Unlinked numerical claims (no cite()
> call): $150K … [1] toggle_layer median_household_income_acs → ERROR
> unknown layer_id … Also it has weird sliders like on live tool log.
> I can't continue the chat it's not moving to history and it's just
> kinda a mess. Can you please fix by building a full thesis against
> the furniture store."

Six concrete bugs + one feature request.

## What Was Wrong

1. **Backend system prompt trains a bad layer id.** The SPEC_081
   example flow hardcodes `toggle_layer(layer_id="median_household_
   income_acs")` — an id that doesn't exist in the registry. The agent
   copies it, gets `ERROR: unknown layer_id`, wastes a loop, recovers
   via `list_layers`, then uses the real `demo_tract_median_income`.
   Same invented-id class of bug as the frontend 404 (commit 6128479)
   — I fixed the frontend but missed the prompt.

2. **Tool log is double-escaped.** `tool_call_started` / `_completed`
   call `escapeHtml(JSON.stringify(args))` and then `appendToolLog`
   writes via `textContent`. `textContent` does not decode entities,
   so the user sees raw `&quot;layer_id&quot;` instead of `"layer_id"`.

3. **The chat column is cluttered ("weird sliders / a mess").**
   `#pilot-tools`, `#pilot-cites`, and `#pilot-meta` were supposed to
   move to the bottom inspector (SPEC_082) but were never actually
   hidden in the chat column. The result: the live tool log (monospace
   step boxes with its own `max-height:200px; overflow-y:auto`) renders
   inside the chat, creating nested scrollbars ("weird sliders") and
   visual noise on top of the narration + options.

4. **Options leak into the narration as text.** The agent ends its
   narration with "What would you like to explore next?" followed by a
   markdown bullet list of the options — *and* (sometimes) also calls
   `present_options`. The user gets duplicate, non-clickable text
   bullets, which reads as broken ("I can't continue the chat").

5. **Citation-hardening false positives.** `$150K` / `$200K` came
   straight from the user's own question ("income > $150K"); they're
   not data claims, but `find_unlinked_claims` flags them, producing
   scary ⚠ warnings on every guided-tour turn.

6. **History view is wired to the wrong localStorage key.** The
   left-panel History view reads/clears `nexdata.pilot.hist`, but the
   Pilot actually saves to `atlas_pilot_history`. The History view is
   therefore *always empty* — "it's not moving to history."

## What Was Fixed (→ SPEC_085)

1. **Real layer id in the prompt example** (`demo_acs_median_income`)
   plus a hard rule: *"NEVER guess a layer_id. The valid ids come from
   list_layers / the registry; if unsure, call list_layers first."*
2. **Tool log escape fix** — drop `escapeHtml` before the `textContent`
   append (textContent is already injection-safe).
3. **Declutter the chat column** — hide `#pilot-tools`, `#pilot-cites`,
   `#pilot-meta` inside `#right-col`; stream the tool log **live** into
   the bottom-inspector Tool Log pane instead of mirroring only at
   `done`. Chat column shows just: turn-bar → question → narration →
   options → history count.
4. **Forbid options-as-text** — strengthen the prompt (final narration
   must NOT contain an options list or "What would you like to explore
   next?"). Defensive backend strip of a trailing options block from
   the narration before returning.
5. **Suppress echoed-number false positives** — pass the `question`
   (and recent history) into `find_unlinked_claims`; skip any numeric
   token that appears verbatim in the user's own text.
6. **Unify the history key** — History view reads/saves the same
   `atlas_pilot_history` key the Pilot writes.

### Feature — furniture-store demo thesis

A one-click **"Load furniture-store demo"** preset that fills a
complete, realistic thesis:
- Industry: Furniture stores (NAICS 442110)
- HHI min $75K
- Age band 25-44
- Region: Austin, TX metro
- Notes: positioning + trade-area assumptions
- Excludes: disaster_nri

So the user can click one button, then ask the Pilot "walk me through
where to open this" and get a clean end-to-end tour.

## Lessons Learned

- **Search the WHOLE repo for an invented constant, not just the file
  you noticed it in.** The fake `median_household_income_acs` lived in
  both the frontend default *and* the backend prompt; fixing one left
  the other to fail. `grep` the id across `app/` + `frontend/` before
  declaring a fix done.
- **`escapeHtml` + `textContent` is a double-escape.** Escape for
  `innerHTML`, never for `textContent`. Pick one sink and match it.
- **A localStorage feature has exactly one key.** When a reader and a
  writer disagree on the key, the feature silently no-ops. Grep both
  sides whenever adding a persisted view.
- **If the model can express an action two ways (a tool call OR prose),
  it will sometimes do both.** Forbid the prose form explicitly when a
  structured tool exists.
- **Citation hardening must know the question.** A number the user
  typed is not a claim the agent must cite.
- **The fastest way to make a flow feel "not funky" is a known-good
  demo path.** Ship the furniture-store preset so there's always a
  one-click happy path to fall back on.

## Files Affected

| File | Change |
|---|---|
| `app/services/atlas/pilot.py` | real id in example + no-guess rule; forbid options-in-narration; strip trailing options block; `find_unlinked_claims(question=...)` |
| `frontend/atlas.html` | tool-log escape fix; hide tools/cites/meta in chat; live-stream tool log to bottom; unify history key; furniture-store demo button |
| `docs/plans/PLAN_074_atlas_vscode_shell_rev_03.md` | this file |
| `docs/plans/PLAN_074_atlas_vscode_shell.md` | `## Revisions` → rev_03 |
| `docs/specs/SPEC_085_atlas_pilot_cleanup.md` | companion spec |
| `tests/test_spec_085_atlas_pilot_cleanup.py` | new tests |

## Acceptance

- [ ] Pilot's first tool call uses a real layer id (no `unknown
      layer_id` error in the log for the demo question).
- [ ] Tool-log entries show real quotes, not `&quot;`.
- [ ] Chat column shows no raw tool log / cites / meta; bottom
      inspector Tool Log streams live.
- [ ] Final narration contains no options bullet list.
- [ ] `$150K` echoed from the question is NOT flagged as unlinked.
- [ ] Asking a question then finishing populates the History view.
- [ ] "Load furniture-store demo" fills the thesis; the subsequent
      Pilot question runs cleanly end-to-end.
