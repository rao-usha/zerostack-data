# SPEC 081 — Atlas Pilot guided-tour mode

**Status:** Draft (this commit)
**Task type:** service + frontend
**Date:** 2026-05-27
**Plan:** PLAN_073 rev_01 Phase B v1.2 (between v1.1 streaming and Phase C)
**Builds on:** SPEC_078 + SPEC_079 + SPEC_080 polish iteration.

## Goal

Turn the Atlas Pilot from one-shot Q&A into a **conversational
guided walkthrough**: the agent answers a question by setting up the
map, narrating what's on screen, then presents 2-4 clickable choices
for what to do next. User clicks an option, the agent continues with
the next step (with prior context). Multi-turn exploration replaces
single-shot Q&A.

## The new interaction shape

**Today (one-shot):**
```
User: "Compare Austin and Houston on income"
Agent: [calls compare_places + toggle_layer + narrates] DONE
```

**With SPEC_081 (guided tour):**
```
User: "I want to open a chair store in Austin"
Agent: [zooms to Austin, activates median income tract layer]
       "Austin's median income varies from $35K in east Austin to
        $180K+ in Westlake. What would you like to look at next?"
       Options:
         [Find the wealthiest neighborhoods]
         [See competition for furniture stores]
         [Look at age demographics]
         [Plant a focal node at a specific spot]
User clicks "See competition for furniture stores"
Agent: [activates a placeholder competition layer, narrates]
       "There are ~120 furniture-related businesses in the Austin
        metro. Highest density: South Congress + Burnet Rd. Next?"
       Options:
         [Zoom into South Congress]
         [Plant a chair store on Burnet Rd]
         [See what kinds of furniture stores compete]
User clicks "Zoom into South Congress"
Agent: [zooms map to South Congress, narrates the trade area]
       "You're now at zoom 13 over South Congress. Median income
        within 1 mi is $78K. Competition: 4 furniture stores within
        a mile. Next?"
       Options: ...
```

Each turn:
1. Mutates the map (one or two UI actions)
2. Narrates what's now on screen
3. Presents 2-4 clickable next-step options
4. Stops and waits for the user to click or type something new

## Acceptance Criteria

- [ ] New UI tool `present_options(intro, options: [{label, prompt}])`
      with OpenAI tool schema, registered in TOOLS, kind="ui".
- [ ] `present_options` returns an action descriptor; frontend
      renders option buttons below the narration.
- [ ] Backend: `/atlas/pilot/stream` accepts optional `history` body
      field — list of `{role: 'user'|'assistant', content: str}` pairs
      preserved across the conversation.
- [ ] Frontend: `PILOT_HISTORY_TURNS` keeps last 6 turns; sent with
      each follow-up question. Manual "new question" clears it.
- [ ] Frontend: option-button click submits the chosen prompt as the
      next question (with history attached).
- [ ] System prompt teaches the agent to use `present_options` for
      EXPLORATORY questions and skip it for specific factual ones.
- [ ] Turn indicator visible: "Turn 3 · 2 prior options chosen".
- [ ] Visible "Start over" button clears history.

## Design Notes

### `present_options` tool schema
```json
{
  "name": "present_options",
  "description": "Present 2-4 clickable next-step choices to the user. Use AFTER you've set up the map and narrated. Each option carries a 'prompt' string that becomes the next user question if clicked.",
  "parameters": {
    "type": "object",
    "properties": {
      "intro": {"type": "string", "description": "1-sentence lead-in like 'What would you like to explore next?'"},
      "options": {
        "type": "array", "minItems": 2, "maxItems": 4,
        "items": {
          "type": "object",
          "properties": {
            "label": {"type": "string", "description": "Short button text (≤6 words)"},
            "prompt": {"type": "string", "description": "The full question to submit if clicked"}
          },
          "required": ["label", "prompt"]
        }
      }
    },
    "required": ["intro", "options"]
  }
}
```

### Conversation history shape (server-side)
`history: [{role, content}, ...]` — same format the OpenAI API
expects. Prepended to messages, after the system prompt:
```python
messages = [{"role": "system", "content": SYSTEM_PROMPT}]
if history:
    messages.extend(history)
messages.append({"role": "user", "content": question})
```
Frontend sends the last 6 turns to keep token cost manageable.

### Frontend option rendering
```html
<div class="pilot-options">
  <button class="opt" data-prompt="..."> · Find wealthiest neighborhoods</button>
  <button class="opt" data-prompt="..."> · See competition for furniture</button>
  ...
</div>
```
Click handler: `askPilot(prompt, /* keep_history= */ true)`.

### System prompt addition
> When a user's question is **exploratory** (open-ended, multi-step,
> like "help me decide where to open a coffee shop"), END your response
> by calling `present_options` with 2-4 next-step choices. Each option
> should be a concrete follow-up question the user might want to ask.
>
> When the question is **specific** (one fact, one comparison), answer
> directly and do NOT call present_options.
>
> Use prior conversation history (passed via messages) to avoid
> repeating tool calls or re-narrating context the user already saw.

### Compact turn history rendering
Above the current turn, show prior exchanges collapsed:
```
[1] "I want to open a chair store in Austin" → Austin's median income … 
[2] "See competition for furniture stores" → ~120 furniture-related …
[Current] "Zoom into South Congress" → You're now at zoom 13 …
```
Click a prior turn to re-expand it.

## Files to Create / Modify

| File | Action |
|---|---|
| `docs/specs/SPEC_081_atlas_pilot_guided_tour.md` | Create |
| `docs/specs/.active_spec` | → SPEC_081 |
| `app/services/atlas/pilot_tools.py` | Add `present_options` UI tool |
| `app/services/atlas/pilot.py` | Accept `history` param; merge into messages; update system prompt |
| `app/api/v1/atlas.py` | `PilotBody` gains `history: Optional[List[Dict]]` |
| `frontend/atlas.html` | Option-button rendering, click handler, turn history UI, history-passed-with-fetch |

## Verification

1. Open `/atlas.html`, ask: `I want to open a chair store in Austin`
2. Agent zooms + activates a layer + narrates + shows 2-4 buttons
3. Click any option → next turn fires with history
4. After 3+ turns, "Start over" clears state

## Feedback History

_No corrections yet._
