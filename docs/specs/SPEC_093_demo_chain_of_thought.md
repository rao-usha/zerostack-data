# SPEC 093 — Demo chain-of-thought (live Pilot reasoning alongside beats)

**Status:** Draft
**Task type:** service (backend + frontend)
**Date:** 2026-06-01
**Plan:** PLAN_076_demo_chain_of_thought
**Test file:** tests/test_spec_093_demo_chain_of_thought.py
**Builds on:** SPEC_086 (Cursor chat) + SPEC_092 (story demo)

## Goal

Keep SPEC_092's 6 scripted beats and their timing. Layer in **real
LLM reasoning** — for each beat, the Pilot streams a 2-3 sentence
rationale into the chat thread (right column) explaining WHY the
scripted action makes sense for the active thesis. The chat thread
becomes the demo's reasoning narrative.

## User decisions (locked)

- Live Pilot CoT alongside scripted beats (Option B from PLAN_076)
- Reasoning renders in the chat thread only
- Fresh LLM call every demo run (no caching for v1)

## Acceptance Criteria

- [ ] New backend endpoint `POST /atlas/explain` streams narration-only
      events (NDJSON: `started`, `delta`, `done`/`error`). No tool
      dispatching, no `present_options`, no map mutations.
- [ ] `app/services/atlas/pilot.py` gains `run_explain_streaming(
      prompt, thesis_context, max_tokens=200)` using OpenAI's
      Chat Completions stream mode. ~6 calls per demo run.
- [ ] Frontend `streamCoT(prompt, thesis)` helper:
      - Appends an assistant-only message to `#pilot-thread`
        (no user bubble — this is internal reasoning narration).
      - Streams text chunks into it as deltas arrive.
      - Returns a promise that resolves when `done` fires.
- [ ] The 6-beat runner (`runFurnitureStoryDemo`) fires `streamCoT`
      for each beat in parallel with the scripted action, with a
      4-second timeout ceiling so the demo never stalls.
- [ ] Each beat has a focused prompt. The 6 prompts are crafted to
      produce ~60-word rationales tied to the furniture-store thesis.
- [ ] Cancellation: Skip ▶︎ aborts in-flight CoT fetches via
      AbortController (same pattern as askPilot).
- [ ] No regressions to the SPEC_092 scripted beats — every visible
      action still fires whether the LLM is available or not.

## Test Cases

| ID | Test | Verifies |
|----|------|----------|
| T1 | test_explain_body_schema | ExplainBody requires prompt; thesis_context optional |
| T2 | test_run_explain_streaming_yields_delta_events | with mocked OpenAI stream, generator yields started + delta(s) + done |
| T3 | test_run_explain_streaming_no_key | missing OPENAI_API_KEY → graceful done event |
| T4 | test_run_explain_streaming_uses_thesis_block | thesis_context populates the system prompt's `<thesis>` block |
| T5 | test_frontend_stream_cot_present | atlas.html declares `function streamCoT` |
| T6 | test_frontend_runner_calls_stream_cot_per_beat | runFurnitureStoryDemo invokes streamCoT 6× with the per-beat prompts |
| T7 | test_frontend_cot_chat_thread_target | streamCoT appends to `#pilot-thread` as assistant message (no user bubble) |
| T8 | test_frontend_cot_abort_on_skip | AbortController wired so skipStory cancels in-flight fetch |

## Design

### Backend

```python
# app/services/atlas/pilot.py — additions

EXPLAIN_SYSTEM_PROMPT = """\
You are a concise site-selection analyst inside a Decision Map app.
Your job: 2-3 sentences of analyst-grade reasoning. Honest, specific,
no hedging. No emojis. No "Sure!" / "Great question!" preambles —
just the reasoning. The user's investment thesis is in <thesis> if set.
"""

def run_explain_streaming(prompt: str,
                           thesis_context: Optional[Dict[str, Any]] = None,
                           max_tokens: int = 200,
                           model: str = MODEL) -> Iterator[str]:
    started = datetime.utcnow()
    def event(kind, **payload):
        return json.dumps({"event": kind, **payload}) + "\n"
    yield event("started")
    if not _have_openai_key():
        yield event("delta", text="(LLM unavailable — reasoning skipped)")
        yield event("done", duration_seconds=0)
        return
    sys_content = _format_thesis_block(thesis_context) + EXPLAIN_SYSTEM_PROMPT
    messages = [
      {"role": "system", "content": sys_content},
      {"role": "user", "content": prompt[:2000]},
    ]
    try:
        from openai import OpenAI
        client = OpenAI()
        stream = client.chat.completions.create(
            model=model, messages=messages, max_tokens=max_tokens, stream=True,
        )
        for chunk in stream:
            delta = chunk.choices[0].delta.content
            if delta:
                yield event("delta", text=delta)
        yield event("done",
                     duration_seconds=(datetime.utcnow()-started).total_seconds())
    except Exception as exc:
        yield event("error", message=str(exc))
```

```python
# app/api/v1/atlas.py — additions

class ExplainBody(BaseModel):
    prompt: str = Field(..., min_length=1, max_length=2000)
    thesis_context: Optional[Dict[str, Any]] = None
    max_tokens: Optional[int] = 200

@router.post("/explain")
def atlas_explain(body: ExplainBody, db: Session = Depends(get_db)):
    from app.services.atlas.pilot import run_explain_streaming
    from fastapi.responses import StreamingResponse
    return StreamingResponse(
        run_explain_streaming(body.prompt, body.thesis_context,
                                body.max_tokens or 200),
        media_type="application/x-ndjson",
        headers={"X-Accel-Buffering": "no", "Cache-Control": "no-cache"},
    )
```

### Frontend

```js
// New helper
async function streamCoT(prompt, thesis, abortSignal) {
  // Append an assistant-only message (no user bubble) and grow it as
  // delta chunks arrive. Returns when {event:"done"} fires or aborted.
  const thread = document.getElementById('pilot-thread');
  const empty = document.getElementById('pilot-empty');
  if (empty) empty.remove();
  const msg = document.createElement('div');
  msg.className = 'msg assistant cot';
  const narr = document.createElement('div');
  narr.className = 'narration';
  msg.appendChild(narr);
  thread.appendChild(msg);
  thread.scrollTop = thread.scrollHeight;
  try {
    const res = await fetch(API + '/explain', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ prompt, thesis_context: thesis || null }),
      signal: abortSignal,
    });
    const reader = res.body.getReader();
    const dec = new TextDecoder();
    let buf = '';
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buf += dec.decode(value, { stream: true });
      let nl;
      while ((nl = buf.indexOf('\n')) >= 0) {
        const line = buf.slice(0, nl).trim();
        buf = buf.slice(nl + 1);
        if (!line) continue;
        const ev = JSON.parse(line);
        if (ev.event === 'delta') {
          narr.textContent += ev.text;
          thread.scrollTop = thread.scrollHeight;
        } else if (ev.event === 'done') return;
        else if (ev.event === 'error') {
          narr.textContent += `\n(reasoning failed: ${ev.message})`;
          return;
        }
      }
    }
  } catch (e) {
    if (e.name !== 'AbortError') console.warn('streamCoT failed', e);
  }
}

// In runFurnitureStoryDemo — before each scripted action:
const ctrl = new AbortController();
COT_ABORTS.push(ctrl);   // collect so Skip can abort them all
await Promise.race([
  streamCoT(beatPrompts[step], FURNITURE_DEMO, ctrl.signal),
  sleep(4500),
]);
```

### Beat prompts (hardcoded for v1; will become Plan-driven in PLAN_076 Phase D)

1. "Setting the stage: this is a furniture-store thesis in the Austin TX metro (NAICS 442110). In 2-3 sentences, explain what drives site selection for this kind of business."
2. "I'm filtering to counties with median household income ≥ $75K. In 2-3 sentences, explain why this income threshold matters for furniture retail."
3. "I'm excluding counties with NRI > 50 (high natural-hazard risk). In 2-3 sentences, explain why this filter matters for a retail thesis."
4. "I'm scoring counties as a 55/30/15 weighted blend of median income, commercial activity (CBP establishments), and broadband. In 2-3 sentences, explain why this blend fits furniture retail."
5. "The top fit-scoring counties are Loudoun VA (70), LA (67), Santa Clara (67), Fairfax VA (63), San Mateo (63). In 2-3 sentences, explain what these counties have in common and what the #1 pick tells us."
6. "I'm opening a 50-mile trade-area view around the top candidate. In 2-3 sentences, explain why trade-area analysis matters and what we'll learn from the neighbour counties."

### CSS

`.msg.assistant.cot` — same as regular assistant but with a subtle
`💭` lead-in icon and slightly muted color to distinguish system
narration from human-question-driven Pilot replies.

## Rubric Checklist

- [ ] Async-safe (no blocking calls)
- [ ] Bounded (prompt ≤2000 chars; max_tokens ≤500)
- [ ] No SQL
- [ ] No PII
- [ ] Honest defaults — LLM-unavailable path narrates that, no fake reasoning
- [ ] Tests cover happy path + abort + missing key

## Files

| File | Action |
|------|--------|
| `app/services/atlas/pilot.py` | Add EXPLAIN_SYSTEM_PROMPT + run_explain_streaming |
| `app/api/v1/atlas.py` | ExplainBody + POST /atlas/explain |
| `frontend/atlas.html` | streamCoT helper, COT_ABORTS array, runner per-beat invocations, CSS .msg.assistant.cot |
| `tests/test_spec_093_demo_chain_of_thought.py` | T1-T8 |

## Verification

1. Click ⚡ Demo. The chat thread (right column) fills up with 6
   streamed Pilot rationales, one per beat, each ~2-3 sentences.
2. Cancel mid-demo (Skip ▶︎) → in-flight LLM stream is aborted; the
   partial message stays in the chat. Subsequent beats don't fire.
3. Re-run → new fresh LLM calls; previous turn stays in scrollback.
4. With OPENAI_API_KEY unset → demo still runs all visible actions;
   chat shows `(LLM unavailable — reasoning skipped)` per beat.

## Feedback History

- 2026-06-01 — Driving feedback: *"I would like to see as part of
  this is an automated chain of thought reasoning."*
