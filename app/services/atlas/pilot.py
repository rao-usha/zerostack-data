"""
SPEC_078 Phase B v0 — Atlas Pilot agent loop.

OpenAI function-calling powered. Non-streaming v0 — client waits for
full response. Streaming upgrade is a follow-up.

Flow:
  1. Receive user question
  2. Build system prompt that constrains plan + citation discipline
  3. Loop:
     - Call OpenAI Chat Completions with TOOL_DEFS
     - If `finish_reason='tool_calls'`: execute each tool, append
       results to messages, continue loop
     - If `finish_reason='stop'`: return final narration
  4. Cap at MAX_LOOPS (8) tool-call rounds
  5. Return full transcript: plan, tool_calls, citations, narration

Cost discipline: gpt-4o-mini for v0 (cheap, fast, fine for our tool
surface). Upgrade to gpt-4o for Phase E when narration quality matters.
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from typing import Any, Dict, List, Optional

from sqlalchemy.orm import Session

from typing import Iterator

from app.services.atlas.pilot_tools import TOOL_DEFS, dispatch, is_ui_tool

logger = logging.getLogger(__name__)


MODEL = os.environ.get("ATLAS_PILOT_MODEL", "gpt-4o-mini")
MAX_LOOPS = 8
MAX_TOKENS = 2000


SYSTEM_PROMPT = """\
You are Atlas Pilot, an expert public-data analyst. The user asks a question
about US places (states, counties, census tracts) and you answer using the
provided tools, which read from a governed public-data corpus (Census ACS,
FEMA, FDIC, IRS migration, USAspending, SEC, CBP, EPA, etc.).

You have two kinds of tools:

  READ TOOLS — fetch data so you can reason:
    list_layers, query_place, compare_places, get_recent_events,
    get_migration_flows, cite

  UI TOOLS — pilot the map. The user sees these side effects after your
  narration renders. Use them to show the answer visually, not just describe it:
    plant_focal_node   — drop a focal-node glyph at lat/lon for a NAICS
    zoom_to            — recenter+zoom the map
    toggle_layer       — activate a choropleth or point overlay
    highlight_place    — open a place's panel + fit the map to it

How to work:
1. Plan briefly before calling tools — one paragraph max.
2. Use list_layers if you need to discover what data exists.
3. Use query_place for single-place facts, compare_places for comparisons.
4. Use get_recent_events for "what's new" questions.
5. Use get_migration_flows for migration questions.
6. For questions about a SPECIFIC place: call highlight_place(geo_id) so the user
   sees the place selected on the map.
7. For questions about a SPECIFIC LAYER (income, broadband, federal dollars,
   etc): call toggle_layer(layer_id) so the choropleth shows visually.
8. For questions about a SPECIFIC LOCATION (street address, business idea at
   coords): if the user gave an ADDRESS or PLACE NAME, call geocode_address
   FIRST to resolve to lat/lon — do not guess coordinates from memory. Then
   call plant_focal_node(naics, lat, lon) and zoom_to(lat, lon, 12).
9. EVERY numerical claim in your final answer MUST be backed by a cite() call,
   made BEFORE you write the final narration. A 'numerical claim' is any
   specific number ($73,104; 91.0%; 27 declarations; 304,305 establishments).
   Round numbers like "about 100" don't need cites. Specific ones DO.
10. If a tool returns an error or no data, say so honestly — don't fabricate.
11. Be concise and analyst-grade. Not chat. Specific numbers, specific places.
12. Prefer 5-digit county FIPS over 2-digit state when both apply.

Final answer format: 3-6 sentences of analyst prose. Numerical claims tied
to cite() calls. No emojis. No "Here's what I found:" preamble. DO NOT
write the literal string "cite()" or "(cite())" in your final answer —
the cite() tool call is a side-effect, not text. Just state the claim
naturally; the citation is logged separately.

Common FIPS you can use without looking up:
  06 = California        06037 = Los Angeles County, CA
  17 = Illinois          17031 = Cook County, IL
  36 = New York          36061 = New York County (Manhattan), NY
  48 = Texas             48201 = Harris County (Houston), TX
                          48453 = Travis County (Austin), TX
  51 = Virginia          51013 = Arlington County, VA
  12 = Florida           12086 = Miami-Dade County, FL
  04 = Arizona           04013 = Maricopa County (Phoenix), AZ
  06 = California        06075 = San Francisco County, CA
"""


def _have_openai_key() -> bool:
    return bool(os.environ.get("OPENAI_API_KEY"))


# Patterns that look like specific numerical claims worth citing.
# We deliberately skip small round numbers ("a few", "about 10") which
# don't require citations.
import re as _re

_NUMERIC_PATTERNS = [
    _re.compile(r"\$[\d,]+(?:\.\d+)?[KMBkmb]?"),     # $73,104, $1.5M, $24K
    _re.compile(r"\d+(?:\.\d+)?\s*%"),                # 91.0%, 14%
    _re.compile(r"\b\d{4,}(?:,\d{3})*\b"),            # 24,065 or 109874
    _re.compile(r"\b\d+\.\d{2,}\b"),                  # 99.94
]


def find_unlinked_claims(narration: str, citations: List[Dict[str, str]]) -> List[str]:
    """Return list of specific numeric strings present in the narration
    that don't appear in any cited claim or source.

    Skip-lists (false-positive suppression):
      - plausible years 1900-2099
      - 5-digit county FIPS / 2-digit state FIPS / 11-digit tract FIPS
        that appear in the cited sources (e.g. "for 48201")
      - any number explicitly cited in claim/source text
    """
    cited_text = " ".join(
        (c.get("claim") or "") + " " + (c.get("source") or "")
        for c in citations
    )
    # Polish #1 — pull all FIPS-shaped tokens out of cited sources and skip them.
    # Models commonly cite as "layer for 48201" — that 48201 isn't a claim.
    fips_in_sources = set(_re.findall(r"\b\d{2}\b|\b\d{5}\b|\b\d{11}\b", cited_text))
    # Also pull bare FIPS that appear elsewhere in the narration as identifiers
    # (parenthetical FIPS callouts are not claims).
    paren_fips = set(_re.findall(r"\((\d{2}|\d{5}|\d{11})\)", narration or ""))

    found = set()
    for pat in _NUMERIC_PATTERNS:
        for m in pat.findall(narration or ""):
            # Skip plausible years 1900-2099
            try:
                if "%" not in m and "$" not in m:
                    n_str = m.replace(",", "").replace(".", "")
                    if n_str in fips_in_sources or n_str in paren_fips:
                        continue
                    n = int(n_str)
                    if 1900 <= n <= 2099:
                        continue
            except (ValueError, TypeError):
                pass
            if m not in cited_text:
                found.add(m)
    return sorted(found)


def run_pilot(
    db: Session,
    question: str,
    session_id: Optional[str] = None,
    model: str = MODEL,
) -> Dict[str, Any]:
    """Run one question through the agent. Returns full transcript."""
    started = datetime.utcnow()
    if not _have_openai_key():
        return {
            "error": "OPENAI_API_KEY not set. Atlas Pilot cannot run without "
                     "an LLM API key. See docs/LAUNCH_GUIDE.md for setup.",
            "model_used": model,
            "duration_seconds": 0.0,
        }
    if not question or not question.strip():
        return {"error": "Empty question.", "model_used": model,
                "duration_seconds": 0.0}

    from openai import OpenAI
    client = OpenAI()  # reads OPENAI_API_KEY from env

    messages: List[Dict[str, Any]] = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": question},
    ]
    tool_calls_log: List[Dict[str, Any]] = []
    citations: List[Dict[str, str]] = []
    ui_actions: List[Dict[str, Any]] = []
    final_narration: Optional[str] = None
    loop_n = 0
    truncated = False

    while loop_n < MAX_LOOPS:
        loop_n += 1
        try:
            resp = client.chat.completions.create(
                model=model,
                messages=messages,
                tools=TOOL_DEFS,
                tool_choice="auto",
                max_tokens=MAX_TOKENS,
            )
        except Exception as exc:  # noqa: BLE001
            logger.exception("OpenAI call failed at loop %d", loop_n)
            return {"error": f"LLM call failed: {exc}",
                    "tool_calls": tool_calls_log,
                    "citations": citations,
                    "model_used": model,
                    "duration_seconds": (datetime.utcnow() - started).total_seconds()}

        choice = resp.choices[0]
        msg = choice.message
        finish = choice.finish_reason

        # Append the assistant's message verbatim so subsequent turns can
        # reference any tool_calls it produced.
        messages.append({
            "role": "assistant",
            "content": msg.content,
            "tool_calls": [tc.model_dump() for tc in (msg.tool_calls or [])],
        })

        if finish == "stop" or not msg.tool_calls:
            final_narration = msg.content or ""
            break

        # Execute every tool call this turn requested
        for tc in msg.tool_calls:
            name = tc.function.name
            try:
                args = json.loads(tc.function.arguments or "{}")
            except json.JSONDecodeError:
                args = {}
            result = dispatch(db, name, args)
            tool_calls_log.append({
                "loop": loop_n, "name": name,
                "args": args, "result": result,
            })
            if name == "cite":
                citations.append({
                    "claim": args.get("claim", ""),
                    "source": args.get("source", ""),
                })
            # SPEC_079 — UI tools queue actions the frontend applies post-render
            if is_ui_tool(name) and isinstance(result, dict) and result.get("action"):
                ui_actions.append(result["action"])
            messages.append({
                "role": "tool",
                "tool_call_id": tc.id,
                "content": json.dumps(result, default=str)[:8000],
            })
    else:
        truncated = True

    unlinked = find_unlinked_claims(final_narration or "", citations)
    return {
        "narration": final_narration or "",
        "tool_calls": tool_calls_log,
        "citations": citations,
        "ui_actions": ui_actions,
        "unlinked_claims": unlinked,
        "loops_used": loop_n,
        "truncated": truncated,
        "model_used": model,
        "duration_seconds": (datetime.utcnow() - started).total_seconds(),
    }


# ─────────────────────────────────────────────────────────────────────────────
# SPEC_079 / SPEC_078 v1.5 — NDJSON streaming endpoint
# Same loop, but yields one JSON-line event per state transition so the frontend
# can render progressively. Cuts perceived latency on multi-loop questions.
# ─────────────────────────────────────────────────────────────────────────────

def _summarize_result(result: Any, max_chars: int = 240) -> str:
    """One-line summary of a tool result for the streaming UX."""
    if isinstance(result, dict):
        if "error" in result:
            return f"ERROR: {result['error']}"
        if "action" in result:
            return f"queued UI action: {result['action'].get('name')}"
        keys = list(result.keys())
        s = ", ".join(f"{k}={result[k]}" for k in keys[:3]
                       if not isinstance(result[k], (list, dict)))
        if not s and keys:
            s = f"{keys[0]}={type(result[keys[0]]).__name__}({len(result[keys[0]]) if hasattr(result[keys[0]], '__len__') else '?'})"
        return s[:max_chars]
    return str(result)[:max_chars]


def run_pilot_streaming(
    db: Session,
    question: str,
    session_id: Optional[str] = None,
    model: str = MODEL,
) -> Iterator[str]:
    """Generator that yields NDJSON events (one JSON object per line)
    describing the agent's progress. The frontend reads the stream and
    renders progressively."""
    started = datetime.utcnow()

    def event(kind: str, **payload):
        return json.dumps({"event": kind, **payload}) + "\n"

    if not _have_openai_key():
        yield event("error", message="OPENAI_API_KEY not set.")
        return
    if not question or not question.strip():
        yield event("error", message="Empty question.")
        return

    yield event("plan_started", question=question, model=model)

    from openai import OpenAI
    client = OpenAI()

    messages: List[Dict[str, Any]] = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": question},
    ]
    tool_calls_log: List[Dict[str, Any]] = []
    citations: List[Dict[str, str]] = []
    ui_actions: List[Dict[str, Any]] = []
    loop_n = 0
    truncated = False
    final_narration = ""

    while loop_n < MAX_LOOPS:
        loop_n += 1
        yield event("thinking", loop=loop_n)
        try:
            resp = client.chat.completions.create(
                model=model, messages=messages, tools=TOOL_DEFS,
                tool_choice="auto", max_tokens=MAX_TOKENS,
            )
        except Exception as exc:  # noqa: BLE001
            logger.exception("OpenAI streaming call failed at loop %d", loop_n)
            yield event("error", message=f"LLM call failed: {exc}")
            return

        choice = resp.choices[0]
        msg = choice.message
        finish = choice.finish_reason

        messages.append({
            "role": "assistant", "content": msg.content,
            "tool_calls": [tc.model_dump() for tc in (msg.tool_calls or [])],
        })

        if finish == "stop" or not msg.tool_calls:
            final_narration = msg.content or ""
            yield event("narration", text=final_narration)
            break

        for tc in msg.tool_calls:
            name = tc.function.name
            try:
                args = json.loads(tc.function.arguments or "{}")
            except json.JSONDecodeError:
                args = {}
            yield event("tool_call_started", loop=loop_n, name=name, args=args,
                         is_ui=is_ui_tool(name))
            result = dispatch(db, name, args)
            tool_calls_log.append({"loop": loop_n, "name": name,
                                    "args": args, "result": result})
            if name == "cite":
                citations.append({"claim": args.get("claim", ""),
                                   "source": args.get("source", "")})
            if is_ui_tool(name) and isinstance(result, dict) and result.get("action"):
                ui_actions.append(result["action"])
                yield event("ui_action_queued", action=result["action"])
            yield event("tool_call_completed", loop=loop_n, name=name,
                         summary=_summarize_result(result),
                         is_error="error" in (result if isinstance(result, dict) else {}))
            messages.append({"role": "tool", "tool_call_id": tc.id,
                              "content": json.dumps(result, default=str)[:8000]})
    else:
        truncated = True

    unlinked = find_unlinked_claims(final_narration, citations)
    yield event("done",
                 loops_used=loop_n,
                 tool_count=len(tool_calls_log),
                 citation_count=len(citations),
                 ui_action_count=len(ui_actions),
                 unlinked_claims=unlinked,
                 truncated=truncated,
                 duration_seconds=(datetime.utcnow() - started).total_seconds())
