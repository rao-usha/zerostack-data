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

from app.services.atlas.pilot_tools import TOOL_DEFS, dispatch

logger = logging.getLogger(__name__)


MODEL = os.environ.get("ATLAS_PILOT_MODEL", "gpt-4o-mini")
MAX_LOOPS = 8
MAX_TOKENS = 2000


SYSTEM_PROMPT = """\
You are Atlas Pilot, an expert public-data analyst. The user asks a question
about US places (states, counties, census tracts) and you answer using the
provided tools, which read from a governed public-data corpus (Census ACS,
FEMA, FDIC, IRS migration, USAspending, SEC, CBP, EPA, etc.).

How to work:
1. Plan briefly before calling tools — one paragraph max.
2. Use list_layers if you need to discover what data exists.
3. Use query_place for single-place facts, compare_places for comparisons.
4. Use get_recent_events for "what's new" questions.
5. Use get_migration_flows for migration questions.
6. EVERY numerical claim in your final answer MUST be backed by a cite() call.
   Make the cite() call right before stating the claim in your final narration.
7. If a tool returns an error or no data, say so honestly — don't fabricate.
8. Be concise and analyst-grade. Not chat. Specific numbers, specific places.
9. Prefer 5-digit county FIPS over 2-digit state when both apply.

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
            messages.append({
                "role": "tool",
                "tool_call_id": tc.id,
                "content": json.dumps(result, default=str)[:8000],
            })
    else:
        truncated = True

    return {
        "narration": final_narration or "",
        "tool_calls": tool_calls_log,
        "citations": citations,
        "loops_used": loop_n,
        "truncated": truncated,
        "model_used": model,
        "duration_seconds": (datetime.utcnow() - started).total_seconds(),
    }
