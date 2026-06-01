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
   NEVER guess a layer_id. Valid ids come ONLY from list_layers / the
   registry (e.g. demo_acs_median_income, demo_tract_median_income,
   demo_tract_population, infra_broadband_subscription, disaster_nri).
   If you are not 100% sure of the exact id, call list_layers FIRST and
   use an id from that response. A wrong id wastes a turn on an error.
8. For questions about a SPECIFIC LOCATION (street address, business idea at
   coords): if the user gave an ADDRESS or PLACE NAME, call geocode_address
   FIRST to resolve to lat/lon — do not guess coordinates from memory. Then
   call plant_focal_node(naics, lat, lon) and zoom_to(lat, lon, 12).
9. EVERY numerical claim in your final answer MUST be backed by a cite() call,
   made BEFORE you write the final narration. A 'numerical claim' is any
   specific number ($73,104; 91.0%; 27 declarations; 304,305 establishments).
   Round numbers like "about 100" don't need cites. Specific ones DO.
9a. SPEC_081 — GUIDED-TOUR MODE (HARD REQUIREMENT). When the user's question
    is EXPLORATORY — open-ended, multi-step, business-idea-flavored, like
      "I want to open a chair store in Austin"
      "help me think about supply chain in Texas"
      "walk me through demographics here"
      "where should I open a coffee shop"
    you MUST:
      1) Mutate the map: zoom_to() the relevant area AND toggle_layer() a
         relevant choropleth (median income, population, broadband, NRI…),
      2) Narrate what's now on screen in 2-3 sentences,
      3) ALWAYS finish by calling present_options() with 2-4 concrete
         next-step choices. Each option.prompt must be a complete, specific
         follow-up question (NOT a question the user asks; a question the
         user would *want* answered next).
    Do NOT reply conversationally ("Would you like to start by looking at…")
    — that pattern is FORBIDDEN. Use present_options() instead. Conversational
    questions in narration text waste a turn.
    SKIP present_options ONLY when the question is SPECIFIC and answerable
    in one shot ("what is median income for Travis County?", "compare A and
    B on Z"). When in doubt, USE present_options.

    EXAMPLE — exploratory flow:
      user: "I want to open a chair store in Austin"
      → call zoom_to(lat=30.27, lon=-97.74, zoom=10)
      → call toggle_layer(layer_id="demo_acs_median_income")
      → call cite(...)  for any specific numbers you'll mention
      → call present_options(
          intro="Where would you like to dig in next?",
          options=[
            {"label":"Find wealthiest neighborhoods",
             "prompt":"Show me Austin tracts with median income above $150K"},
            {"label":"See furniture-store competition",
             "prompt":"Where are existing furniture stores in Austin?"},
            {"label":"Look at age demographics",
             "prompt":"Activate the population layer for Austin"},
            {"label":"Plant a focal node at a specific spot",
             "prompt":"Plant a chair-store focal node on South Congress"},
          ])
      → final narration: 2-3 sentences about what's on screen.
    CRITICAL: the option choices live ONLY inside present_options().
    Your final narration text MUST NOT contain an options list, bullet
    points of choices, or a trailing "What would you like to explore
    next?" line. Those are rendered as clickable buttons from the tool
    call — repeating them as text looks broken. End the narration on a
    statement about what's on screen, not a question with bullets.
9b. When the conversation has HISTORY (prior turns visible in the messages),
    USE that context: don't re-narrate things the user already saw, don't
    re-call tools you've already called this conversation. Reference prior
    state ("As we saw, Austin's median income…").
9c. SPEC_090 — DECISION MAP TOOLS. The map operates as a Decision Map:
    a thesis fits a 0-100 score across counties, pill chips filter the
    candidate set, top-N pins surface recommendations, and a trade-area
    card drills into a candidate. You have these tools to drive it:
      * recommend_candidates(top_n) — read tool. Returns the top-N
        candidates by current thesis fit. Use INSTEAD of toggle_layer
        when the user asks "where should I open X?" or "which areas
        fit best?"
      * add_constraint(dimension, value) — UI. Push a chip when the
        user states a must-have ("only above $80K" →
        add_constraint("hhi_min", 80000); "avoid hurricane risk" →
        add_constraint("exclude_nri", 50)). Valid dimensions:
        hhi_min, hhi_max, establishments_min, broadband_min, exclude_nri.
      * remove_constraint(dimension) — UI. Pop a chip.
      * enter_trade_area(geo_id, radius_mi=50) — UI. After
        recommend_candidates, drill into the top pick.
      * exit_trade_area() — UI. Close the trade-area card.
    Prefer this chain — add_constraint(s) → recommend_candidates →
    enter_trade_area — over manually toggling individual layers when
    the user is in a site-selection conversation.
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


def find_unlinked_claims(narration: str, citations: List[Dict[str, str]],
                          question: str = "") -> List[str]:
    """Return list of specific numeric strings present in the narration
    that don't appear in any cited claim or source.

    Skip-lists (false-positive suppression):
      - plausible years 1900-2099
      - 5-digit county FIPS / 2-digit state FIPS / 11-digit tract FIPS
        that appear in the cited sources (e.g. "for 48201")
      - any number explicitly cited in claim/source text
      - SPEC_085: any numeric token the USER typed in their question
        (e.g. "income > $150K") — echoing a threshold the user gave is
        not a data claim that needs a citation.
    """
    cited_text = " ".join(
        (c.get("claim") or "") + " " + (c.get("source") or "")
        for c in citations
    )
    # SPEC_085 — numbers the user supplied in the question are not claims.
    # Capture each token AND its K/M/B-expanded forms, because the model
    # often reformats "$150K" → "$150,000" in the narration.
    q_numbers: set = set()
    for tok in _re.findall(r"\$?\s*[\d,.]+\s*[KMB%]?", question or ""):
        tok = tok.strip()
        if not tok:
            continue
        q_numbers.add(tok)
        q_numbers.add(tok.replace(" ", ""))
        mult = {"K": 1_000, "M": 1_000_000, "B": 1_000_000_000}
        msuf = _re.match(r"\$?\s*([\d,.]+)\s*([KMB])\b", tok, _re.IGNORECASE)
        if msuf:
            try:
                base = float(msuf.group(1).replace(",", ""))
                val = int(base * mult[msuf.group(2).upper()])
                dollar = "$" if "$" in tok else ""
                q_numbers.add(f"{dollar}{val:,}")     # $150,000
                q_numbers.add(f"{dollar}{val}")        # $150000
                q_numbers.add(f"{val:,}")              # 150,000
                q_numbers.add(str(val))                # 150000
            except (ValueError, KeyError):
                pass
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
            # SPEC_085 — skip numbers the user supplied in the question.
            m_norm = m.replace(" ", "")
            if (m in q_numbers or m_norm in {q.replace(" ", "") for q in q_numbers}):
                continue
            if m not in cited_text:
                found.add(m)
    return sorted(found)


# SPEC_085 — strip a trailing options block the model sometimes appends
# to the narration ("What would you like to explore next?\n- a\n- b").
# The choices are rendered as clickable buttons from present_options;
# repeating them as text reads as broken.
_OPTIONS_TAIL = _re.compile(
    r"\n+\s*(?:what would you like[^\n]*|where would you like[^\n]*|"
    r"what(?:'|’)s next[^\n]*|what next[^\n]*|next steps?[^\n]*|"
    r"here are some (?:options|next steps)[^\n]*)\s*"
    r"(?:\n\s*(?:[-*•]|\d+[.)]).*)+\s*$",
    _re.IGNORECASE,
)


def _strip_options_block(text: Optional[str]) -> str:
    """Remove a trailing 'What would you like next?\\n- bullets' block."""
    if not text:
        return text or ""
    return _OPTIONS_TAIL.sub("", text).rstrip()


# SPEC_081 — exploratory-question heuristic. We use this to force a
# present_options call when the agent forgot to make one.
_EXPLORATORY_PATTERNS = (
    r"\bi want to\b", r"\bi'd like to\b", r"\bi am thinking\b",
    r"\bhelp me\b", r"\bwalk me through\b", r"\bshow me around\b",
    r"\bwhere should i\b", r"\bwhat should i\b", r"\bguide me\b",
    r"\btell me about\b", r"\bexplore\b", r"\bget started\b",
    r"\bopen a\b", r"\bstart a\b", r"\blooking to\b",
)


# SPEC_082 — thesis-context sanitiser. The frontend may send anything;
# we accept only known scalar fields, truncate strings, coerce ints, and
# drop everything else. Result is rendered as a compact <thesis> block
# prepended to the system prompt only when at least one field survives.
_THESIS_FIELDS: Dict[str, Dict[str, Any]] = {
    "industry_label":          {"type": "str", "max": 200},
    "industry_naics":          {"type": "str", "max": 8},
    "target_hhi_min":          {"type": "int", "min": 0, "max": 10_000_000},
    "target_hhi_max":          {"type": "int", "min": 0, "max": 10_000_000},
    "target_pop_density_min":  {"type": "int", "min": 0, "max": 1_000_000},
    "target_age_band":         {"type": "str", "max": 16},
    "notes":                   {"type": "str", "max": 800},
    # SPEC_084 — new fields exposed through the Notion-blocks form
    "region":                  {"type": "str", "max": 80},
    "exclude_layers":          {"type": "str", "max": 200},
}


def _sanitize_thesis(raw: Any) -> Dict[str, Any]:
    if not isinstance(raw, dict):
        return {}
    clean: Dict[str, Any] = {}
    for k, spec in _THESIS_FIELDS.items():
        v = raw.get(k)
        if v is None or v == "":
            continue
        if spec["type"] == "str":
            if not isinstance(v, str):
                continue
            v = v.strip()[: spec["max"]]
            if v:
                clean[k] = v
        elif spec["type"] == "int":
            try:
                iv = int(v)
            except (TypeError, ValueError):
                continue
            lo, hi = spec.get("min", 0), spec.get("max", 10**9)
            if lo <= iv <= hi:
                clean[k] = iv
    return clean


def _format_thesis_block(thesis: Optional[Dict[str, Any]]) -> str:
    """Render a sanitised thesis as a `<thesis>...</thesis>` block to
    prepend to the system prompt. Returns '' when nothing usable."""
    t = _sanitize_thesis(thesis)
    if not t:
        return ""
    lines: List[str] = []
    if t.get("industry_label") or t.get("industry_naics"):
        label = t.get("industry_label") or "(unspecified)"
        naics = t.get("industry_naics")
        lines.append(f"Industry: {label}" + (f" (NAICS {naics})" if naics else ""))
    if t.get("target_hhi_min") is not None or t.get("target_hhi_max") is not None:
        lo, hi = t.get("target_hhi_min"), t.get("target_hhi_max")
        if lo is not None and hi is not None:
            lines.append(f"Target HHI: ${lo:,} – ${hi:,}")
        elif lo is not None:
            lines.append(f"Target HHI: ${lo:,}+")
        else:
            lines.append(f"Target HHI: up to ${hi:,}")
    if t.get("target_pop_density_min") is not None:
        lines.append(f"Min population density: {t['target_pop_density_min']:,} /km²")
    if t.get("target_age_band"):
        lines.append(f"Target age band: {t['target_age_band']}")
    if t.get("region"):
        lines.append(f"Region focus: {t['region']}")
    if t.get("exclude_layers"):
        lines.append(f"Hide layers: {t['exclude_layers']}")
    if t.get("notes"):
        lines.append(f"Notes: {t['notes']}")
    if not lines:
        return ""
    return "<thesis>\n" + "\n".join(lines) + "\n</thesis>\n\n"


def is_exploratory(question: str) -> bool:
    if not question:
        return False
    q = question.strip().lower()
    if len(q.split()) < 4:
        return False  # specific terse queries
    import re as _re
    return any(_re.search(p, q) for p in _EXPLORATORY_PATTERNS)


def _force_present_options(client, model, messages, max_tokens):
    """SPEC_081 — one extra LLM call with tool_choice forcing
    present_options. Used when the agent stops without calling it for
    an exploratory question. Returns (ui_action, tool_call_log_entry)
    or (None, None) on failure."""
    # OpenAI rejects assistant messages with an empty tool_calls array.
    # Drop the field when empty so the API accepts the history.
    cleaned: List[Dict[str, Any]] = []
    for m in messages:
        if (m.get("role") == "assistant"
                and isinstance(m.get("tool_calls"), list)
                and not m["tool_calls"]):
            mm = dict(m)
            mm.pop("tool_calls", None)
            cleaned.append(mm)
        else:
            cleaned.append(m)
    forced_messages = cleaned + [{
        "role": "user",
        "content": ("Before finishing, call present_options(intro, options) "
                    "with 2-4 concrete next-step questions the user might "
                    "want to ask. Do not write a narration; just call the "
                    "tool."),
    }]
    try:
        resp = client.chat.completions.create(
            model=model,
            messages=forced_messages,
            tools=[t for t in TOOL_DEFS
                    if t.get("function", {}).get("name") == "present_options"],
            tool_choice={"type": "function",
                         "function": {"name": "present_options"}},
            max_tokens=max_tokens,
        )
        msg = resp.choices[0].message
        if not msg.tool_calls:
            return None, None
        tc = msg.tool_calls[0]
        try:
            args = json.loads(tc.function.arguments or "{}")
        except json.JSONDecodeError:
            return None, None
        return args, {"loop": "forced", "name": "present_options",
                       "args": args}
    except Exception:  # noqa: BLE001
        logger.exception("forced present_options call failed")
        return None, None


def run_pilot(
    db: Session,
    question: str,
    session_id: Optional[str] = None,
    model: str = MODEL,
    history: Optional[List[Dict[str, Any]]] = None,
    thesis_context: Optional[Dict[str, Any]] = None,
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

    # SPEC_082 — thesis block is prepended to system prompt when populated.
    # SPEC_081 — prepend conversation history so multi-turn guided tours
    # share context (prior tool calls, narrations, chosen options).
    sys_content = _format_thesis_block(thesis_context) + SYSTEM_PROMPT
    messages: List[Dict[str, Any]] = [{"role": "system", "content": sys_content}]
    if history:
        # Sanitize: only role + content; cap to last 6 turns
        for turn in history[-12:]:
            if isinstance(turn, dict) and turn.get("role") in ("user", "assistant"):
                messages.append({"role": turn["role"],
                                  "content": str(turn.get("content", ""))[:4000]})
    messages.append({"role": "user", "content": question})
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
            # SPEC_090 — inject thesis_context into the Decision Map
            # read tool so it can score against the user's current thesis
            # without the model having to forward it.
            if name == "recommend_candidates" and thesis_context is not None \
                    and "thesis_context" not in args:
                args["thesis_context"] = thesis_context
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

    # SPEC_081 — if the question is exploratory and the agent finished
    # without calling present_options, force a follow-up tool call.
    has_options = any(a.get("name") == "present_options" for a in ui_actions)
    if not has_options and is_exploratory(question):
        forced_args, forced_log = _force_present_options(
            client, model, messages, MAX_TOKENS)
        if forced_args:
            result = dispatch(db, "present_options", forced_args)
            if isinstance(result, dict) and result.get("action"):
                ui_actions.append(result["action"])
            if forced_log:
                forced_log["result"] = result
                tool_calls_log.append(forced_log)

    # SPEC_085 — drop any options block the model appended to narration,
    # and skip question-echoed numbers when flagging unlinked claims.
    final_narration = _strip_options_block(final_narration)
    unlinked = find_unlinked_claims(final_narration or "", citations,
                                     question=question)
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
    history: Optional[List[Dict[str, Any]]] = None,
    thesis_context: Optional[Dict[str, Any]] = None,
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

    # SPEC_082 — thesis block prepended to system prompt (only if populated).
    # SPEC_081 — prepend conversation history so multi-turn guided tours
    # share context (prior tool calls, narrations, chosen options).
    sys_content = _format_thesis_block(thesis_context) + SYSTEM_PROMPT
    messages: List[Dict[str, Any]] = [{"role": "system", "content": sys_content}]
    if history:
        # Sanitize: only role + content; cap to last 6 turns
        for turn in history[-12:]:
            if isinstance(turn, dict) and turn.get("role") in ("user", "assistant"):
                messages.append({"role": turn["role"],
                                  "content": str(turn.get("content", ""))[:4000]})
    messages.append({"role": "user", "content": question})
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
            # SPEC_085 — strip any options block before showing narration
            final_narration = _strip_options_block(msg.content or "")
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

    # SPEC_081 — force present_options if exploratory question missed it
    has_options = any(a.get("name") == "present_options" for a in ui_actions)
    if not has_options and is_exploratory(question):
        forced_args, forced_log = _force_present_options(
            client, model, messages, MAX_TOKENS)
        if forced_args:
            result = dispatch(db, "present_options", forced_args)
            if isinstance(result, dict) and result.get("action"):
                ui_actions.append(result["action"])
                yield event("ui_action_queued", action=result["action"])
            if forced_log:
                forced_log["result"] = result
                tool_calls_log.append(forced_log)

    unlinked = find_unlinked_claims(final_narration, citations,
                                     question=question)
    yield event("done",
                 loops_used=loop_n,
                 tool_count=len(tool_calls_log),
                 citation_count=len(citations),
                 ui_action_count=len(ui_actions),
                 unlinked_claims=unlinked,
                 truncated=truncated,
                 duration_seconds=(datetime.utcnow() - started).total_seconds())
