"""
Tests for SPEC 093 — Demo chain-of-thought (live Pilot reasoning
alongside scripted beats).

Backend: ExplainBody schema, run_explain_streaming event shape,
missing-key fallback, thesis_context plumbing. Frontend: parse
atlas.html for streamCoT, the per-beat prompts, runner wiring,
AbortController plumbing.
"""
import json
import re
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

ATLAS_HTML = Path(__file__).parent.parent / "frontend" / "atlas.html"


@pytest.fixture(scope="module")
def html():
    return ATLAS_HTML.read_text(encoding="utf-8")


# ─── Backend mocks ──────────────────────────────────────────────────────

class _FakeChoice:
    def __init__(self, content):
        self.delta = MagicMock(content=content)


class _FakeChunk:
    def __init__(self, content):
        self.choices = [_FakeChoice(content)]


def _fake_stream_chunks(texts):
    """Yield successive chunks of an LLM response."""
    for t in texts:
        yield _FakeChunk(t)


class _FakeOpenAI:
    """Stand-in for the OpenAI() client in run_explain_streaming."""
    def __init__(self):
        self.chat = MagicMock()
        # Default: yield 3 chunks
        self.chat.completions = MagicMock()
        self.chat.completions.create = MagicMock(
            return_value=_fake_stream_chunks(
                ["Furniture is", " discretionary.", " HHI ≥ $75K is sensible."]
            )
        )


class TestSpec093Backend:

    def test_explain_body_schema(self):
        """T1: ExplainBody requires prompt; thesis_context optional."""
        from app.api.v1.atlas import ExplainBody
        b = ExplainBody(prompt="hi")
        assert b.thesis_context is None
        assert b.max_tokens == 200
        b2 = ExplainBody(
            prompt="x", thesis_context={"industry_label": "Furniture"})
        assert b2.thesis_context["industry_label"] == "Furniture"
        with pytest.raises(Exception):
            ExplainBody()  # prompt required

    def test_run_explain_streaming_yields_delta_events(self, monkeypatch):
        """T2: generator yields started + delta(s) + done with a real
        text payload when an OpenAI stream is available."""
        from app.services.atlas import pilot as pilot_mod
        monkeypatch.setenv("OPENAI_API_KEY", "test-stub")
        with patch("openai.OpenAI", return_value=_FakeOpenAI()):
            events = []
            for line in pilot_mod.run_explain_streaming(
                "Why HHI ≥ $75K for furniture?", thesis_context=None,
                max_tokens=100,
            ):
                events.append(json.loads(line.strip()))
        kinds = [e["event"] for e in events]
        assert kinds[0] == "started"
        assert kinds[-1] == "done"
        deltas = [e for e in events if e["event"] == "delta"]
        assert deltas, "no delta events emitted"
        joined = "".join(d["text"] for d in deltas)
        assert "discretionary" in joined

    def test_run_explain_streaming_no_key(self, monkeypatch):
        """T3: missing OPENAI_API_KEY → graceful done event."""
        from app.services.atlas import pilot as pilot_mod
        monkeypatch.delenv("OPENAI_API_KEY", raising=False)
        events = [json.loads(line.strip())
                   for line in pilot_mod.run_explain_streaming(
                       "anything", thesis_context=None, max_tokens=50)]
        kinds = [e["event"] for e in events]
        assert kinds == ["started", "delta", "done"], kinds
        # And the delta surfaces the soft-fail message, not fake reasoning
        assert "LLM unavailable" in events[1]["text"]

    def test_run_explain_streaming_uses_thesis_block(self, monkeypatch):
        """T4: thesis_context populates the system prompt's <thesis> block.
        We capture the messages list sent to OpenAI."""
        from app.services.atlas import pilot as pilot_mod
        monkeypatch.setenv("OPENAI_API_KEY", "test-stub")
        captured = {}

        def _capture_create(**kwargs):
            captured["messages"] = kwargs.get("messages")
            return _fake_stream_chunks(["ok"])

        fake = _FakeOpenAI()
        fake.chat.completions.create = _capture_create
        with patch("openai.OpenAI", return_value=fake):
            list(pilot_mod.run_explain_streaming(
                "explain", thesis_context={
                    "industry_label": "Furniture stores",
                    "target_hhi_min": 75000,
                }, max_tokens=50))
        sys_msg = captured["messages"][0]["content"]
        assert "<thesis>" in sys_msg
        assert "Furniture stores" in sys_msg
        assert "EXPLAIN" not in sys_msg.upper().split("<THESIS>")[0]
        # The user prompt comes through intact
        assert captured["messages"][1]["content"] == "explain"

    def test_run_explain_streaming_clamps_max_tokens(self, monkeypatch):
        """max_tokens is bounded into [1, 500]."""
        from app.services.atlas import pilot as pilot_mod
        monkeypatch.setenv("OPENAI_API_KEY", "test-stub")
        captured = {}

        def _capture_create(**kwargs):
            captured["max_tokens"] = kwargs.get("max_tokens")
            return _fake_stream_chunks([])

        fake = _FakeOpenAI()
        fake.chat.completions.create = _capture_create
        with patch("openai.OpenAI", return_value=fake):
            # Too-large
            list(pilot_mod.run_explain_streaming("x", max_tokens=99999))
            assert captured["max_tokens"] == 500
            # Too-small
            list(pilot_mod.run_explain_streaming("x", max_tokens=0))
            assert captured["max_tokens"] == 200


class TestSpec093Frontend:

    def test_stream_cot_present(self, html):
        """T5: streamCoT helper declared."""
        assert "async function streamCoT" in html
        # It POSTs to /atlas/explain
        assert "/explain" in html
        # And appends to #pilot-thread as an assistant.cot message
        assert "msg.assistant.cot" in html or "'msg assistant cot'" in html

    def test_runner_calls_stream_cot_per_beat(self, html):
        """T6: runFurnitureStoryDemo invokes beatWithCoT 6 times.
        beatWithCoT internally calls streamCoT with the per-beat prompt."""
        m = re.search(r"async function runFurnitureStoryDemo\(\).*?\n  \}",
                      html, re.DOTALL)
        assert m, "runFurnitureStoryDemo body not found"
        body = m.group(0)
        assert body.count("await beatWithCoT(") == 6
        # The beat-prompt array is declared
        assert "COT_BEAT_PROMPTS" in html
        # The labels too
        assert "COT_BEAT_LABELS" in html

    def test_cot_chat_thread_target(self, html):
        """T7: streamCoT appends to #pilot-thread (assistant msg, no user
        bubble)."""
        m = re.search(r"async function streamCoT\(.*?\n  \}", html, re.DOTALL)
        assert m, "streamCoT body not found"
        body = m.group(0)
        assert "pilot-thread" in body
        # No user bubble built — the message is assistant-only.
        assert "msg user" not in body
        # The msg uses the cot class
        assert "cot" in body

    def test_cot_abort_on_skip(self, html):
        """T8: AbortController + COT_ABORTS array, skipStory aborts."""
        assert "const COT_ABORTS" in html
        assert "function abortAllCoT" in html
        # skipStory + cancelAnyRunningStory both call abortAllCoT
        for fn in ("function skipStory", "function cancelAnyRunningStory"):
            m = re.search(re.escape(fn) + r"\(\).*?\n  \}", html, re.DOTALL)
            assert m, f"{fn} not found"
            assert "abortAllCoT" in m.group(0), \
                f"{fn} does not abort CoT fetches"

    def test_cot_css_class_styled(self, html):
        """The .msg.assistant.cot class has a distinct style."""
        assert ".msg.assistant.cot" in html
        # A 💭 lead-in icon hints "this is reasoning"
        assert "💭" in html
