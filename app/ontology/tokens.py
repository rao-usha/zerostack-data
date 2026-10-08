"""Per-model token counting for estimates and reservations (SPEC_164).

Every vendor tokenizes the same text differently (PLAN_100 §4.1), so counts
are per model, chosen by the registry's ``tokenizer`` field:

* ``tiktoken:<encoding>`` — OpenAI family, when ``tiktoken`` is installed
  (it may fetch the encoding file once; it never calls a model).
* ``hf:<path to tokenizer.json>`` — open weights, when ``tokenizers`` is
  installed and the file exists locally (never downloaded here).
* ``remote:<vendor>`` — a provider count endpoint; only the async
  ``count_tokens_remote`` uses it, and only for adapters that implement
  ``count_tokens`` (Anthropic today).
* ``fallback`` — ``ceil(chars / 4)``, flagged ``exact=False`` with the ±30%
  band, which the budget guard applies to the worst case.
"""
from __future__ import annotations

import logging
import math
import os
from dataclasses import dataclass
from functools import lru_cache
from typing import Any, Iterable, Mapping, Optional

from app.ontology.pricing import TOKENIZER_BAND

logger = logging.getLogger(__name__)

CHARS_PER_TOKEN = 4.0


@dataclass(frozen=True)
class TokenCount:
    tokens: int
    method: str
    exact: bool
    band: float  # 0.0 for exact counts, TOKENIZER_BAND for the fallback


def fallback_count(text: str) -> TokenCount:
    return TokenCount(int(math.ceil(len(text) / CHARS_PER_TOKEN)), "fallback", False, TOKENIZER_BAND)


@lru_cache(maxsize=8)
def _tiktoken_encoding(name: str):
    try:
        import tiktoken  # type: ignore
    except ImportError:
        return None
    try:
        return tiktoken.get_encoding(name)
    except Exception as e:  # pragma: no cover - environment dependent
        logger.warning("[ontology] tiktoken encoding %s unavailable: %s", name, e)
        return None


@lru_cache(maxsize=8)
def _hf_tokenizer(path: str):
    if not os.path.isfile(path):
        return None
    try:
        from tokenizers import Tokenizer  # type: ignore
    except ImportError:
        return None
    return Tokenizer.from_file(path)


def count_tokens(text: str, spec: Any) -> TokenCount:
    """Count ``text`` with the model's own counter, or fall back with the band flag."""
    tok = (getattr(spec, "tokenizer", None) or "fallback").strip()
    if tok.startswith("tiktoken:"):
        enc = _tiktoken_encoding(tok.split(":", 1)[1])
        if enc is not None:
            return TokenCount(len(enc.encode(text)), tok, True, 0.0)
    elif tok.startswith("hf:"):
        tk = _hf_tokenizer(tok.split(":", 1)[1])
        if tk is not None:
            return TokenCount(len(tk.encode(text).ids), tok, True, 0.0)
    return fallback_count(text)


def messages_text(messages: Iterable[Mapping[str, Any]]) -> str:
    parts = []
    for m in messages:
        content = m.get("content")
        if isinstance(content, str):
            parts.append(content)
        elif isinstance(content, list):
            parts.extend(str(p.get("text", "")) for p in content if isinstance(p, Mapping))
    return "\n".join(parts) if len(parts) > 1 else (parts[0] if parts else "")


def count_messages(messages: Iterable[Mapping[str, Any]], spec: Any) -> TokenCount:
    return count_tokens(messages_text(list(messages)), spec)


async def count_tokens_remote(adapter: Any, messages, schema: Optional[dict] = None) -> TokenCount:
    """Use the provider's count endpoint when the adapter has one (network call; not used in tests)."""
    counter = getattr(adapter, "count_tokens", None)
    if counter is not None:
        n = await counter(messages, schema)
        return TokenCount(int(n), f"remote:{adapter.spec.provider}", True, 0.0)
    return count_messages(messages, adapter.spec)
