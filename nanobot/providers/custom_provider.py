"""Direct OpenAI-compatible provider — bypasses LiteLLM."""

from __future__ import annotations

import uuid
from typing import Any

import json_repair
from openai import AsyncOpenAI

from nanobot.providers.base import LLMProvider, LLMResponse, ToolCallRequest

# Optional: if llm_utils.py is present in this package (e.g. internal deployment
# with custom RSA request signing), use its async http client automatically.
try:
    from nanobot.providers.llm_utils import get_token as _get_token
except ImportError:
    _get_token = None  # type: ignore[assignment]


class CustomProvider(LLMProvider):

    def __init__(self, api_key: str = "no-key", api_base: str = "http://localhost:8000/v1", default_model: str = "default"):
        super().__init__(api_key, api_base)
        self.default_model = default_model

        # If a custom signing transport is available (llm_utils.get_token),
        # use the async http client it returns so every request is signed.
        # Otherwise fall back to a plain AsyncOpenAI client.
        http_client = None
        if _get_token is not None:
            try:
                _sync_client, http_client = _get_token()
            except Exception:
                http_client = None

        client_kwargs: dict[str, Any] = {
            "api_key": api_key,
            "base_url": api_base,
            "default_headers": {"x-session-affinity": uuid.uuid4().hex},
        }
        if http_client is not None:
            client_kwargs["http_client"] = http_client

        self._client = AsyncOpenAI(**client_kwargs)

    async def chat(self, messages: list[dict[str, Any]], tools: list[dict[str, Any]] | None = None,
                   model: str | None = None, max_tokens: int = 4096, temperature: float = 0.7,
                   reasoning_effort: str | None = None) -> LLMResponse:
        kwargs: dict[str, Any] = {
            "model": model or self.default_model,
            "messages": self._sanitize_empty_content(messages),
            "max_tokens": max(1, max_tokens),
            "temperature": temperature,
        }
        if reasoning_effort:
            kwargs["reasoning_effort"] = reasoning_effort
        if tools:
            kwargs.update(tools=tools, tool_choice="auto")
        try:
            return self._parse(await self._client.chat.completions.create(**kwargs))
        except Exception as e:
            return LLMResponse(content=f"Error: {e}", finish_reason="error")

    async def chat_stream(self, messages: list[dict[str, Any]], tools: list[dict[str, Any]] | None = None,
                          model: str | None = None, max_tokens: int = 4096, temperature: float = 0.7,
                          reasoning_effort: str | None = None):
        """Stream via the same OpenAI client (inherits the custom http-client/signing).

        Yields ("delta", text) chunks then a terminal ("final", LLMResponse).
        """
        kwargs: dict[str, Any] = {
            "model": model or self.default_model,
            "messages": self._sanitize_empty_content(messages),
            "max_tokens": max(1, max_tokens),
            "temperature": temperature,
            "stream": True,
        }
        if reasoning_effort:
            kwargs["reasoning_effort"] = reasoning_effort
        if tools:
            kwargs.update(tools=tools, tool_choice="auto")

        content_parts: list[str] = []
        reasoning_parts: list[str] = []
        tool_acc: dict[int, dict[str, Any]] = {}
        finish_reason = "stop"

        try:
            stream = await self._client.chat.completions.create(**kwargs)
            # A signing transport that buffers the body may return a full
            # response even with stream=True — fall back transparently.
            if not hasattr(stream, "__aiter__"):
                yield ("final", self._parse(stream))
                return
            async for chunk in stream:
                choices = getattr(chunk, "choices", None) or []
                if not choices:
                    continue
                choice = choices[0]
                delta = getattr(choice, "delta", None)
                if delta is not None:
                    text = getattr(delta, "content", None)
                    if text:
                        content_parts.append(text)
                        yield ("delta", text)
                    rc = getattr(delta, "reasoning_content", None)
                    if rc:
                        reasoning_parts.append(rc)
                    for tcd in (getattr(delta, "tool_calls", None) or []):
                        idx = getattr(tcd, "index", 0) or 0
                        slot = tool_acc.setdefault(idx, {"id": None, "name": "", "args": ""})
                        if getattr(tcd, "id", None):
                            slot["id"] = tcd.id
                        fn = getattr(tcd, "function", None)
                        if fn is not None:
                            if getattr(fn, "name", None):
                                slot["name"] = fn.name
                            if getattr(fn, "arguments", None):
                                slot["args"] += fn.arguments
                if getattr(choice, "finish_reason", None):
                    finish_reason = choice.finish_reason
        except Exception as e:
            yield ("final", LLMResponse(content=f"Error: {e}", finish_reason="error"))
            return

        tool_calls = []
        for idx in sorted(tool_acc):
            slot = tool_acc[idx]
            if not slot["name"]:
                continue
            args = slot["args"]
            args = json_repair.loads(args) if isinstance(args, str) and args.strip() else {}
            tool_calls.append(ToolCallRequest(
                id=slot["id"] or uuid.uuid4().hex, name=slot["name"], arguments=args,
            ))
            finish_reason = "tool_calls"

        yield ("final", LLMResponse(
            content="".join(content_parts) or None,
            tool_calls=tool_calls,
            finish_reason=finish_reason,
            reasoning_content="".join(reasoning_parts) or None,
        ))

    def _parse(self, response: Any) -> LLMResponse:
        choice = response.choices[0]
        msg = choice.message
        tool_calls = [
            ToolCallRequest(id=tc.id, name=tc.function.name,
                            arguments=json_repair.loads(tc.function.arguments) if isinstance(tc.function.arguments, str) else tc.function.arguments)
            for tc in (msg.tool_calls or [])
        ]
        u = response.usage
        return LLMResponse(
            content=msg.content, tool_calls=tool_calls, finish_reason=choice.finish_reason or "stop",
            usage={"prompt_tokens": u.prompt_tokens, "completion_tokens": u.completion_tokens, "total_tokens": u.total_tokens} if u else {},
            reasoning_content=getattr(msg, "reasoning_content", None) or None,
        )

    def get_default_model(self) -> str:
        return self.default_model

