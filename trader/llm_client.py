from __future__ import annotations

import json
import os
from typing import Any

from trader.config import LLMConfig
from trader.llm_schema import get_response_format


class OpenAIResponsesClient:
    def __init__(self, config: LLMConfig) -> None:
        self.config = config
        self.provider = str(config.provider).strip().lower()
        self.api_key = os.getenv(config.api_key_env, "")
        if not self.api_key:
            raise RuntimeError(f"missing API key in env var: {config.api_key_env}")

        try:
            from openai import OpenAI
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError("openai package is required for LLM parsing") from exc

        self.client = OpenAI(
            api_key=self.api_key,
            base_url=_resolve_base_url(self.provider, config.base_url),
            timeout=config.timeout_seconds,
        )

    def parse_signal(self, sanitized_text: str) -> dict[str, Any]:
        last_error: Exception | None = None
        for _ in range(self.config.max_retries + 1):
            try:
                if self.provider == "openai":
                    response = self.client.responses.create(
                        model=self.config.model,
                        input=[
                            {
                                "role": "system",
                                "content": (
                                    "You parse trading signal text into strict JSON schema. "
                                    "Never return prose. Keep uncertain fields null and lower confidence."
                                ),
                            },
                            {
                                "role": "user",
                                "content": sanitized_text,
                            },
                        ],
                        text={"format": get_response_format()},
                    )
                    return self._extract_json_from_responses(response)

                response = self.client.chat.completions.create(
                    model=self.config.model,
                    messages=[
                        {
                            "role": "system",
                            "content": (
                                "You parse trading signal text into strict JSON. "
                                "Never return prose. Keep uncertain fields null and lower confidence."
                            ),
                        },
                        {
                            "role": "user",
                            "content": sanitized_text,
                        },
                    ],
                    temperature=0,
                    response_format={"type": "json_object"},
                )
                return self._extract_json_from_chat_completion(response)
            except Exception as exc:  # noqa: BLE001
                last_error = exc
        raise RuntimeError(f"{self.provider} parse request failed after retries: {last_error}")

    def _extract_json_from_responses(self, response: Any) -> dict[str, Any]:
        output_text = getattr(response, "output_text", None)
        if output_text:
            return _parse_json_text(output_text)

        payload = response.model_dump() if hasattr(response, "model_dump") else dict(response)
        output = payload.get("output", [])
        for item in output:
            for content in item.get("content", []):
                text = content.get("text")
                if text:
                    return _parse_json_text(text)

        raise RuntimeError(f"OpenAI response does not contain JSON text: {payload}")

    def _extract_json_from_chat_completion(self, response: Any) -> dict[str, Any]:
        payload = response.model_dump() if hasattr(response, "model_dump") else dict(response)
        choices = payload.get("choices", [])
        if not choices:
            raise RuntimeError(f"{self.provider} chat completion missing choices: {payload}")

        message = choices[0].get("message", {})
        content = message.get("content")
        if isinstance(content, list):
            parts: list[str] = []
            for item in content:
                if not isinstance(item, dict):
                    continue
                text = item.get("text")
                if text:
                    parts.append(str(text))
            raw_text = "".join(parts).strip()
        else:
            raw_text = str(content or "").strip()

        if not raw_text:
            raise RuntimeError(f"{self.provider} chat completion missing content: {payload}")
        return _parse_json_text(raw_text)


def _parse_json_text(raw: str) -> dict[str, Any]:
    text = str(raw).strip()
    if not text:
        raise RuntimeError("empty JSON text")
    try:
        return json.loads(text)
    except Exception:  # noqa: BLE001
        pass

    # Some providers may still wrap output in fenced markdown.
    if text.startswith("```") and text.endswith("```"):
        lines = text.splitlines()
        if len(lines) >= 3:
            inner = "\n".join(lines[1:-1]).strip()
            if inner:
                return json.loads(inner)

    start = text.find("{")
    end = text.rfind("}")
    if start >= 0 and end > start:
        candidate = text[start : end + 1]
        return json.loads(candidate)
    raise RuntimeError(f"invalid JSON text: {text[:200]}")


def _resolve_base_url(provider: str, configured: str | None) -> str | None:
    if configured:
        return configured
    if provider == "deepseek":
        return "https://api.deepseek.com"
    if provider == "qwen":
        return "https://dashscope-us.aliyuncs.com/compatible-mode/v1"
    return None
