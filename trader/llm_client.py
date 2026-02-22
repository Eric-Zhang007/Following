from __future__ import annotations

import json
import os
from typing import Any

from trader.config import LLMConfig
from trader.llm_schema import get_response_format


class OpenAIResponsesClient:
    def __init__(self, config: LLMConfig) -> None:
        self.config = config
        self.api_key = os.getenv(config.api_key_env, "")
        if not self.api_key:
            raise RuntimeError(f"missing API key in env var: {config.api_key_env}")

        try:
            from openai import OpenAI
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError("openai package is required for LLM parsing") from exc

        self.client = OpenAI(api_key=self.api_key, base_url=config.base_url, timeout=config.timeout_seconds)

    def parse_signal(self, sanitized_text: str) -> dict[str, Any]:
        last_error: Exception | None = None
        for _ in range(self.config.max_retries + 1):
            try:
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
                return self._extract_json(response)
            except Exception as exc:  # noqa: BLE001
                last_error = exc
        raise RuntimeError(f"OpenAI responses.create failed after retries: {last_error}")

    def _extract_json(self, response: Any) -> dict[str, Any]:
        output_text = getattr(response, "output_text", None)
        if output_text:
            return json.loads(output_text)

        payload = response.model_dump() if hasattr(response, "model_dump") else dict(response)
        output = payload.get("output", [])
        for item in output:
            for content in item.get("content", []):
                text = content.get("text")
                if text:
                    return json.loads(text)

        raise RuntimeError(f"OpenAI response does not contain JSON text: {payload}")
