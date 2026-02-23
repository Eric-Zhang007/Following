from __future__ import annotations

import base64
import json
import os
import time
from typing import Any

import requests

from trader.config import VLMConfig
from trader.vlm_schema import VLMParsedSignal

_SYSTEM_PROMPT = """You are a strict trading-signal extractor.
Return JSON only, no prose.
Do extraction only. Never make trading decisions.

Hard constraints:
1) Never guess or infer unseen values.
- If symbol is not visible, set symbol=null.
- If side is not fully certain, set side=null.
- If leverage is not visible, set leverage=null.
- If number is unclear, set corresponding field null.
2) Resolve text-image conflicts by choosing stronger explicit evidence and report conflict in extraction_warnings.
3) Every non-null critical field must have at least one direct evidence snippet in evidence.field_evidence.
4) If a field has no evidence, set it to null.
5) safety.should_trade must be exactly "NO_DECISION".
6) If uncertain_fields is non-empty OR any critical field missing, confidence must be <= 0.6.
7) Evidence snippets must be short direct quotes (<=30 chars) from text/image.
"""


class VLMClient:
    def __init__(self, config: VLMConfig) -> None:
        self.config = config
        self.api_key = os.getenv(config.api_key_env, "")
        if not self.api_key:
            raise RuntimeError(f"missing API key in env var: {config.api_key_env}")

        if config.base_url:
            self.base_url = config.base_url.rstrip("/")
        elif config.provider == "nim":
            self.base_url = "https://integrate.api.nvidia.com/v1"
        else:
            self.base_url = "https://api.moonshot.cn/v1"
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
            }
        )

    def extract(self, image_bytes: bytes | None, text_context: str) -> VLMParsedSignal:
        payload = self._build_payload(image_bytes=image_bytes, text_context=text_context)
        raw = self._request_with_retries(payload)
        parsed_json = self._extract_json(raw)
        return VLMParsedSignal.model_validate(parsed_json)

    def _build_payload(self, image_bytes: bytes | None, text_context: str) -> dict[str, Any]:
        content: list[dict[str, Any]] = [{"type": "text", "text": text_context or ""}]
        if image_bytes is not None:
            b64 = base64.b64encode(image_bytes).decode("utf-8")
            content.append(
                {
                    "type": "image_url",
                    "image_url": {"url": f"data:image/jpeg;base64,{b64}"},
                }
            )
        return {
            "model": self.config.model,
            "temperature": 0,
            "messages": [
                {"role": "system", "content": _SYSTEM_PROMPT},
                {"role": "user", "content": content},
            ],
            "response_format": {"type": "json_object"},
        }

    def _request_with_retries(self, payload: dict[str, Any]) -> dict[str, Any]:
        url = f"{self.base_url}/chat/completions"
        last_error: Exception | None = None
        for attempt in range(self.config.max_retries + 1):
            try:
                response = self.session.post(url, data=json.dumps(payload), timeout=self.config.timeout_seconds)
                response.raise_for_status()
                return response.json()
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                if attempt >= self.config.max_retries:
                    break
                time.sleep(0.5 * (2**attempt))
        raise RuntimeError(f"VLM request failed after retries: {last_error}")

    @staticmethod
    def _extract_json(response_payload: dict[str, Any]) -> dict[str, Any]:
        choices = response_payload.get("choices", [])
        if not choices:
            raise RuntimeError(f"VLM response missing choices: {response_payload}")
        message = choices[0].get("message", {})
        content = message.get("content")
        if isinstance(content, list):
            text_parts = [item.get("text", "") for item in content if isinstance(item, dict)]
            raw_text = "".join(text_parts).strip()
        else:
            raw_text = str(content or "").strip()
        if not raw_text:
            raise RuntimeError(f"VLM response missing content: {response_payload}")
        return json.loads(raw_text)
