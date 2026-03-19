from __future__ import annotations

import base64
import json
import os
import time
from typing import Any

import requests

from trader.config import VLMConfig
from trader.vlm_schema import VLMParsedSignal, get_vlm_json_schema

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
8) For MANAGE_ACTION reduce/partial-close without explicit numeric percent, set manage.reduce_pct=35.
9) For explicit full-close intent (e.g. 全平/close all), set manage.reduce_pct=100 and add_pct=null.
"""
_VLM_SCHEMA_JSON_TEXT = json.dumps(get_vlm_json_schema(), ensure_ascii=False)


class VLMClient:
    def __init__(self, config: VLMConfig) -> None:
        self.config = config
        self.api_key = os.getenv(config.api_key_env, "")
        if not self.api_key:
            raise RuntimeError(f"missing API key in env var: {config.api_key_env}")

        self.base_url = _resolve_base_url(config.provider, config.base_url)
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
            }
        )

    def extract(self, image_bytes: bytes | None, text_context: str) -> VLMParsedSignal:
        last_error: Exception | None = None
        for schema_attempt in range(2):
            payload = self._build_payload(
                image_bytes=image_bytes,
                text_context=text_context,
                schema_retry=schema_attempt > 0,
            )
            raw = self._request_with_retries(payload)
            parsed_json = self._extract_json(raw)
            try:
                return VLMParsedSignal.model_validate(parsed_json)
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                if schema_attempt == 0:
                    continue
        raise RuntimeError(f"VLM schema validation failed after one retry: {last_error}")

    def _build_payload(self, image_bytes: bytes | None, text_context: str, schema_retry: bool = False) -> dict[str, Any]:
        schema_hint = (
            "Follow JSON schema exactly (no extra keys):\n"
            f"{_VLM_SCHEMA_JSON_TEXT}\n"
        )
        if schema_retry:
            schema_hint = (
                "Previous output violated schema. Fix it and return strictly valid JSON.\n"
                + schema_hint
            )
        content: list[dict[str, Any]] = [{"type": "text", "text": f"{schema_hint}\nContext:\n{text_context or ''}"}]
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
        return _parse_json_text(raw_text)


def _resolve_base_url(provider: str, configured: str | None) -> str:
    if configured:
        return configured.rstrip("/")
    p = str(provider).strip().lower()
    if p == "nim":
        return "https://integrate.api.nvidia.com/v1"
    if p == "kimi":
        return "https://api.moonshot.cn/v1"
    if p == "qwen":
        return "https://dashscope-us.aliyuncs.com/compatible-mode/v1"
    raise RuntimeError(f"unsupported VLM provider: {provider}")


def _parse_json_text(raw: str) -> dict[str, Any]:
    text = str(raw).strip()
    if not text:
        raise RuntimeError("empty JSON text")
    try:
        return json.loads(text)
    except Exception:  # noqa: BLE001
        pass

    if text.startswith("```") and text.endswith("```"):
        lines = text.splitlines()
        if len(lines) >= 3:
            inner = "\n".join(lines[1:-1]).strip()
            if inner:
                return json.loads(inner)

    start = text.find("{")
    end = text.rfind("}")
    if start >= 0 and end > start:
        return json.loads(text[start : end + 1])
    raise RuntimeError(f"invalid JSON text: {text[:200]}")
