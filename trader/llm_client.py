from __future__ import annotations

import json
import os
import re
from typing import Any

from trader.config import LLMConfig
from trader.llm_schema import get_llm_json_schema, get_response_format

_SCHEMA_KEYS = ("kind", "symbol", "side", "leverage", "entry", "manage", "confidence", "notes")
_SYSTEM_PROMPT = (
    "You parse trading signal text into STRICT JSON that matches the target schema exactly.\n"
    "Rules:\n"
    "1) Output JSON only, no prose.\n"
    "2) Top-level keys must be: kind,symbol,side,leverage,entry,manage,confidence,notes.\n"
    "3) kind must be one of: ENTRY_SIGNAL, MANAGE_ACTION, NON_SIGNAL.\n"
    "4) symbol must end with USDT when present (e.g. PHAUSDT).\n"
    "5) side must be LONG or SHORT when present.\n"
    "6) confidence must be numeric in [0,1] (not strings like 'high').\n"
    "7) For unknown fields use null (or [] for lists), never invent extra keys.\n"
    "8) For MANAGE_ACTION: if text says reduce/partial-close without numeric pct, set manage.reduce_pct=35.\n"
    "9) For explicit full-close intent (e.g. 全平/close all), set manage.reduce_pct=100 and add_pct=null.\n"
)
_SCHEMA_JSON_TEXT = json.dumps(get_llm_json_schema(), ensure_ascii=False)
_MARKET_ANCHOR_RE = re.compile(r"(?:市价|市價|market)\s*([0-9]*\.?[0-9]+)", re.IGNORECASE)
_FULL_CLOSE_HINT_RE = re.compile(
    r"(?:市价止盈|市價止盈|市价止损|市價止損|全平|全部平仓|全部平倉|清仓|清倉|平仓出局|平倉出局|close\s*all)",
    re.IGNORECASE,
)
_REDUCE_HINT_RE = re.compile(r"(?:减仓|減倉|平仓|平倉)", re.IGNORECASE)
_EXPLICIT_REDUCE_PCT_RE = re.compile(r"(?:减仓|減倉|平仓|平倉)\s*(\d{1,3})\s*(?:[%％])?", re.IGNORECASE)
_DEFAULT_REDUCE_PCT = 35.0


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
                                "content": _SYSTEM_PROMPT,
                            },
                            {
                                "role": "user",
                                "content": (
                                    "Parse this trading message and return JSON matching schema exactly.\n"
                                    f"Schema:\n{_SCHEMA_JSON_TEXT}\n\n"
                                    f"Message:\n{sanitized_text}"
                                ),
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
                            "content": _SYSTEM_PROMPT,
                        },
                        {
                            "role": "user",
                            "content": (
                                "Return JSON only and match the schema exactly.\n"
                                f"Schema:\n{_SCHEMA_JSON_TEXT}\n\n"
                                f"Message:\n{sanitized_text}"
                            ),
                        },
                    ],
                    temperature=0,
                    response_format={"type": "json_object"},
                )
                payload = self._extract_json_from_chat_completion(response)
                return _coerce_payload(payload, text_context=sanitized_text)
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


def _coerce_payload(payload: dict[str, Any], text_context: str = "") -> dict[str, Any]:
    if not isinstance(payload, dict):
        return payload
    # Keep already-compliant objects untouched as much as possible.
    if all(k in payload for k in _SCHEMA_KEYS):
        return _backfill_market_anchor(payload, text_context=text_context)
    signal_hints = {
        "kind",
        "action",
        "symbol",
        "side",
        "entry",
        "manage",
        "order_type",
        "price",
        "entry_price",
        "entry_low",
        "entry_high",
        "reduce_pct",
        "add_pct",
        "move_sl_to_be",
        "take_profit",
        "tp",
    }
    if not any(k in payload for k in signal_hints):
        return payload

    normalized: dict[str, Any] = {}

    kind = payload.get("kind")
    action = str(payload.get("action") or "").strip().lower()
    if kind is None and action in {"buy", "long"}:
        kind = "ENTRY_SIGNAL"
    elif kind is None and action in {"sell", "short"}:
        kind = "ENTRY_SIGNAL"
    if kind is not None:
        normalized["kind"] = kind

    side = payload.get("side")
    if side is None and action in {"buy", "long"}:
        side = "LONG"
    elif side is None and action in {"sell", "short"}:
        side = "SHORT"
    if side is not None:
        normalized["side"] = side

    symbol = payload.get("symbol")
    if isinstance(symbol, str):
        symbol = symbol.strip().upper()
        if symbol and symbol.isalnum() and not symbol.endswith("USDT"):
            symbol = f"{symbol}USDT"
    if symbol is not None:
        normalized["symbol"] = symbol

    if "confidence" in payload:
        confidence_raw = payload.get("confidence")
        confidence: float | None = None
        if isinstance(confidence_raw, (int, float)):
            confidence = float(confidence_raw)
        elif isinstance(confidence_raw, str):
            m = confidence_raw.strip().lower()
            if m in {"high", "strong"}:
                confidence = 0.9
            elif m in {"medium", "mid"}:
                confidence = 0.6
            elif m in {"low", "weak"}:
                confidence = 0.3
            else:
                try:
                    confidence = float(m)
                except Exception:  # noqa: BLE001
                    confidence = None
        if confidence is not None:
            normalized["confidence"] = max(0.0, min(1.0, confidence))

    if "leverage" in payload:
        normalized["leverage"] = payload.get("leverage")

    entry_payload = payload.get("entry")
    if not isinstance(entry_payload, dict):
        entry_payload = {}
    entry: dict[str, Any] = {}
    if "type" in entry_payload:
        entry["type"] = entry_payload.get("type")
    else:
        order_type = str(payload.get("order_type") or "").strip().lower()
        if order_type == "market":
            entry["type"] = "MARKET"
        elif order_type == "limit":
            entry["type"] = "LIMIT_RANGE"
    low = entry_payload.get("low", payload.get("entry_low"))
    high = entry_payload.get("high", payload.get("entry_high"))
    price = payload.get("price") or payload.get("entry_price")
    if low is None and high is None and price is not None:
        low = price
        high = price
    if low is not None:
        entry["low"] = low
    if high is not None:
        entry["high"] = high
    if entry:
        normalized["entry"] = entry

    manage_payload = payload.get("manage")
    if not isinstance(manage_payload, dict):
        manage_payload = {}
    manage: dict[str, Any] = {}
    if "reduce_pct" in manage_payload:
        manage["reduce_pct"] = manage_payload.get("reduce_pct")
    elif "reduce_pct" in payload:
        manage["reduce_pct"] = payload.get("reduce_pct")
    if "add_pct" in manage_payload:
        manage["add_pct"] = manage_payload.get("add_pct")
    elif "add_pct" in payload:
        manage["add_pct"] = payload.get("add_pct")
    if "move_sl_to_be" in manage_payload:
        manage["move_sl_to_be"] = manage_payload.get("move_sl_to_be")
    elif "move_sl_to_be" in payload:
        manage["move_sl_to_be"] = payload.get("move_sl_to_be")

    tp_points = None
    if "tp" in manage_payload:
        tp_points = manage_payload.get("tp")
    elif "tp" in payload:
        tp_points = payload.get("tp")
    elif "take_profit" in payload:
        tp_points = payload.get("take_profit")
    if tp_points is not None:
        if isinstance(tp_points, list):
            manage["tp"] = tp_points
        else:
            manage["tp"] = [tp_points]
    inferred_reduce_pct = _infer_default_reduce_pct(text_context)
    if inferred_reduce_pct is not None and "reduce_pct" not in manage:
        manage["reduce_pct"] = inferred_reduce_pct
        if inferred_reduce_pct >= 100:
            manage["add_pct"] = None
    if manage:
        normalized["manage"] = manage
        if "kind" not in normalized:
            normalized["kind"] = "MANAGE_ACTION"

    if "notes" in payload:
        normalized["notes"] = str(payload.get("notes") or "")
    elif "reason" in payload:
        normalized["notes"] = str(payload.get("reason") or "")
    elif "comment" in payload:
        normalized["notes"] = str(payload.get("comment") or "")

    # If we could not map anything useful, keep raw payload for downstream errors/logging.
    if not normalized:
        return payload
    return _backfill_market_anchor(normalized, text_context=text_context)


def _backfill_market_anchor(payload: dict[str, Any], text_context: str) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return payload
    kind = str(payload.get("kind") or "").upper()
    if kind != "ENTRY_SIGNAL":
        return payload
    entry = payload.get("entry")
    if not isinstance(entry, dict):
        return payload
    entry_type = str(entry.get("type") or "").upper()
    if entry_type not in {"MARKET", "MARKET_RANGE"}:
        return payload
    low = entry.get("low")
    high = entry.get("high")
    if low is not None or high is not None:
        return payload

    anchor = _extract_market_anchor_from_text(text_context)
    if anchor is None:
        return payload

    patched = dict(payload)
    patched_entry = dict(entry)
    patched_entry["low"] = anchor
    patched_entry["high"] = anchor
    patched["entry"] = patched_entry
    return patched


def _extract_market_anchor_from_text(text: str) -> float | None:
    if not text:
        return None
    m = _MARKET_ANCHOR_RE.search(text)
    if not m:
        return None
    try:
        value = float(m.group(1))
    except Exception:  # noqa: BLE001
        return None
    if value <= 0:
        return None
    return value


def _infer_default_reduce_pct(text: str) -> float | None:
    if not text:
        return None
    if _FULL_CLOSE_HINT_RE.search(text):
        return 100.0
    explicit = _EXPLICIT_REDUCE_PCT_RE.search(text)
    if explicit:
        try:
            value = float(explicit.group(1))
        except Exception:  # noqa: BLE001
            return None
        return max(0.0, min(100.0, value))
    if _REDUCE_HINT_RE.search(text):
        return _DEFAULT_REDUCE_PCT
    return None
