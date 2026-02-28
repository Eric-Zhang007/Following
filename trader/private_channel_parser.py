from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from trader.config import AppConfig
from trader.models import EntrySignal, EntryType, ManageAction, NeedsManual, NonSignal, ParsedKind, ParsedMessage, Side
from trader.vlm_client import VLMClient

_KEYWORD_RE = re.compile(r"交易\s*信[号號]", re.IGNORECASE)
_SYMBOL_HASH_RE = re.compile(r"#\s*([A-Za-z0-9]{2,20})(?:\s*/\s*(USDT))?", re.IGNORECASE)
_SYMBOL_PAIR_RE = re.compile(r"([A-Za-z0-9]{2,20})\s*/\s*USDT", re.IGNORECASE)
_LEVERAGE_RE = re.compile(r"(\d{1,3})\s*[xX]\s*做?\s*(多|空)?", re.IGNORECASE)
_SIDE_RE = re.compile(r"(做多|做空|LONG|SHORT)", re.IGNORECASE)
_ENTRY_LINE_RE = re.compile(r"(?:進場位|进场位|入場位|入场位|進場|进场)\s*[:：]?\s*([^\n\r]+)", re.IGNORECASE)
_TP_LINE_RE = re.compile(r"(?:盈利位|止盈位|盈利|止盈)\s*[:：]?\s*([^\n\r]+)", re.IGNORECASE)
_SL_LINE_RE = re.compile(r"(?:止損位|止损位|止損|止损|SL)\s*[:：]?\s*([0-9]*\.?[0-9]+)", re.IGNORECASE)
_REDUCE_RE = re.compile(r"(?:减仓|減倉|平仓|平倉)\s*(\d{1,3})(?:\s*[%％])?", re.IGNORECASE)


@dataclass
class PrivateParseOutcome:
    parsed: ParsedMessage
    parse_source: str
    confidence: float
    notes: str = ""
    llm_payload: dict[str, Any] | None = None


class PrivateChannelParser:
    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self._vlm: VLMClient | None = VLMClient(config.vlm) if config.vlm.enabled else None

    def parse(
        self,
        *,
        text: str,
        timestamp: datetime | None,
        image_path: str | None,
        fallback_symbol: str | None,
        thread_id: int | None,
        is_root: bool,
    ) -> PrivateParseOutcome:
        normalized = self._normalize(text)
        if not normalized.strip():
            return PrivateParseOutcome(
                parsed=NonSignal(kind=ParsedKind.NON_SIGNAL, raw_text="", note="empty text", timestamp=timestamp),
                parse_source="RULES_PRIVATE",
                confidence=1.0,
            )

        if _KEYWORD_RE.search(normalized) or is_root:
            parsed, missing = self._parse_entry(normalized, timestamp=timestamp, thread_id=thread_id)
            if parsed is not None:
                return PrivateParseOutcome(parsed=parsed, parse_source="RULES_PRIVATE", confidence=1.0)
            if image_path and self._vlm is not None:
                vlm = self._parse_entry_with_vlm(
                    text=normalized,
                    timestamp=timestamp,
                    image_path=image_path,
                    fallback_symbol=fallback_symbol,
                    thread_id=thread_id,
                )
                if vlm is not None:
                    return vlm
            return PrivateParseOutcome(
                parsed=NeedsManual(
                    kind=ParsedKind.NEEDS_MANUAL,
                    raw_text=normalized,
                    reason="entry_signal_missing_required_fields",
                    missing_fields=missing,
                    timestamp=timestamp,
                ),
                parse_source="RULES_PRIVATE_NEEDS_MANUAL",
                confidence=0.0,
            )

        manage = self._parse_manage(normalized, timestamp=timestamp, thread_id=thread_id)
        if manage is not None:
            if not manage.symbol:
                manage.symbol = fallback_symbol
            return PrivateParseOutcome(parsed=manage, parse_source="RULES_PRIVATE", confidence=1.0)

        return PrivateParseOutcome(
            parsed=NonSignal(
                kind=ParsedKind.NON_SIGNAL,
                raw_text=normalized,
                note="thread_non_actionable",
                timestamp=timestamp,
            ),
            parse_source="RULES_PRIVATE",
            confidence=1.0,
        )

    def _parse_entry(
        self,
        text: str,
        *,
        timestamp: datetime | None,
        thread_id: int | None,
    ) -> tuple[EntrySignal | None, list[str]]:
        symbol = self._extract_symbol(text)
        side = self._extract_side(text)
        leverage = self._extract_leverage(text)
        entry_points = self._extract_price_points(_ENTRY_LINE_RE, text)
        tp_points = self._extract_price_points(_TP_LINE_RE, text)
        sl_price = self._extract_stop_loss(text)

        missing: list[str] = []
        if symbol is None:
            missing.append("symbol")
        if side is None:
            missing.append("side")
        if leverage is None:
            missing.append("leverage")
        if not entry_points:
            missing.append("entry_points")
        if not tp_points:
            missing.append("tp_points")
        if sl_price is None:
            missing.append("sl_price")

        if missing:
            return None, missing

        ordered_entries = [float(x) for x in entry_points]
        entry_low = min(ordered_entries)
        entry_high = max(ordered_entries)
        return (
            EntrySignal(
                kind=ParsedKind.ENTRY_SIGNAL,
                raw_text=text,
                symbol=symbol or "",
                quote="USDT",
                side=side or Side.LONG,
                leverage=leverage,
                entry_type=EntryType.LIMIT,
                entry_low=entry_low,
                entry_high=entry_high,
                entry_points=ordered_entries,
                stop_loss=float(sl_price) if sl_price is not None else None,
                take_profit=[float(x) for x in tp_points],
                tp_points=[float(x) for x in tp_points],
                timestamp=timestamp,
                thread_id=thread_id,
            ),
            [],
        )

    def _parse_entry_with_vlm(
        self,
        *,
        text: str,
        timestamp: datetime | None,
        image_path: str,
        fallback_symbol: str | None,
        thread_id: int | None,
    ) -> PrivateParseOutcome | None:
        if self._vlm is None:
            return None
        try:
            image_bytes = Path(image_path).read_bytes()
            parsed = self._vlm.extract(image_bytes=image_bytes, text_context=text)
        except Exception:
            return None

        payload = parsed.model_dump(mode="json")
        evidence = payload.get("evidence", {}) if isinstance(payload, dict) else {}
        field_evidence = evidence.get("field_evidence", {}) if isinstance(evidence, dict) else {}
        if not isinstance(field_evidence, dict):
            field_evidence = {}

        required = [
            "symbol",
            "side",
            "entry.low",
            "entry.high",
            "entry.tp",
            "entry.stop_loss",
            "leverage",
        ]
        missing = [fp for fp in required if not self._has_field_evidence(field_evidence, fp)]
        if missing:
            return PrivateParseOutcome(
                parsed=NeedsManual(
                    kind=ParsedKind.NEEDS_MANUAL,
                    raw_text=text,
                    reason="vlm_missing_evidence",
                    missing_fields=missing,
                    timestamp=timestamp,
                ),
                parse_source="VLM_PRIVATE_NEEDS_MANUAL",
                confidence=0.0,
                llm_payload=payload,
            )

        signal = parsed.to_parsed_message(text, timestamp=timestamp, fallback_symbol=fallback_symbol)
        if not isinstance(signal, EntrySignal):
            return PrivateParseOutcome(
                parsed=NeedsManual(
                    kind=ParsedKind.NEEDS_MANUAL,
                    raw_text=text,
                    reason="vlm_not_entry_signal",
                    missing_fields=["entry_signal"],
                    timestamp=timestamp,
                ),
                parse_source="VLM_PRIVATE_NEEDS_MANUAL",
                confidence=0.0,
                llm_payload=payload,
            )
        signal.thread_id = thread_id
        if not signal.entry_points:
            signal.entry_points = [signal.entry_low, signal.entry_high] if signal.entry_low != signal.entry_high else [signal.entry_low]
        if not signal.tp_points:
            signal.tp_points = list(signal.take_profit)
        return PrivateParseOutcome(
            parsed=signal,
            parse_source="VLM_PRIVATE",
            confidence=float(parsed.confidence),
            llm_payload=payload,
        )

    def _parse_manage(self, text: str, *, timestamp: datetime | None, thread_id: int | None) -> ManageAction | None:
        reduce_match = _REDUCE_RE.search(text)
        reduce_pct = float(reduce_match.group(1)) if reduce_match else None
        if reduce_pct is not None:
            reduce_pct = max(0.0, min(100.0, reduce_pct))

        move_sl_to_be = any(token in text for token in ["保本", "成本", "止损上移到成本", "止損上移到成本"])
        tp_points = self._extract_price_points(_TP_LINE_RE, text)
        sl_price = self._extract_stop_loss(text)
        symbol = self._extract_symbol(text)

        has_action = reduce_pct is not None or move_sl_to_be or bool(tp_points) or sl_price is not None
        if not has_action:
            return None

        return ManageAction(
            kind=ParsedKind.MANAGE_ACTION,
            raw_text=text,
            symbol=symbol,
            reduce_pct=reduce_pct,
            move_sl_to_be=move_sl_to_be,
            tp_price=float(tp_points[0]) if tp_points else None,
            tp_points=[float(x) for x in tp_points],
            stop_loss=sl_price,
            note=text[:200],
            timestamp=timestamp,
            thread_id=thread_id,
        )

    @staticmethod
    def _normalize(text: str) -> str:
        return unicodedata.normalize("NFKC", text or "")

    @staticmethod
    def _extract_symbol(text: str) -> str | None:
        m = _SYMBOL_HASH_RE.search(text)
        if m:
            base = m.group(1).upper()
            return base if base.endswith("USDT") else f"{base}USDT"
        m = _SYMBOL_PAIR_RE.search(text)
        if m:
            return f"{m.group(1).upper()}USDT"
        return None

    @staticmethod
    def _extract_side(text: str) -> Side | None:
        m = _SIDE_RE.search(text)
        if not m:
            return None
        token = m.group(1).upper()
        if token in {"做空", "SHORT"}:
            return Side.SHORT
        if token in {"做多", "LONG"}:
            return Side.LONG
        return None

    @staticmethod
    def _extract_leverage(text: str) -> int | None:
        m = _LEVERAGE_RE.search(text)
        if m:
            return int(m.group(1))
        return None

    @staticmethod
    def _extract_stop_loss(text: str) -> float | None:
        m = _SL_LINE_RE.search(text)
        if not m:
            return None
        return float(m.group(1))

    @staticmethod
    def _extract_price_points(pattern: re.Pattern[str], text: str) -> list[float]:
        m = pattern.search(text)
        if not m:
            return []
        body = m.group(1)
        values = re.findall(r"[0-9]*\.?[0-9]+", body)
        return [float(v) for v in values if v and v != "."]

    @staticmethod
    def _has_field_evidence(field_evidence: dict[str, list[str]], field_path: str) -> bool:
        if field_path in field_evidence and field_evidence.get(field_path):
            return True
        if field_path == "entry.tp":
            return any(str(k).startswith("entry.tp") and field_evidence.get(str(k)) for k in field_evidence.keys())
        if field_path == "entry.stop_loss":
            for alias in ("entry.stop_loss", "entry.sl", "stop_loss"):
                if field_evidence.get(alias):
                    return True
            return False
        return False
