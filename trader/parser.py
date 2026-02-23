from __future__ import annotations

import re
from datetime import datetime

from trader.models import EntrySignal, EntryType, ManageAction, NonSignal, ParsedKind, ParsedMessage, Side

SYMBOL_RE = re.compile(r"#?\s*([A-Za-z0-9]+)\s*/\s*(USDT)", re.IGNORECASE)
LEVERAGE_RE = re.compile(r"(\d{1,3})\s*(?:x|X|倍)")
ENTRY_RANGE_RE = re.compile(
    r"(?:进场|入场)\s*[:：]?\s*(?:市价|限价)?\s*([0-9]*\.?[0-9]+)\s*(?:附近)?\s*(?:-|—|~|～|到|至)?\s*([0-9]*\.?[0-9]+)?",
    re.IGNORECASE,
)
REDUCE_RE = re.compile(r"减仓\s*(\d{1,3})(?:\s*[%％])?", re.IGNORECASE)
TP_RE = re.compile(r"TP\s*\d*\s*(?:看|到|:|：)?\s*([0-9]*\.?[0-9]+)", re.IGNORECASE)
STOP_RE = re.compile(r"(?:止损|SL)\s*[:：]?\s*([0-9]*\.?[0-9]+)", re.IGNORECASE)
ENTRY_TP_RE = re.compile(r"(?:止盈|TP)\s*\d*\s*(?:看|到|:|：)?\s*([0-9]*\.?[0-9]+)", re.IGNORECASE)


class SignalParser:
    def __init__(self) -> None:
        self._last_symbol_by_source: dict[str, str] = {}

    def parse(
        self,
        text: str | None,
        source_key: str,
        fallback_symbol: str | None = None,
        timestamp: datetime | None = None,
    ) -> ParsedMessage:
        normalized = (text or "").strip()
        if not normalized:
            return NonSignal(kind=ParsedKind.NON_SIGNAL, raw_text="", note="empty text", timestamp=timestamp)

        entry = self._parse_entry(normalized, timestamp=timestamp)
        if entry:
            self._last_symbol_by_source[source_key] = entry.symbol
            return entry

        manage = self._parse_manage(normalized, timestamp=timestamp)
        if manage:
            if not manage.symbol:
                manage.symbol = self._last_symbol_by_source.get(source_key) or fallback_symbol
            return manage

        return NonSignal(kind=ParsedKind.NON_SIGNAL, raw_text=normalized, note="no trading intent", timestamp=timestamp)

    def _parse_entry(self, text: str, timestamp: datetime | None) -> EntrySignal | None:
        lowered = text.lower()
        if "进场" not in text and "入场" not in text:
            return None

        symbol_match = SYMBOL_RE.search(text)
        if not symbol_match:
            return None

        side = self._extract_side(lowered)
        if side is None:
            return None

        range_match = ENTRY_RANGE_RE.search(text)
        if not range_match:
            return None

        p1 = float(range_match.group(1))
        p2_raw = range_match.group(2)
        p2 = float(p2_raw) if p2_raw else p1
        entry_low = min(p1, p2)
        entry_high = max(p1, p2)

        leverage_match = LEVERAGE_RE.search(text)
        leverage = int(leverage_match.group(1)) if leverage_match else None

        entry_type = EntryType.MARKET if "市价" in text else EntryType.LIMIT
        stop_match = STOP_RE.search(text)
        stop_loss = float(stop_match.group(1)) if stop_match else None
        take_profit = [float(v) for v in ENTRY_TP_RE.findall(text)]

        base, quote = symbol_match.group(1).upper(), symbol_match.group(2).upper()
        return EntrySignal(
            kind=ParsedKind.ENTRY_SIGNAL,
            raw_text=text,
            symbol=f"{base}{quote}",
            quote=quote,
            side=side,
            leverage=leverage,
            entry_type=entry_type,
            entry_low=entry_low,
            entry_high=entry_high,
            stop_loss=stop_loss,
            take_profit=take_profit,
            timestamp=timestamp,
        )

    def _parse_manage(self, text: str, timestamp: datetime | None) -> ManageAction | None:
        reduce_match = REDUCE_RE.search(text)
        reduce_pct = float(reduce_match.group(1)) if reduce_match else None

        move_sl_to_be = any(token in text for token in ["保本", "设保本", "移到开仓价", "止损到开仓价"])

        tp_match = TP_RE.search(text)
        tp_price = float(tp_match.group(1)) if tp_match else None

        if reduce_pct is None and not move_sl_to_be and tp_price is None and "留底仓" not in text:
            return None

        symbol_match = SYMBOL_RE.search(text)
        symbol = None
        if symbol_match:
            symbol = f"{symbol_match.group(1).upper()}{symbol_match.group(2).upper()}"

        return ManageAction(
            kind=ParsedKind.MANAGE_ACTION,
            raw_text=text,
            symbol=symbol,
            reduce_pct=reduce_pct,
            move_sl_to_be=move_sl_to_be,
            tp_price=tp_price,
            note=text[:200],
            timestamp=timestamp,
        )

    @staticmethod
    def _extract_side(lowered_text: str) -> Side | None:
        if any(token in lowered_text for token in ["做空", "short"]):
            return Side.SHORT
        if any(token in lowered_text for token in ["做多", "long"]):
            return Side.LONG
        return None
