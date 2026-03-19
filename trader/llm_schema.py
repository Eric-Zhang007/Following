from __future__ import annotations

from datetime import datetime
from enum import Enum
import re

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from trader.models import EntrySignal, EntryType, ManageAction, NonSignal, ParsedKind, ParsedMessage, Side

_FULL_CLOSE_HINT_RE = re.compile(
    r"(?:市价止盈|市價止盈|市价止损|市價止損|全平|全部平仓|全部平倉|清仓|清倉|平仓出局|平倉出局|close\s*all)",
    re.IGNORECASE,
)
_REDUCE_HINT_RE = re.compile(
    r"(?:减仓|減倉|平仓|平倉|减掉\s*补仓|減掉\s*補倉|出掉\s*补仓|出掉\s*補倉|出\s*补仓|出\s*補倉|出了\s*补仓(?:资金)?|出了\s*補倉(?:資金)?)",
    re.IGNORECASE,
)
_EXIT_ADDON_HINT_RE = re.compile(
    r"(?:减掉\s*补仓|減掉\s*補倉|减掉\s*補倉|減掉\s*补仓|出掉\s*补仓|出掉\s*補倉|出\s*补仓|出\s*補倉|出了\s*补仓(?:资金)?|出了\s*補倉(?:資金)?)",
    re.IGNORECASE,
)
_DEFAULT_REDUCE_PCT = 35.0


class LLMKind(str, Enum):
    ENTRY_SIGNAL = "ENTRY_SIGNAL"
    MANAGE_ACTION = "MANAGE_ACTION"
    NON_SIGNAL = "NON_SIGNAL"


class LLMSide(str, Enum):
    LONG = "LONG"
    SHORT = "SHORT"


class LLMEntryType(str, Enum):
    LIMIT_RANGE = "LIMIT_RANGE"
    MARKET_RANGE = "MARKET_RANGE"
    MARKET = "MARKET"


class LLMEntry(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: LLMEntryType | None = None
    low: float | None = None
    high: float | None = None

    @model_validator(mode="after")
    def validate_range(self) -> "LLMEntry":
        if self.low is not None and self.high is not None and self.low > self.high:
            raise ValueError("entry.low must be <= entry.high")
        return self


class LLMManage(BaseModel):
    model_config = ConfigDict(extra="forbid")

    reduce_pct: float | None = Field(default=None, ge=0, le=100)
    add_pct: float | None = Field(default=None, gt=0, le=200)
    move_sl_to_be: bool | None = None
    tp: list[float] = Field(default_factory=list)


class LLMParsedOutput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    kind: LLMKind
    symbol: str | None = None
    side: LLMSide | None = None
    leverage: int | None = Field(default=None, ge=1, le=125)
    entry: LLMEntry = Field(default_factory=LLMEntry)
    manage: LLMManage = Field(default_factory=LLMManage)
    confidence: float = Field(ge=0, le=1)
    notes: str = ""

    @field_validator("symbol")
    @classmethod
    def validate_symbol(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip().upper()
        if not normalized.endswith("USDT"):
            raise ValueError("symbol quote must be USDT")
        base = normalized[:-4]
        if not base or not base.isalnum():
            raise ValueError("symbol must be <BASE>USDT")
        return normalized

    def to_parsed_message(
        self,
        raw_text: str,
        timestamp: datetime | None,
        fallback_symbol: str | None = None,
    ) -> ParsedMessage:
        if self.kind == LLMKind.NON_SIGNAL:
            return NonSignal(kind=ParsedKind.NON_SIGNAL, raw_text=raw_text, note=self.notes, timestamp=timestamp)

        if self.kind == LLMKind.ENTRY_SIGNAL:
            symbol = self.symbol
            side = self.side
            entry_type = self.entry.type
            if symbol is None or side is None or entry_type is None:
                return NonSignal(
                    kind=ParsedKind.NON_SIGNAL,
                    raw_text=raw_text,
                    note="incomplete_entry_fields",
                    timestamp=timestamp,
                )

            low = self.entry.low
            high = self.entry.high
            if low is None and high is None:
                return NonSignal(
                    kind=ParsedKind.NON_SIGNAL,
                    raw_text=raw_text,
                    note="incomplete_entry_price",
                    timestamp=timestamp,
                )
            if low is None:
                low = high
            if high is None:
                high = low
            if low is None or high is None:
                return NonSignal(
                    kind=ParsedKind.NON_SIGNAL,
                    raw_text=raw_text,
                    note="incomplete_entry_price",
                    timestamp=timestamp,
                )
            if low > high:
                low, high = high, low

            mapped_entry_type = EntryType.LIMIT if entry_type == LLMEntryType.LIMIT_RANGE else EntryType.MARKET
            return EntrySignal(
                kind=ParsedKind.ENTRY_SIGNAL,
                raw_text=raw_text,
                symbol=symbol,
                quote="USDT",
                side=Side(side.value),
                leverage=self.leverage,
                entry_type=mapped_entry_type,
                entry_low=float(low),
                entry_high=float(high),
                timestamp=timestamp,
            )

        # MANAGE_ACTION
        symbol = self.symbol or fallback_symbol
        reduce_pct = self.manage.reduce_pct
        add_pct = self.manage.add_pct
        move_sl_to_be = bool(self.manage.move_sl_to_be)
        tp_price = self.manage.tp[0] if self.manage.tp else None
        exit_addon = _EXIT_ADDON_HINT_RE.search(raw_text) is not None
        if reduce_pct is None:
            reduce_pct = _infer_reduce_default(raw_text)
        if exit_addon:
            if reduce_pct is None:
                reduce_pct = _DEFAULT_REDUCE_PCT
            add_pct = None
        elif reduce_pct is not None and reduce_pct >= 100.0:
            add_pct = None

        if reduce_pct is None and add_pct is None and not move_sl_to_be and tp_price is None:
            return NonSignal(
                kind=ParsedKind.NON_SIGNAL,
                raw_text=raw_text,
                note="incomplete_manage_fields",
                timestamp=timestamp,
            )

        return ManageAction(
            kind=ParsedKind.MANAGE_ACTION,
            raw_text=raw_text,
            symbol=symbol,
            reduce_pct=reduce_pct,
            add_pct=add_pct,
            move_sl_to_be=move_sl_to_be,
            tp_price=tp_price,
            note=self.notes,
            timestamp=timestamp,
        )


def get_llm_json_schema() -> dict:
    return LLMParsedOutput.model_json_schema()


def get_response_format(name: str = "signal_parser") -> dict:
    return {
        "type": "json_schema",
        "name": name,
        "schema": get_llm_json_schema(),
        "strict": True,
    }


def _infer_reduce_default(raw_text: str) -> float | None:
    if not raw_text:
        return None
    if _FULL_CLOSE_HINT_RE.search(raw_text):
        return 100.0
    if _REDUCE_HINT_RE.search(raw_text):
        return _DEFAULT_REDUCE_PCT
    return None
