from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any


class ParsedKind(str, Enum):
    ENTRY_SIGNAL = "ENTRY_SIGNAL"
    MANAGE_ACTION = "MANAGE_ACTION"
    NON_SIGNAL = "NON_SIGNAL"
    NEEDS_MANUAL = "NEEDS_MANUAL"


class Side(str, Enum):
    LONG = "LONG"
    SHORT = "SHORT"


class EntryType(str, Enum):
    MARKET = "MARKET"
    LIMIT = "LIMIT"


@dataclass
class EntrySignal:
    kind: ParsedKind
    raw_text: str
    symbol: str
    quote: str
    side: Side
    leverage: int | None
    entry_type: EntryType
    entry_low: float
    entry_high: float
    entry_points: list[float] = field(default_factory=list)
    stop_loss: float | None = None
    take_profit: list[float] = field(default_factory=list)
    tp_points: list[float] = field(default_factory=list)
    timestamp: datetime | None = None
    thread_id: int | None = None

    def __post_init__(self) -> None:
        if not self.entry_points:
            if self.entry_low > 0 and self.entry_high > 0:
                if self.entry_low == self.entry_high:
                    self.entry_points = [self.entry_low]
                else:
                    self.entry_points = [self.entry_low, self.entry_high]
        if not self.tp_points and self.take_profit:
            self.tp_points = list(self.take_profit)
        if not self.take_profit and self.tp_points:
            self.take_profit = list(self.tp_points)


@dataclass
class ManageAction:
    kind: ParsedKind
    raw_text: str
    symbol: str | None
    reduce_pct: float | None
    move_sl_to_be: bool
    tp_price: float | None
    tp_points: list[float] = field(default_factory=list)
    stop_loss: float | None = None
    note: str | None = None
    timestamp: datetime | None = None
    thread_id: int | None = None

    def __post_init__(self) -> None:
        if self.tp_price is None and self.tp_points:
            self.tp_price = float(self.tp_points[0])


@dataclass
class NonSignal:
    kind: ParsedKind
    raw_text: str
    note: str = ""
    timestamp: datetime | None = None


@dataclass
class NeedsManual:
    kind: ParsedKind
    raw_text: str
    reason: str
    missing_fields: list[str] = field(default_factory=list)
    timestamp: datetime | None = None


ParsedMessage = EntrySignal | ManageAction | NonSignal | NeedsManual


@dataclass
class RiskDecision:
    approved: bool
    reason: str | None = None
    symbol: str | None = None
    side: Side | None = None
    leverage: int | None = None
    notional: float | None = None
    quantity: float | None = None
    entry_price: float | None = None
    stop_loss_price: float | None = None
    stop_distance_ratio: float | None = None
    quality_score: float | None = None
    warnings: list[str] = field(default_factory=list)

    @classmethod
    def reject(cls, reason: str) -> "RiskDecision":
        return cls(approved=False, reason=reason)


@dataclass
class OrderIntent:
    action_type: str
    symbol: str
    side: str
    trade_side: str | None
    order_type: str
    quantity: float
    price: float | None
    reduce_only: bool
    source_chat_id: int
    source_message_id: int
    source_version: int
    client_order_id: str | None = None
    purpose: str = "entry"
    note: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class OrderAck:
    order_id: str | None
    client_oid: str | None
    status: str
    raw: dict[str, Any]


@dataclass
class TelegramEvent:
    chat_id: int
    message_id: int
    date: datetime
    text: str = ""
    is_edit: bool = False
    raw_text: str | None = None
    reply_to_msg_id: int | None = None
    media_type: str = "none"
    media_bytes: str | None = None
    media_path: str | None = None
    image_url: str | None = None
    media_sha256: str | None = None
    source: str = "telegram"
    thread_id: int | None = None

    def __post_init__(self) -> None:
        if self.raw_text is None:
            self.raw_text = self.text
        if not self.text:
            self.text = self.raw_text or ""
        if self.media_path is None and self.media_bytes:
            self.media_path = self.media_bytes


def utc_now() -> datetime:
    return datetime.now(timezone.utc)
