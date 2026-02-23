from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from trader.models import (
    EntrySignal,
    EntryType,
    ManageAction,
    NeedsManual,
    NonSignal,
    ParsedKind,
    ParsedMessage,
    Side,
)


class VLMKind(str, Enum):
    ENTRY_SIGNAL = "ENTRY_SIGNAL"
    MANAGE_ACTION = "MANAGE_ACTION"
    NON_SIGNAL = "NON_SIGNAL"
    NEEDS_MANUAL = "NEEDS_MANUAL"


class VLMSide(str, Enum):
    LONG = "LONG"
    SHORT = "SHORT"


class VLMEntry(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: Literal["MARKET", "LIMIT"] | None
    low: float | None
    high: float | None
    stop_loss: float | None
    tp: list[float]

    @model_validator(mode="after")
    def validate_range(self) -> "VLMEntry":
        if self.low is not None and self.high is not None and self.low > self.high:
            raise ValueError("entry.low must be <= entry.high")
        return self


class VLMManage(BaseModel):
    model_config = ConfigDict(extra="forbid")

    reduce_pct: float | None = Field(ge=0, le=100)
    move_sl_to_be: bool
    tp: list[float]


class VLMEvidence(BaseModel):
    model_config = ConfigDict(extra="forbid")

    field_evidence: dict[str, list[str]]
    source: dict[str, Literal["text", "image", "both", "unknown"]]

    @field_validator("field_evidence")
    @classmethod
    def validate_field_evidence(cls, value: dict[str, list[str]]) -> dict[str, list[str]]:
        for field_path, snippets in value.items():
            if not isinstance(field_path, str) or not field_path:
                raise ValueError("evidence field path must be non-empty string")
            if not snippets:
                raise ValueError(f"evidence snippets empty for {field_path}")
            for snippet in snippets:
                if not isinstance(snippet, str) or not snippet.strip():
                    raise ValueError(f"invalid evidence snippet for {field_path}")
                if len(snippet.strip()) > 30:
                    raise ValueError(f"evidence snippet too long for {field_path}")
        return value


class VLMSafety(BaseModel):
    model_config = ConfigDict(extra="forbid")

    should_trade: Literal["NO_DECISION"]


class VLMParsedSignal(BaseModel):
    model_config = ConfigDict(extra="forbid")

    kind: VLMKind
    symbol: str | None
    side: VLMSide | None
    leverage: int | None = Field(default=None, ge=1, le=125)
    entry: VLMEntry
    manage: VLMManage
    evidence: VLMEvidence
    uncertain_fields: list[str]
    extraction_warnings: list[str]
    safety: VLMSafety
    confidence: float = Field(ge=0, le=1)
    notes: str = ""

    @field_validator("symbol")
    @classmethod
    def validate_symbol(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip().upper().replace("/", "")
        if not normalized.endswith("USDT"):
            raise ValueError("symbol quote must be USDT")
        base = normalized[:-4]
        if not base or not base.isalnum():
            raise ValueError("symbol must be <BASE>USDT")
        return normalized

    @model_validator(mode="after")
    def validate_evidence_and_confidence(self) -> "VLMParsedSignal":
        critical_missing = self._missing_critical_fields()
        if (self.uncertain_fields or critical_missing) and self.confidence > 0.6:
            raise ValueError("confidence must be <= 0.6 when uncertain_fields exist or critical fields are missing")

        required_evidence_fields: dict[str, object | None] = {
            "symbol": self.symbol,
            "side": self.side.value if self.side else None,
            "entry.low": self.entry.low,
            "entry.high": self.entry.high,
            "manage.reduce_pct": self.manage.reduce_pct,
        }
        for field_path, field_value in required_evidence_fields.items():
            if field_value is None:
                continue
            snippets = self.evidence.field_evidence.get(field_path) or []
            if not snippets:
                raise ValueError(f"non-null field {field_path} must include evidence.field_evidence")
            if field_path not in self.evidence.source:
                raise ValueError(f"non-null field {field_path} must include evidence.source")

        return self

    def _missing_critical_fields(self) -> list[str]:
        if self.kind == VLMKind.ENTRY_SIGNAL:
            missing: list[str] = []
            if self.symbol is None:
                missing.append("symbol")
            if self.side is None:
                missing.append("side")
            if self.entry.type is None:
                missing.append("entry.type")
            if self.entry.low is None and self.entry.high is None:
                missing.extend(["entry.low", "entry.high"])
            return missing

        if self.kind == VLMKind.MANAGE_ACTION:
            has_manage = (
                self.manage.reduce_pct is not None
                or self.manage.move_sl_to_be
                or len(self.manage.tp) > 0
            )
            return [] if has_manage else ["manage"]
        return []

    def to_parsed_message(
        self,
        raw_text: str,
        timestamp: datetime | None,
        fallback_symbol: str | None = None,
    ) -> ParsedMessage:
        if self.kind == VLMKind.NON_SIGNAL:
            return NonSignal(
                kind=ParsedKind.NON_SIGNAL,
                raw_text=raw_text,
                note=self.notes or "no trading intent",
                timestamp=timestamp,
            )

        if self.kind == VLMKind.NEEDS_MANUAL:
            return NeedsManual(
                kind=ParsedKind.NEEDS_MANUAL,
                raw_text=raw_text,
                reason=self.notes or "manual review required",
                missing_fields=self._missing_critical_fields(),
                timestamp=timestamp,
            )

        if self.kind == VLMKind.ENTRY_SIGNAL:
            missing = self._missing_critical_fields()
            if missing:
                return NeedsManual(
                    kind=ParsedKind.NEEDS_MANUAL,
                    raw_text=raw_text,
                    reason="incomplete_entry_fields",
                    missing_fields=missing,
                    timestamp=timestamp,
                )

            low = self.entry.low if self.entry.low is not None else self.entry.high
            high = self.entry.high if self.entry.high is not None else self.entry.low
            if low is None or high is None:
                return NeedsManual(
                    kind=ParsedKind.NEEDS_MANUAL,
                    raw_text=raw_text,
                    reason="incomplete_entry_price",
                    missing_fields=["entry.low", "entry.high"],
                    timestamp=timestamp,
                )
            if low > high:
                low, high = high, low

            entry_type = EntryType.MARKET if self.entry.type == "MARKET" else EntryType.LIMIT
            return EntrySignal(
                kind=ParsedKind.ENTRY_SIGNAL,
                raw_text=raw_text,
                symbol=str(self.symbol),
                quote="USDT",
                side=Side(str(self.side.value)),
                leverage=self.leverage,
                entry_type=entry_type,
                entry_low=float(low),
                entry_high=float(high),
                stop_loss=self.entry.stop_loss,
                take_profit=[float(v) for v in self.entry.tp],
                timestamp=timestamp,
            )

        symbol = self.symbol or fallback_symbol
        tp_price = self.manage.tp[0] if self.manage.tp else None
        has_manage = self.manage.reduce_pct is not None or self.manage.move_sl_to_be or tp_price is not None
        if not has_manage:
            return NeedsManual(
                kind=ParsedKind.NEEDS_MANUAL,
                raw_text=raw_text,
                reason="incomplete_manage_fields",
                missing_fields=["manage"],
                timestamp=timestamp,
            )
        return ManageAction(
            kind=ParsedKind.MANAGE_ACTION,
            raw_text=raw_text,
            symbol=symbol,
            reduce_pct=self.manage.reduce_pct,
            move_sl_to_be=self.manage.move_sl_to_be,
            tp_price=tp_price,
            note=self.notes or None,
            timestamp=timestamp,
        )


def get_vlm_json_schema() -> dict:
    return VLMParsedSignal.model_json_schema()


def get_vlm_response_format(name: str = "vlm_signal_parser") -> dict:
    return {
        "type": "json_schema",
        "name": name,
        "schema": get_vlm_json_schema(),
        "strict": True,
    }
