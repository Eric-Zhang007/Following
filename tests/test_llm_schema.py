from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from trader.llm_schema import LLMParsedOutput
from trader.models import EntrySignal, ManageAction


def test_llm_schema_valid_entry_maps_to_entry_signal() -> None:
    payload = {
        "kind": "ENTRY_SIGNAL",
        "symbol": "CYBERUSDT",
        "side": "SHORT",
        "leverage": 25,
        "entry": {"type": "LIMIT_RANGE", "low": 0.73, "high": 0.746},
        "manage": {"reduce_pct": None, "move_sl_to_be": None, "tp": []},
        "confidence": 0.91,
        "notes": "parsed from post",
    }
    parsed = LLMParsedOutput.model_validate(payload)
    msg = parsed.to_parsed_message("raw", datetime.now(timezone.utc))

    assert isinstance(msg, EntrySignal)
    assert msg.symbol == "CYBERUSDT"
    assert msg.entry_low == 0.73
    assert msg.entry_high == 0.746


def test_llm_schema_rejects_non_usdt_symbol() -> None:
    payload = {
        "kind": "ENTRY_SIGNAL",
        "symbol": "BTCUSD",
        "side": "LONG",
        "leverage": 5,
        "entry": {"type": "MARKET", "low": 100, "high": 100},
        "manage": {"reduce_pct": None, "move_sl_to_be": None, "tp": []},
        "confidence": 0.9,
        "notes": "",
    }
    with pytest.raises(ValidationError):
        LLMParsedOutput.model_validate(payload)


def test_llm_schema_rejects_invalid_entry_range() -> None:
    payload = {
        "kind": "ENTRY_SIGNAL",
        "symbol": "BTCUSDT",
        "side": "LONG",
        "leverage": 5,
        "entry": {"type": "LIMIT_RANGE", "low": 101, "high": 100},
        "manage": {"reduce_pct": None, "move_sl_to_be": None, "tp": []},
        "confidence": 0.9,
        "notes": "",
    }
    with pytest.raises(ValidationError):
        LLMParsedOutput.model_validate(payload)


def test_llm_schema_manage_reduce_pct_bounds() -> None:
    payload = {
        "kind": "MANAGE_ACTION",
        "symbol": "BTCUSDT",
        "side": None,
        "leverage": None,
        "entry": {"type": None, "low": None, "high": None},
        "manage": {"reduce_pct": 30, "move_sl_to_be": True, "tp": [101.2]},
        "confidence": 0.88,
        "notes": "",
    }
    parsed = LLMParsedOutput.model_validate(payload)
    msg = parsed.to_parsed_message("raw", datetime.now(timezone.utc))

    assert isinstance(msg, ManageAction)
    assert msg.reduce_pct == 30
    assert msg.move_sl_to_be is True
    assert msg.tp_price == 101.2
