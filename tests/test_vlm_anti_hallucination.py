import pytest
from pydantic import ValidationError

from trader.vlm_schema import VLMParsedSignal


def test_unclear_price_sets_entry_fields_null_and_uncertain() -> None:
    payload = {
        "kind": "ENTRY_SIGNAL",
        "symbol": "AKTUSDT",
        "side": "LONG",
        "leverage": 50,
        "entry": {"type": "MARKET", "low": None, "high": None, "stop_loss": None, "tp": []},
        "manage": {"reduce_pct": None, "move_sl_to_be": False, "tp": []},
        "evidence": {
            "field_evidence": {
                "symbol": ["#AKT/USDT"],
                "side": ["做多"],
            },
            "source": {
                "symbol": "text",
                "side": "text",
            },
        },
        "uncertain_fields": ["entry.low", "entry.high"],
        "extraction_warnings": ["image_low_resolution"],
        "safety": {"should_trade": "NO_DECISION"},
        "confidence": 0.6,
        "notes": "price unreadable",
    }
    parsed = VLMParsedSignal.model_validate(payload)
    assert parsed.entry.low is None
    assert parsed.entry.high is None
    assert "entry.low" in parsed.uncertain_fields
    assert "entry.high" in parsed.uncertain_fields


def test_missing_symbol_keeps_symbol_null() -> None:
    payload = {
        "kind": "ENTRY_SIGNAL",
        "symbol": None,
        "side": "SHORT",
        "leverage": None,
        "entry": {"type": "LIMIT", "low": 0.73, "high": 0.74, "stop_loss": None, "tp": []},
        "manage": {"reduce_pct": None, "move_sl_to_be": False, "tp": []},
        "evidence": {
            "field_evidence": {
                "side": ["做空"],
                "entry.low": ["0.73"],
                "entry.high": ["0.74"],
            },
            "source": {
                "side": "text",
                "entry.low": "text",
                "entry.high": "text",
            },
        },
        "uncertain_fields": ["symbol"],
        "extraction_warnings": ["symbol_missing"],
        "safety": {"should_trade": "NO_DECISION"},
        "confidence": 0.4,
        "notes": "symbol not visible",
    }
    parsed = VLMParsedSignal.model_validate(payload)
    assert parsed.symbol is None


def test_non_null_field_without_evidence_is_rejected() -> None:
    payload = {
        "kind": "ENTRY_SIGNAL",
        "symbol": "BTCUSDT",
        "side": "LONG",
        "leverage": None,
        "entry": {"type": "LIMIT", "low": 100, "high": 101, "stop_loss": 99, "tp": []},
        "manage": {"reduce_pct": None, "move_sl_to_be": False, "tp": []},
        "evidence": {
            "field_evidence": {
                "symbol": ["#BTC/USDT"],
                # missing side / entry.low / entry.high evidence on purpose
            },
            "source": {
                "symbol": "text",
            },
        },
        "uncertain_fields": [],
        "extraction_warnings": [],
        "safety": {"should_trade": "NO_DECISION"},
        "confidence": 0.9,
        "notes": "",
    }
    with pytest.raises(ValidationError):
        VLMParsedSignal.model_validate(payload)
