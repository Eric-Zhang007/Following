from trader.entry_fallback import convert_market_to_limit_signal, is_market_slippage_reject
from trader.models import EntrySignal, EntryType, ParsedKind, Side


def _signal_market(**kwargs) -> EntrySignal:
    payload = {
        "kind": ParsedKind.ENTRY_SIGNAL,
        "raw_text": "x",
        "symbol": "ZROUSDT",
        "quote": "USDT",
        "side": Side.LONG,
        "leverage": 20,
        "entry_type": EntryType.MARKET,
        "entry_low": 0.0,
        "entry_high": 0.0,
        "entry_points": [],
    }
    payload.update(kwargs)
    return EntrySignal(**payload)


def test_is_market_slippage_reject_matches_reason() -> None:
    assert is_market_slippage_reject("market anchor deviation 0.0143 exceeds max_entry_slippage_pct 0.0010")
    assert not is_market_slippage_reject("symbol not in whitelist")
    assert not is_market_slippage_reject(None)


def test_convert_market_to_limit_with_entry_points() -> None:
    signal = _signal_market(entry_points=[1.8149, 1.778], entry_low=1.778, entry_high=1.8149)
    out = convert_market_to_limit_signal(signal)
    assert out is not None
    assert out.entry_type == EntryType.LIMIT
    assert out.entry_points == [1.8149, 1.778]
    assert out.entry_low == 1.778
    assert out.entry_high == 1.8149


def test_convert_market_to_limit_uses_low_high_when_points_missing() -> None:
    signal = _signal_market(entry_low=1.778, entry_high=1.8149, entry_points=[])
    out = convert_market_to_limit_signal(signal)
    assert out is not None
    assert out.entry_type == EntryType.LIMIT
    assert out.entry_points == [1.778, 1.8149]


def test_convert_market_to_limit_returns_none_when_no_anchor() -> None:
    signal = _signal_market(entry_low=0.0, entry_high=0.0, entry_points=[])
    assert convert_market_to_limit_signal(signal) is None
