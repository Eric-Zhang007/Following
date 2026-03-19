import logging
from datetime import datetime, timezone

from trader.config import AppConfig
from trader.executor import TradeExecutor
from trader.models import EntrySignal, EntryType, ParsedKind, Side
from trader.notifier import Notifier
from trader.private_channel_parser import PrivateChannelParser
from trader.state import StateStore
from trader.store import SQLiteStore


class _FakeBitget:
    pass


def _config() -> AppConfig:
    return AppConfig.model_validate(
        {
            "dry_run": True,
            "listener": {"mode": "telegram_private"},
            "telegram": {
                "api_id": 1,
                "api_hash": "x",
                "session_name": "s",
                "channel_id": -1000000000000,
                "channel": "@IvanCryptotalk",
            },
            "bitget": {
                "base_url": "https://api.bitget.com",
                "api_key": "",
                "api_secret": "",
                "passphrase": "",
                "product_type": "USDT-FUTURES",
            },
            "filters": {
                "symbol_whitelist": ["ZROUSDT"],
                "max_leverage": 100,
                "allow_sides": ["LONG", "SHORT"],
                "max_signal_age_seconds": 20,
                "leverage_over_limit_action": "CLAMP",
            },
            "risk": {
                "account_risk_per_trade": 0.005,
                "max_notional_per_trade": 200,
                "entry_slippage_pct": 0.3,
                "cooldown_seconds": 300,
                "default_stop_loss_pct": 1.0,
                "assumed_equity_usdt": 1000,
            },
            "logging": {"level": "INFO", "file": "trader.log", "rich": False},
            "vlm": {"enabled": False},
        }
    )


def test_apply_thread_edit_ignores_market_entry() -> None:
    store = SQLiteStore(":memory:")
    state = StateStore()
    exe = TradeExecutor(
        config=_config(),
        bitget=_FakeBitget(),  # type: ignore[arg-type]
        store=store,
        notifier=Notifier(logging.getLogger("test")),
        logger=logging.getLogger("test"),
        runtime_state=state,
    )
    signal = EntrySignal(
        kind=ParsedKind.ENTRY_SIGNAL,
        raw_text="#ZRO",
        symbol="ZROUSDT",
        quote="USDT",
        side=Side.LONG,
        leverage=60,
        entry_type=EntryType.MARKET,
        entry_low=1.778,
        entry_high=1.8149,
        entry_points=[1.8149, 1.778],
        stop_loss=1.72,
        tp_points=[1.9062, 1.9994, 2.1234],
    )
    out = exe.apply_thread_edit(signal, chat_id=1, message_id=14, version=2, thread_id=14)
    assert out == {"replaced": 0, "canceled": 0}
    row = store.conn.execute(
        "SELECT id FROM events WHERE type='ENTRY_EDIT_MARKET_IGNORED' ORDER BY id DESC LIMIT 1"
    ).fetchone()
    assert row is not None


def test_duplicate_edit_hash_detectable() -> None:
    store = SQLiteStore(":memory:")
    parser = PrivateChannelParser(_config())
    text = (
        "🖥 交易信號 🖥\n\n"
        "#ZRO（60x做多🚀🚀🚀）\n\n"
        "✏️進場位：市價1.8149附近—1.778\n\n"
        "👁 盈利位：1.9062—1.9994—2.1234\n\n"
        " ❌止損位：1.72"
    )
    ts = datetime(2026, 3, 2, 3, 8, 49, tzinfo=timezone.utc)
    first = store.record_message(chat_id=-1003831751615, message_id=14, text=text, is_edit=False, event_time=ts)
    second = store.record_message(chat_id=-1003831751615, message_id=14, text=text, is_edit=True, event_time=ts)
    assert first.duplicate is False
    assert second.duplicate is True
    parsed = parser.parse(
        text=text,
        timestamp=ts,
        image_path=None,
        fallback_symbol=None,
        thread_id=14,
        is_root=True,
    )
    assert parsed.parsed.kind == ParsedKind.ENTRY_SIGNAL
