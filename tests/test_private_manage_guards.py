from datetime import datetime, timezone

from trader.models import EntrySignal, EntryType, ManageAction, ParsedKind, Side
from trader.private_manage_guards import resolve_private_fallback_symbol, should_reject_reply_manage_without_thread_symbol
from trader.store import SQLiteStore


def _entry_signal(symbol: str) -> EntrySignal:
    return EntrySignal(
        kind=ParsedKind.ENTRY_SIGNAL,
        raw_text=f"#{symbol}",
        symbol=symbol,
        quote="USDT",
        side=Side.LONG,
        leverage=20,
        entry_type=EntryType.LIMIT,
        entry_low=1.0,
        entry_high=1.0,
        entry_points=[1.0],
        stop_loss=0.9,
        tp_points=[1.1],
        timestamp=datetime(2026, 3, 16, 2, 34, 10, tzinfo=timezone.utc),
        thread_id=123,
    )


def _manage_action(symbol: str | None = None) -> ManageAction:
    return ManageAction(
        kind=ParsedKind.MANAGE_ACTION,
        raw_text="市價結束持倉",
        symbol=symbol,
        reduce_pct=100.0,
        move_sl_to_be=False,
        tp_price=None,
        add_pct=None,
        tp_points=[],
        stop_loss=None,
        note="",
        timestamp=datetime(2026, 3, 16, 6, 5, 51, tzinfo=timezone.utc),
        thread_id=101,
    )


def test_resolve_private_fallback_symbol_does_not_use_last_entry_when_thread_exists_without_symbol(tmp_path) -> None:
    store = SQLiteStore(str(tmp_path / "private_fallback_guard.db"))
    store.record_parsed_signal(
        chat_id=-1003831751615,
        message_id=123,
        version=1,
        parsed=_entry_signal("ETHUSDT"),
        parse_source="RULES_PRIVATE",
        confidence=1.0,
    )

    fallback = resolve_private_fallback_symbol(
        latest_thread={"thread_id": 101, "symbol": None},
        chat_id=-1003831751615,
        store=store,
    )
    assert fallback is None

    last_entry_fallback = resolve_private_fallback_symbol(
        latest_thread=None,
        chat_id=-1003831751615,
        store=store,
    )
    assert last_entry_fallback == "ETHUSDT"


def test_reply_manage_requires_resolved_thread_symbol() -> None:
    assert should_reject_reply_manage_without_thread_symbol(
        is_root=False,
        parsed=_manage_action(symbol=None),
        thread={"thread_id": 101, "symbol": None},
    )
    assert not should_reject_reply_manage_without_thread_symbol(
        is_root=False,
        parsed=_manage_action(symbol="ETHUSDT"),
        thread={"thread_id": 101, "symbol": None},
    )
    assert not should_reject_reply_manage_without_thread_symbol(
        is_root=False,
        parsed=_manage_action(symbol=None),
        thread={"thread_id": 101, "symbol": "ETHUSDT"},
    )
    assert not should_reject_reply_manage_without_thread_symbol(
        is_root=True,
        parsed=_manage_action(symbol=None),
        thread={"thread_id": 101, "symbol": None},
    )
