from __future__ import annotations

from typing import Any

from trader.models import ManageAction
from trader.store import SQLiteStore


def resolve_private_fallback_symbol(
    *,
    latest_thread: dict[str, Any] | None,
    chat_id: int,
    store: SQLiteStore,
) -> str | None:
    if latest_thread is not None:
        symbol = latest_thread.get("symbol")
        return str(symbol) if symbol else None
    return store.get_last_entry_symbol(chat_id)


def should_reject_reply_manage_without_thread_symbol(
    *,
    is_root: bool,
    parsed: ManageAction,
    thread: dict[str, Any] | None,
) -> bool:
    if is_root or parsed.symbol:
        return False
    if thread is None:
        return True
    return not bool(thread.get("symbol"))
