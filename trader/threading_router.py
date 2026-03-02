from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass

from trader.store import SQLiteStore

TRADE_SIGNAL_KEYWORD_RE = re.compile(r"交易\s*信[号號]", re.IGNORECASE)
SYMBOL_HASH_RE = re.compile(r"#\s*[A-Za-z0-9]{1,20}(?:\s*/\s*USDT)?", re.IGNORECASE)
ENTRY_HINT_RE = re.compile(r"(?:進場位|进场位|入場位|入场位|進場|进场)", re.IGNORECASE)
TP_HINT_RE = re.compile(r"(?:盈利位|止盈位|盈利|止盈)", re.IGNORECASE)
SL_HINT_RE = re.compile(r"(?:止損位|止损位|止損|止损|SL)", re.IGNORECASE)


@dataclass
class ThreadResolveResult:
    thread_id: int | None
    is_root: bool
    reason: str


class TradeThreadRouter:
    def __init__(self, store: SQLiteStore) -> None:
        self.store = store

    def resolve(self, *, message_id: int, text: str, reply_to_msg_id: int | None) -> ThreadResolveResult:
        normalized = self._normalize(text)
        if TRADE_SIGNAL_KEYWORD_RE.search(normalized):
            return ThreadResolveResult(thread_id=message_id, is_root=True, reason="root_keyword_detected")
        if self._looks_like_standalone_entry_signal(normalized):
            return ThreadResolveResult(thread_id=message_id, is_root=True, reason="root_structure_detected")

        thread_id = self.store.resolve_thread_root_by_reply(reply_to_msg_id)
        if thread_id is not None:
            return ThreadResolveResult(thread_id=thread_id, is_root=False, reason="reply_thread_resolved")

        return ThreadResolveResult(thread_id=None, is_root=False, reason="not_thread_message")

    @staticmethod
    def _normalize(text: str) -> str:
        return unicodedata.normalize("NFKC", text or "")

    @staticmethod
    def _looks_like_standalone_entry_signal(text: str) -> bool:
        if not SYMBOL_HASH_RE.search(text):
            return False
        # Accept root signals that carry core trading structure even without the "交易信号" banner.
        has_entry = ENTRY_HINT_RE.search(text) is not None
        has_tp = TP_HINT_RE.search(text) is not None
        has_sl = SL_HINT_RE.search(text) is not None
        return has_entry and (has_tp or has_sl)
