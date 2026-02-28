from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass

from trader.store import SQLiteStore

TRADE_SIGNAL_KEYWORD_RE = re.compile(r"交易\s*信[号號]", re.IGNORECASE)


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

        thread_id = self.store.resolve_thread_root_by_reply(reply_to_msg_id)
        if thread_id is not None:
            return ThreadResolveResult(thread_id=thread_id, is_root=False, reason="reply_thread_resolved")

        return ThreadResolveResult(thread_id=None, is_root=False, reason="not_thread_message")

    @staticmethod
    def _normalize(text: str) -> str:
        return unicodedata.normalize("NFKC", text or "")
