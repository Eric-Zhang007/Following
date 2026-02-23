from __future__ import annotations

import json
import logging
import uuid
from typing import Any

from trader.notifier import Notifier
from trader.store import SQLiteStore

_LEVEL_ORDER = {"INFO": 20, "WARN": 30, "ERROR": 40, "CRITICAL": 50}


class AlertManager:
    def __init__(
        self,
        notifier: Notifier,
        store: SQLiteStore,
        logger: logging.Logger,
        min_level: str = "INFO",
    ) -> None:
        self.notifier = notifier
        self.store = store
        self.logger = logger
        self.min_level = min_level.upper()

    def emit(
        self,
        level: str,
        event_type: str,
        msg: str,
        payload: dict[str, Any] | None = None,
        trace_id: str | None = None,
    ) -> str:
        lvl = level.upper()
        trace = trace_id or uuid.uuid4().hex[:12]
        body = {
            "trace_id": trace,
            "level": lvl,
            "type": event_type,
            "msg": msg,
            "payload": payload or {},
        }
        # Structured JSON line for machine parsing.
        self.logger.log(_LEVEL_ORDER.get(lvl, 20), json.dumps(body, ensure_ascii=False, default=str))
        self.store.record_event(event_type=event_type, level=lvl, msg=msg, payload=payload, trace_id=trace)

        if _LEVEL_ORDER.get(lvl, 20) >= _LEVEL_ORDER.get(self.min_level, 20):
            if lvl in {"ERROR", "CRITICAL"}:
                self.notifier.error(f"[{lvl}] {msg} trace={trace}")
            elif lvl == "WARN":
                self.notifier.warning(f"[{lvl}] {msg} trace={trace}")
            else:
                self.notifier.info(f"[{lvl}] {msg} trace={trace}")
        return trace

    def info(self, event_type: str, msg: str, payload: dict[str, Any] | None = None) -> str:
        return self.emit("INFO", event_type, msg, payload)

    def warn(self, event_type: str, msg: str, payload: dict[str, Any] | None = None) -> str:
        return self.emit("WARN", event_type, msg, payload)

    def error(self, event_type: str, msg: str, payload: dict[str, Any] | None = None) -> str:
        return self.emit("ERROR", event_type, msg, payload)

    def critical(self, event_type: str, msg: str, payload: dict[str, Any] | None = None) -> str:
        return self.emit("CRITICAL", event_type, msg, payload)
