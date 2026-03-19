from __future__ import annotations

import os
from enum import Enum
from pathlib import Path

from trader.store import SQLiteStore


class KillSwitchAction(str, Enum):
    NONE = "NONE"
    SAFE_MODE = "SAFE_MODE"
    PANIC_CLOSE = "PANIC_CLOSE"


class KillSwitch:
    def __init__(
        self,
        store: SQLiteStore,
        file_path: str = "./KILL_SWITCH",
        env_key: str = "TRADER_KILL_SWITCH",
        sqlite_key: str = "kill_switch",
    ) -> None:
        self.store = store
        self.file_path = Path(file_path)
        self.env_key = env_key
        self.sqlite_key = sqlite_key

    def read_action(self) -> KillSwitchAction:
        file_action = self._read_file_action()
        if file_action is not KillSwitchAction.NONE:
            return file_action

        env_value = str(os.getenv(self.env_key, "")).strip().lower()
        if env_value in {"1", "true", "safe", "safe_mode"}:
            return KillSwitchAction.SAFE_MODE
        if env_value in {"panic", "panic_close", "2"}:
            return KillSwitchAction.PANIC_CLOSE

        flag = self.store.get_system_flag(self.sqlite_key)
        if flag:
            normalized = str(flag).strip().lower()
            if normalized in {"safe", "safe_mode", "1", "true"}:
                return KillSwitchAction.SAFE_MODE
            if normalized in {"panic", "panic_close", "2"}:
                return KillSwitchAction.PANIC_CLOSE

        return KillSwitchAction.NONE

    def _read_file_action(self) -> KillSwitchAction:
        if not self.file_path.exists():
            return KillSwitchAction.NONE

        content = self.file_path.read_text(encoding="utf-8").strip().lower()
        if content in {"", "safe", "safe_mode", "1", "true"}:
            return KillSwitchAction.SAFE_MODE
        if content in {"panic", "panic_close", "2"}:
            return KillSwitchAction.PANIC_CLOSE
        return KillSwitchAction.SAFE_MODE
