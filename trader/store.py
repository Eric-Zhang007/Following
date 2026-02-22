from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import asdict, dataclass, is_dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from trader.models import ParsedMessage


@dataclass
class MessageRecordResult:
    duplicate: bool
    version: int
    text_changed: bool
    text_hash: str


class SQLiteStore:
    def __init__(self, db_path: str) -> None:
        path = Path(db_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._init_schema()

    def _init_schema(self) -> None:
        cur = self.conn.cursor()
        cur.executescript(
            """
            PRAGMA journal_mode=WAL;

            CREATE TABLE IF NOT EXISTS message_state (
                chat_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL,
                last_hash TEXT NOT NULL,
                latest_version INTEGER NOT NULL,
                first_seen TEXT NOT NULL,
                last_seen TEXT NOT NULL,
                PRIMARY KEY(chat_id, message_id)
            );

            CREATE TABLE IF NOT EXISTS message_versions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL,
                version INTEGER NOT NULL,
                is_edit INTEGER NOT NULL,
                text_hash TEXT NOT NULL,
                text TEXT,
                event_time TEXT,
                created_at TEXT NOT NULL,
                UNIQUE(chat_id, message_id, version)
            );

            CREATE TABLE IF NOT EXISTS parsed_signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL,
                version INTEGER NOT NULL,
                signal_type TEXT NOT NULL,
                symbol TEXT,
                side TEXT,
                parse_source TEXT,
                confidence REAL,
                payload_json TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS llm_parses (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL,
                version INTEGER NOT NULL,
                text_hash TEXT NOT NULL,
                provider TEXT NOT NULL,
                model TEXT NOT NULL,
                raw_text TEXT,
                sanitized_text TEXT,
                response_json TEXT NOT NULL,
                kind TEXT,
                confidence REAL,
                created_at TEXT NOT NULL,
                UNIQUE(chat_id, message_id, version, text_hash)
            );

            CREATE TABLE IF NOT EXISTS executions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL,
                version INTEGER NOT NULL,
                action_type TEXT NOT NULL,
                symbol TEXT,
                side TEXT,
                status TEXT NOT NULL,
                reason TEXT,
                intent_json TEXT,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS order_receipts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                execution_id INTEGER NOT NULL,
                exchange_order_id TEXT,
                payload_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY(execution_id) REFERENCES executions(id)
            );
            """
        )

        # Lightweight migration support for older DBs.
        self._ensure_column("parsed_signals", "parse_source", "TEXT")
        self._ensure_column("parsed_signals", "confidence", "REAL")

        self.conn.commit()

    def _ensure_column(self, table: str, column: str, column_type: str) -> None:
        cur = self.conn.cursor()
        cur.execute(f"PRAGMA table_info({table})")
        existing = {str(row[1]) for row in cur.fetchall()}
        if column not in existing:
            cur.execute(f"ALTER TABLE {table} ADD COLUMN {column} {column_type}")

    def record_message(
        self,
        chat_id: int,
        message_id: int,
        text: str,
        is_edit: bool,
        event_time: datetime | None,
    ) -> MessageRecordResult:
        now = self._now_iso()
        text_hash = hashlib.sha256((text or "").encode("utf-8")).hexdigest()

        cur = self.conn.cursor()
        cur.execute(
            "SELECT last_hash, latest_version FROM message_state WHERE chat_id=? AND message_id=?",
            (chat_id, message_id),
        )
        row = cur.fetchone()

        if row is None:
            version = 1
            cur.execute(
                """
                INSERT INTO message_state(chat_id, message_id, last_hash, latest_version, first_seen, last_seen)
                VALUES(?,?,?,?,?,?)
                """,
                (chat_id, message_id, text_hash, version, now, now),
            )
            self._insert_message_version(chat_id, message_id, version, is_edit, text_hash, text, event_time)
            self.conn.commit()
            return MessageRecordResult(duplicate=False, version=version, text_changed=True, text_hash=text_hash)

        if row["last_hash"] == text_hash:
            cur.execute(
                "UPDATE message_state SET last_seen=? WHERE chat_id=? AND message_id=?",
                (now, chat_id, message_id),
            )
            self.conn.commit()
            return MessageRecordResult(
                duplicate=True,
                version=int(row["latest_version"]),
                text_changed=False,
                text_hash=text_hash,
            )

        version = int(row["latest_version"]) + 1
        cur.execute(
            """
            UPDATE message_state
            SET last_hash=?, latest_version=?, last_seen=?
            WHERE chat_id=? AND message_id=?
            """,
            (text_hash, version, now, chat_id, message_id),
        )
        self._insert_message_version(chat_id, message_id, version, is_edit, text_hash, text, event_time)
        self.conn.commit()
        return MessageRecordResult(duplicate=False, version=version, text_changed=True, text_hash=text_hash)

    def _insert_message_version(
        self,
        chat_id: int,
        message_id: int,
        version: int,
        is_edit: bool,
        text_hash: str,
        text: str,
        event_time: datetime | None,
    ) -> None:
        self.conn.execute(
            """
            INSERT INTO message_versions(chat_id, message_id, version, is_edit, text_hash, text, event_time, created_at)
            VALUES(?,?,?,?,?,?,?,?)
            """,
            (
                chat_id,
                message_id,
                version,
                1 if is_edit else 0,
                text_hash,
                text,
                self._iso(event_time),
                self._now_iso(),
            ),
        )

    def record_parsed_signal(
        self,
        chat_id: int,
        message_id: int,
        version: int,
        parsed: ParsedMessage,
        parse_source: str = "RULES",
        confidence: float | None = None,
    ) -> None:
        payload = self._json(parsed)
        symbol = payload.get("symbol")
        kind = payload.get("kind", "UNKNOWN")
        if hasattr(kind, "value"):
            kind = kind.value

        side = payload.get("side")
        if isinstance(side, dict) and "value" in side:
            side_value = side["value"]
        elif hasattr(side, "value"):
            side_value = side.value
        else:
            side_value = side

        self.conn.execute(
            """
            INSERT INTO parsed_signals(
                chat_id, message_id, version, signal_type, symbol, side, parse_source, confidence, payload_json, created_at
            )
            VALUES(?,?,?,?,?,?,?,?,?,?)
            """,
            (
                chat_id,
                message_id,
                version,
                kind,
                symbol,
                side_value,
                parse_source,
                confidence,
                json.dumps(payload, ensure_ascii=False, default=str),
                self._now_iso(),
            ),
        )
        self.conn.commit()

    def get_llm_parse_cache(
        self,
        chat_id: int,
        message_id: int,
        version: int,
        text_hash: str,
    ) -> dict[str, Any] | None:
        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT response_json
            FROM llm_parses
            WHERE chat_id=? AND message_id=? AND version=? AND text_hash=?
            LIMIT 1
            """,
            (chat_id, message_id, version, text_hash),
        )
        row = cur.fetchone()
        if row is None:
            return None
        return json.loads(row["response_json"])

    def save_llm_parse(
        self,
        chat_id: int,
        message_id: int,
        version: int,
        text_hash: str,
        provider: str,
        model: str,
        raw_text: str,
        sanitized_text: str,
        response_payload: dict[str, Any],
    ) -> None:
        self.conn.execute(
            """
            INSERT OR REPLACE INTO llm_parses(
                chat_id, message_id, version, text_hash, provider, model, raw_text, sanitized_text,
                response_json, kind, confidence, created_at
            )
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                chat_id,
                message_id,
                version,
                text_hash,
                provider,
                model,
                raw_text,
                sanitized_text,
                json.dumps(response_payload, ensure_ascii=False, default=str),
                response_payload.get("kind"),
                float(response_payload.get("confidence", 0.0)),
                self._now_iso(),
            ),
        )
        self.conn.commit()

    def record_execution(
        self,
        chat_id: int,
        message_id: int,
        version: int,
        action_type: str,
        symbol: str | None,
        side: str | None,
        status: str,
        reason: str | None,
        intent: dict[str, Any] | None,
    ) -> int:
        cur = self.conn.cursor()
        cur.execute(
            """
            INSERT INTO executions(chat_id, message_id, version, action_type, symbol, side, status, reason, intent_json, created_at)
            VALUES(?,?,?,?,?,?,?,?,?,?)
            """,
            (
                chat_id,
                message_id,
                version,
                action_type,
                symbol,
                side,
                status,
                reason,
                json.dumps(intent, ensure_ascii=False, default=str) if intent is not None else None,
                self._now_iso(),
            ),
        )
        self.conn.commit()
        return int(cur.lastrowid)

    def record_order_receipt(self, execution_id: int, exchange_order_id: str | None, payload: Any) -> None:
        self.conn.execute(
            """
            INSERT INTO order_receipts(execution_id, exchange_order_id, payload_json, created_at)
            VALUES(?,?,?,?)
            """,
            (execution_id, exchange_order_id, json.dumps(payload, ensure_ascii=False, default=str), self._now_iso()),
        )
        self.conn.commit()

    def within_cooldown(self, symbol: str, side: str, cooldown_seconds: int, now: datetime) -> bool:
        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT created_at
            FROM executions
            WHERE symbol=? AND side=? AND status IN ('EXECUTED', 'DRY_RUN')
            ORDER BY id DESC
            LIMIT 1
            """,
            (symbol, side),
        )
        row = cur.fetchone()
        if row is None:
            return False

        last_at = datetime.fromisoformat(row["created_at"])
        if last_at.tzinfo is None:
            last_at = last_at.replace(tzinfo=timezone.utc)
        return (now - last_at).total_seconds() < cooldown_seconds

    def get_last_entry_symbol(self, chat_id: int) -> str | None:
        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT symbol
            FROM parsed_signals
            WHERE chat_id=? AND signal_type='ENTRY_SIGNAL' AND symbol IS NOT NULL
            ORDER BY id DESC
            LIMIT 1
            """,
            (chat_id,),
        )
        row = cur.fetchone()
        return str(row["symbol"]) if row else None

    def close(self) -> None:
        self.conn.close()

    @staticmethod
    def _json(payload: Any) -> dict[str, Any]:
        if is_dataclass(payload):
            return asdict(payload)
        if isinstance(payload, dict):
            return payload
        raise TypeError(f"cannot serialize payload type: {type(payload)}")

    @staticmethod
    def _now_iso() -> str:
        return datetime.now(timezone.utc).isoformat()

    @staticmethod
    def _iso(dt: datetime | None) -> str | None:
        if dt is None:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.isoformat()
