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
                thread_id INTEGER,
                action_type TEXT NOT NULL,
                purpose TEXT,
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

            CREATE TABLE IF NOT EXISTS media_assets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sha256 TEXT NOT NULL UNIQUE,
                source_url TEXT,
                local_path TEXT NOT NULL,
                mime_type TEXT,
                size_bytes INTEGER,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS message_media (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL,
                version INTEGER NOT NULL,
                sha256 TEXT NOT NULL,
                source_url TEXT,
                created_at TEXT NOT NULL,
                UNIQUE(chat_id, message_id, version, sha256),
                FOREIGN KEY(sha256) REFERENCES media_assets(sha256)
            );

            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                type TEXT NOT NULL,
                level TEXT NOT NULL,
                msg TEXT NOT NULL,
                reason TEXT,
                thread_id INTEGER,
                payload_json TEXT,
                trace_id TEXT,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS equity_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                equity REAL NOT NULL,
                available REAL,
                margin_used REAL,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS invariants_violations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                invariant_name TEXT NOT NULL,
                symbol TEXT,
                reason TEXT NOT NULL,
                payload_json TEXT,
                trace_id TEXT,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS reconciler_actions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                thread_id INTEGER,
                symbol TEXT,
                order_id TEXT,
                client_order_id TEXT,
                action TEXT NOT NULL,
                purpose TEXT,
                reason TEXT,
                payload_json TEXT,
                trace_id TEXT,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS trade_threads (
                thread_id INTEGER PRIMARY KEY,
                symbol TEXT,
                side TEXT,
                leverage INTEGER,
                stop_loss REAL,
                entry_points_json TEXT,
                tp_points_json TEXT,
                target_version INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                status TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS thread_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                thread_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL UNIQUE,
                is_root INTEGER NOT NULL,
                kind TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY(thread_id) REFERENCES trade_threads(thread_id)
            );

            CREATE TABLE IF NOT EXISTS runtime_state_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                state_json TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS system_flags (
                key TEXT PRIMARY KEY,
                value TEXT,
                updated_at TEXT NOT NULL
            );
            """
        )

        # Lightweight migration support for older DBs.
        self._ensure_column("parsed_signals", "parse_source", "TEXT")
        self._ensure_column("parsed_signals", "confidence", "REAL")
        self._ensure_column("executions", "thread_id", "INTEGER")
        self._ensure_column("executions", "purpose", "TEXT")
        self._ensure_column("events", "reason", "TEXT")
        self._ensure_column("events", "thread_id", "INTEGER")
        self._ensure_column("reconciler_actions", "thread_id", "INTEGER")
        self._ensure_column("reconciler_actions", "purpose", "TEXT")
        self._ensure_column("trade_threads", "target_version", "INTEGER NOT NULL DEFAULT 1")
        self._ensure_column("trade_threads", "stop_loss", "REAL")
        self._ensure_column("trade_threads", "entry_points_json", "TEXT")
        self._ensure_column("trade_threads", "tp_points_json", "TEXT")

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
        thread_id: int | None = None,
        purpose: str | None = None,
    ) -> int:
        cur = self.conn.cursor()
        cur.execute(
            """
            INSERT INTO executions(
                chat_id, message_id, version, thread_id, action_type, purpose, symbol, side, status, reason, intent_json, created_at
            )
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                chat_id,
                message_id,
                version,
                thread_id,
                action_type,
                purpose,
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

    def record_event(
        self,
        event_type: str,
        level: str,
        msg: str,
        payload: dict[str, Any] | None = None,
        trace_id: str | None = None,
        reason: str | None = None,
        thread_id: int | None = None,
    ) -> int:
        cur = self.conn.cursor()
        cur.execute(
            """
            INSERT INTO events(type, level, msg, reason, thread_id, payload_json, trace_id, created_at)
            VALUES(?,?,?,?,?,?,?,?)
            """,
            (
                event_type,
                level,
                msg,
                reason,
                thread_id,
                json.dumps(payload, ensure_ascii=False, default=str) if payload is not None else None,
                trace_id,
                self._now_iso(),
            ),
        )
        self.conn.commit()
        return int(cur.lastrowid)

    def snapshot_equity(self, equity: float, available: float | None, margin_used: float | None) -> None:
        self.conn.execute(
            """
            INSERT INTO equity_snapshots(equity, available, margin_used, created_at)
            VALUES(?,?,?,?)
            """,
            (equity, available, margin_used, self._now_iso()),
        )
        self.conn.commit()

    def record_invariant_violation(
        self,
        invariant_name: str,
        symbol: str | None,
        reason: str,
        payload: dict[str, Any] | None = None,
        trace_id: str | None = None,
    ) -> int:
        cur = self.conn.cursor()
        cur.execute(
            """
            INSERT INTO invariants_violations(invariant_name, symbol, reason, payload_json, trace_id, created_at)
            VALUES(?,?,?,?,?,?)
            """,
            (
                invariant_name,
                symbol,
                reason,
                json.dumps(payload, ensure_ascii=False, default=str) if payload is not None else None,
                trace_id,
                self._now_iso(),
            ),
        )
        self.conn.commit()
        return int(cur.lastrowid)

    def record_reconciler_action(
        self,
        symbol: str | None,
        order_id: str | None,
        client_order_id: str | None,
        action: str,
        reason: str | None = None,
        payload: dict[str, Any] | None = None,
        trace_id: str | None = None,
        thread_id: int | None = None,
        purpose: str | None = None,
    ) -> int:
        cur = self.conn.cursor()
        cur.execute(
            """
            INSERT INTO reconciler_actions(
                thread_id, symbol, order_id, client_order_id, action, purpose, reason, payload_json, trace_id, created_at
            )
            VALUES(?,?,?,?,?,?,?,?,?,?)
            """,
            (
                thread_id,
                symbol,
                order_id,
                client_order_id,
                action,
                purpose,
                reason,
                json.dumps(payload, ensure_ascii=False, default=str) if payload is not None else None,
                trace_id,
                self._now_iso(),
            ),
        )
        self.conn.commit()
        return int(cur.lastrowid)

    def save_runtime_snapshot(self, state_payload: dict[str, Any]) -> None:
        self.conn.execute(
            """
            INSERT INTO runtime_state_snapshots(state_json, created_at)
            VALUES(?,?)
            """,
            (json.dumps(state_payload, ensure_ascii=False, default=str), self._now_iso()),
        )
        self.conn.commit()

    def set_system_flag(self, key: str, value: str | None) -> None:
        self.conn.execute(
            """
            INSERT INTO system_flags(key, value, updated_at) VALUES(?,?,?)
            ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at
            """,
            (key, value, self._now_iso()),
        )
        self.conn.commit()

    def get_system_flag(self, key: str) -> str | None:
        cur = self.conn.cursor()
        cur.execute("SELECT value FROM system_flags WHERE key=? LIMIT 1", (key,))
        row = cur.fetchone()
        if row is None:
            return None
        return str(row["value"]) if row["value"] is not None else None

    def get_recent_equity_max(self) -> float | None:
        cur = self.conn.cursor()
        cur.execute("SELECT MAX(equity) AS max_equity FROM equity_snapshots")
        row = cur.fetchone()
        if row is None or row["max_equity"] is None:
            return None
        return float(row["max_equity"])

    def get_media_by_sha256(self, sha256: str) -> dict[str, Any] | None:
        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT sha256, source_url, local_path, mime_type, size_bytes, created_at
            FROM media_assets
            WHERE sha256=?
            LIMIT 1
            """,
            (sha256,),
        )
        row = cur.fetchone()
        if row is None:
            return None
        return {
            "sha256": row["sha256"],
            "source_url": row["source_url"],
            "local_path": row["local_path"],
            "mime_type": row["mime_type"],
            "size_bytes": row["size_bytes"],
            "created_at": row["created_at"],
        }

    def save_media_asset(
        self,
        sha256: str,
        source_url: str | None,
        local_path: str,
        mime_type: str | None,
        size_bytes: int,
    ) -> None:
        self.conn.execute(
            """
            INSERT OR IGNORE INTO media_assets(sha256, source_url, local_path, mime_type, size_bytes, created_at)
            VALUES(?,?,?,?,?,?)
            """,
            (sha256, source_url, local_path, mime_type, size_bytes, self._now_iso()),
        )
        self.conn.commit()

    def link_message_media(
        self,
        chat_id: int,
        message_id: int,
        version: int,
        sha256: str,
        source_url: str | None,
    ) -> None:
        self.conn.execute(
            """
            INSERT OR IGNORE INTO message_media(chat_id, message_id, version, sha256, source_url, created_at)
            VALUES(?,?,?,?,?,?)
            """,
            (chat_id, message_id, version, sha256, source_url, self._now_iso()),
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

    def upsert_trade_thread(
        self,
        *,
        thread_id: int,
        symbol: str | None,
        side: str | None,
        leverage: int | None,
        stop_loss: float | None = None,
        entry_points: list[float] | None = None,
        tp_points: list[float] | None = None,
        status: str = "ACTIVE",
        target_version: int = 1,
    ) -> None:
        now = self._now_iso()
        self.conn.execute(
            """
            INSERT INTO trade_threads(
                thread_id, symbol, side, leverage, stop_loss, entry_points_json, tp_points_json,
                target_version, created_at, updated_at, status
            )
            VALUES(?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(thread_id) DO UPDATE SET
                symbol=COALESCE(excluded.symbol, trade_threads.symbol),
                side=COALESCE(excluded.side, trade_threads.side),
                leverage=COALESCE(excluded.leverage, trade_threads.leverage),
                stop_loss=COALESCE(excluded.stop_loss, trade_threads.stop_loss),
                entry_points_json=COALESCE(excluded.entry_points_json, trade_threads.entry_points_json),
                tp_points_json=COALESCE(excluded.tp_points_json, trade_threads.tp_points_json),
                target_version=MAX(excluded.target_version, trade_threads.target_version),
                updated_at=excluded.updated_at,
                status=excluded.status
            """,
            (
                thread_id,
                symbol,
                side,
                leverage,
                stop_loss,
                json.dumps(entry_points, ensure_ascii=False, default=str) if entry_points is not None else None,
                json.dumps(tp_points, ensure_ascii=False, default=str) if tp_points is not None else None,
                int(target_version),
                now,
                now,
                status,
            ),
        )
        self.conn.commit()

    def get_trade_thread(self, thread_id: int) -> dict[str, Any] | None:
        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT
                thread_id, symbol, side, leverage, stop_loss, entry_points_json, tp_points_json,
                target_version, created_at, updated_at, status
            FROM trade_threads
            WHERE thread_id=?
            LIMIT 1
            """,
            (thread_id,),
        )
        row = cur.fetchone()
        if row is None:
            return None
        return {
            "thread_id": int(row["thread_id"]),
            "symbol": row["symbol"],
            "side": row["side"],
            "leverage": row["leverage"],
            "stop_loss": row["stop_loss"],
            "entry_points": json.loads(row["entry_points_json"]) if row["entry_points_json"] else [],
            "tp_points": json.loads(row["tp_points_json"]) if row["tp_points_json"] else [],
            "target_version": int(row["target_version"]),
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
            "status": row["status"],
        }

    def set_trade_thread_status(self, thread_id: int, status: str) -> None:
        self.conn.execute(
            "UPDATE trade_threads SET status=?, updated_at=? WHERE thread_id=?",
            (status, self._now_iso(), thread_id),
        )
        self.conn.commit()

    def bump_trade_thread_version(self, thread_id: int) -> int:
        cur = self.conn.cursor()
        cur.execute(
            "SELECT target_version FROM trade_threads WHERE thread_id=? LIMIT 1",
            (thread_id,),
        )
        row = cur.fetchone()
        if row is None:
            version = 1
            self.upsert_trade_thread(
                thread_id=thread_id,
                symbol=None,
                side=None,
                leverage=None,
                status="ACTIVE",
                target_version=version,
            )
            return version
        version = int(row["target_version"]) + 1
        self.conn.execute(
            "UPDATE trade_threads SET target_version=?, updated_at=? WHERE thread_id=?",
            (version, self._now_iso(), thread_id),
        )
        self.conn.commit()
        return version

    def count_active_trade_threads(self) -> int:
        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT COUNT(*) AS c
            FROM trade_threads
            WHERE status IN ('ACTIVE', 'PARTIAL', 'PENDING_ENTRY', 'RUNNING')
            """
        )
        row = cur.fetchone()
        return int(row["c"]) if row else 0

    def record_thread_message(
        self,
        *,
        thread_id: int,
        message_id: int,
        is_root: bool,
        kind: str,
    ) -> None:
        self.conn.execute(
            """
            INSERT OR IGNORE INTO thread_messages(thread_id, message_id, is_root, kind, created_at)
            VALUES(?,?,?,?,?)
            """,
            (thread_id, message_id, 1 if is_root else 0, kind, self._now_iso()),
        )
        self.conn.commit()

    def resolve_thread_root_by_message(self, message_id: int) -> int | None:
        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT thread_id
            FROM thread_messages
            WHERE message_id=?
            LIMIT 1
            """,
            (message_id,),
        )
        row = cur.fetchone()
        if row is None:
            return None
        return int(row["thread_id"])

    def resolve_thread_root_by_reply(self, reply_to_msg_id: int | None) -> int | None:
        if reply_to_msg_id is None:
            return None
        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT thread_id
            FROM trade_threads
            WHERE thread_id=?
            LIMIT 1
            """,
            (reply_to_msg_id,),
        )
        row = cur.fetchone()
        if row is not None:
            return int(row["thread_id"])
        return self.resolve_thread_root_by_message(reply_to_msg_id)

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
