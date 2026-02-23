from __future__ import annotations

import asyncio
import json
from datetime import datetime

from trader.config import AppConfig
from trader.state import StateStore, utc_now


class HealthServer:
    def __init__(self, config: AppConfig, state: StateStore) -> None:
        self.config = config
        self.state = state
        self._server: asyncio.AbstractServer | None = None

    async def start(self) -> None:
        self._server = await asyncio.start_server(
            self._handle_client,
            host=self.config.monitor.health.host,
            port=self.config.monitor.health.port,
        )

    async def run(self, stop_event: asyncio.Event) -> None:
        await self.start()
        assert self._server is not None
        async with self._server:
            await stop_event.wait()

    async def stop(self) -> None:
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None

    async def _handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            request_line = await reader.readline()
            line = request_line.decode("utf-8", errors="ignore").strip()
            parts = line.split()
            path = parts[1] if len(parts) >= 2 else "/healthz"

            if path == "/healthz":
                await self._write_json(writer, 200, {"status": "ok"})
            elif path == "/readyz":
                ready_payload = self._ready_payload()
                status = 200 if ready_payload["ready"] else 503
                await self._write_json(writer, status, ready_payload)
            elif path == "/metrics" and self.config.monitor.health.enable_metrics:
                await self._write_metrics(writer)
            else:
                await self._write_json(writer, 404, {"error": "not found"})
        except Exception:
            try:
                await self._write_json(writer, 500, {"error": "internal"})
            except Exception:  # noqa: BLE001
                pass
        finally:
            writer.close()
            await writer.wait_closed()

    async def _write_json(self, writer: asyncio.StreamWriter, status: int, payload: dict) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        head = (
            f"HTTP/1.1 {status} {'OK' if status < 400 else 'ERROR'}\r\n"
            "Content-Type: application/json\r\n"
            f"Content-Length: {len(body)}\r\n"
            "Connection: close\r\n\r\n"
        ).encode("utf-8")
        writer.write(head + body)
        await writer.drain()

    async def _write_metrics(self, writer: asyncio.StreamWriter) -> None:
        lines = []
        snap = self.state.to_snapshot()
        metrics = snap.get("metrics", {})
        for key in ["account_equity", "open_positions", "api_errors", "sl_missing_count", "circuit_breaker_state"]:
            value = float(metrics.get(key, 0.0))
            lines.append(f"trader_{key} {value}")
        body = ("\n".join(lines) + "\n").encode("utf-8")
        head = (
            "HTTP/1.1 200 OK\r\n"
            "Content-Type: text/plain; version=0.0.4\r\n"
            f"Content-Length: {len(body)}\r\n"
            "Connection: close\r\n\r\n"
        ).encode("utf-8")
        writer.write(head + body)
        await writer.drain()

    def _ready_payload(self) -> dict:
        now = utc_now()
        pi = self.config.monitor.poll_intervals
        max_stale = self.config.monitor.price_feed.max_stale_seconds
        sl_covered = self._sl_covered()
        checks = {
            "account": _is_fresh(self.state.last_account_ok_at, pi.account_seconds, now),
            "positions": _is_fresh(self.state.last_positions_ok_at, pi.positions_seconds, now),
            "orders": _is_fresh(self.state.last_orders_ok_at, pi.open_orders_seconds, now),
            "price": _is_fresh(self.state.last_price_ok_at, max_stale, now),
            "sl_covered": sl_covered,
            "ws_mode_or_rest": (self.state.price_feed_mode == "ws" and not self.state.price_feed_degraded)
            or self.state.price_feed_mode == "rest",
        }
        return {
            "ready": all(checks.values()),
            "checks": checks,
            "safe_mode": self.state.safe_mode,
            "panic_mode": self.state.panic_mode,
            "reason": self.state.block_new_entries_reason,
            "price_feed_mode": self.state.price_feed_mode,
            "price_feed_degraded": self.state.price_feed_degraded,
        }

    def _sl_covered(self) -> bool:
        if not self.state.positions:
            return True
        for pos in self.state.positions.values():
            if not self.state.has_valid_stop_loss(pos.symbol, pos.side):
                return False
        return True


def _is_fresh(last_ok: datetime | None, interval_seconds: int, now: datetime) -> bool:
    if last_ok is None:
        return False
    return (now - last_ok).total_seconds() <= max(interval_seconds, 1)
