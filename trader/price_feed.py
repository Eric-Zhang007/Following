from __future__ import annotations

import asyncio
import json
from datetime import datetime

from trader.alerts import AlertManager
from trader.bitget_client import BitgetClient
from trader.config import AppConfig
from trader.state import PriceSnapshot, StateStore, utc_now


class PriceFeed:
    def __init__(
        self,
        config: AppConfig,
        bitget: BitgetClient,
        state: StateStore,
        alerts: AlertManager,
    ) -> None:
        self.config = config
        self.bitget = bitget
        self.state = state
        self.alerts = alerts

    async def run(self, stop_event: asyncio.Event) -> None:
        requested = self.config.monitor.price_feed.mode
        if requested == "ws":
            ws_ok = await self._run_ws_loop(stop_event)
            if ws_ok:
                return
            self.alerts.warn(
                "PRICE_FEED_WS_FALLBACK",
                "ws mode failed; falling back to rest",
                {
                    "requested_mode": "ws",
                    "active_mode": "rest",
                    "purpose": "price_feed",
                    "reason": "ws_unavailable",
                },
            )
            self._apply_local_guard_fallback_policy()

        await self._run_rest_loop(stop_event)

    async def refresh_once(self) -> None:
        symbols = self._watch_symbols()
        if not symbols:
            self.state.set_price_fresh()
            return

        for symbol in symbols:
            ticker = await asyncio.to_thread(self.bitget.get_ticker, symbol)
            self.state.set_price_snapshot(
                symbol=symbol,
                mark=ticker.get("mark_price"),
                last=ticker.get("last_price"),
                bid=ticker.get("bid_price"),
                ask=ticker.get("ask_price"),
                timestamp=utc_now(),
            )
        self.state.set_price_fresh()

    def get_price(self, symbol: str) -> PriceSnapshot | None:
        return self.state.get_price(symbol)

    async def _run_rest_loop(self, stop_event: asyncio.Event) -> None:
        self.state.set_price_feed_mode(mode="rest", degraded=True)
        interval = self.config.monitor.price_feed.interval_seconds
        while not stop_event.is_set():
            try:
                await self.refresh_once()
            except Exception as exc:  # noqa: BLE001
                self.state.register_api_error()
                self.alerts.error("PRICE_FEED_ERROR", f"price feed refresh failed: {exc}")
            if stop_event.is_set():
                break
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=interval)
            except TimeoutError:
                pass

    async def _run_ws_loop(self, stop_event: asyncio.Event) -> bool:
        try:
            import websockets  # type: ignore
        except Exception:
            return False

        self.state.set_price_feed_mode(mode="ws", degraded=False)
        ws_url = "wss://ws.bitget.com/v2/ws/public"

        while not stop_event.is_set():
            symbols = self._watch_symbols()
            if not symbols:
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=1.0)
                except TimeoutError:
                    pass
                continue

            try:
                async with websockets.connect(ws_url, ping_interval=15, ping_timeout=10, close_timeout=5) as ws:  # type: ignore[attr-defined]
                    subs = [
                        {
                            "instType": self.config.bitget.product_type,
                            "channel": "ticker",
                            "instId": symbol,
                        }
                        for symbol in symbols
                    ]
                    await ws.send(json.dumps({"op": "subscribe", "args": subs}, ensure_ascii=False))

                    while not stop_event.is_set():
                        raw = await asyncio.wait_for(ws.recv(), timeout=self.config.monitor.price_feed.max_stale_seconds)
                        payload = json.loads(raw)
                        if not isinstance(payload, dict):
                            continue
                        data = payload.get("data")
                        if not isinstance(data, list):
                            continue
                        for item in data:
                            symbol = str(item.get("instId") or item.get("symbol") or "").upper()
                            if not symbol:
                                continue
                            mark = _to_float(item.get("markPrice") or item.get("markPr"))
                            last = _to_float(item.get("lastPr") or item.get("last"))
                            bid = _to_float(item.get("bidPr") or item.get("bidPrice"))
                            ask = _to_float(item.get("askPr") or item.get("askPrice"))
                            self.state.set_price_snapshot(
                                symbol=symbol,
                                mark=mark,
                                last=last,
                                bid=bid,
                                ask=ask,
                                timestamp=utc_now(),
                            )
                        self.state.set_price_fresh()
                        self.state.metrics["ws_fresh"] = 1.0

            except Exception as exc:  # noqa: BLE001
                self.state.register_api_error()
                self.state.set_price_feed_mode(mode="rest", degraded=True)
                self.alerts.warn(
                    "PRICE_FEED_WS_RECONNECT",
                    "ws connection interrupted; will reconnect",
                    {
                        "purpose": "price_feed",
                        "reason": str(exc),
                        "reconnect_seconds": self.config.monitor.price_feed.ws_reconnect_seconds,
                    },
                )
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=self.config.monitor.price_feed.ws_reconnect_seconds)
                except TimeoutError:
                    pass
                continue

        return True

    def _watch_symbols(self) -> list[str]:
        symbols = set(self.state.positions.keys())
        if not symbols:
            symbols.update(self.config.risk.symbol_allowlist)
        if not symbols:
            symbols.update(self.config.filters.symbol_whitelist)
        return sorted(s for s in symbols if s)

    def _apply_local_guard_fallback_policy(self) -> None:
        if self.config.risk.stoploss.sl_order_type != "local_guard":
            return
        action = self.config.monitor.price_feed.rest_fallback_action_when_local_guard
        payload = {
            "purpose": "price_feed",
            "reason": "ws_to_rest_with_local_guard",
            "action": action,
        }
        if action == "safe_mode":
            self.state.enable_safe_mode("ws fallback to rest while using local_guard stop-loss")
            self.alerts.error(
                "PRICE_FEED_LOCAL_GUARD_DEGRADED",
                "ws fallback to rest in local_guard mode; safe_mode enabled",
                payload,
            )
            return
        self.alerts.warn(
            "PRICE_FEED_LOCAL_GUARD_DEGRADED",
            "ws fallback to rest in local_guard mode; notify_only policy",
            payload,
        )


def is_price_fresh(last_price_at: datetime | None, max_stale_seconds: int, now: datetime | None = None) -> bool:
    if last_price_at is None:
        return False
    ref = now or utc_now()
    return (ref - last_price_at).total_seconds() <= max_stale_seconds


def _to_float(value: object) -> float | None:
    if value in {None, ""}:
        return None
    try:
        return float(value)
    except Exception:  # noqa: BLE001
        return None
