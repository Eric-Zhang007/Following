from __future__ import annotations

import asyncio
from datetime import datetime

from trader.alerts import AlertManager
from trader.bitget_client import BitgetClient
from trader.config import AppConfig
from trader.state import StateStore, utc_now


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
        mode = self.config.monitor.price_feed.mode
        if mode == "ws":
            self.alerts.warn(
                "PRICE_FEED_MODE",
                "ws mode requested but websocket backend is not enabled yet; fallback to rest",
                {"requested_mode": "ws", "active_mode": "rest"},
            )

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

    async def refresh_once(self) -> None:
        symbols = self._watch_symbols()
        if not symbols:
            self.state.set_price_fresh()
            return

        for symbol in symbols:
            ticker = await asyncio.to_thread(self.bitget.get_ticker, symbol)
            mark = ticker.get("mark_price") or ticker.get("last_price")
            if mark is not None:
                self.state.set_mark_price(symbol, float(mark), timestamp=utc_now())
        self.state.set_price_fresh()

    def _watch_symbols(self) -> list[str]:
        symbols = set(self.state.positions.keys())
        if not symbols:
            symbols.update(self.config.risk.symbol_allowlist)
        if not symbols:
            symbols.update(self.config.filters.symbol_whitelist)
        return sorted(s for s in symbols if s)


def is_price_fresh(last_price_at: datetime | None, interval_seconds: int, now: datetime | None = None) -> bool:
    if last_price_at is None:
        return False
    ref = now or utc_now()
    return (ref - last_price_at).total_seconds() <= interval_seconds * 2
