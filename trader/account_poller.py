from __future__ import annotations

import asyncio
from dataclasses import asdict
from datetime import datetime

from trader.alerts import AlertManager
from trader.bitget_client import BitgetClient
from trader.config import AppConfig
from trader.state import OrderState, PositionState, StateStore, utc_now
from trader.store import SQLiteStore


class AccountPoller:
    def __init__(
        self,
        config: AppConfig,
        bitget: BitgetClient,
        state: StateStore,
        store: SQLiteStore,
        alerts: AlertManager,
    ) -> None:
        self.config = config
        self.bitget = bitget
        self.state = state
        self.store = store
        self.alerts = alerts
        self._last_runs: dict[str, datetime] = {}

    async def run(self, stop_event: asyncio.Event) -> None:
        while not stop_event.is_set():
            now = utc_now()
            try:
                await self._tick(now)
            except Exception as exc:  # noqa: BLE001
                self.state.register_api_error()
                self.alerts.error("POLLER_TICK_ERROR", f"poller tick failed: {exc}")
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=1.0)
            except TimeoutError:
                pass

    async def _tick(self, now: datetime) -> None:
        pi = self.config.monitor.poll_intervals
        if self._due("account", pi.account_seconds, now):
            await self.poll_account()
            self._last_runs["account"] = now
        if self._due("positions", pi.positions_seconds, now):
            await self.poll_positions()
            self._last_runs["positions"] = now
        if self._due("open_orders", pi.open_orders_seconds, now):
            await self.poll_open_orders()
            self._last_runs["open_orders"] = now
        if self._due("funding", pi.funding_seconds, now):
            await self.poll_funding()
            self._last_runs["funding"] = now
        if self._due("contracts", pi.contracts_seconds, now):
            await self.poll_contracts()
            self._last_runs["contracts"] = now

    async def poll_account(self) -> None:
        payload = await asyncio.to_thread(self.bitget.get_account_snapshot)
        equity = float(payload.get("equity", 0.0))
        available = float(payload.get("available", equity))
        margin_used = float(payload.get("margin_used", 0.0))
        self.state.set_account(equity=equity, available=available, margin_used=margin_used, timestamp=utc_now())
        self.store.snapshot_equity(equity=equity, available=available, margin_used=margin_used)
        self.store.save_runtime_snapshot(self.state.to_snapshot())

    async def poll_positions(self) -> None:
        raw_positions = await asyncio.to_thread(self.bitget.get_positions)
        parsed_positions: list[PositionState] = []

        known_entry_symbols = self.state.known_entry_symbols()
        old_symbols = set(self.state.positions.keys())

        for row in raw_positions:
            symbol = str(row.get("symbol") or row.get("instId") or "").upper()
            if not symbol:
                continue
            size = abs(float(row.get("total", row.get("size", 0)) or 0.0))
            if size <= 0:
                continue

            side = self._extract_position_side(row)
            position = PositionState(
                symbol=symbol,
                side=side,
                size=size,
                entry_price=self._to_float(row, ["openPriceAvg", "entryPrice", "openPrice"]),
                mark_price=self._to_float(row, ["markPrice", "mark", "lastPr"]),
                liq_price=self._to_float(row, ["liquidationPrice", "liqPx", "liquidation"]),
                pnl=self._to_float(row, ["unrealizedPL", "upl", "unrealizedPnl"]),
                leverage=self._to_int(row, ["leverage"]),
                margin_mode=str(row.get("marginMode") or self.config.bitget.margin_mode),
                timestamp=utc_now(),
                unknown_origin=(symbol not in known_entry_symbols),
                opened_at=utc_now(),
            )
            parsed_positions.append(position)

            if position.unknown_origin:
                self.state.enable_safe_mode(f"unknown position detected on exchange: {symbol}")
                self.alerts.warn(
                    "UNKNOWN_POSITION",
                    "exchange reports unknown position; blocking new entries",
                    {"symbol": symbol, "size": size, "side": side},
                )

        self.state.set_positions(parsed_positions)
        new_symbols = {p.symbol for p in parsed_positions}
        cleared = old_symbols - new_symbols
        for symbol in cleared:
            self.state.clear_orders_for_symbol(symbol)
            self.alerts.info(
                "POSITION_CLEARED",
                "position no longer exists on exchange; cleared local order state",
                {"symbol": symbol},
            )

    async def poll_open_orders(self) -> None:
        raw_orders = await asyncio.to_thread(self.bitget.get_open_orders)
        now = utc_now()
        for row in raw_orders:
            symbol = str(row.get("symbol") or "").upper()
            if not symbol:
                continue
            side = str(row.get("side") or "").lower() or "buy"
            purpose = self._infer_purpose(row)
            state = OrderState(
                symbol=symbol,
                side=side,
                status=str(row.get("state", row.get("status", "NEW"))),
                filled=float(row.get("baseVolume", row.get("filledQty", 0.0)) or 0.0),
                avg_price=self._to_float(row, ["priceAvg", "avgPrice"]),
                reduce_only=str(row.get("reduceOnly", "NO")).upper() == "YES",
                trade_side=str(row.get("tradeSide") or "").lower() or None,
                purpose=purpose,
                timestamp=now,
                client_order_id=str(row.get("clientOid") or "") or None,
                order_id=str(row.get("orderId") or "") or None,
            )
            self.state.upsert_order(state)

    async def poll_funding(self) -> None:
        # Funding refresh is informational; errors should not block the rest.
        symbols = sorted(self.state.positions.keys())[:10]
        for symbol in symbols:
            try:
                await asyncio.to_thread(self.bitget.get_funding_rate, symbol)
            except Exception:  # noqa: BLE001
                self.state.register_api_error()

    async def poll_contracts(self) -> None:
        await asyncio.to_thread(self.bitget.get_contracts)

    @staticmethod
    def _infer_purpose(row: dict) -> str:
        trade_side = str(row.get("tradeSide") or "").lower()
        reduce_only = str(row.get("reduceOnly", "NO")).upper() == "YES"
        if reduce_only or trade_side == "close":
            return "sl"
        return "entry"

    @staticmethod
    def _extract_position_side(row: dict) -> str:
        hold_side = str(row.get("holdSide") or "").lower()
        if hold_side in {"long", "short"}:
            return hold_side
        size = float(row.get("total", row.get("size", 0)) or 0.0)
        return "long" if size >= 0 else "short"

    @staticmethod
    def _to_float(row: dict, keys: list[str]) -> float | None:
        for key in keys:
            if key in row and row[key] is not None and row[key] != "":
                try:
                    return float(row[key])
                except Exception:  # noqa: BLE001
                    continue
        return None

    @staticmethod
    def _to_int(row: dict, keys: list[str]) -> int | None:
        for key in keys:
            if key in row and row[key] is not None and row[key] != "":
                try:
                    return int(float(row[key]))
                except Exception:  # noqa: BLE001
                    continue
        return None

    def _due(self, key: str, interval_seconds: int, now: datetime) -> bool:
        last = self._last_runs.get(key)
        if last is None:
            return True
        return (now - last).total_seconds() >= interval_seconds
