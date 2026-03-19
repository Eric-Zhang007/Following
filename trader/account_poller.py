from __future__ import annotations

import asyncio
import re
from datetime import datetime, timedelta, timezone

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
        self._unknown_position_active: set[str] = set()

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
        old_positions = dict(self.state.positions)
        old_symbols = set(self.state.positions.keys())
        unknown_symbols_now: set[str] = set()

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
                unknown_origin=(
                    symbol not in known_entry_symbols
                    and self.store.get_latest_trade_thread_by_symbol(symbol, active_only=True) is None
                ),
                opened_at=utc_now(),
            )
            parsed_positions.append(position)

            if position.unknown_origin:
                unknown_symbols_now.add(symbol)
                if symbol not in self._unknown_position_active:
                    self.alerts.warn(
                        "UNKNOWN_POSITION",
                        "exchange reports unknown position; notify only",
                        {"symbol": symbol, "size": size, "side": side, "impact": "notify_only"},
                    )
                    self._unknown_position_active.add(symbol)

        self.state.set_positions(parsed_positions)
        recovered_unknown = self._unknown_position_active - unknown_symbols_now
        if recovered_unknown:
            for symbol in recovered_unknown:
                self._unknown_position_active.discard(symbol)
                self.alerts.info(
                    "UNKNOWN_POSITION_RECOVERED",
                    "unknown position state recovered",
                    {"symbol": symbol},
                )
            # Release unknown-position safe mode once all unknown positions are gone.
            if not self._unknown_position_active:
                reason = str(self.state.block_new_entries_reason or "")
                if reason.startswith("unknown position detected on exchange:"):
                    self.state.disable_safe_mode()
                    self.alerts.info(
                        "UNKNOWN_POSITION_SAFE_MODE_RELEASED",
                        "safe mode released after unknown positions recovered",
                        {"reason": "unknown_position_recovered"},
                    )

        new_symbols = {p.symbol for p in parsed_positions}
        cleared = old_symbols - new_symbols
        for symbol in cleared:
            prev = old_positions.get(symbol)
            thread_id = self.store.find_latest_thread_id_by_symbol(symbol)
            cancel_summary = self._cancel_orders_on_position_clear(symbol, thread_id)
            self.state.clear_orders_for_symbol(symbol)
            if thread_id is not None:
                self.store.set_trade_thread_status(thread_id, "CLOSED")

            account = self.state.account
            realized_pnl, pnl_source = await self._resolve_realized_pnl(symbol=symbol, side=(prev.side if prev else None))
            payload = {
                "symbol": symbol,
                "thread_id": thread_id,
                "position_side": prev.side if prev is not None else None,
                "position_size": prev.size if prev is not None else None,
                "entry_price": prev.entry_price if prev is not None else None,
                "last_mark_price": prev.mark_price if prev is not None else None,
                "realized_pnl": realized_pnl,
                "pnl_source": pnl_source,
                "account_equity": account.equity if account is not None else None,
                "account_available": account.available if account is not None else None,
                "account_margin_used": account.margin_used if account is not None else None,
            }
            if thread_id is not None:
                payload["entry_times"] = self.store.count_thread_actions(thread_id, "ENTRY")
                payload["add_times"] = self.store.count_thread_actions(thread_id, "MANAGE_ADD")
                payload["reduce_times"] = self.store.count_thread_actions(thread_id, "MANAGE_REDUCE")

            self.alerts.info(
                "POSITION_CLEARED",
                "position no longer exists on exchange; cleared local order state",
                {
                    "symbol": symbol,
                    "entry_cancel_attempted": cancel_summary["attempted"],
                    "entry_cancel_succeeded": cancel_summary["canceled"],
                    "entry_cancel_failed": cancel_summary["failed"],
                    "protection_cancel_attempted": cancel_summary["protection_attempted"],
                    "protection_cancel_succeeded": cancel_summary["protection_canceled"],
                    "protection_cancel_failed": cancel_summary["protection_failed"],
                },
            )
            self.alerts.info(
                "POSITION_CLOSED_SUMMARY",
                "position closed summary",
                payload,
            )

    def _cancel_orders_on_position_clear(self, symbol: str, thread_id: int | None) -> dict[str, int]:
        """Best-effort one-shot cleanup for lingering orders when a position is fully closed."""
        rows: list[dict] = []
        try:
            rows.extend(self.bitget.get_open_orders() or [])
        except Exception as exc:  # noqa: BLE001
            self.state.register_api_error()
            self.alerts.warn(
                "POSITION_CLEAR_CANCEL_SCAN_FAILED",
                "failed to scan open orders during position-clear cleanup",
                {"symbol": symbol, "error": str(exc)},
            )
            return {"attempted": 0, "canceled": 0, "failed": 0}

        if hasattr(self.bitget, "list_plan_orders"):
            try:
                rows.extend(self.bitget.list_plan_orders() or [])
            except Exception as exc:  # noqa: BLE001
                self.state.register_api_error()
                self.alerts.warn(
                    "POSITION_CLEAR_CANCEL_SCAN_FAILED",
                    "failed to scan plan orders during position-clear cleanup",
                    {"symbol": symbol, "error": str(exc)},
                )

        entry_attempted = 0
        entry_canceled = 0
        entry_failed = 0
        protection_attempted = 0
        protection_canceled = 0
        protection_failed = 0
        seen: set[tuple[bool, str | None, str | None]] = set()
        symbol_upper = symbol.upper()
        for row in rows:
            row_symbol = str(row.get("symbol") or "").upper()
            if row_symbol != symbol_upper:
                continue
            order_id = str(row.get("orderId") or "") or None
            client_oid = str(row.get("clientOid") or "") or None
            existing = self.state.find_order(client_order_id=client_oid, order_id=order_id)
            order_thread_id, _ = self._resolve_order_thread_context(
                symbol=symbol_upper,
                client_order_id=client_oid,
                existing=existing,
            )
            if thread_id is not None and order_thread_id is not None and order_thread_id != thread_id:
                continue
            purpose = self._resolve_order_purpose(row, existing)
            if not self._should_cancel_on_position_clear(purpose):
                continue
            is_plan_order = bool(row.get("planType") or row.get("triggerType"))
            key = (is_plan_order, order_id, client_oid)
            if key in seen:
                continue
            seen.add(key)
            protection_order = purpose != "entry"
            if protection_order:
                protection_attempted += 1
            else:
                entry_attempted += 1
            try:
                if is_plan_order and hasattr(self.bitget, "cancel_plan_order"):
                    self.bitget.cancel_plan_order(symbol=symbol_upper, order_id=order_id, client_oid=client_oid)
                elif order_id:
                    self.bitget.cancel_order(symbol_upper, order_id)
                else:
                    raise RuntimeError("missing order_id for non-plan order cancel")
                if protection_order:
                    protection_canceled += 1
                else:
                    entry_canceled += 1
                self.store.record_reconciler_action(
                    symbol=symbol_upper,
                    order_id=order_id,
                    client_order_id=client_oid,
                    action="POSITION_CLEAR_CANCEL_ORDER",
                    reason="position_cleared",
                    payload={"is_plan_order": is_plan_order, "purpose": purpose},
                    thread_id=thread_id,
                    purpose=purpose,
                )
            except Exception as exc:  # noqa: BLE001
                if protection_order:
                    protection_failed += 1
                else:
                    entry_failed += 1
                self.state.register_api_error()
                self.store.record_reconciler_action(
                    symbol=symbol_upper,
                    order_id=order_id,
                    client_order_id=client_oid,
                    action="POSITION_CLEAR_CANCEL_ORDER_FAILED",
                    reason=str(exc),
                    payload={"is_plan_order": is_plan_order, "purpose": purpose},
                    thread_id=thread_id,
                    purpose=purpose,
                )
        return {
            "attempted": entry_attempted,
            "canceled": entry_canceled,
            "failed": entry_failed,
            "protection_attempted": protection_attempted,
            "protection_canceled": protection_canceled,
            "protection_failed": protection_failed,
        }

    async def poll_open_orders(self) -> None:
        raw_orders = await asyncio.to_thread(self.bitget.get_open_orders)
        plan_orders: list[dict] = []
        if hasattr(self.bitget, "list_plan_orders"):
            try:
                plan_orders = await asyncio.to_thread(self.bitget.list_plan_orders)
            except Exception:  # noqa: BLE001
                self.state.register_api_error()
        raw_orders = list(raw_orders) + list(plan_orders)
        now = utc_now()
        for row in raw_orders:
            symbol = str(row.get("symbol") or "").upper()
            if not symbol:
                continue
            side = str(row.get("side") or "").lower() or "buy"
            client_oid = str(row.get("clientOid") or "") or None
            order_id = str(row.get("orderId") or "") or None
            existing = self.state.find_order(client_order_id=client_oid, order_id=order_id)

            purpose = self._resolve_order_purpose(row, existing)
            thread_id, entry_index = self._resolve_order_thread_context(
                symbol=symbol,
                client_order_id=client_oid,
                existing=existing,
            )
            state = OrderState(
                symbol=symbol,
                side=side,
                status=str(row.get("state", row.get("status", "NEW"))),
                # Bitget open-orders payload may use baseVolume as order quantity.
                # Use explicit filled fields to avoid misclassifying pending orders as filled.
                filled=self._to_float(row, ["filledQty", "filledSize", "dealSize", "accBaseVolume"]) or 0.0,
                quantity=self._to_float(row, ["size", "qty", "baseVolume"]),
                avg_price=self._to_float(row, ["priceAvg", "avgPrice"]),
                reduce_only=str(row.get("reduceOnly", "NO")).upper() == "YES",
                trade_side=str(row.get("tradeSide") or "").lower() or None,
                purpose=purpose,
                timestamp=now,
                client_order_id=client_oid,
                order_id=order_id,
                trigger_price=self._to_float(row, ["triggerPrice", "triggerPx"]),
                is_plan_order=bool(row.get("planType") or row.get("triggerType")),
                parent_client_order_id=existing.parent_client_order_id if existing is not None else None,
                thread_id=thread_id,
                entry_index=entry_index,
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
        client_oid = str(row.get("clientOid") or "").lower()
        if client_oid.startswith("be-local-"):
            return "be_reduce_local"
        if client_oid.startswith("be-"):
            return "be_reduce"
        if client_oid.startswith("tp-") or "-tp-" in client_oid:
            return "tp"
        if client_oid.startswith("sl-") or "-sl-" in client_oid:
            return "sl"
        plan_type = str(row.get("planType") or "").lower()
        if "profit" in plan_type:
            return "tp"
        if "loss" in plan_type:
            return "sl"
        if plan_type == "normal_plan":
            # Some accounts return TP/SL as normal_plan; infer from preset fields when available.
            has_tp_fields = any(
                row.get(key) not in {None, ""}
                for key in ("stopSurplusTriggerPrice", "stopSurplusExecutePrice", "presetTakeProfitPrice")
            )
            has_sl_fields = any(
                row.get(key) not in {None, ""}
                for key in ("stopLossTriggerPrice", "stopLossExecutePrice", "presetStopLossPrice")
            )
            if has_tp_fields and not has_sl_fields:
                return "tp"
            if has_sl_fields and not has_tp_fields:
                return "sl"
        trade_side = str(row.get("tradeSide") or "").lower()
        reduce_only = str(row.get("reduceOnly", "NO")).upper() == "YES"
        if reduce_only or trade_side == "close":
            # Generic close-type plan order. We avoid forcing it to "sl" because
            # normal_plan close orders may actually be TP, and mislabeling can
            # trigger duplicate TP placement loops.
            return "close"
        return "entry"

    @staticmethod
    def _should_cancel_on_position_clear(purpose: str) -> bool:
        return purpose in {"entry", "tp", "sl", "be_reduce", "be_reduce_local", "close"}

    def _resolve_order_purpose(self, row: dict, existing: OrderState | None) -> str:
        if existing is not None and existing.purpose:
            return existing.purpose
        return self._infer_purpose(row)

    def _resolve_order_thread_context(
        self,
        *,
        symbol: str,
        client_order_id: str | None,
        existing: OrderState | None,
    ) -> tuple[int | None, int | None]:
        if existing is not None and existing.thread_id is not None:
            return existing.thread_id, existing.entry_index

        parsed_thread, parsed_entry = self._parse_entry_thread(client_order_id)
        if parsed_thread is not None:
            return parsed_thread, parsed_entry

        latest = self.store.get_latest_trade_thread_by_symbol(symbol, active_only=True)
        return (int(latest["thread_id"]), None) if latest is not None else (None, None)

    @staticmethod
    def _parse_entry_thread(client_order_id: str | None) -> tuple[int | None, int | None]:
        if not client_order_id:
            return None, None
        # Preferred format: entry-{thread_id}-{entry_index}-{suffix}
        match = re.match(r"^entry-(\d+)-(\d+)-", client_order_id)
        if not match:
            return None, None
        try:
            return int(match.group(1)), int(match.group(2))
        except Exception:  # noqa: BLE001
            return None, None

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

    async def _resolve_realized_pnl(self, *, symbol: str, side: str | None) -> tuple[float | None, str]:
        if not hasattr(self.bitget, "get_history_positions"):
            return None, "history_position.unsupported"

        now = utc_now()
        start = now - timedelta(days=7)
        try:
            rows = await asyncio.to_thread(
                self.bitget.get_history_positions,
                symbol=symbol,
                start_time=start,
                end_time=now,
                limit=50,
            )
        except Exception as exc:  # noqa: BLE001
            self.state.register_api_error()
            self.alerts.warn(
                "POSITION_CLOSED_PNL_FETCH_FAIL",
                "failed to query realized pnl for closed position",
                {"symbol": symbol, "error": str(exc)},
            )
            return None, "history_position.error"

        row = self._pick_latest_history_position(rows, symbol=symbol, side=side)
        if row is None:
            return None, "history_position.not_found"
        pnl_value, pnl_key = self._extract_realized_pnl_from_row(row)
        if pnl_value is None:
            return None, "history_position.pnl_missing"
        return pnl_value, f"history_position.{pnl_key}"

    @classmethod
    def _pick_latest_history_position(
        cls,
        rows: list[dict],
        *,
        symbol: str,
        side: str | None,
    ) -> dict | None:
        symbol_upper = symbol.upper()
        side_lower = (side or "").lower()
        candidates: list[tuple[float, dict]] = []
        for row in rows:
            row_symbol = str(row.get("symbol") or row.get("instId") or "").upper()
            if row_symbol and row_symbol != symbol_upper:
                continue
            row_side = cls._extract_history_position_side(row)
            if side_lower and row_side and row_side != side_lower:
                continue
            ts = cls._extract_history_position_ts(row)
            candidates.append((ts, row))
        if not candidates:
            return None
        candidates.sort(key=lambda item: item[0], reverse=True)
        return candidates[0][1]

    @staticmethod
    def _extract_history_position_side(row: dict) -> str | None:
        for key in ("holdSide", "posSide", "positionSide"):
            value = str(row.get(key) or "").lower()
            if value in {"long", "short"}:
                return value
        return None

    @staticmethod
    def _extract_history_position_ts(row: dict) -> float:
        for key in ("uTime", "utime", "closeTime", "cTime", "ctime"):
            raw = row.get(key)
            if raw in (None, ""):
                continue
            try:
                return float(raw)
            except Exception:  # noqa: BLE001
                continue
        return datetime.now(timezone.utc).timestamp() * 1000

    @classmethod
    def _extract_realized_pnl_from_row(cls, row: dict) -> tuple[float | None, str | None]:
        for key in ("netProfit", "realizedPL", "achievedProfits", "pnl", "profit"):
            value = cls._to_float(row, [key])
            if value is not None:
                return value, key
        return None, None

    def _due(self, key: str, interval_seconds: int, now: datetime) -> bool:
        last = self._last_runs.get(key)
        if last is None:
            return True
        return (now - last).total_seconds() >= interval_seconds
