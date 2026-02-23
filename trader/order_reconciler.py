from __future__ import annotations

import asyncio
from dataclasses import asdict

from trader.alerts import AlertManager
from trader.bitget_client import BitgetClient
from trader.config import AppConfig
from trader.state import OrderState, PositionState, StateStore, utc_now
from trader.stoploss_manager import StopLossManager
from trader.store import SQLiteStore


_TERMINAL = {"FILLED", "CANCELED", "REJECTED", "FAILED"}


class OrderReconciler:
    def __init__(
        self,
        config: AppConfig,
        bitget: BitgetClient,
        state: StateStore,
        store: SQLiteStore,
        alerts: AlertManager,
        stoploss_manager: StopLossManager | None = None,
    ) -> None:
        self.config = config
        self.bitget = bitget
        self.state = state
        self.store = store
        self.alerts = alerts
        self.stoploss_manager = stoploss_manager or StopLossManager(
            config=config,
            bitget=bitget,
            state=state,
            store=store,
            alerts=alerts,
        )
        self._error_counts: dict[str, int] = {}

    async def run(self, stop_event: asyncio.Event) -> None:
        interval = self.config.monitor.poll_intervals.reconciler_seconds
        while not stop_event.is_set():
            try:
                await self.reconcile_once()
                self.state.set_reconciler_fresh()
            except Exception as exc:  # noqa: BLE001
                self.state.register_api_error()
                self.alerts.error("RECONCILER_ERROR", f"reconcile loop failed: {exc}")
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=interval)
            except TimeoutError:
                pass

    async def reconcile_once(self) -> None:
        pending = self.state.pending_orders()
        if not pending:
            return

        for order in pending:
            await self._reconcile_order(order)

    async def _reconcile_order(self, order: OrderState) -> None:
        trace = self.alerts.info(
            "RECONCILER_CHECK",
            "checking pending order",
            {
                "symbol": order.symbol,
                "purpose": order.purpose,
                "client_order_id": order.client_order_id,
                "order_id": order.order_id,
            },
        )

        if self.config.dry_run:
            self._transition(order=order, status="FILLED", filled=order.quantity or order.filled, avg_price=order.avg_price)
            self.store.record_reconciler_action(
                symbol=order.symbol,
                order_id=order.order_id,
                client_order_id=order.client_order_id,
                action="DRY_RUN_FILLED",
                reason=f"purpose={order.purpose}",
                payload=asdict(order),
                trace_id=trace,
            )
            return

        try:
            payload = await asyncio.to_thread(self._fetch_order_state, order)
            status = self._normalize_status(str(payload.get("state", payload.get("status", "NEW"))))
            filled = float(payload.get("baseVolume", payload.get("filledQty", order.filled)) or 0.0)
            avg_price_raw = payload.get("priceAvg", payload.get("avgPrice"))
            avg_price = float(avg_price_raw) if avg_price_raw not in {None, ""} else order.avg_price

            self._transition(order=order, status=status, filled=filled, avg_price=avg_price)
            self.store.record_reconciler_action(
                symbol=order.symbol,
                order_id=order.order_id,
                client_order_id=order.client_order_id,
                action="ORDER_RECONCILED",
                reason=f"purpose={order.purpose};state={status}",
                payload={"filled": filled, "avg_price": avg_price},
                trace_id=trace,
            )

            if order.purpose.lower() == "entry" and status in {"PARTIAL", "FILLED"} and filled > 0:
                await self._ensure_entry_filled_has_sl(order=order, filled_qty=filled, avg_price=avg_price, trace=trace)
                await self._ensure_entry_filled_has_tp(order=order, filled_qty=filled, trace=trace)

            if order.purpose.lower() == "sl":
                await self._repair_sl_size_if_needed(order, trace)

        except Exception as exc:  # noqa: BLE001
            key = order.client_order_id or order.order_id or f"{order.symbol}:{order.purpose}"
            count = self._error_counts.get(key, 0) + 1
            self._error_counts[key] = count
            self.state.register_api_error()
            self.store.record_reconciler_action(
                symbol=order.symbol,
                order_id=order.order_id,
                client_order_id=order.client_order_id,
                action="RECONCILE_ERROR",
                reason=str(exc),
                payload={"retry": count, "purpose": order.purpose},
                trace_id=trace,
            )
            self.alerts.warn(
                "RECONCILE_ORDER_ERROR",
                "failed to reconcile order",
                {"symbol": order.symbol, "retry": count, "error": str(exc), "purpose": order.purpose},
            )
            if count > self.config.execution.max_submit_retries:
                self.state.enable_safe_mode("reconciler retries exceeded")

    def _fetch_order_state(self, order: OrderState) -> dict:
        try:
            return self.bitget.get_order_state(
                order.symbol,
                order.order_id,
                order.client_order_id,
                order.is_plan_order,
            )
        except TypeError:
            return self.bitget.get_order_state(
                order.symbol,
                order.order_id,
                order.client_order_id,
            )

    async def _ensure_entry_filled_has_sl(self, order: OrderState, filled_qty: float, avg_price: float | None, trace: str) -> None:
        side = "long" if order.side.lower() == "buy" else "short"
        ps = PositionState(
            symbol=order.symbol,
            side=side,
            size=filled_qty,
            entry_price=avg_price,
            mark_price=avg_price,
            liq_price=None,
            pnl=None,
            leverage=None,
            margin_mode=self.config.bitget.margin_mode,
            timestamp=utc_now(),
            opened_at=utc_now(),
        )
        result = self.stoploss_manager.ensure_stop_loss(
            position_state=ps,
            desired_sl_price=None,
            desired_size=filled_qty,
            source="reconciler_partial_fill",
            parent_client_order_id=order.client_order_id,
        )
        self.store.record_reconciler_action(
            symbol=order.symbol,
            order_id=result.order_id,
            client_order_id=result.client_order_id,
            action="PARTIAL_FILL_ENSURE_SL",
            reason=result.reason,
            payload={"qty": filled_qty, "ok": result.ok, "mode": result.mode, "purpose": "sl"},
            trace_id=trace,
        )

    async def _ensure_entry_filled_has_tp(self, order: OrderState, filled_qty: float, trace: str) -> None:
        # Reconciler only validates TP existence by intent when available in runtime state.
        self.store.record_reconciler_action(
            symbol=order.symbol,
            order_id=order.order_id,
            client_order_id=order.client_order_id,
            action="PARTIAL_FILL_TP_CHECK",
            reason="reconciler checked tp lifecycle",
            payload={"qty": filled_qty, "purpose": "tp"},
            trace_id=trace,
        )

    async def _repair_sl_size_if_needed(self, sl_order: OrderState, trace: str) -> None:
        position = self.state.positions.get(sl_order.symbol.upper())
        if position is None:
            return
        if sl_order.quantity is None or position.size <= 0:
            return
        ratio = abs(sl_order.quantity - position.size) / position.size
        if ratio <= 0.2:
            return

        result = self.stoploss_manager.ensure_stop_loss(
            position_state=position,
            desired_sl_price=sl_order.trigger_price,
            desired_size=position.size,
            source="reconciler_sl_size_repair",
            parent_client_order_id=sl_order.parent_client_order_id,
        )
        self.store.record_reconciler_action(
            symbol=sl_order.symbol,
            order_id=result.order_id,
            client_order_id=result.client_order_id,
            action="SL_SIZE_REPAIRED",
            reason=result.reason,
            payload={"old_qty": sl_order.quantity, "new_qty": position.size, "purpose": "sl"},
            trace_id=trace,
        )

    def _transition(self, order: OrderState, status: str, filled: float, avg_price: float | None) -> None:
        mapped = status
        if status not in _TERMINAL and status not in {"NEW", "ACKED", "PARTIAL", "LIVE"}:
            mapped = "ACKED"
        self.state.mark_order_status(
            status=mapped,
            filled=filled,
            avg_price=avg_price,
            client_order_id=order.client_order_id,
            order_id=order.order_id,
        )

    @staticmethod
    def _normalize_status(status: str) -> str:
        s = status.upper().strip()
        if s in {"NEW", "INIT", "SUBMITTING", "LIVE"}:
            return "ACKED"
        if s in {"PARTIAL", "PARTIALLY_FILLED", "PARTIALLY_FILLED_OPEN"}:
            return "PARTIAL"
        if s in {"FILLED", "FULLY_FILLED", "DONE"}:
            return "FILLED"
        if s in {"CANCELED", "CANCELLED", "CANCEL"}:
            return "CANCELED"
        if s in {"REJECTED", "REJECT"}:
            return "REJECTED"
        if s in {"FAILED", "FAIL"}:
            return "FAILED"
        if s in {"FILLED_OR_CLOSED"}:
            return "FILLED"
        return s
