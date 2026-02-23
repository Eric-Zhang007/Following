from __future__ import annotations

import asyncio
from dataclasses import asdict
from datetime import datetime

from trader.alerts import AlertManager
from trader.bitget_client import BitgetClient
from trader.config import AppConfig
from trader.state import OrderState, StateStore, utc_now
from trader.store import SQLiteStore


class OrderReconciler:
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
                "client_order_id": order.client_order_id,
                "order_id": order.order_id,
                "purpose": order.purpose,
            },
        )

        if self.config.dry_run:
            self.state.mark_order_status(
                status="FILLED",
                filled=order.filled or 0.0,
                avg_price=order.avg_price,
                client_order_id=order.client_order_id,
                order_id=order.order_id,
            )
            self.store.record_reconciler_action(
                symbol=order.symbol,
                order_id=order.order_id,
                client_order_id=order.client_order_id,
                action="DRY_RUN_FILLED",
                reason="dry_run",
                payload=asdict(order),
                trace_id=trace,
            )
            return

        try:
            payload = await asyncio.to_thread(
                self.bitget.get_order_state,
                order.symbol,
                order.order_id,
                order.client_order_id,
            )
            status = str(payload.get("state", payload.get("status", "NEW"))).upper()
            filled = float(payload.get("baseVolume", payload.get("filledQty", 0.0)) or 0.0)
            avg_price = payload.get("priceAvg", payload.get("avgPrice"))
            avg_price_val = float(avg_price) if avg_price not in {None, ""} else None

            if status in {"PARTIAL", "PARTIALLY_FILLED", "PARTIALLY_FILLED_OPEN", "LIVE"} and filled > 0:
                if order.purpose.lower() == "entry":
                    await self._ensure_partial_fill_sl(order, filled, trace)
                self.store.record_reconciler_action(
                    symbol=order.symbol,
                    order_id=order.order_id,
                    client_order_id=order.client_order_id,
                    action="PARTIAL_FILL",
                    reason="partial fill observed",
                    payload={"filled": filled, "status": status},
                    trace_id=trace,
                )

            if status in {"FILLED", "CANCELED", "REJECTED", "FAILED"}:
                self.state.mark_order_status(
                    status=status,
                    filled=filled,
                    avg_price=avg_price_val,
                    client_order_id=order.client_order_id,
                    order_id=order.order_id,
                )
                self.store.record_reconciler_action(
                    symbol=order.symbol,
                    order_id=order.order_id,
                    client_order_id=order.client_order_id,
                    action="ORDER_FINALIZED",
                    reason=status,
                    payload={"filled": filled, "avg_price": avg_price_val},
                    trace_id=trace,
                )
            else:
                self.state.mark_order_status(
                    status=status,
                    filled=filled,
                    avg_price=avg_price_val,
                    client_order_id=order.client_order_id,
                    order_id=order.order_id,
                )
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
                payload={"retry": count},
                trace_id=trace,
            )
            self.alerts.warn(
                "RECONCILE_ORDER_ERROR",
                "failed to reconcile order",
                {"symbol": order.symbol, "retry": count, "error": str(exc)},
            )
            if count > self.config.execution.max_submit_retries:
                self.state.enable_safe_mode("reconciler retries exceeded")

    async def _ensure_partial_fill_sl(self, order: OrderState, filled_qty: float, trace_id: str) -> None:
        if filled_qty <= 0:
            return
        close_side = "sell" if order.side.lower() == "buy" else "buy"

        # Skip if any SL already exists for this symbol.
        if self.state.has_valid_stop_loss(order.symbol, "long" if close_side == "sell" else "short"):
            return

        if self.config.dry_run:
            self.store.record_reconciler_action(
                symbol=order.symbol,
                order_id=order.order_id,
                client_order_id=order.client_order_id,
                action="PARTIAL_SL_DRY_RUN",
                reason="dry_run",
                payload={"qty": filled_qty},
                trace_id=trace_id,
            )
            return

        receipt = await asyncio.to_thread(
            self.bitget.place_order,
            symbol=order.symbol,
            side=close_side,
            trade_side="close" if self.config.bitget.position_mode == "hedge_mode" else None,
            size=filled_qty,
            order_type="market",
            reduce_only=self.config.bitget.position_mode == "one_way_mode",
            client_oid=f"sl-partial-{int(utc_now().timestamp() * 1000)}",
        )
        sl_state = OrderState(
            symbol=order.symbol,
            side=close_side,
            status="SUBMITTED",
            filled=0.0,
            avg_price=None,
            reduce_only=self.config.bitget.position_mode == "one_way_mode",
            trade_side="close" if self.config.bitget.position_mode == "hedge_mode" else None,
            purpose="sl",
            timestamp=utc_now(),
            client_order_id=str(receipt.get("clientOid") or "") or None,
            order_id=str(receipt.get("orderId") or "") or None,
        )
        self.state.upsert_order(sl_state)
        self.store.record_reconciler_action(
            symbol=order.symbol,
            order_id=sl_state.order_id,
            client_order_id=sl_state.client_order_id,
            action="PARTIAL_SL_SUBMITTED",
            reason="partial fill protection",
            payload={"qty": filled_qty},
            trace_id=trace_id,
        )
