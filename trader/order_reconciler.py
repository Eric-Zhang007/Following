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
        for order in pending:
            await self._reconcile_order(order)
        self._process_be_reduce_local_guards()

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
            filled = order.quantity or order.filled
            self._transition(order=order, status="FILLED", filled=filled, avg_price=order.avg_price)
            self.store.record_reconciler_action(
                symbol=order.symbol,
                order_id=order.order_id,
                client_order_id=order.client_order_id,
                action="DRY_RUN_FILLED",
                reason=f"purpose={order.purpose}",
                payload=asdict(order),
                trace_id=trace,
                thread_id=order.thread_id,
                purpose=order.purpose,
            )
            if order.purpose.lower() == "entry" and filled > 0:
                await self._ensure_entry_filled_has_sl(order=order, filled_qty=filled, avg_price=order.avg_price, trace=trace)
                await self._ensure_entry_filled_has_tp(order=order, filled_qty=filled, trace=trace)
                await self._maybe_place_be_reduce(order=order, trace=trace)
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
                thread_id=order.thread_id,
                purpose=order.purpose,
            )

            if order.purpose.lower() == "entry" and status in {"PARTIAL", "FILLED"} and filled > 0:
                await self._ensure_entry_filled_has_sl(order=order, filled_qty=filled, avg_price=avg_price, trace=trace)
                await self._ensure_entry_filled_has_tp(order=order, filled_qty=filled, trace=trace)
                await self._maybe_place_be_reduce(order=order, trace=trace)

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
                thread_id=order.thread_id,
                purpose=order.purpose,
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
            desired_sl_price=self._thread_stop_loss(order.thread_id),
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
            thread_id=order.thread_id,
            purpose="sl",
        )
        if not result.ok:
            self.store.record_event(
                event_type="STOPLOSS_PLACE_FAIL",
                level="ERROR",
                msg="failed to place stop-loss on entry fill",
                payload={"symbol": order.symbol, "reason": result.reason},
                reason=result.reason,
                thread_id=order.thread_id,
            )

    async def _ensure_entry_filled_has_tp(self, order: OrderState, filled_qty: float, trace: str) -> None:
        if not self.config.execution.place_tp_on_fill:
            return
        thread = self.store.get_trade_thread(order.thread_id) if order.thread_id is not None else None
        if not thread:
            return
        tp_points = [float(v) for v in thread.get("tp_points", []) if float(v) > 0]
        if not tp_points:
            return
        if self._has_active_tp(order.symbol, order.thread_id):
            return
        self._place_tp_orders(
            symbol=order.symbol,
            thread_id=order.thread_id,
            side_hint=thread.get("side"),
            total_size=filled_qty,
            tp_points=tp_points,
            parent_client_order_id=order.client_order_id,
        )
        self.store.record_reconciler_action(
            symbol=order.symbol,
            order_id=order.order_id,
            client_order_id=order.client_order_id,
            action="PARTIAL_FILL_TP_CHECK",
            reason="reconciler checked tp lifecycle",
            payload={"qty": filled_qty, "purpose": "tp"},
            trace_id=trace,
            thread_id=order.thread_id,
            purpose="tp",
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
            thread_id=sl_order.thread_id,
            purpose="sl",
        )

    async def _maybe_place_be_reduce(self, order: OrderState, trace: str) -> None:
        if not self.config.execution.be_reduce_on_two_entries:
            return
        if order.thread_id is None:
            return

        thread_id = order.thread_id
        thread_orders = [
            o
            for o in self.state.orders_by_client_id.values()
            if o.thread_id == thread_id and o.purpose.lower() == "entry"
        ]
        filled_by_index: dict[int, OrderState] = {}
        for item in thread_orders:
            if item.entry_index is None:
                continue
            if item.status.upper() not in {"FILLED", "PARTIAL"}:
                continue
            if (item.filled or 0) <= 0:
                continue
            filled_by_index[item.entry_index] = item
        if 0 not in filled_by_index or 1 not in filled_by_index:
            return

        if any(
            o.purpose.lower() == "be_reduce" and o.thread_id == thread_id and o.status.upper() not in {"CANCELED", "FAILED", "REJECTED"}
            for o in self.state.orders_by_client_id.values()
        ):
            return

        o1 = filled_by_index[0]
        o2 = filled_by_index[1]
        qty1 = float(o1.filled or 0)
        qty2 = float(o2.filled or 0)
        avg1 = float(o1.avg_price or 0)
        avg2 = float(o2.avg_price or 0)
        if qty1 <= 0 or qty2 <= 0 or avg1 <= 0 or avg2 <= 0:
            return

        avg_entry = ((qty1 * avg1) + (qty2 * avg2)) / (qty1 + qty2)
        thread = self.store.get_trade_thread(thread_id)
        side = str((thread or {}).get("side") or ("LONG" if order.side.lower() == "buy" else "SHORT"))
        symbol = order.symbol
        total_size = qty1 + qty2
        reduce_size = total_size * (float(self.config.execution.be_reduce_pct) / 100.0)
        if reduce_size <= 0:
            return

        close_side = "sell" if side.upper() == "LONG" else "buy"
        reduce_only = self.config.bitget.position_mode == "one_way_mode"
        trade_side = "close" if self.config.bitget.position_mode == "hedge_mode" else None
        hold_side = "long" if close_side == "sell" else "short"
        trigger = avg_entry
        client_oid = f"be-{thread_id}-{int(utc_now().timestamp())}"

        if self.config.dry_run:
            self.state.upsert_order(
                OrderState(
                    symbol=symbol,
                    side=close_side,
                    status="ACKED",
                    filled=0.0,
                    quantity=reduce_size,
                    avg_price=None,
                    reduce_only=reduce_only,
                    trade_side=trade_side,
                    purpose="be_reduce",
                    timestamp=utc_now(),
                    client_order_id=client_oid,
                    order_id=f"dry-{client_oid}",
                    trigger_price=trigger,
                    is_plan_order=True,
                    parent_client_order_id=order.client_order_id,
                    thread_id=thread_id,
                )
            )
            self.store.record_reconciler_action(
                symbol=symbol,
                order_id=f"dry-{client_oid}",
                client_order_id=client_oid,
                action="BE_REDUCE_SUBMITTED",
                reason="dry_run",
                payload={"avg_entry": avg_entry, "size": reduce_size, "purpose": "be_reduce"},
                trace_id=trace,
                thread_id=thread_id,
                purpose="be_reduce",
            )
            return

        if not self._supports_plan_orders():
            self._arm_be_reduce_local_guard(
                symbol=symbol,
                close_side=close_side,
                trade_side=trade_side,
                reduce_only=reduce_only,
                trigger_price=trigger,
                size=reduce_size,
                parent_client_order_id=order.client_order_id,
                thread_id=thread_id,
                trace=trace,
            )
            return

        try:
            ack = self.bitget.place_take_profit(
                symbol=symbol,
                product_type=self.config.bitget.product_type,
                margin_mode=self.config.bitget.margin_mode,
                position_mode=self.config.bitget.position_mode,
                hold_side=hold_side,
                trigger_price=trigger,
                order_price=None,
                size=reduce_size,
                side=close_side,
                trade_side=trade_side,
                reduce_only=reduce_only,
                client_oid=client_oid,
                trigger_type=self.config.execution.be_reduce_trigger_type,
            )
            self.state.upsert_order(
                OrderState(
                    symbol=symbol,
                    side=close_side,
                    status=ack.status or "ACKED",
                    filled=0.0,
                    quantity=reduce_size,
                    avg_price=None,
                    reduce_only=reduce_only,
                    trade_side=trade_side,
                    purpose="be_reduce",
                    timestamp=utc_now(),
                    client_order_id=ack.client_oid or client_oid,
                    order_id=ack.order_id,
                    trigger_price=trigger,
                    is_plan_order=True,
                    parent_client_order_id=order.client_order_id,
                    thread_id=thread_id,
                )
            )
            self.store.record_reconciler_action(
                symbol=symbol,
                order_id=ack.order_id,
                client_order_id=ack.client_oid or client_oid,
                action="BE_REDUCE_SUBMITTED",
                reason="submitted",
                payload={"avg_entry": avg_entry, "size": reduce_size, "purpose": "be_reduce"},
                trace_id=trace,
                thread_id=thread_id,
                purpose="be_reduce",
            )
        except Exception as exc:  # noqa: BLE001
            self._arm_be_reduce_local_guard(
                symbol=symbol,
                close_side=close_side,
                trade_side=trade_side,
                reduce_only=reduce_only,
                trigger_price=trigger,
                size=reduce_size,
                parent_client_order_id=order.client_order_id,
                thread_id=thread_id,
                trace=trace,
            )
            self.store.record_reconciler_action(
                symbol=symbol,
                order_id=None,
                client_order_id=client_oid,
                action="BE_REDUCE_FAILED",
                reason=str(exc),
                payload={"avg_entry": avg_entry, "size": reduce_size, "purpose": "be_reduce"},
                trace_id=trace,
                thread_id=thread_id,
                purpose="be_reduce",
            )

    def _thread_stop_loss(self, thread_id: int | None) -> float | None:
        if thread_id is None:
            return None
        thread = self.store.get_trade_thread(thread_id)
        if not thread:
            return None
        stop_loss = thread.get("stop_loss")
        if stop_loss in {None, ""}:
            return None
        return float(stop_loss)

    def _has_active_tp(self, symbol: str, thread_id: int | None) -> bool:
        for item in self.state.orders_by_client_id.values():
            if item.symbol.upper() != symbol.upper():
                continue
            if item.thread_id != thread_id:
                continue
            if item.purpose.lower() != "tp":
                continue
            if item.status.upper() in {"CANCELED", "FAILED", "REJECTED"}:
                continue
            return True
        return False

    def _place_tp_orders(
        self,
        *,
        symbol: str,
        thread_id: int | None,
        side_hint: str | None,
        total_size: float,
        tp_points: list[float],
        parent_client_order_id: str | None,
    ) -> None:
        if total_size <= 0 or not tp_points:
            return
        side = "sell" if str(side_hint or "LONG").upper() == "LONG" else "buy"
        trade_side = "close" if self.config.bitget.position_mode == "hedge_mode" else None
        reduce_only = self.config.bitget.position_mode == "one_way_mode"
        hold_side = "long" if side == "sell" else "short"

        size_each = total_size / len(tp_points)
        for idx, tp in enumerate(tp_points):
            if size_each <= 0:
                self.store.record_event(
                    event_type="TP_SKIPPED_INVALID_SIZE",
                    level="WARN",
                    msg="skip TP placement due to non-positive size",
                    payload={"symbol": symbol, "tp_price": float(tp), "size_each": size_each},
                    reason="size_each<=0",
                    thread_id=thread_id,
                )
                continue
            client_oid = f"tp-{thread_id or 0}-{idx}-{int(utc_now().timestamp())}"
            if self.config.dry_run:
                self.state.upsert_order(
                    OrderState(
                        symbol=symbol,
                        side=side,
                        status="ACKED",
                        filled=0.0,
                        quantity=size_each,
                        avg_price=None,
                        reduce_only=reduce_only,
                        trade_side=trade_side,
                        purpose="tp",
                        timestamp=utc_now(),
                        client_order_id=client_oid,
                        order_id=f"dry-{client_oid}",
                        trigger_price=float(tp),
                        is_plan_order=True,
                        parent_client_order_id=parent_client_order_id,
                        thread_id=thread_id,
                    )
                )
                continue
            try:
                ack = self.bitget.place_take_profit(
                    symbol=symbol,
                    product_type=self.config.bitget.product_type,
                    margin_mode=self.config.bitget.margin_mode,
                    position_mode=self.config.bitget.position_mode,
                    hold_side=hold_side,
                    trigger_price=float(tp),
                    order_price=None,
                    size=size_each,
                    side=side,
                    trade_side=trade_side,
                    reduce_only=reduce_only,
                    client_oid=client_oid,
                    trigger_type=self.config.risk.stoploss.trigger_price_type,
                )
                self.state.upsert_order(
                    OrderState(
                        symbol=symbol,
                        side=side,
                        status=ack.status or "ACKED",
                        filled=0.0,
                        quantity=size_each,
                        avg_price=None,
                        reduce_only=reduce_only,
                        trade_side=trade_side,
                        purpose="tp",
                        timestamp=utc_now(),
                        client_order_id=ack.client_oid or client_oid,
                        order_id=ack.order_id,
                        trigger_price=float(tp),
                        is_plan_order=True,
                        parent_client_order_id=parent_client_order_id,
                        thread_id=thread_id,
                    )
                )
            except Exception as exc:  # noqa: BLE001
                self.store.record_event(
                    event_type="TP_PLACE_FAILED",
                    level="ERROR",
                    msg="failed to place TP plan order",
                    payload={"symbol": symbol, "reason": str(exc), "tp_price": float(tp), "size": size_each},
                    reason=str(exc),
                    thread_id=thread_id,
                )

    def _arm_be_reduce_local_guard(
        self,
        *,
        symbol: str,
        close_side: str,
        trade_side: str | None,
        reduce_only: bool,
        trigger_price: float,
        size: float,
        parent_client_order_id: str | None,
        thread_id: int | None,
        trace: str,
    ) -> None:
        client_oid = f"be-local-{thread_id or 0}-{int(utc_now().timestamp())}"
        self.state.upsert_order(
            OrderState(
                symbol=symbol,
                side=close_side,
                status="ACKED",
                filled=0.0,
                quantity=size,
                avg_price=None,
                reduce_only=reduce_only,
                trade_side=trade_side,
                purpose="be_reduce_local",
                timestamp=utc_now(),
                client_order_id=client_oid,
                order_id=None,
                trigger_price=trigger_price,
                is_plan_order=False,
                parent_client_order_id=parent_client_order_id,
                thread_id=thread_id,
            )
        )
        self.store.record_event(
            event_type="PLAN_ORDER_FALLBACK",
            level="ERROR",
            msg="be_reduce trigger fallback to local guard",
            payload={"symbol": symbol, "thread_id": thread_id, "trigger_price": trigger_price, "size": size},
            reason="be_reduce_plan_unsupported",
            thread_id=thread_id,
        )
        self.store.record_reconciler_action(
            symbol=symbol,
            order_id=None,
            client_order_id=client_oid,
            action="BE_REDUCE_LOCAL_GUARD_ARMED",
            reason="plan_fallback",
            payload={"trigger_price": trigger_price, "size": size, "purpose": "be_reduce"},
            trace_id=trace,
            thread_id=thread_id,
            purpose="be_reduce",
        )

    def _process_be_reduce_local_guards(self) -> None:
        for order in list(self.state.orders_by_client_id.values()):
            if order.purpose.lower() != "be_reduce_local":
                continue
            if order.status.upper() in {"FILLED", "CANCELED", "FAILED", "REJECTED"}:
                continue
            if order.trigger_price is None or not order.quantity or order.quantity <= 0:
                continue
            snap = self.state.get_price(order.symbol)
            if snap is None:
                continue
            px = snap.mark if snap.mark is not None else snap.last
            if px is None:
                continue
            should_trigger = False
            if order.side.lower() == "sell" and px >= order.trigger_price:
                should_trigger = True
            if order.side.lower() == "buy" and px <= order.trigger_price:
                should_trigger = True
            if not should_trigger:
                continue

            if self.config.dry_run:
                self.state.mark_order_status(
                    status="FILLED",
                    client_order_id=order.client_order_id,
                    order_id=order.order_id,
                )
                continue

            try:
                self.bitget.place_order(
                    symbol=order.symbol,
                    side=order.side,
                    trade_side=order.trade_side,
                    size=float(order.quantity),
                    order_type="market",
                    reduce_only=bool(order.reduce_only),
                    client_oid=f"be-local-close-{int(utc_now().timestamp())}",
                )
                self.state.mark_order_status(
                    status="FILLED",
                    client_order_id=order.client_order_id,
                    order_id=order.order_id,
                )
            except Exception as exc:  # noqa: BLE001
                self.state.mark_order_status(
                    status="FAILED",
                    client_order_id=order.client_order_id,
                    order_id=order.order_id,
                )
                self.store.record_event(
                    event_type="BE_REDUCE_LOCAL_TRIGGER_FAIL",
                    level="ERROR",
                    msg="local be_reduce trigger market close failed",
                    payload={"symbol": order.symbol, "reason": str(exc)},
                    reason=str(exc),
                    thread_id=order.thread_id,
                )

    def _supports_plan_orders(self) -> bool:
        fn = getattr(self.bitget, "supports_plan_orders", None)
        if callable(fn):
            try:
                return bool(fn())
            except Exception:  # noqa: BLE001
                return False
        return False

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
