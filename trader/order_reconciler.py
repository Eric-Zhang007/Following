from __future__ import annotations

import asyncio
import math
import time
from dataclasses import asdict

from trader.alerts import AlertManager
from trader.bitget_client import BitgetClient
from trader.config import AppConfig
from trader.side_mapper import close_side_for_hold, normalize_hold_side
from trader.state import OrderState, PositionState, StateStore, utc_now
from trader.stoploss_manager import StopLossManager
from trader.store import SQLiteStore
from trader.symbol_registry import SymbolRegistry
from trader.tp_allocation import remaining_tp_weights


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
        symbol_registry: SymbolRegistry | None = None,
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
        self.symbol_registry = symbol_registry
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
        # Avoid high-frequency info events for every polling tick to keep DB/log volume bounded.
        trace = f"reconcile-{int(time.time() * 1000)}"

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
            prev_status = str(order.status).upper()
            prev_filled = float(order.filled or 0.0)
            payload = await asyncio.to_thread(self._fetch_order_state, order)
            raw_status = str(payload.get("state", payload.get("status", "NEW")))
            status = self._normalize_status(raw_status)
            if self._is_ambiguous_tp_closure(order=order, raw_status=raw_status, payload=payload):
                status = "CANCELED"
            filled = float(payload.get("baseVolume", payload.get("filledQty", order.filled)) or 0.0)
            avg_price_raw = payload.get("priceAvg", payload.get("avgPrice"))
            avg_price = float(avg_price_raw) if avg_price_raw not in {None, ""} else order.avg_price
            if self._should_cancel_stale_unfilled(order=order, status=status, filled=filled, payload=payload):
                self._cancel_stale_order(order=order, trace=trace, payload=payload)
                return

            self._transition(order=order, status=status, filled=filled, avg_price=avg_price)
            self._emit_order_fill_event(
                order=order,
                status=status,
                filled=filled,
                avg_price=avg_price,
                prev_status=prev_status,
                prev_filled=prev_filled,
            )
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
            self._track_filled_tp_progress(order=order, status=status, prev_status=prev_status, trace=trace)

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
                self.alerts.error(
                    "RECONCILE_ORDER_ERROR_BURST",
                    "reconcile retries exceeded configured threshold",
                    {
                        "symbol": order.symbol,
                        "retry": count,
                        "max_submit_retries": self.config.execution.max_submit_retries,
                        "purpose": order.purpose,
                    },
                )

    def _fetch_order_state(self, order: OrderState) -> dict:
        try:
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
        except Exception as exc:  # noqa: BLE001
            # Bitget 40109: order record lookup unavailable.
            # For long-wait limit entries we must keep tracking instead of force-canceling.
            message = str(exc).lower()
            if "40109" in message or ("order cannot be found" in message) or ("data of the order cannot be found" in message):
                # Try to infer from current position for entry orders so protection can still be attached.
                if order.purpose.lower() == "entry":
                    try:
                        pos_payload = self.bitget.get_position(order.symbol)
                        inferred_size = self._extract_position_size(pos_payload)
                        if inferred_size > 0:
                            return {
                                "state": "FILLED",
                                "baseVolume": max(float(order.filled or 0.0), float(inferred_size)),
                            }
                    except Exception:  # noqa: BLE001
                        pass
                # Keep order alive and continue reconciliation on later ticks.
                return {"state": "ACKED", "baseVolume": order.filled}
            raise

    async def _ensure_entry_filled_has_sl(self, order: OrderState, filled_qty: float, avg_price: float | None, trace: str) -> None:
        desired_sl = self._thread_stop_loss(order.thread_id)
        if desired_sl is None and (avg_price is None or avg_price <= 0):
            # External/manual orders may not map to a tracked thread and can lack
            # reliable average price from the exchange payload. Skip instead of
            # raising repeated invalid_trigger_price failures every poll tick.
            self.store.record_reconciler_action(
                symbol=order.symbol,
                order_id=order.order_id,
                client_order_id=order.client_order_id,
                action="PARTIAL_FILL_ENSURE_SL_SKIPPED",
                reason="missing_thread_stoploss_and_avg_price",
                payload={"qty": filled_qty, "purpose": "sl"},
                trace_id=trace,
                thread_id=order.thread_id,
                purpose="sl",
            )
            return

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
            desired_sl_price=desired_sl,
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
            self.alerts.error(
                "STOPLOSS_PLACE_FAIL",
                "failed to place stop-loss on entry fill",
                {"symbol": order.symbol, "reason": result.reason, "thread_id": order.thread_id},
            )

    async def _ensure_entry_filled_has_tp(self, order: OrderState, filled_qty: float, trace: str) -> None:
        if not self.config.execution.place_tp_on_fill:
            return
        thread = self.store.get_trade_thread(order.thread_id) if order.thread_id is not None else None
        if not thread:
            return
        tp_points = self._remaining_tp_points(order.thread_id)
        if not tp_points:
            return
        if self._has_active_tp(order.symbol, order.thread_id, tp_points=tp_points):
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

        hold_side = normalize_hold_side(side)
        close_side = close_side_for_hold(hold_side, self.config.bitget.position_mode)
        reduce_only = self.config.bitget.position_mode == "one_way_mode"
        trade_side = "close" if self.config.bitget.position_mode == "hedge_mode" else None
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

    def _track_filled_tp_progress(self, *, order: OrderState, status: str, prev_status: str, trace: str) -> None:
        if order.purpose.lower() != "tp":
            return
        if status != "FILLED" or prev_status == "FILLED":
            return
        if order.thread_id is None or order.trigger_price is None:
            return
        remaining_tp_points = self.store.mark_tp_point_filled(thread_id=order.thread_id, tp_price=float(order.trigger_price))
        progress_key = f"tp_progress::{order.symbol.upper()}::{order.thread_id}"
        self.store.set_system_flag(progress_key, str(utc_now().timestamp()))
        self.store.record_reconciler_action(
            symbol=order.symbol,
            order_id=order.order_id,
            client_order_id=order.client_order_id,
            action="TP_PROGRESS_RECORDED",
            reason="tp_filled",
            payload={
                "filled_tp_price": float(order.trigger_price),
                "remaining_tp_points": remaining_tp_points,
            },
            trace_id=trace,
            thread_id=order.thread_id,
            purpose="tp",
        )

    def _remaining_tp_points(self, thread_id: int | None) -> list[float]:
        if thread_id is None:
            return []
        for item in self.state.all_orders():
            if item.thread_id != thread_id:
                continue
            if item.purpose.lower() != "tp":
                continue
            if item.status.upper() != "FILLED":
                continue
            if item.trigger_price is None:
                continue
            self.store.mark_tp_point_filled(thread_id=thread_id, tp_price=float(item.trigger_price))
        return self.store.get_remaining_tp_points(thread_id)

    @staticmethod
    def _is_ambiguous_tp_closure(*, order: OrderState, raw_status: str, payload: dict) -> bool:
        if order.purpose.lower() != "tp":
            return False
        if raw_status.upper().strip() != "FILLED_OR_CLOSED":
            return False
        filled = float(payload.get("baseVolume", payload.get("filledQty", 0.0)) or 0.0)
        avg_price = payload.get("priceAvg", payload.get("avgPrice"))
        return filled <= 0 and avg_price in {None, ""}

    def _has_active_tp(self, symbol: str, thread_id: int | None, *, tp_points: list[float] | None = None) -> bool:
        thread = self.store.get_trade_thread(thread_id) if thread_id is not None else None
        side_hint = str((thread or {}).get("side") or "").upper()
        remaining_tp_points = (
            [float(v) for v in tp_points]
            if tp_points is not None
            else self._remaining_tp_points(thread_id)
        )
        position = self.state.positions.get(symbol.upper())
        entry_ref = float(position.entry_price) if position and position.entry_price not in {None, 0} else None
        if entry_ref is None and remaining_tp_points:
            # Infer side from relative TP location when position snapshot is absent.
            min_tp = min(remaining_tp_points)
            max_tp = max(remaining_tp_points)
            if side_hint == "LONG":
                entry_ref = min_tp
            elif side_hint == "SHORT":
                entry_ref = max_tp
        expected_close_side = None
        if side_hint == "LONG":
            expected_close_side = close_side_for_hold("long", self.config.bitget.position_mode)
        elif side_hint == "SHORT":
            expected_close_side = close_side_for_hold("short", self.config.bitget.position_mode)

        for item in self.state.all_orders():
            if item.symbol.upper() != symbol.upper():
                continue
            if item.thread_id != thread_id:
                continue
            if item.status.upper() in {"CANCELED", "FAILED", "REJECTED", "FILLED"}:
                continue
            is_close_order = bool(item.reduce_only) or (item.trade_side or "").lower() == "close"
            if not is_close_order:
                continue
            if expected_close_side is not None and item.side.lower() != expected_close_side:
                continue
            purpose = item.purpose.lower()
            if purpose == "tp":
                if item.trigger_price is None:
                    return True
                trigger = float(item.trigger_price)
                if not remaining_tp_points:
                    continue
                if any(abs(trigger - p) <= max(1e-9, abs(p) * 1e-6) for p in remaining_tp_points):
                    return True
                continue
            if purpose in {"sl", "be_reduce", "be_reduce_local"}:
                continue
            if not item.is_plan_order:
                continue
            if item.trigger_price is None:
                continue
            trigger = float(item.trigger_price)
            if remaining_tp_points and any(abs(trigger - p) <= max(1e-9, abs(p) * 1e-6) for p in remaining_tp_points):
                return True
            if remaining_tp_points:
                continue
            if entry_ref is None:
                continue
            if side_hint == "LONG" and trigger > entry_ref:
                return True
            if side_hint == "SHORT" and trigger < entry_ref:
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
        started_at = time.perf_counter()
        if total_size <= 0 or not tp_points:
            return
        hold_side = "long" if str(side_hint or "LONG").upper() == "LONG" else "short"
        side = close_side_for_hold(hold_side, self.config.bitget.position_mode)
        trade_side = "close" if self.config.bitget.position_mode == "hedge_mode" else None
        reduce_only = self.config.bitget.position_mode == "one_way_mode"

        thread = self.store.get_trade_thread(thread_id) if thread_id is not None else None
        all_tp_points = [float(v) for v in (thread or {}).get("tp_points", [])] or [float(v) for v in tp_points]
        weights = remaining_tp_weights(all_tp_points, [float(v) for v in tp_points])
        remaining_size = total_size
        placed = 0
        skipped = 0
        last_reason: str | None = None
        for idx, tp in enumerate(tp_points):
            weight = weights[idx] if idx < len(weights) else (1.0 / max(len(tp_points), 1))
            requested_size = remaining_size if idx == len(tp_points) - 1 else (total_size * float(weight))
            normalized_size, reject_reason = self._normalize_reduce_size(symbol, requested_size)
            if reject_reason or normalized_size <= 0:
                skipped += 1
                last_reason = reject_reason or "size_non_positive_after_normalize"
                self.store.record_event(
                    event_type="TP_SKIPPED_INVALID_SIZE",
                    level="WARN",
                    msg="skip TP placement due to non-positive size",
                    payload={
                        "symbol": symbol,
                        "tp_price": float(tp),
                        "requested_size": requested_size,
                        "normalized_size": normalized_size,
                        "reason": last_reason,
                    },
                    reason=last_reason,
                    thread_id=thread_id,
                )
                continue
            order_size = float(normalized_size)
            client_oid = f"tp-{thread_id or 0}-{idx}-{int(utc_now().timestamp())}"
            if self.config.dry_run:
                self.state.upsert_order(
                    OrderState(
                        symbol=symbol,
                        side=side,
                        status="ACKED",
                        filled=0.0,
                        quantity=order_size,
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
                placed += 1
                remaining_size = max(0.0, remaining_size - order_size)
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
                    size=order_size,
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
                        quantity=order_size,
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
                placed += 1
                remaining_size = max(0.0, remaining_size - order_size)
            except Exception as exc:  # noqa: BLE001
                skipped += 1
                last_reason = str(exc)
                self.store.record_event(
                    event_type="TP_PLACE_FAILED",
                    level="ERROR",
                    msg="failed to place TP plan order",
                    payload={"symbol": symbol, "reason": str(exc), "tp_price": float(tp), "size": order_size},
                    reason=str(exc),
                    thread_id=thread_id,
                )
        if self.config.dry_run:
            return

        elapsed_ms = max(0, int((time.perf_counter() - started_at) * 1000))
        if placed > 0:
            self.alerts.info(
                "TP_SUBMITTED",
                "take-profit orders submitted",
                {
                    "symbol": symbol,
                    "purpose": "tp",
                    "thread_id": thread_id,
                    "tp_count": placed,
                    "tp_total": len(tp_points),
                    "skipped": skipped,
                    "total_size": total_size,
                    "elapsed_ms": elapsed_ms,
                    "source": "reconciler_partial_fill",
                },
            )

    def _normalize_reduce_size(self, symbol: str, quantity: float) -> tuple[float, str | None]:
        if quantity <= 0:
            return 0.0, "quantity<=0"
        if self.symbol_registry is None:
            rounded_qty = float(f"{quantity:.6f}")
            return rounded_qty, None if rounded_qty > 0 else "quantity<=0_after_rounding"

        contract = self.symbol_registry.get_contract(symbol)
        if contract is None:
            return 0.0, f"contract config unavailable for symbol: {symbol}"

        rounded_qty = self._round_down(quantity, contract.size_place)
        if rounded_qty <= 0:
            return rounded_qty, f"quantity<=0_after_sizePlace_rounding({contract.size_place})"
        if contract.min_trade_num > 0 and rounded_qty < contract.min_trade_num:
            return rounded_qty, f"quantity {rounded_qty} below minTradeNum {contract.min_trade_num}"
        return rounded_qty, None

    @staticmethod
    def _round_down(value: float, places: int) -> float:
        if places < 0:
            return value
        factor = 10**places
        return math.floor(value * factor + 1e-12) / factor
        if skipped > 0:
            self.alerts.error(
                "TP_SUBMIT_FAILED",
                "failed to submit take-profit orders",
                {
                    "symbol": symbol,
                    "purpose": "tp",
                    "thread_id": thread_id,
                    "failed_count": skipped,
                    "tp_total": len(tp_points),
                    "placed": placed,
                    "reason": last_reason,
                    "elapsed_ms": elapsed_ms,
                    "source": "reconciler_partial_fill",
                },
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
        self.alerts.error(
            "PLAN_ORDER_FALLBACK",
            "be_reduce trigger fallback to local guard",
            {"symbol": symbol, "thread_id": thread_id, "trigger_price": trigger_price, "size": size},
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

    def _should_cancel_stale_unfilled(self, *, order: OrderState, status: str, filled: float, payload: dict) -> bool:
        hours = int(self.config.execution.cancel_unfilled_after_hours)
        if hours <= 0:
            return False
        if order.purpose.lower() != "entry":
            return False
        if status in _TERMINAL or status == "PARTIAL":
            return False
        if float(filled or 0.0) > 0:
            return False
        created_ts = self._extract_order_created_ts(payload)
        if created_ts is None:
            return False
        age_seconds = max(0.0, utc_now().timestamp() - created_ts)
        return age_seconds >= float(hours) * 3600.0

    def _cancel_stale_order(self, *, order: OrderState, trace: str, payload: dict) -> None:
        created_ts = self._extract_order_created_ts(payload)
        age_hours = None
        if created_ts is not None:
            age_hours = (utc_now().timestamp() - created_ts) / 3600.0
        if self.config.dry_run:
            self.state.mark_order_status(
                status="CANCELED",
                client_order_id=order.client_order_id,
                order_id=order.order_id,
            )
        else:
            if order.is_plan_order and hasattr(self.bitget, "cancel_plan_order"):
                self.bitget.cancel_plan_order(
                    symbol=order.symbol,
                    order_id=order.order_id,
                    client_oid=order.client_order_id,
                )
            elif order.order_id:
                self.bitget.cancel_order(order.symbol, order.order_id)
            else:
                raise RuntimeError("cannot cancel stale order without order_id")
            self.state.mark_order_status(
                status="CANCELED",
                client_order_id=order.client_order_id,
                order_id=order.order_id,
            )
        self.store.record_reconciler_action(
            symbol=order.symbol,
            order_id=order.order_id,
            client_order_id=order.client_order_id,
            action="ORDER_STALE_CANCELED",
            reason="entry_unfilled_timeout",
            payload={
                "purpose": order.purpose,
                "age_hours": age_hours,
                "cancel_unfilled_after_hours": self.config.execution.cancel_unfilled_after_hours,
            },
            trace_id=trace,
            thread_id=order.thread_id,
            purpose=order.purpose,
        )
        self.alerts.warn(
            "ORDER_STALE_CANCELED",
            "entry order canceled after unfilled timeout",
            {
                "symbol": order.symbol,
                "purpose": order.purpose,
                "thread_id": order.thread_id,
                "order_id": order.order_id,
                "client_order_id": order.client_order_id,
                "age_hours": age_hours,
                "cancel_unfilled_after_hours": self.config.execution.cancel_unfilled_after_hours,
            },
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

    def _emit_order_fill_event(
        self,
        *,
        order: OrderState,
        status: str,
        filled: float,
        avg_price: float | None,
        prev_status: str,
        prev_filled: float,
    ) -> None:
        if status not in {"PARTIAL", "FILLED"}:
            return
        if status == "FILLED" and float(filled or 0.0) <= 0 and float(prev_filled or 0.0) <= 0:
            return
        if status == "PARTIAL" and prev_status in {"PARTIAL", "FILLED"}:
            return
        if status == "FILLED" and prev_status == "FILLED":
            return
        self.alerts.info(
            "ORDER_FILLED",
            "order fill update",
            {
                "symbol": order.symbol,
                "purpose": order.purpose,
                "thread_id": order.thread_id,
                "side": order.side,
                "status": status,
                "filled": filled,
                "filled_delta": max(0.0, float(filled) - float(prev_filled)),
                "avg_price": avg_price,
                "order_id": order.order_id,
                "client_order_id": order.client_order_id,
            },
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

    @staticmethod
    def _extract_position_size(position_payload: dict | list[dict] | None) -> float:
        if isinstance(position_payload, list):
            rows = position_payload
        elif isinstance(position_payload, dict):
            rows = position_payload.get("list") if isinstance(position_payload.get("list"), list) else [position_payload]
        else:
            rows = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            try:
                size = abs(float(row.get("total", row.get("size", 0)) or 0.0))
            except Exception:  # noqa: BLE001
                size = 0.0
            if size > 0:
                return size
        return 0.0

    @staticmethod
    def _extract_order_created_ts(payload: dict) -> float | None:
        for key in ("cTime", "createTime", "createdTime", "uTime"):
            raw = payload.get(key)
            if raw in (None, ""):
                continue
            try:
                value = float(raw)
            except Exception:  # noqa: BLE001
                continue
            # Bitget times are usually milliseconds.
            if value > 10_000_000_000:
                return value / 1000.0
            if value > 1_000_000_000:
                return value
        return None
