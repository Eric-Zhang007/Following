from __future__ import annotations

import asyncio
import math
import time
from dataclasses import asdict

from trader.alerts import AlertManager
from trader.bitget_client import BitgetClient
from trader.config import AppConfig
from trader.kill_switch import KillSwitch, KillSwitchAction
from trader.state import OrderState, PositionState, StateStore, utc_now
from trader.stoploss_manager import StopLossManager
from trader.store import SQLiteStore
from trader.symbol_registry import SymbolRegistry


class RiskDaemon:
    def __init__(
        self,
        config: AppConfig,
        bitget: BitgetClient,
        state: StateStore,
        store: SQLiteStore,
        alerts: AlertManager,
        kill_switch: KillSwitch,
        stoploss_manager: StopLossManager | None = None,
        symbol_registry: SymbolRegistry | None = None,
    ) -> None:
        self.config = config
        self.bitget = bitget
        self.state = state
        self.store = store
        self.alerts = alerts
        self.kill_switch = kill_switch
        self.stoploss_manager = stoploss_manager or StopLossManager(
            config=config,
            bitget=bitget,
            state=state,
            store=store,
            alerts=alerts,
        )
        self.symbol_registry = symbol_registry
        self._api_error_burst_active = False
        self._margin_used_high_active = False
        self._sl_missing_active: set[str] = set()
        self._protection_retry_after: dict[str, float] = {}

    async def run(self, stop_event: asyncio.Event) -> None:
        interval = self.config.monitor.poll_intervals.risk_daemon_seconds
        while not stop_event.is_set():
            try:
                await self.tick_once()
            except Exception as exc:  # noqa: BLE001
                self.state.register_api_error()
                self.alerts.error("RISK_DAEMON_ERROR", f"risk daemon tick failed: {exc}")
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=interval)
            except TimeoutError:
                pass

    async def tick_once(self) -> None:
        self._apply_kill_switch()
        if self.config.risk.enabled:
            self._check_api_error_burst()
            self._check_drawdown_and_margin()

        # local_guard stop-loss processing is part of SL reliability guarantees.
        self.stoploss_manager.process_local_guards()

        for position in list(self.state.positions.values()):
            await self._ensure_tracked_position_protection(position)
            await self._check_position_invariants(position)

        self.state.recompute_sl_coverage_metric()

    def _apply_kill_switch(self) -> None:
        action = self.kill_switch.read_action()
        if action == KillSwitchAction.NONE:
            return
        if action == KillSwitchAction.SAFE_MODE:
            if not self.state.safe_mode:
                self.state.enable_safe_mode("kill switch SAFE_MODE")
                self.alerts.critical(
                    "KILL_SWITCH",
                    "kill switch activated SAFE_MODE",
                    {"purpose": "risk_control", "reason": "manual_safe_mode"},
                )
            return

        if not self.state.panic_mode:
            self.state.enable_panic_mode("kill switch PANIC_CLOSE")
            self.alerts.critical(
                "KILL_SWITCH",
                "kill switch activated PANIC_CLOSE",
                {"purpose": "risk_control", "reason": "manual_panic_close"},
            )

    def _check_api_error_burst(self) -> None:
        cb = self.config.risk.circuit_breaker
        count = self.state.api_errors_in_window(cb.api_error_window_seconds)
        if count >= cb.api_error_burst:
            self.state.enable_safe_mode("api error burst exceeded")
            if not self._api_error_burst_active:
                self.alerts.error(
                    "API_ERROR_BURST",
                    "api errors exceeded burst threshold",
                    {
                        "purpose": "risk_control",
                        "reason": "api_error_burst",
                        "count": count,
                        "window_seconds": cb.api_error_window_seconds,
                    },
                )
                self._api_error_burst_active = True
            return

        if self._api_error_burst_active:
            self.alerts.info(
                "API_ERROR_BURST_RECOVERED",
                "api error burst recovered below threshold",
                {
                    "purpose": "risk_control",
                    "reason": "api_error_burst_recovered",
                    "count": count,
                    "window_seconds": cb.api_error_window_seconds,
                },
            )
        self._api_error_burst_active = False

    def _check_drawdown_and_margin(self) -> None:
        account = self.state.account
        if account is None:
            return

        if self.state.peak_equity and self.state.peak_equity > 0:
            drawdown = (self.state.peak_equity - account.equity) / self.state.peak_equity
            if drawdown > self.config.risk.max_account_drawdown_pct:
                self.state.enable_safe_mode("drawdown circuit breaker")
                self.alerts.error(
                    "DRAWDOWN_BREAKER",
                    "drawdown exceeded max threshold",
                    {
                        "purpose": "risk_control",
                        "reason": "drawdown_exceeded",
                        "drawdown": drawdown,
                        "max_account_drawdown_pct": self.config.risk.max_account_drawdown_pct,
                    },
                )

        if account.equity > 0:
            margin_ratio = max(account.margin_used, 0.0) / account.equity
            if margin_ratio > self.config.risk.max_total_margin_used_pct:
                self.state.enable_safe_mode("margin used ratio too high")
                if not self._margin_used_high_active:
                    self.alerts.warn(
                        "MARGIN_USED_HIGH",
                        "margin used ratio above threshold",
                        {
                            "purpose": "risk_control",
                            "reason": "margin_used_high",
                            "margin_ratio": margin_ratio,
                            "max_total_margin_used_pct": self.config.risk.max_total_margin_used_pct,
                        },
                    )
                    self._margin_used_high_active = True
            elif self._margin_used_high_active:
                self.alerts.info(
                    "MARGIN_USED_HIGH_RECOVERED",
                    "margin used ratio recovered below threshold",
                    {
                        "purpose": "risk_control",
                        "reason": "margin_used_high_recovered",
                        "margin_ratio": margin_ratio,
                        "max_total_margin_used_pct": self.config.risk.max_total_margin_used_pct,
                    },
                )
                self._margin_used_high_active = False

    async def _check_position_invariants(self, position: PositionState) -> None:
        if self.config.risk.enabled and self._is_liq_too_close(position):
            if not self.config.execution.close_on_invariant_violation:
                trace = self.alerts.warn(
                    "LIQUIDATION_DISTANCE_RISK",
                    "liquidation distance is too close (report-only mode, no auto close/reduce)",
                    {
                        "symbol": position.symbol,
                        "purpose": "risk_control",
                        "reason": "liquidation_distance_too_close",
                        "mark_price": position.mark_price,
                        "liq_price": position.liq_price,
                        "max_liquidation_distance_pct": self.config.risk.max_liquidation_distance_pct,
                    },
                )
                self.store.record_invariant_violation(
                    invariant_name="LIQ_DISTANCE_TOO_CLOSE",
                    symbol=position.symbol,
                    reason="report_only_no_auto_action",
                    payload=asdict(position),
                    trace_id=trace,
                )
                return
            reduced = await self._reduce_position_once(position, reason="liquidation_distance_too_close")
            if not reduced:
                await self._protective_close(position, reason="liquidation_distance_too_close")
            return

        require_sl = self.config.risk.stoploss.must_exist or self.config.risk.hard_invariants.require_stoploss
        if not require_sl:
            return

        sl_key = f"{position.symbol.upper()}::{position.side.lower()}"
        if self.state.has_valid_stop_loss(position.symbol, position.side):
            if sl_key in self._sl_missing_active:
                self.alerts.info(
                    "SL_MISSING_RECOVERED",
                    "stop-loss protection recovered",
                    {
                        "symbol": position.symbol,
                        "purpose": "sl",
                        "reason": "stoploss_recovered",
                        "side": position.side,
                        "size": position.size,
                    },
                )
            self._sl_missing_active.discard(sl_key)
            return

        self.state.metrics["sl_missing_count"] = self.state.metrics.get("sl_missing_count", 0.0) + 1.0
        trace: str | None = None
        if sl_key not in self._sl_missing_active:
            trace = self.alerts.warn(
                "SL_MISSING",
                "position without valid stop-loss detected",
                {
                    "symbol": position.symbol,
                    "purpose": "sl",
                    "reason": "missing_stoploss",
                    "side": position.side,
                    "size": position.size,
                },
            )
            self.store.record_invariant_violation(
                invariant_name="SL_MUST_EXIST",
                symbol=position.symbol,
                reason="missing protective stop-loss",
                payload=asdict(position),
                trace_id=trace,
            )
            self._sl_missing_active.add(sl_key)
        if not self.config.execution.close_on_invariant_violation:
            self.store.record_reconciler_action(
                symbol=position.symbol,
                order_id=None,
                client_order_id=None,
                action="SL_AUTOFIX_SKIPPED_REPORT_ONLY",
                reason="close_on_invariant_violation=false",
                payload={"purpose": "sl"},
                trace_id=trace,
            )
            return

        result = self.stoploss_manager.ensure_stop_loss(
            position_state=position,
            desired_sl_price=None,
            desired_size=position.size,
            source="risk_daemon_autofix",
        )
        self.store.record_reconciler_action(
            symbol=position.symbol,
            order_id=result.order_id,
            client_order_id=result.client_order_id,
            action="SL_AUTOFIX_ATTEMPT",
            reason=result.reason,
            payload={"purpose": "sl", "mode": result.mode, "ok": result.ok},
            trace_id=result.trace_id,
        )
        if not result.ok:
            self.alerts.error(
                "STOPLOSS_PLACE_FAIL",
                "stoploss placement failed during invariant check",
                {
                    "symbol": position.symbol,
                    "purpose": "sl",
                    "reason": result.reason,
                },
            )

        if result.ok:
            return

        elapsed = 0.0
        if position.opened_at is not None:
            elapsed = (utc_now() - position.opened_at).total_seconds()

        if (
            elapsed >= self.config.risk.stoploss.max_time_without_sl_seconds
            and self.config.risk.stoploss.emergency_close_if_sl_place_fails
        ):
            await self._protective_close(position, reason="sl_autofix_failed_then_panic")
            self.state.enable_safe_mode("SL placement failed and timeout reached")
            self.alerts.critical(
                "SL_AUTOFIX_FAILED_THEN_PANIC",
                "stoploss autofix failed beyond timeout; panic close triggered",
                {
                    "symbol": position.symbol,
                    "purpose": "emergency_close",
                    "reason": "sl_autofix_failed_then_panic",
                    "elapsed": elapsed,
                    "timeout": self.config.risk.stoploss.max_time_without_sl_seconds,
                },
            )

    async def _reduce_position_once(self, position: PositionState, reason: str) -> bool:
        qty = max(position.size * 0.5, 0.0)
        if qty <= 0:
            return False
        close_side = "sell" if position.side.lower() == "long" else "buy"
        trace = self.alerts.warn(
            "RISK_REDUCE_ATTEMPT",
            "trying risk-driven partial reduce before full close",
            {"symbol": position.symbol, "purpose": "reduce", "reason": reason, "qty": qty},
        )

        if self.config.dry_run:
            self.store.record_reconciler_action(
                symbol=position.symbol,
                order_id=None,
                client_order_id=None,
                action="RISK_REDUCE_DRY_RUN",
                reason=reason,
                payload={"qty": qty, "purpose": "reduce"},
                trace_id=trace,
            )
            return True

        try:
            self.bitget.place_order(
                symbol=position.symbol,
                side=close_side,
                trade_side="close" if self.config.bitget.position_mode == "hedge_mode" else None,
                size=qty,
                order_type="market",
                reduce_only=self.config.bitget.position_mode == "one_way_mode",
                client_oid=f"risk-reduce-{int(utc_now().timestamp() * 1000)}",
            )
            self.store.record_reconciler_action(
                symbol=position.symbol,
                order_id=None,
                client_order_id=None,
                action="RISK_REDUCE_EXECUTED",
                reason=reason,
                payload={"qty": qty, "purpose": "reduce"},
                trace_id=trace,
            )
            return True
        except Exception as exc:  # noqa: BLE001
            self.state.register_api_error()
            self.store.record_reconciler_action(
                symbol=position.symbol,
                order_id=None,
                client_order_id=None,
                action="RISK_REDUCE_FAILED",
                reason=str(exc),
                payload={"qty": qty, "purpose": "reduce", "origin_reason": reason},
                trace_id=trace,
            )
            return False

    async def _protective_close(self, position: PositionState, reason: str) -> None:
        self.alerts.critical(
            "PANIC_CLOSE",
            "panic close requested",
            {
                "symbol": position.symbol,
                "purpose": "emergency_close",
                "reason": reason,
                "size": position.size,
            },
        )
        trace = self.alerts.critical(
            "PROTECTIVE_CLOSE",
            "triggering protective close",
            {
                "symbol": position.symbol,
                "purpose": "emergency_close",
                "reason": reason,
                "size": position.size,
            },
        )
        self.store.record_invariant_violation(
            invariant_name="PROTECTIVE_CLOSE",
            symbol=position.symbol,
            reason=reason,
            payload=asdict(position),
            trace_id=trace,
        )

        if self.config.dry_run:
            self.store.record_reconciler_action(
                symbol=position.symbol,
                order_id=None,
                client_order_id=None,
                action="PROTECTIVE_CLOSE_DRY_RUN",
                reason=reason,
                payload={"size": position.size, "purpose": "emergency_close"},
                trace_id=trace,
            )
            return

        try:
            await asyncio.to_thread(self.bitget.protective_close_position, position.symbol, position.side, position.size)
            self.store.record_reconciler_action(
                symbol=position.symbol,
                order_id=None,
                client_order_id=None,
                action="PROTECTIVE_CLOSE_EXECUTED",
                reason=reason,
                payload={"size": position.size, "purpose": "emergency_close"},
                trace_id=trace,
            )
        except Exception as exc:  # noqa: BLE001
            self.state.register_api_error()
            self.store.record_reconciler_action(
                symbol=position.symbol,
                order_id=None,
                client_order_id=None,
                action="PROTECTIVE_CLOSE_FAILED",
                reason=str(exc),
                payload={"size": position.size, "origin_reason": reason, "purpose": "emergency_close"},
                trace_id=trace,
            )
            self.alerts.error(
                "PROTECTIVE_CLOSE_FAILED",
                "failed to execute protective close",
                {
                    "symbol": position.symbol,
                    "purpose": "emergency_close",
                    "reason": str(exc),
                },
            )

    def _is_liq_too_close(self, position: PositionState) -> bool:
        if position.liq_price is None or position.mark_price is None or position.mark_price <= 0:
            return False
        distance = abs(position.liq_price - position.mark_price) / position.mark_price
        return distance <= self.config.risk.max_liquidation_distance_pct

    async def _ensure_tracked_position_protection(self, position: PositionState) -> None:
        thread = self.store.get_latest_trade_thread_by_symbol(position.symbol, active_only=True)
        if thread is None:
            return

        key = f"{position.symbol.upper()}::{position.side.lower()}"
        now_ts = time.time()
        if now_ts < float(self._protection_retry_after.get(key, 0.0)):
            return

        trace_id: str | None = None
        sl_ready = self.state.has_valid_stop_loss(position.symbol, position.side)
        if not sl_ready:
            result = self.stoploss_manager.ensure_stop_loss(
                position_state=position,
                desired_sl_price=thread.get("stop_loss"),
                desired_size=position.size,
                source="tracked_position_autoprotect",
            )
            trace_id = result.trace_id
            self.store.record_reconciler_action(
                symbol=position.symbol,
                order_id=result.order_id,
                client_order_id=result.client_order_id,
                action="TRACKED_POSITION_ENSURE_SL",
                reason=result.reason,
                payload={"purpose": "sl", "thread_id": thread.get("thread_id"), "ok": result.ok, "mode": result.mode},
                trace_id=result.trace_id,
                thread_id=thread.get("thread_id"),
                purpose="sl",
            )
            if not result.ok:
                self._protection_retry_after[key] = now_ts + 15.0
                return

        if not self.config.execution.place_tp_on_fill:
            return
        tp_points = [float(v) for v in thread.get("tp_points", []) if float(v) > 0]
        if not tp_points:
            return
        if self._has_active_tp(position, int(thread["thread_id"])):
            return
        ok = self._place_tp_for_tracked_position(
            symbol=position.symbol,
            side_hint=thread.get("side"),
            total_size=position.size,
            thread_id=int(thread["thread_id"]),
            tp_points=tp_points,
            parent_client_order_id=None,
        )
        if not ok:
            self._protection_retry_after[key] = now_ts + 15.0
            return
        self._protection_retry_after.pop(key, None)

    def _has_active_tp(self, position: PositionState, thread_id: int) -> bool:
        symbol = position.symbol
        expected_close_side = "sell" if position.side.lower() == "long" else "buy"
        entry_price = float(position.entry_price) if position.entry_price not in {None, 0} else None
        for order in self.state.all_orders():
            if order.symbol.upper() != symbol.upper():
                continue
            if order.status.upper() in {"CANCELED", "FAILED", "REJECTED"}:
                continue
            if order.thread_id == thread_id and order.purpose.lower() == "tp":
                return True
            if not order.is_plan_order:
                continue
            purpose = (order.purpose or "").lower()
            client_oid = (order.client_order_id or "").lower()
            if purpose == "sl" or client_oid.startswith("sl-"):
                continue
            if client_oid.startswith("tp-"):
                return True
            if order.side.lower() != expected_close_side:
                continue
            if not order.reduce_only and (order.trade_side or "").lower() != "close":
                continue
            if entry_price is None or order.trigger_price is None:
                continue
            trigger_price = float(order.trigger_price)
            if expected_close_side == "sell" and trigger_price > entry_price:
                return True
            if expected_close_side == "buy" and trigger_price < entry_price:
                return True
        return False

    def _place_tp_for_tracked_position(
        self,
        *,
        symbol: str,
        side_hint: str | None,
        total_size: float,
        thread_id: int,
        tp_points: list[float],
        parent_client_order_id: str | None,
    ) -> bool:
        if total_size <= 0 or not tp_points:
            return False

        side = "sell" if str(side_hint or "LONG").upper() == "LONG" else "buy"
        trade_side = "close" if self.config.bitget.position_mode == "hedge_mode" else None
        reduce_only = self.config.bitget.position_mode == "one_way_mode"
        hold_side = "long" if side == "sell" else "short"
        remaining_size = total_size
        placed = 0
        failed = 0
        last_reason: str | None = None

        for idx, tp in enumerate(tp_points):
            legs_left = len(tp_points) - idx
            requested_size = remaining_size if idx == len(tp_points) - 1 else (remaining_size / max(legs_left, 1))
            normalized_size, reject_reason = self._normalize_reduce_size(symbol, requested_size)
            if reject_reason or normalized_size <= 0:
                failed += 1
                last_reason = reject_reason or "invalid_tp_size"
                continue
            order_size = float(normalized_size)
            client_oid = f"tp-{thread_id}-{idx}-{int(utc_now().timestamp())}"
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
                failed += 1
                last_reason = str(exc)

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
                    "failed_count": failed,
                    "total_size": total_size,
                    "source": "tracked_position_autoprotect",
                },
            )
        if failed > 0:
            self.alerts.error(
                "TP_SUBMIT_FAILED",
                "failed to submit take-profit orders",
                {
                    "symbol": symbol,
                    "purpose": "tp",
                    "thread_id": thread_id,
                    "failed_count": failed,
                    "tp_total": len(tp_points),
                    "placed": placed,
                    "reason": last_reason,
                    "source": "tracked_position_autoprotect",
                },
            )
        return placed > 0 and failed == 0

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
