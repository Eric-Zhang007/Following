from __future__ import annotations

import asyncio
import math
import time
from dataclasses import asdict

from trader.alerts import AlertManager
from trader.bitget_client import BitgetClient
from trader.config import AppConfig
from trader.kill_switch import KillSwitch, KillSwitchAction
from trader.side_mapper import close_side_for_hold
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
        self._kill_switch_safe_active = False
        self._sl_missing_active: set[str] = set()
        self._protection_retry_after: dict[str, float] = {}
        self._tp_retry_after: dict[str, float] = {}
        self._no_sl_loss_alert_active: set[str] = set()
        self._no_sl_loss_alert_seq: dict[str, int] = {}

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

        no_sl_alert_seen_keys: set[str] = set()
        for position in list(self.state.positions.values()):
            await self._ensure_tracked_position_protection(position)
            await self._check_position_invariants(position)
            alert_key = self._check_no_sl_loss_alert(position)
            if alert_key is not None:
                no_sl_alert_seen_keys.add(alert_key)

        stale_keys = set(self._no_sl_loss_alert_seq.keys()) - no_sl_alert_seen_keys
        for key in stale_keys:
            self._no_sl_loss_alert_active.discard(key)
            self._no_sl_loss_alert_seq.pop(key, None)

        self.state.recompute_sl_coverage_metric()

    def _apply_kill_switch(self) -> None:
        action = self.kill_switch.read_action()
        if action == KillSwitchAction.NONE:
            if self._kill_switch_safe_active:
                self.alerts.info(
                    "KILL_SWITCH_RECOVERED",
                    "kill switch SAFE_MODE request cleared",
                    {"purpose": "risk_control", "reason": "manual_safe_mode_cleared"},
                )
                self._kill_switch_safe_active = False
            return
        if action == KillSwitchAction.SAFE_MODE:
            if not self._kill_switch_safe_active:
                self.alerts.critical(
                    "KILL_SWITCH",
                    "kill switch SAFE_MODE requested (alert-only; safe_mode disabled)",
                    {"purpose": "risk_control", "reason": "manual_safe_mode_alert_only"},
                )
                self._kill_switch_safe_active = True
            return
        self._kill_switch_safe_active = False

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

        thread = self.store.get_latest_trade_thread_by_symbol(position.symbol, active_only=True)
        if self._allow_no_stop_loss_for_thread(thread) and not self.state.has_valid_stop_loss(position.symbol, position.side):
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
        close_side = close_side_for_hold(position.side, self.config.bitget.position_mode)
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
        tp_key = f"{key}::tp"
        now_ts = time.time()
        if now_ts < float(self._protection_retry_after.get(key, 0.0)):
            return

        trace_id: str | None = None
        sl_ready = self.state.has_valid_stop_loss(position.symbol, position.side)
        if not sl_ready:
            if self._allow_no_stop_loss_for_thread(thread):
                pass
            else:
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
        thread_id = int(thread["thread_id"])
        tp_points = self._remaining_tp_points(thread_id)
        if not tp_points:
            return
        tp_guard_key = f"tp_submit_guard::{position.symbol.upper()}::{position.side.lower()}::{thread_id}"
        tp_progress_key = f"tp_progress::{position.symbol.upper()}::{thread_id}"
        last_tp_submit = self.store.get_system_flag(tp_guard_key)
        last_tp_progress = self.store.get_system_flag(tp_progress_key)
        last_tp_submit_ts = None
        last_tp_progress_ts = None
        try:
            last_tp_submit_ts = float(last_tp_submit) if last_tp_submit is not None else None
        except Exception:  # noqa: BLE001
            last_tp_submit_ts = None
        try:
            last_tp_progress_ts = float(last_tp_progress) if last_tp_progress is not None else None
        except Exception:  # noqa: BLE001
            last_tp_progress_ts = None
        bypass_submit_cooldown = (
            last_tp_progress_ts is not None
            and (last_tp_submit_ts is None or last_tp_progress_ts >= last_tp_submit_ts)
        )
        if last_tp_submit is not None:
            try:
                if not bypass_submit_cooldown and now_ts - float(last_tp_submit) < 120.0:
                    return
            except Exception:  # noqa: BLE001
                pass
        if not bypass_submit_cooldown and now_ts < float(self._tp_retry_after.get(tp_key, 0.0)):
            return
        if self._has_active_tp(position, thread_id, tp_points=tp_points):
            self.store.set_system_flag(tp_guard_key, str(now_ts))
            self._tp_retry_after[tp_key] = now_ts + 60.0
            return
        ok = self._place_tp_for_tracked_position(
            symbol=position.symbol,
            side_hint=thread.get("side"),
            total_size=position.size,
            thread_id=thread_id,
            tp_points=tp_points,
            parent_client_order_id=None,
        )
        if not ok:
            self._tp_retry_after[tp_key] = now_ts + 15.0
            self._protection_retry_after[key] = now_ts + 15.0
            return
        self.store.set_system_flag(tp_guard_key, str(now_ts))
        self._tp_retry_after[tp_key] = now_ts + 120.0
        self._protection_retry_after.pop(key, None)

    def _allow_no_stop_loss_for_thread(self, thread: dict | None) -> bool:
        if thread is None:
            return False
        if not self.config.risk.allow_entry_without_stop_loss:
            return False
        return thread.get("stop_loss") in {None, ""}

    def _check_no_sl_loss_alert(self, position: PositionState) -> str | None:
        thread = self.store.get_latest_trade_thread_by_symbol(position.symbol, active_only=True)
        if not self._allow_no_stop_loss_for_thread(thread):
            return None
        if self.state.has_valid_stop_loss(position.symbol, position.side):
            return None

        entry = float(position.entry_price or 0.0)
        mark = float(position.mark_price or 0.0)
        if entry <= 0 or mark <= 0:
            return None

        side = str(position.side or "").lower()
        if side == "short":
            loss_ratio = max((mark - entry) / entry, 0.0)
        else:
            loss_ratio = max((entry - mark) / entry, 0.0)

        threshold = float(self.config.risk.no_stop_loss_loss_alert_pct)
        thread_id = int(thread.get("thread_id")) if thread and thread.get("thread_id") is not None else 0
        key = f"{position.symbol.upper()}::{side}::{thread_id}"
        if loss_ratio >= threshold:
            if key not in self._no_sl_loss_alert_active:
                seq = int(self._no_sl_loss_alert_seq.get(key, 0)) + 1
                self._no_sl_loss_alert_seq[key] = seq
                self._no_sl_loss_alert_active.add(key)
                self.alerts.error(
                    "NO_SL_DRAWDOWN_20",
                    "position without stop-loss exceeded configured loss threshold",
                    {
                        "symbol": position.symbol,
                        "side": side,
                        "thread_id": thread_id,
                        "entry_price": entry,
                        "mark_price": mark,
                        "position_size": position.size,
                        "loss_pct": round(loss_ratio * 100.0, 4),
                        "threshold_pct": round(threshold * 100.0, 4),
                        "cross_seq": seq,
                        "purpose": "risk_control",
                        "reason": "no_stop_loss_drawdown_threshold",
                    },
                )
        elif key in self._no_sl_loss_alert_active:
            self._no_sl_loss_alert_active.discard(key)
        return key

    def _remaining_tp_points(self, thread_id: int) -> list[float]:
        for order in self.state.all_orders():
            if order.thread_id != thread_id:
                continue
            if order.purpose.lower() != "tp":
                continue
            if order.status.upper() != "FILLED":
                continue
            if order.trigger_price is None:
                continue
            self.store.mark_tp_point_filled(thread_id=thread_id, tp_price=float(order.trigger_price))
        return self.store.get_remaining_tp_points(thread_id)

    def _has_active_tp(self, position: PositionState, thread_id: int, *, tp_points: list[float] | None = None) -> bool:
        symbol = position.symbol
        expected_close_side = close_side_for_hold(position.side, self.config.bitget.position_mode)
        entry_price = float(position.entry_price) if position.entry_price not in {None, 0} else None
        remaining_tp_points = [float(v) for v in (tp_points if tp_points is not None else self._remaining_tp_points(thread_id))]
        for order in self.state.all_orders():
            if order.symbol.upper() != symbol.upper():
                continue
            if order.status.upper() in {"CANCELED", "FAILED", "REJECTED", "FILLED"}:
                continue
            is_close_order = bool(order.reduce_only) or (order.trade_side or "").lower() == "close"
            if not is_close_order:
                continue
            if order.side.lower() != expected_close_side:
                continue

            if order.thread_id == thread_id and order.purpose.lower() == "tp":
                if order.trigger_price is None:
                    return True
                trigger_price = float(order.trigger_price)
                if not remaining_tp_points:
                    continue
                if any(abs(trigger_price - p) <= max(1e-9, abs(p) * 1e-6) for p in remaining_tp_points):
                    return True
                continue

            purpose = (order.purpose or "").lower()
            client_oid = (order.client_order_id or "").lower()
            if purpose == "sl" or client_oid.startswith("sl-"):
                continue
            if client_oid.startswith("tp-"):
                if order.trigger_price is None:
                    return True
                trigger_price = float(order.trigger_price)
                if remaining_tp_points and any(abs(trigger_price - p) <= max(1e-9, abs(p) * 1e-6) for p in remaining_tp_points):
                    return True
                if remaining_tp_points:
                    continue
                return True
            if not order.is_plan_order:
                continue
            if entry_price is None or order.trigger_price is None:
                continue
            trigger_price = float(order.trigger_price)
            if remaining_tp_points and any(abs(trigger_price - p) <= max(1e-9, abs(p) * 1e-6) for p in remaining_tp_points):
                return True
            if remaining_tp_points:
                continue
            if position.side.lower() == "long" and trigger_price > entry_price:
                return True
            if position.side.lower() == "short" and trigger_price < entry_price:
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

        hold_side = "long" if str(side_hint or "LONG").upper() == "LONG" else "short"
        side = close_side_for_hold(hold_side, self.config.bitget.position_mode)
        trade_side = "close" if self.config.bitget.position_mode == "hedge_mode" else None
        reduce_only = self.config.bitget.position_mode == "one_way_mode"
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
