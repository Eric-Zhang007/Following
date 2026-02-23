from __future__ import annotations

import asyncio
from dataclasses import asdict

from trader.alerts import AlertManager
from trader.bitget_client import BitgetClient
from trader.config import AppConfig
from trader.kill_switch import KillSwitch, KillSwitchAction
from trader.state import PositionState, StateStore, utc_now
from trader.stoploss_manager import StopLossManager
from trader.store import SQLiteStore


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
        self._check_api_error_burst()
        self._check_drawdown_and_margin()

        # local_guard stop-loss processing is part of SL reliability guarantees.
        self.stoploss_manager.process_local_guards()

        for position in list(self.state.positions.values()):
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

    async def _check_position_invariants(self, position: PositionState) -> None:
        if self._is_liq_too_close(position):
            reduced = await self._reduce_position_once(position, reason="liquidation_distance_too_close")
            if not reduced:
                await self._protective_close(position, reason="liquidation_distance_too_close")
            return

        if not self.config.risk.stoploss.must_exist:
            return

        if self.state.has_valid_stop_loss(position.symbol, position.side):
            return

        self.state.metrics["sl_missing_count"] = self.state.metrics.get("sl_missing_count", 0.0) + 1.0
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
