from __future__ import annotations

import asyncio
from dataclasses import asdict

from trader.alerts import AlertManager
from trader.bitget_client import BitgetClient
from trader.config import AppConfig
from trader.kill_switch import KillSwitch, KillSwitchAction
from trader.state import OrderState, PositionState, StateStore, utc_now
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
    ) -> None:
        self.config = config
        self.bitget = bitget
        self.state = state
        self.store = store
        self.alerts = alerts
        self.kill_switch = kill_switch

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

        for position in list(self.state.positions.values()):
            await self._check_position_invariants(position)

    def _apply_kill_switch(self) -> None:
        action = self.kill_switch.read_action()
        if action == KillSwitchAction.NONE:
            return
        if action == KillSwitchAction.SAFE_MODE:
            if not self.state.safe_mode:
                self.state.enable_safe_mode("kill switch SAFE_MODE")
                self.alerts.critical("KILL_SWITCH", "kill switch activated SAFE_MODE")
            return

        if not self.state.panic_mode:
            self.state.enable_panic_mode("kill switch PANIC_CLOSE")
            self.alerts.critical("KILL_SWITCH", "kill switch activated PANIC_CLOSE")

    def _check_api_error_burst(self) -> None:
        cb = self.config.risk.circuit_breaker
        count = self.state.api_errors_in_window(cb.api_error_window_seconds)
        if count >= cb.api_error_burst:
            self.state.enable_safe_mode("api error burst exceeded")
            self.alerts.error(
                "API_ERROR_BURST",
                "api errors exceeded burst threshold",
                {"count": count, "window_seconds": cb.api_error_window_seconds},
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
                        "margin_ratio": margin_ratio,
                        "max_total_margin_used_pct": self.config.risk.max_total_margin_used_pct,
                    },
                )

    async def _check_position_invariants(self, position: PositionState) -> None:
        if self._is_liq_too_close(position):
            await self._protective_close(position, reason="liquidation_distance_too_close")
            return

        if not self.config.risk.stoploss.must_exist:
            return

        has_sl = self.state.has_valid_stop_loss(position.symbol, position.side)
        if has_sl:
            return

        self.state.metrics["sl_missing_count"] = self.state.metrics.get("sl_missing_count", 0.0) + 1.0
        trace = self.alerts.warn(
            "SL_MISSING",
            "position without valid stop-loss detected",
            {"symbol": position.symbol, "side": position.side, "size": position.size},
        )
        self.store.record_invariant_violation(
            invariant_name="SL_MUST_EXIST",
            symbol=position.symbol,
            reason="missing protective stop-loss",
            payload=asdict(position),
            trace_id=trace,
        )

        placed = await self._try_place_stop_loss(position, trace_id=trace)
        if placed:
            return

        elapsed = 0.0
        if position.opened_at is not None:
            elapsed = (utc_now() - position.opened_at).total_seconds()

        if elapsed >= self.config.risk.stoploss.max_time_without_sl_seconds and self.config.risk.stoploss.emergency_close_if_sl_place_fails:
            await self._protective_close(position, reason="sl_place_failed_timeout")
            self.state.enable_safe_mode("SL placement failed")

    async def _try_place_stop_loss(self, position: PositionState, trace_id: str) -> bool:
        close_side = "sell" if position.side.lower() == "long" else "buy"
        reduce_only = self.config.bitget.position_mode == "one_way_mode"
        trade_side = "close" if self.config.bitget.position_mode == "hedge_mode" else None

        if self.config.dry_run:
            dummy = OrderState(
                symbol=position.symbol,
                side=close_side,
                status="SUBMITTED",
                filled=0.0,
                avg_price=None,
                reduce_only=reduce_only,
                trade_side=trade_side,
                purpose="sl",
                timestamp=utc_now(),
                client_order_id=f"dry-sl-{position.symbol}",
                order_id=f"dry-sl-{position.symbol}",
            )
            self.state.upsert_order(dummy)
            self.store.record_reconciler_action(
                symbol=position.symbol,
                order_id=dummy.order_id,
                client_order_id=dummy.client_order_id,
                action="SL_AUTOFIX_DRY_RUN",
                reason="dry_run",
                payload={"size": position.size},
                trace_id=trace_id,
            )
            return True

        try:
            receipt = await asyncio.to_thread(
                self.bitget.place_order,
                symbol=position.symbol,
                side=close_side,
                trade_side=trade_side,
                size=position.size,
                order_type="market",
                reduce_only=reduce_only,
                client_oid=f"sl-autofix-{int(utc_now().timestamp() * 1000)}",
            )
            sl_order = OrderState(
                symbol=position.symbol,
                side=close_side,
                status="SUBMITTED",
                filled=0.0,
                avg_price=None,
                reduce_only=reduce_only,
                trade_side=trade_side,
                purpose="sl",
                timestamp=utc_now(),
                client_order_id=str(receipt.get("clientOid") or "") or None,
                order_id=str(receipt.get("orderId") or "") or None,
            )
            self.state.upsert_order(sl_order)
            self.store.record_reconciler_action(
                symbol=position.symbol,
                order_id=sl_order.order_id,
                client_order_id=sl_order.client_order_id,
                action="SL_AUTOFIX_SUBMITTED",
                reason="missing SL autofix",
                payload={"size": position.size},
                trace_id=trace_id,
            )
            self.alerts.warn(
                "SL_AUTOFIX",
                "submitted stop-loss autofix",
                {"symbol": position.symbol, "size": position.size},
            )
            return True
        except Exception as exc:  # noqa: BLE001
            self.state.register_api_error()
            self.store.record_reconciler_action(
                symbol=position.symbol,
                order_id=None,
                client_order_id=None,
                action="SL_AUTOFIX_FAILED",
                reason=str(exc),
                payload={"size": position.size},
                trace_id=trace_id,
            )
            self.alerts.error(
                "SL_AUTOFIX_FAILED",
                "failed to place protective stop-loss",
                {"symbol": position.symbol, "error": str(exc)},
            )
            return False

    async def _protective_close(self, position: PositionState, reason: str) -> None:
        trace = self.alerts.critical(
            "PROTECTIVE_CLOSE",
            "triggering protective close",
            {"symbol": position.symbol, "reason": reason, "size": position.size},
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
                payload={"size": position.size},
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
                payload={"size": position.size},
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
                payload={"size": position.size, "origin_reason": reason},
                trace_id=trace,
            )
            self.alerts.error(
                "PROTECTIVE_CLOSE_FAILED",
                "failed to execute protective close",
                {"symbol": position.symbol, "error": str(exc)},
            )

    def _is_liq_too_close(self, position: PositionState) -> bool:
        if position.liq_price is None or position.mark_price is None or position.mark_price <= 0:
            return False
        distance = abs(position.liq_price - position.mark_price) / position.mark_price
        return distance <= self.config.risk.max_liquidation_distance_pct
