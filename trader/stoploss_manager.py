from __future__ import annotations

import uuid
from dataclasses import dataclass

from trader.alerts import AlertManager
from trader.bitget_client import BitgetClient
from trader.config import AppConfig
from trader.state import LocalGuardStop, OrderState, PositionState, StateStore, utc_now
from trader.store import SQLiteStore


@dataclass
class StopLossResult:
    ok: bool
    mode: str
    reason: str
    trace_id: str
    order_id: str | None = None
    client_order_id: str | None = None


class StopLossManager:
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

    def ensure_stop_loss(
        self,
        position_state: PositionState,
        desired_sl_price: float | None,
        desired_size: float | None,
        source: str,
        parent_client_order_id: str | None = None,
    ) -> StopLossResult:
        size = desired_size if desired_size is not None else position_state.size
        if size <= 0:
            trace = self.alerts.warn(
                "SL_AUTOFIX_SKIPPED",
                "skip stop loss placement due to non-positive size",
                {"symbol": position_state.symbol, "purpose": "sl", "reason": "size<=0"},
            )
            return StopLossResult(ok=False, mode="none", reason="size<=0", trace_id=trace)

        trace = self.alerts.info(
            "SL_AUTOFIX_ATTEMPT",
            "attempting stop loss ensure",
            {
                "symbol": position_state.symbol,
                "purpose": "sl",
                "reason": source,
                "desired_size": size,
                "desired_sl_price": desired_sl_price,
            },
        )

        existing = self.state.get_stop_loss_order(position_state.symbol, position_state.side)
        if existing is not None:
            ok, reason = self.validate_existing_sl(position_state, existing)
            if ok:
                if (
                    desired_sl_price is not None
                    and existing.trigger_price is not None
                    and abs(existing.trigger_price - desired_sl_price) > max(abs(desired_sl_price), 1.0) * 0.0001
                ):
                    self._cancel_existing_sl(existing, trace, "sl_trigger_price_mismatch")
                else:
                    return StopLossResult(
                        ok=True,
                        mode="existing",
                        reason="already_covered",
                        trace_id=trace,
                        order_id=existing.order_id,
                        client_order_id=existing.client_order_id,
                    )
            if not ok:
                self._cancel_existing_sl(existing, trace, reason)

        trigger_price = desired_sl_price if desired_sl_price is not None else self._default_sl_price(position_state)
        if trigger_price <= 0:
            return StopLossResult(ok=False, mode="none", reason="invalid_trigger_price", trace_id=trace)

        sl_mode = self.config.risk.stoploss.sl_order_type
        if sl_mode in {"trigger", "plan"} and self._supports_plan_orders():
            return self._place_exchange_trigger_sl(
                position_state=position_state,
                trigger_price=trigger_price,
                size=size,
                source=source,
                parent_client_order_id=parent_client_order_id,
                trace_id=trace,
            )

        return self._arm_local_guard(
            position_state=position_state,
            trigger_price=trigger_price,
            size=size,
            source=source,
            trace_id=trace,
        )

    def move_to_break_even(self, position_state: PositionState, buffer_pct: float) -> StopLossResult:
        if position_state.entry_price is None or position_state.entry_price <= 0:
            trace = self.alerts.warn(
                "SL_MOVE_BE_SKIPPED",
                "cannot move stop to break-even without entry price",
                {"symbol": position_state.symbol, "purpose": "sl", "reason": "entry_price_missing"},
            )
            return StopLossResult(ok=False, mode="none", reason="entry_price_missing", trace_id=trace)

        base = position_state.entry_price
        if position_state.side.lower() == "long":
            be_price = base * (1 + buffer_pct)
        else:
            be_price = base * (1 - buffer_pct)

        return self.ensure_stop_loss(
            position_state=position_state,
            desired_sl_price=be_price,
            desired_size=position_state.size,
            source="move_sl_to_be",
        )

    def validate_existing_sl(self, position_state: PositionState, sl_order_state: OrderState) -> tuple[bool, str]:
        expected_close_side = "sell" if position_state.side.lower() == "long" else "buy"
        if sl_order_state.side.lower() != expected_close_side:
            return False, f"sl_side_mismatch: expected {expected_close_side}"

        if not sl_order_state.reduce_only and (sl_order_state.trade_side or "").lower() != "close":
            return False, "sl_not_reduce_only_or_close"

        if sl_order_state.quantity is not None and position_state.size > 0:
            ratio = abs(sl_order_state.quantity - position_state.size) / position_state.size
            if ratio > 0.2:
                return False, f"sl_size_mismatch ratio={ratio:.4f}"

        if sl_order_state.trigger_price is not None and sl_order_state.trigger_price <= 0:
            return False, "invalid_trigger_price"

        return True, "ok"

    def process_local_guards(self) -> None:
        guards = self.state.active_local_guards()
        for guard in guards:
            snap = self.state.get_price(guard.symbol)
            if snap is None:
                continue
            px = snap.mark if snap.mark is not None else snap.last
            if px is None:
                continue

            trigger = False
            if guard.side.lower() == "long" and px <= guard.trigger_price:
                trigger = True
            if guard.side.lower() == "short" and px >= guard.trigger_price:
                trigger = True
            if not trigger:
                continue

            trace = self.alerts.critical(
                "LOCAL_GUARD_TRIGGERED",
                "local guard stop-loss triggered",
                {
                    "symbol": guard.symbol,
                    "purpose": "emergency_close",
                    "reason": guard.reason,
                    "trigger_price": guard.trigger_price,
                    "observed_price": px,
                },
            )

            if self.config.dry_run:
                self.store.record_reconciler_action(
                    symbol=guard.symbol,
                    order_id=None,
                    client_order_id=None,
                    action="LOCAL_GUARD_TRIGGER_DRY_RUN",
                    reason=guard.reason,
                    payload={"trigger_price": guard.trigger_price, "observed_price": px},
                    trace_id=trace,
                )
                self.state.deactivate_local_guard_stop(guard.symbol, guard.side)
                continue

            try:
                self.bitget.protective_close_position(guard.symbol, guard.side, guard.size)
                self.store.record_reconciler_action(
                    symbol=guard.symbol,
                    order_id=None,
                    client_order_id=None,
                    action="LOCAL_GUARD_TRIGGER_CLOSE",
                    reason=guard.reason,
                    payload={"trigger_price": guard.trigger_price, "observed_price": px, "size": guard.size},
                    trace_id=trace,
                )
                self.state.deactivate_local_guard_stop(guard.symbol, guard.side)
                self.state.enable_safe_mode("local guard triggered")
            except Exception as exc:  # noqa: BLE001
                self.state.register_api_error()
                self.store.record_reconciler_action(
                    symbol=guard.symbol,
                    order_id=None,
                    client_order_id=None,
                    action="LOCAL_GUARD_TRIGGER_FAILED",
                    reason=str(exc),
                    payload={"trigger_price": guard.trigger_price, "observed_price": px, "size": guard.size},
                    trace_id=trace,
                )
                self.alerts.error(
                    "LOCAL_GUARD_TRIGGER_FAILED",
                    "local guard close failed",
                    {"symbol": guard.symbol, "purpose": "emergency_close", "reason": str(exc)},
                )

    def _place_exchange_trigger_sl(
        self,
        *,
        position_state: PositionState,
        trigger_price: float,
        size: float,
        source: str,
        parent_client_order_id: str | None,
        trace_id: str,
    ) -> StopLossResult:
        close_side = "sell" if position_state.side.lower() == "long" else "buy"
        reduce_only = self.config.bitget.position_mode == "one_way_mode"
        trade_side = "close" if self.config.bitget.position_mode == "hedge_mode" else None
        hold_side = "long" if position_state.side.lower() == "long" else "short"
        client_oid = f"sl-{uuid.uuid4().hex[:16]}"

        if self.config.dry_run:
            sl_order = OrderState(
                symbol=position_state.symbol,
                side=close_side,
                status="ACKED",
                filled=0.0,
                quantity=size,
                avg_price=None,
                reduce_only=reduce_only,
                trade_side=trade_side,
                purpose="sl",
                timestamp=utc_now(),
                client_order_id=client_oid,
                order_id=f"dry-{client_oid}",
                trigger_price=trigger_price,
                is_plan_order=True,
                parent_client_order_id=parent_client_order_id,
            )
            self.state.upsert_order(sl_order)
            self.state.deactivate_local_guard_stop(position_state.symbol, position_state.side)
            self.store.record_reconciler_action(
                symbol=position_state.symbol,
                order_id=sl_order.order_id,
                client_order_id=sl_order.client_order_id,
                action="SL_TRIGGER_DRY_RUN",
                reason=source,
                payload={"trigger_price": trigger_price, "size": size},
                trace_id=trace_id,
            )
            return StopLossResult(
                ok=True,
                mode="trigger",
                reason="dry_run",
                trace_id=trace_id,
                order_id=sl_order.order_id,
                client_order_id=sl_order.client_order_id,
            )

        try:
            ack = self.bitget.place_stop_loss(
                symbol=position_state.symbol,
                product_type=self.config.bitget.product_type,
                margin_mode=self.config.bitget.margin_mode,
                position_mode=self.config.bitget.position_mode,
                hold_side=hold_side,
                trigger_price=trigger_price,
                order_price=None,
                size=size,
                side=close_side,
                trade_side=trade_side,
                reduce_only=reduce_only,
                client_oid=client_oid,
                trigger_type=self.config.risk.stoploss.trigger_price_type,
            )
            sl_order = OrderState(
                symbol=position_state.symbol,
                side=close_side,
                status=ack.status or "ACKED",
                filled=0.0,
                quantity=size,
                avg_price=None,
                reduce_only=reduce_only,
                trade_side=trade_side,
                purpose="sl",
                timestamp=utc_now(),
                client_order_id=ack.client_oid or client_oid,
                order_id=ack.order_id,
                trigger_price=trigger_price,
                is_plan_order=True,
                parent_client_order_id=parent_client_order_id,
            )
            self.state.upsert_order(sl_order)
            self.state.deactivate_local_guard_stop(position_state.symbol, position_state.side)
            self.store.record_reconciler_action(
                symbol=position_state.symbol,
                order_id=sl_order.order_id,
                client_order_id=sl_order.client_order_id,
                action="SL_TRIGGER_SUBMITTED",
                reason=source,
                payload={"trigger_price": trigger_price, "size": size, "purpose": "sl"},
                trace_id=trace_id,
            )
            self.alerts.warn(
                "SL_TRIGGER_SUBMITTED",
                "submitted exchange trigger stop-loss",
                {
                    "symbol": position_state.symbol,
                    "purpose": "sl",
                    "reason": source,
                    "trigger_price": trigger_price,
                    "size": size,
                },
            )
            return StopLossResult(
                ok=True,
                mode="trigger",
                reason="submitted",
                trace_id=trace_id,
                order_id=sl_order.order_id,
                client_order_id=sl_order.client_order_id,
            )
        except Exception as exc:  # noqa: BLE001
            self.state.register_api_error()
            self.store.record_reconciler_action(
                symbol=position_state.symbol,
                order_id=None,
                client_order_id=client_oid,
                action="SL_TRIGGER_FAILED",
                reason=str(exc),
                payload={"trigger_price": trigger_price, "size": size, "purpose": "sl"},
                trace_id=trace_id,
            )
            self.alerts.error(
                "SL_TRIGGER_FAILED",
                "exchange trigger stop-loss submit failed",
                {
                    "symbol": position_state.symbol,
                    "purpose": "sl",
                    "reason": str(exc),
                    "trigger_price": trigger_price,
                    "size": size,
                },
            )
            return StopLossResult(ok=False, mode="trigger", reason=str(exc), trace_id=trace_id)

    def _arm_local_guard(
        self,
        *,
        position_state: PositionState,
        trigger_price: float,
        size: float,
        source: str,
        trace_id: str,
    ) -> StopLossResult:
        guard = LocalGuardStop(
            symbol=position_state.symbol,
            side=position_state.side,
            trigger_price=trigger_price,
            size=size,
            reason=source,
            created_at=utc_now(),
            active=True,
        )
        self.state.register_local_guard_stop(guard)

        client_oid = f"local-guard-{uuid.uuid4().hex[:12]}"
        pseudo_order = OrderState(
            symbol=position_state.symbol,
            side="sell" if position_state.side.lower() == "long" else "buy",
            status="LOCAL_GUARD_ACTIVE",
            filled=0.0,
            quantity=size,
            avg_price=None,
            reduce_only=True,
            trade_side="close" if self.config.bitget.position_mode == "hedge_mode" else None,
            purpose="sl",
            timestamp=utc_now(),
            client_order_id=client_oid,
            order_id=None,
            trigger_price=trigger_price,
            is_plan_order=False,
        )
        self.state.upsert_order(pseudo_order)
        self.store.record_reconciler_action(
            symbol=position_state.symbol,
            order_id=None,
            client_order_id=client_oid,
            action="SL_LOCAL_GUARD_ARMED",
            reason=source,
            payload={"trigger_price": trigger_price, "size": size, "purpose": "sl"},
            trace_id=trace_id,
        )

        if self.state.price_feed_mode == "rest" and self.config.monitor.price_feed.rest_fallback_action_when_local_guard == "safe_mode":
            self.state.enable_safe_mode("local_guard with rest price feed")
            self.alerts.warn(
                "LOCAL_GUARD_REST_DEGRADED",
                "local guard armed while price feed in REST mode; safe_mode enabled",
                {"symbol": position_state.symbol, "purpose": "sl", "reason": source},
            )

        return StopLossResult(
            ok=True,
            mode="local_guard",
            reason="armed",
            trace_id=trace_id,
            client_order_id=client_oid,
        )

    def _cancel_existing_sl(self, existing: OrderState, trace_id: str, reason: str) -> None:
        if self.config.dry_run:
            self.state.mark_order_status(
                status="CANCELED",
                client_order_id=existing.client_order_id,
                order_id=existing.order_id,
            )
            self.store.record_reconciler_action(
                symbol=existing.symbol,
                order_id=existing.order_id,
                client_order_id=existing.client_order_id,
                action="SL_CANCEL_DRY_RUN",
                reason=reason,
                payload={"purpose": "sl"},
                trace_id=trace_id,
            )
            return

        if existing.is_plan_order and (existing.order_id or existing.client_order_id):
            try:
                self.bitget.cancel_plan_order(
                    symbol=existing.symbol,
                    order_id=existing.order_id,
                    client_oid=existing.client_order_id,
                )
                self.state.mark_order_status(
                    status="CANCELED",
                    client_order_id=existing.client_order_id,
                    order_id=existing.order_id,
                )
                self.store.record_reconciler_action(
                    symbol=existing.symbol,
                    order_id=existing.order_id,
                    client_order_id=existing.client_order_id,
                    action="SL_CANCELLED",
                    reason=reason,
                    payload={"purpose": "sl"},
                    trace_id=trace_id,
                )
            except Exception as exc:  # noqa: BLE001
                self.state.register_api_error()
                self.store.record_reconciler_action(
                    symbol=existing.symbol,
                    order_id=existing.order_id,
                    client_order_id=existing.client_order_id,
                    action="SL_CANCEL_FAILED",
                    reason=str(exc),
                    payload={"purpose": "sl"},
                    trace_id=trace_id,
                )

    def _default_sl_price(self, position: PositionState) -> float:
        base = position.entry_price or position.mark_price or 0.0
        ratio = self.config.risk.default_stop_loss_pct
        if ratio > 0.05:
            ratio = ratio / 100.0
        if position.side.lower() == "long":
            return base * (1 - ratio)
        return base * (1 + ratio)

    def _supports_plan_orders(self) -> bool:
        fn = getattr(self.bitget, "supports_plan_orders", None)
        if callable(fn):
            try:
                return bool(fn())
            except Exception:  # noqa: BLE001
                return False
        return False
