from __future__ import annotations

import logging
import time
import uuid
from decimal import Decimal, ROUND_DOWN

from trader.bitget_client import BitgetClient
from trader.config import AppConfig
from trader.models import EntrySignal, ManageAction, OrderIntent, RiskDecision
from trader.notifier import Notifier
from trader.state import OrderState, PositionState, StateStore, utc_now
from trader.stoploss_manager import StopLossManager
from trader.store import SQLiteStore
from trader.symbol_registry import SymbolRegistry


class TradeExecutor:
    def __init__(
        self,
        config: AppConfig,
        bitget: BitgetClient,
        store: SQLiteStore,
        notifier: Notifier,
        logger: logging.Logger,
        symbol_registry: SymbolRegistry | None = None,
        runtime_state: StateStore | None = None,
        stoploss_manager: StopLossManager | None = None,
    ) -> None:
        self.config = config
        self.bitget = bitget
        self.store = store
        self.notifier = notifier
        self.logger = logger
        self.symbol_registry = symbol_registry
        self.runtime_state = runtime_state
        self.stoploss_manager = stoploss_manager

    def execute_entry(
        self,
        signal: EntrySignal,
        decision: RiskDecision,
        chat_id: int,
        message_id: int,
        version: int,
    ) -> None:
        side = "buy" if signal.side.value == "LONG" else "sell"
        trade_side = "open" if self.config.bitget.position_mode == "hedge_mode" else None
        order_type = "market" if signal.entry_type.value == "MARKET" else "limit"
        client_order_id = f"entry-{uuid.uuid4().hex[:16]}"

        if self.config.risk.hard_stop_loss_required and not self.config.dry_run and not self._supports_exchange_stop_loss():
            reason = (
                "hard_stop_loss_required=true but no SL backend available "
                "(trigger unsupported and local_guard disabled)"
            )
            intent = {
                "entry": {
                    "symbol": signal.symbol,
                    "side": side,
                    "trade_side": trade_side,
                    "order_type": order_type,
                    "quantity": float(decision.quantity or 0),
                    "price": None if order_type == "market" else float(decision.entry_price or 0),
                },
                "stop_loss": {"required": True, "trigger_price": decision.stop_loss_price},
                "take_profit": [{"target_price": float(tp)} for tp in signal.take_profit],
            }
            self.store.record_execution(
                chat_id,
                message_id,
                version,
                action_type="ENTRY",
                symbol=signal.symbol,
                side=signal.side.value,
                status="REJECTED",
                reason=reason,
                intent=intent,
            )
            self.notifier.warning(f"ENTRY rejected: {reason}")
            return

        raw_size = float(decision.quantity)
        raw_price = None if order_type == "market" else float(decision.entry_price)
        size, price, reject_reason = self._normalize_order_params(signal.symbol, raw_size, raw_price)

        intent = OrderIntent(
            action_type="ENTRY",
            symbol=signal.symbol,
            side=side,
            trade_side=trade_side,
            order_type=order_type,
            quantity=size,
            price=price,
            reduce_only=False,
            source_chat_id=chat_id,
            source_message_id=message_id,
            source_version=version,
            client_order_id=client_order_id,
            purpose="entry",
            note=(
                f"risk_notional={float(decision.notional or 0):.4f};"
                f"stop_loss={decision.stop_loss_price};"
                f"warnings={','.join(decision.warnings)}"
            ),
        )
        bundle = self._build_entry_bundle(signal, decision, intent=intent.to_dict())

        self._register_runtime_order(
            symbol=signal.symbol,
            side=side,
            reduce_only=False,
            trade_side=trade_side,
            purpose="entry",
            client_order_id=client_order_id,
        )

        if reject_reason:
            self.store.record_execution(
                chat_id,
                message_id,
                version,
                action_type="ENTRY",
                symbol=signal.symbol,
                side=signal.side.value,
                status="REJECTED",
                reason=reject_reason,
                intent=bundle,
            )
            self.notifier.warning(f"ENTRY rejected: {reject_reason}")
            return

        if self.config.dry_run:
            self.store.record_execution(
                chat_id,
                message_id,
                version,
                action_type="ENTRY",
                symbol=signal.symbol,
                side=signal.side.value,
                status="DRY_RUN",
                reason="dry_run enabled",
                intent=bundle,
            )
            self.notifier.info(
                f"DRY_RUN ENTRY {signal.symbol} {signal.side.value} qty={size} "
                f"price={price} stop_loss={decision.stop_loss_price} tradeSide={trade_side}"
            )
            return

        try:
            if decision.leverage:
                hold_side = "long" if signal.side.value == "LONG" else "short"
                self.bitget.set_leverage(signal.symbol, decision.leverage, hold_side=hold_side)

            receipt = self.bitget.place_order(
                symbol=signal.symbol,
                side=side,
                trade_side=trade_side,
                size=size,
                order_type=order_type,
                price=price,
                reduce_only=False,
                client_oid=client_order_id,
            )
            exchange_order_id: str | None = None
            if isinstance(receipt, dict):
                exchange_order_id = str(receipt.get("orderId") or "") or None
                if self.runtime_state is not None and exchange_order_id:
                    self.runtime_state.mark_order_status(
                        status="SUBMITTED",
                        client_order_id=client_order_id,
                        order_id=exchange_order_id,
                    )

            if self.config.execution.require_order_ack:
                acked, ack_reason = self._wait_order_ack(
                    symbol=signal.symbol,
                    order_id=exchange_order_id,
                    client_order_id=client_order_id,
                )
                if not acked:
                    self.store.record_execution(
                        chat_id,
                        message_id,
                        version,
                        action_type="ENTRY",
                        symbol=signal.symbol,
                        side=signal.side.value,
                        status="FAILED",
                        reason=f"order ack timeout: {ack_reason}",
                        intent=bundle,
                    )
                    self.notifier.error(f"ENTRY FAILED {signal.symbol}: order ack timeout")
                    return

            execution_id = self.store.record_execution(
                chat_id,
                message_id,
                version,
                action_type="ENTRY",
                symbol=signal.symbol,
                side=signal.side.value,
                status="EXECUTED",
                reason=None,
                intent=bundle,
            )

            order_id = exchange_order_id or client_order_id
            self.store.record_order_receipt(execution_id, str(order_id) if order_id else None, receipt)
            self._ensure_entry_protection(
                signal=signal,
                decision=decision,
                executed_qty=size,
                parent_client_order_id=client_order_id,
            )
            self.notifier.info(
                f"EXECUTED ENTRY {signal.symbol} {signal.side.value} qty={size} order_id={order_id}"
            )
        except Exception as exc:  # noqa: BLE001
            self.logger.exception("execute_entry failed")
            self.store.record_execution(
                chat_id,
                message_id,
                version,
                action_type="ENTRY",
                symbol=signal.symbol,
                side=signal.side.value,
                status="FAILED",
                reason=str(exc),
                intent=bundle,
            )
            if self.runtime_state is not None:
                self.runtime_state.mark_order_status(
                    status="FAILED",
                    client_order_id=client_order_id,
                )
            self.notifier.error(f"ENTRY FAILED {signal.symbol}: {exc}")

    def execute_manage(
        self,
        action: ManageAction,
        chat_id: int,
        message_id: int,
        version: int,
    ) -> None:
        symbol = action.symbol
        if symbol is None:
            self.store.record_execution(
                chat_id,
                message_id,
                version,
                action_type="MANAGE",
                symbol=None,
                side=None,
                status="REJECTED",
                reason="symbol unresolved",
                intent=None,
            )
            self.notifier.warning("MANAGE rejected: symbol unresolved")
            return

        if self.config.dry_run:
            intent = OrderIntent(
                action_type="MANAGE",
                symbol=symbol,
                side="reduce_or_update",
                trade_side="close" if self.config.bitget.position_mode == "hedge_mode" else None,
                order_type="market",
                quantity=0.0,
                price=None,
                reduce_only=self.config.bitget.position_mode == "one_way_mode",
                source_chat_id=chat_id,
                source_message_id=message_id,
                source_version=version,
                note=action.note,
            )
            self.store.record_execution(
                chat_id,
                message_id,
                version,
                action_type="MANAGE",
                symbol=symbol,
                side=None,
                status="DRY_RUN",
                reason="dry_run enabled",
                intent=intent.to_dict(),
            )
            self.notifier.info(f"DRY_RUN MANAGE symbol={symbol} reduce={action.reduce_pct} be={action.move_sl_to_be}")
            return

        if action.reduce_pct is not None:
            try:
                position_payload = self.bitget.get_position(symbol)
                position = self._pick_position(position_payload)
                position_size = abs(float(position.get("total", position.get("size", 0))))
                hold_side = self._extract_hold_side(position)

                if position_size <= 0:
                    self.store.record_execution(
                        chat_id,
                        message_id,
                        version,
                        action_type="MANAGE",
                        symbol=symbol,
                        side=None,
                        status="REJECTED",
                        reason="no position to reduce",
                        intent=None,
                    )
                    self.notifier.warning(f"MANAGE reduce rejected: no position for {symbol}")
                    return

                close_qty_raw = position_size * (action.reduce_pct / 100.0)
                side = "sell" if hold_side == "long" else "buy"
                trade_side = "close" if self.config.bitget.position_mode == "hedge_mode" else None
                reduce_only = self.config.bitget.position_mode == "one_way_mode"

                close_qty, _, reject_reason = self._normalize_order_params(symbol, close_qty_raw, None)
                intent = OrderIntent(
                    action_type="MANAGE_REDUCE",
                    symbol=symbol,
                    side=side,
                    trade_side=trade_side,
                    order_type="market",
                    quantity=close_qty,
                    price=None,
                    reduce_only=reduce_only,
                    source_chat_id=chat_id,
                    source_message_id=message_id,
                    source_version=version,
                    note=f"reduce_pct={action.reduce_pct}",
                )

                if reject_reason:
                    self.store.record_execution(
                        chat_id,
                        message_id,
                        version,
                        action_type="MANAGE_REDUCE",
                        symbol=symbol,
                        side=side,
                        status="REJECTED",
                        reason=reject_reason,
                        intent=intent.to_dict(),
                    )
                    self.notifier.warning(f"MANAGE reduce rejected: {reject_reason}")
                    return

                receipt = self.bitget.place_order(
                    symbol=symbol,
                    side=side,
                    trade_side=trade_side,
                    size=close_qty,
                    order_type="market",
                    reduce_only=reduce_only,
                )
                execution_id = self.store.record_execution(
                    chat_id,
                    message_id,
                    version,
                    action_type="MANAGE_REDUCE",
                    symbol=symbol,
                    side=side,
                    status="EXECUTED",
                    reason=None,
                    intent=intent.to_dict(),
                )
                order_id = None
                if isinstance(receipt, dict):
                    order_id = receipt.get("orderId") or receipt.get("clientOid")
                self.store.record_order_receipt(execution_id, str(order_id) if order_id else None, receipt)
                self.notifier.info(
                    f"EXECUTED MANAGE reduce {symbol} qty={close_qty} reduce_pct={action.reduce_pct}"
                )
            except Exception as exc:  # noqa: BLE001
                self.logger.exception("execute_manage reduce failed")
                self.store.record_execution(
                    chat_id,
                    message_id,
                    version,
                    action_type="MANAGE_REDUCE",
                    symbol=symbol,
                    side=None,
                    status="FAILED",
                    reason=str(exc),
                    intent=None,
                )
                self.notifier.error(f"MANAGE reduce failed {symbol}: {exc}")

        if action.move_sl_to_be:
            try:
                position_payload = self.bitget.get_position(symbol)
                position = self._pick_position(position_payload)
                position_size = abs(float(position.get("total", position.get("size", 0)) or 0.0))
                if position_size <= 0:
                    self.store.record_execution(
                        chat_id,
                        message_id,
                        version,
                        action_type="MANAGE_MOVE_SL_BE",
                        symbol=symbol,
                        side=None,
                        status="REJECTED",
                        reason="no position for move_sl_to_be",
                        intent={"symbol": symbol, "move_sl_to_be": True},
                    )
                    self.notifier.warning(f"MANAGE move_sl_to_be rejected: no position for {symbol}")
                elif self.stoploss_manager is None:
                    self.store.record_execution(
                        chat_id,
                        message_id,
                        version,
                        action_type="MANAGE_MOVE_SL_BE",
                        symbol=symbol,
                        side=None,
                        status="REJECTED",
                        reason="stoploss_manager unavailable",
                        intent={"symbol": symbol, "move_sl_to_be": True},
                    )
                    self.notifier.warning("MANAGE move_sl_to_be rejected: stoploss_manager unavailable")
                else:
                    ps = PositionState(
                        symbol=symbol,
                        side=self._extract_hold_side(position),
                        size=position_size,
                        entry_price=self._to_float(position, ["openPriceAvg", "entryPrice", "openPrice"]),
                        mark_price=self._to_float(position, ["markPrice", "mark", "lastPr"]),
                        liq_price=self._to_float(position, ["liquidationPrice", "liqPx"]),
                        pnl=self._to_float(position, ["unrealizedPL", "upl"]),
                        leverage=self._to_int(position, ["leverage"]),
                        margin_mode=str(position.get("marginMode") or self.config.bitget.margin_mode),
                        timestamp=utc_now(),
                        opened_at=utc_now(),
                    )
                    result = self.stoploss_manager.move_to_break_even(
                        position_state=ps,
                        buffer_pct=self.config.risk.stoploss.break_even_buffer_pct,
                    )
                    status = "EXECUTED" if result.ok else "FAILED"
                    self.store.record_execution(
                        chat_id,
                        message_id,
                        version,
                        action_type="MANAGE_MOVE_SL_BE",
                        symbol=symbol,
                        side=None,
                        status=status,
                        reason=result.reason,
                        intent={
                            "symbol": symbol,
                            "move_sl_to_be": True,
                            "trace_id": result.trace_id,
                            "mode": result.mode,
                        },
                    )
                    if result.ok:
                        self.notifier.info(f"EXECUTED MANAGE move_sl_to_be for {symbol}")
                    else:
                        self.notifier.warning(f"MANAGE move_sl_to_be failed for {symbol}: {result.reason}")
            except Exception as exc:  # noqa: BLE001
                self.logger.exception("execute_manage move_sl_to_be failed")
                self.store.record_execution(
                    chat_id,
                    message_id,
                    version,
                    action_type="MANAGE_MOVE_SL_BE",
                    symbol=symbol,
                    side=None,
                    status="FAILED",
                    reason=str(exc),
                    intent={"symbol": symbol, "move_sl_to_be": True},
                )
                self.notifier.error(f"MANAGE move_sl_to_be failed {symbol}: {exc}")

        if action.tp_price is not None:
            self._place_take_profit_orders(
                symbol=symbol,
                side_hint=None,
                total_size=None,
                tp_list=[float(action.tp_price)],
                parent_client_order_id=None,
            )

    def _normalize_order_params(
        self,
        symbol: str,
        quantity: float,
        price: float | None,
    ) -> tuple[float, float | None, str | None]:
        if quantity <= 0:
            return 0.0, price, "quantity <= 0"

        if self.symbol_registry is None:
            rounded_qty = float(f"{quantity:.6f}")
            rounded_price = float(f"{price:.8f}") if price is not None else None
            return rounded_qty, rounded_price, None

        contract = self.symbol_registry.get_contract(symbol)
        if contract is None:
            return 0.0, price, f"contract config unavailable for symbol: {symbol}"

        rounded_qty = self._round_down(quantity, contract.size_place)
        if rounded_qty <= 0:
            return rounded_qty, price, f"quantity <= 0 after sizePlace rounding ({contract.size_place})"

        if contract.min_trade_num > 0 and rounded_qty < contract.min_trade_num:
            return (
                rounded_qty,
                price,
                f"quantity {rounded_qty} below minTradeNum {contract.min_trade_num} for {symbol}",
            )

        rounded_price = None
        if price is not None:
            if price <= 0:
                return rounded_qty, price, "price <= 0"
            rounded_price = self._round_down(price, contract.price_place)
            if rounded_price <= 0:
                return rounded_qty, rounded_price, f"price <= 0 after pricePlace rounding ({contract.price_place})"

        return rounded_qty, rounded_price, None

    def _register_runtime_order(
        self,
        *,
        symbol: str,
        side: str,
        reduce_only: bool,
        trade_side: str | None,
        purpose: str,
        client_order_id: str | None,
    ) -> None:
        if self.runtime_state is None:
            return
        self.runtime_state.upsert_order(
            OrderState(
                symbol=symbol,
                side=side,
                status="SUBMITTING",
                filled=0.0,
                quantity=None,
                avg_price=None,
                reduce_only=reduce_only,
                trade_side=trade_side,
                purpose=purpose,
                timestamp=utc_now(),
                client_order_id=client_order_id,
                order_id=None,
                trigger_price=None,
                is_plan_order=False,
            )
        )

    def _wait_order_ack(self, symbol: str, order_id: str | None, client_order_id: str | None) -> tuple[bool, str]:
        deadline = time.time() + self.config.execution.ack_timeout_seconds
        last_error = ""
        while time.time() < deadline:
            try:
                if self.config.dry_run:
                    return True, "dry_run"
                payload = self.bitget.get_order_state(symbol, order_id=order_id, client_order_id=client_order_id)
                if payload:
                    if self.runtime_state is not None:
                        self.runtime_state.mark_order_status(
                            status=str(payload.get("state", payload.get("status", "SUBMITTED"))),
                            filled=float(payload.get("baseVolume", payload.get("filledQty", 0.0)) or 0.0),
                            avg_price=(
                                float(payload.get("priceAvg", payload.get("avgPrice")))
                                if payload.get("priceAvg", payload.get("avgPrice")) not in {None, ""}
                                else None
                            ),
                            client_order_id=client_order_id,
                            order_id=order_id,
                        )
                    return True, "ok"
            except Exception as exc:  # noqa: BLE001
                last_error = str(exc)
            time.sleep(0.5)
        return False, last_error or "ack_timeout"

    def _build_entry_bundle(self, signal: EntrySignal, decision: RiskDecision, intent: dict) -> dict:
        stop_loss = {
            "symbol": signal.symbol,
            "trigger_price": decision.stop_loss_price,
            "order_type": "market",
            "reduce_only": True,
            "trade_side": "close" if self.config.bitget.position_mode == "hedge_mode" else None,
            "required": True,
        }
        take_profit = [
            {
                "symbol": signal.symbol,
                "target_price": float(tp),
                "reduce_only": True,
            }
            for tp in signal.take_profit
        ]
        return {
            "entry": intent,
            "stop_loss": stop_loss,
            "take_profit": take_profit,
        }

    def _supports_exchange_stop_loss(self) -> bool:
        if self.stoploss_manager is None:
            return False
        if self.config.risk.stoploss.sl_order_type not in {"trigger", "plan", "local_guard"}:
            return False
        if self.config.risk.stoploss.sl_order_type in {"trigger", "plan"}:
            supports = getattr(self.bitget, "supports_plan_orders", None)
            if callable(supports):
                try:
                    return bool(supports())
                except Exception:  # noqa: BLE001
                    return False
            return False
        return self.config.risk.stoploss.sl_order_type == "local_guard"

    @staticmethod
    def _round_down(value: float, decimals: int) -> float:
        q = Decimal(1).scaleb(-max(decimals, 0))
        return float(Decimal(str(value)).quantize(q, rounding=ROUND_DOWN))

    @staticmethod
    def _extract_hold_side(position: dict) -> str:
        hold_side = str(position.get("holdSide", "")).lower()
        if hold_side in {"long", "short"}:
            return hold_side

        size = float(position.get("total", position.get("size", 0)) or 0)
        if size >= 0:
            return "long"
        return "short"

    def _ensure_entry_protection(
        self,
        *,
        signal: EntrySignal,
        decision: RiskDecision,
        executed_qty: float,
        parent_client_order_id: str | None,
    ) -> None:
        if self.stoploss_manager is None:
            self.notifier.warning("ENTRY protection skipped: stoploss_manager unavailable")
            return
        if executed_qty <= 0:
            return

        position = PositionState(
            symbol=signal.symbol,
            side="long" if signal.side.value == "LONG" else "short",
            size=executed_qty,
            entry_price=decision.entry_price,
            mark_price=decision.entry_price,
            liq_price=None,
            pnl=None,
            leverage=decision.leverage,
            margin_mode=self.config.bitget.margin_mode,
            timestamp=utc_now(),
            opened_at=utc_now(),
        )
        result = self.stoploss_manager.ensure_stop_loss(
            position_state=position,
            desired_sl_price=decision.stop_loss_price,
            desired_size=executed_qty,
            source="entry_fill",
            parent_client_order_id=parent_client_order_id,
        )
        if not result.ok:
            self.notifier.warning(f"ENTRY stop-loss ensure failed: {result.reason}")
        self._place_take_profit_orders(
            symbol=signal.symbol,
            side_hint=signal.side.value,
            total_size=executed_qty,
            tp_list=signal.take_profit,
            parent_client_order_id=parent_client_order_id,
        )

    def _place_take_profit_orders(
        self,
        *,
        symbol: str,
        side_hint: str | None,
        total_size: float | None,
        tp_list: list[float],
        parent_client_order_id: str | None,
    ) -> None:
        if not tp_list:
            return
        side = "sell" if (side_hint or "LONG").upper() == "LONG" else "buy"
        size_each = None
        if total_size is not None and total_size > 0:
            size_each = total_size / len(tp_list)
        for tp in tp_list:
            try:
                client_oid = f"tp-{uuid.uuid4().hex[:16]}"
                if self.config.dry_run:
                    if self.runtime_state is not None:
                        self.runtime_state.upsert_order(
                            OrderState(
                                symbol=symbol,
                                side=side,
                                status="ACKED",
                                filled=0.0,
                                quantity=size_each,
                                avg_price=None,
                                reduce_only=self.config.bitget.position_mode == "one_way_mode",
                                trade_side="close" if self.config.bitget.position_mode == "hedge_mode" else None,
                                purpose="tp",
                                timestamp=utc_now(),
                                client_order_id=client_oid,
                                order_id=f"dry-{client_oid}",
                                trigger_price=float(tp),
                                is_plan_order=True,
                                parent_client_order_id=parent_client_order_id,
                            )
                        )
                    continue
                ack = self.bitget.place_take_profit(
                    symbol=symbol,
                    product_type=self.config.bitget.product_type,
                    margin_mode=self.config.bitget.margin_mode,
                    position_mode=self.config.bitget.position_mode,
                    hold_side="long" if side == "sell" else "short",
                    trigger_price=float(tp),
                    order_price=None,
                    size=float(size_each or 0.0),
                    side=side,
                    trade_side="close" if self.config.bitget.position_mode == "hedge_mode" else None,
                    reduce_only=self.config.bitget.position_mode == "one_way_mode",
                    client_oid=client_oid,
                    trigger_type=self.config.risk.stoploss.trigger_price_type,
                )
                if self.runtime_state is not None:
                    self.runtime_state.upsert_order(
                        OrderState(
                            symbol=symbol,
                            side=side,
                            status=ack.status or "ACKED",
                            filled=0.0,
                            quantity=size_each,
                            avg_price=None,
                            reduce_only=self.config.bitget.position_mode == "one_way_mode",
                            trade_side="close" if self.config.bitget.position_mode == "hedge_mode" else None,
                            purpose="tp",
                            timestamp=utc_now(),
                            client_order_id=ack.client_oid or client_oid,
                            order_id=ack.order_id,
                            trigger_price=float(tp),
                            is_plan_order=True,
                            parent_client_order_id=parent_client_order_id,
                        )
                    )
            except Exception as exc:  # noqa: BLE001
                self.logger.warning("place take profit failed symbol=%s tp=%s err=%s", symbol, tp, exc)

    @staticmethod
    def _to_float(payload: dict, keys: list[str]) -> float | None:
        for key in keys:
            if key in payload and payload[key] not in {None, ""}:
                try:
                    return float(payload[key])
                except Exception:  # noqa: BLE001
                    continue
        return None

    @staticmethod
    def _to_int(payload: dict, keys: list[str]) -> int | None:
        for key in keys:
            if key in payload and payload[key] not in {None, ""}:
                try:
                    return int(float(payload[key]))
                except Exception:  # noqa: BLE001
                    continue
        return None

    @staticmethod
    def _pick_position(position_payload: dict | list[dict]) -> dict:
        if isinstance(position_payload, list):
            return position_payload[0] if position_payload else {}
        if isinstance(position_payload, dict):
            if isinstance(position_payload.get("list"), list):
                return position_payload["list"][0] if position_payload["list"] else {}
            return position_payload
        return {}
