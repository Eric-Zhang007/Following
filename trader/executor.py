from __future__ import annotations

import logging
from decimal import Decimal, ROUND_DOWN

from trader.bitget_client import BitgetClient
from trader.config import AppConfig
from trader.models import EntrySignal, ManageAction, OrderIntent, RiskDecision
from trader.notifier import Notifier
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
    ) -> None:
        self.config = config
        self.bitget = bitget
        self.store = store
        self.notifier = notifier
        self.logger = logger
        self.symbol_registry = symbol_registry

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
            note=f"risk_notional={decision.notional:.4f}",
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
                intent=intent.to_dict(),
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
                intent=intent.to_dict(),
            )
            self.notifier.info(
                f"DRY_RUN ENTRY {signal.symbol} {signal.side.value} qty={size} price={price} tradeSide={trade_side}"
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
            )

            execution_id = self.store.record_execution(
                chat_id,
                message_id,
                version,
                action_type="ENTRY",
                symbol=signal.symbol,
                side=signal.side.value,
                status="EXECUTED",
                reason=None,
                intent=intent.to_dict(),
            )

            order_id = None
            if isinstance(receipt, dict):
                order_id = receipt.get("orderId") or receipt.get("clientOid")
            self.store.record_order_receipt(execution_id, str(order_id) if order_id else None, receipt)
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
                intent=intent.to_dict(),
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
            # MVP: only records intent and asks for manual confirmation because full stop-order
            # lifecycle (trigger + reduceOnly + cancellation of old stop) is exchange-strategy specific.
            self.store.record_execution(
                chat_id,
                message_id,
                version,
                action_type="MANAGE_MOVE_SL_BE",
                symbol=symbol,
                side=None,
                status="PENDING_MANUAL",
                reason="move_sl_to_be requires dedicated stop-order workflow",
                intent={"symbol": symbol, "move_sl_to_be": True},
            )
            self.notifier.warning(
                f"MANAGE move_sl_to_be for {symbol} recorded as PENDING_MANUAL (MVP behavior)"
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

    @staticmethod
    def _pick_position(position_payload: dict | list[dict]) -> dict:
        if isinstance(position_payload, list):
            return position_payload[0] if position_payload else {}
        if isinstance(position_payload, dict):
            if isinstance(position_payload.get("list"), list):
                return position_payload["list"][0] if position_payload["list"] else {}
            return position_payload
        return {}
