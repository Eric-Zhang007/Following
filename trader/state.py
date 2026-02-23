from __future__ import annotations

import threading
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any


@dataclass
class AccountState:
    equity: float
    available: float
    margin_used: float
    timestamp: datetime


@dataclass
class PositionState:
    symbol: str
    side: str
    size: float
    entry_price: float | None
    mark_price: float | None
    liq_price: float | None
    pnl: float | None
    leverage: int | None
    margin_mode: str | None
    timestamp: datetime
    unknown_origin: bool = False
    opened_at: datetime | None = None


@dataclass
class OrderState:
    symbol: str
    side: str
    status: str
    filled: float
    quantity: float | None
    avg_price: float | None
    reduce_only: bool
    trade_side: str | None
    purpose: str
    timestamp: datetime
    client_order_id: str | None = None
    order_id: str | None = None
    trigger_price: float | None = None
    is_plan_order: bool = False
    parent_client_order_id: str | None = None


@dataclass
class LocalGuardStop:
    symbol: str
    side: str
    trigger_price: float
    size: float
    reason: str
    created_at: datetime
    active: bool = True


@dataclass
class PriceSnapshot:
    symbol: str
    timestamp: datetime
    mark: float | None
    last: float | None
    bid: float | None
    ask: float | None


class StateStore:
    """Runtime state for monitor/reconciler/risk-daemon.

    Invariant: exchange truth has priority over local assumptions.
    """

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self.account: AccountState | None = None
        self.positions: dict[str, PositionState] = {}
        self.orders_by_client_id: dict[str, OrderState] = {}
        self.orders_by_exchange_id: dict[str, OrderState] = {}
        self.local_guard_stops: dict[str, LocalGuardStop] = {}
        self.prices: dict[str, PriceSnapshot] = {}
        self.price_feed_mode: str = "rest"
        self.price_feed_degraded: bool = False
        self.safe_mode: bool = False
        self.panic_mode: bool = False
        self.block_new_entries_reason: str | None = None
        self.last_account_ok_at: datetime | None = None
        self.last_positions_ok_at: datetime | None = None
        self.last_orders_ok_at: datetime | None = None
        self.last_price_ok_at: datetime | None = None
        self.last_reconciler_ok_at: datetime | None = None
        self.peak_equity: float | None = None
        self.api_error_timestamps: list[datetime] = []
        self.metrics: dict[str, float] = {
            "api_errors": 0.0,
            "sl_missing_count": 0.0,
            "circuit_breaker_state": 0.0,
            "open_positions": 0.0,
            "account_equity": 0.0,
            "ws_fresh": 0.0,
            "sl_coverage_ratio": 1.0,
        }

    def set_account(self, equity: float, available: float, margin_used: float, timestamp: datetime | None = None) -> None:
        with self._lock:
            now = timestamp or utc_now()
            self.account = AccountState(
                equity=float(equity),
                available=float(available),
                margin_used=float(margin_used),
                timestamp=now,
            )
            self.last_account_ok_at = now
            if self.peak_equity is None or equity > self.peak_equity:
                self.peak_equity = float(equity)
            self.metrics["account_equity"] = float(equity)

    def set_positions(self, positions: list[PositionState], timestamp: datetime | None = None) -> None:
        with self._lock:
            now = timestamp or utc_now()
            current = {p.symbol.upper(): p for p in positions}
            for p in current.values():
                if p.opened_at is None:
                    old = self.positions.get(p.symbol.upper())
                    p.opened_at = old.opened_at if old and old.opened_at else now
                p.timestamp = now
            self.positions = current
            self.last_positions_ok_at = now
            self.metrics["open_positions"] = float(len(self.positions))

    def upsert_order(self, order: OrderState) -> None:
        with self._lock:
            now = utc_now()
            order.timestamp = now
            if order.client_order_id:
                self.orders_by_client_id[order.client_order_id] = order
            if order.order_id:
                self.orders_by_exchange_id[order.order_id] = order
            self.last_orders_ok_at = now

    def find_order(self, client_order_id: str | None = None, order_id: str | None = None) -> OrderState | None:
        with self._lock:
            if client_order_id and client_order_id in self.orders_by_client_id:
                return self.orders_by_client_id[client_order_id]
            if order_id and order_id in self.orders_by_exchange_id:
                return self.orders_by_exchange_id[order_id]
            return None

    def pending_orders(self) -> list[OrderState]:
        with self._lock:
            return [
                order
                for order in self.orders_by_client_id.values()
                if order.status.upper() not in {"FILLED", "CANCELED", "REJECTED", "FAILED"}
            ]

    def clear_orders_for_symbol(self, symbol: str) -> None:
        with self._lock:
            key = symbol.upper()
            keep_client: dict[str, OrderState] = {}
            keep_exchange: dict[str, OrderState] = {}
            for client_id, order in self.orders_by_client_id.items():
                if order.symbol.upper() == key:
                    continue
                keep_client[client_id] = order
            for exchange_id, order in self.orders_by_exchange_id.items():
                if order.symbol.upper() == key:
                    continue
                keep_exchange[exchange_id] = order
            self.orders_by_client_id = keep_client
            self.orders_by_exchange_id = keep_exchange

    def known_entry_symbols(self) -> set[str]:
        with self._lock:
            return {
                order.symbol.upper()
                for order in self.orders_by_client_id.values()
                if order.purpose.lower() in {"entry", "entry_partial"} and order.status.upper() != "REJECTED"
            }

    def mark_order_status(
        self,
        *,
        status: str,
        filled: float | None = None,
        avg_price: float | None = None,
        client_order_id: str | None = None,
        order_id: str | None = None,
    ) -> None:
        with self._lock:
            order = self.find_order(client_order_id=client_order_id, order_id=order_id)
            if order is None:
                return
            order.status = status
            if filled is not None:
                order.filled = float(filled)
            if avg_price is not None:
                order.avg_price = float(avg_price)
            order.timestamp = utc_now()
            if order.order_id:
                self.orders_by_exchange_id[order.order_id] = order
            if order.client_order_id:
                self.orders_by_client_id[order.client_order_id] = order

    def has_valid_stop_loss(self, symbol: str, position_side: str) -> bool:
        expected_close_side = "sell" if position_side.lower() == "long" else "buy"
        with self._lock:
            guard = self.local_guard_stops.get(_guard_key(symbol, position_side))
            if guard and guard.active:
                return True
            for order in self.orders_by_client_id.values():
                if order.symbol.upper() != symbol.upper():
                    continue
                if order.purpose.lower() != "sl":
                    continue
                if order.status.upper() in {"CANCELED", "FAILED", "REJECTED"}:
                    continue
                if order.side.lower() != expected_close_side:
                    continue
                if not order.reduce_only and (order.trade_side or "").lower() != "close":
                    continue
                return True
        return False

    def get_stop_loss_order(self, symbol: str, position_side: str) -> OrderState | None:
        expected_close_side = "sell" if position_side.lower() == "long" else "buy"
        with self._lock:
            for order in self.orders_by_client_id.values():
                if order.symbol.upper() != symbol.upper():
                    continue
                if order.purpose.lower() != "sl":
                    continue
                if order.status.upper() in {"CANCELED", "FAILED", "REJECTED"}:
                    continue
                if order.side.lower() != expected_close_side:
                    continue
                return order
        return None

    def register_api_error(self, timestamp: datetime | None = None) -> None:
        with self._lock:
            now = timestamp or utc_now()
            self.api_error_timestamps.append(now)
            self.metrics["api_errors"] = self.metrics.get("api_errors", 0.0) + 1.0

    def api_errors_in_window(self, window_seconds: int, now: datetime | None = None) -> int:
        with self._lock:
            ref = now or utc_now()
            cutoff = ref.timestamp() - window_seconds
            kept = [t for t in self.api_error_timestamps if t.timestamp() >= cutoff]
            self.api_error_timestamps = kept
            return len(kept)

    def enable_safe_mode(self, reason: str) -> None:
        with self._lock:
            self.safe_mode = True
            self.block_new_entries_reason = reason
            self.metrics["circuit_breaker_state"] = 1.0

    def disable_safe_mode(self) -> None:
        with self._lock:
            self.safe_mode = False
            self.block_new_entries_reason = None
            if not self.panic_mode:
                self.metrics["circuit_breaker_state"] = 0.0

    def enable_panic_mode(self, reason: str) -> None:
        with self._lock:
            self.panic_mode = True
            self.safe_mode = True
            self.block_new_entries_reason = reason
            self.metrics["circuit_breaker_state"] = 2.0

    def set_price_fresh(self, timestamp: datetime | None = None) -> None:
        with self._lock:
            self.last_price_ok_at = timestamp or utc_now()

    def set_mark_price(self, symbol: str, mark_price: float, timestamp: datetime | None = None) -> None:
        with self._lock:
            key = symbol.upper()
            pos = self.positions.get(key)
            if pos is not None:
                pos.mark_price = float(mark_price)
                pos.timestamp = timestamp or utc_now()
            snap = self.prices.get(key)
            if snap is None:
                snap = PriceSnapshot(
                    symbol=key,
                    timestamp=timestamp or utc_now(),
                    mark=float(mark_price),
                    last=None,
                    bid=None,
                    ask=None,
                )
            else:
                snap.mark = float(mark_price)
                snap.timestamp = timestamp or utc_now()
            self.prices[key] = snap

    def set_price_snapshot(
        self,
        symbol: str,
        mark: float | None,
        last: float | None,
        bid: float | None,
        ask: float | None,
        timestamp: datetime | None = None,
    ) -> None:
        with self._lock:
            key = symbol.upper()
            self.prices[key] = PriceSnapshot(
                symbol=key,
                timestamp=timestamp or utc_now(),
                mark=mark,
                last=last,
                bid=bid,
                ask=ask,
            )
            if mark is not None:
                pos = self.positions.get(key)
                if pos is not None:
                    pos.mark_price = mark
                    pos.timestamp = timestamp or utc_now()

    def get_price(self, symbol: str) -> PriceSnapshot | None:
        with self._lock:
            return self.prices.get(symbol.upper())

    def set_price_feed_mode(self, mode: str, degraded: bool) -> None:
        with self._lock:
            self.price_feed_mode = mode
            self.price_feed_degraded = degraded
            self.metrics["ws_fresh"] = 0.0 if degraded or mode != "ws" else 1.0

    def register_local_guard_stop(self, guard: LocalGuardStop) -> None:
        with self._lock:
            self.local_guard_stops[_guard_key(guard.symbol, guard.side)] = guard

    def get_local_guard_stop(self, symbol: str, side: str) -> LocalGuardStop | None:
        with self._lock:
            return self.local_guard_stops.get(_guard_key(symbol, side))

    def deactivate_local_guard_stop(self, symbol: str, side: str) -> None:
        with self._lock:
            guard = self.local_guard_stops.get(_guard_key(symbol, side))
            if guard is not None:
                guard.active = False

    def active_local_guards(self) -> list[LocalGuardStop]:
        with self._lock:
            return [g for g in self.local_guard_stops.values() if g.active]

    def recompute_sl_coverage_metric(self) -> None:
        with self._lock:
            if not self.positions:
                self.metrics["sl_coverage_ratio"] = 1.0
                return
            covered = 0
            for p in self.positions.values():
                if self.has_valid_stop_loss(p.symbol, p.side):
                    covered += 1
            self.metrics["sl_coverage_ratio"] = covered / max(len(self.positions), 1)

    def set_reconciler_fresh(self, timestamp: datetime | None = None) -> None:
        with self._lock:
            self.last_reconciler_ok_at = timestamp or utc_now()

    def to_snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                "account": asdict(self.account) if self.account else None,
                "positions": {k: asdict(v) for k, v in self.positions.items()},
                "orders": {k: asdict(v) for k, v in self.orders_by_client_id.items()},
                "local_guards": {k: asdict(v) for k, v in self.local_guard_stops.items()},
                "prices": {k: asdict(v) for k, v in self.prices.items()},
                "price_feed_mode": self.price_feed_mode,
                "price_feed_degraded": self.price_feed_degraded,
                "safe_mode": self.safe_mode,
                "panic_mode": self.panic_mode,
                "block_new_entries_reason": self.block_new_entries_reason,
                "metrics": dict(self.metrics),
                "last_account_ok_at": self.last_account_ok_at.isoformat() if self.last_account_ok_at else None,
                "last_positions_ok_at": self.last_positions_ok_at.isoformat() if self.last_positions_ok_at else None,
                "last_orders_ok_at": self.last_orders_ok_at.isoformat() if self.last_orders_ok_at else None,
                "last_price_ok_at": self.last_price_ok_at.isoformat() if self.last_price_ok_at else None,
                "last_reconciler_ok_at": self.last_reconciler_ok_at.isoformat() if self.last_reconciler_ok_at else None,
            }


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _guard_key(symbol: str, side: str) -> str:
    return f"{symbol.upper()}::{side.lower()}"
