import asyncio
import logging

from trader.alerts import AlertManager
from trader.config import AppConfig
from trader.notifier import Notifier
from trader.order_reconciler import OrderReconciler
from trader.state import OrderState, StateStore, utc_now
from trader.store import SQLiteStore


class FakeBitgetOrderMissing:
    def get_order_state(self, symbol: str, order_id: str | None = None, client_order_id: str | None = None):  # noqa: ARG002
        raise RuntimeError(
            'Bitget request failed after retries: Bitget HTTP 400: {"code":"40109","msg":"The data of the order cannot be found"}'
        )

    def supports_plan_orders(self):
        return True


class FakeBitgetOrderMissingWithPosition(FakeBitgetOrderMissing):
    def get_position(self, symbol: str):  # noqa: ARG002
        return {"symbol": "ETHUSDT", "total": "1.2", "holdSide": "long"}


class FakeBitgetOrderMissingNoPosition(FakeBitgetOrderMissing):
    def get_position(self, symbol: str):  # noqa: ARG002
        return {"symbol": "ETHUSDT", "total": "0", "holdSide": "long"}


def _config() -> AppConfig:
    return AppConfig.model_validate(
        {
            "dry_run": False,
            "listener": {"mode": "web_preview"},
            "telegram": {"session_name": "s", "channel": "@IvanCryptotalk"},
            "bitget": {
                "base_url": "https://api.bitget.com",
                "api_key": "k",
                "api_secret": "s",
                "passphrase": "p",
                "product_type": "USDT-FUTURES",
                "position_mode": "one_way_mode",
            },
            "filters": {
                "symbol_policy": "ALLOWLIST",
                "symbol_whitelist": ["ETHUSDT"],
                "symbol_blacklist": [],
                "require_exchange_symbol": False,
                "min_usdt_volume_24h": None,
                "max_leverage": 10,
                "allow_sides": ["LONG", "SHORT"],
                "max_signal_age_seconds": 20,
                "leverage_over_limit_action": "CLAMP",
            },
            "risk": {
                "account_risk_per_trade": 0.003,
                "max_notional_per_trade": 200,
                "default_stop_loss_pct": 0.006,
                "assumed_equity_usdt": 1000,
            },
            "logging": {"level": "INFO", "file": "trader.log", "rich": False},
            "monitor": {"enabled": True},
        }
    )


def test_reconcile_missing_order_40109_with_position_treated_as_filled(tmp_path) -> None:
    store = SQLiteStore(str(tmp_path / "missing_order.db"))
    alerts = AlertManager(Notifier(logging.getLogger("test")), store, logging.getLogger("test"))
    state = StateStore()
    state.upsert_order(
        OrderState(
            symbol="ETHUSDT",
            side="buy",
            status="ACKED",
            filled=0.0,
            quantity=1.0,
            avg_price=None,
            reduce_only=False,
            trade_side=None,
            purpose="entry",
            timestamp=utc_now(),
            client_order_id="entry-eth-1",
            order_id="123",
        )
    )

    reconciler = OrderReconciler(_config(), FakeBitgetOrderMissingWithPosition(), state, store, alerts)
    asyncio.run(reconciler.reconcile_once())

    order = state.find_order(client_order_id="entry-eth-1")
    assert order is not None
    assert order.status == "FILLED"

    err = store.conn.execute(
        "SELECT id FROM events WHERE type='RECONCILE_ORDER_ERROR' ORDER BY id DESC LIMIT 1"
    ).fetchone()
    assert err is None


def test_reconcile_missing_order_40109_without_position_kept_acked(tmp_path) -> None:
    store = SQLiteStore(str(tmp_path / "missing_order_no_pos.db"))
    alerts = AlertManager(Notifier(logging.getLogger("test")), store, logging.getLogger("test"))
    state = StateStore()
    state.upsert_order(
        OrderState(
            symbol="ETHUSDT",
            side="buy",
            status="ACKED",
            filled=0.0,
            quantity=1.0,
            avg_price=None,
            reduce_only=False,
            trade_side=None,
            purpose="entry",
            timestamp=utc_now(),
            client_order_id="entry-eth-2",
            order_id="456",
        )
    )

    reconciler = OrderReconciler(_config(), FakeBitgetOrderMissingNoPosition(), state, store, alerts)
    asyncio.run(reconciler.reconcile_once())

    order = state.find_order(client_order_id="entry-eth-2")
    assert order is not None
    assert order.status == "ACKED"
    err = store.conn.execute(
        "SELECT id FROM events WHERE type='RECONCILE_ORDER_ERROR' ORDER BY id DESC LIMIT 1"
    ).fetchone()
    assert err is None
