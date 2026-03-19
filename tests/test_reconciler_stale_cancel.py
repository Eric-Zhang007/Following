import asyncio
import logging
from datetime import timedelta

from trader.alerts import AlertManager
from trader.config import AppConfig
from trader.notifier import Notifier
from trader.order_reconciler import OrderReconciler
from trader.state import OrderState, StateStore, utc_now
from trader.store import SQLiteStore


class FakeBitgetStale:
    def __init__(self) -> None:
        self.canceled: list[tuple[str, str]] = []

    def get_order_state(self, symbol: str, order_id: str | None = None, client_order_id: str | None = None, is_plan_order: bool = False):  # noqa: ARG002
        created_ms = int((utc_now() - timedelta(days=4)).timestamp() * 1000)
        return {
            "symbol": symbol,
            "orderId": order_id,
            "clientOid": client_order_id,
            "state": "live",
            "baseVolume": "0",
            "cTime": str(created_ms),
        }

    def cancel_order(self, symbol: str, order_id: str):
        self.canceled.append((symbol, order_id))
        return {"ok": True}

    def supports_plan_orders(self):
        return True


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
                "symbol_policy": "ALLOW_ALL",
                "symbol_whitelist": [],
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
            "execution": {
                "cancel_unfilled_after_hours": 72,
            },
            "logging": {"level": "INFO", "file": "trader.log", "rich": False},
            "monitor": {"enabled": True},
        }
    )


def test_reconcile_stale_unfilled_entry_cancels_order(tmp_path) -> None:
    store = SQLiteStore(str(tmp_path / "stale_cancel.db"))
    alerts = AlertManager(Notifier(logging.getLogger("test")), store, logging.getLogger("test"))
    state = StateStore()
    state.upsert_order(
        OrderState(
            symbol="INXUSDT",
            side="buy",
            status="ACKED",
            filled=0.0,
            quantity=1000.0,
            avg_price=None,
            reduce_only=False,
            trade_side="open",
            purpose="entry",
            timestamp=utc_now(),
            client_order_id="entry-14-0-abcd",
            order_id="111122223333",
            thread_id=14,
        )
    )
    bitget = FakeBitgetStale()
    reconciler = OrderReconciler(_config(), bitget, state, store, alerts)
    asyncio.run(reconciler.reconcile_once())

    order = state.find_order(client_order_id="entry-14-0-abcd")
    assert order is not None
    assert order.status == "CANCELED"
    assert bitget.canceled == [("INXUSDT", "111122223333")]

    row = store.conn.execute(
        "SELECT id FROM reconciler_actions WHERE action='ORDER_STALE_CANCELED' ORDER BY id DESC LIMIT 1"
    ).fetchone()
    assert row is not None
