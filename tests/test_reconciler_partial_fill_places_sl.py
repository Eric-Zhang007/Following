import asyncio
import logging

from trader.alerts import AlertManager
from trader.config import AppConfig
from trader.models import OrderAck
from trader.notifier import Notifier
from trader.order_reconciler import OrderReconciler
from trader.state import OrderState, StateStore, utc_now
from trader.store import SQLiteStore


class FakeBitgetPartial:
    def __init__(self) -> None:
        self.stoploss_calls = 0
        self.last_stoploss_size = None

    def get_order_state(self, symbol: str, order_id: str | None = None, client_order_id: str | None = None):
        return {
            "state": "PARTIAL",
            "baseVolume": "0.75",
            "priceAvg": "100.0",
            "orderId": order_id,
            "clientOid": client_order_id,
        }

    def supports_plan_orders(self):
        return True

    def place_stop_loss(self, **kwargs):
        self.stoploss_calls += 1
        self.last_stoploss_size = float(kwargs.get("size") or 0)
        return OrderAck(order_id="sl-partial-1", client_oid=kwargs.get("client_oid"), status="ACKED", raw={})


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
            },
            "filters": {
                "symbol_policy": "ALLOWLIST",
                "symbol_whitelist": ["BTCUSDT"],
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
                "stoploss": {
                    "sl_order_type": "trigger",
                },
                "assumed_equity_usdt": 1000,
            },
            "logging": {"level": "INFO", "file": "trader.log", "rich": False},
            "monitor": {"enabled": True},
        }
    )


def test_partial_fill_places_stoploss_for_filled_size(tmp_path) -> None:
    store = SQLiteStore(str(tmp_path / "reconcile_partial.db"))
    alerts = AlertManager(Notifier(logging.getLogger("test")), store, logging.getLogger("test"))
    state = StateStore()
    state.upsert_order(
        OrderState(
            symbol="BTCUSDT",
            side="buy",
            status="NEW",
            filled=0.0,
            quantity=2.0,
            avg_price=None,
            reduce_only=False,
            trade_side=None,
            purpose="entry",
            timestamp=utc_now(),
            client_order_id="entry-100",
            order_id="100",
        )
    )

    bitget = FakeBitgetPartial()
    reconciler = OrderReconciler(_config(), bitget, state, store, alerts)

    asyncio.run(reconciler.reconcile_once())

    assert bitget.stoploss_calls == 1
    assert bitget.last_stoploss_size == 0.75

    sl_order = state.find_order(order_id="sl-partial-1")
    assert sl_order is not None
    assert sl_order.purpose == "sl"
    assert sl_order.quantity == 0.75

    row = store.conn.execute(
        "SELECT action FROM reconciler_actions WHERE action='PARTIAL_FILL_ENSURE_SL' ORDER BY id DESC LIMIT 1"
    ).fetchone()
    assert row is not None
