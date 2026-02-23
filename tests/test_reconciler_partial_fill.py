import asyncio
import logging

from trader.alerts import AlertManager
from trader.config import AppConfig
from trader.notifier import Notifier
from trader.order_reconciler import OrderReconciler
from trader.state import OrderState, StateStore, utc_now
from trader.store import SQLiteStore


class FakeBitgetPartial:
    def __init__(self) -> None:
        self.sl_orders = 0

    def get_order_state(self, symbol: str, order_id: str | None = None, client_order_id: str | None = None):
        return {
            "state": "PARTIAL",
            "baseVolume": "1.5",
            "priceAvg": "100.2",
            "orderId": order_id,
            "clientOid": client_order_id,
        }

    def place_order(self, **kwargs):
        self.sl_orders += 1
        return {"orderId": "sl-001", "clientOid": kwargs.get("client_oid", "sl-client")}


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
                "assumed_equity_usdt": 1000,
            },
            "logging": {"level": "INFO", "file": "trader.log", "rich": False},
            "monitor": {"enabled": True},
        }
    )


def test_reconciler_partial_fill_places_proportional_sl_and_records_reason(tmp_path) -> None:
    store = SQLiteStore(str(tmp_path / "reconcile.db"))
    alerts = AlertManager(Notifier(logging.getLogger("test")), store, logging.getLogger("test"))
    state = StateStore()
    state.upsert_order(
        OrderState(
            symbol="BTCUSDT",
            side="buy",
            status="NEW",
            filled=0.0,
            avg_price=None,
            reduce_only=False,
            trade_side=None,
            purpose="entry",
            timestamp=utc_now(),
            client_order_id="entry-abc",
            order_id="1001",
        )
    )

    bitget = FakeBitgetPartial()
    reconciler = OrderReconciler(_config(), bitget, state, store, alerts)
    asyncio.run(reconciler.reconcile_once())

    assert bitget.sl_orders == 1
    sl_order = state.find_order(order_id="sl-001")
    assert sl_order is not None
    assert sl_order.purpose == "sl"

    row = store.conn.execute(
        "SELECT action, reason FROM reconciler_actions WHERE action='PARTIAL_SL_SUBMITTED' ORDER BY id DESC LIMIT 1"
    ).fetchone()
    assert row is not None
    assert row["reason"] == "partial fill protection"
