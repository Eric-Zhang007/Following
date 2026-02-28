import logging

from trader.alerts import AlertManager
from trader.config import AppConfig
from trader.notifier import Notifier
from trader.order_reconciler import OrderReconciler
from trader.state import OrderState, StateStore, utc_now
from trader.store import SQLiteStore


class _FakeBitget:
    def get_order_state(self, *args, **kwargs):  # noqa: ANN002, ANN003
        return {}


def _config() -> AppConfig:
    return AppConfig.model_validate(
        {
            "dry_run": True,
            "listener": {"mode": "web_preview"},
            "telegram": {"session_name": "s", "channel": "@x"},
            "bitget": {
                "base_url": "https://api.bitget.com",
                "api_key": "",
                "api_secret": "",
                "passphrase": "",
                "product_type": "USDT-FUTURES",
            },
            "filters": {
                "symbol_whitelist": ["ETHUSDT"],
                "max_leverage": 100,
                "allow_sides": ["LONG", "SHORT"],
                "max_signal_age_seconds": 30,
                "leverage_over_limit_action": "CLAMP",
            },
            "risk": {
                "account_risk_per_trade": 0.003,
                "max_notional_per_trade": 100000,
                "entry_slippage_pct": 1,
                "cooldown_seconds": 0,
                "default_stop_loss_pct": 1,
                "assumed_equity_usdt": 1000,
            },
            "execution": {
                "be_reduce_on_two_entries": True,
                "be_reduce_pct": 50,
            },
            "logging": {"level": "INFO", "file": "trader.log", "rich": False},
        }
    )


def test_two_entries_filled_places_be_reduce_order(tmp_path) -> None:
    config = _config()
    store = SQLiteStore(str(tmp_path / "be.db"))
    state = StateStore()
    alerts = AlertManager(Notifier(logging.getLogger("test")), store, logging.getLogger("test"))
    reconciler = OrderReconciler(
        config=config,
        bitget=_FakeBitget(),  # type: ignore[arg-type]
        state=state,
        store=store,
        alerts=alerts,
    )

    thread_id = 77
    store.upsert_trade_thread(
        thread_id=thread_id,
        symbol="ETHUSDT",
        side="LONG",
        leverage=30,
        stop_loss=1800.0,
        entry_points=[2000.0, 1900.0],
        tp_points=[2100.0, 2200.0],
        status="ACTIVE",
    )
    o1 = OrderState(
        symbol="ETHUSDT",
        side="buy",
        status="FILLED",
        filled=1.0,
        quantity=1.0,
        avg_price=2000.0,
        reduce_only=False,
        trade_side="open",
        purpose="entry",
        timestamp=utc_now(),
        client_order_id="e1",
        order_id="e1",
        thread_id=thread_id,
        entry_index=0,
    )
    o2 = OrderState(
        symbol="ETHUSDT",
        side="buy",
        status="FILLED",
        filled=2.0,
        quantity=2.0,
        avg_price=1900.0,
        reduce_only=False,
        trade_side="open",
        purpose="entry",
        timestamp=utc_now(),
        client_order_id="e2",
        order_id="e2",
        thread_id=thread_id,
        entry_index=1,
    )
    state.upsert_order(o1)
    state.upsert_order(o2)

    import asyncio

    asyncio.run(reconciler._maybe_place_be_reduce(order=o2, trace="t1"))
    be_orders = [o for o in state.orders_by_client_id.values() if o.purpose == "be_reduce" and o.thread_id == thread_id]
    assert len(be_orders) == 1
    expected_avg = (1.0 * 2000.0 + 2.0 * 1900.0) / 3.0
    assert abs((be_orders[0].trigger_price or 0) - expected_avg) < 1e-8
