import json
import logging

from trader.alerts import AlertManager
from trader.config import AppConfig
from trader.executor import TradeExecutor
from trader.models import OrderAck
from trader.notifier import Notifier
from trader.store import SQLiteStore


class _BitgetOk:
    def place_take_profit(self, **kwargs):
        return OrderAck(order_id="tp-1", client_oid=kwargs.get("client_oid"), status="ACKED", raw={})


class _BitgetFail:
    def place_take_profit(self, **kwargs):  # noqa: ARG002
        raise RuntimeError("tp submit failed")


def _config() -> AppConfig:
    return AppConfig.model_validate(
        {
            "dry_run": False,
            "listener": {"mode": "web_preview"},
            "telegram": {"session_name": "s", "channel": "@x"},
            "bitget": {
                "base_url": "https://api.bitget.com",
                "api_key": "",
                "api_secret": "",
                "passphrase": "",
                "product_type": "USDT-FUTURES",
                "position_mode": "one_way_mode",
            },
            "filters": {
                "symbol_whitelist": ["BTCUSDT"],
                "max_leverage": 20,
                "allow_sides": ["LONG", "SHORT"],
                "max_signal_age_seconds": 30,
                "leverage_over_limit_action": "CLAMP",
            },
            "risk": {
                "account_risk_per_trade": 0.003,
                "max_notional_per_trade": 100000,
                "default_stop_loss_pct": 1,
                "assumed_equity_usdt": 1000,
            },
            "logging": {"level": "INFO", "file": "trader.log", "rich": False},
        }
    )


def test_tp_submitted_event_contains_elapsed_ms(tmp_path) -> None:
    store = SQLiteStore(str(tmp_path / "tp_submit_ok.db"))
    alerts = AlertManager(Notifier(logging.getLogger("test")), store, logging.getLogger("test"))
    executor = TradeExecutor(
        config=_config(),
        bitget=_BitgetOk(),  # type: ignore[arg-type]
        store=store,
        notifier=Notifier(logging.getLogger("test")),
        logger=logging.getLogger("test"),
        alerts=alerts,
    )
    result = executor._place_take_profit_orders(
        symbol="BTCUSDT",
        side_hint="LONG",
        total_size=2.0,
        tp_list=[101000.0, 102000.0],
        parent_client_order_id="entry-1",
    )
    assert result["placed"] == 2
    event = store.conn.execute(
        "SELECT payload_json FROM events WHERE type='TP_SUBMITTED' ORDER BY id DESC LIMIT 1"
    ).fetchone()
    assert event is not None
    payload = json.loads(str(event["payload_json"]))
    assert payload["tp_count"] == 2
    assert payload.get("elapsed_ms") is not None


def test_tp_submit_failed_event_contains_elapsed_ms(tmp_path) -> None:
    store = SQLiteStore(str(tmp_path / "tp_submit_fail.db"))
    alerts = AlertManager(Notifier(logging.getLogger("test")), store, logging.getLogger("test"))
    executor = TradeExecutor(
        config=_config(),
        bitget=_BitgetFail(),  # type: ignore[arg-type]
        store=store,
        notifier=Notifier(logging.getLogger("test")),
        logger=logging.getLogger("test"),
        alerts=alerts,
    )
    result = executor._place_take_profit_orders(
        symbol="BTCUSDT",
        side_hint="LONG",
        total_size=1.0,
        tp_list=[101000.0],
        parent_client_order_id="entry-2",
    )
    assert result["placed"] == 0
    event = store.conn.execute(
        "SELECT payload_json FROM events WHERE type='TP_SUBMIT_FAILED' ORDER BY id DESC LIMIT 1"
    ).fetchone()
    assert event is not None
    payload = json.loads(str(event["payload_json"]))
    assert payload["failed_count"] == 1
    assert payload.get("elapsed_ms") is not None
