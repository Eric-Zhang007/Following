import json
import logging

import pytest

from trader.alerts import AlertManager
from trader.config import AppConfig
from trader.notifier import Notifier
from trader.price_feed import PriceFeed
from trader.state import StateStore
from trader.store import SQLiteStore


class FakeBitget:
    pass


def _config() -> AppConfig:
    return AppConfig.model_validate(
        {
            "dry_run": True,
            "listener": {"mode": "web_preview"},
            "telegram": {"session_name": "s", "channel": "@IvanCryptotalk"},
            "bitget": {
                "base_url": "https://api.bitget.com",
                "api_key": "",
                "api_secret": "",
                "passphrase": "",
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
                "assumed_equity_usdt": 1000,
            },
            "monitor": {
                "enabled": True,
                "price_feed": {
                    "mode": "ws",
                    "interval_seconds": 2,
                    "max_stale_seconds": 5,
                },
            },
            "logging": {"level": "INFO", "file": "trader.log", "rich": False},
        }
    )


@pytest.mark.parametrize(
    "item, expected_mark, expected_last, expected_bid, expected_ask",
    [
        ({"instId": "BTCUSDT", "markPrice": "100", "lastPr": "101", "bidPr": "99", "askPr": "102"}, 100.0, 101.0, 99.0, 102.0),
        ({"instId": "BTCUSDT", "markPr": "200", "last": "201", "bidPrice": "199", "askPrice": "202"}, 200.0, 201.0, 199.0, 202.0),
        ({"instId": "BTCUSDT", "last": "301"}, None, 301.0, None, None),
    ],
)
def test_ws_parsing_supports_multiple_field_names(
    tmp_path,
    item: dict,
    expected_mark: float | None,
    expected_last: float | None,
    expected_bid: float | None,
    expected_ask: float | None,
) -> None:
    store = SQLiteStore(str(tmp_path / "ws_parse.db"))
    alerts = AlertManager(Notifier(logging.getLogger("test")), store, logging.getLogger("test"))
    state = StateStore()
    feed = PriceFeed(_config(), FakeBitget(), state, alerts)

    valid = feed._process_ws_raw(json.dumps({"action": "snapshot", "data": [item]}))

    assert valid == 1
    snap = state.get_price("BTCUSDT")
    assert snap is not None
    assert snap.mark == expected_mark
    assert snap.last == expected_last
    assert snap.bid == expected_bid
    assert snap.ask == expected_ask
    assert state.last_price_ok_at is not None
    assert state.ws_parse_errors_total == 0


def test_ws_bad_packets_increase_parse_error_and_do_not_set_fresh(tmp_path) -> None:
    store = SQLiteStore(str(tmp_path / "ws_bad.db"))
    alerts = AlertManager(Notifier(logging.getLogger("test")), store, logging.getLogger("test"))
    state = StateStore()
    feed = PriceFeed(_config(), FakeBitget(), state, alerts)

    assert feed._process_ws_raw(json.dumps({"data": {"bad": "shape"}})) == 0
    assert feed._process_ws_raw(json.dumps({"data": [{"markPrice": "100"}]})) == 0
    assert feed._process_ws_raw(json.dumps({"data": [{"instId": "BTCUSDT", "bidPr": "99", "askPr": "101"}]})) == 0

    assert state.ws_parse_errors_total == 3
    assert state.last_price_ok_at is None


def test_ws_control_messages_are_ignored_without_parse_error(tmp_path) -> None:
    store = SQLiteStore(str(tmp_path / "ws_ctrl.db"))
    alerts = AlertManager(Notifier(logging.getLogger("test")), store, logging.getLogger("test"))
    state = StateStore()
    feed = PriceFeed(_config(), FakeBitget(), state, alerts)

    assert feed._process_ws_raw(json.dumps({"event": "subscribe", "arg": {"channel": "ticker"}})) == 0
    assert state.ws_messages_total == 1
    assert state.ws_parse_errors_total == 0
