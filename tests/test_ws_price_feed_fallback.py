import asyncio
import logging

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
                "stoploss": {
                    "sl_order_type": "local_guard",
                },
                "assumed_equity_usdt": 1000,
            },
            "monitor": {
                "enabled": True,
                "price_feed": {
                    "mode": "ws",
                    "interval_seconds": 1,
                    "ws_reconnect_seconds": 1,
                    "max_stale_seconds": 2,
                    "rest_fallback_action_when_local_guard": "safe_mode",
                },
            },
            "logging": {"level": "INFO", "file": "trader.log", "rich": False},
        }
    )


def test_ws_failure_falls_back_to_rest_and_enables_safe_mode(tmp_path) -> None:
    config = _config()
    store = SQLiteStore(str(tmp_path / "ws_fallback.db"))
    alerts = AlertManager(Notifier(logging.getLogger("test")), store, logging.getLogger("test"))
    state = StateStore()
    feed = PriceFeed(config=config, bitget=FakeBitget(), state=state, alerts=alerts)

    called = {"rest": 0}

    async def fake_ws_loop(stop_event):
        return False

    async def fake_rest_loop(stop_event):
        called["rest"] += 1
        stop_event.set()

    feed._run_ws_loop = fake_ws_loop  # type: ignore[method-assign]
    feed._run_rest_loop = fake_rest_loop  # type: ignore[method-assign]

    stop_event = asyncio.Event()
    asyncio.run(feed.run(stop_event))

    assert called["rest"] == 1
    assert state.safe_mode is True

    row = store.conn.execute(
        "SELECT type FROM events WHERE type='PRICE_FEED_LOCAL_GUARD_DEGRADED' ORDER BY id DESC LIMIT 1"
    ).fetchone()
    assert row is not None
