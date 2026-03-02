import asyncio
import logging

from trader.alerts import AlertManager
from trader.config import AppConfig
from trader.kill_switch import KillSwitch
from trader.notifier import Notifier
from trader.risk_daemon import RiskDaemon
from trader.state import StateStore
from trader.store import SQLiteStore


class FakeBitgetNoop:
    def protective_close_position(self, symbol: str, side: str, size: float):  # noqa: ARG002
        return {"ok": True}

    def place_order(self, **kwargs):  # noqa: ANN003
        return {"ok": True}


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
                "circuit_breaker": {
                    "api_error_burst": 3,
                    "api_error_window_seconds": 120,
                },
            },
            "logging": {"level": "INFO", "file": "trader.log", "rich": False},
            "monitor": {"enabled": True},
        }
    )


def test_api_error_burst_emits_once_until_recovered(tmp_path) -> None:
    store = SQLiteStore(str(tmp_path / "api_burst.db"))
    alerts = AlertManager(Notifier(logging.getLogger("test")), store, logging.getLogger("test"))
    state = StateStore()
    for _ in range(4):
        state.register_api_error()

    daemon = RiskDaemon(
        config=_config(),
        bitget=FakeBitgetNoop(),
        state=state,
        store=store,
        alerts=alerts,
        kill_switch=KillSwitch(store=store, file_path=str(tmp_path / "NO_SWITCH")),
    )

    asyncio.run(daemon.tick_once())
    asyncio.run(daemon.tick_once())

    burst_rows = store.conn.execute("SELECT id FROM events WHERE type='API_ERROR_BURST' ORDER BY id ASC").fetchall()
    assert len(burst_rows) == 1

    state.api_error_timestamps = []
    asyncio.run(daemon.tick_once())
    recover = store.conn.execute(
        "SELECT id FROM events WHERE type='API_ERROR_BURST_RECOVERED' ORDER BY id DESC LIMIT 1"
    ).fetchone()
    assert recover is not None
