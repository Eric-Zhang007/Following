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
    def protective_close_position(self, symbol: str, side: str, size: float):
        return {"ok": True}

    def place_order(self, **kwargs):
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
                "max_account_drawdown_pct": 0.15,
                "account_risk_per_trade": 0.003,
                "max_notional_per_trade": 200,
                "default_stop_loss_pct": 0.006,
                "assumed_equity_usdt": 1000,
            },
            "logging": {"level": "INFO", "file": "trader.log", "rich": False},
            "monitor": {"enabled": True},
        }
    )


def test_drawdown_breaker_enters_safe_mode_and_emits_event(tmp_path) -> None:
    store = SQLiteStore(str(tmp_path / "dd.db"))
    alerts = AlertManager(Notifier(logging.getLogger("test")), store, logging.getLogger("test"))
    state = StateStore()
    state.set_account(equity=1000, available=900, margin_used=100)
    state.set_account(equity=800, available=700, margin_used=100)

    daemon = RiskDaemon(
        config=_config(),
        bitget=FakeBitgetNoop(),
        state=state,
        store=store,
        alerts=alerts,
        kill_switch=KillSwitch(store=store, file_path=str(tmp_path / "NONE")),
    )

    asyncio.run(daemon.tick_once())

    assert state.safe_mode is True
    row = store.conn.execute("SELECT type FROM events WHERE type='DRAWDOWN_BREAKER' ORDER BY id DESC LIMIT 1").fetchone()
    assert row is not None
