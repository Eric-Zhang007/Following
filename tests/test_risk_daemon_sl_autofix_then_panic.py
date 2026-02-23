import asyncio
import logging
from datetime import timedelta

from trader.alerts import AlertManager
from trader.config import AppConfig
from trader.kill_switch import KillSwitch
from trader.notifier import Notifier
from trader.risk_daemon import RiskDaemon
from trader.state import PositionState, StateStore, utc_now
from trader.store import SQLiteStore


class FakeBitgetFailSL:
    def __init__(self) -> None:
        self.close_calls = 0

    def supports_plan_orders(self):
        return True

    def place_stop_loss(self, **kwargs):
        raise RuntimeError("sl place failed")

    def protective_close_position(self, symbol: str, side: str, size: float):
        self.close_calls += 1
        return {"ok": True}


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
                "max_account_drawdown_pct": 0.15,
                "account_risk_per_trade": 0.003,
                "max_notional_per_trade": 200,
                "default_stop_loss_pct": 0.006,
                "hard_stop_loss_required": True,
                "stoploss": {
                    "must_exist": True,
                    "max_time_without_sl_seconds": 10,
                    "sl_order_type": "trigger",
                    "emergency_close_if_sl_place_fails": True,
                },
                "assumed_equity_usdt": 1000,
            },
            "logging": {"level": "INFO", "file": "trader.log", "rich": False},
            "monitor": {"enabled": True},
        }
    )


def test_sl_autofix_failures_timeout_then_panic_close(tmp_path) -> None:
    store = SQLiteStore(str(tmp_path / "slpanic.db"))
    alerts = AlertManager(Notifier(logging.getLogger("test")), store, logging.getLogger("test"))
    state = StateStore()
    state.set_account(equity=1000, available=900, margin_used=100)
    state.set_positions(
        [
            PositionState(
                symbol="BTCUSDT",
                side="long",
                size=1.0,
                entry_price=100,
                mark_price=100,
                liq_price=50,
                pnl=0,
                leverage=5,
                margin_mode="isolated",
                timestamp=utc_now(),
                opened_at=utc_now() - timedelta(seconds=30),
            )
        ]
    )

    bitget = FakeBitgetFailSL()
    daemon = RiskDaemon(
        config=_config(),
        bitget=bitget,
        state=state,
        store=store,
        alerts=alerts,
        kill_switch=KillSwitch(store=store, file_path=str(tmp_path / "NO_SWITCH")),
    )

    asyncio.run(daemon.tick_once())

    assert bitget.close_calls == 1
    assert state.safe_mode is True

    event = store.conn.execute(
        "SELECT type FROM events WHERE type='SL_AUTOFIX_FAILED_THEN_PANIC' ORDER BY id DESC LIMIT 1"
    ).fetchone()
    assert event is not None

    action = store.conn.execute(
        "SELECT action, reason FROM reconciler_actions WHERE action='PROTECTIVE_CLOSE_EXECUTED' ORDER BY id DESC LIMIT 1"
    ).fetchone()
    assert action is not None
    assert action["reason"] == "sl_autofix_failed_then_panic"
