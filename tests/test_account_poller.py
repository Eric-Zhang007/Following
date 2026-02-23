import asyncio
import logging

from trader.account_poller import AccountPoller
from trader.alerts import AlertManager
from trader.config import AppConfig
from trader.notifier import Notifier
from trader.state import StateStore
from trader.store import SQLiteStore


class FakeBitget:
    def get_account_snapshot(self):
        return {"equity": 1200.0, "available": 900.0, "margin_used": 300.0}

    def get_positions(self):
        return [
            {
                "symbol": "BTCUSDT",
                "total": "0.02",
                "holdSide": "long",
                "openPriceAvg": "100000",
                "markPrice": "101000",
                "liquidationPrice": "90000",
                "unrealizedPL": "20",
                "leverage": "5",
                "marginMode": "isolated",
            }
        ]

    def get_open_orders(self):
        return [
            {
                "symbol": "BTCUSDT",
                "side": "buy",
                "state": "NEW",
                "baseVolume": "0",
                "clientOid": "entry-1",
                "orderId": "1001",
                "reduceOnly": "NO",
            }
        ]

    def list_plan_orders(self):
        return []

    def get_funding_rate(self, symbol: str):
        return 0.0001

    def get_contracts(self):
        return [{"symbol": "BTCUSDT"}]


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
            "logging": {"level": "INFO", "file": "trader.log", "rich": False},
            "monitor": {"enabled": True},
        }
    )


def test_account_poller_updates_state_and_snapshots(tmp_path) -> None:
    store = SQLiteStore(str(tmp_path / "monitor.db"))
    alerts = AlertManager(Notifier(logging.getLogger("test")), store, logging.getLogger("test"))
    state = StateStore()
    poller = AccountPoller(_config(), FakeBitget(), state, store, alerts)

    asyncio.run(poller.poll_account())
    asyncio.run(poller.poll_positions())
    asyncio.run(poller.poll_open_orders())

    assert state.account is not None
    assert state.account.equity == 1200.0
    assert "BTCUSDT" in state.positions
    assert state.positions["BTCUSDT"].size == 0.02
    assert state.find_order(client_order_id="entry-1") is not None

    row = store.conn.execute("SELECT equity, margin_used FROM equity_snapshots ORDER BY id DESC LIMIT 1").fetchone()
    assert row is not None
    assert float(row["equity"]) == 1200.0
    assert float(row["margin_used"]) == 300.0
