import logging

from trader.config import AppConfig
from trader.executor import TradeExecutor
from trader.models import EntrySignal, EntryType, ParsedKind, RiskDecision, Side, utc_now
from trader.notifier import Notifier
from trader.risk import RiskManager
from trader.store import SQLiteStore


class DummyBitget:
    def set_leverage(self, *args, **kwargs):
        return {"ok": True}

    def place_order(self, *args, **kwargs):
        return {"orderId": "1"}


class DummyRegistry:
    def get_contract(self, symbol: str):
        return None


def _config(dry_run: bool = True, hard_sl: bool = True) -> AppConfig:
    return AppConfig.model_validate(
        {
            "dry_run": dry_run,
            "listener": {"mode": "telegram"},
            "telegram": {"api_id": 1, "api_hash": "x", "session_name": "s", "channel": "@IvanCryptotalk"},
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
                "max_leverage": 10,
                "leverage_policy": "CAP",
                "default_stop_loss_pct": 0.006,
                "hard_stop_loss_required": hard_sl,
                "max_entry_slippage_pct": 0.003,
                "max_notional_per_trade": 200,
                "max_open_positions": 3,
                "cooldown_seconds": 300,
                "min_signal_quality": 0.8,
                "allow_symbols_policy": "ALLOWLIST",
                "symbol_allowlist": ["BTCUSDT"],
                "symbol_blacklist": [],
                "min_24h_usdt_volume": None,
                "assumed_equity_usdt": 1000,
            },
            "logging": {"level": "INFO", "file": "trader.log", "rich": False},
        }
    )


def _signal() -> EntrySignal:
    return EntrySignal(
        kind=ParsedKind.ENTRY_SIGNAL,
        raw_text="#BTC/USDT 进场: 限价100-101",
        symbol="BTCUSDT",
        quote="USDT",
        side=Side.LONG,
        leverage=5,
        entry_type=EntryType.LIMIT,
        entry_low=100,
        entry_high=101,
        timestamp=utc_now(),
    )


def test_default_stop_loss_generated_when_missing() -> None:
    manager = RiskManager(_config())
    decision = manager.evaluate_entry(
        signal=_signal(),
        current_price=100.5,
        account_equity=1000,
        now=utc_now(),
        within_cooldown=False,
    )
    assert decision.approved is True
    assert decision.stop_loss_price is not None


def test_hard_stop_loss_required_rejects_when_stop_order_unavailable(tmp_path) -> None:
    config = _config(dry_run=False, hard_sl=True)
    store = SQLiteStore(str(tmp_path / "risk_stop.db"))
    executor = TradeExecutor(
        config=config,
        bitget=DummyBitget(),  # type: ignore[arg-type]
        store=store,
        notifier=Notifier(logging.getLogger("test")),
        logger=logging.getLogger("test"),
        symbol_registry=DummyRegistry(),  # type: ignore[arg-type]
    )
    signal = _signal()
    decision = RiskDecision(
        approved=True,
        symbol="BTCUSDT",
        side=Side.LONG,
        leverage=5,
        notional=100,
        quantity=1,
        entry_price=100,
        stop_loss_price=99,
        stop_distance_ratio=0.01,
    )

    executor.execute_entry(signal, decision, chat_id=1, message_id=1, version=1)

    row = store.conn.execute("SELECT status, reason FROM executions ORDER BY id DESC LIMIT 1").fetchone()
    assert row is not None
    assert row["status"] == "REJECTED"
    assert "hard_stop_loss_required" in row["reason"]
