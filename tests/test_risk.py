from datetime import timedelta

from trader.config import AppConfig
from trader.models import EntrySignal, EntryType, ParsedKind, Side, utc_now
from trader.risk import RiskManager


def build_config(overrides: dict | None = None) -> AppConfig:
    data = {
        "dry_run": True,
        "telegram": {
            "api_id": 1,
            "api_hash": "x",
            "session_name": "s",
            "channel": "@IvanCryptotalk",
        },
        "bitget": {
            "base_url": "https://api.bitget.com",
            "api_key": "",
            "api_secret": "",
            "passphrase": "",
            "product_type": "USDT-FUTURES",
        },
        "filters": {
            "symbol_whitelist": ["BTCUSDT", "CYBERUSDT"],
            "max_leverage": 10,
            "allow_sides": ["LONG", "SHORT"],
            "max_signal_age_seconds": 20,
            "leverage_over_limit_action": "CLAMP",
        },
        "risk": {
            "account_risk_per_trade": 0.005,
            "max_notional_per_trade": 200,
            "entry_slippage_pct": 0.3,
            "cooldown_seconds": 300,
            "default_stop_loss_pct": 1.0,
            "assumed_equity_usdt": 1000,
        },
        "logging": {"level": "INFO", "file": "trader.log", "rich": False},
    }
    if overrides:
        for key, value in overrides.items():
            data[key] = value
    return AppConfig.model_validate(data)


def build_signal(**kwargs) -> EntrySignal:
    now = utc_now()
    payload = {
        "kind": ParsedKind.ENTRY_SIGNAL,
        "raw_text": "#BTC/USDT (5x long) 进场: 限价100-101",
        "symbol": "BTCUSDT",
        "quote": "USDT",
        "side": Side.LONG,
        "leverage": 5,
        "entry_type": EntryType.LIMIT,
        "entry_low": 100.0,
        "entry_high": 101.0,
        "timestamp": now,
    }
    payload.update(kwargs)
    return EntrySignal(**payload)


def test_risk_rejects_symbol_not_whitelisted() -> None:
    config = build_config()
    manager = RiskManager(config)
    signal = build_signal(symbol="ETHUSDT")

    decision = manager.evaluate_entry(signal, current_price=100, account_equity=1000, now=utc_now(), within_cooldown=False)
    assert decision.approved is False
    assert "whitelist" in str(decision.reason)


def test_risk_clamps_leverage() -> None:
    config = build_config()
    manager = RiskManager(config)
    signal = build_signal(leverage=50)

    decision = manager.evaluate_entry(signal, current_price=100.5, account_equity=1000, now=utc_now(), within_cooldown=False)
    assert decision.approved is True
    assert decision.leverage == 10


def test_risk_rejects_slippage_for_limit() -> None:
    config = build_config()
    manager = RiskManager(config)
    signal = build_signal(entry_low=100, entry_high=101)

    decision = manager.evaluate_entry(signal, current_price=110, account_equity=1000, now=utc_now(), within_cooldown=False)
    assert decision.approved is False
    assert "deviation" in str(decision.reason)


def test_risk_rejects_cooldown() -> None:
    config = build_config()
    manager = RiskManager(config)
    signal = build_signal()

    decision = manager.evaluate_entry(signal, current_price=100.5, account_equity=1000, now=utc_now(), within_cooldown=True)
    assert decision.approved is False
    assert "cooldown" in str(decision.reason)


def test_risk_rejects_expired_signal() -> None:
    config = build_config()
    manager = RiskManager(config)
    signal = build_signal(timestamp=utc_now() - timedelta(seconds=100))

    decision = manager.evaluate_entry(signal, current_price=100.5, account_equity=1000, now=utc_now(), within_cooldown=False)
    assert decision.approved is False
    assert "old" in str(decision.reason)
