from trader.config import AppConfig
from trader.models import EntrySignal, EntryType, ParsedKind, Side, utc_now
from trader.risk import RiskManager


def _config() -> AppConfig:
    return AppConfig.model_validate(
        {
            "dry_run": True,
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
                "hard_stop_loss_required": True,
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


def test_drawdown_circuit_breaker_blocks_new_entries() -> None:
    manager = RiskManager(_config())

    first = manager.evaluate_entry(
        signal=_signal(),
        current_price=100.5,
        account_equity=1000,
        now=utc_now(),
        within_cooldown=False,
    )
    assert first.approved is True

    second = manager.evaluate_entry(
        signal=_signal(),
        current_price=100.5,
        account_equity=800,
        now=utc_now(),
        within_cooldown=False,
    )
    assert second.approved is False
    assert "drawdown" in str(second.reason)
