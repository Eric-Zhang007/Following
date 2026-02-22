from trader.config import AppConfig
from trader.models import EntrySignal, EntryType, ParsedKind, Side, utc_now
from trader.risk import RiskManager


class FakeRegistry:
    def __init__(self, tradable: set[str], volumes: dict[str, float]) -> None:
        self.tradable = {s.upper() for s in tradable}
        self.volumes = {k.upper(): v for k, v in volumes.items()}

    def is_tradable(self, symbol: str) -> bool:
        return symbol.upper() in self.tradable

    def get_24h_volume(self, symbol: str) -> float | None:
        return self.volumes.get(symbol.upper())


def build_config(filters: dict) -> AppConfig:
    return AppConfig.model_validate(
        {
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
            "filters": filters,
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
    )


def build_signal(symbol: str) -> EntrySignal:
    return EntrySignal(
        kind=ParsedKind.ENTRY_SIGNAL,
        raw_text="#X/USDT",
        symbol=symbol,
        quote="USDT",
        side=Side.LONG,
        leverage=3,
        entry_type=EntryType.LIMIT,
        entry_low=100,
        entry_high=101,
        timestamp=utc_now(),
    )


def test_allow_all_accepts_tradable_symbol() -> None:
    config = build_config(
        {
            "symbol_policy": "ALLOW_ALL",
            "symbol_whitelist": [],
            "symbol_blacklist": [],
            "require_exchange_symbol": True,
            "min_usdt_volume_24h": None,
            "max_leverage": 10,
            "allow_sides": ["LONG", "SHORT"],
            "max_signal_age_seconds": 20,
            "leverage_over_limit_action": "CLAMP",
        }
    )
    registry = FakeRegistry(tradable={"NICHEUSDT"}, volumes={})
    manager = RiskManager(config, symbol_registry=registry)

    decision = manager.evaluate_entry(
        build_signal("NICHEUSDT"),
        current_price=100.5,
        account_equity=1000,
        now=utc_now(),
        within_cooldown=False,
    )
    assert decision.approved is True


def test_allow_all_rejects_blacklist() -> None:
    config = build_config(
        {
            "symbol_policy": "ALLOW_ALL",
            "symbol_whitelist": [],
            "symbol_blacklist": ["BADUSDT"],
            "require_exchange_symbol": False,
            "min_usdt_volume_24h": None,
            "max_leverage": 10,
            "allow_sides": ["LONG", "SHORT"],
            "max_signal_age_seconds": 20,
            "leverage_over_limit_action": "CLAMP",
        }
    )
    manager = RiskManager(config, symbol_registry=FakeRegistry(set(), {}))

    decision = manager.evaluate_entry(
        build_signal("BADUSDT"),
        current_price=100.5,
        account_equity=1000,
        now=utc_now(),
        within_cooldown=False,
    )
    assert decision.approved is False
    assert "blacklist" in str(decision.reason)


def test_allow_all_rejects_non_exchange_symbol_when_required() -> None:
    config = build_config(
        {
            "symbol_policy": "ALLOW_ALL",
            "symbol_whitelist": [],
            "symbol_blacklist": [],
            "require_exchange_symbol": True,
            "min_usdt_volume_24h": None,
            "max_leverage": 10,
            "allow_sides": ["LONG", "SHORT"],
            "max_signal_age_seconds": 20,
            "leverage_over_limit_action": "CLAMP",
        }
    )
    manager = RiskManager(config, symbol_registry=FakeRegistry(tradable={"BTCUSDT"}, volumes={}))

    decision = manager.evaluate_entry(
        build_signal("UNKNOWNUSDT"),
        current_price=100.5,
        account_equity=1000,
        now=utc_now(),
        within_cooldown=False,
    )
    assert decision.approved is False
    assert "not tradable" in str(decision.reason)


def test_allow_all_rejects_low_volume() -> None:
    config = build_config(
        {
            "symbol_policy": "ALLOW_ALL",
            "symbol_whitelist": [],
            "symbol_blacklist": [],
            "require_exchange_symbol": True,
            "min_usdt_volume_24h": 1_000_000,
            "max_leverage": 10,
            "allow_sides": ["LONG", "SHORT"],
            "max_signal_age_seconds": 20,
            "leverage_over_limit_action": "CLAMP",
        }
    )
    manager = RiskManager(
        config,
        symbol_registry=FakeRegistry(tradable={"THINUSDT"}, volumes={"THINUSDT": 10_000}),
    )

    decision = manager.evaluate_entry(
        build_signal("THINUSDT"),
        current_price=100.5,
        account_equity=1000,
        now=utc_now(),
        within_cooldown=False,
    )
    assert decision.approved is False
    assert "below threshold" in str(decision.reason)
