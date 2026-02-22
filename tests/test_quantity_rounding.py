import logging

from trader.config import AppConfig
from trader.executor import TradeExecutor
from trader.notifier import Notifier
from trader.symbol_registry import ContractInfo


class FakeRegistry:
    def __init__(self, contract: ContractInfo | None) -> None:
        self.contract = contract

    def get_contract(self, symbol: str):
        return self.contract


def build_config() -> AppConfig:
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
            "filters": {
                "symbol_policy": "ALLOWLIST",
                "symbol_whitelist": ["BTCUSDT"],
                "symbol_blacklist": [],
                "require_exchange_symbol": True,
                "min_usdt_volume_24h": None,
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
    )


def test_quantity_and_price_rounding() -> None:
    contract = ContractInfo(symbol="BTCUSDT", size_place=3, price_place=2, min_trade_num=0.01, raw={})
    executor = TradeExecutor(
        config=build_config(),
        bitget=object(),  # type: ignore[arg-type]
        store=object(),  # type: ignore[arg-type]
        notifier=Notifier(logging.getLogger("test")),
        logger=logging.getLogger("test"),
        symbol_registry=FakeRegistry(contract),  # type: ignore[arg-type]
    )

    qty, price, reason = executor._normalize_order_params("BTCUSDT", 1.23456, 123.4567)
    assert reason is None
    assert qty == 1.234
    assert price == 123.45


def test_quantity_below_min_trade_rejected() -> None:
    contract = ContractInfo(symbol="BTCUSDT", size_place=3, price_place=2, min_trade_num=0.01, raw={})
    executor = TradeExecutor(
        config=build_config(),
        bitget=object(),  # type: ignore[arg-type]
        store=object(),  # type: ignore[arg-type]
        notifier=Notifier(logging.getLogger("test")),
        logger=logging.getLogger("test"),
        symbol_registry=FakeRegistry(contract),  # type: ignore[arg-type]
    )

    qty, _, reason = executor._normalize_order_params("BTCUSDT", 0.0099, 100.0)
    assert qty == 0.009
    assert reason is not None
    assert "minTradeNum" in reason
