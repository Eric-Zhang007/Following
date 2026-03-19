import logging

from trader.config import AppConfig
from trader.executor import TradeExecutor
from trader.notifier import Notifier
from trader.store import SQLiteStore


class _FakeBitget:
    def __init__(self) -> None:
        self.tp_calls = 0

    def get_position(self, symbol: str):  # noqa: ARG002
        raise RuntimeError("position unavailable")

    def place_take_profit(self, **kwargs):  # noqa: ANN003
        self.tp_calls += 1
        return {"orderId": "x"}


def _config() -> AppConfig:
    return AppConfig.model_validate(
        {
            "dry_run": False,
            "listener": {"mode": "web_preview"},
            "telegram": {"session_name": "s", "channel": "@x"},
            "bitget": {
                "base_url": "https://api.bitget.com",
                "api_key": "",
                "api_secret": "",
                "passphrase": "",
                "product_type": "USDT-FUTURES",
            },
            "filters": {
                "symbol_whitelist": ["BTCUSDT"],
                "max_leverage": 20,
                "allow_sides": ["LONG", "SHORT"],
                "max_signal_age_seconds": 30,
                "leverage_over_limit_action": "CLAMP",
            },
            "risk": {
                "account_risk_per_trade": 0.003,
                "max_notional_per_trade": 100000,
                "entry_slippage_pct": 1,
                "cooldown_seconds": 0,
                "default_stop_loss_pct": 1,
                "assumed_equity_usdt": 1000,
            },
            "logging": {"level": "INFO", "file": "trader.log", "rich": False},
        }
    )


def test_tp_total_size_unknown_skip_without_zero_size_order(tmp_path) -> None:
    bitget = _FakeBitget()
    store = SQLiteStore(str(tmp_path / "tp_nonzero.db"))
    executor = TradeExecutor(
        config=_config(),
        bitget=bitget,  # type: ignore[arg-type]
        store=store,
        notifier=Notifier(logging.getLogger("test")),
        logger=logging.getLogger("test"),
        symbol_registry=None,
        runtime_state=None,
    )

    result = executor._place_take_profit_orders(
        symbol="BTCUSDT",
        side_hint="LONG",
        total_size=None,
        tp_list=[100000.0],
        parent_client_order_id=None,
    )
    assert result["placed"] == 0
    assert result["last_reason"] == "size_unknown"
    assert bitget.tp_calls == 0
