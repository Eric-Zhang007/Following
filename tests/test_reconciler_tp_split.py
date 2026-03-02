import logging

from trader.alerts import AlertManager
from trader.config import AppConfig
from trader.models import OrderAck
from trader.notifier import Notifier
from trader.order_reconciler import OrderReconciler
from trader.state import StateStore
from trader.store import SQLiteStore


class _FakeBitgetTP:
    def __init__(self) -> None:
        self.tp_sizes: list[float] = []

    def place_take_profit(self, **kwargs):  # noqa: ANN003
        self.tp_sizes.append(float(kwargs.get("size", 0.0)))
        idx = len(self.tp_sizes)
        return OrderAck(order_id=f"tp-{idx}", client_oid=kwargs.get("client_oid"), status="ACKED", raw={})


class _FakeContract:
    def __init__(self, size_place: int, min_trade_num: float = 0.0) -> None:
        self.size_place = size_place
        self.min_trade_num = min_trade_num


class _FakeSymbolRegistry:
    def __init__(self, size_place: int, min_trade_num: float = 0.0) -> None:
        self._contract = _FakeContract(size_place=size_place, min_trade_num=min_trade_num)

    def get_contract(self, symbol: str):  # noqa: ANN001
        return self._contract


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
                "position_mode": "one_way_mode",
                "margin_mode": "isolated",
            },
            "filters": {
                "symbol_policy": "ALLOW_ALL",
                "symbol_whitelist": [],
                "symbol_blacklist": [],
                "require_exchange_symbol": False,
                "min_usdt_volume_24h": None,
                "max_leverage": 50,
                "allow_sides": ["LONG", "SHORT"],
                "max_signal_age_seconds": 20,
                "leverage_over_limit_action": "CLAMP",
            },
            "risk": {
                "account_risk_per_trade": 0.003,
                "max_notional_per_trade": 200,
                "default_stop_loss_pct": 0.006,
                "assumed_equity_usdt": 1000,
                "stoploss": {
                    "sl_order_type": "trigger",
                    "trigger_price_type": "mark",
                },
            },
            "logging": {"level": "INFO", "file": "trader.log", "rich": False},
            "monitor": {"enabled": True},
        }
    )


def test_reconciler_tp_split_last_leg_consumes_remainder(tmp_path) -> None:
    store = SQLiteStore(str(tmp_path / "reconciler_tp_split.db"))
    alerts = AlertManager(Notifier(logging.getLogger("test")), store, logging.getLogger("test"))
    state = StateStore()
    bitget = _FakeBitgetTP()
    reconciler = OrderReconciler(
        _config(),
        bitget,
        state,
        store,
        alerts,
        symbol_registry=_FakeSymbolRegistry(size_place=3),
    )

    reconciler._place_tp_orders(
        symbol="INXUSDT",
        thread_id=100,
        side_hint="LONG",
        total_size=1.0,
        tp_points=[0.15, 0.18, 0.2],
        parent_client_order_id=None,
    )

    assert bitget.tp_sizes == [0.333, 0.333, 0.334]
