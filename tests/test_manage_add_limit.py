import logging

from trader.config import AppConfig
from trader.executor import TradeExecutor
from trader.models import ManageAction, ParsedKind
from trader.notifier import Notifier
from trader.store import SQLiteStore


class FakeBitgetManageAdd:
    def __init__(self) -> None:
        self.place_calls = 0

    def get_position(self, symbol: str):  # noqa: ARG002
        return {
            "symbol": "BTCUSDT",
            "total": "1",
            "holdSide": "long",
            "openPriceAvg": "100",
        }

    def place_order(self, **kwargs):  # noqa: ANN003
        self.place_calls += 1
        return {"orderId": f"order-{self.place_calls}", "clientOid": kwargs.get("client_oid")}


def _config() -> AppConfig:
    return AppConfig.model_validate(
        {
            "dry_run": False,
            "listener": {"mode": "web_preview"},
            "telegram": {"session_name": "s", "channel": "@x"},
            "bitget": {
                "base_url": "https://api.bitget.com",
                "api_key": "k",
                "api_secret": "s",
                "passphrase": "p",
                "product_type": "USDT-FUTURES",
                "position_mode": "one_way_mode",
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
            "execution": {
                "require_order_ack": False,
                "max_manage_add_times_per_thread": 2,
            },
            "logging": {"level": "INFO", "file": "trader.log", "rich": False},
            "monitor": {"enabled": False},
        }
    )


def test_manage_add_limited_to_two_times_per_thread(tmp_path) -> None:
    store = SQLiteStore(str(tmp_path / "manage_add.db"))
    store.upsert_trade_thread(
        thread_id=77,
        symbol="BTCUSDT",
        side="LONG",
        leverage=10,
        status="ACTIVE",
    )
    bitget = FakeBitgetManageAdd()
    executor = TradeExecutor(
        config=_config(),
        bitget=bitget,
        store=store,
        notifier=Notifier(logging.getLogger("test")),
        logger=logging.getLogger("test"),
    )
    action = ManageAction(
        kind=ParsedKind.MANAGE_ACTION,
        raw_text="补仓50%",
        symbol="BTCUSDT",
        reduce_pct=None,
        move_sl_to_be=False,
        tp_price=None,
        add_pct=50,
    )

    executor.execute_manage(action, chat_id=1, message_id=1, version=1, thread_id=77)
    executor.execute_manage(action, chat_id=1, message_id=2, version=1, thread_id=77)
    executor.execute_manage(action, chat_id=1, message_id=3, version=1, thread_id=77)

    assert bitget.place_calls == 2
    assert store.count_thread_actions(77, "MANAGE_ADD") == 2
    rejected = store.conn.execute(
        "SELECT reason FROM executions WHERE thread_id=77 AND action_type='MANAGE_ADD' AND status='REJECTED' ORDER BY id DESC LIMIT 1"
    ).fetchone()
    assert rejected is not None
    assert "exceeded limit" in str(rejected["reason"])
