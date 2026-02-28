import json
import logging

from trader.config import AppConfig
from trader.executor import TradeExecutor
from trader.models import EntrySignal, EntryType, ParsedKind, Side
from trader.notifier import Notifier
from trader.state import StateStore
from trader.store import SQLiteStore
from trader.symbol_registry import ContractInfo


class _FakeRegistry:
    def __init__(self, contract: ContractInfo) -> None:
        self.contract = contract

    def get_contract(self, symbol: str) -> ContractInfo | None:
        if symbol.upper() == self.contract.symbol:
            return self.contract
        return None


class _FakeBitget:
    pass


def _config() -> AppConfig:
    return AppConfig.model_validate(
        {
            "dry_run": True,
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
                "symbol_whitelist": ["PEPEUSDT"],
                "max_leverage": 100,
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
            "execution": {
                "per_trade_margin_usdt": 30,
                "entry_split_ratio": [1, 2],
            },
            "logging": {"level": "INFO", "file": "trader.log", "rich": False},
        }
    )


def test_two_entry_split_ratio_keeps_post_order(tmp_path) -> None:
    store = SQLiteStore(str(tmp_path / "split.db"))
    state = StateStore()
    contract = ContractInfo(symbol="PEPEUSDT", size_place=6, price_place=8, min_trade_num=0.0, raw={})
    executor = TradeExecutor(
        config=_config(),
        bitget=_FakeBitget(),  # type: ignore[arg-type]
        store=store,
        notifier=Notifier(logging.getLogger("test")),
        logger=logging.getLogger("test"),
        symbol_registry=_FakeRegistry(contract),  # type: ignore[arg-type]
        runtime_state=state,
    )
    signal = EntrySignal(
        kind=ParsedKind.ENTRY_SIGNAL,
        raw_text="交易信號",
        symbol="PEPEUSDT",
        quote="USDT",
        side=Side.LONG,
        leverage=10,
        entry_type=EntryType.LIMIT,
        entry_low=8.0,
        entry_high=10.0,
        entry_points=[10.0, 8.0],
        stop_loss=7.5,
        tp_points=[11.0, 12.0],
    )
    result = executor.execute_thread_entry(signal, chat_id=1, message_id=11, version=1, thread_id=9001)
    assert result["placed"] == 2

    rows = store.conn.execute(
        "SELECT intent_json FROM executions WHERE thread_id=9001 AND purpose='entry' ORDER BY id ASC"
    ).fetchall()
    assert len(rows) == 2
    i0 = json.loads(rows[0]["intent_json"])
    i1 = json.loads(rows[1]["intent_json"])
    assert i0["entry_points"] == [10.0, 8.0]
    assert i1["entry_points"] == [10.0, 8.0]
    assert i0["entry_index"] == 0
    assert i1["entry_index"] == 1
    assert i0["quantity"] < i1["quantity"]
    assert i0["price"] == 10.0
    assert i1["price"] == 8.0
