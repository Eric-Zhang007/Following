import json
import logging

from trader.config import AppConfig
from trader.executor import TradeExecutor
from trader.models import EntrySignal, EntryType, ParsedKind, RiskDecision, Side
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


def test_two_entry_points_each_use_full_margin_notional(tmp_path) -> None:
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
    notional0 = float(i0["quantity"]) * float(i0["price"])
    notional1 = float(i1["quantity"]) * float(i1["price"])
    assert abs(notional0 - 300.0) < 1e-4
    assert abs(notional1 - 300.0) < 1e-4
    assert i0["price"] == 10.0
    assert i1["price"] == 8.0


def test_high_leverage_two_entry_points_keep_equal_notional_per_point(tmp_path) -> None:
    store = SQLiteStore(str(tmp_path / "split_major.db"))
    state = StateStore()
    contract = ContractInfo(symbol="BTCUSDT", size_place=6, price_place=2, min_trade_num=0.0, raw={})
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
        symbol="BTCUSDT",
        quote="USDT",
        side=Side.LONG,
        leverage=60,
        entry_type=EntryType.LIMIT,
        entry_low=60000.0,
        entry_high=62000.0,
        entry_points=[62000.0, 60000.0],
        stop_loss=59000.0,
        tp_points=[64000.0, 66000.0],
    )
    result = executor.execute_thread_entry(signal, chat_id=1, message_id=22, version=1, thread_id=9002)
    assert result["placed"] == 2

    rows = store.conn.execute(
        "SELECT intent_json FROM executions WHERE thread_id=9002 AND purpose='entry' ORDER BY id ASC"
    ).fetchall()
    assert len(rows) == 2
    i0 = json.loads(rows[0]["intent_json"])
    i1 = json.loads(rows[1]["intent_json"])

    notional0 = float(i0["quantity"]) * float(i0["price"])
    notional1 = float(i1["quantity"]) * float(i1["price"])
    assert abs(notional0 - 1800.0) < 0.05
    assert abs(notional1 - 1800.0) < 0.05


def test_thread_entry_ignores_risk_notional_cap_and_keeps_margin_sizing(tmp_path) -> None:
    store = SQLiteStore(str(tmp_path / "split_cap.db"))
    state = StateStore()
    contract = ContractInfo(symbol="BTCUSDT", size_place=6, price_place=2, min_trade_num=0.0, raw={})
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
        symbol="BTCUSDT",
        quote="USDT",
        side=Side.LONG,
        leverage=50,
        entry_type=EntryType.LIMIT,
        entry_low=60000.0,
        entry_high=62000.0,
        entry_points=[62000.0, 60000.0],
        stop_loss=59000.0,
        tp_points=[64000.0, 66000.0],
    )
    # per_trade_margin_usdt=30 and leverage=50 implies notional=1500 per entry.
    # risk_decision.notional no longer clamps executor sizing.
    decision = RiskDecision(approved=True, notional=300.0)
    result = executor.execute_thread_entry(
        signal,
        chat_id=1,
        message_id=33,
        version=1,
        thread_id=9003,
        risk_decision=decision,
    )
    assert result["placed"] == 2
    rows = store.conn.execute(
        "SELECT intent_json FROM executions WHERE thread_id=9003 AND purpose='entry' ORDER BY id ASC"
    ).fetchall()
    assert len(rows) == 2
    i0 = json.loads(rows[0]["intent_json"])
    i1 = json.loads(rows[1]["intent_json"])
    n0 = float(i0["quantity"]) * float(i0["price"])
    n1 = float(i1["quantity"]) * float(i1["price"])
    total_notional = n0 + n1
    assert abs(n0 - 1500.0) < 0.1
    assert abs(n1 - 1500.0) < 0.1
    assert abs(total_notional - 3000.0) < 0.2


def test_market_entry_without_numeric_range_uses_risk_anchor(tmp_path) -> None:
    store = SQLiteStore(str(tmp_path / "split_market_anchor.db"))
    state = StateStore()
    contract = ContractInfo(symbol="INXUSDT", size_place=6, price_place=6, min_trade_num=0.0, raw={})
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
        symbol="INXUSDT",
        quote="USDT",
        side=Side.LONG,
        leverage=10,
        entry_type=EntryType.MARKET,
        entry_low=0.0,
        entry_high=0.0,
        entry_points=[],
        stop_loss=None,
        tp_points=[0.12, 0.13],
    )
    decision = RiskDecision(approved=True, entry_price=0.0115, notional=300.0)
    result = executor.execute_thread_entry(
        signal,
        chat_id=1,
        message_id=44,
        version=1,
        thread_id=9004,
        risk_decision=decision,
    )
    assert result["placed"] == 1
    row = store.conn.execute(
        "SELECT intent_json FROM executions WHERE thread_id=9004 AND purpose='entry' ORDER BY id DESC LIMIT 1"
    ).fetchone()
    assert row is not None
    intent = json.loads(row["intent_json"])
    assert intent["order_type"] == "market"
    assert intent["price"] is None
    assert intent["entry_points"] == [0.0115]
    assert float(intent["quantity"]) > 0


def test_market_entry_rejected_when_anchor_unavailable(tmp_path) -> None:
    store = SQLiteStore(str(tmp_path / "split_market_no_anchor.db"))
    state = StateStore()
    contract = ContractInfo(symbol="INXUSDT", size_place=6, price_place=6, min_trade_num=0.0, raw={})
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
        symbol="INXUSDT",
        quote="USDT",
        side=Side.LONG,
        leverage=10,
        entry_type=EntryType.MARKET,
        entry_low=0.0,
        entry_high=0.0,
        entry_points=[],
    )
    result = executor.execute_thread_entry(signal, chat_id=1, message_id=45, version=1, thread_id=9005)
    assert result["placed"] == 0
    assert result["failed"] == 1
    row = store.conn.execute(
        "SELECT status, reason FROM executions WHERE thread_id=9005 AND purpose='entry' ORDER BY id DESC LIMIT 1"
    ).fetchone()
    assert row is not None
    assert row["status"] == "REJECTED"
    assert "market_entry_anchor_price_unavailable" in str(row["reason"])
