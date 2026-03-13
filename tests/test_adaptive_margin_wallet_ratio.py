import json
import logging
import math

from trader.config import AppConfig
from trader.executor import TradeExecutor
from trader.models import EntrySignal, EntryType, ParsedKind, Side
from trader.notifier import Notifier
from trader.state import StateStore, utc_now
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
                "symbol_whitelist": ["BTCUSDT"],
                "max_leverage": 125,
                "allow_sides": ["LONG", "SHORT"],
                "max_signal_age_seconds": 30,
                "leverage_over_limit_action": "CLAMP",
            },
            "risk": {
                "account_risk_per_trade": 0.01,
                "max_notional_per_trade": 1_000_000,
                "entry_slippage_pct": 1,
                "cooldown_seconds": 0,
                "default_stop_loss_pct": 1,
                "assumed_equity_usdt": 1000,
            },
            "execution": {
                "margin_sizing_mode": "adaptive_leverage",
                "adaptive_margin_wallet_balance_source": "available",
                "adaptive_margin_base_ratio": 0.04,
                "adaptive_margin_min_ratio": 0.01,
                "adaptive_margin_max_ratio": 0.12,
                "adaptive_margin_base_leverage": 50,
                "adaptive_margin_min_usdt": 1,
                "adaptive_margin_max_usdt": 1000,
                "entry_split_ratio": [1],
            },
            "logging": {"level": "INFO", "file": "trader.log", "rich": False},
        }
    )


def _signal(leverage: int) -> EntrySignal:
    return EntrySignal(
        kind=ParsedKind.ENTRY_SIGNAL,
        raw_text="BTC long",
        symbol="BTCUSDT",
        quote="USDT",
        side=Side.LONG,
        leverage=leverage,
        entry_type=EntryType.LIMIT,
        entry_low=2.0,
        entry_high=2.0,
        entry_points=[2.0],
        stop_loss=1.5,
        tp_points=[2.2],
    )


def test_adaptive_margin_uses_available_wallet_ratio(tmp_path) -> None:
    store = SQLiteStore(str(tmp_path / "adaptive_wallet.db"))
    state = StateStore()
    state.set_account(equity=1000, available=600, margin_used=0, timestamp=utc_now())
    contract = ContractInfo(symbol="BTCUSDT", size_place=6, price_place=6, min_trade_num=0.0, raw={})
    executor = TradeExecutor(
        config=_config(),
        bitget=_FakeBitget(),  # type: ignore[arg-type]
        store=store,
        notifier=Notifier(logging.getLogger("test")),
        logger=logging.getLogger("test"),
        symbol_registry=_FakeRegistry(contract),  # type: ignore[arg-type]
        runtime_state=state,
    )
    result = executor.execute_thread_entry(_signal(leverage=50), chat_id=1, message_id=1, version=1, thread_id=101)
    assert result["placed"] == 1

    row = store.conn.execute(
        "SELECT intent_json FROM executions WHERE thread_id=101 AND purpose='entry' ORDER BY id DESC LIMIT 1"
    ).fetchone()
    assert row is not None
    intent = json.loads(str(row["intent_json"]))
    assert abs(float(intent["margin_usdt_per_entry"]) - 24.0) < 1e-6
    assert abs(float(intent["notional_per_entry"]) - 1200.0) < 1e-6
    adaptive = intent["adaptive_margin"]
    assert adaptive["wallet_balance_source"] == "available"
    assert abs(float(adaptive["wallet_balance"]) - 600.0) < 1e-6
    assert abs(float(adaptive["adaptive_ratio"]) - 0.04) < 1e-6


def test_adaptive_margin_falls_back_to_assumed_equity_when_runtime_account_missing(tmp_path) -> None:
    store = SQLiteStore(str(tmp_path / "adaptive_fallback.db"))
    contract = ContractInfo(symbol="BTCUSDT", size_place=6, price_place=6, min_trade_num=0.0, raw={})
    executor = TradeExecutor(
        config=_config(),
        bitget=_FakeBitget(),  # type: ignore[arg-type]
        store=store,
        notifier=Notifier(logging.getLogger("test")),
        logger=logging.getLogger("test"),
        symbol_registry=_FakeRegistry(contract),  # type: ignore[arg-type]
        runtime_state=StateStore(),
    )
    result = executor.execute_thread_entry(_signal(leverage=10), chat_id=1, message_id=2, version=1, thread_id=102)
    assert result["placed"] == 1

    row = store.conn.execute(
        "SELECT intent_json FROM executions WHERE thread_id=102 AND purpose='entry' ORDER BY id DESC LIMIT 1"
    ).fetchone()
    assert row is not None
    intent = json.loads(str(row["intent_json"]))
    expected_ratio = 0.04 * math.sqrt(50.0 / 10.0)
    expected_margin = 1000.0 * expected_ratio
    assert abs(float(intent["margin_usdt_per_entry"]) - expected_margin) < 1e-6
    adaptive = intent["adaptive_margin"]
    assert adaptive["wallet_balance_source"] == "assumed_equity"
    assert abs(float(adaptive["wallet_balance"]) - 1000.0) < 1e-6
    assert abs(float(adaptive["adaptive_ratio"]) - expected_ratio) < 1e-6


def test_adaptive_margin_ignores_max_margin_cap_and_uses_ratio_value(tmp_path) -> None:
    cfg = _config()
    cfg.execution.adaptive_margin_max_usdt = 100.0

    store = SQLiteStore(str(tmp_path / "adaptive_no_max_cap.db"))
    state = StateStore()
    state.set_account(equity=10_000, available=5_000, margin_used=0, timestamp=utc_now())
    contract = ContractInfo(symbol="BTCUSDT", size_place=6, price_place=6, min_trade_num=0.0, raw={})
    executor = TradeExecutor(
        config=cfg,
        bitget=_FakeBitget(),  # type: ignore[arg-type]
        store=store,
        notifier=Notifier(logging.getLogger("test")),
        logger=logging.getLogger("test"),
        symbol_registry=_FakeRegistry(contract),  # type: ignore[arg-type]
        runtime_state=state,
    )

    result = executor.execute_thread_entry(_signal(leverage=50), chat_id=1, message_id=3, version=1, thread_id=103)
    assert result["placed"] == 1

    row = store.conn.execute(
        "SELECT intent_json FROM executions WHERE thread_id=103 AND purpose='entry' ORDER BY id DESC LIMIT 1"
    ).fetchone()
    assert row is not None
    intent = json.loads(str(row["intent_json"]))
    # available=5000, adaptive_ratio=0.04 => margin should be 200, not capped to 100.
    assert abs(float(intent["margin_usdt_per_entry"]) - 200.0) < 1e-6
