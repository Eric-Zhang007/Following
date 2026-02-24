import logging

from trader.config import AppConfig
from trader.executor import TradeExecutor
from trader.models import OrderAck
from trader.notifier import Notifier
from trader.state import PositionState, StateStore, utc_now
from trader.store import SQLiteStore
from trader.symbol_registry import ContractInfo


class FakeBitget:
    def __init__(self, position_payload=None) -> None:
        self.position_payload = position_payload if position_payload is not None else {}
        self.tp_calls: list[dict] = []

    def place_take_profit(self, **kwargs):
        self.tp_calls.append(kwargs)
        return OrderAck(order_id="tp-1", client_oid=kwargs.get("client_oid"), status="ACKED", raw={})

    def get_position(self, symbol: str):
        return self.position_payload


class FakeRegistry:
    def __init__(self, min_trade_num: float) -> None:
        self.min_trade_num = min_trade_num

    def get_contract(self, symbol: str):
        return ContractInfo(
            symbol=symbol,
            size_place=3,
            price_place=2,
            min_trade_num=self.min_trade_num,
            raw={},
        )


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
            "logging": {"level": "INFO", "file": "trader.log", "rich": False},
            "monitor": {"enabled": True},
        }
    )


def _make_executor(tmp_path, bitget: FakeBitget, state: StateStore, symbol_registry=None) -> TradeExecutor:
    return TradeExecutor(
        config=_config(),
        bitget=bitget,
        store=SQLiteStore(str(tmp_path / "tp.db")),
        notifier=Notifier(logging.getLogger("test")),
        logger=logging.getLogger("test"),
        symbol_registry=symbol_registry,
        runtime_state=state,
    )


def test_tp_size_resolved_from_runtime_position_when_total_size_unknown(tmp_path) -> None:
    state = StateStore()
    state.set_positions(
        [
            PositionState(
                symbol="BTCUSDT",
                side="long",
                size=2.0,
                entry_price=100,
                mark_price=101,
                liq_price=50,
                pnl=0,
                leverage=5,
                margin_mode="isolated",
                timestamp=utc_now(),
                opened_at=utc_now(),
            )
        ]
    )
    bitget = FakeBitget(position_payload={})
    executor = _make_executor(tmp_path, bitget, state)

    result = executor._place_take_profit_orders(
        symbol="BTCUSDT",
        side_hint=None,
        total_size=None,
        tp_list=[110.0],
        parent_client_order_id=None,
    )

    assert result["placed"] == 1
    assert len(bitget.tp_calls) == 1
    assert float(bitget.tp_calls[0]["size"]) > 0


def test_tp_skipped_when_total_size_unknown_and_no_position(tmp_path) -> None:
    state = StateStore()
    bitget = FakeBitget(position_payload={})
    executor = _make_executor(tmp_path, bitget, state)

    result = executor._place_take_profit_orders(
        symbol="BTCUSDT",
        side_hint=None,
        total_size=None,
        tp_list=[110.0],
        parent_client_order_id=None,
    )

    assert result["placed"] == 0
    assert len(bitget.tp_calls) == 0
    row = executor.store.conn.execute(
        "SELECT type, trace_id FROM events WHERE type='TP_SKIPPED_SIZE_UNKNOWN' ORDER BY id DESC LIMIT 1"
    ).fetchone()
    assert row is not None
    assert row["trace_id"] is not None


def test_tp_size_below_min_trade_num_is_skipped_with_reason(tmp_path) -> None:
    state = StateStore()
    state.set_positions(
        [
            PositionState(
                symbol="BTCUSDT",
                side="long",
                size=0.5,
                entry_price=100,
                mark_price=101,
                liq_price=50,
                pnl=0,
                leverage=5,
                margin_mode="isolated",
                timestamp=utc_now(),
                opened_at=utc_now(),
            )
        ]
    )
    bitget = FakeBitget(position_payload={})
    registry = FakeRegistry(min_trade_num=1.0)
    executor = _make_executor(tmp_path, bitget, state, symbol_registry=registry)

    result = executor._place_take_profit_orders(
        symbol="BTCUSDT",
        side_hint="LONG",
        total_size=None,
        tp_list=[110.0, 120.0],
        parent_client_order_id=None,
    )

    assert result["placed"] == 0
    assert len(bitget.tp_calls) == 0
    row = executor.store.conn.execute(
        "SELECT type, payload_json FROM events WHERE type='TP_SKIPPED_INVALID_SIZE' ORDER BY id DESC LIMIT 1"
    ).fetchone()
    assert row is not None
    assert "minTradeNum" in (row["payload_json"] or "")
