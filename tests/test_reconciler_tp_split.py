import asyncio
import logging

from trader.alerts import AlertManager
from trader.config import AppConfig
from trader.models import OrderAck
from trader.notifier import Notifier
from trader.order_reconciler import OrderReconciler
from trader.state import OrderState, PositionState, StateStore, utc_now
from trader.store import SQLiteStore


class _FakeBitgetTP:
    def __init__(self) -> None:
        self.tp_sizes: list[float] = []

    def place_take_profit(self, **kwargs):  # noqa: ANN003
        self.tp_sizes.append(float(kwargs.get("size", 0.0)))
        idx = len(self.tp_sizes)
        return OrderAck(order_id=f"tp-{idx}", client_oid=kwargs.get("client_oid"), status="ACKED", raw={})


class _FakeBitgetTPFill(_FakeBitgetTP):
    def get_order_state(
        self,
        symbol: str,
        order_id: str | None = None,
        client_order_id: str | None = None,
        is_plan_order: bool = False,
    ):  # noqa: ARG002
        return {"state": "FILLED", "baseVolume": 333.0, "priceAvg": 0.15}


class _FakeBitgetTPClosedAmbiguous(_FakeBitgetTP):
    def get_order_state(
        self,
        symbol: str,
        order_id: str | None = None,
        client_order_id: str | None = None,
        is_plan_order: bool = False,
    ):  # noqa: ARG002
        return {"state": "FILLED_OR_CLOSED"}


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

    assert bitget.tp_sizes == [0.35, 0.35, 0.3]


def test_reconciler_detects_existing_close_plan_tp(tmp_path) -> None:
    store = SQLiteStore(str(tmp_path / "reconciler_tp_exists.db"))
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
    store.upsert_trade_thread(
        thread_id=42,
        symbol="INXUSDT",
        side="LONG",
        leverage=10,
        stop_loss=0.0865,
        tp_points=[0.15, 0.18, 0.2],
        status="ACTIVE",
    )
    state.set_positions(
        [
            PositionState(
                symbol="INXUSDT",
                side="long",
                size=1000.0,
                entry_price=0.1,
                mark_price=0.1,
                liq_price=0.05,
                pnl=0.0,
                leverage=10,
                margin_mode="crossed",
                timestamp=utc_now(),
                opened_at=utc_now(),
            )
        ]
    )
    state.upsert_order(
        OrderState(
            symbol="INXUSDT",
            side="sell",
            status="NEW",
            filled=0.0,
            quantity=333.0,
            avg_price=None,
            reduce_only=False,
            trade_side="close",
            purpose="close",
            timestamp=utc_now(),
            client_order_id="manual-close-1",
            order_id="manual-close-1",
            trigger_price=0.15,
            is_plan_order=True,
            thread_id=42,
        )
    )

    assert reconciler._has_active_tp("INXUSDT", 42) is True


def test_reconciler_records_filled_tp_progress(tmp_path) -> None:
    store = SQLiteStore(str(tmp_path / "reconciler_tp_progress.db"))
    alerts = AlertManager(Notifier(logging.getLogger("test")), store, logging.getLogger("test"))
    state = StateStore()
    bitget = _FakeBitgetTPFill()
    reconciler = OrderReconciler(
        _config(),
        bitget,
        state,
        store,
        alerts,
        symbol_registry=_FakeSymbolRegistry(size_place=3),
    )
    store.upsert_trade_thread(
        thread_id=77,
        symbol="INXUSDT",
        side="LONG",
        leverage=10,
        stop_loss=0.0865,
        tp_points=[0.15, 0.18, 0.2],
        status="ACTIVE",
    )
    state.set_positions(
        [
            PositionState(
                symbol="INXUSDT",
                side="long",
                size=667.0,
                entry_price=0.1,
                mark_price=0.1,
                liq_price=0.05,
                pnl=0.0,
                leverage=10,
                margin_mode="crossed",
                timestamp=utc_now(),
                opened_at=utc_now(),
            )
        ]
    )
    state.upsert_order(
        OrderState(
            symbol="INXUSDT",
            side="sell",
            status="NEW",
            filled=0.0,
            quantity=333.0,
            avg_price=None,
            reduce_only=True,
            trade_side=None,
            purpose="tp",
            timestamp=utc_now(),
            client_order_id="tp-77-0-1",
            order_id="tp-77-0-1",
            trigger_price=0.15,
            is_plan_order=True,
            thread_id=77,
        )
    )

    asyncio.run(reconciler.reconcile_once())

    thread = store.get_trade_thread(77)
    assert thread is not None
    assert thread["filled_tp_points"] == [0.15]
    assert store.get_remaining_tp_points(77) == [0.18, 0.2]


def test_reconciler_inferrs_ambiguous_tp_closure_as_fill_after_price_cross(tmp_path) -> None:
    store = SQLiteStore(str(tmp_path / "reconciler_tp_ambiguous_fill.db"))
    alerts = AlertManager(Notifier(logging.getLogger("test")), store, logging.getLogger("test"))
    state = StateStore()
    state.set_positions(
        [
            PositionState(
                symbol="INXUSDT",
                side="long",
                size=650.0,
                entry_price=0.1,
                mark_price=0.151,
                liq_price=0.05,
                pnl=0.0,
                leverage=10,
                margin_mode="crossed",
                timestamp=utc_now(),
                opened_at=utc_now(),
            )
        ]
    )
    bitget = _FakeBitgetTPClosedAmbiguous()
    reconciler = OrderReconciler(
        _config(),
        bitget,
        state,
        store,
        alerts,
        symbol_registry=_FakeSymbolRegistry(size_place=3),
    )
    store.upsert_trade_thread(
        thread_id=78,
        symbol="INXUSDT",
        side="LONG",
        leverage=10,
        stop_loss=0.0865,
        tp_points=[0.15, 0.18, 0.2],
        status="ACTIVE",
    )
    state.upsert_order(
        OrderState(
            symbol="INXUSDT",
            side="sell",
            status="ACKED",
            filled=0.0,
            quantity=350.0,
            avg_price=None,
            reduce_only=True,
            trade_side=None,
            purpose="tp",
            timestamp=utc_now(),
            client_order_id="tp-78-0-1",
            order_id="tp-78-0-1",
            trigger_price=0.15,
            is_plan_order=True,
            thread_id=78,
        )
    )

    asyncio.run(reconciler.reconcile_once())

    order = state.find_order(client_order_id="tp-78-0-1")
    assert order is not None
    assert order.status == "FILLED"
    thread = store.get_trade_thread(78)
    assert thread is not None
    assert thread["filled_tp_points"] == [0.15]
    assert store.get_remaining_tp_points(78) == [0.18, 0.2]


def test_reconciler_keeps_ambiguous_tp_closure_canceled_during_manual_reduce_rearm(tmp_path) -> None:
    store = SQLiteStore(str(tmp_path / "reconciler_tp_ambiguous_rearm.db"))
    alerts = AlertManager(Notifier(logging.getLogger("test")), store, logging.getLogger("test"))
    state = StateStore()
    state.set_positions(
        [
            PositionState(
                symbol="INXUSDT",
                side="long",
                size=650.0,
                entry_price=0.1,
                mark_price=0.151,
                liq_price=0.05,
                pnl=0.0,
                leverage=10,
                margin_mode="crossed",
                timestamp=utc_now(),
                opened_at=utc_now(),
            )
        ]
    )
    bitget = _FakeBitgetTPClosedAmbiguous()
    reconciler = OrderReconciler(
        _config(),
        bitget,
        state,
        store,
        alerts,
        symbol_registry=_FakeSymbolRegistry(size_place=3),
    )
    store.upsert_trade_thread(
        thread_id=79,
        symbol="INXUSDT",
        side="LONG",
        leverage=10,
        stop_loss=0.0865,
        tp_points=[0.15, 0.18, 0.2],
        status="ACTIVE",
    )
    store.set_system_flag(f"tp_rearm_required::INXUSDT::{79}", str(utc_now().timestamp()))
    state.upsert_order(
        OrderState(
            symbol="INXUSDT",
            side="sell",
            status="ACKED",
            filled=0.0,
            quantity=350.0,
            avg_price=None,
            reduce_only=True,
            trade_side=None,
            purpose="tp",
            timestamp=utc_now(),
            client_order_id="tp-79-0-1",
            order_id="tp-79-0-1",
            trigger_price=0.15,
            is_plan_order=True,
            thread_id=79,
        )
    )

    asyncio.run(reconciler.reconcile_once())

    order = state.find_order(client_order_id="tp-79-0-1")
    assert order is not None
    assert order.status == "CANCELED"
    thread = store.get_trade_thread(79)
    assert thread is not None
    assert thread["filled_tp_points"] == []
