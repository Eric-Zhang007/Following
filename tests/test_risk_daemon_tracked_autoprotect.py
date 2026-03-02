import asyncio
import logging

from trader.alerts import AlertManager
from trader.config import AppConfig
from trader.kill_switch import KillSwitch
from trader.models import OrderAck
from trader.notifier import Notifier
from trader.risk_daemon import RiskDaemon
from trader.state import OrderState, PositionState, StateStore, utc_now
from trader.store import SQLiteStore


class FakeBitgetTrackedProtect:
    def __init__(self) -> None:
        self.sl_calls = 0
        self.tp_calls = 0
        self.tp_sizes: list[float] = []

    def supports_plan_orders(self):
        return True

    def place_stop_loss(self, **kwargs):  # noqa: ANN003
        self.sl_calls += 1
        return OrderAck(order_id="sl-1", client_oid=kwargs.get("client_oid"), status="ACKED", raw={})

    def place_take_profit(self, **kwargs):  # noqa: ANN003
        self.tp_calls += 1
        self.tp_sizes.append(float(kwargs.get("size", 0.0)))
        idx = self.tp_calls
        return OrderAck(order_id=f"tp-{idx}", client_oid=kwargs.get("client_oid"), status="ACKED", raw={})

    def protective_close_position(self, symbol: str, side: str, size: float):  # noqa: ARG002
        return {"ok": True}

    def place_order(self, **kwargs):  # noqa: ANN003
        return {"ok": True}


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
                "hard_stop_loss_required": True,
                "stoploss": {
                    "must_exist": True,
                    "sl_order_type": "trigger",
                    "trigger_price_type": "mark",
                    "max_time_without_sl_seconds": 10,
                    "emergency_close_if_sl_place_fails": True,
                },
            },
            "execution": {
                "close_on_invariant_violation": False,
                "place_tp_on_fill": True,
            },
            "logging": {"level": "INFO", "file": "trader.log", "rich": False},
            "monitor": {"enabled": True},
        }
    )


def test_tracked_position_autoprotect_in_report_only_mode(tmp_path) -> None:
    store = SQLiteStore(str(tmp_path / "tracked_autoprotect.db"))
    alerts = AlertManager(Notifier(logging.getLogger("test")), store, logging.getLogger("test"))
    state = StateStore()
    state.set_account(equity=1000, available=900, margin_used=100)
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
                margin_mode="isolated",
                timestamp=utc_now(),
                opened_at=utc_now(),
                unknown_origin=False,
            )
        ]
    )
    store.upsert_trade_thread(
        thread_id=14,
        symbol="INXUSDT",
        side="LONG",
        leverage=10,
        stop_loss=0.0865,
        tp_points=[0.15, 0.18, 0.2],
        status="ACTIVE",
    )

    bitget = FakeBitgetTrackedProtect()
    daemon = RiskDaemon(
        config=_config(),
        bitget=bitget,
        state=state,
        store=store,
        alerts=alerts,
        kill_switch=KillSwitch(store=store, file_path=str(tmp_path / "NO_SWITCH")),
    )
    asyncio.run(daemon.tick_once())

    assert bitget.sl_calls == 1
    assert bitget.tp_calls == 3
    assert state.has_valid_stop_loss("INXUSDT", "long") is True

    tp_event = store.conn.execute("SELECT id FROM events WHERE type='TP_SUBMITTED' ORDER BY id DESC LIMIT 1").fetchone()
    assert tp_event is not None


class _FakeContract:
    def __init__(self, size_place: int, min_trade_num: float = 0.0) -> None:
        self.size_place = size_place
        self.min_trade_num = min_trade_num


class _FakeSymbolRegistry:
    def __init__(self, size_place: int, min_trade_num: float = 0.0) -> None:
        self._contract = _FakeContract(size_place=size_place, min_trade_num=min_trade_num)

    def get_contract(self, symbol: str):  # noqa: ANN001
        return self._contract


def test_tracked_position_autoprotect_last_tp_consumes_remainder(tmp_path) -> None:
    store = SQLiteStore(str(tmp_path / "tracked_autoprotect_sizes.db"))
    alerts = AlertManager(Notifier(logging.getLogger("test")), store, logging.getLogger("test"))
    state = StateStore()
    state.set_account(equity=1000, available=900, margin_used=100)
    state.set_positions(
        [
            PositionState(
                symbol="INXUSDT",
                side="long",
                size=1.0,
                entry_price=0.1,
                mark_price=0.1,
                liq_price=0.05,
                pnl=0.0,
                leverage=10,
                margin_mode="isolated",
                timestamp=utc_now(),
                opened_at=utc_now(),
                unknown_origin=False,
            )
        ]
    )
    store.upsert_trade_thread(
        thread_id=15,
        symbol="INXUSDT",
        side="LONG",
        leverage=10,
        stop_loss=0.0865,
        tp_points=[0.15, 0.18, 0.2],
        status="ACTIVE",
    )
    bitget = FakeBitgetTrackedProtect()
    daemon = RiskDaemon(
        config=_config(),
        bitget=bitget,
        state=state,
        store=store,
        alerts=alerts,
        kill_switch=KillSwitch(store=store, file_path=str(tmp_path / "NO_SWITCH")),
        symbol_registry=_FakeSymbolRegistry(size_place=3),
    )
    asyncio.run(daemon.tick_once())

    assert bitget.tp_calls == 3
    assert bitget.tp_sizes == [0.333, 0.333, 0.334]


def test_tracked_position_autoprotect_skips_when_manual_tp_exists(tmp_path) -> None:
    store = SQLiteStore(str(tmp_path / "tracked_autoprotect_manual_tp.db"))
    alerts = AlertManager(Notifier(logging.getLogger("test")), store, logging.getLogger("test"))
    state = StateStore()
    state.set_account(equity=1000, available=900, margin_used=100)
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
                margin_mode="isolated",
                timestamp=utc_now(),
                opened_at=utc_now(),
                unknown_origin=False,
            )
        ]
    )
    store.upsert_trade_thread(
        thread_id=16,
        symbol="INXUSDT",
        side="LONG",
        leverage=10,
        stop_loss=0.0865,
        tp_points=[0.15, 0.18, 0.2],
        status="ACTIVE",
    )
    # Manual SL already exists.
    state.upsert_order(
        OrderState(
            symbol="INXUSDT",
            side="sell",
            status="NEW",
            filled=0.0,
            quantity=1000.0,
            avg_price=None,
            reduce_only=True,
            trade_side="close",
            purpose="sl",
            timestamp=utc_now(),
            client_order_id="manual-sl-1",
            order_id="manual-sl-1",
            trigger_price=0.0865,
            is_plan_order=True,
        )
    )
    # Manual TP detected by client order id prefix.
    state.upsert_order(
        OrderState(
            symbol="INXUSDT",
            side="sell",
            status="NEW",
            filled=0.0,
            quantity=500.0,
            avg_price=None,
            reduce_only=True,
            trade_side="close",
            purpose="tp",
            timestamp=utc_now(),
            client_order_id="tp-manual-1",
            order_id="tp-manual-1",
            trigger_price=0.15,
            is_plan_order=True,
        )
    )

    bitget = FakeBitgetTrackedProtect()
    daemon = RiskDaemon(
        config=_config(),
        bitget=bitget,
        state=state,
        store=store,
        alerts=alerts,
        kill_switch=KillSwitch(store=store, file_path=str(tmp_path / "NO_SWITCH")),
    )
    asyncio.run(daemon.tick_once())

    assert bitget.sl_calls == 0
    assert bitget.tp_calls == 0


def test_tracked_position_autoprotect_sl_above_entry_does_not_block_tp(tmp_path) -> None:
    store = SQLiteStore(str(tmp_path / "tracked_autoprotect_sl_above_entry.db"))
    alerts = AlertManager(Notifier(logging.getLogger("test")), store, logging.getLogger("test"))
    state = StateStore()
    state.set_account(equity=1000, available=900, margin_used=100)
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
                margin_mode="isolated",
                timestamp=utc_now(),
                opened_at=utc_now(),
                unknown_origin=False,
            )
        ]
    )
    store.upsert_trade_thread(
        thread_id=17,
        symbol="INXUSDT",
        side="LONG",
        leverage=10,
        stop_loss=0.0865,
        tp_points=[0.15, 0.18, 0.2],
        status="ACTIVE",
    )
    # Some SLs are moved above entry (break-even/profit-protect); this must not be treated as TP.
    state.upsert_order(
        OrderState(
            symbol="INXUSDT",
            side="sell",
            status="NEW",
            filled=0.0,
            quantity=1000.0,
            avg_price=None,
            reduce_only=True,
            trade_side="close",
            purpose="sl",
            timestamp=utc_now(),
            client_order_id="sl-manual-2",
            order_id="sl-manual-2",
            trigger_price=0.11,
            is_plan_order=True,
        )
    )

    bitget = FakeBitgetTrackedProtect()
    daemon = RiskDaemon(
        config=_config(),
        bitget=bitget,
        state=state,
        store=store,
        alerts=alerts,
        kill_switch=KillSwitch(store=store, file_path=str(tmp_path / "NO_SWITCH")),
    )
    asyncio.run(daemon.tick_once())

    assert bitget.tp_calls == 3
