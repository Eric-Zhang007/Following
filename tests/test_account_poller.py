import asyncio
import json
import logging

from trader.account_poller import AccountPoller
from trader.alerts import AlertManager
from trader.config import AppConfig
from trader.notifier import Notifier
from trader.state import OrderState, PositionState, StateStore, utc_now
from trader.store import SQLiteStore


class FakeBitget:
    def get_account_snapshot(self):
        return {"equity": 1200.0, "available": 900.0, "margin_used": 300.0}

    def get_positions(self):
        return [
            {
                "symbol": "BTCUSDT",
                "total": "0.02",
                "holdSide": "long",
                "openPriceAvg": "100000",
                "markPrice": "101000",
                "liquidationPrice": "90000",
                "unrealizedPL": "20",
                "leverage": "5",
                "marginMode": "isolated",
            }
        ]

    def get_open_orders(self):
        return [
            {
                "symbol": "BTCUSDT",
                "side": "buy",
                "state": "NEW",
                "baseVolume": "0",
                "clientOid": "entry-1",
                "orderId": "1001",
                "reduceOnly": "NO",
            }
        ]

    def list_plan_orders(self):
        return []

    def get_funding_rate(self, symbol: str):
        return 0.0001

    def get_contracts(self):
        return [{"symbol": "BTCUSDT"}]

    def get_history_positions(self, **kwargs):
        return []


class FakeBitgetNoPosition(FakeBitget):
    def get_positions(self):
        return []

    def get_history_positions(self, **kwargs):
        return [
            {
                "symbol": "BTCUSDT",
                "holdSide": "long",
                "netProfit": "18.5",
                "uTime": "1710000000000",
            }
        ]


class FakeBitgetOpenOrdersMetadata(FakeBitget):
    def get_open_orders(self):
        return [
            {
                "symbol": "HUSDT",
                "side": "buy",
                "state": "live",
                "baseVolume": "0",
                "clientOid": "entry-8-1-9de000fa",
                "orderId": "1411972928034729985",
                "reduceOnly": "NO",
                "tradeSide": "open",
            },
            {
                "symbol": "MEWUSDT",
                "side": "buy",
                "state": "live",
                "baseVolume": "0",
                "clientOid": "entry-10-0-2e9f770c",
                "orderId": "1411989540888539137",
                "reduceOnly": "NO",
                "tradeSide": "open",
            },
        ]


class FakeBitgetUnknownThenClear(FakeBitget):
    def __init__(self) -> None:
        self._calls = 0

    def get_positions(self):
        self._calls += 1
        if self._calls == 1:
            return super().get_positions()
        return []


class FakeBitgetNoPositionWithOpenEntries(FakeBitgetNoPosition):
    def __init__(self) -> None:
        self.canceled_spot: list[tuple[str, str]] = []
        self.canceled_plan: list[tuple[str, str | None, str | None]] = []

    def get_open_orders(self):
        return [
            {
                "symbol": "BTCUSDT",
                "side": "buy",
                "state": "live",
                "orderId": "E-1001",
                "clientOid": "manual-entry-1",
                "reduceOnly": "NO",
            },
            {
                "symbol": "BTCUSDT",
                "side": "sell",
                "state": "live",
                "orderId": "TP-1002",
                "clientOid": "tp-88-0-abc",
                "reduceOnly": "YES",
                "tradeSide": "close",
            },
        ]

    def list_plan_orders(self):
        return [
            {
                "symbol": "BTCUSDT",
                "side": "buy",
                "orderId": "P-1003",
                "clientOid": "legacy-plan-entry",
                "planType": "normal_plan",
                "reduceOnly": "NO",
            },
            {
                "symbol": "BTCUSDT",
                "side": "sell",
                "orderId": "SL-1004",
                "clientOid": "sl-88-xyz",
                "planType": "loss_plan",
                "reduceOnly": "YES",
                "tradeSide": "close",
            },
        ]

    def cancel_order(self, symbol: str, order_id: str):
        self.canceled_spot.append((symbol, order_id))
        return {"ok": True}

    def cancel_plan_order(self, *, symbol: str, order_id: str | None = None, client_oid: str | None = None):
        self.canceled_plan.append((symbol, order_id, client_oid))
        return {"ok": True}


def _config() -> AppConfig:
    return AppConfig.model_validate(
        {
            "dry_run": True,
            "listener": {"mode": "web_preview"},
            "telegram": {"session_name": "s", "channel": "@IvanCryptotalk"},
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


def test_account_poller_updates_state_and_snapshots(tmp_path) -> None:
    store = SQLiteStore(str(tmp_path / "monitor.db"))
    alerts = AlertManager(Notifier(logging.getLogger("test")), store, logging.getLogger("test"))
    state = StateStore()
    poller = AccountPoller(_config(), FakeBitget(), state, store, alerts)

    asyncio.run(poller.poll_account())
    asyncio.run(poller.poll_positions())
    asyncio.run(poller.poll_open_orders())

    assert state.account is not None
    assert state.account.equity == 1200.0
    assert "BTCUSDT" in state.positions
    assert state.positions["BTCUSDT"].size == 0.02
    assert state.find_order(client_order_id="entry-1") is not None

    row = store.conn.execute("SELECT equity, margin_used FROM equity_snapshots ORDER BY id DESC LIMIT 1").fetchone()
    assert row is not None
    assert float(row["equity"]) == 1200.0
    assert float(row["margin_used"]) == 300.0


def test_account_poller_emits_position_closed_summary(tmp_path) -> None:
    store = SQLiteStore(str(tmp_path / "monitor_close.db"))
    alerts = AlertManager(Notifier(logging.getLogger("test")), store, logging.getLogger("test"))
    state = StateStore()
    state.set_account(equity=1300.0, available=1100.0, margin_used=200.0)
    state.set_positions(
        [
            PositionState(
                symbol="BTCUSDT",
                side="long",
                size=0.02,
                entry_price=100000.0,
                mark_price=101000.0,
                liq_price=90000.0,
                pnl=20.0,
                leverage=5,
                margin_mode="isolated",
                timestamp=utc_now(),
                opened_at=utc_now(),
            )
        ]
    )
    store.upsert_trade_thread(
        thread_id=88,
        symbol="BTCUSDT",
        side="LONG",
        leverage=10,
        status="ACTIVE",
    )
    store.record_execution(
        chat_id=1,
        message_id=10,
        version=1,
        action_type="ENTRY",
        symbol="BTCUSDT",
        side="LONG",
        status="EXECUTED",
        reason=None,
        intent={"x": 1},
        thread_id=88,
        purpose="entry",
    )
    store.record_execution(
        chat_id=1,
        message_id=11,
        version=1,
        action_type="MANAGE_ADD",
        symbol="BTCUSDT",
        side="buy",
        status="EXECUTED",
        reason=None,
        intent={"x": 1},
        thread_id=88,
        purpose="manage_add",
    )

    poller = AccountPoller(_config(), FakeBitgetNoPosition(), state, store, alerts)
    asyncio.run(poller.poll_positions())

    event = store.conn.execute(
        "SELECT payload_json FROM events WHERE type='POSITION_CLOSED_SUMMARY' ORDER BY id DESC LIMIT 1"
    ).fetchone()
    assert event is not None
    payload = json.loads(str(event["payload_json"]))
    assert payload["symbol"] == "BTCUSDT"
    assert payload["thread_id"] == 88
    assert payload["realized_pnl"] == 18.5
    assert payload["pnl_source"] == "history_position.netProfit"
    assert payload["account_equity"] == 1300.0
    assert payload["add_times"] == 1

    thread = store.get_trade_thread(88)
    assert thread is not None
    assert thread["status"] == "CLOSED"


def test_position_clear_cancels_entry_orders_once(tmp_path) -> None:
    store = SQLiteStore(str(tmp_path / "monitor_close_cancel.db"))
    alerts = AlertManager(Notifier(logging.getLogger("test")), store, logging.getLogger("test"))
    state = StateStore()
    state.set_account(equity=1300.0, available=1100.0, margin_used=200.0)
    state.set_positions(
        [
            PositionState(
                symbol="BTCUSDT",
                side="long",
                size=0.02,
                entry_price=100000.0,
                mark_price=101000.0,
                liq_price=90000.0,
                pnl=20.0,
                leverage=5,
                margin_mode="isolated",
                timestamp=utc_now(),
                opened_at=utc_now(),
            )
        ]
    )
    store.upsert_trade_thread(
        thread_id=88,
        symbol="BTCUSDT",
        side="LONG",
        leverage=10,
        status="ACTIVE",
    )

    bitget = FakeBitgetNoPositionWithOpenEntries()
    poller = AccountPoller(_config(), bitget, state, store, alerts)
    asyncio.run(poller.poll_positions())

    assert bitget.canceled_spot == [("BTCUSDT", "E-1001")]
    assert bitget.canceled_plan == [("BTCUSDT", "P-1003", "legacy-plan-entry")]

    rows = store.conn.execute(
        "SELECT action, order_id FROM reconciler_actions WHERE action='POSITION_CLEAR_CANCEL_ENTRY' ORDER BY id ASC"
    ).fetchall()
    assert len(rows) == 2

    event = store.conn.execute(
        "SELECT payload_json FROM events WHERE type='POSITION_CLEARED' ORDER BY id DESC LIMIT 1"
    ).fetchone()
    assert event is not None
    payload = json.loads(str(event["payload_json"]))
    assert payload["entry_cancel_attempted"] == 2
    assert payload["entry_cancel_succeeded"] == 2
    assert payload["entry_cancel_failed"] == 0


def test_poll_open_orders_preserves_existing_thread_context(tmp_path) -> None:
    store = SQLiteStore(str(tmp_path / "monitor_open_order_merge.db"))
    alerts = AlertManager(Notifier(logging.getLogger("test")), store, logging.getLogger("test"))
    state = StateStore()
    state.upsert_order(
        OrderState(
            symbol="HUSDT",
            side="buy",
            status="ACKED",
            filled=0.0,
            quantity=136.0,
            avg_price=None,
            reduce_only=False,
            trade_side="open",
            purpose="entry",
            timestamp=utc_now(),
            client_order_id="entry-8-1-9de000fa",
            order_id="1411972928034729985",
            thread_id=8,
            entry_index=1,
        )
    )
    store.upsert_trade_thread(
        thread_id=10,
        symbol="MEWUSDT",
        side="LONG",
        leverage=50,
        stop_loss=0.000536,
        status="ACTIVE",
    )

    poller = AccountPoller(_config(), FakeBitgetOpenOrdersMetadata(), state, store, alerts)
    asyncio.run(poller.poll_open_orders())

    h = state.find_order(client_order_id="entry-8-1-9de000fa")
    assert h is not None
    assert h.thread_id == 8
    assert h.entry_index == 1
    assert h.purpose == "entry"

    mew = state.find_order(client_order_id="entry-10-0-2e9f770c")
    assert mew is not None
    assert mew.thread_id == 10
    assert mew.entry_index == 0
    assert mew.purpose == "entry"


def test_unknown_position_alert_emits_once_while_persistent(tmp_path) -> None:
    store = SQLiteStore(str(tmp_path / "monitor_unknown_once.db"))
    alerts = AlertManager(Notifier(logging.getLogger("test")), store, logging.getLogger("test"))
    state = StateStore()
    poller = AccountPoller(_config(), FakeBitget(), state, store, alerts)

    asyncio.run(poller.poll_positions())
    asyncio.run(poller.poll_positions())

    rows = store.conn.execute("SELECT id FROM events WHERE type='UNKNOWN_POSITION' ORDER BY id ASC").fetchall()
    assert len(rows) == 1


def test_unknown_position_recovery_does_not_enable_safe_mode(tmp_path) -> None:
    store = SQLiteStore(str(tmp_path / "monitor_unknown_recovery.db"))
    alerts = AlertManager(Notifier(logging.getLogger("test")), store, logging.getLogger("test"))
    state = StateStore()
    poller = AccountPoller(_config(), FakeBitgetUnknownThenClear(), state, store, alerts)

    asyncio.run(poller.poll_positions())
    assert state.safe_mode is False
    assert state.block_new_entries_reason is None

    asyncio.run(poller.poll_positions())
    assert state.safe_mode is False
    assert state.block_new_entries_reason is None

    recovered = store.conn.execute(
        "SELECT id FROM events WHERE type='UNKNOWN_POSITION_RECOVERED' ORDER BY id DESC LIMIT 1"
    ).fetchone()
    released = store.conn.execute(
        "SELECT id FROM events WHERE type='UNKNOWN_POSITION_SAFE_MODE_RELEASED' ORDER BY id DESC LIMIT 1"
    ).fetchone()
    assert recovered is not None
    assert released is None


def test_infer_purpose_prefers_tp_client_oid_prefix() -> None:
    row = {
        "clientOid": "tp-13-2-1772417536",
        "planType": "normal_plan",
        "reduceOnly": "YES",
        "tradeSide": "close",
    }
    assert AccountPoller._infer_purpose(row) == "tp"


def test_infer_purpose_uses_normal_plan_preset_fields() -> None:
    tp_row = {
        "planType": "normal_plan",
        "stopSurplusTriggerPrice": "0.12",
        "reduceOnly": "YES",
        "tradeSide": "close",
    }
    sl_row = {
        "planType": "normal_plan",
        "stopLossTriggerPrice": "0.08",
        "reduceOnly": "YES",
        "tradeSide": "close",
    }
    assert AccountPoller._infer_purpose(tp_row) == "tp"
    assert AccountPoller._infer_purpose(sl_row) == "sl"


def test_infer_purpose_close_order_defaults_to_close() -> None:
    row = {
        "planType": "normal_plan",
        "reduceOnly": "NO",
        "tradeSide": "close",
    }
    assert AccountPoller._infer_purpose(row) == "close"
