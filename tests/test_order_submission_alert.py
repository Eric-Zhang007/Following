import json
import logging

from trader.alerts import AlertManager
from trader.config import AppConfig
from trader.executor import TradeExecutor
from trader.models import EntrySignal, EntryType, ParsedKind, Side
from trader.notifier import Notifier
from trader.state import StateStore
from trader.store import SQLiteStore


class FakeBitget:
    def set_leverage(self, symbol: str, leverage: int, hold_side: str | None = None):  # noqa: ARG002
        return {"ok": True}

    def place_order(self, **kwargs):
        oid = kwargs.get("client_oid") or "oid-1"
        return {"orderId": f"ex-{oid}", "clientOid": oid, "state": "new"}


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
            },
            "filters": {
                "symbol_whitelist": ["MEWUSDT"],
                "max_leverage": 50,
                "allow_sides": ["LONG", "SHORT"],
                "max_signal_age_seconds": 30,
                "leverage_over_limit_action": "CLAMP",
            },
            "risk": {
                "account_risk_per_trade": 0.003,
                "max_notional_per_trade": 100000,
                "default_stop_loss_pct": 0.01,
                "assumed_equity_usdt": 1000,
            },
            "execution": {
                "per_trade_margin_usdt": 30,
                "entry_split_ratio": [1],
            },
            "logging": {"level": "INFO", "file": "trader.log", "rich": False},
        }
    )


def test_thread_entry_emits_order_submitted_event(tmp_path) -> None:
    store = SQLiteStore(str(tmp_path / "order_submit.db"))
    state = StateStore()
    alerts = AlertManager(Notifier(logging.getLogger("test")), store, logging.getLogger("test"))
    executor = TradeExecutor(
        config=_config(),
        bitget=FakeBitget(),  # type: ignore[arg-type]
        store=store,
        notifier=Notifier(logging.getLogger("test")),
        logger=logging.getLogger("test"),
        runtime_state=state,
        alerts=alerts,
    )
    signal = EntrySignal(
        kind=ParsedKind.ENTRY_SIGNAL,
        raw_text="MEW long 10x",
        symbol="MEWUSDT",
        quote="USDT",
        side=Side.LONG,
        leverage=10,
        entry_type=EntryType.LIMIT,
        entry_low=0.01,
        entry_high=0.01,
        entry_points=[0.01],
        stop_loss=0.009,
        tp_points=[0.011],
    )

    result = executor.execute_thread_entry(signal, chat_id=1, message_id=1, version=1, thread_id=77)
    assert result["placed"] == 1

    event = store.conn.execute(
        "SELECT payload_json FROM events WHERE type='ORDER_SUBMITTED' ORDER BY id DESC LIMIT 1"
    ).fetchone()
    assert event is not None
    payload = json.loads(str(event["payload_json"]))
    assert payload["symbol"] == "MEWUSDT"
    assert payload["purpose"] == "entry"
    assert payload["thread_id"] == 77
