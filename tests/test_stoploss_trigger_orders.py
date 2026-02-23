import logging

from trader.alerts import AlertManager
from trader.config import AppConfig
from trader.executor import TradeExecutor
from trader.models import EntrySignal, EntryType, OrderAck, ParsedKind, RiskDecision, Side
from trader.notifier import Notifier
from trader.state import StateStore
from trader.stoploss_manager import StopLossManager
from trader.store import SQLiteStore


class FakeBitget:
    def __init__(self) -> None:
        self.stoploss_calls = 0

    def supports_plan_orders(self):
        return True

    def place_order(self, **kwargs):
        return {"orderId": "entry-001", "clientOid": kwargs.get("client_oid")}

    def place_stop_loss(self, **kwargs):
        self.stoploss_calls += 1
        return OrderAck(order_id="sl-001", client_oid=kwargs.get("client_oid"), status="ACKED", raw={"ok": True})


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
                "hard_stop_loss_required": True,
                "stoploss": {
                    "must_exist": True,
                    "sl_order_type": "trigger",
                },
                "assumed_equity_usdt": 1000,
            },
            "execution": {
                "require_order_ack": False,
                "ack_timeout_seconds": 2,
            },
            "logging": {"level": "INFO", "file": "trader.log", "rich": False},
            "monitor": {"enabled": True},
        }
    )


def test_entry_fill_places_trigger_stoploss_order(tmp_path) -> None:
    store = SQLiteStore(str(tmp_path / "executor.db"))
    notifier = Notifier(logging.getLogger("test"))
    alerts = AlertManager(notifier, store, logging.getLogger("test"))
    state = StateStore()
    bitget = FakeBitget()

    stoploss_manager = StopLossManager(
        config=_config(),
        bitget=bitget,
        state=state,
        store=store,
        alerts=alerts,
    )
    executor = TradeExecutor(
        config=_config(),
        bitget=bitget,
        store=store,
        notifier=notifier,
        logger=logging.getLogger("test"),
        runtime_state=state,
        stoploss_manager=stoploss_manager,
    )

    signal = EntrySignal(
        kind=ParsedKind.ENTRY_SIGNAL,
        raw_text="test",
        symbol="BTCUSDT",
        quote="USDT",
        side=Side.LONG,
        leverage=None,
        entry_type=EntryType.MARKET,
        entry_low=100.0,
        entry_high=100.0,
        stop_loss=99.0,
        take_profit=[],
    )
    decision = RiskDecision(
        approved=True,
        symbol="BTCUSDT",
        side=Side.LONG,
        quantity=1.0,
        entry_price=100.0,
        stop_loss_price=99.0,
    )

    executor.execute_entry(signal, decision, chat_id=1, message_id=1, version=1)

    assert bitget.stoploss_calls == 1
    sl_order = next((o for o in state.orders_by_client_id.values() if o.purpose == "sl"), None)
    assert sl_order is not None
    assert sl_order.is_plan_order is True

    row = store.conn.execute(
        "SELECT action FROM reconciler_actions WHERE action='SL_TRIGGER_SUBMITTED' ORDER BY id DESC LIMIT 1"
    ).fetchone()
    assert row is not None
