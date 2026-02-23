import logging

from trader.alerts import AlertManager
from trader.config import AppConfig
from trader.models import OrderAck
from trader.notifier import Notifier
from trader.state import OrderState, PositionState, StateStore, utc_now
from trader.stoploss_manager import StopLossManager
from trader.store import SQLiteStore


class FakeBitget:
    def __init__(self) -> None:
        self.cancel_calls = 0
        self.place_calls = 0
        self.last_trigger = None

    def supports_plan_orders(self):
        return True

    def cancel_plan_order(self, **kwargs):
        self.cancel_calls += 1
        return {"ok": True}

    def place_stop_loss(self, **kwargs):
        self.place_calls += 1
        self.last_trigger = float(kwargs.get("trigger_price") or 0)
        return OrderAck(order_id="sl-new", client_oid=kwargs.get("client_oid"), status="ACKED", raw={})


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
                "stoploss": {
                    "sl_order_type": "trigger",
                    "break_even_buffer_pct": 0.0005,
                },
                "assumed_equity_usdt": 1000,
            },
            "logging": {"level": "INFO", "file": "trader.log", "rich": False},
            "monitor": {"enabled": True},
        }
    )


def test_move_stoploss_to_break_even_replaces_existing_order(tmp_path) -> None:
    store = SQLiteStore(str(tmp_path / "be.db"))
    alerts = AlertManager(Notifier(logging.getLogger("test")), store, logging.getLogger("test"))
    state = StateStore()
    bitget = FakeBitget()

    state.upsert_order(
        OrderState(
            symbol="BTCUSDT",
            side="sell",
            status="ACKED",
            filled=0.0,
            quantity=1.0,
            avg_price=None,
            reduce_only=True,
            trade_side=None,
            purpose="sl",
            timestamp=utc_now(),
            client_order_id="sl-old",
            order_id="sl-old-id",
            trigger_price=95.0,
            is_plan_order=True,
        )
    )

    manager = StopLossManager(config=_config(), bitget=bitget, state=state, store=store, alerts=alerts)
    result = manager.move_to_break_even(
        PositionState(
            symbol="BTCUSDT",
            side="long",
            size=1.0,
            entry_price=100.0,
            mark_price=100.0,
            liq_price=50.0,
            pnl=0.0,
            leverage=5,
            margin_mode="isolated",
            timestamp=utc_now(),
            opened_at=utc_now(),
        ),
        buffer_pct=0.0005,
    )

    assert result.ok is True
    assert bitget.cancel_calls == 1
    assert bitget.place_calls == 1
    assert abs((bitget.last_trigger or 0) - 100.05) < 1e-6

    sl_new = state.find_order(order_id="sl-new")
    assert sl_new is not None
    assert abs((sl_new.trigger_price or 0) - 100.05) < 1e-6

    row = store.conn.execute(
        "SELECT action FROM reconciler_actions WHERE action='SL_CANCELLED' ORDER BY id DESC LIMIT 1"
    ).fetchone()
    assert row is not None
