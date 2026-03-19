import logging
from datetime import timedelta

from trader.config import AppConfig
from trader.health_server import HealthServer
from trader.state import StateStore, utc_now


def _config(required_symbols: list[str] | None = None) -> AppConfig:
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
                "symbol_whitelist": ["BTCUSDT", "ETHUSDT"],
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
                "stoploss": {"sl_order_type": "trigger"},
                "assumed_equity_usdt": 1000,
            },
            "monitor": {
                "enabled": True,
                "price_feed": {
                    "mode": "ws",
                    "interval_seconds": 2,
                    "max_stale_seconds": 5,
                    "max_ws_parse_error_ratio": 0.2,
                    "required_symbols": required_symbols or [],
                },
            },
            "logging": {"level": "INFO", "file": "trader.log", "rich": False},
        }
    )


def _make_ready_state() -> StateStore:
    state = StateStore()
    now = utc_now()
    state.set_account(equity=1000, available=900, margin_used=100, timestamp=now)
    state.set_positions([], timestamp=now)
    state.last_orders_ok_at = now
    state.set_price_feed_mode("ws", degraded=False)
    state.set_price_fresh(now)
    return state


def test_readyz_fails_when_ws_parse_error_ratio_too_high() -> None:
    cfg = _config()
    state = _make_ready_state()
    server = HealthServer(cfg, state)

    for _ in range(10):
        state.register_ws_message()
    for _ in range(3):
        state.register_ws_parse_error("bad")

    payload = server._ready_payload()
    assert payload["ready"] is False
    assert payload["checks"]["ws_parse_error_ratio"] is False
    assert payload["ws_parse_error_ratio"] > 0.2


def test_readyz_fails_when_required_symbol_is_stale() -> None:
    cfg = _config(required_symbols=["BTCUSDT", "ETHUSDT"])
    state = _make_ready_state()
    server = HealthServer(cfg, state)

    now = utc_now()
    state.set_symbol_price_fresh("BTCUSDT", timestamp=now)
    state.set_symbol_price_fresh("ETHUSDT", timestamp=now - timedelta(seconds=20))

    payload = server._ready_payload()
    assert payload["ready"] is False
    assert payload["checks"]["required_symbols_fresh"] is False
    assert any("required_symbols_stale" in reason for reason in payload["reasons"])
