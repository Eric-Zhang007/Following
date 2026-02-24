import logging

from trader.alerts import AlertManager
from trader.bitget_client import BitgetClient
from trader.config import AppConfig, BitgetConfig
from trader.notifier import Notifier
from trader.state import StateStore
from trader.startup_probe import probe_plan_order_capability_on_startup
from trader.store import SQLiteStore


def _bitget_config() -> BitgetConfig:
    return BitgetConfig(
        base_url="https://api.bitget.com",
        api_key="k",
        api_secret="s",
        passphrase="p",
        product_type="USDT-FUTURES",
        plan_orders_capability_ttl_seconds=300,
        plan_orders_probe_timeout_seconds=6,
    )


def test_supports_plan_orders_uses_ttl_cache() -> None:
    client = BitgetClient(_bitget_config())
    calls = {"count": 0}

    def fake_request(method, path, params=None, body=None, auth=False, timeout_override=None):
        calls["count"] += 1
        return []

    client._request = fake_request  # type: ignore[method-assign]

    assert client.supports_plan_orders() is True
    assert client.supports_plan_orders() is True
    assert calls["count"] == 1


def test_probe_plan_orders_capability_handles_not_found_and_network_errors() -> None:
    client_404 = BitgetClient(_bitget_config())

    def fake_404(method, path, params=None, body=None, auth=False, timeout_override=None):
        raise RuntimeError("Bitget HTTP 404: not found")

    client_404._request = fake_404  # type: ignore[method-assign]
    state_404 = client_404.probe_plan_orders_capability(force=True)
    assert state_404["supported"] is False
    assert state_404["reason"] == "endpoint_not_found"

    client_net = BitgetClient(_bitget_config())

    def fake_net(method, path, params=None, body=None, auth=False, timeout_override=None):
        raise RuntimeError("request timed out")

    client_net._request = fake_net  # type: ignore[method-assign]
    state_net = client_net.probe_plan_orders_capability(force=True)
    assert state_net["supported"] is None
    assert state_net["reason"] == "network_error"
    assert (state_net["expires_at"] - state_net["ts"]) <= 30


def test_startup_probe_fallbacks_to_local_guard_and_safe_mode(tmp_path) -> None:
    cfg = AppConfig.model_validate(
        {
            "dry_run": True,
            "listener": {"mode": "web_preview"},
            "telegram": {"session_name": "s", "channel": "@IvanCryptotalk"},
            "bitget": {
                "base_url": "https://api.bitget.com",
                "api_key": "k",
                "api_secret": "s",
                "passphrase": "p",
                "product_type": "USDT-FUTURES",
                "plan_orders_probe_on_startup": True,
                "plan_orders_probe_safe_mode_on_failure": True,
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
                "stoploss": {"sl_order_type": "trigger"},
                "assumed_equity_usdt": 1000,
            },
            "logging": {"level": "INFO", "file": "trader.log", "rich": False},
            "monitor": {"enabled": True},
        }
    )
    store = SQLiteStore(str(tmp_path / "probe.db"))
    alerts = AlertManager(Notifier(logging.getLogger("test")), store, logging.getLogger("test"))
    runtime_state = StateStore()
    client = BitgetClient(cfg.bitget)

    client.probe_plan_orders_capability = lambda force=True: {  # type: ignore[method-assign]
        "supported": False,
        "reason": "endpoint_not_found",
        "ts": 1,
        "expires_at": 301,
    }

    probe_plan_order_capability_on_startup(
        config=cfg,
        bitget=client,
        alerts=alerts,
        runtime_state=runtime_state,
    )

    assert cfg.risk.stoploss.sl_order_type == "local_guard"
    assert runtime_state.safe_mode is True
    row = store.conn.execute(
        "SELECT type FROM events WHERE type='PLAN_ORDER_FALLBACK' ORDER BY id DESC LIMIT 1"
    ).fetchone()
    assert row is not None
