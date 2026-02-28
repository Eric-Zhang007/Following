import logging
from trader.alerts import AlertManager
from trader.config import AppConfig
from trader.email_alert import SMTPAlertSender
from trader.notifier import Notifier
from trader.store import SQLiteStore


class _FakeSMTP:
    sent_messages = []

    def __init__(self, host: str, port: int, timeout: int = 10) -> None:  # noqa: ARG002
        self.host = host
        self.port = port

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):  # noqa: ANN001, ANN201
        return False

    def ehlo(self) -> None:
        return None

    def starttls(self) -> None:
        return None

    def login(self, user: str, pwd: str) -> None:  # noqa: ARG002
        return None

    def send_message(self, msg) -> None:  # noqa: ANN001
        self.sent_messages.append(msg)


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
                "max_leverage": 20,
                "allow_sides": ["LONG", "SHORT"],
                "max_signal_age_seconds": 30,
                "leverage_over_limit_action": "CLAMP",
            },
            "risk": {
                "enabled": False,
                "account_risk_per_trade": 0.003,
                "max_notional_per_trade": 100000,
                "entry_slippage_pct": 1,
                "cooldown_seconds": 0,
                "default_stop_loss_pct": 1,
                "assumed_equity_usdt": 1000,
            },
            "alerts": {
                "email": {
                    "enabled": True,
                    "smtp_host": "smtp.example.com",
                    "smtp_port": 587,
                    "smtp_user": "bot@example.com",
                    "smtp_pass_env": "SMTP_PASS",
                    "from_addr": "bot@example.com",
                    "to_addrs": ["ops@example.com"],
                    "send_on": ["RISK_MODE_DISABLED"],
                }
            },
            "logging": {"level": "INFO", "file": "trader.log", "rich": False},
        }
    )


def test_risk_disabled_emits_email_alert(monkeypatch, tmp_path) -> None:
    cfg = _config()
    store = SQLiteStore(str(tmp_path / "risk_email.db"))
    notifier = Notifier(logging.getLogger("test"))
    sender = SMTPAlertSender(cfg.alerts.email)
    alerts = AlertManager(notifier=notifier, store=store, logger=logging.getLogger("test"), email_sender=sender)

    _FakeSMTP.sent_messages.clear()
    monkeypatch.setenv("SMTP_PASS", "dummy")
    monkeypatch.setattr("smtplib.SMTP", _FakeSMTP)
    alerts.error("RISK_MODE_DISABLED", "risk.enabled=false", {"hard_invariants": cfg.risk.hard_invariants.model_dump()})

    assert len(_FakeSMTP.sent_messages) == 1
    row = store.conn.execute("SELECT type FROM events WHERE type='RISK_MODE_DISABLED' ORDER BY id DESC LIMIT 1").fetchone()
    assert row is not None
    assert row["type"] == "RISK_MODE_DISABLED"
