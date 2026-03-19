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


class _FlakySMTP:
    sent_messages = []
    fail_before_success = 0
    attempts = 0

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
        _FlakySMTP.attempts += 1
        if _FlakySMTP.attempts <= _FlakySMTP.fail_before_success:
            raise RuntimeError("Connection unexpectedly closed")
        _FlakySMTP.sent_messages.append(msg)


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
    body = _FakeSMTP.sent_messages[0].get_content()
    assert "Following 交易风控提醒" in body
    assert "请手动登录服务器核查" in body
    assert '"event_type"' not in body
    row = store.conn.execute("SELECT type FROM events WHERE type='RISK_MODE_DISABLED' ORDER BY id DESC LIMIT 1").fetchone()
    assert row is not None
    assert row["type"] == "RISK_MODE_DISABLED"


def test_high_leverage_email_skips_60x_and_below(monkeypatch, tmp_path) -> None:
    cfg = _config()
    cfg.alerts.email.send_on = ["HIGH_LEVERAGE"]
    store = SQLiteStore(str(tmp_path / "high_leverage_skip.db"))
    notifier = Notifier(logging.getLogger("test"))
    sender = SMTPAlertSender(cfg.alerts.email)
    alerts = AlertManager(notifier=notifier, store=store, logger=logging.getLogger("test"), email_sender=sender)

    _FakeSMTP.sent_messages.clear()
    monkeypatch.setenv("SMTP_PASS", "dummy")
    monkeypatch.setattr("smtplib.SMTP", _FakeSMTP)
    alerts.warn("HIGH_LEVERAGE", "high leverage entry signal received", {"symbol": "BTCUSDT", "leverage": 60})

    assert len(_FakeSMTP.sent_messages) == 0


def test_high_leverage_email_sends_above_60x(monkeypatch, tmp_path) -> None:
    cfg = _config()
    cfg.alerts.email.send_on = ["HIGH_LEVERAGE"]
    store = SQLiteStore(str(tmp_path / "high_leverage_send.db"))
    notifier = Notifier(logging.getLogger("test"))
    sender = SMTPAlertSender(cfg.alerts.email)
    alerts = AlertManager(notifier=notifier, store=store, logger=logging.getLogger("test"), email_sender=sender)

    _FakeSMTP.sent_messages.clear()
    monkeypatch.setenv("SMTP_PASS", "dummy")
    monkeypatch.setattr("smtplib.SMTP", _FakeSMTP)
    alerts.warn("HIGH_LEVERAGE", "high leverage entry signal received", {"symbol": "BTCUSDT", "leverage": 61})

    assert len(_FakeSMTP.sent_messages) == 1


def test_cross_margin_email_is_suppressed(monkeypatch, tmp_path) -> None:
    cfg = _config()
    cfg.alerts.email.send_on = ["CROSS_MARGIN"]
    store = SQLiteStore(str(tmp_path / "cross_margin_suppressed.db"))
    notifier = Notifier(logging.getLogger("test"))
    sender = SMTPAlertSender(cfg.alerts.email)
    alerts = AlertManager(notifier=notifier, store=store, logger=logging.getLogger("test"), email_sender=sender)

    _FakeSMTP.sent_messages.clear()
    monkeypatch.setenv("SMTP_PASS", "dummy")
    monkeypatch.setattr("smtplib.SMTP", _FakeSMTP)
    alerts.warn("CROSS_MARGIN", "cross margin mode enabled for this thread", {"thread_id": 1, "margin_mode": "cross"})

    assert len(_FakeSMTP.sent_messages) == 0


def test_order_submitted_email_is_deduped(monkeypatch, tmp_path) -> None:
    cfg = _config()
    cfg.alerts.email.send_on = ["ORDER_SUBMITTED"]
    cfg.alerts.email.dedupe_seconds = 300
    store = SQLiteStore(str(tmp_path / "order_submitted_dedupe.db"))
    notifier = Notifier(logging.getLogger("test"))
    sender = SMTPAlertSender(cfg.alerts.email)
    alerts = AlertManager(notifier=notifier, store=store, logger=logging.getLogger("test"), email_sender=sender)

    _FakeSMTP.sent_messages.clear()
    monkeypatch.setenv("SMTP_PASS", "dummy")
    monkeypatch.setattr("smtplib.SMTP", _FakeSMTP)
    payload = {"symbol": "MEWUSDT", "purpose": "entry", "thread_id": 10, "client_order_id": "entry-1"}
    alerts.info("ORDER_SUBMITTED", "order submitted to exchange", payload)
    alerts.info("ORDER_SUBMITTED", "order submitted to exchange", payload)

    assert len(_FakeSMTP.sent_messages) == 1


def test_api_error_burst_email_dedupes_when_count_changes(monkeypatch, tmp_path) -> None:
    cfg = _config()
    cfg.alerts.email.send_on = ["API_ERROR_BURST"]
    cfg.alerts.email.dedupe_seconds = 300
    store = SQLiteStore(str(tmp_path / "api_error_burst_dedupe.db"))
    notifier = Notifier(logging.getLogger("test"))
    sender = SMTPAlertSender(cfg.alerts.email)
    alerts = AlertManager(notifier=notifier, store=store, logger=logging.getLogger("test"), email_sender=sender)

    _FakeSMTP.sent_messages.clear()
    monkeypatch.setenv("SMTP_PASS", "dummy")
    monkeypatch.setattr("smtplib.SMTP", _FakeSMTP)
    alerts.error(
        "API_ERROR_BURST",
        "api errors exceeded burst threshold",
        {"purpose": "risk_control", "reason": "api_error_burst", "count": 40, "window_seconds": 120},
    )
    alerts.error(
        "API_ERROR_BURST",
        "api errors exceeded burst threshold",
        {"purpose": "risk_control", "reason": "api_error_burst", "count": 42, "window_seconds": 120},
    )

    assert len(_FakeSMTP.sent_messages) == 1


def test_incident_email_sends_once_then_recovered_then_reopens(monkeypatch, tmp_path) -> None:
    cfg = _config()
    cfg.alerts.email.send_on = ["API_ERROR_BURST"]
    cfg.alerts.email.dedupe_seconds = 300
    store = SQLiteStore(str(tmp_path / "incident_lifecycle.db"))
    notifier = Notifier(logging.getLogger("test"))
    sender = SMTPAlertSender(cfg.alerts.email)
    alerts = AlertManager(notifier=notifier, store=store, logger=logging.getLogger("test"), email_sender=sender)

    _FakeSMTP.sent_messages.clear()
    monkeypatch.setenv("SMTP_PASS", "dummy")
    monkeypatch.setattr("smtplib.SMTP", _FakeSMTP)

    # first trigger: send
    alerts.error(
        "API_ERROR_BURST",
        "api errors exceeded burst threshold",
        {"purpose": "risk_control", "reason": "api_error_burst", "count": 40, "window_seconds": 120},
    )
    # repeated trigger while active: suppress
    alerts.error(
        "API_ERROR_BURST",
        "api errors exceeded burst threshold",
        {"purpose": "risk_control", "reason": "api_error_burst", "count": 42, "window_seconds": 120},
    )
    # recovered: should send once even if not explicitly in send_on
    alerts.info(
        "API_ERROR_BURST_RECOVERED",
        "api error burst recovered below threshold",
        {"purpose": "risk_control", "reason": "api_error_burst_recovered", "count": 2, "window_seconds": 120},
    )
    # duplicate recovered with no active incident: suppress
    alerts.info(
        "API_ERROR_BURST_RECOVERED",
        "api error burst recovered below threshold",
        {"purpose": "risk_control", "reason": "api_error_burst_recovered", "count": 1, "window_seconds": 120},
    )
    # trigger again after recovery: send again
    alerts.error(
        "API_ERROR_BURST",
        "api errors exceeded burst threshold",
        {"purpose": "risk_control", "reason": "api_error_burst", "count": 39, "window_seconds": 120},
    )

    assert len(_FakeSMTP.sent_messages) == 3


def test_email_send_retries_and_succeeds(monkeypatch) -> None:
    cfg = _config()
    cfg.alerts.email.send_on = ["ORDER_SUBMITTED"]
    sender = SMTPAlertSender(cfg.alerts.email)

    _FlakySMTP.sent_messages.clear()
    _FlakySMTP.attempts = 0
    _FlakySMTP.fail_before_success = 2
    monkeypatch.setenv("SMTP_PASS", "dummy")
    monkeypatch.setattr("smtplib.SMTP", _FlakySMTP)
    monkeypatch.setattr("time.sleep", lambda _: None)

    sender.send(
        event_type="ORDER_SUBMITTED",
        level="INFO",
        msg="order submitted to exchange",
        trace_id="t-retry-ok",
        payload={"symbol": "ALICEUSDT", "purpose": "entry"},
    )

    assert _FlakySMTP.attempts == 3
    assert len(_FlakySMTP.sent_messages) == 1


def test_email_send_retries_exhausted(monkeypatch) -> None:
    cfg = _config()
    cfg.alerts.email.send_on = ["ORDER_SUBMITTED"]
    sender = SMTPAlertSender(cfg.alerts.email)

    _FlakySMTP.sent_messages.clear()
    _FlakySMTP.attempts = 0
    _FlakySMTP.fail_before_success = 10
    monkeypatch.setenv("SMTP_PASS", "dummy")
    monkeypatch.setattr("smtplib.SMTP", _FlakySMTP)
    monkeypatch.setattr("time.sleep", lambda _: None)

    try:
        sender.send(
            event_type="ORDER_SUBMITTED",
            level="INFO",
            msg="order submitted to exchange",
            trace_id="t-retry-fail",
            payload={"symbol": "ALICEUSDT", "purpose": "entry"},
        )
        raised = False
    except RuntimeError as exc:
        raised = True
        assert "unexpectedly closed" in str(exc).lower()

    assert raised is True
    assert _FlakySMTP.attempts == 3
    assert len(_FlakySMTP.sent_messages) == 0
