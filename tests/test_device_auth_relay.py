from __future__ import annotations

import email
import logging
from datetime import datetime, timezone

from trader.alerts import AlertManager
from trader.config import DeviceAuthRelayConfig, EmailAlertConfig
from trader.device_auth_relay import DeviceAuthRelay
from trader.models import TelegramEvent
from trader.notifier import Notifier
from trader.store import SQLiteStore


class _FakeSMTP:
    sent_messages = []

    def __init__(self, host: str, port: int, timeout: int = 10) -> None:  # noqa: D401
        self.host = host
        self.port = port
        self.timeout = timeout

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> None:  # noqa: ANN001
        return None

    def ehlo(self) -> None:
        return None

    def starttls(self) -> None:
        return None

    def login(self, user: str, password: str) -> None:
        self.user = user
        self.password = password

    def send_message(self, msg) -> None:  # noqa: ANN001
        self.__class__.sent_messages.append(msg)


class _FakeIMAP:
    def __init__(self, host: str, port: int) -> None:
        self.host = host
        self.port = port
        self.messages = {
            b"1": _mail_bytes(
                subject="授权新设备",
                date="Fri, 14 Mar 2026 00:01:00 +0000",
                from_addr="noreply@bitget.com",
            ),
            b"2": _mail_bytes(
                subject="授权新设备",
                date="Fri, 14 Mar 2026 00:03:00 +0000",
                from_addr="noreply@example.com",
            ),
            b"3": _mail_bytes(
                subject="授权新设备",
                date="Fri, 14 Mar 2026 00:02:30 +0000",
                from_addr="support@send007.mail.bitget.com",
            ),
        }

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> None:  # noqa: ANN001
        return None

    def login(self, user: str, password: str) -> tuple[str, list[bytes]]:
        return "OK", [b"logged"]

    def select(self, mailbox: str, readonly: bool = True) -> tuple[str, list[bytes]]:  # noqa: ARG002
        return "OK", [b"3"]

    def search(self, charset, *criteria):  # noqa: ANN001, ARG002
        return "OK", [b"1 2 3"]

    def fetch(self, message_id: bytes, parts: str):  # noqa: ARG002
        return "OK", [(b"RFC822", self.messages[message_id])]


def _mail_bytes(*, subject: str, date: str, from_addr: str, received_domain: str = "mx.qq.com") -> bytes:
    msg = email.message.EmailMessage()
    msg["Subject"] = subject
    msg["Date"] = date
    msg["Received"] = f"from {received_domain} (unknown [{received_domain}]) by mx.qq.com"
    msg["From"] = from_addr
    msg["To"] = "1145106531@qq.com"
    msg.set_content("bitget auth")
    return msg.as_bytes()


def _relay(tmp_path) -> DeviceAuthRelay:
    store = SQLiteStore(str(tmp_path / "device_auth_relay.db"))
    alerts = AlertManager(Notifier(logging.getLogger("test")), store, logging.getLogger("test"))
    relay_cfg = DeviceAuthRelayConfig(enabled=True)
    email_cfg = EmailAlertConfig(
        enabled=True,
        smtp_host="smtp.qq.com",
        smtp_port=587,
        smtp_user="1145106531@qq.com",
        smtp_pass_env="SMTP_PASS",
        from_addr="1145106531@qq.com",
        to_addrs=["ops@example.com"],
    )
    return DeviceAuthRelay(relay_cfg, email_cfg, alerts, logging.getLogger("test"))


def test_device_auth_relay_matches_control_message(tmp_path) -> None:
    relay = _relay(tmp_path)
    event = TelegramEvent(
        chat_id=1,
        message_id=2,
        date=datetime(2026, 3, 14, 0, 2, 0, tzinfo=timezone.utc),
        text="14865424",
        raw_text="14865424",
        sender_username="@aa3845226",
    )

    assert relay._matches_trigger(event) is True


def test_device_auth_relay_picks_nearest_bitget_mail_and_forwards(monkeypatch, tmp_path) -> None:
    relay = _relay(tmp_path)
    _FakeSMTP.sent_messages = []
    monkeypatch.setenv("SMTP_PASS", "secret")
    monkeypatch.setattr("imaplib.IMAP4_SSL", _FakeIMAP)
    monkeypatch.setattr("smtplib.SMTP", _FakeSMTP)

    outcome = relay._process_trigger(
        TelegramEvent(
            chat_id=1,
            message_id=2,
            date=datetime(2026, 3, 14, 0, 2, 0, tzinfo=timezone.utc),
            text="14865424",
            raw_text="14865424",
            sender_username="@aa3845226",
        )
    )

    assert outcome.status == "FORWARDED"
    assert outcome.matched_server == "send007.mail.bitget.com"
    assert len(_FakeSMTP.sent_messages) == 1
    sent = _FakeSMTP.sent_messages[0]
    assert sent["To"] == "yizhikai2023@163.com"
    assert sent["Subject"] == "Fwd: 授权新设备"


def test_device_auth_relay_ignores_non_matching_sender_domain(monkeypatch, tmp_path) -> None:
    relay = _relay(tmp_path)
    monkeypatch.setenv("SMTP_PASS", "secret")

    class _OnlyWrongServerIMAP(_FakeIMAP):
        def __init__(self, host: str, port: int) -> None:
            super().__init__(host, port)
            self.messages = {
                b"1": _mail_bytes(
                    subject="授权新设备",
                    date="Fri, 14 Mar 2026 00:02:00 +0000",
                    from_addr="noreply@example.com",
                )
            }

        def search(self, charset, *criteria):  # noqa: ANN001, ARG002
            return "OK", [b"1"]

    monkeypatch.setattr("imaplib.IMAP4_SSL", _OnlyWrongServerIMAP)
    monkeypatch.setattr("smtplib.SMTP", _FakeSMTP)

    outcome = relay._process_trigger(
        TelegramEvent(
            chat_id=1,
            message_id=2,
            date=datetime(2026, 3, 14, 0, 2, 0, tzinfo=timezone.utc),
            text="14865424",
            raw_text="14865424",
            sender_username="@aa3845226",
        )
    )

    assert outcome.status == "NOT_FOUND"
