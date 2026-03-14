from __future__ import annotations

import asyncio
import imaplib
import logging
import os
import re
import smtplib
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from email import policy
from email.header import decode_header
from email.message import EmailMessage
from email.parser import BytesParser
from email.utils import getaddresses
from email.utils import parsedate_to_datetime

from trader.alerts import AlertManager
from trader.config import DeviceAuthRelayConfig, EmailAlertConfig
from trader.models import TelegramEvent

_DOMAIN_RE = re.compile(r"([A-Za-z0-9.-]+\.[A-Za-z]{2,})")


@dataclass
class RelayOutcome:
    status: str
    detail: str
    subject: str | None = None
    mail_date: str | None = None
    matched_server: str | None = None


@dataclass
class _MatchedMail:
    raw_bytes: bytes
    subject: str
    mail_date: datetime
    matched_server: str


class DeviceAuthRelay:
    def __init__(
        self,
        config: DeviceAuthRelayConfig,
        email_config: EmailAlertConfig,
        alerts: AlertManager,
        logger: logging.Logger,
    ) -> None:
        self.config = config
        self.email_config = email_config
        self.alerts = alerts
        self.logger = logger

    async def maybe_handle(self, event: TelegramEvent) -> bool:
        if not self._matches_trigger(event):
            return False
        try:
            outcome = await asyncio.to_thread(self._process_trigger, event)
        except Exception as exc:  # noqa: BLE001
            self.logger.exception("device auth relay failed")
            self.alerts.error(
                "DEVICE_AUTH_RELAY_FAILED",
                "device auth relay failed",
                {
                    "message_id": event.message_id,
                    "chat_id": event.chat_id,
                    "reason": str(exc),
                    "sender_username": event.sender_username,
                },
            )
            return True

        payload = {
            "message_id": event.message_id,
            "chat_id": event.chat_id,
            "sender_username": event.sender_username,
            "subject": outcome.subject,
            "mail_date": outcome.mail_date,
            "matched_server": outcome.matched_server,
            "reason": outcome.detail,
        }
        if outcome.status == "FORWARDED":
            self.alerts.info("DEVICE_AUTH_RELAY_FORWARDED", "device auth email forwarded", payload)
        elif outcome.status == "NOT_FOUND":
            self.alerts.warn("DEVICE_AUTH_RELAY_NOT_FOUND", "device auth email not found", payload)
        else:
            self.alerts.error("DEVICE_AUTH_RELAY_FAILED", "device auth relay failed", payload)
        return True

    def _matches_trigger(self, event: TelegramEvent) -> bool:
        if not self.config.enabled:
            return False
        text = str(event.raw_text or event.text or "").strip()
        if text != str(self.config.trigger_text or "").strip():
            return False
        sender_username = self._normalize_username(event.sender_username)
        if sender_username is None:
            return False
        allowed = {self._normalize_username(v) for v in self.config.trigger_usernames}
        return sender_username in allowed

    def _process_trigger(self, event: TelegramEvent) -> RelayOutcome:
        matched = self._find_nearest_mail(trigger_at=event.date)
        if matched is None:
            return RelayOutcome(status="NOT_FOUND", detail="no matching mail found")
        self._forward_mail(matched)
        return RelayOutcome(
            status="FORWARDED",
            detail="forwarded",
            subject=matched.subject,
            mail_date=matched.mail_date.isoformat(),
            matched_server=matched.matched_server,
        )

    def _find_nearest_mail(self, *, trigger_at: datetime) -> _MatchedMail | None:
        imap_host = self._imap_host()
        smtp_user = str(self.email_config.smtp_user or "").strip()
        password = os.getenv(self.email_config.smtp_pass_env, "")
        if not imap_host or not smtp_user or not password:
            raise RuntimeError("device auth relay missing IMAP credentials")

        if trigger_at.tzinfo is None:
            trigger_at = trigger_at.replace(tzinfo=timezone.utc)
        since = (trigger_at - timedelta(hours=int(self.config.search_lookback_hours))).astimezone(timezone.utc)

        with imaplib.IMAP4_SSL(imap_host, int(self.config.imap_port)) as client:
            client.login(smtp_user, password)
            status, _ = client.select(self.config.mailbox, readonly=True)
            if status != "OK":
                raise RuntimeError(f"imap select failed: {status}")
            status, data = client.search(None, "SINCE", since.strftime("%d-%b-%Y"))
            if status != "OK":
                raise RuntimeError(f"imap search failed: {status}")
            message_ids = [item for item in (data[0] or b"").split() if item][-100:]
            best: _MatchedMail | None = None
            best_delta: float | None = None
            for message_id in message_ids:
                raw_bytes = self._fetch_rfc822(client, message_id)
                if raw_bytes is None:
                    continue
                message = BytesParser(policy=policy.default).parsebytes(raw_bytes)
                subject = self._decode_subject(message)
                if not subject or str(self.config.mail_subject or "").strip() not in subject:
                    continue
                matched_server = self._match_sender_domain(message)
                if matched_server is None:
                    continue
                mail_date = self._extract_mail_date(message)
                if mail_date is None:
                    continue
                delta = abs((mail_date - trigger_at).total_seconds())
                if best_delta is None or delta < best_delta:
                    best = _MatchedMail(
                        raw_bytes=raw_bytes,
                        subject=subject,
                        mail_date=mail_date,
                        matched_server=matched_server,
                    )
                    best_delta = delta
            return best

    def _forward_mail(self, mail: _MatchedMail) -> None:
        smtp_host = str(self.email_config.smtp_host or "").strip()
        smtp_user = str(self.email_config.smtp_user or "").strip()
        password = os.getenv(self.email_config.smtp_pass_env, "")
        if not smtp_host or not smtp_user or not password:
            raise RuntimeError("device auth relay missing SMTP credentials")

        outgoing = EmailMessage()
        outgoing["Subject"] = f"Fwd: {mail.subject}"
        outgoing["From"] = self.email_config.from_addr or smtp_user
        outgoing["To"] = ", ".join(self.config.forward_to_addrs)
        outgoing.set_content(
            "\n".join(
                [
                    "自动转发的 Bitget 新设备授权邮件。",
                    f"原标题：{mail.subject}",
                    f"邮件时间：{mail.mail_date.isoformat()}",
                    f"命中邮件服务器：{mail.matched_server}",
                    "",
                    "原始邮件作为附件附上。",
                ]
            )
        )
        outgoing.add_attachment(
            mail.raw_bytes,
            maintype="message",
            subtype="rfc822",
            filename="bitget-device-auth.eml",
        )

        with smtplib.SMTP(smtp_host, int(self.email_config.smtp_port), timeout=10) as smtp:
            smtp.ehlo()
            try:
                smtp.starttls()
                smtp.ehlo()
            except Exception:
                pass
            smtp.login(smtp_user, password)
            smtp.send_message(outgoing)

    @staticmethod
    def _fetch_rfc822(client: imaplib.IMAP4_SSL, message_id: bytes) -> bytes | None:
        status, payload = client.fetch(message_id, "(RFC822)")
        if status != "OK":
            return None
        for item in payload:
            if isinstance(item, tuple) and len(item) >= 2 and isinstance(item[1], bytes):
                return item[1]
        return None

    @staticmethod
    def _decode_subject(message) -> str:
        raw_subject = str(message.get("Subject", "") or "")
        parts: list[str] = []
        for chunk, encoding in decode_header(raw_subject):
            if isinstance(chunk, bytes):
                parts.append(chunk.decode(encoding or "utf-8", errors="replace"))
            else:
                parts.append(str(chunk))
        return "".join(parts).strip()

    @staticmethod
    def _extract_mail_date(message) -> datetime | None:
        raw_date = message.get("Date")
        if not raw_date:
            return None
        try:
            value = parsedate_to_datetime(raw_date)
        except Exception:  # noqa: BLE001
            return None
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    def _match_sender_domain(self, message) -> str | None:
        suffix = str(self.config.mail_server_suffix or "").strip().lower().lstrip(".")
        if not suffix:
            return None
        for _, addr in getaddresses(message.get_all("From", [])):
            domain = str(addr or "").split("@")[-1].lower().strip(".")
            if not domain:
                continue
            if domain == suffix or domain.endswith(f".{suffix}"):
                return domain
        return None

    def _imap_host(self) -> str:
        configured = str(self.config.imap_host or "").strip()
        if configured:
            return configured
        smtp_host = str(self.email_config.smtp_host or "").strip()
        if smtp_host.startswith("smtp."):
            return f"imap.{smtp_host[5:]}"
        if smtp_host:
            return f"imap.{smtp_host}"
        return ""

    @staticmethod
    def _normalize_username(value: str | None) -> str | None:
        raw = str(value or "").strip()
        if not raw:
            return None
        return raw.lower() if raw.startswith("@") else f"@{raw.lower()}"
