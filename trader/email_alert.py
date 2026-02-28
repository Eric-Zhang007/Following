from __future__ import annotations

import json
import os
import smtplib
from email.message import EmailMessage
from typing import Any

from trader.config import EmailAlertConfig


class SMTPAlertSender:
    def __init__(self, config: EmailAlertConfig) -> None:
        self.config = config

    def should_send(self, event_type: str) -> bool:
        if not self.config.enabled:
            return False
        allowed = {item.strip() for item in self.config.send_on if item.strip()}
        if not allowed:
            return False
        return event_type in allowed

    def send(
        self,
        *,
        event_type: str,
        level: str,
        msg: str,
        trace_id: str,
        payload: dict[str, Any] | None,
    ) -> None:
        if not self.should_send(event_type):
            return
        if not self.config.smtp_host or not self.config.to_addrs:
            return

        email_msg = EmailMessage()
        email_msg["Subject"] = f"[Following][{level}] {event_type}"
        email_msg["From"] = self.config.from_addr or self.config.smtp_user
        email_msg["To"] = ", ".join(self.config.to_addrs)
        body = {
            "event_type": event_type,
            "level": level,
            "msg": msg,
            "trace_id": trace_id,
            "payload": payload or {},
        }
        email_msg.set_content(json.dumps(body, ensure_ascii=False, indent=2, default=str))

        password = os.getenv(self.config.smtp_pass_env, "")
        with smtplib.SMTP(self.config.smtp_host, self.config.smtp_port, timeout=10) as smtp:
            smtp.ehlo()
            try:
                smtp.starttls()
                smtp.ehlo()
            except Exception:
                pass
            if self.config.smtp_user:
                smtp.login(self.config.smtp_user, password)
            smtp.send_message(email_msg)
