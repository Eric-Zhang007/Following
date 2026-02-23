from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from datetime import datetime, timezone

from telethon import TelegramClient, events

from trader.config import TelegramConfig
from trader.models import TelegramEvent


class TelegramListener:
    def __init__(self, config: TelegramConfig, logger: logging.Logger) -> None:
        self.config = config
        self.logger = logger

    async def run(self, on_event: Callable[[TelegramEvent], Awaitable[None]]) -> None:
        if self.config.api_id is None or self.config.api_hash is None:
            raise RuntimeError("telegram api_id/api_hash are required for TelegramListener")
        client = TelegramClient(self.config.session_name, self.config.api_id, self.config.api_hash)
        await client.start()
        self.logger.info("Telethon login successful. Listening channel=%s", self.config.channel)

        @client.on(events.NewMessage)
        async def on_new_message(event: events.NewMessage.Event) -> None:
            await self._dispatch(event, on_event=on_event, is_edit=False)

        @client.on(events.MessageEdited)
        async def on_edited_message(event: events.MessageEdited.Event) -> None:
            await self._dispatch(event, on_event=on_event, is_edit=True)

        await client.run_until_disconnected()

    async def _dispatch(self, event, on_event: Callable[[TelegramEvent], Awaitable[None]], is_edit: bool) -> None:
        try:
            chat = await event.get_chat()
            if not self._match_channel(chat, event):
                return

            event_time = event.message.date if event.message and event.message.date else datetime.now(timezone.utc)
            wrapped = TelegramEvent(
                chat_id=int(event.chat_id or 0),
                message_id=int(event.id),
                text=event.raw_text or "",
                is_edit=is_edit,
                date=event_time,
            )
            await on_event(wrapped)
        except Exception:  # noqa: BLE001
            self.logger.exception("Telegram handler error (is_edit=%s)", is_edit)

    def _match_channel(self, chat, event) -> bool:
        channel = self.config.channel.strip()
        if not channel:
            return True

        if channel.startswith("@"):
            target = channel[1:].lower()
            username = str(getattr(chat, "username", "") or "").lower()
            return username == target

        # Support numeric channel id in config, e.g. -1001234567890
        if channel.lstrip("-").isdigit():
            return int(channel) == int(event.chat_id or 0)

        title = str(getattr(chat, "title", "") or "").lower()
        return channel.lower() == title
