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
            reply_to_msg_id = None
            if event.message and getattr(event.message, "reply_to", None) is not None:
                reply_to_msg_id = getattr(event.message.reply_to, "reply_to_msg_id", None)
            media_type = "none"
            if event.message and getattr(event.message, "photo", None) is not None:
                media_type = "photo"
            elif event.message and getattr(event.message, "sticker", None) is not None:
                media_type = "sticker"
            elif event.message and getattr(event.message, "media", None) is not None:
                media_type = "document"
            wrapped = TelegramEvent(
                chat_id=int(event.chat_id or 0),
                message_id=int(event.id),
                text=event.raw_text or "",
                raw_text=event.raw_text or "",
                is_edit=is_edit,
                date=event_time,
                reply_to_msg_id=reply_to_msg_id,
                media_type=media_type,
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
