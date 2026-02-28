from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from datetime import datetime, timezone
from pathlib import Path

from telethon import TelegramClient, events

from trader.config import TelegramConfig
from trader.models import TelegramEvent


class TelegramPrivateListener:
    def __init__(self, config: TelegramConfig, logger: logging.Logger, media_dir: str = "media/private") -> None:
        self.config = config
        self.logger = logger
        self.media_dir = Path(media_dir)
        self.media_dir.mkdir(parents=True, exist_ok=True)
        self._startup_at: datetime | None = None

    async def run(self, on_event: Callable[[TelegramEvent], Awaitable[None]]) -> None:
        if self.config.api_id is None or self.config.api_hash is None:
            raise RuntimeError("telegram api_id/api_hash are required for TelegramPrivateListener")
        if self.config.channel_id is None:
            raise RuntimeError("telegram.channel_id is required for TelegramPrivateListener")

        client = TelegramClient(self.config.session_name, self.config.api_id, self.config.api_hash)
        await client.start()
        self._startup_at = datetime.now(timezone.utc)
        self.logger.info(
            "Telethon private listener started. channel_id=%s title_hint=%s",
            self.config.channel_id,
            self.config.channel_title_hint,
        )

        @client.on(events.NewMessage(chats=self.config.channel_id))
        async def on_new_message(event: events.NewMessage.Event) -> None:
            await self._dispatch(event, on_event=on_event, is_edit=False)

        if self.config.enable_edited_events:
            @client.on(events.MessageEdited(chats=self.config.channel_id))
            async def on_edited_message(event: events.MessageEdited.Event) -> None:
                await self._dispatch(event, on_event=on_event, is_edit=True)

        await client.run_until_disconnected()

    async def _dispatch(self, event, on_event: Callable[[TelegramEvent], Awaitable[None]], is_edit: bool) -> None:
        try:
            message = getattr(event, "message", None)
            if message is None:
                return

            event_time = message.date if message.date else datetime.now(timezone.utc)
            if event_time.tzinfo is None:
                event_time = event_time.replace(tzinfo=timezone.utc)

            if self.config.accept_only_after_startup and self._startup_at is not None and event_time < self._startup_at:
                return

            reply_to_msg_id: int | None = None
            if getattr(message, "reply_to", None) is not None:
                reply_to_msg_id = getattr(message.reply_to, "reply_to_msg_id", None)

            media_type, media_path = await self._extract_media(event)

            raw_text = event.raw_text or ""
            wrapped = TelegramEvent(
                chat_id=int(event.chat_id or self.config.channel_id or 0),
                message_id=int(event.id),
                text=raw_text,
                raw_text=raw_text,
                is_edit=is_edit,
                date=event_time,
                reply_to_msg_id=reply_to_msg_id,
                media_type=media_type,
                media_bytes=media_path,
                media_path=media_path,
                source="telegram_private",
            )
            await on_event(wrapped)
        except Exception:  # noqa: BLE001
            self.logger.exception("Telegram private handler error (is_edit=%s)", is_edit)

    async def _extract_media(self, event) -> tuple[str, str | None]:
        message = getattr(event, "message", None)
        if message is None or getattr(message, "media", None) is None:
            return "none", None

        media_type = "document"
        if getattr(message, "photo", None) is not None:
            media_type = "photo"
        elif getattr(message, "sticker", None) is not None:
            media_type = "sticker"

        day_dir = self.media_dir / datetime.now(timezone.utc).strftime("%Y%m%d")
        day_dir.mkdir(parents=True, exist_ok=True)
        path = await event.download_media(file=str(day_dir))
        if not path:
            return media_type, None
        return media_type, str(path)
