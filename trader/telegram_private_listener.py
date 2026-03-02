from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

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

    async def run(
        self,
        on_event: Callable[[TelegramEvent], Awaitable[None]],
        on_ignored: Callable[[dict[str, Any]], Awaitable[None]] | None = None,
    ) -> None:
        if self.config.api_id is None or self.config.api_hash is None:
            raise RuntimeError("telegram api_id/api_hash are required for TelegramPrivateListener")
        chats = self._listener_chats()
        if not chats:
            raise RuntimeError("telegram.channel_id or telegram.channel_ids is required for TelegramPrivateListener")

        client = TelegramClient(self.config.session_name, self.config.api_id, self.config.api_hash)
        await client.start()
        self._startup_at = datetime.now(timezone.utc)
        self.logger.info(
            "Telethon private listener started. channel_ids=%s title_hint=%s",
            chats,
            self.config.channel_title_hint,
        )

        @client.on(events.NewMessage(chats=chats))
        async def on_new_message(event: events.NewMessage.Event) -> None:
            await self._dispatch(event, on_event=on_event, on_ignored=on_ignored, is_edit=False)

        if self.config.enable_edited_events:
            @client.on(events.MessageEdited(chats=chats))
            async def on_edited_message(event: events.MessageEdited.Event) -> None:
                await self._dispatch(event, on_event=on_event, on_ignored=on_ignored, is_edit=True)

        if self.config.startup_replay_days > 0:
            await self._replay_recent_messages(client, on_event=on_event, on_ignored=on_ignored)

        await client.run_until_disconnected()

    async def _dispatch(
        self,
        event,
        on_event: Callable[[TelegramEvent], Awaitable[None]],
        on_ignored: Callable[[dict[str, Any]], Awaitable[None]] | None,
        is_edit: bool,
    ) -> None:
        try:
            message = getattr(event, "message", None)
            if message is None:
                return
            await self._dispatch_message(
                message=message,
                chat_id=int(event.chat_id or self._primary_channel_id() or 0),
                on_event=on_event,
                on_ignored=on_ignored,
                is_edit=is_edit,
            )
        except Exception:  # noqa: BLE001
            self.logger.exception("Telegram private handler error (is_edit=%s)", is_edit)

    async def _dispatch_message(
        self,
        *,
        message,
        chat_id: int,
        on_event: Callable[[TelegramEvent], Awaitable[None]],
        on_ignored: Callable[[dict[str, Any]], Awaitable[None]] | None,
        is_edit: bool,
    ) -> None:
        event_time = message.date if message.date else datetime.now(timezone.utc)
        if event_time.tzinfo is None:
            event_time = event_time.replace(tzinfo=timezone.utc)

        pre_startup = self._startup_at is not None and event_time < self._startup_at
        if self.config.accept_only_after_startup and pre_startup:
            replay_window_start = self._startup_replay_window_start()
            if replay_window_start is None or event_time < replay_window_start:
                payload = {
                    "reason": "before_startup_outside_replay_window",
                    "channel_id": int(chat_id or self._primary_channel_id() or 0),
                    "message_id": int(getattr(message, "id", 0) or 0),
                    "is_edit": bool(is_edit),
                    "event_time": event_time.isoformat(),
                    "startup_at": self._startup_at.isoformat() if self._startup_at else None,
                    "startup_replay_days": int(self.config.startup_replay_days),
                }
                self.logger.warning("Private message ignored due to startup replay window: %s", payload)
                if on_ignored is not None:
                    await on_ignored(payload)
                return

        reply_to_msg_id: int | None = None
        if getattr(message, "reply_to", None) is not None:
            reply_to_msg_id = getattr(message.reply_to, "reply_to_msg_id", None)

        media_type, media_path = await self._extract_media(message)

        raw_text = getattr(message, "message", "") or ""
        wrapped = TelegramEvent(
            chat_id=int(chat_id or self._primary_channel_id() or 0),
            message_id=int(getattr(message, "id", 0) or 0),
            text=raw_text,
            raw_text=raw_text,
            is_edit=is_edit,
            date=event_time,
            reply_to_msg_id=reply_to_msg_id,
            media_type=media_type,
            media_bytes=media_path,
            media_path=media_path,
            source="telegram_private",
            pre_startup=pre_startup,
            startup_at=self._startup_at if pre_startup else None,
        )
        await on_event(wrapped)

    async def _replay_recent_messages(
        self,
        client: TelegramClient,
        on_event: Callable[[TelegramEvent], Awaitable[None]],
        on_ignored: Callable[[dict[str, Any]], Awaitable[None]] | None,
    ) -> None:
        if self._startup_at is None:
            return
        replay_window_start = self._startup_replay_window_start()
        if replay_window_start is None:
            return

        peer = await self._resolve_replay_peer(client)
        if peer is None:
            self.logger.warning("Telethon private replay skipped: unable to resolve replay peer. channels=%s", self._listener_chats())
            return

        buffer: list[Any] = []
        try:
            async for message in client.iter_messages(peer):
                event_time = message.date if message.date else datetime.now(timezone.utc)
                if event_time.tzinfo is None:
                    event_time = event_time.replace(tzinfo=timezone.utc)
                if event_time >= self._startup_at:
                    continue
                if event_time < replay_window_start:
                    break
                buffer.append(message)
        except Exception as exc:  # noqa: BLE001
            self.logger.warning("Telethon private replay skipped due to history fetch error: %s", exc)
            return

        replayed = 0
        for message in reversed(buffer):
            chat_id = int(getattr(message, "chat_id", 0) or self._primary_channel_id() or 0)
            await self._dispatch_message(
                message=message,
                chat_id=chat_id,
                on_event=on_event,
                on_ignored=on_ignored,
                is_edit=False,
            )
            replayed += 1

        self.logger.info(
            "Telethon private replay finished. replayed=%s startup_at=%s replay_window_start=%s",
            replayed,
            self._startup_at.isoformat(),
            replay_window_start.isoformat(),
        )

    def _startup_replay_window_start(self) -> datetime | None:
        if self._startup_at is None or self.config.startup_replay_days <= 0:
            return None
        return self._startup_at - timedelta(days=self.config.startup_replay_days)

    async def _resolve_replay_peer(self, client: TelegramClient):
        candidates: list[Any] = []
        for cid in self._listener_chats():
            candidates.append(cid)
        if self.config.channel:
            candidates.append(self.config.channel)

        for candidate in candidates:
            try:
                return await client.get_input_entity(candidate)
            except Exception:  # noqa: BLE001
                continue

        title_hint = (self.config.channel_title_hint or "").strip()
        if not title_hint:
            return None

        try:
            async for dialog in client.iter_dialogs():
                name = (getattr(dialog, "name", "") or "").strip()
                if title_hint and title_hint in name:
                    return dialog.input_entity
        except Exception:  # noqa: BLE001
            return None
        return None

    def _listener_chats(self) -> list[int]:
        ids: list[int] = []
        seen: set[int] = set()
        for cid in self.config.channel_ids:
            try:
                value = int(cid)
            except Exception:  # noqa: BLE001
                continue
            if value in seen:
                continue
            seen.add(value)
            ids.append(value)
        if self.config.channel_id is not None:
            cid = int(self.config.channel_id)
            if cid not in seen:
                ids.append(cid)
        return ids

    def _primary_channel_id(self) -> int | None:
        chats = self._listener_chats()
        return chats[0] if chats else None

    async def _extract_media(self, message) -> tuple[str, str | None]:
        if message is None or getattr(message, "media", None) is None:
            return "none", None

        media_type = "document"
        if getattr(message, "photo", None) is not None:
            media_type = "photo"
        elif getattr(message, "sticker", None) is not None:
            media_type = "sticker"

        day_dir = self.media_dir / datetime.now(timezone.utc).strftime("%Y%m%d")
        day_dir.mkdir(parents=True, exist_ok=True)
        path = await message.download_media(file=str(day_dir))
        if not path:
            return media_type, None
        return media_type, str(path)
