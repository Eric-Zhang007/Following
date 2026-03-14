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
    def __init__(
        self,
        config: TelegramConfig,
        logger: logging.Logger,
        media_dir: str = "media/private",
        control_usernames: list[str] | None = None,
    ) -> None:
        self.config = config
        self.logger = logger
        self.media_dir = Path(media_dir)
        self.media_dir.mkdir(parents=True, exist_ok=True)
        self._startup_at: datetime | None = None
        self._forward_targets: list[Any] = []
        self._forward_source_ids: set[int] = set()
        self._replay_days_cap_logged = False
        self._control_usernames = self._normalize_usernames(control_usernames or [])

    async def run(
        self,
        on_event: Callable[[TelegramEvent], Awaitable[None]],
        on_ignored: Callable[[dict[str, Any]], Awaitable[None]] | None = None,
    ) -> None:
        if self.config.api_id is None or self.config.api_hash is None:
            raise RuntimeError("telegram api_id/api_hash are required for TelegramPrivateListener")
        chats = self._listener_chats()
        if not chats:
            raise RuntimeError(
                "telegram.channel_id / channel_ids / channel_usernames / channel is required for TelegramPrivateListener"
            )

        client = TelegramClient(self.config.session_name, self.config.api_id, self.config.api_hash)
        await client.start()
        self._startup_at = datetime.now(timezone.utc)
        self._forward_source_ids = self._build_forward_source_id_set()
        self._forward_targets = await self._resolve_forward_targets(client)
        self.logger.info(
            "Telethon private listener started. channel_ids=%s title_hint=%s",
            chats,
            self.config.channel_title_hint,
        )
        if self._forward_targets:
            self.logger.info(
                "Telethon validation mirror enabled. source_ids=%s targets=%s include_edits=%s skip_prestartup=%s",
                sorted(self._forward_source_ids),
                self.config.mirror_forward_targets,
                self.config.mirror_forward_include_edits,
                self.config.mirror_forward_skip_prestartup,
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
                client=event.client,
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
        client: TelegramClient,
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
        reply_to_chat_id: int | None = None
        if getattr(message, "reply_to", None) is not None:
            reply_to_msg_id = getattr(message.reply_to, "reply_to_msg_id", None)
            reply_to_chat_id = self._reply_peer_to_chat_id(getattr(message.reply_to, "reply_to_peer_id", None))

        media_type, media_path = await self._extract_media(message)
        sender_id = getattr(message, "sender_id", None)
        sender_username: str | None = None
        try:
            sender = await message.get_sender()
        except Exception:  # noqa: BLE001
            sender = None
        if sender is not None:
            raw_username = str(getattr(sender, "username", "") or "").strip()
            if raw_username:
                sender_username = raw_username if raw_username.startswith("@") else f"@{raw_username}"

        raw_text = getattr(message, "message", "") or ""
        wrapped = TelegramEvent(
            chat_id=int(chat_id or self._primary_channel_id() or 0),
            message_id=int(getattr(message, "id", 0) or 0),
            text=raw_text,
            raw_text=raw_text,
            is_edit=is_edit,
            date=event_time,
            reply_to_msg_id=reply_to_msg_id,
            reply_to_chat_id=reply_to_chat_id,
            media_type=media_type,
            media_bytes=media_path,
            media_path=media_path,
            source="telegram_private",
            sender_id=int(sender_id) if sender_id is not None else None,
            sender_username=sender_username,
            pre_startup=pre_startup,
            startup_at=self._startup_at if pre_startup else None,
        )
        await self._forward_validation_copy(
            client=client,
            message=message,
            chat_id=chat_id,
            is_edit=is_edit,
            pre_startup=pre_startup,
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
                client=client,
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
        replay_days = int(self.config.startup_replay_days)
        effective_days = min(replay_days, 1)
        if replay_days > effective_days and not self._replay_days_cap_logged:
            self._replay_days_cap_logged = True
            self.logger.warning(
                "startup_replay_days is capped to 1 day in telegram_private mode. configured=%s effective=%s",
                replay_days,
                effective_days,
            )
        return self._startup_at - timedelta(days=effective_days)

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

    async def _resolve_forward_targets(self, client: TelegramClient) -> list[Any]:
        targets = [str(v or "").strip() for v in self.config.mirror_forward_targets if str(v or "").strip()]
        if not targets:
            return []
        out: list[Any] = []
        for target in targets:
            try:
                out.append(await client.get_input_entity(target))
            except Exception as exc:  # noqa: BLE001
                self.logger.warning("Telethon mirror target resolve failed. target=%s err=%s", target, exc)
        return out

    @staticmethod
    def _chat_id_variants(chat_id: int) -> set[int]:
        variants = {int(chat_id)}
        cid = int(chat_id)
        if cid >= 0:
            return variants
        abs_id = str(abs(cid))
        if abs_id.startswith("100") and len(abs_id) >= 11:
            with_prefix_removed = int(abs_id[3:])
            variants.add(-with_prefix_removed)
        elif len(abs_id) >= 10:
            with_prefix_added = int(f"100{abs_id}")
            variants.add(-with_prefix_added)
        return variants

    @staticmethod
    def _reply_peer_to_chat_id(peer: Any) -> int | None:
        if peer is None:
            return None
        channel_id = getattr(peer, "channel_id", None)
        if channel_id is not None:
            return -(1000000000000 + int(channel_id))
        chat_id = getattr(peer, "chat_id", None)
        if chat_id is not None:
            return -int(chat_id)
        user_id = getattr(peer, "user_id", None)
        if user_id is not None:
            return int(user_id)
        return None

    def _build_forward_source_id_set(self) -> set[int]:
        out: set[int] = set()
        for cid in self.config.mirror_forward_source_ids:
            out |= self._chat_id_variants(int(cid))
        return out

    def _chat_matches_forward_source(self, chat_id: int) -> bool:
        if not self._forward_source_ids:
            return False
        return bool(self._chat_id_variants(chat_id) & self._forward_source_ids)

    def _should_forward_message(self, *, chat_id: int, is_edit: bool, pre_startup: bool) -> bool:
        if not self._forward_targets or not self._forward_source_ids:
            return False
        if is_edit and not self.config.mirror_forward_include_edits:
            return False
        if pre_startup and self.config.mirror_forward_skip_prestartup:
            return False
        return self._chat_matches_forward_source(chat_id)

    async def _forward_validation_copy(
        self,
        *,
        client: TelegramClient,
        message,
        chat_id: int,
        is_edit: bool,
        pre_startup: bool,
    ) -> None:
        if not self._should_forward_message(chat_id=chat_id, is_edit=is_edit, pre_startup=pre_startup):
            return
        for target in self._forward_targets:
            try:
                await client.forward_messages(entity=target, messages=message)
            except Exception as exc:  # noqa: BLE001
                self.logger.warning(
                    "Telethon mirror forward failed. source_chat_id=%s target=%s message_id=%s err=%s",
                    chat_id,
                    target,
                    int(getattr(message, "id", 0) or 0),
                    exc,
                )

    @staticmethod
    def _normalize_usernames(values: list[str]) -> list[str]:
        out: list[str] = []
        seen: set[str] = set()
        for raw in values:
            name = str(raw or "").strip()
            if not name:
                continue
            if not name.startswith("@"):
                name = f"@{name}"
            key = name.lower()
            if key in seen:
                continue
            seen.add(key)
            out.append(name)
        return out

    def _listener_chats(self) -> list[int | str]:
        chats: list[int | str] = []
        seen_ids: set[int] = set()
        seen_names: set[str] = set()
        for cid in self.config.channel_ids:
            try:
                value = int(cid)
            except Exception:  # noqa: BLE001
                continue
            if value in seen_ids:
                continue
            seen_ids.add(value)
            chats.append(value)
        if self.config.channel_id is not None:
            cid = int(self.config.channel_id)
            if cid not in seen_ids:
                seen_ids.add(cid)
                chats.append(cid)
        for cid in self.config.discussion_chat_ids:
            try:
                value = int(cid)
            except Exception:  # noqa: BLE001
                continue
            for variant in self._chat_id_variants(value):
                if variant in seen_ids:
                    continue
                seen_ids.add(variant)
                chats.append(variant)
        for name in self.config.channel_usernames:
            normalized = str(name or "").strip()
            if not normalized:
                continue
            if not normalized.startswith("@"):
                normalized = f"@{normalized}"
            key = normalized.lower()
            if key in seen_names:
                continue
            seen_names.add(key)
            chats.append(normalized)
        for control_name in self._control_usernames:
            key = control_name.lower()
            if key in seen_names:
                continue
            seen_names.add(key)
            chats.append(control_name)
        channel_name = str(self.config.channel or "").strip()
        if channel_name:
            if not channel_name.startswith("@"):
                channel_name = f"@{channel_name}"
            key = channel_name.lower()
            if key not in seen_names:
                chats.append(channel_name)
        return chats

    def _primary_channel_id(self) -> int | None:
        chats = self._listener_chats()
        for item in chats:
            if isinstance(item, int):
                return item
        return None

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
