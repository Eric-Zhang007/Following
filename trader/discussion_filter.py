from __future__ import annotations

from trader.models import TelegramEvent


def chat_id_variants(chat_id: int) -> set[int]:
    variants = {int(chat_id)}
    cid = int(chat_id)
    if cid >= 0:
        return variants
    abs_id = str(abs(cid))
    if abs_id.startswith("100") and len(abs_id) >= 11:
        variants.add(-int(abs_id[3:]))
    elif len(abs_id) >= 10:
        variants.add(-int(f"100{abs_id}"))
    return variants


def is_discussion_chat(*, discussion_chat_ids: list[int], chat_id: int) -> bool:
    if not discussion_chat_ids:
        return False
    incoming = chat_id_variants(chat_id)
    for cid in discussion_chat_ids:
        if incoming & chat_id_variants(int(cid)):
            return True
    return False


def is_channel_chat(*, channel_id: int | None, channel_ids: list[int], chat_id: int) -> bool:
    configured: list[int] = []
    if channel_id is not None:
        configured.append(int(channel_id))
    configured.extend(int(v) for v in channel_ids)
    if not configured:
        return False
    incoming = chat_id_variants(chat_id)
    for cid in configured:
        if incoming & chat_id_variants(int(cid)):
            return True
    return False


def should_skip_discussion_noise(
    *,
    discussion_chat_ids: list[int],
    channel_id: int | None,
    channel_ids: list[int],
    event: TelegramEvent,
) -> bool:
    if not is_discussion_chat(discussion_chat_ids=discussion_chat_ids, chat_id=event.chat_id):
        return False
    # For discussion groups, only parse messages that directly reply/quote channel posts.
    if event.reply_to_msg_id is None:
        return True
    if event.reply_to_chat_id is None:
        return True
    if not is_channel_chat(
        channel_id=channel_id,
        channel_ids=channel_ids,
        chat_id=event.reply_to_chat_id,
    ):
        return True
    return False
