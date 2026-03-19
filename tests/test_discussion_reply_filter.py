from datetime import datetime, timezone

from trader.discussion_filter import should_skip_discussion_noise
from trader.models import TelegramEvent


_CHANNEL_ID = -1003831751615
_CHANNEL_IDS: list[int] = []
_DISCUSSION_IDS = [-1616997199]


def _event(*, chat_id: int, reply_to_msg_id: int | None, reply_to_chat_id: int | None) -> TelegramEvent:
    return TelegramEvent(
        chat_id=chat_id,
        message_id=1,
        date=datetime(2026, 3, 6, 10, 21, tzinfo=timezone.utc),
        text="#Q 市價0.0127附近多 (20X)",
        reply_to_msg_id=reply_to_msg_id,
        reply_to_chat_id=reply_to_chat_id,
    )


def test_non_discussion_message_is_not_filtered() -> None:
    event = _event(chat_id=-1003831751615, reply_to_msg_id=None, reply_to_chat_id=None)
    assert (
        should_skip_discussion_noise(
            discussion_chat_ids=_DISCUSSION_IDS,
            channel_id=_CHANNEL_ID,
            channel_ids=_CHANNEL_IDS,
            event=event,
        )
        is False
    )


def test_discussion_non_reply_is_filtered() -> None:
    event = _event(chat_id=-1001616997199, reply_to_msg_id=None, reply_to_chat_id=None)
    assert (
        should_skip_discussion_noise(
            discussion_chat_ids=_DISCUSSION_IDS,
            channel_id=_CHANNEL_ID,
            channel_ids=_CHANNEL_IDS,
            event=event,
        )
        is True
    )


def test_discussion_reply_to_non_channel_is_filtered() -> None:
    event = _event(
        chat_id=-1001616997199,
        reply_to_msg_id=12345,
        reply_to_chat_id=-1001616997199,
    )
    assert (
        should_skip_discussion_noise(
            discussion_chat_ids=_DISCUSSION_IDS,
            channel_id=_CHANNEL_ID,
            channel_ids=_CHANNEL_IDS,
            event=event,
        )
        is True
    )


def test_discussion_reply_to_channel_is_allowed() -> None:
    event = _event(
        chat_id=-1001616997199,
        reply_to_msg_id=12345,
        reply_to_chat_id=-3831751615,
    )
    assert (
        should_skip_discussion_noise(
            discussion_chat_ids=_DISCUSSION_IDS,
            channel_id=_CHANNEL_ID,
            channel_ids=_CHANNEL_IDS,
            event=event,
        )
        is False
    )
