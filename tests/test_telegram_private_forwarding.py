import logging
from datetime import datetime, timedelta, timezone

import pytest

from trader.config import TelegramConfig

pytest.importorskip("telethon")

from trader.telegram_private_listener import TelegramPrivateListener


def _listener(cfg: TelegramConfig, control_usernames: list[str] | None = None) -> TelegramPrivateListener:
    return TelegramPrivateListener(
        cfg,
        logger=logging.getLogger("test.telegram.forward"),
        control_usernames=control_usernames,
    )


def test_forward_source_id_matches_short_and_prefixed_variants() -> None:
    cfg = TelegramConfig(
        api_id=1,
        api_hash="x",
        channel="@IvanCryptotalk",
        mirror_forward_source_ids=[-1616997199],
        mirror_forward_targets=["@aa3845226"],
    )
    listener = _listener(cfg)
    listener._forward_source_ids = listener._build_forward_source_id_set()
    listener._forward_targets = [object()]

    assert listener._chat_matches_forward_source(-1616997199)
    assert listener._chat_matches_forward_source(-1001616997199)


def test_should_forward_respects_edit_and_prestartup_flags() -> None:
    cfg = TelegramConfig(
        api_id=1,
        api_hash="x",
        channel="@IvanCryptotalk",
        mirror_forward_source_ids=[-1616997199],
        mirror_forward_targets=["@aa3845226"],
        mirror_forward_include_edits=False,
        mirror_forward_skip_prestartup=True,
    )
    listener = _listener(cfg)
    listener._forward_source_ids = listener._build_forward_source_id_set()
    listener._forward_targets = [object()]

    assert listener._should_forward_message(chat_id=-1616997199, is_edit=False, pre_startup=False)
    assert not listener._should_forward_message(chat_id=-1616997199, is_edit=True, pre_startup=False)
    assert not listener._should_forward_message(chat_id=-1616997199, is_edit=False, pre_startup=True)


def test_startup_replay_window_is_capped_to_one_day() -> None:
    cfg = TelegramConfig(
        api_id=1,
        api_hash="x",
        channel="@IvanCryptotalk",
        startup_replay_days=3,
    )
    listener = _listener(cfg)
    startup_at = datetime(2026, 3, 3, 12, 0, 0, tzinfo=timezone.utc)
    listener._startup_at = startup_at

    replay_start = listener._startup_replay_window_start()

    assert replay_start == startup_at - timedelta(days=1)


def test_listener_chats_include_discussion_chat_ids_with_variants() -> None:
    cfg = TelegramConfig(
        api_id=1,
        api_hash="x",
        channel_id=-1003831751615,
        discussion_chat_ids=[-1616997199],
    )
    listener = _listener(cfg)

    chats = listener._listener_chats()

    assert -1003831751615 in chats
    assert -1616997199 in chats
    assert -1001616997199 in chats


def test_reply_peer_to_chat_id_maps_channel_peer() -> None:
    class _Peer:
        channel_id = 3831751615

    assert TelegramPrivateListener._reply_peer_to_chat_id(_Peer()) == -1003831751615


def test_listener_chats_include_control_usernames() -> None:
    cfg = TelegramConfig(
        api_id=1,
        api_hash="x",
        channel_id=-1003831751615,
    )
    listener = _listener(cfg, control_usernames=["@aa3845226"])

    chats = listener._listener_chats()

    assert "@aa3845226" in chats
