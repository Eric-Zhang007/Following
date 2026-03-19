from trader.store import SQLiteStore
from trader.threading_router import TradeThreadRouter


def test_private_channel_threading_reply_chain(tmp_path) -> None:
    store = SQLiteStore(str(tmp_path / "threading.db"))
    router = TradeThreadRouter(store)

    root = router.resolve(message_id=1001, text="这是交易信號 #ETH (50x做多)", reply_to_msg_id=None)
    assert root.thread_id == 1001
    assert root.is_root is True

    store.upsert_trade_thread(thread_id=1001, symbol="ETHUSDT", side="LONG", leverage=50, status="ACTIVE")
    store.record_thread_message(thread_id=1001, chat_id=0, message_id=1001, is_root=True, kind="ROOT")

    r1 = router.resolve(message_id=1002, text="收到", reply_to_msg_id=1001)
    assert r1.thread_id == 1001
    assert r1.is_root is False
    store.record_thread_message(thread_id=1001, chat_id=0, message_id=1002, is_root=False, kind="REPLY")

    r2 = router.resolve(message_id=1003, text="继续", reply_to_msg_id=1002)
    assert r2.thread_id == 1001
    assert r2.is_root is False


def test_private_channel_threading_detects_root_by_signal_structure(tmp_path) -> None:
    store = SQLiteStore(str(tmp_path / "threading_structure.db"))
    router = TradeThreadRouter(store)

    msg = "#ACU（10x做多）\n進場位：市價\n盈利位：0.15—0.18—0.2\n止損位：0.0865"
    root = router.resolve(message_id=2001, text=msg, reply_to_msg_id=None)

    assert root.thread_id == 2001
    assert root.is_root is True


def test_private_channel_threading_uses_chat_scoped_thread_id(tmp_path) -> None:
    store = SQLiteStore(str(tmp_path / "threading_chat_scope.db"))
    router = TradeThreadRouter(store)

    r1 = router.resolve(chat_id=-1001, message_id=3001, text="交易信號 #BTC (50x做多)", reply_to_msg_id=None)
    r2 = router.resolve(chat_id=-1002, message_id=3001, text="交易信號 #BTC (50x做多)", reply_to_msg_id=None)

    assert r1.thread_id is not None
    assert r2.thread_id is not None
    assert r1.thread_id != r2.thread_id


def test_private_channel_reply_resolution_is_chat_scoped(tmp_path) -> None:
    store = SQLiteStore(str(tmp_path / "threading_reply_scope.db"))
    router = TradeThreadRouter(store)

    thread_a = router.resolve(chat_id=-1001, message_id=4001, text="交易信號 #ETH (50x做多)", reply_to_msg_id=None).thread_id
    thread_b = router.resolve(chat_id=-1002, message_id=4001, text="交易信號 #ETH (50x做多)", reply_to_msg_id=None).thread_id
    assert thread_a is not None
    assert thread_b is not None

    store.upsert_trade_thread(thread_id=thread_a, symbol="ETHUSDT", side="LONG", leverage=50, status="ACTIVE")
    store.upsert_trade_thread(thread_id=thread_b, symbol="ETHUSDT", side="LONG", leverage=50, status="ACTIVE")
    store.record_thread_message(thread_id=thread_a, chat_id=-1001, message_id=4001, is_root=True, kind="ROOT")
    store.record_thread_message(thread_id=thread_b, chat_id=-1002, message_id=4001, is_root=True, kind="ROOT")

    r_a = router.resolve(chat_id=-1001, message_id=4002, text="收到", reply_to_msg_id=4001)
    r_b = router.resolve(chat_id=-1002, message_id=4002, text="收到", reply_to_msg_id=4001)
    assert r_a.thread_id == thread_a
    assert r_b.thread_id == thread_b


def test_private_channel_reply_resolution_supports_cross_chat_reply_peer(tmp_path) -> None:
    store = SQLiteStore(str(tmp_path / "threading_cross_chat_reply_peer.db"))
    router = TradeThreadRouter(store)

    channel_chat_id = -1003831751615
    discussion_chat_id = -1001616997199
    root = router.resolve(
        chat_id=channel_chat_id,
        message_id=5001,
        text="交易信號 #NEAR（10x做多）\n進場位：1.0-1.1\n止損位：0.9",
        reply_to_msg_id=None,
    )
    assert root.thread_id is not None
    store.upsert_trade_thread(thread_id=root.thread_id, symbol="NEARUSDT", side="LONG", leverage=10, status="ACTIVE")
    store.record_thread_message(
        thread_id=root.thread_id,
        chat_id=channel_chat_id,
        message_id=5001,
        is_root=True,
        kind="ROOT",
    )

    discussion_reply = router.resolve(
        chat_id=discussion_chat_id,
        message_id=9001,
        text="收到，继续持有",
        reply_to_msg_id=5001,
        reply_to_chat_id=channel_chat_id,
    )
    assert discussion_reply.thread_id == root.thread_id
    assert discussion_reply.is_root is False
