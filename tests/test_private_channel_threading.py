from trader.store import SQLiteStore
from trader.threading_router import TradeThreadRouter


def test_private_channel_threading_reply_chain(tmp_path) -> None:
    store = SQLiteStore(str(tmp_path / "threading.db"))
    router = TradeThreadRouter(store)

    root = router.resolve(message_id=1001, text="这是交易信號 #ETH (50x做多)", reply_to_msg_id=None)
    assert root.thread_id == 1001
    assert root.is_root is True

    store.upsert_trade_thread(thread_id=1001, symbol="ETHUSDT", side="LONG", leverage=50, status="ACTIVE")
    store.record_thread_message(thread_id=1001, message_id=1001, is_root=True, kind="ROOT")

    r1 = router.resolve(message_id=1002, text="收到", reply_to_msg_id=1001)
    assert r1.thread_id == 1001
    assert r1.is_root is False
    store.record_thread_message(thread_id=1001, message_id=1002, is_root=False, kind="REPLY")

    r2 = router.resolve(message_id=1003, text="继续", reply_to_msg_id=1002)
    assert r2.thread_id == 1001
    assert r2.is_root is False
