from trader.models import EntrySignal, EntryType, ManageAction, NonSignal, Side
from trader.parser import SignalParser


def test_parse_entry_limit_short() -> None:
    parser = SignalParser()
    msg = "#CYBER/USDT（50x做空） 进场：限价0.73—0.746"
    parsed = parser.parse(msg, source_key="c1")

    assert isinstance(parsed, EntrySignal)
    assert parsed.symbol == "CYBERUSDT"
    assert parsed.side == Side.SHORT
    assert parsed.leverage == 50
    assert parsed.entry_type == EntryType.LIMIT
    assert parsed.entry_low == 0.73
    assert parsed.entry_high == 0.746


def test_parse_entry_market_short_range() -> None:
    parser = SignalParser()
    msg = "#KITE/USDT（25x做空） 进场：市价0.2493附近—0.26032"
    parsed = parser.parse(msg, source_key="c1")

    assert isinstance(parsed, EntrySignal)
    assert parsed.symbol == "KITEUSDT"
    assert parsed.side == Side.SHORT
    assert parsed.leverage == 25
    assert parsed.entry_type == EntryType.MARKET
    assert parsed.entry_low == 0.2493
    assert parsed.entry_high == 0.26032


def test_parse_entry_long() -> None:
    parser = SignalParser()
    msg = "#BTC/USDT (10x long) 入场: 限价62000-62500"
    parsed = parser.parse(msg, source_key="c1")

    assert isinstance(parsed, EntrySignal)
    assert parsed.symbol == "BTCUSDT"
    assert parsed.side == Side.LONG
    assert parsed.leverage == 10


def test_parse_manage_reduce_with_symbol() -> None:
    parser = SignalParser()
    msg = "#CYBER/USDT 减仓30%，TP1看0.69"
    parsed = parser.parse(msg, source_key="c1")

    assert isinstance(parsed, ManageAction)
    assert parsed.symbol == "CYBERUSDT"
    assert parsed.reduce_pct == 30
    assert parsed.tp_price == 0.69


def test_parse_manage_move_be_with_state_machine() -> None:
    parser = SignalParser()
    entry = "#SOL/USDT（20x做多） 进场：限价100-102"
    manage = "已到目标，设保本，留底仓放飞"

    parsed_entry = parser.parse(entry, source_key="chatA")
    parsed_manage = parser.parse(manage, source_key="chatA")

    assert isinstance(parsed_entry, EntrySignal)
    assert isinstance(parsed_manage, ManageAction)
    assert parsed_manage.symbol == "SOLUSDT"
    assert parsed_manage.move_sl_to_be is True


def test_parse_manage_tp_only() -> None:
    parser = SignalParser()
    msg = "TP2看 1.25"
    parsed = parser.parse(msg, source_key="c1", fallback_symbol="DOGEUSDT")

    assert isinstance(parsed, ManageAction)
    assert parsed.symbol == "DOGEUSDT"
    assert parsed.tp_price == 1.25


def test_parse_non_signal_text() -> None:
    parser = SignalParser()
    msg = "今天行情波动很大，注意风险"
    parsed = parser.parse(msg, source_key="c1")

    assert isinstance(parsed, NonSignal)


def test_parse_empty_text() -> None:
    parser = SignalParser()
    parsed = parser.parse("", source_key="c1")
    assert isinstance(parsed, NonSignal)
