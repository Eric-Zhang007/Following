from datetime import datetime, timezone

from trader.config import AppConfig
from trader.models import EntrySignal, EntryType, ManageAction
from trader.private_channel_parser import PrivateChannelParser


def _build_config() -> AppConfig:
    return AppConfig.model_validate(
        {
            "dry_run": True,
            "listener": {"mode": "telegram_private"},
            "telegram": {
                "api_id": 1,
                "api_hash": "x",
                "session_name": "s",
                "channel_id": -1000000000000,
                "channel": "@IvanCryptotalk",
            },
            "bitget": {
                "base_url": "https://api.bitget.com",
                "api_key": "",
                "api_secret": "",
                "passphrase": "",
                "product_type": "USDT-FUTURES",
            },
            "filters": {
                "symbol_whitelist": ["HUSDT"],
                "max_leverage": 100,
                "allow_sides": ["LONG", "SHORT"],
                "max_signal_age_seconds": 20,
                "leverage_over_limit_action": "CLAMP",
            },
            "risk": {
                "account_risk_per_trade": 0.005,
                "max_notional_per_trade": 200,
                "entry_slippage_pct": 0.3,
                "cooldown_seconds": 300,
                "default_stop_loss_pct": 1.0,
                "assumed_equity_usdt": 1000,
            },
            "logging": {"level": "INFO", "file": "trader.log", "rich": False},
            "vlm": {"enabled": False},
        }
    )


def test_private_parser_accepts_single_letter_symbol() -> None:
    parser = PrivateChannelParser(_build_config())
    text = (
        "🖥 交易信號 🖥\n\n"
        "#H（50x做多🚀🚀🚀）\n\n"
        "✏️進場位：0.11625—0.11338\n\n"
        "👁 盈利位：0.12137—0.12950—0.14419\n\n"
        "❌止損位：0.10973"
    )

    out = parser.parse(
        text=text,
        timestamp=datetime(2026, 3, 1, 10, 34, 23, tzinfo=timezone.utc),
        image_path=None,
        fallback_symbol=None,
        thread_id=8,
        is_root=True,
    )
    assert isinstance(out.parsed, EntrySignal)
    assert out.parsed.symbol == "HUSDT"
    assert out.parsed.leverage == 50
    assert out.parsed.side.value == "LONG"


def test_private_parser_accepts_chinese_hashtag_symbol() -> None:
    parser = PrivateChannelParser(_build_config())
    text = (
        "🖥 交易信號 🖥\n\n"
        "#龙虾（10x做多🚀🚀🚀）\n\n"
        "✏️進場位：市價0.017500\n\n"
        "👁 盈利位：0.019809—0.023879—0.030354\n\n"
        "❌止損位：0.015170"
    )

    out = parser.parse(
        text=text,
        timestamp=datetime(2026, 3, 16, 3, 48, 37, tzinfo=timezone.utc),
        image_path=None,
        fallback_symbol="ETHUSDT",
        thread_id=125,
        is_root=True,
    )
    assert isinstance(out.parsed, EntrySignal)
    assert out.parsed.symbol == "龙虾USDT"
    assert out.parsed.entry_type == EntryType.MARKET
    assert out.parsed.entry_points == [0.0175]


def test_private_parser_manage_add_defaults_to_100pct() -> None:
    parser = PrivateChannelParser(_build_config())
    out = parser.parse(
        text="#H 补仓，拿住",
        timestamp=datetime(2026, 3, 1, 10, 40, 0, tzinfo=timezone.utc),
        image_path=None,
        fallback_symbol="HUSDT",
        thread_id=8,
        is_root=False,
    )
    assert isinstance(out.parsed, ManageAction)
    assert out.parsed.symbol == "HUSDT"
    assert out.parsed.add_pct == 100


def test_private_parser_market_entry_without_numeric_entry_points() -> None:
    parser = PrivateChannelParser(_build_config())
    text = (
        "#ACU（10x做多🚀🚀🚀）\n\n"
        "✏️進場位：市價\n\n"
        "👁 盈利位：0.15—0.18—0.2\n\n"
        "❌止損位：0.0865"
    )

    out = parser.parse(
        text=text,
        timestamp=datetime(2026, 3, 2, 1, 12, 11, tzinfo=timezone.utc),
        image_path=None,
        fallback_symbol=None,
        thread_id=12,
        is_root=True,
    )
    assert isinstance(out.parsed, EntrySignal)
    assert out.parsed.symbol == "ACUUSDT"
    assert out.parsed.entry_type == EntryType.MARKET
    assert out.parsed.entry_points == []
    assert out.parsed.entry_low == 0.0
    assert out.parsed.entry_high == 0.0


def test_private_parser_accepts_entry_without_tp_and_sl() -> None:
    parser = PrivateChannelParser(_build_config())
    text = (
        "#INX（10x做多）\n"
        "進場位：市價\n"
    )

    out = parser.parse(
        text=text,
        timestamp=datetime(2026, 3, 2, 1, 12, 11, tzinfo=timezone.utc),
        image_path=None,
        fallback_symbol=None,
        thread_id=14,
        is_root=True,
    )
    assert isinstance(out.parsed, EntrySignal)
    assert out.parsed.symbol == "INXUSDT"
    assert out.parsed.entry_type == EntryType.MARKET
    assert out.parsed.take_profit == []
    assert out.parsed.stop_loss is None


def test_private_parser_inline_market_anchor_nearby_phrase_is_entry_signal() -> None:
    parser = PrivateChannelParser(_build_config())
    text = (
        "#Q 市價0.0127附近多 (20X) 留補倉，分批進場\n"
        "當個穩定幣拿著，不能盯盤的帶好防守和止盈。目前第一止盈就定翻倍0.013434"
    )

    out = parser.parse(
        text=text,
        timestamp=datetime(2026, 3, 6, 1, 37, 0, tzinfo=timezone.utc),
        image_path=None,
        fallback_symbol=None,
        thread_id=937,
        is_root=True,
    )
    assert isinstance(out.parsed, EntrySignal)
    assert out.parsed.symbol == "QUSDT"
    assert out.parsed.side.value == "LONG"
    assert out.parsed.leverage == 20
    assert out.parsed.entry_type == EntryType.MARKET
    assert out.parsed.entry_points == [0.0127]
    assert out.parsed.tp_points == [0.013434]


def test_private_parser_root_manage_is_recognized() -> None:
    parser = PrivateChannelParser(_build_config())
    out = parser.parse(
        text="#INX 减仓50%",
        timestamp=datetime(2026, 3, 2, 2, 0, 0, tzinfo=timezone.utc),
        image_path=None,
        fallback_symbol="INXUSDT",
        thread_id=99,
        is_root=True,
    )
    assert isinstance(out.parsed, ManageAction)
    assert out.parsed.symbol == "INXUSDT"
    assert out.parsed.reduce_pct == 50


def test_private_parser_manage_reduce_without_pct_defaults_to_35() -> None:
    parser = PrivateChannelParser(_build_config())
    out = parser.parse(
        text="#INX 减仓",
        timestamp=datetime(2026, 3, 2, 2, 1, 0, tzinfo=timezone.utc),
        image_path=None,
        fallback_symbol="INXUSDT",
        thread_id=99,
        is_root=False,
    )
    assert isinstance(out.parsed, ManageAction)
    assert out.parsed.symbol == "INXUSDT"
    assert out.parsed.reduce_pct == 35


def test_private_parser_market_take_profit_close_all_phrase() -> None:
    parser = PrivateChannelParser(_build_config())
    out = parser.parse(
        text="市價止盈",
        timestamp=datetime(2026, 3, 2, 9, 29, 46, tzinfo=timezone.utc),
        image_path=None,
        fallback_symbol="ACUUSDT",
        thread_id=12,
        is_root=False,
    )
    assert isinstance(out.parsed, ManageAction)
    assert out.parsed.symbol == "ACUUSDT"
    assert out.parsed.reduce_pct == 100


def test_private_parser_exit_addon_phrase_prefers_reduce_not_add() -> None:
    parser = PrivateChannelParser(_build_config())
    out = parser.parse(
        text="🚩AXS🚩\n\n🟡減掉補倉\n🟡收益:+106.20%",
        timestamp=datetime(2026, 3, 3, 16, 17, 53, tzinfo=timezone.utc),
        image_path=None,
        fallback_symbol="AXSUSDT",
        thread_id=36,
        is_root=False,
    )
    assert isinstance(out.parsed, ManageAction)
    assert out.parsed.symbol == "AXSUSDT"
    assert out.parsed.reduce_pct == 35
    assert out.parsed.add_pct is None


def test_private_parser_exit_addon_composite_phrase_prefers_reduce_not_add() -> None:
    parser = PrivateChannelParser(_build_config())
    out = parser.parse(
        text="🚩 SAHARA 🚩\n\n🟡補倉進，出補倉\n🟡收益:+127.69%",
        timestamp=datetime(2026, 3, 3, 8, 39, 30, tzinfo=timezone.utc),
        image_path=None,
        fallback_symbol="SAHARAUSDT",
        thread_id=34,
        is_root=False,
    )
    assert isinstance(out.parsed, ManageAction)
    assert out.parsed.symbol == "SAHARAUSDT"
    assert out.parsed.reduce_pct == 35
    assert out.parsed.add_pct is None


class _FakeLLM:
    def parse_signal(self, sanitized_text: str) -> dict:  # noqa: ARG002
        return {
            "kind": "ENTRY_SIGNAL",
            "symbol": "INXUSDT",
            "side": "LONG",
            "leverage": 10,
            "entry": {"type": "MARKET", "low": 0.1, "high": 0.1},
            "manage": {"reduce_pct": None, "add_pct": None, "move_sl_to_be": None, "tp": []},
            "confidence": 0.86,
            "notes": "llm fallback",
        }


class _FlakyLLM:
    def __init__(self) -> None:
        self.calls = 0

    def parse_signal(self, sanitized_text: str) -> dict:  # noqa: ARG002
        self.calls += 1
        if self.calls == 1:
            return {
                "symbol": "PHA",
                "action": "sell",
                "order_type": "market",
                "price": 0.037,
                "confidence": "high",
            }
        return {
            "kind": "ENTRY_SIGNAL",
            "symbol": "PHAUSDT",
            "side": "SHORT",
            "leverage": 10,
            "entry": {"type": "MARKET", "low": 0.037, "high": 0.037},
            "manage": {"reduce_pct": None, "add_pct": None, "move_sl_to_be": None, "tp": []},
            "confidence": 0.82,
            "notes": "retry success",
        }


def test_private_parser_prefers_llm_fallback_on_ignored_root() -> None:
    parser = PrivateChannelParser(_build_config())
    parser._llm = _FakeLLM()
    out = parser.parse(
        text="这是一条格式不完整的根消息",
        timestamp=datetime(2026, 3, 2, 2, 10, 0, tzinfo=timezone.utc),
        image_path=None,
        fallback_symbol=None,
        thread_id=100,
        is_root=True,
        prefer_llm_fallback=True,
    )
    assert isinstance(out.parsed, EntrySignal)
    assert out.parse_source == "LLM_PRIVATE"
    assert out.parsed.symbol == "INXUSDT"


def test_private_parser_llm_retry_once_on_schema_failure() -> None:
    parser = PrivateChannelParser(_build_config())
    flaky = _FlakyLLM()
    parser._llm = flaky
    out = parser.parse(
        text="#PHA 市價0.037附近空",
        timestamp=datetime(2026, 3, 4, 1, 36, 23, tzinfo=timezone.utc),
        image_path=None,
        fallback_symbol=None,
        thread_id=13508,
        is_root=True,
        prefer_llm_fallback=True,
    )
    assert flaky.calls == 2
    assert isinstance(out.parsed, EntrySignal)
    assert out.parse_source == "LLM_PRIVATE"
    assert out.parsed.symbol == "PHAUSDT"
    assert out.parsed.side.value == "SHORT"
