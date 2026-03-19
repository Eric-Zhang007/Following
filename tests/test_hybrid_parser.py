from datetime import datetime, timezone

from trader.config import AppConfig
from trader.llm_parser import HybridSignalParser, LLMParseError, ParseOutcome
from trader.models import EntrySignal, EntryType, ManageAction, NonSignal, ParsedKind, Side
from trader.store import SQLiteStore


class FakeLLMParser:
    def __init__(self, outcome: ParseOutcome | None = None, error: Exception | None = None) -> None:
        self.outcome = outcome
        self.error = error
        self.calls = 0

    def parse(self, **kwargs):
        self.calls += 1
        if self.error is not None:
            raise self.error
        return self.outcome


def build_config(mode: str, enabled: bool = True) -> AppConfig:
    return AppConfig.model_validate(
        {
            "dry_run": True,
            "telegram": {
                "api_id": 1,
                "api_hash": "x",
                "session_name": "s",
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
                "symbol_whitelist": ["BTCUSDT", "CYBERUSDT"],
                "max_leverage": 10,
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
            "llm": {
                "enabled": enabled,
                "mode": mode,
                "provider": "openai",
                "model": "gpt-4.1-mini",
                "api_key_env": "OPENAI_API_KEY",
                "base_url": None,
                "timeout_seconds": 15,
                "max_retries": 1,
                "confidence_threshold": 0.75,
                "require_confirmation_below_threshold": True,
                "redact_patterns": [r"(?i)secret\\s*[:=]\\s*\\S+"],
            },
        }
    )


def test_rules_only_mode_never_calls_llm(tmp_path) -> None:
    config = build_config(mode="rules_only", enabled=True)
    store = SQLiteStore(str(tmp_path / "rules_only.db"))
    fake_llm = FakeLLMParser(
        error=AssertionError("llm should not be called in rules_only mode"),
    )

    parser = HybridSignalParser(config, store, logger=_logger_stub(), llm_parser=fake_llm)
    outcome = parser.parse(
        chat_id=1,
        message_id=10,
        version=1,
        text_hash="abc",
        text="#CYBER/USDT（25x做空） 进场：限价0.73-0.74",
        source_key="1",
        fallback_symbol=None,
        timestamp=datetime.now(timezone.utc),
    )

    assert isinstance(outcome.parsed, EntrySignal)
    assert outcome.parse_source == "RULES"
    assert fake_llm.calls == 0


def test_hybrid_calls_llm_when_rules_incomplete(tmp_path) -> None:
    config = build_config(mode="hybrid", enabled=True)
    store = SQLiteStore(str(tmp_path / "hybrid.db"))

    llm_outcome = ParseOutcome(
        parsed=EntrySignal(
            kind=ParsedKind.ENTRY_SIGNAL,
            raw_text="raw",
            symbol="CYBERUSDT",
            quote="USDT",
            side=Side.SHORT,
            leverage=10,
            entry_type=EntryType.LIMIT,
            entry_low=0.73,
            entry_high=0.74,
            timestamp=datetime.now(timezone.utc),
        ),
        parse_source="LLM",
        confidence=0.9,
    )
    fake_llm = FakeLLMParser(outcome=llm_outcome)

    parser = HybridSignalParser(config, store, logger=_logger_stub(), llm_parser=fake_llm)
    outcome = parser.parse(
        chat_id=1,
        message_id=11,
        version=1,
        text_hash="def",
        text="看起来准备做空cyber，等待机会",
        source_key="1",
        fallback_symbol=None,
        timestamp=datetime.now(timezone.utc),
    )

    assert outcome.parse_source == "LLM"
    assert isinstance(outcome.parsed, EntrySignal)
    assert fake_llm.calls == 1


def test_hybrid_keeps_rules_when_complete(tmp_path) -> None:
    config = build_config(mode="hybrid", enabled=True)
    store = SQLiteStore(str(tmp_path / "hybrid_rules.db"))
    fake_llm = FakeLLMParser(error=AssertionError("llm should not be called when rules are complete"))

    parser = HybridSignalParser(config, store, logger=_logger_stub(), llm_parser=fake_llm)
    outcome = parser.parse(
        chat_id=1,
        message_id=12,
        version=1,
        text_hash="ghi",
        text="#BTC/USDT (10x long) 入场: 限价62000-62500",
        source_key="1",
        fallback_symbol=None,
        timestamp=datetime.now(timezone.utc),
    )

    assert outcome.parse_source == "RULES"
    assert isinstance(outcome.parsed, EntrySignal)
    assert fake_llm.calls == 0


def test_llm_only_returns_error_outcome_on_llm_failure(tmp_path) -> None:
    config = build_config(mode="llm_only", enabled=True)
    store = SQLiteStore(str(tmp_path / "llm_only.db"))
    fake_llm = FakeLLMParser(error=LLMParseError("boom"))

    parser = HybridSignalParser(config, store, logger=_logger_stub(), llm_parser=fake_llm)
    outcome = parser.parse(
        chat_id=1,
        message_id=13,
        version=1,
        text_hash="jkl",
        text="#ETH/USDT long",
        source_key="1",
        fallback_symbol=None,
        timestamp=datetime.now(timezone.utc),
    )

    assert outcome.parse_source == "LLM_ERROR"
    assert outcome.confidence == 0.0
    assert isinstance(outcome.parsed, NonSignal)
    assert fake_llm.calls == 1


def test_llm_only_uses_llm_output(tmp_path) -> None:
    config = build_config(mode="llm_only", enabled=True)
    store = SQLiteStore(str(tmp_path / "llm_only_ok.db"))
    fake_llm = FakeLLMParser(
        outcome=ParseOutcome(
            parsed=ManageAction(
                kind=ParsedKind.MANAGE_ACTION,
                raw_text="raw",
                symbol="BTCUSDT",
                reduce_pct=25,
                move_sl_to_be=True,
                tp_price=65000,
                note="",
                timestamp=datetime.now(timezone.utc),
            ),
            parse_source="LLM",
            confidence=0.82,
        )
    )

    parser = HybridSignalParser(config, store, logger=_logger_stub(), llm_parser=fake_llm)
    outcome = parser.parse(
        chat_id=1,
        message_id=14,
        version=1,
        text_hash="mno",
        text="减仓25%，设保本",
        source_key="1",
        fallback_symbol="BTCUSDT",
        timestamp=datetime.now(timezone.utc),
    )

    assert outcome.parse_source == "LLM"
    assert isinstance(outcome.parsed, ManageAction)
    assert fake_llm.calls == 1


class _logger_stub:
    def warning(self, *args, **kwargs):
        return None
