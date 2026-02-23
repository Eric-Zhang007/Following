from datetime import datetime, timezone

from trader.config import AppConfig
from trader.llm_parser import HybridSignalParser, ParseOutcome
from trader.models import EntrySignal, EntryType, NeedsManual, ParsedKind, Side
from trader.store import SQLiteStore


class FakeVLMParser:
    def __init__(self, outcome: ParseOutcome) -> None:
        self.outcome = outcome
        self.calls = 0

    def parse(self, **kwargs):
        self.calls += 1
        return self.outcome


class _logger_stub:
    def warning(self, *args, **kwargs):
        return None


def _config(vlm_enabled: bool = True) -> AppConfig:
    return AppConfig.model_validate(
        {
            "dry_run": True,
            "listener": {"mode": "telegram"},
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
                "symbol_policy": "ALLOWLIST",
                "symbol_whitelist": ["BTCUSDT", "CYBERUSDT"],
                "symbol_blacklist": [],
                "require_exchange_symbol": True,
                "min_usdt_volume_24h": None,
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
                "enabled": False,
                "mode": "rules_only",
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
            "vlm": {
                "enabled": vlm_enabled,
                "provider": "nim",
                "model": "x",
                "api_key_env": "NIM_API_KEY",
                "base_url": "https://example.com/v1",
                "timeout_seconds": 20,
                "max_retries": 1,
                "confidence_threshold": 0.8,
                "below_threshold_action": "notify_only",
            },
        }
    )


def test_image_post_forces_vlm_branch_even_when_rules_complete(tmp_path) -> None:
    config = _config(vlm_enabled=True)
    store = SQLiteStore(str(tmp_path / "vlm_branch.db"))

    vlm_outcome = ParseOutcome(
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
        parse_source="VLM",
        confidence=0.9,
    )
    fake_vlm = FakeVLMParser(vlm_outcome)

    parser = HybridSignalParser(config, store, logger=_logger_stub(), llm_parser=None, vlm_parser=fake_vlm)
    outcome = parser.parse(
        chat_id=1,
        message_id=11,
        version=1,
        text_hash="def",
        text="#BTC/USDT (10x long) 入场: 限价62000-62500",
        source_key="1",
        fallback_symbol=None,
        timestamp=datetime.now(timezone.utc),
        image_bytes=b"fake_image",
    )

    assert outcome.parse_source == "VLM"
    assert fake_vlm.calls == 1


def test_vlm_can_return_needs_manual_for_incomplete_entry(tmp_path) -> None:
    config = _config(vlm_enabled=True)
    store = SQLiteStore(str(tmp_path / "vlm_manual.db"))

    fake_vlm = FakeVLMParser(
        ParseOutcome(
            parsed=NeedsManual(
                kind=ParsedKind.NEEDS_MANUAL,
                raw_text="raw",
                reason="incomplete_entry_fields",
                missing_fields=["symbol", "entry.low", "entry.high"],
                timestamp=datetime.now(timezone.utc),
            ),
            parse_source="VLM",
            confidence=0.5,
        )
    )

    parser = HybridSignalParser(config, store, logger=_logger_stub(), llm_parser=None, vlm_parser=fake_vlm)
    outcome = parser.parse(
        chat_id=1,
        message_id=12,
        version=1,
        text_hash="ghi",
        text="图里有交易计划",
        source_key="1",
        fallback_symbol=None,
        timestamp=datetime.now(timezone.utc),
        image_bytes=b"fake_image",
    )

    assert outcome.parse_source == "VLM"
    assert isinstance(outcome.parsed, NeedsManual)
