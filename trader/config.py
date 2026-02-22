from __future__ import annotations

from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator


class TelegramConfig(BaseModel):
    api_id: int
    api_hash: str
    session_name: str = "ivan_listener"
    channel: str = "@IvanCryptotalk"
    notify_chat_id: int | None = None


class BitgetConfig(BaseModel):
    base_url: str = "https://api.bitget.com"
    api_key: str = ""
    api_secret: str = ""
    passphrase: str = ""
    product_type: str = "USDT-FUTURES"
    margin_mode: Literal["isolated", "crossed"] = "isolated"
    position_mode: Literal["one_way_mode", "hedge_mode"] = "one_way_mode"
    force: Literal["gtc", "ioc", "fok", "post_only"] = "gtc"


class FiltersConfig(BaseModel):
    symbol_policy: Literal["ALLOWLIST", "ALLOW_ALL"] = "ALLOWLIST"
    symbol_whitelist: list[str] = Field(default_factory=list)
    symbol_blacklist: list[str] = Field(default_factory=list)
    require_exchange_symbol: bool = True
    min_usdt_volume_24h: float | None = None
    max_leverage: int = 10
    allow_sides: list[Literal["LONG", "SHORT"]] = Field(default_factory=lambda: ["LONG", "SHORT"])
    max_signal_age_seconds: int = 20
    leverage_over_limit_action: Literal["CLAMP", "REJECT"] = "CLAMP"

    @field_validator("symbol_whitelist")
    @classmethod
    def normalize_symbol_whitelist(cls, value: list[str]) -> list[str]:
        return [v.strip().upper().replace("/", "") for v in value if v.strip()]

    @field_validator("symbol_blacklist")
    @classmethod
    def normalize_symbol_blacklist(cls, value: list[str]) -> list[str]:
        return [v.strip().upper().replace("/", "") for v in value if v.strip()]


class RiskConfig(BaseModel):
    account_risk_per_trade: float = 0.005
    max_notional_per_trade: float = 200.0
    entry_slippage_pct: float = 0.3
    cooldown_seconds: int = 300
    default_stop_loss_pct: float = 1.0
    assumed_equity_usdt: float = 1000.0


class LoggingConfig(BaseModel):
    level: str = "INFO"
    file: str = "trader.log"
    rich: bool = True


class StorageConfig(BaseModel):
    db_path: str = "trader.db"


class ExecutionConfig(BaseModel):
    limit_price_strategy: Literal["MID", "LOW", "HIGH"] = "MID"


class LLMConfig(BaseModel):
    enabled: bool = True
    mode: Literal["rules_only", "hybrid", "llm_only"] = "hybrid"
    provider: Literal["openai"] = "openai"
    model: str = "gpt-4.1-mini"
    api_key_env: str = "OPENAI_API_KEY"
    base_url: str | None = None
    timeout_seconds: int = Field(default=15, ge=1, le=120)
    max_retries: int = Field(default=2, ge=0, le=5)
    confidence_threshold: float = Field(default=0.75, ge=0.0, le=1.0)
    require_confirmation_below_threshold: bool = True
    redact_patterns: list[str] = Field(
        default_factory=lambda: [
            r"(?i)api_key\s*[:=]\s*\S+",
            r"(?i)secret\s*[:=]\s*\S+",
        ]
    )


class AppConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    dry_run: bool = True
    telegram: TelegramConfig
    bitget: BitgetConfig
    filters: FiltersConfig
    risk: RiskConfig
    logging: LoggingConfig
    storage: StorageConfig = Field(default_factory=StorageConfig)
    execution: ExecutionConfig = Field(default_factory=ExecutionConfig)
    llm: LLMConfig = Field(default_factory=LLMConfig)


def load_config(path: str | Path) -> AppConfig:
    config_path = Path(path)
    if not config_path.exists():
        raise FileNotFoundError(f"Config not found: {config_path}")

    data = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    try:
        return AppConfig.model_validate(data)
    except ValidationError as exc:
        raise ValueError(f"Invalid config: {exc}") from exc
