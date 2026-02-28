from __future__ import annotations

from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator


class TelegramConfig(BaseModel):
    api_id: int | None = None
    api_hash: str | None = None
    session_name: str = "ivan_listener"
    channel_id: int | None = None
    channel_title_hint: str | None = None
    accept_only_after_startup: bool = True
    enable_edited_events: bool = True
    # Legacy field kept for compatibility with old telegram mode.
    channel: str = "@IvanCryptotalk"
    notify_chat_id: int | None = None


class ListenerConfig(BaseModel):
    mode: Literal["telegram_private", "telegram", "web_preview"] = "telegram"
    polling_seconds: int = Field(default=5, ge=1, le=300)
    target_url: str = "https://t.me/s/IvanCryptotalk"
    user_agent: str = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0 Safari/537.36 FollowingBot/1.0"
    )
    request_timeout_seconds: int = Field(default=15, ge=1, le=120)
    max_retries: int = Field(default=3, ge=0, le=10)
    backoff_seconds: float = Field(default=1.0, ge=0.1, le=30)
    web_chat_id: int = 777001


class BitgetConfig(BaseModel):
    base_url: str = "https://api.bitget.com"
    api_key: str = ""
    api_secret: str = ""
    passphrase: str = ""
    product_type: str = "USDT-FUTURES"
    margin_mode: Literal["isolated", "crossed"] = "isolated"
    position_mode: Literal["one_way_mode", "hedge_mode"] = "one_way_mode"
    force: Literal["gtc", "ioc", "fok", "post_only"] = "gtc"
    plan_orders_probe_on_startup: bool = True
    plan_orders_capability_ttl_seconds: int = Field(default=300, ge=10, le=86400)
    plan_orders_probe_timeout_seconds: int = Field(default=6, ge=1, le=30)
    plan_orders_probe_safe_mode_on_failure: bool = True


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
    class HardInvariantsConfig(BaseModel):
        require_stoploss: bool = True
        max_concurrent_trades_enforced: bool = True
        kill_switch_enforced: bool = True
        no_size_zero_orders: bool = True

    enabled: bool = True
    max_account_drawdown_pct: float = Field(default=0.15, ge=0, le=1)
    account_risk_per_trade: float = Field(default=0.003, ge=0, le=1)
    max_leverage: int = Field(default=10, ge=1, le=125)
    leverage_policy: Literal["CAP", "REJECT"] = "CAP"
    default_stop_loss_pct: float = Field(default=0.006, gt=0)
    hard_stop_loss_required: bool = True
    max_entry_slippage_pct: float = Field(default=0.003, ge=0)
    max_notional_per_trade: float = 200.0
    entry_slippage_pct: float | None = None
    max_open_positions: int = Field(default=3, ge=1, le=100)
    cooldown_seconds: int = 300
    min_signal_quality: float = Field(default=0.8, ge=0, le=1)
    allow_symbols_policy: Literal["ALLOWLIST", "ALLOW_ALL"] = "ALLOWLIST"
    symbol_allowlist: list[str] = Field(default_factory=list)
    symbol_blacklist: list[str] = Field(default_factory=list)
    min_24h_usdt_volume: float | None = None
    consecutive_stoploss_limit: int = Field(default=3, ge=1, le=20)
    stoploss_cooldown_seconds: int = Field(default=3600, ge=1, le=86400)
    max_total_margin_used_pct: float = Field(default=0.35, ge=0, le=1)
    max_liquidation_distance_pct: float = Field(default=0.01, ge=0, le=1)
    assumed_equity_usdt: float = 1000.0

    # Legacy flat settings are kept for backward compatibility.
    class StopLossConfig(BaseModel):
        must_exist: bool = True
        max_time_without_sl_seconds: int = Field(default=10, ge=1, le=600)
        sl_order_type: Literal["trigger", "plan", "local_guard"] = "local_guard"
        trigger_price_type: Literal["mark", "last"] = "mark"
        break_even_buffer_pct: float = Field(default=0.0005, ge=0, le=0.02)
        emergency_close_if_sl_place_fails: bool = True

    class CircuitBreakerConfig(BaseModel):
        consecutive_stop_losses: int = Field(default=3, ge=1, le=20)
        cooldown_seconds: int = Field(default=3600, ge=1, le=86400)
        api_error_burst: int = Field(default=10, ge=1, le=200)
        api_error_window_seconds: int = Field(default=120, ge=1, le=3600)

    stoploss: StopLossConfig = Field(default_factory=StopLossConfig)
    circuit_breaker: CircuitBreakerConfig = Field(default_factory=CircuitBreakerConfig)
    hard_invariants: HardInvariantsConfig = Field(default_factory=HardInvariantsConfig)

    @field_validator("symbol_allowlist")
    @classmethod
    def normalize_symbol_allowlist(cls, value: list[str]) -> list[str]:
        return [v.strip().upper().replace("/", "") for v in value if v.strip()]

    @field_validator("symbol_blacklist")
    @classmethod
    def normalize_symbol_blacklist(cls, value: list[str]) -> list[str]:
        return [v.strip().upper().replace("/", "") for v in value if v.strip()]

    @model_validator(mode="after")
    def sync_legacy_and_nested(self) -> "RiskConfig":
        if "stoploss" not in self.model_fields_set:
            self.stoploss.must_exist = self.hard_stop_loss_required
        else:
            self.hard_stop_loss_required = self.stoploss.must_exist

        if "circuit_breaker" not in self.model_fields_set:
            self.circuit_breaker.consecutive_stop_losses = self.consecutive_stoploss_limit
            self.circuit_breaker.cooldown_seconds = self.stoploss_cooldown_seconds
        else:
            self.consecutive_stoploss_limit = self.circuit_breaker.consecutive_stop_losses
            self.stoploss_cooldown_seconds = self.circuit_breaker.cooldown_seconds
        return self


class LoggingConfig(BaseModel):
    level: str = "INFO"
    file: str = "trader.log"
    rich: bool = True


class StorageConfig(BaseModel):
    db_path: str = "trader.db"
    media_dir: str = "media"


class ExecutionConfig(BaseModel):
    margin_mode: Literal["cross", "isolated"] = "cross"
    per_trade_margin_usdt: float = Field(default=25, gt=0)
    entry_split_ratio: list[int] = Field(default_factory=lambda: [1, 2])
    order_type: Literal["limit_only"] = "limit_only"
    max_concurrent_trades: int = Field(default=3, ge=1, le=100)
    place_tp_on_fill: bool = True
    be_reduce_on_two_entries: bool = True
    be_reduce_pct: float = Field(default=50.0, gt=0, le=100)
    be_reduce_trigger_type: Literal["mark", "last"] = "mark"
    be_reduce_buffer_pct: float = Field(default=0.0, ge=0, le=0.05)
    limit_price_strategy: Literal["MID", "LOW", "HIGH"] = "MID"
    require_order_ack: bool = True
    ack_timeout_seconds: int = Field(default=8, ge=1, le=120)
    max_submit_retries: int = Field(default=2, ge=0, le=10)
    prefer_post_only_limit: bool = False
    close_on_invariant_violation: bool = True

    @field_validator("entry_split_ratio")
    @classmethod
    def validate_entry_split_ratio(cls, value: list[int]) -> list[int]:
        if not value:
            raise ValueError("entry_split_ratio must not be empty")
        if any(v <= 0 for v in value):
            raise ValueError("entry_split_ratio values must be > 0")
        return value


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


class VLMConfig(BaseModel):
    enabled: bool = False
    provider: Literal["nim", "kimi"] = "nim"
    model: str = "default"
    api_key_env: str = "NIM_API_KEY"
    base_url: str | None = None
    timeout_seconds: int = Field(default=20, ge=1, le=120)
    max_retries: int = Field(default=2, ge=0, le=5)
    confidence_threshold: float = Field(default=0.8, ge=0, le=1)
    below_threshold_action: Literal["notify_only"] = "notify_only"


class MonitorPollIntervalsConfig(BaseModel):
    account_seconds: int = Field(default=5, ge=1, le=300)
    positions_seconds: int = Field(default=3, ge=1, le=300)
    open_orders_seconds: int = Field(default=3, ge=1, le=300)
    funding_seconds: int = Field(default=60, ge=1, le=3600)
    contracts_seconds: int = Field(default=3600, ge=30, le=86400)
    reconciler_seconds: int = Field(default=2, ge=1, le=300)
    risk_daemon_seconds: int = Field(default=2, ge=1, le=60)


class MonitorPriceFeedConfig(BaseModel):
    mode: Literal["ws", "rest"] = "rest"
    interval_seconds: int = Field(default=2, ge=1, le=60)
    ws_reconnect_seconds: int = Field(default=3, ge=1, le=60)
    max_stale_seconds: int = Field(default=5, ge=1, le=120)
    max_ws_parse_error_ratio: float = Field(default=0.2, ge=0.0, le=1.0)
    ws_required_for_local_guard: bool = True
    required_symbols: list[str] = Field(default_factory=list)
    rest_fallback_action_when_local_guard: Literal["notify_only", "safe_mode"] = "safe_mode"

    @field_validator("required_symbols")
    @classmethod
    def normalize_required_symbols(cls, value: list[str]) -> list[str]:
        return [v.strip().upper().replace("/", "") for v in value if v.strip()]


class MonitorHealthConfig(BaseModel):
    host: str = "127.0.0.1"
    port: int = Field(default=8080, ge=1, le=65535)
    enable_metrics: bool = True


class MonitorAlertsConfig(BaseModel):
    level: Literal["INFO", "WARN", "ERROR", "CRITICAL"] = "INFO"
    telegram_enabled: bool = False
    telegram_chat_id: int | None = None


class MonitorConfig(BaseModel):
    enabled: bool = True
    poll_intervals: MonitorPollIntervalsConfig = Field(default_factory=MonitorPollIntervalsConfig)
    price_feed: MonitorPriceFeedConfig = Field(default_factory=MonitorPriceFeedConfig)
    health: MonitorHealthConfig = Field(default_factory=MonitorHealthConfig)
    alerts: MonitorAlertsConfig = Field(default_factory=MonitorAlertsConfig)


class EmailAlertConfig(BaseModel):
    enabled: bool = False
    smtp_host: str = ""
    smtp_port: int = Field(default=587, ge=1, le=65535)
    smtp_user: str = ""
    smtp_pass_env: str = "SMTP_PASS"
    from_addr: str = ""
    to_addrs: list[str] = Field(default_factory=list)
    send_on: list[str] = Field(
        default_factory=lambda: [
            "RISK_MODE_DISABLED",
            "CROSS_MARGIN",
            "HIGH_LEVERAGE",
            "STOPLOSS_PLACE_FAIL",
            "PANIC_CLOSE",
            "PLAN_ORDER_FALLBACK",
            "WS_DEGRADED",
        ]
    )


class AlertsConfig(BaseModel):
    email: EmailAlertConfig = Field(default_factory=EmailAlertConfig)


class AppConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    dry_run: bool = True
    listener: ListenerConfig = Field(default_factory=ListenerConfig)
    telegram: TelegramConfig
    bitget: BitgetConfig
    filters: FiltersConfig
    risk: RiskConfig
    logging: LoggingConfig
    storage: StorageConfig = Field(default_factory=StorageConfig)
    execution: ExecutionConfig = Field(default_factory=ExecutionConfig)
    llm: LLMConfig = Field(default_factory=LLMConfig)
    vlm: VLMConfig = Field(default_factory=VLMConfig)
    monitor: MonitorConfig = Field(default_factory=MonitorConfig)
    alerts: AlertsConfig = Field(default_factory=AlertsConfig)

    @model_validator(mode="after")
    def validate_listener_requirements(self) -> "AppConfig":
        if self.listener.mode in {"telegram", "telegram_private"}:
            if not self.telegram.api_id or not self.telegram.api_hash:
                raise ValueError(
                    "telegram.api_id and telegram.api_hash are required when listener.mode in {telegram, telegram_private}"
                )
        if self.listener.mode == "telegram_private":
            if self.telegram.channel_id is None:
                raise ValueError("telegram.channel_id is required when listener.mode=telegram_private")
        return self


def load_config(path: str | Path) -> AppConfig:
    config_path = Path(path)
    if not config_path.exists():
        raise FileNotFoundError(f"Config not found: {config_path}")

    data = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    try:
        return AppConfig.model_validate(data)
    except ValidationError as exc:
        raise ValueError(f"Invalid config: {exc}") from exc
