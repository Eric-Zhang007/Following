from __future__ import annotations

from datetime import datetime, timedelta, timezone

from trader.config import AppConfig
from trader.models import EntrySignal, EntryType, ManageAction, RiskDecision
from trader.symbol_registry import SymbolRegistry


class RiskManager:
    def __init__(self, config: AppConfig, symbol_registry: SymbolRegistry | None = None) -> None:
        self.config = config
        self.symbol_registry = symbol_registry
        self._peak_equity: float | None = None
        self._consecutive_stoplosses = 0
        self._stoploss_cooldown_until: datetime | None = None

    def evaluate_entry(
        self,
        signal: EntrySignal,
        current_price: float,
        account_equity: float,
        now: datetime,
        within_cooldown: bool,
        open_positions_count: int = 0,
        signal_quality: float = 1.0,
    ) -> RiskDecision:
        symbol = signal.symbol.upper()
        side = signal.side.value
        warnings: list[str] = []

        if not self.config.risk.enabled:
            entry_price = self._pick_limit_price(signal)
            if entry_price <= 0:
                return RiskDecision.reject("entry_price <= 0")
            stop_loss_price, stop_distance = self._resolve_stop_loss(signal, entry_price)
            if self.config.risk.hard_invariants.require_stoploss and (stop_loss_price is None or stop_distance <= 0):
                return RiskDecision.reject("hard invariant require_stoploss failed")
            leverage = signal.leverage or 1
            notional = min(
                float(self.config.risk.max_notional_per_trade),
                float(max(account_equity, 0.0) * max(self.config.risk.account_risk_per_trade, 0.0) * max(leverage, 1)),
            )
            quantity = (notional / entry_price) if entry_price > 0 else 0.0
            if self.config.risk.hard_invariants.no_size_zero_orders and quantity <= 0:
                return RiskDecision.reject("hard invariant no_size_zero_orders failed")
            return RiskDecision(
                approved=True,
                symbol=symbol,
                side=signal.side,
                leverage=leverage,
                notional=notional,
                quantity=quantity,
                entry_price=entry_price,
                stop_loss_price=stop_loss_price,
                stop_distance_ratio=stop_distance,
                quality_score=signal_quality,
                warnings=["risk.enabled=false bypassed strategy filters"],
            )

        if symbol in self._symbol_blacklist():
            return RiskDecision.reject(f"symbol in blacklist: {symbol}")

        symbol_policy = self._symbol_policy()
        if symbol_policy == "ALLOWLIST":
            allowlist = self._symbol_allowlist()
            if symbol not in allowlist:
                return RiskDecision.reject(f"symbol not in whitelist: {symbol}")
        elif symbol_policy == "ALLOW_ALL":
            if self.config.filters.require_exchange_symbol:
                if self.symbol_registry is None:
                    return RiskDecision.reject("symbol registry unavailable while require_exchange_symbol=true")
                if not self.symbol_registry.is_tradable(symbol):
                    return RiskDecision.reject(f"symbol not tradable on Bitget USDT futures: {symbol}")
        else:
            return RiskDecision.reject(f"unsupported symbol policy: {symbol_policy}")

        min_volume = self._min_24h_volume()
        if min_volume is not None:
            if self.symbol_registry is None:
                return RiskDecision.reject("symbol registry unavailable while min_usdt_volume_24h is enabled")
            volume = self.symbol_registry.get_24h_volume(symbol)
            if volume is None:
                return RiskDecision.reject(f"24h volume unavailable for symbol: {symbol}")
            if volume < min_volume:
                return RiskDecision.reject(f"24h volume {volume:.2f} below threshold {min_volume:.2f} for {symbol}")

        if side not in self.config.filters.allow_sides:
            return RiskDecision.reject(f"side not allowed: {side}")

        leverage = signal.leverage or 1
        max_leverage = self._max_leverage()
        leverage_policy = self._leverage_policy()
        if leverage > max_leverage:
            if leverage_policy == "REJECT":
                return RiskDecision.reject(
                    f"leverage {leverage} exceeds max_leverage {max_leverage}"
                )
            warnings.append(f"leverage capped from {leverage} to {max_leverage}")
            leverage = max_leverage

        signal_time = signal.timestamp
        if signal_time:
            if signal_time.tzinfo is None:
                signal_time = signal_time.replace(tzinfo=timezone.utc)
            age_sec = (now - signal_time).total_seconds()
            if age_sec > self.config.filters.max_signal_age_seconds:
                return RiskDecision.reject(f"signal too old: {age_sec:.1f}s")

        if self._stoploss_cooldown_until is not None and now < self._stoploss_cooldown_until:
            return RiskDecision.reject(
                f"circuit breaker cooldown active until {self._stoploss_cooldown_until.isoformat()}"
            )

        if within_cooldown:
            return RiskDecision.reject(
                f"cooldown active for {symbol} {side}, {self.config.risk.cooldown_seconds}s"
            )

        if open_positions_count >= self.config.risk.max_open_positions:
            return RiskDecision.reject(
                f"max_open_positions reached: {open_positions_count}/{self.config.risk.max_open_positions}"
            )

        if signal_quality < self.config.risk.min_signal_quality:
            return RiskDecision.reject(
                f"signal quality {signal_quality:.2f} below min_signal_quality {self.config.risk.min_signal_quality:.2f}"
            )

        drawdown = self._compute_drawdown(account_equity)
        if drawdown > self.config.risk.max_account_drawdown_pct:
            return RiskDecision.reject(
                f"drawdown {drawdown:.4f} exceeds max_account_drawdown_pct {self.config.risk.max_account_drawdown_pct:.4f}"
            )

        if current_price <= 0:
            return RiskDecision.reject("invalid market price")

        if signal.entry_type == EntryType.LIMIT:
            if current_price < signal.entry_low:
                deviation = (signal.entry_low - current_price) / signal.entry_low
            elif current_price > signal.entry_high:
                deviation = (current_price - signal.entry_high) / signal.entry_high
            else:
                deviation = 0.0

            max_slippage = self._ratio_from_percent_or_ratio(
                self.config.risk.entry_slippage_pct
                if self.config.risk.entry_slippage_pct is not None
                else self.config.risk.max_entry_slippage_pct
            )
            if deviation > max_slippage:
                return RiskDecision.reject(
                    f"price deviation {deviation:.4f} exceeds max_entry_slippage_pct {max_slippage:.4f}"
                )

        entry_price = self._pick_limit_price(signal)
        if entry_price <= 0:
            return RiskDecision.reject("entry_price <= 0")

        stop_loss_price, stop_distance = self._resolve_stop_loss(signal, entry_price)
        if stop_loss_price is None or stop_distance <= 0:
            return RiskDecision.reject("stop loss unavailable or invalid")

        if self.config.risk.hard_stop_loss_required and stop_loss_price is None:
            return RiskDecision.reject("hard_stop_loss_required=true but stop loss is unavailable")

        max_loss = account_equity * self.config.risk.account_risk_per_trade
        quantity = max_loss / (stop_distance * entry_price)
        if quantity <= 0:
            return RiskDecision.reject("quantity <= 0")

        notional = quantity * entry_price
        if notional > self.config.risk.max_notional_per_trade:
            notional = self.config.risk.max_notional_per_trade
            quantity = notional / entry_price

        return RiskDecision(
            approved=True,
            reason=None,
            symbol=symbol,
            side=signal.side,
            leverage=leverage,
            notional=notional,
            quantity=quantity,
            entry_price=entry_price,
            stop_loss_price=stop_loss_price,
            stop_distance_ratio=stop_distance,
            quality_score=signal_quality,
            warnings=warnings,
        )

    def evaluate_manage(self, action: ManageAction) -> RiskDecision:
        if not action.symbol:
            return RiskDecision.reject("manage action missing symbol and cannot be inferred")

        if action.reduce_pct is not None and (action.reduce_pct <= 0 or action.reduce_pct > 100):
            return RiskDecision.reject(f"invalid reduce_pct: {action.reduce_pct}")

        if action.reduce_pct is None and not action.move_sl_to_be and action.tp_price is None:
            return RiskDecision.reject("manage action has no executable fields")

        return RiskDecision(approved=True, symbol=action.symbol)

    def record_stop_loss(self, now: datetime) -> None:
        self._consecutive_stoplosses += 1
        if self._consecutive_stoplosses >= self.config.risk.consecutive_stoploss_limit:
            self._stoploss_cooldown_until = now + timedelta(seconds=self.config.risk.stoploss_cooldown_seconds)

    def record_non_stoploss_close(self) -> None:
        self._consecutive_stoplosses = 0

    def _compute_drawdown(self, account_equity: float) -> float:
        if account_equity <= 0:
            return 1.0
        if self._peak_equity is None:
            self._peak_equity = account_equity
            return 0.0
        if account_equity > self._peak_equity:
            self._peak_equity = account_equity
            return 0.0
        return (self._peak_equity - account_equity) / self._peak_equity

    def _resolve_stop_loss(self, signal: EntrySignal, entry_price: float) -> tuple[float | None, float]:
        if entry_price <= 0:
            return None, 0.0

        if signal.stop_loss is not None:
            stop_price = float(signal.stop_loss)
            if signal.side.value == "LONG":
                if stop_price >= entry_price:
                    return None, 0.0
            else:
                if stop_price <= entry_price:
                    return None, 0.0
            return stop_price, abs(entry_price - stop_price) / entry_price

        default_ratio = self._ratio_from_percent_or_ratio(self.config.risk.default_stop_loss_pct)
        if default_ratio <= 0:
            return None, 0.0
        if signal.side.value == "LONG":
            stop_price = entry_price * (1 - default_ratio)
        else:
            stop_price = entry_price * (1 + default_ratio)
        return stop_price, default_ratio

    def _symbol_policy(self) -> str:
        if self.config.risk.symbol_allowlist:
            return self.config.risk.allow_symbols_policy
        return self.config.filters.symbol_policy

    def _symbol_allowlist(self) -> set[str]:
        risk_allowlist = set(self.config.risk.symbol_allowlist)
        if risk_allowlist:
            return risk_allowlist
        return set(self.config.filters.symbol_whitelist)

    def _symbol_blacklist(self) -> set[str]:
        return set(self.config.filters.symbol_blacklist) | set(self.config.risk.symbol_blacklist)

    def _min_24h_volume(self) -> float | None:
        if self.config.risk.min_24h_usdt_volume is not None:
            return self.config.risk.min_24h_usdt_volume
        return self.config.filters.min_usdt_volume_24h

    def _max_leverage(self) -> int:
        if self.config.risk.max_leverage > 0:
            return self.config.risk.max_leverage
        return self.config.filters.max_leverage

    def _leverage_policy(self) -> str:
        if self.config.risk.leverage_policy == "REJECT":
            return "REJECT"
        if self.config.filters.leverage_over_limit_action == "REJECT":
            return "REJECT"
        if self.config.risk.leverage_policy == "CAP":
            return "CAP"
        return "REJECT" if self.config.filters.leverage_over_limit_action == "REJECT" else "CAP"

    @staticmethod
    def _ratio_from_percent_or_ratio(value: float) -> float:
        if value <= 0:
            return 0.0
        if value >= 1:
            return value / 100.0
        if value > 0.05:
            return value / 100.0
        return value

    def _pick_limit_price(self, signal: EntrySignal) -> float:
        strategy = self.config.execution.limit_price_strategy
        if signal.entry_type == EntryType.MARKET:
            return signal.entry_high

        if strategy == "LOW":
            return signal.entry_low
        if strategy == "HIGH":
            return signal.entry_high
        return (signal.entry_low + signal.entry_high) / 2
