from __future__ import annotations

from datetime import datetime, timezone

from trader.config import AppConfig
from trader.models import EntrySignal, EntryType, ManageAction, RiskDecision
from trader.symbol_registry import SymbolRegistry


class RiskManager:
    def __init__(self, config: AppConfig, symbol_registry: SymbolRegistry | None = None) -> None:
        self.config = config
        self.symbol_registry = symbol_registry

    def evaluate_entry(
        self,
        signal: EntrySignal,
        current_price: float,
        account_equity: float,
        now: datetime,
        within_cooldown: bool,
    ) -> RiskDecision:
        symbol = signal.symbol.upper()
        side = signal.side.value

        if symbol in self.config.filters.symbol_blacklist:
            return RiskDecision.reject(f"symbol in blacklist: {symbol}")

        if self.config.filters.symbol_policy == "ALLOWLIST":
            if symbol not in self.config.filters.symbol_whitelist:
                return RiskDecision.reject(f"symbol not in whitelist: {symbol}")
        elif self.config.filters.symbol_policy == "ALLOW_ALL":
            if self.config.filters.require_exchange_symbol:
                if self.symbol_registry is None:
                    return RiskDecision.reject("symbol registry unavailable while require_exchange_symbol=true")
                if not self.symbol_registry.is_tradable(symbol):
                    return RiskDecision.reject(f"symbol not tradable on Bitget USDT futures: {symbol}")
        else:
            return RiskDecision.reject(f"unsupported symbol policy: {self.config.filters.symbol_policy}")

        min_volume = self.config.filters.min_usdt_volume_24h
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
        if leverage > self.config.filters.max_leverage:
            action = self.config.filters.leverage_over_limit_action
            if action == "REJECT":
                return RiskDecision.reject(
                    f"leverage {leverage} exceeds max_leverage {self.config.filters.max_leverage}"
                )
            leverage = self.config.filters.max_leverage

        signal_time = signal.timestamp
        if signal_time:
            if signal_time.tzinfo is None:
                signal_time = signal_time.replace(tzinfo=timezone.utc)
            age_sec = (now - signal_time).total_seconds()
            if age_sec > self.config.filters.max_signal_age_seconds:
                return RiskDecision.reject(f"signal too old: {age_sec:.1f}s")

        if within_cooldown:
            return RiskDecision.reject(
                f"cooldown active for {symbol} {side}, {self.config.risk.cooldown_seconds}s"
            )

        if signal.entry_type == EntryType.LIMIT:
            if current_price < signal.entry_low:
                deviation_pct = ((signal.entry_low - current_price) / signal.entry_low) * 100
            elif current_price > signal.entry_high:
                deviation_pct = ((current_price - signal.entry_high) / signal.entry_high) * 100
            else:
                deviation_pct = 0.0

            if deviation_pct > self.config.risk.entry_slippage_pct:
                return RiskDecision.reject(
                    f"price deviation {deviation_pct:.3f}% exceeds {self.config.risk.entry_slippage_pct}%"
                )

        stop_loss_pct = max(self.config.risk.default_stop_loss_pct, 0.05) / 100
        risk_capital = account_equity * self.config.risk.account_risk_per_trade
        notional_by_risk = risk_capital / stop_loss_pct
        notional = min(notional_by_risk, self.config.risk.max_notional_per_trade)

        if notional <= 0:
            return RiskDecision.reject("notional <= 0 after risk sizing")

        if current_price <= 0:
            return RiskDecision.reject("invalid market price")

        quantity = notional / current_price
        if quantity <= 0:
            return RiskDecision.reject("quantity <= 0")

        entry_price = self._pick_limit_price(signal)

        return RiskDecision(
            approved=True,
            reason=None,
            symbol=symbol,
            side=signal.side,
            leverage=leverage,
            notional=notional,
            quantity=quantity,
            entry_price=entry_price,
        )

    def evaluate_manage(self, action: ManageAction) -> RiskDecision:
        if not action.symbol:
            return RiskDecision.reject("manage action missing symbol and cannot be inferred")

        if action.reduce_pct is not None and (action.reduce_pct <= 0 or action.reduce_pct > 100):
            return RiskDecision.reject(f"invalid reduce_pct: {action.reduce_pct}")

        if action.reduce_pct is None and not action.move_sl_to_be and action.tp_price is None:
            return RiskDecision.reject("manage action has no executable fields")

        return RiskDecision(approved=True, symbol=action.symbol)

    def _pick_limit_price(self, signal: EntrySignal) -> float:
        strategy = self.config.execution.limit_price_strategy
        if signal.entry_type == EntryType.MARKET:
            return signal.entry_high

        if strategy == "LOW":
            return signal.entry_low
        if strategy == "HIGH":
            return signal.entry_high
        return (signal.entry_low + signal.entry_high) / 2
