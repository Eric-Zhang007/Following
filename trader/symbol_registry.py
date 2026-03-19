from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from trader.bitget_client import BitgetClient


@dataclass
class ContractInfo:
    symbol: str
    size_place: int
    price_place: int
    min_trade_num: float
    raw: dict[str, Any]


class SymbolRegistry:
    def __init__(
        self,
        bitget: BitgetClient,
        logger: logging.Logger,
        refresh_interval_seconds: int = 1800,
    ) -> None:
        self.bitget = bitget
        self.logger = logger
        self.refresh_interval = timedelta(seconds=refresh_interval_seconds)
        self._contracts: dict[str, ContractInfo] = {}
        self._volumes: dict[str, float] = {}
        self._last_refresh: datetime | None = None

    def refresh(self, force: bool = False) -> None:
        if not force and self._last_refresh is not None:
            if datetime.now(timezone.utc) - self._last_refresh < self.refresh_interval:
                return

        contracts = self.bitget.get_contracts()
        parsed_contracts: dict[str, ContractInfo] = {}
        for item in contracts:
            symbol = str(item.get("symbol", "")).upper()
            if not symbol:
                continue

            size_place = self._int_from(item, ["sizePlace", "volumePlace", "qtyPlace"], default=3)
            price_place = self._int_from(item, ["pricePlace"], default=6)
            min_trade_num = self._float_from(item, ["minTradeNum", "minTradeUSDT", "minTradeAmount"], default=0.0)

            parsed_contracts[symbol] = ContractInfo(
                symbol=symbol,
                size_place=size_place,
                price_place=price_place,
                min_trade_num=min_trade_num,
                raw=item,
            )

        self._contracts = parsed_contracts
        self._refresh_volumes()
        self._last_refresh = datetime.now(timezone.utc)
        self.logger.info("SymbolRegistry refreshed: contracts=%s volumes=%s", len(self._contracts), len(self._volumes))

    def _refresh_volumes(self) -> None:
        try:
            tickers = self.bitget.get_tickers()
        except Exception as exc:  # noqa: BLE001
            self.logger.warning("SymbolRegistry refresh volumes failed: %s", exc)
            self._volumes = {}
            return

        volumes: dict[str, float] = {}
        for item in tickers:
            symbol = str(item.get("symbol", "")).upper()
            if not symbol:
                continue

            value = self._float_from(
                item,
                [
                    "usdtVolume",
                    "quoteVolume",
                    "quoteVol",
                    "turnover24h",
                    "baseVolume",
                ],
                default=None,
            )
            if value is not None:
                volumes[symbol] = value
        self._volumes = volumes

    def is_tradable(self, symbol: str) -> bool:
        return symbol.upper() in self._contracts

    def get_contract(self, symbol: str) -> ContractInfo | None:
        return self._contracts.get(symbol.upper())

    def get_24h_volume(self, symbol: str) -> float | None:
        return self._volumes.get(symbol.upper())

    @staticmethod
    def _int_from(payload: dict[str, Any], keys: list[str], default: int) -> int:
        for key in keys:
            if key in payload and payload[key] not in (None, ""):
                try:
                    return int(float(payload[key]))
                except (TypeError, ValueError):
                    continue
        return default

    @staticmethod
    def _float_from(payload: dict[str, Any], keys: list[str], default: float | None) -> float | None:
        for key in keys:
            if key in payload and payload[key] not in (None, ""):
                try:
                    return float(payload[key])
                except (TypeError, ValueError):
                    continue
        return default
