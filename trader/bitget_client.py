from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time
from typing import Any
from urllib.parse import urlencode

import requests

from trader.config import BitgetConfig
from trader.rate_limiter import TokenBucketRateLimiter, exponential_backoff_seconds


class BitgetClient:
    def __init__(
        self,
        config: BitgetConfig,
        timeout: int = 10,
        rate_limiter: TokenBucketRateLimiter | None = None,
        max_retries: int = 2,
    ) -> None:
        self.config = config
        self.timeout = timeout
        self.max_retries = max_retries
        self.session = requests.Session()
        self.rate_limiter = rate_limiter or TokenBucketRateLimiter(rate_per_sec=8.0, capacity=16.0)

    def get_ticker_price(self, symbol: str) -> float:
        data = self._request(
            "GET",
            "/api/v2/mix/market/ticker",
            params={"symbol": symbol, "productType": self.config.product_type},
            auth=False,
        )

        # Bitget ticker payload may be a dict or a list with one item.
        if isinstance(data, list):
            payload = data[0] if data else {}
        else:
            payload = data or {}

        for key in ("lastPr", "last", "markPrice"):
            if key in payload:
                return float(payload[key])
        raise RuntimeError(f"Ticker response missing price: {payload}")

    def get_ticker(self, symbol: str) -> dict[str, Any]:
        data = self._request(
            "GET",
            "/api/v2/mix/market/ticker",
            params={"symbol": symbol, "productType": self.config.product_type},
            auth=False,
        )
        payload = data[0] if isinstance(data, list) and data else (data or {})
        return {
            "symbol": symbol,
            "last_price": self._float(payload, ["lastPr", "last", "price"]),
            "mark_price": self._float(payload, ["markPrice", "markPr"]),
            "bid_price": self._float(payload, ["bidPr", "bidPrice"]),
            "ask_price": self._float(payload, ["askPr", "askPrice"]),
            "raw": payload,
        }

    def get_contracts(self) -> list[dict[str, Any]]:
        data = self._request(
            "GET",
            "/api/v2/mix/market/contracts",
            params={"productType": self.config.product_type},
            auth=False,
        )
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            if isinstance(data.get("list"), list):
                return data["list"]
            return [data]
        return []

    def get_tickers(self) -> list[dict[str, Any]]:
        data = self._request(
            "GET",
            "/api/v2/mix/market/tickers",
            params={"productType": self.config.product_type},
            auth=False,
        )
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            if isinstance(data.get("list"), list):
                return data["list"]
            return [data]
        return []

    def get_account_equity(self) -> float:
        data = self._request(
            "GET",
            "/api/v2/mix/account/accounts",
            params={"productType": self.config.product_type},
            auth=True,
        )

        records = data if isinstance(data, list) else [data]
        for record in records:
            if str(record.get("marginCoin", "")).upper() == "USDT":
                for key in ("usdtEquity", "equity", "accountEquity"):
                    if key in record:
                        return float(record[key])

        for record in records:
            for key in ("usdtEquity", "equity", "accountEquity"):
                if key in record:
                    return float(record[key])
        raise RuntimeError(f"Account response missing equity: {data}")

    def get_account_snapshot(self) -> dict[str, float]:
        data = self._request(
            "GET",
            "/api/v2/mix/account/accounts",
            params={"productType": self.config.product_type},
            auth=True,
        )
        records = data if isinstance(data, list) else [data]
        target = None
        for row in records:
            if str(row.get("marginCoin", "")).upper() == "USDT":
                target = row
                break
        if target is None and records:
            target = records[0]
        if target is None:
            raise RuntimeError("account snapshot unavailable")

        equity = self._float(target, ["usdtEquity", "equity", "accountEquity"]) or 0.0
        available = self._float(target, ["available", "availableBalance", "usdtAvailable"]) or equity
        margin_used = self._float(target, ["locked", "margin", "marginUsed"]) or max(equity - available, 0.0)
        return {"equity": equity, "available": available, "margin_used": margin_used}

    def set_leverage(self, symbol: str, leverage: int, hold_side: str | None = None) -> dict[str, Any]:
        body: dict[str, Any] = {
            "symbol": symbol,
            "productType": self.config.product_type,
            "marginCoin": "USDT",
            "leverage": str(leverage),
        }
        if hold_side:
            body["holdSide"] = hold_side

        return self._request("POST", "/api/v2/mix/account/set-leverage", body=body, auth=True)

    def place_order(
        self,
        symbol: str,
        side: str,
        size: float,
        order_type: str,
        price: float | None = None,
        reduce_only: bool = False,
        trade_side: str | None = None,
        client_oid: str | None = None,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {
            "symbol": symbol,
            "productType": self.config.product_type,
            "marginCoin": "USDT",
            "marginMode": self.config.margin_mode,
            "side": side,
            "orderType": order_type.lower(),
            "size": f"{size:.6f}",
        }
        if self.config.position_mode == "one_way_mode":
            body["reduceOnly"] = "YES" if reduce_only else "NO"
        elif trade_side:
            body["tradeSide"] = trade_side

        if price is not None:
            body["price"] = f"{price:.8f}"
        if order_type.lower() == "limit":
            body["force"] = self.config.force
        if client_oid:
            body["clientOid"] = client_oid

        return self._request("POST", "/api/v2/mix/order/place-order", body=body, auth=True)

    def cancel_order(self, symbol: str, order_id: str) -> dict[str, Any]:
        body = {
            "symbol": symbol,
            "productType": self.config.product_type,
            "orderId": order_id,
        }
        return self._request("POST", "/api/v2/mix/order/cancel-order", body=body, auth=True)

    def get_position(self, symbol: str) -> dict[str, Any] | list[dict[str, Any]]:
        return self._request(
            "GET",
            "/api/v2/mix/position/single-position",
            params={"symbol": symbol, "productType": self.config.product_type, "marginCoin": "USDT"},
            auth=True,
        )

    def get_positions(self) -> list[dict[str, Any]]:
        data = self._request(
            "GET",
            "/api/v2/mix/position/all-position",
            params={"productType": self.config.product_type, "marginCoin": "USDT"},
            auth=True,
        )
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            if isinstance(data.get("list"), list):
                return data.get("list", [])
            return [data]
        return []

    def get_order(self, symbol: str, order_id: str) -> dict[str, Any]:
        return self._request(
            "GET",
            "/api/v2/mix/order/detail",
            params={
                "symbol": symbol,
                "productType": self.config.product_type,
                "orderId": order_id,
            },
            auth=True,
        )

    def get_order_state(
        self,
        symbol: str,
        order_id: str | None = None,
        client_order_id: str | None = None,
    ) -> dict[str, Any]:
        params = {
            "symbol": symbol,
            "productType": self.config.product_type,
        }
        if order_id:
            params["orderId"] = order_id
        elif client_order_id:
            params["clientOid"] = client_order_id
        else:
            raise ValueError("order_id or client_order_id required")

        return self._request("GET", "/api/v2/mix/order/detail", params=params, auth=True)

    def get_open_orders(self, symbol: str | None = None) -> list[dict[str, Any]]:
        params: dict[str, Any] = {"productType": self.config.product_type}
        if symbol:
            params["symbol"] = symbol
        data = self._request("GET", "/api/v2/mix/order/orders-pending", params=params, auth=True)
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            if isinstance(data.get("entrustedList"), list):
                return data["entrustedList"]
            if isinstance(data.get("list"), list):
                return data["list"]
            return [data]
        return []

    def get_funding_rate(self, symbol: str) -> float | None:
        data = self._request(
            "GET",
            "/api/v2/mix/market/current-fund-rate",
            params={"symbol": symbol, "productType": self.config.product_type},
            auth=False,
        )
        payload = data[0] if isinstance(data, list) and data else (data or {})
        return self._float(payload, ["fundingRate", "fundRate", "currentFundRate"])

    def protective_close_position(self, symbol: str, side: str, size: float) -> dict[str, Any]:
        close_side = "sell" if side.lower() == "long" else "buy"
        return self.place_order(
            symbol=symbol,
            side=close_side,
            trade_side="close" if self.config.position_mode == "hedge_mode" else None,
            size=size,
            order_type="market",
            reduce_only=self.config.position_mode == "one_way_mode",
        )

    def get_open_positions_count(self) -> int:
        data = self._request(
            "GET",
            "/api/v2/mix/position/all-position",
            params={"productType": self.config.product_type, "marginCoin": "USDT"},
            auth=True,
        )
        records = data if isinstance(data, list) else [data]
        count = 0
        for row in records:
            try:
                size = float(row.get("total", row.get("size", 0)) or 0)
            except Exception:  # noqa: BLE001
                size = 0.0
            if abs(size) > 0:
                count += 1
        return count

    def _request(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | None = None,
        body: dict[str, Any] | None = None,
        auth: bool = False,
    ) -> Any:
        method = method.upper()
        params = params or {}
        body = body or {}

        query_string = urlencode(params)
        url = f"{self.config.base_url}{path}"
        if query_string:
            url = f"{url}?{query_string}"

        headers = {"Content-Type": "application/json"}
        data = json.dumps(body, separators=(",", ":")) if body and method != "GET" else ""

        if auth:
            timestamp = str(int(time.time() * 1000))
            sign = self._sign(timestamp, method, path, query_string, data)
            headers.update(
                {
                    "ACCESS-KEY": self.config.api_key,
                    "ACCESS-SIGN": sign,
                    "ACCESS-TIMESTAMP": timestamp,
                    "ACCESS-PASSPHRASE": self.config.passphrase,
                }
            )

        last_error: Exception | None = None
        for attempt in range(self.max_retries + 1):
            try:
                self.rate_limiter.acquire(1.0)
                response = self.session.request(
                    method,
                    url,
                    headers=headers,
                    data=data if data else None,
                    timeout=self.timeout,
                )

                if response.status_code == 429:
                    raise RuntimeError(f"Bitget rate limited 429: {response.text}")
                if response.status_code >= 400:
                    raise RuntimeError(f"Bitget HTTP {response.status_code}: {response.text}")

                payload = response.json()
                code = str(payload.get("code", ""))
                if code not in {"00000", "0", "success", ""}:
                    raise RuntimeError(f"Bitget API error {code}: {payload.get('msg')} | payload={payload}")

                return payload.get("data")
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                if attempt >= self.max_retries:
                    break
                time.sleep(exponential_backoff_seconds(attempt))
        raise RuntimeError(f"Bitget request failed after retries: {last_error}")

    def _sign(self, timestamp: str, method: str, path: str, query_string: str, body: str) -> str:
        request_path = path if not query_string else f"{path}?{query_string}"
        prehash = f"{timestamp}{method}{request_path}{body}"
        digest = hmac.new(
            self.config.api_secret.encode("utf-8"),
            prehash.encode("utf-8"),
            hashlib.sha256,
        ).digest()
        return base64.b64encode(digest).decode("utf-8")

    @staticmethod
    def _float(payload: dict[str, Any], keys: list[str]) -> float | None:
        for key in keys:
            if key in payload and payload[key] not in {None, ""}:
                try:
                    return float(payload[key])
                except Exception:  # noqa: BLE001
                    continue
        return None
