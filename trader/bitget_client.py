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


class BitgetClient:
    def __init__(self, config: BitgetConfig, timeout: int = 10) -> None:
        self.config = config
        self.timeout = timeout
        self.session = requests.Session()

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

        response = self.session.request(
            method,
            url,
            headers=headers,
            data=data if data else None,
            timeout=self.timeout,
        )

        if response.status_code >= 400:
            raise RuntimeError(f"Bitget HTTP {response.status_code}: {response.text}")

        payload = response.json()
        code = str(payload.get("code", ""))
        if code not in {"00000", "0", "success", ""}:
            raise RuntimeError(f"Bitget API error {code}: {payload.get('msg')} | payload={payload}")

        return payload.get("data")

    def _sign(self, timestamp: str, method: str, path: str, query_string: str, body: str) -> str:
        request_path = path if not query_string else f"{path}?{query_string}"
        prehash = f"{timestamp}{method}{request_path}{body}"
        digest = hmac.new(
            self.config.api_secret.encode("utf-8"),
            prehash.encode("utf-8"),
            hashlib.sha256,
        ).digest()
        return base64.b64encode(digest).decode("utf-8")
