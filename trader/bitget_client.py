from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import urlencode

import requests

from trader.config import BitgetConfig
from trader.models import OrderAck
from trader.side_mapper import close_side_for_hold
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
        self._plan_capability_state: dict[str, Any] = {
            "supported": None,  # True / False / None(unknown)
            "ok": False,
            "ts": 0.0,
            "expires_at": 0.0,
            "reason": "uninitialized",
        }

    def supports_plan_orders(self) -> bool:
        state = self._plan_capability_state
        now = time.time()
        if now < float(state.get("expires_at", 0.0)):
            return state.get("supported") is True
        refreshed = self.probe_plan_orders_capability()
        return refreshed.get("supported") is True

    def probe_plan_orders_capability(self, force: bool = False) -> dict[str, Any]:
        state = self._plan_capability_state
        now = time.time()
        if not force and now < float(state.get("expires_at", 0.0)):
            return dict(state)

        errors: list[str] = []
        probe_params_candidates = [
            {"productType": self.config.product_type, "planType": "normal_plan"},
            {"productType": self.config.product_type},
        ]
        for params in probe_params_candidates:
            try:
                data = self._request(
                    "GET",
                    "/api/v2/mix/order/orders-plan-pending",
                    params=params,
                    auth=True,
                    timeout_override=self.config.plan_orders_probe_timeout_seconds,
                )
                if isinstance(data, list) or isinstance(data, dict):
                    return self._set_plan_capability(True, "ok", ttl=self.config.plan_orders_capability_ttl_seconds)
                errors.append("invalid_response_structure")
            except Exception as exc:  # noqa: BLE001
                errors.append(str(exc))

        reason, supported, ttl = self._classify_plan_probe_error(" | ".join(errors))
        return self._set_plan_capability(supported, reason, ttl=ttl)

    def get_plan_orders_capability_state(self) -> dict[str, Any]:
        return dict(self._plan_capability_state)

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

    def get_history_candles(
        self,
        *,
        symbol: str,
        start_time: datetime,
        end_time: datetime,
        granularity: str = "1m",
        limit: int = 200,
    ) -> list[dict[str, float]]:
        start_ms = int(start_time.timestamp() * 1000)
        end_ms = int(end_time.timestamp() * 1000)
        if end_ms <= start_ms:
            return []

        step_ms = self._granularity_to_ms(granularity)
        if step_ms <= 0:
            raise ValueError(f"unsupported granularity: {granularity}")
        page_span = max(step_ms, step_ms * max(1, min(limit, 1000)))

        records: dict[int, dict[str, float]] = {}
        cursor_end = end_ms
        while cursor_end > start_ms:
            cursor_start = max(start_ms, cursor_end - page_span + 1)
            payload = self._request(
                "GET",
                "/api/v2/mix/market/history-candles",
                params={
                    "symbol": symbol,
                    "productType": self.config.product_type,
                    "granularity": granularity,
                    "startTime": str(cursor_start),
                    "endTime": str(cursor_end),
                    "limit": str(limit),
                },
                auth=False,
            )
            candles = self._normalize_history_candles(payload)
            if not candles:
                break

            min_ts = None
            for item in candles:
                ts = int(item["ts"])
                if ts < start_ms or ts > end_ms:
                    continue
                records[ts] = item
                if min_ts is None or ts < min_ts:
                    min_ts = ts

            if min_ts is None:
                break
            cursor_end = min_ts - 1

        return [records[k] for k in sorted(records)]

    def was_stop_loss_touched(
        self,
        *,
        symbol: str,
        side: str,
        stop_loss: float,
        start_time: datetime,
        end_time: datetime,
        granularity: str = "1m",
    ) -> bool:
        if stop_loss <= 0:
            return False
        candles = self.get_history_candles(
            symbol=symbol,
            start_time=start_time,
            end_time=end_time,
            granularity=granularity,
        )
        if not candles:
            raise RuntimeError("history candles unavailable")

        side_upper = side.upper()
        if side_upper == "LONG":
            return any(float(item["low"]) <= stop_loss for item in candles)
        if side_upper == "SHORT":
            return any(float(item["high"]) >= stop_loss for item in candles)
        raise ValueError(f"unsupported side: {side}")

    def get_reference_price_at(
        self,
        *,
        symbol: str,
        at_time: datetime,
        granularity: str = "1m",
        lookback_minutes: int = 10,
        lookahead_minutes: int = 2,
    ) -> float | None:
        ref = at_time
        if ref.tzinfo is None:
            ref = ref.replace(tzinfo=timezone.utc)
        start = ref - timedelta(minutes=max(1, lookback_minutes))
        end = ref + timedelta(minutes=max(0, lookahead_minutes))
        candles = self.get_history_candles(
            symbol=symbol,
            start_time=start,
            end_time=end,
            granularity=granularity,
        )
        if not candles:
            return None

        target_ts = int(ref.timestamp() * 1000)
        prior = [c for c in candles if int(c.get("ts", 0)) <= target_ts]
        pick = prior[-1] if prior else min(candles, key=lambda c: abs(int(c.get("ts", 0)) - target_ts))
        close_px = float(pick.get("close") or 0.0)
        return close_px if close_px > 0 else None

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

    def place_stop_loss(
        self,
        symbol: str,
        product_type: str | None,
        margin_mode: str | None,
        position_mode: str | None,
        hold_side: str | None,
        trigger_price: float,
        order_price: float | None,
        size: float,
        side: str,
        trade_side: str | None,
        reduce_only: bool,
        client_oid: str | None,
        trigger_type: str = "mark",
    ) -> OrderAck:
        return self._place_plan_order(
            symbol=symbol,
            product_type=product_type,
            margin_mode=margin_mode,
            position_mode=position_mode,
            hold_side=hold_side,
            trigger_price=trigger_price,
            execute_price=order_price,
            size=size,
            side=side,
            trade_side=trade_side,
            reduce_only=reduce_only,
            client_oid=client_oid,
            trigger_type=trigger_type,
            plan_type="normal_plan",
        )

    def place_take_profit(
        self,
        symbol: str,
        product_type: str | None,
        margin_mode: str | None,
        position_mode: str | None,
        hold_side: str | None,
        trigger_price: float,
        order_price: float | None,
        size: float,
        side: str,
        trade_side: str | None,
        reduce_only: bool,
        client_oid: str | None,
        trigger_type: str = "mark",
    ) -> OrderAck:
        try:
            return self._place_plan_order(
                symbol=symbol,
                product_type=product_type,
                margin_mode=margin_mode,
                position_mode=position_mode,
                hold_side=hold_side,
                trigger_price=trigger_price,
                execute_price=order_price,
                size=size,
                side=side,
                trade_side=trade_side,
                reduce_only=reduce_only,
                client_oid=client_oid,
                trigger_type=trigger_type,
                plan_type="profit_plan",
            )
        except Exception as exc:  # noqa: BLE001
            message = str(exc).lower()
            # Some accounts reject profit_plan and only accept normal_plan for TP.
            if "400172" not in message and "planType" not in str(exc):
                raise
            return self._place_plan_order(
                symbol=symbol,
                product_type=product_type,
                margin_mode=margin_mode,
                position_mode=position_mode,
                hold_side=hold_side,
                trigger_price=trigger_price,
                execute_price=order_price,
                size=size,
                side=side,
                trade_side=trade_side,
                reduce_only=reduce_only,
                client_oid=client_oid,
                trigger_type=trigger_type,
                plan_type="normal_plan",
            )

    def cancel_plan_order(
        self,
        *,
        symbol: str,
        order_id: str | None = None,
        client_oid: str | None = None,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {
            "symbol": symbol,
            "productType": self.config.product_type,
            "marginCoin": "USDT",
        }
        if order_id:
            body["orderId"] = order_id
        if client_oid:
            body["clientOid"] = client_oid
        if not order_id and not client_oid:
            raise ValueError("order_id or client_oid is required for cancel_plan_order")
        return self._request("POST", "/api/v2/mix/order/cancel-plan-order", body=body, auth=True)

    def list_plan_orders(self, symbol: str | None = None) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        seen: set[tuple[str, str, str]] = set()
        for plan_type in ("normal_plan", "profit_plan", "track_plan"):
            for item in self._list_plan_orders_by_type(symbol=symbol, plan_type=plan_type):
                oid = str(item.get("orderId") or "")
                coid = str(item.get("clientOid") or "")
                sym = str(item.get("symbol") or "")
                key = (oid, coid, sym)
                if key in seen:
                    continue
                seen.add(key)
                rows.append(item)
        return rows

    def _list_plan_orders_by_type(
        self,
        *,
        symbol: str | None,
        plan_type: str,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {
            "productType": self.config.product_type,
            "planType": plan_type,
        }
        if symbol:
            params["symbol"] = symbol
        try:
            data = self._request("GET", "/api/v2/mix/order/orders-plan-pending", params=params, auth=True)
        except Exception as exc:  # noqa: BLE001
            message = str(exc).lower()
            # Some accounts only support specific planType values for query APIs.
            if "40812" in message or "condition plantype is not met" in message:
                return []
            # Backward compatibility: some environments accept this endpoint without planType.
            if "400172" in message and "parameter verification failed" in message:
                fallback_params = {"productType": self.config.product_type}
                if symbol:
                    fallback_params["symbol"] = symbol
                data = self._request("GET", "/api/v2/mix/order/orders-plan-pending", params=fallback_params, auth=True)
            else:
                raise
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            if isinstance(data.get("entrustedList"), list):
                return data["entrustedList"]
            if isinstance(data.get("list"), list):
                return data["list"]
            # Bitget may return metadata-only payloads like
            # {"entrustedList": null, "endId": null} when no rows exist.
            if any(
                data.get(key) not in (None, "")
                for key in ("orderId", "clientOid", "symbol", "planType", "triggerPrice")
            ):
                return [data]
            return []
        return []

    def get_order_detail(self, order_id: str) -> dict[str, Any]:
        data = self._request(
            "GET",
            "/api/v2/mix/order/detail",
            params={"productType": self.config.product_type, "orderId": order_id},
            auth=True,
        )
        if isinstance(data, dict):
            return data
        if isinstance(data, list) and data:
            return data[0]
        return {}

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

    def get_history_positions(
        self,
        *,
        symbol: str | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {
            "productType": self.config.product_type,
            "marginCoin": "USDT",
            "limit": str(max(1, min(int(limit), 100))),
        }
        if symbol:
            params["symbol"] = symbol
        if start_time is not None:
            params["startTime"] = str(int(start_time.timestamp() * 1000))
        if end_time is not None:
            params["endTime"] = str(int(end_time.timestamp() * 1000))

        data = self._request("GET", "/api/v2/mix/position/history-position", params=params, auth=True)
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
        is_plan_order: bool = False,
    ) -> dict[str, Any]:
        if is_plan_order:
            return self.get_plan_order_state(symbol=symbol, order_id=order_id, client_order_id=client_order_id)
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

    def get_plan_order_state(
        self,
        *,
        symbol: str,
        order_id: str | None = None,
        client_order_id: str | None = None,
    ) -> dict[str, Any]:
        orders = self.list_plan_orders(symbol=symbol)
        for item in orders:
            if order_id and str(item.get("orderId") or "") == str(order_id):
                return item
            if client_order_id and str(item.get("clientOid") or "") == str(client_order_id):
                return item
        if order_id:
            return {"orderId": order_id, "state": "FILLED_OR_CLOSED"}
        if client_order_id:
            return {"clientOid": client_order_id, "state": "FILLED_OR_CLOSED"}
        raise ValueError("order_id or client_order_id required")

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
        close_side = close_side_for_hold(side, self.config.position_mode)
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
        timeout_override: int | None = None,
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
                    timeout=timeout_override if timeout_override is not None else self.timeout,
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

    @staticmethod
    def _normalize_history_candles(data: Any) -> list[dict[str, float]]:
        rows: list[Any]
        if isinstance(data, dict):
            if isinstance(data.get("list"), list):
                rows = data.get("list", [])
            else:
                rows = [data]
        elif isinstance(data, list):
            rows = data
        else:
            return []

        out: list[dict[str, float]] = []
        for row in rows:
            if isinstance(row, list) and len(row) >= 5:
                try:
                    out.append(
                        {
                            "ts": float(row[0]),
                            "open": float(row[1]),
                            "high": float(row[2]),
                            "low": float(row[3]),
                            "close": float(row[4]),
                        }
                    )
                except Exception:  # noqa: BLE001
                    continue
                continue

            if isinstance(row, dict):
                ts = row.get("ts") or row.get("time") or row.get("cTime")
                open_p = row.get("open") or row.get("o")
                high_p = row.get("high") or row.get("h")
                low_p = row.get("low") or row.get("l")
                close_p = row.get("close") or row.get("c")
                if None in {ts, open_p, high_p, low_p, close_p}:
                    continue
                try:
                    out.append(
                        {
                            "ts": float(ts),
                            "open": float(open_p),
                            "high": float(high_p),
                            "low": float(low_p),
                            "close": float(close_p),
                        }
                    )
                except Exception:  # noqa: BLE001
                    continue
        return out

    @staticmethod
    def _granularity_to_ms(granularity: str) -> int:
        text = str(granularity or "").strip().lower()
        mapping = {
            "1m": 60_000,
            "3m": 3 * 60_000,
            "5m": 5 * 60_000,
            "15m": 15 * 60_000,
            "30m": 30 * 60_000,
            "1h": 60 * 60_000,
            "4h": 4 * 60 * 60_000,
            "6h": 6 * 60 * 60_000,
            "12h": 12 * 60 * 60_000,
            "1d": 24 * 60 * 60_000,
        }
        if text in mapping:
            return mapping[text]

        if text.endswith("m") and text[:-1].isdigit():
            return int(text[:-1]) * 60_000
        if text.endswith("h") and text[:-1].isdigit():
            return int(text[:-1]) * 60 * 60_000
        if text.endswith("d") and text[:-1].isdigit():
            return int(text[:-1]) * 24 * 60 * 60_000
        if text.isdigit():
            # Bitget also accepts pure minute values (e.g. "1", "5", "15").
            return int(text) * 60_000
        return 0

    def _set_plan_capability(self, supported: bool | None, reason: str, ttl: int) -> dict[str, Any]:
        now = time.time()
        state = {
            "supported": supported,
            "ok": supported is True,
            "ts": now,
            "expires_at": now + max(ttl, 1),
            "reason": reason,
        }
        self._plan_capability_state = state
        return dict(state)

    def _classify_plan_probe_error(self, message: str) -> tuple[str, bool | None, int]:
        msg = message.lower()
        normal_ttl = self.config.plan_orders_capability_ttl_seconds
        short_ttl = max(5, min(30, normal_ttl // 10 if normal_ttl > 10 else 5))

        if "http 404" in msg or "not found" in msg or "api does not exist" in msg:
            return "endpoint_not_found", False, normal_ttl
        if "http 401" in msg or "http 403" in msg or "permission" in msg or "forbidden" in msg:
            return "permission_denied", False, normal_ttl
        if "400172" in msg or "parameter verification failed" in msg or "condition plantype is not met" in msg:
            return "parameter_mismatch", None, short_ttl

        network_tokens = [
            "timeout",
            "timed out",
            "connection",
            "temporarily unavailable",
            "name or service",
            "network",
        ]
        if any(token in msg for token in network_tokens):
            return "network_error", None, short_ttl

        return "probe_failed", False, normal_ttl

    def _sign(self, timestamp: str, method: str, path: str, query_string: str, body: str) -> str:
        request_path = path if not query_string else f"{path}?{query_string}"
        prehash = f"{timestamp}{method}{request_path}{body}"
        digest = hmac.new(
            self.config.api_secret.encode("utf-8"),
            prehash.encode("utf-8"),
            hashlib.sha256,
        ).digest()
        return base64.b64encode(digest).decode("utf-8")

    def _place_plan_order(
        self,
        *,
        symbol: str,
        product_type: str | None,
        margin_mode: str | None,
        position_mode: str | None,
        hold_side: str | None,
        trigger_price: float,
        execute_price: float | None,
        size: float,
        side: str,
        trade_side: str | None,
        reduce_only: bool,
        client_oid: str | None,
        trigger_type: str,
        plan_type: str,
    ) -> OrderAck:
        ptype = product_type or self.config.product_type
        pmode = position_mode or self.config.position_mode
        mmode = margin_mode or self.config.margin_mode
        body: dict[str, Any] = {
            "symbol": symbol,
            "productType": ptype,
            "marginCoin": "USDT",
            "marginMode": mmode,
            "size": f"{size:.6f}",
            "planType": plan_type,
            "triggerPrice": f"{trigger_price:.8f}",
            "triggerType": "mark_price" if trigger_type.lower() == "mark" else "fill_price",
            "side": side,
            "orderType": "limit" if execute_price is not None else "market",
        }
        if execute_price is not None:
            body["executePrice"] = f"{execute_price:.8f}"
        else:
            body["executePrice"] = "0"
        if client_oid:
            body["clientOid"] = client_oid

        if pmode == "one_way_mode":
            body["reduceOnly"] = "YES" if reduce_only else "NO"
        else:
            body["tradeSide"] = trade_side or "close"
            if hold_side:
                body["holdSide"] = hold_side

        raw = self._request("POST", "/api/v2/mix/order/place-plan-order", body=body, auth=True)
        return self._to_ack(raw)

    @staticmethod
    def _to_ack(payload: dict[str, Any] | list[dict[str, Any]] | None) -> OrderAck:
        raw: dict[str, Any]
        if isinstance(payload, list):
            raw = payload[0] if payload else {}
        else:
            raw = payload or {}
        order_id = str(raw.get("orderId") or "") or None
        client_oid = str(raw.get("clientOid") or "") or None
        status = str(raw.get("state") or raw.get("status") or "ACKED")
        return OrderAck(order_id=order_id, client_oid=client_oid, status=status, raw=raw)

    @staticmethod
    def _float(payload: dict[str, Any], keys: list[str]) -> float | None:
        for key in keys:
            if key in payload and payload[key] not in {None, ""}:
                try:
                    return float(payload[key])
                except Exception:  # noqa: BLE001
                    continue
        return None
