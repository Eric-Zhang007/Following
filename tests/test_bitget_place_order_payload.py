from trader.bitget_client import BitgetClient
from trader.config import BitgetConfig
from trader.models import OrderAck


def test_place_order_payload_includes_margin_mode_and_force_for_limit() -> None:
    config = BitgetConfig(
        base_url="https://api.bitget.com",
        api_key="k",
        api_secret="s",
        passphrase="p",
        product_type="USDT-FUTURES",
        margin_mode="isolated",
        position_mode="one_way_mode",
        force="gtc",
    )
    client = BitgetClient(config)
    captured = {}

    def fake_request(method, path, params=None, body=None, auth=False):
        captured["method"] = method
        captured["path"] = path
        captured["body"] = body
        captured["auth"] = auth
        return {"ok": True}

    client._request = fake_request  # type: ignore[method-assign]
    client.place_order(
        symbol="BTCUSDT",
        side="buy",
        size=0.123456,
        order_type="limit",
        price=100.25,
        reduce_only=True,
    )

    body = captured["body"]
    assert captured["path"] == "/api/v2/mix/order/place-order"
    assert body["marginMode"] == "isolated"
    assert body["force"] == "gtc"
    assert body["reduceOnly"] == "YES"
    assert "tradeSide" not in body


def test_place_order_payload_hedge_mode_uses_trade_side() -> None:
    config = BitgetConfig(
        base_url="https://api.bitget.com",
        api_key="k",
        api_secret="s",
        passphrase="p",
        product_type="USDT-FUTURES",
        margin_mode="crossed",
        position_mode="hedge_mode",
        force="ioc",
    )
    client = BitgetClient(config)
    captured = {}

    def fake_request(method, path, params=None, body=None, auth=False):
        captured["body"] = body
        return {"ok": True}

    client._request = fake_request  # type: ignore[method-assign]
    client.place_order(
        symbol="ETHUSDT",
        side="sell",
        trade_side="open",
        size=1.5,
        order_type="market",
        reduce_only=True,
    )

    body = captured["body"]
    assert body["marginMode"] == "crossed"
    assert body["tradeSide"] == "open"
    assert "reduceOnly" not in body


def test_place_stop_loss_payload_uses_normal_plan_and_order_type() -> None:
    config = BitgetConfig(
        base_url="https://api.bitget.com",
        api_key="k",
        api_secret="s",
        passphrase="p",
        product_type="USDT-FUTURES",
        margin_mode="isolated",
        position_mode="one_way_mode",
    )
    client = BitgetClient(config)
    captured = {}

    def fake_request(method, path, params=None, body=None, auth=False):
        captured["path"] = path
        captured["body"] = body
        return {"orderId": "1", "clientOid": body.get("clientOid"), "state": "new"}

    client._request = fake_request  # type: ignore[method-assign]
    client.place_stop_loss(
        symbol="BTCUSDT",
        product_type="USDT-FUTURES",
        margin_mode="isolated",
        position_mode="one_way_mode",
        hold_side="long",
        trigger_price=100.0,
        order_price=None,
        size=0.1,
        side="sell",
        trade_side=None,
        reduce_only=True,
        client_oid="sl-test-1",
        trigger_type="mark",
    )

    body = captured["body"]
    assert captured["path"] == "/api/v2/mix/order/place-plan-order"
    assert body["planType"] == "normal_plan"
    assert body["orderType"] == "market"


def test_list_plan_orders_ignores_metadata_only_payload() -> None:
    config = BitgetConfig(
        base_url="https://api.bitget.com",
        api_key="k",
        api_secret="s",
        passphrase="p",
        product_type="USDT-FUTURES",
        margin_mode="isolated",
        position_mode="one_way_mode",
    )
    client = BitgetClient(config)

    def fake_request(method, path, params=None, body=None, auth=False):
        if path == "/api/v2/mix/order/orders-plan-pending":
            return {"entrustedList": None, "endId": None}
        return {"ok": True}

    client._request = fake_request  # type: ignore[method-assign]

    assert client.list_plan_orders() == []


def test_list_plan_orders_keeps_single_row_dict_payload() -> None:
    config = BitgetConfig(
        base_url="https://api.bitget.com",
        api_key="k",
        api_secret="s",
        passphrase="p",
        product_type="USDT-FUTURES",
        margin_mode="isolated",
        position_mode="one_way_mode",
    )
    client = BitgetClient(config)

    def fake_request(method, path, params=None, body=None, auth=False):
        if path == "/api/v2/mix/order/orders-plan-pending":
            return {
                "orderId": "12345",
                "clientOid": "test-oid",
                "symbol": "BTCUSDT",
                "planType": "normal_plan",
            }
        return {"ok": True}

    client._request = fake_request  # type: ignore[method-assign]

    rows = client.list_plan_orders()
    assert len(rows) == 1
    assert rows[0]["orderId"] == "12345"


def test_get_history_positions_parses_nested_list() -> None:
    config = BitgetConfig(
        base_url="https://api.bitget.com",
        api_key="k",
        api_secret="s",
        passphrase="p",
        product_type="USDT-FUTURES",
    )
    client = BitgetClient(config)
    captured = {}

    def fake_request(method, path, params=None, body=None, auth=False):
        captured["method"] = method
        captured["path"] = path
        captured["params"] = params
        captured["auth"] = auth
        return {"list": [{"symbol": "BTCUSDT", "netProfit": "12.3"}]}

    client._request = fake_request  # type: ignore[method-assign]
    rows = client.get_history_positions(symbol="BTCUSDT", limit=5)

    assert captured["method"] == "GET"
    assert captured["path"] == "/api/v2/mix/position/history-position"
    assert captured["params"]["symbol"] == "BTCUSDT"
    assert captured["params"]["limit"] == "5"
    assert captured["auth"] is True
    assert rows == [{"symbol": "BTCUSDT", "netProfit": "12.3"}]


def test_place_take_profit_fallbacks_to_normal_plan_on_400172() -> None:
    config = BitgetConfig(
        base_url="https://api.bitget.com",
        api_key="k",
        api_secret="s",
        passphrase="p",
        product_type="USDT-FUTURES",
    )
    client = BitgetClient(config)
    attempts: list[str] = []

    def fake_place_plan_order(**kwargs):  # noqa: ANN003
        attempts.append(str(kwargs.get("plan_type")))
        if kwargs.get("plan_type") == "profit_plan":
            raise RuntimeError('Bitget HTTP 400: {"code":"400172","msg":"planType Illegal type"}')
        return OrderAck(order_id="tp-1", client_oid=kwargs.get("client_oid"), status="ACKED", raw={})

    client._place_plan_order = fake_place_plan_order  # type: ignore[method-assign]
    ack = client.place_take_profit(
        symbol="INXUSDT",
        product_type="USDT-FUTURES",
        margin_mode="isolated",
        position_mode="hedge_mode",
        hold_side="long",
        trigger_price=0.012,
        order_price=None,
        size=100.0,
        side="sell",
        trade_side="close",
        reduce_only=False,
        client_oid="tp-test",
        trigger_type="mark",
    )

    assert attempts == ["profit_plan", "normal_plan"]
    assert ack.order_id == "tp-1"
