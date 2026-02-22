from trader.bitget_client import BitgetClient
from trader.config import BitgetConfig


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
