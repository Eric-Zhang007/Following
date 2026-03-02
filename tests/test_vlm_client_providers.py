import json

from trader.config import VLMConfig
from trader.vlm_client import VLMClient


class _FakeHTTPResponse:
    def __init__(self, payload: dict) -> None:
        self._payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict:
        return self._payload


class _FakeSession:
    def __init__(self, payload: dict) -> None:
        self.payload = payload
        self.headers: dict[str, str] = {}
        self.calls: list[dict] = []

    def post(self, url: str, data: str, timeout: int) -> _FakeHTTPResponse:
        self.calls.append({"url": url, "data": data, "timeout": timeout})
        return _FakeHTTPResponse(self.payload)


def _valid_vlm_output() -> dict:
    return {
        "kind": "ENTRY_SIGNAL",
        "symbol": "BTCUSDT",
        "side": "LONG",
        "leverage": 5,
        "entry": {"type": "LIMIT", "low": 100.0, "high": 101.0, "stop_loss": None, "tp": []},
        "manage": {"reduce_pct": None, "move_sl_to_be": False, "tp": []},
        "evidence": {
            "field_evidence": {
                "symbol": ["BTCUSDT"],
                "side": ["LONG"],
                "entry.low": ["100.0"],
                "entry.high": ["101.0"],
            },
            "source": {
                "symbol": "text",
                "side": "text",
                "entry.low": "text",
                "entry.high": "text",
            },
        },
        "uncertain_fields": [],
        "extraction_warnings": [],
        "safety": {"should_trade": "NO_DECISION"},
        "confidence": 0.9,
        "notes": "ok",
    }


def test_vlm_qwen_provider_uses_dashscope_compatible_endpoint(monkeypatch) -> None:
    response_payload = {"choices": [{"message": {"content": json.dumps(_valid_vlm_output())}}]}
    fake_session = _FakeSession(response_payload)
    monkeypatch.setattr("trader.vlm_client.requests.Session", lambda: fake_session)
    monkeypatch.setenv("TEST_VLM_KEY", "k-qwen-vlm")

    config = VLMConfig(
        enabled=True,
        provider="qwen",
        model="qwen-vl-max-latest",
        api_key_env="TEST_VLM_KEY",
        base_url=None,
        max_retries=0,
    )
    client = VLMClient(config)
    parsed = client.extract(image_bytes=b"img", text_context="signal text")

    assert client.base_url == "https://dashscope-us.aliyuncs.com/compatible-mode/v1"
    assert str(parsed.symbol) == "BTCUSDT"
    assert fake_session.calls
    assert fake_session.calls[0]["url"].endswith("/chat/completions")


def test_vlm_extract_handles_fenced_json_response(monkeypatch) -> None:
    fenced = "```json\n" + json.dumps(_valid_vlm_output()) + "\n```"
    response_payload = {"choices": [{"message": {"content": fenced}}]}
    fake_session = _FakeSession(response_payload)
    monkeypatch.setattr("trader.vlm_client.requests.Session", lambda: fake_session)
    monkeypatch.setenv("TEST_VLM_KEY", "k-kimi-vlm")

    config = VLMConfig(
        enabled=True,
        provider="kimi",
        model="moonshot-v1-vision",
        api_key_env="TEST_VLM_KEY",
        base_url=None,
        max_retries=0,
    )
    client = VLMClient(config)
    parsed = client.extract(image_bytes=None, text_context="signal text")

    assert str(parsed.symbol) == "BTCUSDT"
    assert parsed.side.value == "LONG"
