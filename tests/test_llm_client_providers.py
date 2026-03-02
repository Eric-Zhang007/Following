import sys
from types import ModuleType

from trader.config import LLMConfig
from trader.llm_client import OpenAIResponsesClient


class _FakeResponse:
    def __init__(self, payload: dict, output_text: str | None = None) -> None:
        self._payload = payload
        self.output_text = output_text

    def model_dump(self) -> dict:
        return self._payload


class _FakeResponsesAPI:
    def __init__(self, store: dict) -> None:
        self.store = store

    def create(self, **kwargs):  # noqa: ANN003
        self.store["responses_kwargs"] = kwargs
        return _FakeResponse(payload={}, output_text='{"provider":"openai"}')


class _FakeChatCompletionsAPI:
    def __init__(self, store: dict, content: str | list[dict]) -> None:
        self.store = store
        self.content = content

    def create(self, **kwargs):  # noqa: ANN003
        self.store["chat_kwargs"] = kwargs
        return _FakeResponse(payload={"choices": [{"message": {"content": self.content}}]})


class _FakeOpenAIClient:
    def __init__(self, store: dict, content: str | list[dict]) -> None:
        self.responses = _FakeResponsesAPI(store)
        self.chat = type("Chat", (), {"completions": _FakeChatCompletionsAPI(store, content)})()


def _install_fake_openai(monkeypatch, store: dict, content: str | list[dict]) -> None:  # noqa: ANN001
    module = ModuleType("openai")

    class OpenAI:  # noqa: D401
        def __init__(self, api_key: str, base_url: str | None, timeout: int) -> None:
            store["api_key"] = api_key
            store["base_url"] = base_url
            store["timeout"] = timeout
            self._client = _FakeOpenAIClient(store, content)
            self.responses = self._client.responses
            self.chat = self._client.chat

    module.OpenAI = OpenAI  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "openai", module)


def test_openai_provider_uses_responses_api(monkeypatch) -> None:
    store: dict = {}
    _install_fake_openai(monkeypatch, store, content='{"provider":"unused"}')
    monkeypatch.setenv("TEST_LLM_KEY", "k-openai")

    config = LLMConfig(
        provider="openai",
        model="gpt-4.1-mini",
        api_key_env="TEST_LLM_KEY",
        max_retries=0,
    )
    client = OpenAIResponsesClient(config)
    parsed = client.parse_signal("hello")

    assert parsed["provider"] == "openai"
    assert "responses_kwargs" in store
    assert "chat_kwargs" not in store
    assert store["base_url"] is None


def test_deepseek_provider_uses_chat_completions(monkeypatch) -> None:
    store: dict = {}
    _install_fake_openai(monkeypatch, store, content='{"provider":"deepseek"}')
    monkeypatch.setenv("TEST_LLM_KEY", "k-deepseek")

    config = LLMConfig(
        provider="deepseek",
        model="deepseek-chat",
        api_key_env="TEST_LLM_KEY",
        max_retries=0,
    )
    client = OpenAIResponsesClient(config)
    parsed = client.parse_signal("signal")

    assert parsed["provider"] == "deepseek"
    assert "chat_kwargs" in store
    assert "responses_kwargs" not in store
    assert store["base_url"] == "https://api.deepseek.com"
    assert store["chat_kwargs"]["response_format"] == {"type": "json_object"}


def test_qwen_provider_parses_list_content(monkeypatch) -> None:
    store: dict = {}
    _install_fake_openai(
        monkeypatch,
        store,
        content=[{"type": "text", "text": '{"provider":"qwen","model":"qwen3.5-plus"}'}],
    )
    monkeypatch.setenv("TEST_LLM_KEY", "k-qwen")

    config = LLMConfig(
        provider="qwen",
        model="qwen3.5-plus",
        api_key_env="TEST_LLM_KEY",
        max_retries=0,
    )
    client = OpenAIResponsesClient(config)
    parsed = client.parse_signal("signal")

    assert parsed["provider"] == "qwen"
    assert parsed["model"] == "qwen3.5-plus"
    assert store["base_url"] == "https://dashscope-us.aliyuncs.com/compatible-mode/v1"
