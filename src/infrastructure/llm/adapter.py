"""LLM adapter — OpenAI-compatible API for QUE Engine (intent, rewrite, HyDE)."""
import json
from typing import Any

from loguru import logger
from openai import OpenAI

from common.config_loader import get_config
from common.exceptions import IntentRecognitionException

_client: OpenAI | None = None


def _get_cfg() -> dict[str, Any]:
    return get_config()["llm"]


def get_client() -> OpenAI:
    global _client
    if _client is not None:
        return _client
    c = _get_cfg()
    _client = OpenAI(
        api_key=c["api_key"],
        base_url=c["base_url"] or None,
        timeout=c.get("timeout", 60),
    )
    logger.info(f"LLM client initialized: base_url={c.get('base_url', 'default')}")
    return _client


@_retry(max_retries=3, base_delay=1.0)
def chat(
    messages: list[dict[str, str]],
    temperature: float = 0.2,
    max_tokens: int = 2048,
    response_format: str | None = None,
) -> str:
    c = _get_cfg()
    client = get_client()
    kwargs: dict[str, Any] = {
        "model": c.get("chat_model", "gpt-4o-mini"),
        "messages": messages,
        "temperature": temperature,
        "max_tokens": max_tokens,
    }
    if response_format == "json_object":
        kwargs["response_format"] = {"type": "json_object"}

    resp = client.chat.completions.create(**kwargs)
    return resp.choices[0].message.content or ""


@_retry(max_retries=3, base_delay=1.0)
def chat_structured(
    messages: list[dict[str, str]],
    temperature: float = 0.2,
    max_tokens: int = 2048,
) -> dict[str, Any]:
    raw = chat(messages, temperature, max_tokens, response_format="json_object")
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        start = raw.find("{")
        end = raw.rfind("}")
        if start >= 0 and end > start:
            return json.loads(raw[start:end + 1])
        raise IntentRecognitionException(f"LLM response is not valid JSON: {raw[:200]}")


def health_check() -> bool:
    try:
        get_client().models.list()
        return True
    except Exception as e:
        logger.warning(f"LLM health check failed: {e}")
        return False
