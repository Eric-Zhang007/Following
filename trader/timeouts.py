from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Any, Callable, TypeVar

from trader.rate_limiter import exponential_backoff_seconds

T = TypeVar("T")


@dataclass
class RetryPolicy:
    timeout_seconds: float = 10.0
    max_retries: int = 2
    backoff_base_seconds: float = 0.25
    backoff_cap_seconds: float = 8.0


def run_with_retries(func: Callable[[], T], policy: RetryPolicy) -> T:
    last_error: Exception | None = None
    for attempt in range(policy.max_retries + 1):
        start = time.monotonic()
        try:
            result = func()
            elapsed = time.monotonic() - start
            if elapsed > policy.timeout_seconds:
                raise TimeoutError(f"operation exceeded timeout: {elapsed:.3f}s > {policy.timeout_seconds}s")
            return result
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            if attempt >= policy.max_retries:
                break
            time.sleep(exponential_backoff_seconds(attempt, policy.backoff_base_seconds, policy.backoff_cap_seconds))
    raise RuntimeError(f"operation failed after retries: {last_error}")


async def run_async_with_retries(func: Callable[[], Any], policy: RetryPolicy) -> Any:
    last_error: Exception | None = None
    for attempt in range(policy.max_retries + 1):
        try:
            return await asyncio.wait_for(func(), timeout=policy.timeout_seconds)
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            if attempt >= policy.max_retries:
                break
            await asyncio.sleep(
                exponential_backoff_seconds(attempt, policy.backoff_base_seconds, policy.backoff_cap_seconds)
            )
    raise RuntimeError(f"async operation failed after retries: {last_error}")
