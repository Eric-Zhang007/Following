import time

from trader.rate_limiter import TokenBucketRateLimiter, exponential_backoff_seconds


def test_token_bucket_waits_when_tokens_exhausted() -> None:
    limiter = TokenBucketRateLimiter(rate_per_sec=2.0, capacity=2.0)

    t0 = time.monotonic()
    limiter.acquire(1.0)
    limiter.acquire(1.0)
    limiter.acquire(1.0)
    elapsed = time.monotonic() - t0

    # 3rd token should wait around 0.5s at 2 tokens/sec.
    assert elapsed >= 0.45


def test_backoff_grows_exponentially_and_caps() -> None:
    assert exponential_backoff_seconds(0, base=0.25, cap=2.0) == 0.25
    assert exponential_backoff_seconds(1, base=0.25, cap=2.0) == 0.5
    assert exponential_backoff_seconds(2, base=0.25, cap=2.0) == 1.0
    assert exponential_backoff_seconds(10, base=0.25, cap=2.0) == 2.0
