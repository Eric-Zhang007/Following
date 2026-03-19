from __future__ import annotations

import threading
import time


class TokenBucketRateLimiter:
    def __init__(self, rate_per_sec: float = 10.0, capacity: float = 20.0) -> None:
        self.rate_per_sec = max(rate_per_sec, 0.1)
        self.capacity = max(capacity, 1.0)
        self.tokens = self.capacity
        self.updated_at = time.monotonic()
        self._lock = threading.Lock()

    def acquire(self, tokens: float = 1.0) -> None:
        need = max(tokens, 0.1)
        while True:
            with self._lock:
                now = time.monotonic()
                elapsed = now - self.updated_at
                self.updated_at = now
                self.tokens = min(self.capacity, self.tokens + elapsed * self.rate_per_sec)
                if self.tokens >= need:
                    self.tokens -= need
                    return
                missing = need - self.tokens
                wait = missing / self.rate_per_sec
            time.sleep(max(wait, 0.01))


def exponential_backoff_seconds(attempt: int, base: float = 0.25, cap: float = 8.0) -> float:
    power = max(attempt, 0)
    return min(cap, base * (2**power))
