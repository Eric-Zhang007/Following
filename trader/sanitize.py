from __future__ import annotations

import re


def sanitize_text(text: str, redact_patterns: list[str], max_length: int = 4000) -> str:
    sanitized = text or ""
    for pattern in redact_patterns:
        sanitized = re.sub(pattern, "[REDACTED]", sanitized)

    # Bound size to keep token usage predictable.
    if len(sanitized) > max_length:
        sanitized = sanitized[: max_length - 3] + "..."
    return sanitized
