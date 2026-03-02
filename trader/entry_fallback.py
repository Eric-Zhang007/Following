from __future__ import annotations

from dataclasses import replace

from trader.models import EntrySignal, EntryType


def is_market_slippage_reject(reason: str | None) -> bool:
    if not reason:
        return False
    lowered = reason.lower()
    return "market anchor deviation" in lowered and "max_entry_slippage_pct" in lowered


def convert_market_to_limit_signal(signal: EntrySignal) -> EntrySignal | None:
    if signal.entry_type != EntryType.MARKET:
        return None

    candidates: list[float] = [float(p) for p in (signal.entry_points or []) if float(p) > 0]
    if not candidates:
        if signal.entry_high > 0:
            candidates.append(float(signal.entry_high))
        if signal.entry_low > 0:
            candidates.append(float(signal.entry_low))

    if not candidates:
        return None

    points: list[float] = []
    seen: set[float] = set()
    for raw in candidates:
        key = round(float(raw), 12)
        if key in seen:
            continue
        seen.add(key)
        points.append(float(raw))

    if not points:
        return None

    return replace(
        signal,
        entry_type=EntryType.LIMIT,
        entry_low=min(points),
        entry_high=max(points),
        entry_points=points,
    )
