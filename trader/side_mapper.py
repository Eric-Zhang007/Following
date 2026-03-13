from __future__ import annotations


def normalize_hold_side(side: str | None) -> str:
    value = str(side or "").strip().lower()
    if value in {"long", "buy"}:
        return "long"
    if value in {"short", "sell"}:
        return "short"
    return "long"


def open_side_for_hold(hold_side: str | None) -> str:
    return "buy" if normalize_hold_side(hold_side) == "long" else "sell"


def close_side_for_hold(hold_side: str | None, position_mode: str | None) -> str:
    normalized = normalize_hold_side(hold_side)
    if str(position_mode or "").lower() == "hedge_mode":
        # In hedge mode, side encodes position direction and tradeSide encodes open/close.
        return open_side_for_hold(normalized)
    # In one-way mode, side encodes order direction.
    return "sell" if normalized == "long" else "buy"

