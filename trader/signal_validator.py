from __future__ import annotations

import re

from trader.models import EntrySignal, EntryType, ManageAction, ParsedMessage, Side

SYMBOL_RE = re.compile(r"^[A-Z0-9]+USDT$")


def validate_parsed_message(parsed: ParsedMessage) -> str | None:
    if isinstance(parsed, EntrySignal):
        if not SYMBOL_RE.match(parsed.symbol.upper()):
            return f"invalid symbol format: {parsed.symbol}"
        if parsed.side not in {Side.LONG, Side.SHORT}:
            return f"invalid side: {parsed.side}"
        if parsed.entry_type == EntryType.LIMIT:
            if parsed.entry_low <= 0 or parsed.entry_high <= 0:
                return "entry prices must be > 0"
            if parsed.entry_low > parsed.entry_high:
                return "entry_low must be <= entry_high"
        if parsed.stop_loss is not None:
            entry_ref = parsed.entry_high if parsed.entry_high > 0 else parsed.entry_low
            if entry_ref > 0 and parsed.side == Side.LONG and parsed.stop_loss >= entry_ref:
                return "long stop_loss must be below entry"
            if entry_ref > 0 and parsed.side == Side.SHORT and parsed.stop_loss <= entry_ref:
                return "short stop_loss must be above entry"
        return None

    if isinstance(parsed, ManageAction):
        if parsed.reduce_pct is not None and not (0 <= parsed.reduce_pct <= 100):
            return f"reduce_pct out of range: {parsed.reduce_pct}"
        if parsed.add_pct is not None and not (0 < parsed.add_pct <= 200):
            return f"add_pct out of range: {parsed.add_pct}"
        return None

    return None
