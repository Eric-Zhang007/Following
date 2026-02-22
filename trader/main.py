from __future__ import annotations

import asyncio
import contextlib
import logging
from dataclasses import asdict, is_dataclass
from datetime import timezone
from pathlib import Path
from typing import Any

import typer

from trader.bitget_client import BitgetClient
from trader.config import AppConfig, load_config
from trader.executor import TradeExecutor
from trader.llm_parser import HybridSignalParser, ParseOutcome
from trader.models import EntrySignal, ManageAction, NonSignal, ParsedMessage, utc_now
from trader.notifier import Notifier
from trader.risk import RiskManager
from trader.store import SQLiteStore
from trader.symbol_registry import SymbolRegistry
from trader.telegram_listener import TelegramListener

app = typer.Typer(add_completion=False, help="Telegram signal -> Bitget executor")


def _setup_logging(config: AppConfig) -> logging.Logger:
    level = getattr(logging, config.logging.level.upper(), logging.INFO)
    handlers: list[logging.Handler] = [
        logging.FileHandler(config.logging.file, encoding="utf-8"),
        logging.StreamHandler(),
    ]

    if config.logging.rich:
        try:
            from rich.logging import RichHandler

            handlers[1] = RichHandler(rich_tracebacks=True)
        except Exception:  # noqa: BLE001
            pass

    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        handlers=handlers,
    )
    return logging.getLogger("trader")


@app.command()
def run(config: Path = typer.Option(Path("config.yaml"), exists=True, help="Path to YAML config")) -> None:
    """Run Telegram listener and execution loop."""
    asyncio.run(_run_async(config))


async def _run_async(config_path: Path) -> None:
    config = load_config(config_path)
    logger = _setup_logging(config)
    notifier = Notifier(logger)

    store = SQLiteStore(config.storage.db_path)
    parser_engine = HybridSignalParser(config, store, logger)
    bitget = BitgetClient(config.bitget)
    symbol_registry = SymbolRegistry(bitget, logger)
    try:
        symbol_registry.refresh(force=True)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Initial SymbolRegistry refresh failed: %s", exc)

    risk_manager = RiskManager(config, symbol_registry=symbol_registry)
    executor = TradeExecutor(config, bitget, store, notifier, logger, symbol_registry=symbol_registry)

    refresh_task = asyncio.create_task(_symbol_registry_refresh_loop(symbol_registry, logger))

    logger.info(
        "Starting trader. dry_run=%s db=%s llm_mode=%s llm_enabled=%s",
        config.dry_run,
        config.storage.db_path,
        config.llm.mode,
        config.llm.enabled,
    )

    async def on_telegram_event(event) -> None:
        try:
            message_state = store.record_message(
                chat_id=event.chat_id,
                message_id=event.message_id,
                text=event.text,
                is_edit=event.is_edit,
                event_time=event.date,
            )

            if message_state.duplicate and not event.is_edit:
                logger.info("Duplicate message ignored: chat=%s message=%s", event.chat_id, event.message_id)
                return

            fallback_symbol = store.get_last_entry_symbol(event.chat_id)
            parse_outcome = parser_engine.parse(
                chat_id=event.chat_id,
                message_id=event.message_id,
                version=message_state.version,
                text_hash=message_state.text_hash,
                text=event.text,
                source_key=str(event.chat_id),
                fallback_symbol=fallback_symbol,
                timestamp=event.date,
            )
            parsed = parse_outcome.parsed

            store.record_parsed_signal(
                event.chat_id,
                event.message_id,
                message_state.version,
                parsed,
                parse_source=parse_outcome.parse_source,
                confidence=parse_outcome.confidence,
            )

            if parse_outcome.llm_error:
                logger.warning("LLM parse error: %s", parse_outcome.llm_error)

            if isinstance(parsed, NonSignal):
                if parsed.note.startswith("incomplete_"):
                    store.record_execution(
                        chat_id=event.chat_id,
                        message_id=event.message_id,
                        version=message_state.version,
                        action_type="PARSE",
                        symbol=None,
                        side=None,
                        status="REJECTED",
                        reason=parsed.note,
                        intent={
                            "parse_source": parse_outcome.parse_source,
                            "confidence": parse_outcome.confidence,
                            "notes": parse_outcome.notes,
                        },
                    )
                    notifier.warning(f"Signal rejected due to uncertain fields: {parsed.note}")
                else:
                    logger.debug("Non-signal message ignored: message_id=%s", event.message_id)
                return

            if event.is_edit:
                notifier.warning(
                    f"Edited message recorded (version={message_state.version}) and skipped for execution"
                )
                return

            if _below_confidence_threshold(config, parse_outcome):
                reason = (
                    f"confidence {parse_outcome.confidence:.2f} below threshold "
                    f"{config.llm.confidence_threshold:.2f}; manual confirmation required"
                )
                action_type = "ENTRY" if isinstance(parsed, EntrySignal) else "MANAGE"
                store.record_execution(
                    chat_id=event.chat_id,
                    message_id=event.message_id,
                    version=message_state.version,
                    action_type=action_type,
                    symbol=getattr(parsed, "symbol", None),
                    side=getattr(getattr(parsed, "side", None), "value", None),
                    status="PENDING_CONFIRMATION",
                    reason=reason,
                    intent={
                        "parsed": _to_dict(parsed),
                        "parse_source": parse_outcome.parse_source,
                        "confidence": parse_outcome.confidence,
                    },
                )
                notifier.warning(reason)
                return

            if isinstance(parsed, EntrySignal):
                await _handle_entry(
                    config=config,
                    store=store,
                    risk_manager=risk_manager,
                    bitget=bitget,
                    executor=executor,
                    parsed=parsed,
                    chat_id=event.chat_id,
                    message_id=event.message_id,
                    version=message_state.version,
                )
                return

            if isinstance(parsed, ManageAction):
                decision = risk_manager.evaluate_manage(parsed)
                if not decision.approved:
                    store.record_execution(
                        chat_id=event.chat_id,
                        message_id=event.message_id,
                        version=message_state.version,
                        action_type="MANAGE",
                        symbol=parsed.symbol,
                        side=None,
                        status="REJECTED",
                        reason=decision.reason,
                        intent=_to_dict(parsed),
                    )
                    notifier.warning(f"MANAGE rejected: {decision.reason}")
                    return

                executor.execute_manage(parsed, event.chat_id, event.message_id, message_state.version)
        except Exception as exc:  # noqa: BLE001
            logger.exception("Unhandled processing error for message_id=%s", getattr(event, "message_id", "?"))
            store.record_execution(
                chat_id=getattr(event, "chat_id", 0),
                message_id=getattr(event, "message_id", 0),
                version=1,
                action_type="SYSTEM",
                symbol=None,
                side=None,
                status="FAILED",
                reason=str(exc),
                intent=None,
            )

    listener = TelegramListener(config.telegram, logger)
    try:
        await listener.run(on_telegram_event)
    finally:
        refresh_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await refresh_task
        store.close()


async def _handle_entry(
    config: AppConfig,
    store: SQLiteStore,
    risk_manager: RiskManager,
    bitget: BitgetClient,
    executor: TradeExecutor,
    parsed: EntrySignal,
    chat_id: int,
    message_id: int,
    version: int,
) -> None:
    now = utc_now()
    within_cooldown = store.within_cooldown(
        parsed.symbol,
        parsed.side.value,
        config.risk.cooldown_seconds,
        now=now,
    )

    try:
        current_price = bitget.get_ticker_price(parsed.symbol)
    except Exception as exc:  # noqa: BLE001
        store.record_execution(
            chat_id,
            message_id,
            version,
            action_type="ENTRY",
            symbol=parsed.symbol,
            side=parsed.side.value,
            status="REJECTED",
            reason=f"ticker unavailable: {exc}",
            intent=_to_dict(parsed),
        )
        return

    if config.dry_run:
        account_equity = config.risk.assumed_equity_usdt
    else:
        try:
            account_equity = bitget.get_account_equity()
        except Exception as exc:  # noqa: BLE001
            store.record_execution(
                chat_id,
                message_id,
                version,
                action_type="ENTRY",
                symbol=parsed.symbol,
                side=parsed.side.value,
                status="REJECTED",
                reason=f"equity unavailable: {exc}",
                intent=_to_dict(parsed),
            )
            return

    decision = risk_manager.evaluate_entry(
        signal=parsed,
        current_price=current_price,
        account_equity=account_equity,
        now=now.astimezone(timezone.utc),
        within_cooldown=within_cooldown,
    )

    if not decision.approved:
        store.record_execution(
            chat_id,
            message_id,
            version,
            action_type="ENTRY",
            symbol=parsed.symbol,
            side=parsed.side.value,
            status="REJECTED",
            reason=decision.reason,
            intent=_to_dict(parsed),
        )
        return

    executor.execute_entry(parsed, decision, chat_id, message_id, version)


def _to_dict(payload: ParsedMessage | Any) -> dict[str, Any]:
    if is_dataclass(payload):
        return asdict(payload)
    if isinstance(payload, dict):
        return payload
    return {"value": str(payload)}


def _below_confidence_threshold(config: AppConfig, outcome: ParseOutcome) -> bool:
    if not config.llm.require_confirmation_below_threshold:
        return False
    return outcome.confidence < config.llm.confidence_threshold


async def _symbol_registry_refresh_loop(registry: SymbolRegistry, logger: logging.Logger) -> None:
    while True:
        await asyncio.sleep(1800)
        try:
            registry.refresh(force=True)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Scheduled SymbolRegistry refresh failed: %s", exc)


def main() -> None:
    app()


if __name__ == "__main__":
    main()
