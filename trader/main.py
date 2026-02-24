from __future__ import annotations

import asyncio
import contextlib
import logging
import signal
from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import typer

from trader.account_poller import AccountPoller
from trader.alerts import AlertManager
from trader.bitget_client import BitgetClient
from trader.config import AppConfig, load_config
from trader.executor import TradeExecutor
from trader.health_server import HealthServer
from trader.kill_switch import KillSwitch
from trader.llm_parser import HybridSignalParser, ParseOutcome
from trader.media import MediaManager
from trader.models import EntrySignal, ManageAction, NeedsManual, NonSignal, ParsedKind, ParsedMessage, TelegramEvent, utc_now
from trader.notifier import Notifier
from trader.order_reconciler import OrderReconciler
from trader.price_feed import PriceFeed
from trader.risk import RiskManager
from trader.risk_daemon import RiskDaemon
from trader.signal_validator import validate_parsed_message
from trader.state import StateStore
from trader.startup_probe import probe_plan_order_capability_on_startup
from trader.stoploss_manager import StopLossManager
from trader.store import SQLiteStore
from trader.symbol_registry import SymbolRegistry
from trader.telegram_listener import TelegramListener
from trader.web_preview_listener import WebPreviewListener

app = typer.Typer(add_completion=False, help="Telegram/WebPreview signal -> Bitget executor")


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
    """Run listener and execution loop."""
    asyncio.run(_run_async(config))


async def _run_async(config_path: Path) -> None:
    config = load_config(config_path)
    logger = _setup_logging(config)
    notifier = Notifier(logger)

    store = SQLiteStore(config.storage.db_path)
    alerts = AlertManager(notifier=notifier, store=store, logger=logger, min_level=config.monitor.alerts.level)
    runtime_state = StateStore()

    parser_engine = HybridSignalParser(config, store, logger)
    bitget = BitgetClient(config.bitget)
    symbol_registry = SymbolRegistry(bitget, logger)
    media_manager = MediaManager(
        media_dir=config.storage.media_dir,
        store=store,
        logger=logger,
        timeout_seconds=config.listener.request_timeout_seconds,
        max_retries=config.listener.max_retries,
        backoff_seconds=config.listener.backoff_seconds,
    )

    try:
        symbol_registry.refresh(force=True)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Initial SymbolRegistry refresh failed: %s", exc)

    probe_plan_order_capability_on_startup(
        config=config,
        bitget=bitget,
        alerts=alerts,
        runtime_state=runtime_state,
    )

    risk_manager = RiskManager(config, symbol_registry=symbol_registry)
    stoploss_manager = StopLossManager(
        config=config,
        bitget=bitget,
        state=runtime_state,
        store=store,
        alerts=alerts,
    )
    executor = TradeExecutor(
        config,
        bitget,
        store,
        notifier,
        logger,
        symbol_registry=symbol_registry,
        runtime_state=runtime_state,
        stoploss_manager=stoploss_manager,
    )

    stop_event = asyncio.Event()
    _install_signal_handlers(stop_event, logger)

    monitor_tasks: list[asyncio.Task] = []
    refresh_task = asyncio.create_task(_symbol_registry_refresh_loop(symbol_registry, logger, stop_event))

    if config.monitor.enabled:
        poller = AccountPoller(config=config, bitget=bitget, state=runtime_state, store=store, alerts=alerts)
        price_feed = PriceFeed(config=config, bitget=bitget, state=runtime_state, alerts=alerts)
        reconciler = OrderReconciler(
            config=config,
            bitget=bitget,
            state=runtime_state,
            store=store,
            alerts=alerts,
            stoploss_manager=stoploss_manager,
        )
        kill_switch = KillSwitch(store=store)
        risk_daemon = RiskDaemon(
            config=config,
            bitget=bitget,
            state=runtime_state,
            store=store,
            alerts=alerts,
            kill_switch=kill_switch,
            stoploss_manager=stoploss_manager,
        )
        health_server = HealthServer(config=config, state=runtime_state)

        monitor_tasks = [
            asyncio.create_task(poller.run(stop_event), name="account_poller"),
            asyncio.create_task(price_feed.run(stop_event), name="price_feed"),
            asyncio.create_task(reconciler.run(stop_event), name="order_reconciler"),
            asyncio.create_task(risk_daemon.run(stop_event), name="risk_daemon"),
            asyncio.create_task(health_server.run(stop_event), name="health_server"),
        ]

    logger.info(
        "Starting trader. listener_mode=%s dry_run=%s db=%s llm_mode=%s vlm_enabled=%s monitor=%s",
        config.listener.mode,
        config.dry_run,
        config.storage.db_path,
        config.llm.mode,
        config.vlm.enabled,
        config.monitor.enabled,
    )

    async def on_event(event: TelegramEvent) -> None:
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

            image_bytes: bytes | None = None
            if event.image_url:
                try:
                    media_result = media_manager.download_and_store(event.image_url)
                    image_bytes = media_result.image_bytes
                    store.link_message_media(
                        chat_id=event.chat_id,
                        message_id=event.message_id,
                        version=message_state.version,
                        sha256=media_result.sha256,
                        source_url=event.image_url,
                    )
                except Exception as exc:  # noqa: BLE001
                    logger.warning("image download/store failed for message_id=%s: %s", event.message_id, exc)

            fallback_symbol = store.get_last_entry_symbol(event.chat_id)
            force_vlm = image_bytes is not None and len((event.text or "").strip()) < 20

            parse_outcome = parser_engine.parse(
                chat_id=event.chat_id,
                message_id=event.message_id,
                version=message_state.version,
                text_hash=message_state.text_hash,
                text=event.text,
                source_key=str(event.chat_id),
                fallback_symbol=fallback_symbol,
                timestamp=event.date,
                image_bytes=image_bytes,
                force_vlm=force_vlm,
            )
            parsed = parse_outcome.parsed

            parsed = _enforce_vlm_evidence_gate(
                parsed=parsed,
                outcome=parse_outcome,
                timestamp=event.date,
                has_image=image_bytes is not None,
            )

            store.record_parsed_signal(
                event.chat_id,
                event.message_id,
                message_state.version,
                parsed,
                parse_source=parse_outcome.parse_source,
                confidence=parse_outcome.confidence,
            )

            if parse_outcome.llm_error:
                logger.warning("AI parse error: %s", parse_outcome.llm_error)

            if isinstance(parsed, NeedsManual):
                store.record_execution(
                    chat_id=event.chat_id,
                    message_id=event.message_id,
                    version=message_state.version,
                    action_type="PARSE",
                    symbol=None,
                    side=None,
                    status="PENDING_MANUAL",
                    reason=parsed.reason,
                    intent={
                        "missing_fields": parsed.missing_fields,
                        "parse_source": parse_outcome.parse_source,
                        "confidence": parse_outcome.confidence,
                        "uncertain_fields": parse_outcome.uncertain_fields,
                        "extraction_warnings": parse_outcome.extraction_warnings,
                    },
                )
                notifier.warning(f"Signal requires manual review: {parsed.reason} missing={parsed.missing_fields}")
                return

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

            validation_error = validate_parsed_message(parsed)
            if validation_error:
                action_type = "ENTRY" if isinstance(parsed, EntrySignal) else "MANAGE"
                store.record_execution(
                    chat_id=event.chat_id,
                    message_id=event.message_id,
                    version=message_state.version,
                    action_type=action_type,
                    symbol=getattr(parsed, "symbol", None),
                    side=getattr(getattr(parsed, "side", None), "value", None),
                    status="REJECTED",
                    reason=validation_error,
                    intent={
                        "parsed": _to_dict(parsed),
                        "parse_source": parse_outcome.parse_source,
                        "confidence": parse_outcome.confidence,
                    },
                )
                notifier.warning(f"{action_type} rejected by validation: {validation_error}")
                return

            if event.is_edit:
                notifier.warning(
                    f"Edited message recorded (version={message_state.version}) and skipped for execution"
                )
                return

            if _below_confidence_threshold(config, parse_outcome):
                threshold = _confidence_threshold(config, parse_outcome)
                reason = (
                    f"confidence {parse_outcome.confidence:.2f} below threshold "
                    f"{threshold:.2f}; notify_only"
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
                        "uncertain_fields": parse_outcome.uncertain_fields,
                        "extraction_warnings": parse_outcome.extraction_warnings,
                    },
                )
                notifier.warning(reason)
                return

            if isinstance(parsed, EntrySignal):
                if runtime_state.safe_mode:
                    reason = f"safe_mode active: {runtime_state.block_new_entries_reason or 'risk daemon'}"
                    store.record_execution(
                        chat_id=event.chat_id,
                        message_id=event.message_id,
                        version=message_state.version,
                        action_type="ENTRY",
                        symbol=parsed.symbol,
                        side=parsed.side.value,
                        status="REJECTED",
                        reason=reason,
                        intent=_to_dict(parsed),
                    )
                    notifier.warning(f"ENTRY blocked: {reason}")
                    return

                await _handle_entry(
                    config=config,
                    store=store,
                    risk_manager=risk_manager,
                    bitget=bitget,
                    executor=executor,
                    notifier=notifier,
                    parsed=parsed,
                    chat_id=event.chat_id,
                    message_id=event.message_id,
                    version=message_state.version,
                    signal_quality=_signal_quality(parse_outcome),
                    runtime_state=runtime_state,
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
            runtime_state.register_api_error()

    if config.listener.mode == "web_preview":
        listener = WebPreviewListener(config.listener, logger)
    else:
        listener = TelegramListener(config.telegram, logger)

    listener_task = asyncio.create_task(listener.run(on_event), name="listener")
    stop_wait_task = asyncio.create_task(stop_event.wait(), name="stop_wait")

    try:
        done, _ = await asyncio.wait({listener_task, stop_wait_task}, return_when=asyncio.FIRST_COMPLETED)
        if listener_task in done and listener_task.exception() is not None:
            raise listener_task.exception()
    finally:
        stop_event.set()
        listener_task.cancel()
        stop_wait_task.cancel()
        refresh_task.cancel()
        for task in monitor_tasks:
            task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await listener_task
        with contextlib.suppress(asyncio.CancelledError):
            await stop_wait_task
        with contextlib.suppress(asyncio.CancelledError):
            await refresh_task
        for task in monitor_tasks:
            with contextlib.suppress(asyncio.CancelledError):
                await task
        store.save_runtime_snapshot(runtime_state.to_snapshot())
        store.close()


async def _handle_entry(
    config: AppConfig,
    store: SQLiteStore,
    risk_manager: RiskManager,
    bitget: BitgetClient,
    executor: TradeExecutor,
    notifier: Notifier,
    parsed: EntrySignal,
    chat_id: int,
    message_id: int,
    version: int,
    signal_quality: float,
    runtime_state: StateStore | None,
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
        notifier.warning(f"ENTRY rejected: ticker unavailable for {parsed.symbol}")
        if runtime_state is not None:
            runtime_state.register_api_error()
        return

    if runtime_state is not None and runtime_state.account is not None:
        account_equity = runtime_state.account.equity
        open_positions_count = len(runtime_state.positions)
    elif config.dry_run:
        account_equity = config.risk.assumed_equity_usdt
        open_positions_count = 0
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
            notifier.warning(f"ENTRY rejected: equity unavailable for {parsed.symbol}")
            if runtime_state is not None:
                runtime_state.register_api_error()
            return
        try:
            open_positions_count = bitget.get_open_positions_count()
        except Exception:  # noqa: BLE001
            open_positions_count = 0

    decision = risk_manager.evaluate_entry(
        signal=parsed,
        current_price=current_price,
        account_equity=account_equity,
        now=now.astimezone(timezone.utc),
        within_cooldown=within_cooldown,
        open_positions_count=open_positions_count,
        signal_quality=signal_quality,
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
        notifier.warning(f"ENTRY rejected: {decision.reason}")
        return

    for warning in decision.warnings:
        notifier.warning(warning)

    executor.execute_entry(parsed, decision, chat_id, message_id, version)


def _install_signal_handlers(stop_event: asyncio.Event, logger: logging.Logger) -> None:
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            logger.warning("Signal handler not supported on this platform: %s", sig)


def _to_dict(payload: ParsedMessage | Any) -> dict[str, Any]:
    if is_dataclass(payload):
        return asdict(payload)
    if isinstance(payload, dict):
        return payload
    return {"value": str(payload)}


def _below_confidence_threshold(config: AppConfig, outcome: ParseOutcome) -> bool:
    if outcome.parse_source.startswith("VLM"):
        return outcome.confidence < config.vlm.confidence_threshold
    if not config.llm.require_confirmation_below_threshold:
        return False
    return outcome.confidence < config.llm.confidence_threshold


def _confidence_threshold(config: AppConfig, outcome: ParseOutcome) -> float:
    if outcome.parse_source.startswith("VLM"):
        return config.vlm.confidence_threshold
    return config.llm.confidence_threshold


def _signal_quality(outcome: ParseOutcome) -> float:
    if outcome.parse_source == "RULES":
        return 1.0
    return float(outcome.confidence)


def _enforce_vlm_evidence_gate(
    *,
    parsed: ParsedMessage,
    outcome: ParseOutcome,
    timestamp: datetime | None,
    has_image: bool,
) -> ParsedMessage:
    if isinstance(parsed, NeedsManual) or isinstance(parsed, NonSignal):
        return parsed

    if has_image and not outcome.parse_source.startswith("VLM"):
        return NeedsManual(
            kind=ParsedKind.NEEDS_MANUAL,
            raw_text=getattr(parsed, "raw_text", ""),
            reason="image_post_requires_vlm_manual_on_fallback",
            missing_fields=["vlm_output"],
            timestamp=timestamp,
        )

    if not outcome.parse_source.startswith("VLM"):
        return parsed

    payload = outcome.llm_payload or {}
    evidence = payload.get("evidence", {}) if isinstance(payload, dict) else {}
    field_evidence = evidence.get("field_evidence", {}) if isinstance(evidence, dict) else {}
    if not isinstance(field_evidence, dict):
        field_evidence = {}

    required_fields: list[str] = []
    if isinstance(parsed, EntrySignal):
        required_fields.extend(["symbol", "side", "entry.low", "entry.high"])
        if parsed.stop_loss is not None:
            required_fields.append("entry.stop_loss")
        if parsed.take_profit:
            required_fields.append("entry.tp")
    elif isinstance(parsed, ManageAction):
        if parsed.symbol:
            required_fields.append("symbol")
        if parsed.reduce_pct is not None:
            required_fields.append("manage.reduce_pct")
        if parsed.move_sl_to_be:
            required_fields.append("manage.move_sl_to_be")
        if parsed.tp_price is not None:
            required_fields.append("manage.tp")

    missing: list[str] = []
    for fp in required_fields:
        if not _has_field_evidence(field_evidence, fp):
            missing.append(fp)

    if missing:
        return NeedsManual(
            kind=ParsedKind.NEEDS_MANUAL,
            raw_text=getattr(parsed, "raw_text", ""),
            reason="missing_evidence_for_order_fields",
            missing_fields=missing,
            timestamp=timestamp,
        )
    return parsed


def _has_field_evidence(field_evidence: dict[str, list[str]], field_path: str) -> bool:
    if field_path in field_evidence and field_evidence.get(field_path):
        return True
    if field_path in {"entry.tp", "manage.tp"}:
        return any(str(key).startswith(field_path) and field_evidence.get(str(key)) for key in field_evidence.keys())
    if field_path == "entry.stop_loss":
        aliases = ("entry.stop_loss", "entry.sl", "stop_loss")
        return any(field_evidence.get(alias) for alias in aliases)
    return False


async def _symbol_registry_refresh_loop(
    registry: SymbolRegistry,
    logger: logging.Logger,
    stop_event: asyncio.Event,
) -> None:
    while not stop_event.is_set():
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=1800)
            break
        except TimeoutError:
            pass
        try:
            registry.refresh(force=True)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Scheduled SymbolRegistry refresh failed: %s", exc)


def main() -> None:
    app()


if __name__ == "__main__":
    main()
