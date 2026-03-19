from __future__ import annotations

import asyncio
import contextlib
import logging
import signal
from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import typer

from trader.account_poller import AccountPoller
from trader.alerts import AlertManager
from trader.bitget_client import BitgetClient
from trader.config import AppConfig, load_config
from trader.device_auth_relay import DeviceAuthRelay
from trader.discussion_filter import (
    chat_id_variants as _chat_id_variants_impl,
    is_channel_chat as _is_channel_chat_impl,
    is_discussion_chat as _is_discussion_chat_impl,
    should_skip_discussion_noise as _should_skip_discussion_noise_impl,
)
from trader.email_alert import SMTPAlertSender
from trader.entry_fallback import convert_market_to_limit_signal, is_market_slippage_reject
from trader.executor import TradeExecutor
from trader.health_server import HealthServer
from trader.kill_switch import KillSwitch
from trader.llm_parser import HybridSignalParser, ParseOutcome
from trader.media import MediaManager
from trader.models import EntrySignal, EntryType, ManageAction, NeedsManual, NonSignal, ParsedKind, ParsedMessage, TelegramEvent, utc_now
from trader.notifier import Notifier
from trader.order_reconciler import OrderReconciler
from trader.private_manage_guards import (
    private_manage_edit_ignore_reason,
    resolve_private_fallback_symbol,
    should_reject_reply_manage_without_thread_symbol,
)
from trader.private_channel_parser import PrivateChannelParser
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
from trader.telegram_private_listener import TelegramPrivateListener
from trader.threading_router import ThreadResolveResult, TradeThreadRouter
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
    email_sender = SMTPAlertSender(config.alerts.email)
    alerts = AlertManager(
        notifier=notifier,
        store=store,
        logger=logger,
        min_level=config.monitor.alerts.level,
        email_sender=email_sender,
    )
    device_auth_relay = DeviceAuthRelay(config.alerts.device_auth_relay, config.alerts.email, alerts, logger, store)
    runtime_state = StateStore()

    parser_engine = HybridSignalParser(config, store, logger)
    private_parser = PrivateChannelParser(config)
    thread_router = TradeThreadRouter(store)
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
        alerts=alerts,
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
            symbol_registry=symbol_registry,
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
            symbol_registry=symbol_registry,
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

    if not config.risk.enabled:
        alerts.error(
            "RISK_MODE_DISABLED",
            "risk.enabled=false, strategy risk checks are disabled",
            {
                "hard_invariants": config.risk.hard_invariants.model_dump(mode="json"),
            },
        )

    async def on_event(event: TelegramEvent) -> bool:
        if config.listener.mode == "telegram_private":
            if await device_auth_relay.maybe_handle(event):
                return False
            return await _handle_private_event(
                config=config,
                store=store,
                parser=private_parser,
                thread_router=thread_router,
                bitget=bitget,
                risk_manager=risk_manager,
                executor=executor,
                notifier=notifier,
                alerts=alerts,
                event=event,
                runtime_state=runtime_state,
            )
        try:
            message_state = store.record_message(
                chat_id=event.chat_id,
                message_id=event.message_id,
                text=event.text,
                is_edit=event.is_edit,
                event_time=event.date,
            )

            if message_state.duplicate and not event.is_edit:
                if store.has_message_processing_records(
                    chat_id=event.chat_id,
                    message_id=event.message_id,
                    version=message_state.version,
                ):
                    logger.info("Duplicate message ignored: chat=%s message=%s", event.chat_id, event.message_id)
                    return False
                logger.warning(
                    "Duplicate message without processing records; reprocessing. chat=%s message=%s version=%s",
                    event.chat_id,
                    event.message_id,
                    message_state.version,
                )

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
                return True

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
                return True

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
                return True

            if event.is_edit:
                notifier.warning(
                    f"Edited message recorded (version={message_state.version}) and skipped for execution"
                )
                return True

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
                return True

            if isinstance(parsed, EntrySignal):
                if runtime_state.panic_mode:
                    reason = f"panic_mode active: {runtime_state.block_new_entries_reason or 'risk daemon'}"
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
                    return True

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
                return True

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
                    return True

                executor.execute_manage(parsed, event.chat_id, event.message_id, message_state.version)
                return True
            return True
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
            return False

    if config.listener.mode == "web_preview":
        listener = WebPreviewListener(config.listener, logger)
    elif config.listener.mode == "telegram_private":
        listener = TelegramPrivateListener(
            config.telegram,
            logger,
            media_dir=str(Path(config.storage.media_dir) / "telegram_private"),
            control_usernames=config.alerts.device_auth_relay.trigger_usernames
            if config.alerts.device_auth_relay.enabled
            else [],
        )
    else:
        listener = TelegramListener(config.telegram, logger)

    async def on_private_ignored(payload: dict[str, Any]) -> None:
        chat_id = int(payload.get("channel_id", 0) or 0)
        message_id = int(payload.get("message_id", 0) or 0)
        store.record_execution(
            chat_id=chat_id,
            message_id=message_id,
            version=1,
            action_type="SYSTEM",
            purpose="ENTRY",
            symbol=None,
            side=None,
            status="REJECTED",
            reason="ignored_before_startup",
            intent=payload,
        )
        alerts.warn(
            "PRIVATE_MESSAGE_SKIPPED_STARTUP",
            "private channel message ignored because it is older than startup time",
            payload,
        )

    if config.listener.mode == "telegram_private":
        listener_task = asyncio.create_task(
            listener.run(on_event, on_ignored=on_private_ignored),  # type: ignore[call-arg]
            name="listener",
        )
    else:
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

    if not decision.approved and is_market_slippage_reject(decision.reason):
        limit_signal = convert_market_to_limit_signal(parsed)
        if limit_signal is not None:
            limit_decision = risk_manager.evaluate_entry(
                signal=limit_signal,
                current_price=current_price,
                account_equity=account_equity,
                now=now.astimezone(timezone.utc),
                within_cooldown=within_cooldown,
                open_positions_count=open_positions_count,
                signal_quality=signal_quality,
            )
            if limit_decision.approved:
                store.record_event(
                    event_type="ENTRY_MARKET_FALLBACK_LIMIT",
                    level="WARN",
                    msg="market entry rejected by slippage; fallback to limit entry",
                    payload={
                        "symbol": parsed.symbol,
                        "side": parsed.side.value,
                        "reason": decision.reason,
                        "entry_points": limit_signal.entry_points,
                        "current_price": current_price,
                    },
                )
                notifier.warning("ENTRY market slippage fallback: converted to limit entry")
                parsed = limit_signal
                decision = limit_decision

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


async def _handle_private_event(
    *,
    config: AppConfig,
    store: SQLiteStore,
    parser: PrivateChannelParser,
    thread_router: TradeThreadRouter,
    bitget: BitgetClient,
    risk_manager: RiskManager,
    executor: TradeExecutor,
    notifier: Notifier,
    alerts: AlertManager,
    event: TelegramEvent,
    runtime_state: StateStore,
) -> bool:
    text = (event.raw_text or event.text or "").strip()
    if _should_skip_discussion_noise(config=config, event=event, text=text):
        store.record_event(
            event_type="DISCUSSION_NON_REPLY_IGNORED",
            level="INFO",
            msg="discussion message ignored because it is non-reply and has no trade hints",
            payload={"chat_id": event.chat_id, "message_id": event.message_id},
            reason="discussion_non_reply_no_trade_hint",
            thread_id=event.thread_id,
        )
        return True

    message_state = store.record_message(
        chat_id=event.chat_id,
        message_id=event.message_id,
        text=text,
        is_edit=event.is_edit,
        event_time=event.date,
    )
    if message_state.duplicate:
        if event.is_edit:
            store.record_event(
                event_type="THREAD_EDIT_DUPLICATE_IGNORED",
                level="INFO",
                msg="edit ignored because text hash unchanged",
                payload={"message_id": event.message_id, "thread_id": event.thread_id},
                reason="duplicate_edit",
                thread_id=event.thread_id,
            )
            return False
        if store.has_message_processing_records(
            chat_id=event.chat_id,
            message_id=event.message_id,
            version=message_state.version,
        ):
            return False
        store.record_event(
            event_type="THREAD_DUPLICATE_RECOVERY",
            level="WARN",
            msg="duplicate message has no processing records; forcing reprocess",
            payload={"message_id": event.message_id, "version": message_state.version},
            reason="duplicate_without_processing",
            thread_id=event.thread_id,
        )

    thread_result = thread_router.resolve(
        chat_id=event.chat_id,
        message_id=event.message_id,
        text=text,
        reply_to_msg_id=event.reply_to_msg_id,
        reply_to_chat_id=event.reply_to_chat_id,
    )
    if thread_result.thread_id is None:
        # Fallback parse for non-thread messages to reduce missed root signals.
        fallback_outcome = parser.parse(
            text=text,
            timestamp=event.date,
            image_path=event.media_path,
            fallback_symbol=store.get_last_entry_symbol(event.chat_id),
            thread_id=event.message_id,
            is_root=True,
            prefer_llm_fallback=True,
        )
        fallback_parsed = fallback_outcome.parsed
        if isinstance(fallback_parsed, NonSignal):
            recovered = parser.recover_from_non_signal(
                text=text,
                timestamp=event.date,
                image_path=event.media_path,
                fallback_symbol=store.get_last_entry_symbol(event.chat_id),
                thread_id=event.message_id,
            )
            if recovered is not None and isinstance(recovered.parsed, (EntrySignal, ManageAction)):
                fallback_outcome = recovered
                fallback_parsed = recovered.parsed
                store.record_event(
                    event_type="NON_SIGNAL_AI_RECOVERED",
                    level="INFO",
                    msg="non-thread message recovered by AI reparse",
                    payload={
                        "message_id": event.message_id,
                        "parse_source": recovered.parse_source,
                        "confidence": recovered.confidence,
                    },
                    reason="non_thread_non_signal_reparse",
                )
        if isinstance(fallback_parsed, (EntrySignal, ManageAction)):
            store.record_event(
                event_type="THREAD_MESSAGE_FALLBACK_PARSED",
                level="INFO",
                msg="non-thread message accepted by fallback parser",
                payload={
                    "message_id": event.message_id,
                    "parse_source": fallback_outcome.parse_source,
                    "confidence": fallback_outcome.confidence,
                },
                reason=thread_result.reason,
            )
            if isinstance(fallback_parsed, ManageAction):
                symbol = str(fallback_parsed.symbol or "").upper()
                if symbol:
                    existing_for_symbol = store.get_latest_trade_thread_by_symbol(symbol, active_only=True)
                    if existing_for_symbol is not None:
                        thread_result = ThreadResolveResult(
                            thread_id=int(existing_for_symbol["thread_id"]),
                            is_root=False,
                            reason="fallback_manage_bound_by_symbol",
                        )
                    else:
                        thread_id = thread_router.compose_thread_id(chat_id=event.chat_id, message_id=event.message_id)
                        thread_result = ThreadResolveResult(
                            thread_id=thread_id,
                            is_root=True,
                            reason="fallback_manage_new_root",
                        )
                else:
                    thread_id = thread_router.compose_thread_id(chat_id=event.chat_id, message_id=event.message_id)
                    thread_result = ThreadResolveResult(
                        thread_id=thread_id,
                        is_root=True,
                        reason="fallback_manage_new_root",
                    )
            else:
                thread_id = thread_router.compose_thread_id(chat_id=event.chat_id, message_id=event.message_id)
                thread_result = ThreadResolveResult(
                    thread_id=thread_id,
                    is_root=True,
                    reason="fallback_root_parsed",
                )
        else:
            store.record_event(
                event_type="THREAD_MESSAGE_IGNORED",
                level="INFO",
                msg="message ignored because no trade thread mapping",
                payload={"message_id": event.message_id, "reply_to_msg_id": event.reply_to_msg_id},
                reason=thread_result.reason,
            )
            return True

    thread_id = thread_result.thread_id
    event.thread_id = thread_id
    existing_thread = store.get_trade_thread(thread_id)
    if (
        thread_result.is_root
        and existing_thread is None
        and config.risk.hard_invariants.max_concurrent_trades_enforced
        and store.count_active_trade_threads() >= config.execution.max_concurrent_trades
    ):
        store.upsert_trade_thread(
            thread_id=thread_id,
            symbol=None,
            side=None,
            leverage=None,
            status="REJECTED_LIMIT",
        )
        store.record_execution(
            chat_id=event.chat_id,
            message_id=event.message_id,
            version=message_state.version,
            action_type="ENTRY",
            symbol=None,
            side=None,
            status="REJECTED",
            reason="max_concurrent_trades reached",
            intent={"limit": config.execution.max_concurrent_trades},
            thread_id=thread_id,
            purpose="entry",
        )
        alerts.warn(
            "MAX_CONCURRENT_TRADES_REJECTED",
            "entry rejected because active threads reached max_concurrent_trades",
            {"thread_id": thread_id, "limit": config.execution.max_concurrent_trades},
        )
        notifier.warning("ENTRY rejected: max_concurrent_trades reached")
        return True

    if existing_thread is None:
        store.upsert_trade_thread(
            thread_id=thread_id,
            symbol=None,
            side=None,
            leverage=None,
            status="PENDING_ENTRY",
        )
    store.record_thread_message(
        thread_id=thread_id,
        chat_id=event.chat_id,
        message_id=event.message_id,
        is_root=thread_result.is_root,
        kind="ROOT" if thread_result.is_root else "REPLY",
    )

    latest_thread = store.get_trade_thread(thread_id)
    fallback_symbol = resolve_private_fallback_symbol(
        latest_thread=latest_thread,
        chat_id=event.chat_id,
        store=store,
    )
    parse_outcome = parser.parse(
        text=text,
        timestamp=event.date,
        image_path=event.media_path,
        fallback_symbol=fallback_symbol,
        thread_id=thread_id,
        is_root=thread_result.is_root,
    )
    parsed = parse_outcome.parsed
    if isinstance(parsed, NonSignal):
        recovered = parser.recover_from_non_signal(
            text=text,
            timestamp=event.date,
            image_path=event.media_path,
            fallback_symbol=fallback_symbol,
            thread_id=thread_id,
        )
        if recovered is not None and isinstance(recovered.parsed, (EntrySignal, ManageAction)):
            parse_outcome = recovered
            parsed = recovered.parsed
            store.record_event(
                event_type="NON_SIGNAL_AI_RECOVERED",
                level="INFO",
                msg="non-signal recovered by AI reparse",
                payload={
                    "thread_id": thread_id,
                    "message_id": event.message_id,
                    "parse_source": recovered.parse_source,
                    "confidence": recovered.confidence,
                },
                reason="non_signal_reparse",
                thread_id=thread_id,
            )

    if isinstance(parsed, EntrySignal) and parsed.entry_type == EntryType.MARKET and not _entry_has_anchor(parsed):
        ai_anchor = parser.recover_from_non_signal(
            text=text,
            timestamp=event.date,
            image_path=event.media_path,
            fallback_symbol=fallback_symbol,
            thread_id=thread_id,
        )
        if ai_anchor is not None and isinstance(ai_anchor.parsed, EntrySignal) and _entry_has_anchor(ai_anchor.parsed):
            parsed.entry_low = ai_anchor.parsed.entry_low
            parsed.entry_high = ai_anchor.parsed.entry_high
            parsed.entry_points = [float(p) for p in ai_anchor.parsed.entry_points if float(p) > 0]
            if parsed.stop_loss is None and ai_anchor.parsed.stop_loss is not None:
                parsed.stop_loss = float(ai_anchor.parsed.stop_loss)
            if not parsed.tp_points and ai_anchor.parsed.tp_points:
                parsed.tp_points = [float(p) for p in ai_anchor.parsed.tp_points]
                parsed.take_profit = [float(p) for p in ai_anchor.parsed.take_profit or ai_anchor.parsed.tp_points]
            store.record_event(
                event_type="MARKET_ANCHOR_FROM_AI",
                level="INFO",
                msg="market signal anchor hydrated from AI parse",
                payload={
                    "thread_id": thread_id,
                    "symbol": parsed.symbol,
                    "parse_source": ai_anchor.parse_source,
                    "entry_points": parsed.entry_points,
                },
                reason="market_anchor_ai",
                thread_id=thread_id,
            )

    if isinstance(parsed, EntrySignal):
        _hydrate_market_anchor_from_history(
            signal=parsed,
            event_time=event.date,
            bitget=bitget,
            store=store,
            thread_id=thread_id,
        )

    store.record_parsed_signal(
        event.chat_id,
        event.message_id,
        message_state.version,
        parsed,
        parse_source=parse_outcome.parse_source,
        confidence=parse_outcome.confidence,
    )

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
            intent={"missing_fields": parsed.missing_fields, "parse_source": parse_outcome.parse_source},
            thread_id=thread_id,
            purpose="parse",
        )
        return True

    if isinstance(parsed, NonSignal):
        store.record_execution(
            chat_id=event.chat_id,
            message_id=event.message_id,
            version=message_state.version,
            action_type="PARSE",
            symbol=None,
            side=None,
            status="RECORDED",
            reason=parsed.note,
            intent={"parse_source": parse_outcome.parse_source},
            thread_id=thread_id,
            purpose="record",
        )
        return True

    validation_error = validate_parsed_message(parsed)
    if validation_error:
        store.record_execution(
            chat_id=event.chat_id,
            message_id=event.message_id,
            version=message_state.version,
            action_type="ENTRY" if isinstance(parsed, EntrySignal) else "MANAGE",
            symbol=getattr(parsed, "symbol", None),
            side=getattr(getattr(parsed, "side", None), "value", None),
            status="REJECTED",
            reason=validation_error,
            intent=_to_dict(parsed),
            thread_id=thread_id,
            purpose="validate",
        )
        return True

    edit_ignore_reason = private_manage_edit_ignore_reason(
        event=event,
        parsed=parsed,
        parse_source=parse_outcome.parse_source,
    )
    if edit_ignore_reason is not None:
        store.record_execution(
            chat_id=event.chat_id,
            message_id=event.message_id,
            version=message_state.version,
            action_type="MANAGE",
            symbol=getattr(parsed, "symbol", None),
            side=None,
            status="RECORDED",
            reason=edit_ignore_reason,
            intent=_to_dict(parsed),
            thread_id=thread_id,
            purpose="manage",
        )
        store.record_event(
            event_type="SHOWCASE_EDIT_IGNORED",
            level="INFO",
            msg="edited showcase reply ignored to prevent duplicate reduce execution",
            payload={"thread_id": thread_id, "message_id": event.message_id, "version": message_state.version},
            reason=edit_ignore_reason,
            thread_id=thread_id,
        )
        return True

    if isinstance(parsed, EntrySignal):
        existing_status = str((existing_thread or {}).get("status") or "").upper()
        if event.pre_startup and thread_result.is_root and existing_status == "CLOSED":
            store.record_execution(
                chat_id=event.chat_id,
                message_id=event.message_id,
                version=message_state.version,
                action_type="ENTRY",
                symbol=parsed.symbol,
                side=parsed.side.value,
                status="RECORDED",
                reason="prestartup_closed_thread_replay_ignored",
                intent=_to_dict(parsed),
                thread_id=thread_id,
                purpose="entry",
            )
            store.record_event(
                event_type="PRESTARTUP_CLOSED_THREAD_REPLAY_IGNORED",
                level="INFO",
                msg="prestartup root entry replay ignored because thread already closed",
                payload={"thread_id": thread_id, "symbol": parsed.symbol, "message_id": event.message_id},
                reason="closed_thread_replay_ignored",
                thread_id=thread_id,
            )
            return True

        parsed.thread_id = thread_id
        store.upsert_trade_thread(
            thread_id=thread_id,
            symbol=parsed.symbol,
            side=parsed.side.value,
            leverage=parsed.leverage,
            stop_loss=parsed.stop_loss,
            entry_points=parsed.entry_points,
            tp_points=parsed.tp_points or parsed.take_profit,
            status="PENDING_ENTRY",
        )
        _emit_once_per_thread_alert(
            store=store,
            thread_id=thread_id,
            dedupe_key=f"cross_margin:{thread_id}",
            emit=lambda: alerts.warn(
                "CROSS_MARGIN",
                "cross margin mode enabled for this thread",
                {"thread_id": thread_id, "margin_mode": config.execution.margin_mode},
            ),
            should_emit=config.execution.margin_mode == "cross",
        )
        _emit_once_per_thread_alert(
            store=store,
            thread_id=thread_id,
            dedupe_key=f"high_leverage:{thread_id}",
            emit=lambda: alerts.warn(
                "HIGH_LEVERAGE",
                "high leverage entry signal received",
                {"thread_id": thread_id, "symbol": parsed.symbol, "leverage": parsed.leverage},
            ),
            should_emit=(parsed.leverage or 0) >= 20,
        )

        if runtime_state.panic_mode:
            store.record_execution(
                chat_id=event.chat_id,
                message_id=event.message_id,
                version=message_state.version,
                action_type="ENTRY",
                symbol=parsed.symbol,
                side=parsed.side.value,
                status="REJECTED",
                reason=f"panic_mode active: {runtime_state.block_new_entries_reason or 'risk daemon'}",
                intent=_to_dict(parsed),
                thread_id=thread_id,
                purpose="entry",
            )
            store.set_trade_thread_status(thread_id, "REJECTED")
            return True

        if event.is_edit and thread_result.is_root:
            new_version = store.bump_trade_thread_version(thread_id)
            store.record_event(
                event_type="THREAD_TARGET_UPDATED",
                level="WARN",
                msg="root signal edited and thread target version bumped",
                payload={"thread_id": thread_id, "target_version": new_version},
                reason="root_edited",
                thread_id=thread_id,
            )
            executor.apply_thread_edit(
                parsed,
                chat_id=event.chat_id,
                message_id=event.message_id,
                version=message_state.version,
                thread_id=thread_id,
            )
            return True

        if not thread_result.is_root:
            store.record_execution(
                chat_id=event.chat_id,
                message_id=event.message_id,
                version=message_state.version,
                action_type="ENTRY",
                symbol=parsed.symbol,
                side=parsed.side.value,
                status="RECORDED",
                reason="non_root_entry_ignored",
                intent=_to_dict(parsed),
                thread_id=thread_id,
                purpose="entry",
            )
            store.set_trade_thread_status(thread_id, "RECORDED")
            return True

        now = utc_now()
        current_price = parsed.entry_high
        try:
            current_price = bitget.get_ticker_price(parsed.symbol)
        except Exception as exc:  # noqa: BLE001
            store.record_execution(
                event.chat_id,
                event.message_id,
                message_state.version,
                action_type="ENTRY",
                symbol=parsed.symbol,
                side=parsed.side.value,
                status="REJECTED",
                reason=f"ticker unavailable: {exc}",
                intent=_to_dict(parsed),
                thread_id=thread_id,
                purpose="entry",
            )
            notifier.warning(f"ENTRY rejected: ticker unavailable for {parsed.symbol}")
            runtime_state.register_api_error()
            store.set_trade_thread_status(thread_id, "REJECTED")
            return True

        startup_guard_reason = _prestartup_stoploss_guard_reason(
            config=config,
            bitget=bitget,
            signal=parsed,
            event=event,
        )
        if startup_guard_reason:
            store.record_execution(
                chat_id=event.chat_id,
                message_id=event.message_id,
                version=message_state.version,
                action_type="ENTRY",
                symbol=parsed.symbol,
                side=parsed.side.value,
                status="REJECTED",
                reason=startup_guard_reason,
                intent=_to_dict(parsed),
                thread_id=thread_id,
                purpose="entry",
            )
            alerts.warn(
                "PRESTARTUP_STOPLOSS_GUARD_REJECTED",
                "startup replay entry rejected by stop-loss history guard",
                {
                    "thread_id": thread_id,
                    "symbol": parsed.symbol,
                    "side": parsed.side.value,
                    "reason": startup_guard_reason,
                    "message_id": event.message_id,
                },
            )
            notifier.warning(f"ENTRY rejected: {startup_guard_reason}")
            store.set_trade_thread_status(thread_id, "REJECTED")
            return True

        if runtime_state.account is not None:
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
                    event.chat_id,
                    event.message_id,
                    message_state.version,
                    action_type="ENTRY",
                    symbol=parsed.symbol,
                    side=parsed.side.value,
                    status="REJECTED",
                    reason=f"equity unavailable: {exc}",
                    intent=_to_dict(parsed),
                    thread_id=thread_id,
                    purpose="entry",
                )
                notifier.warning(f"ENTRY rejected: equity unavailable for {parsed.symbol}")
                runtime_state.register_api_error()
                store.set_trade_thread_status(thread_id, "REJECTED")
                return True
            try:
                open_positions_count = bitget.get_open_positions_count()
            except Exception:  # noqa: BLE001
                open_positions_count = 0

        within_cooldown = store.within_cooldown(
            parsed.symbol,
            parsed.side.value,
            config.risk.cooldown_seconds,
            now=now,
        )
        decision = risk_manager.evaluate_entry(
            signal=parsed,
            current_price=current_price,
            account_equity=account_equity,
            now=now.astimezone(timezone.utc),
            within_cooldown=within_cooldown,
            open_positions_count=open_positions_count,
            signal_quality=float(parse_outcome.confidence),
            ignore_signal_age=event.pre_startup,
        )
        if not decision.approved and is_market_slippage_reject(decision.reason):
            limit_signal = convert_market_to_limit_signal(parsed)
            if limit_signal is not None:
                limit_decision = risk_manager.evaluate_entry(
                    signal=limit_signal,
                    current_price=current_price,
                    account_equity=account_equity,
                    now=now.astimezone(timezone.utc),
                    within_cooldown=within_cooldown,
                    open_positions_count=open_positions_count,
                    signal_quality=float(parse_outcome.confidence),
                    ignore_signal_age=event.pre_startup,
                )
                if limit_decision.approved:
                    store.record_event(
                        event_type="ENTRY_MARKET_FALLBACK_LIMIT",
                        level="WARN",
                        msg="market entry rejected by slippage; fallback to limit entry",
                        payload={
                            "thread_id": thread_id,
                            "symbol": parsed.symbol,
                            "side": parsed.side.value,
                            "reason": decision.reason,
                            "entry_points": limit_signal.entry_points,
                            "current_price": current_price,
                        },
                        reason="market_slippage_auto_limit",
                        thread_id=thread_id,
                    )
                    notifier.warning("ENTRY market slippage fallback: converted to limit entry")
                    parsed = limit_signal
                    decision = limit_decision
        if not decision.approved:
            store.record_execution(
                chat_id=event.chat_id,
                message_id=event.message_id,
                version=message_state.version,
                action_type="ENTRY",
                symbol=parsed.symbol,
                side=parsed.side.value,
                status="REJECTED",
                reason=decision.reason,
                intent=_to_dict(parsed),
                thread_id=thread_id,
                purpose="entry",
            )
            notifier.warning(f"ENTRY rejected: {decision.reason}")
            store.set_trade_thread_status(thread_id, "REJECTED")
            return True
        for warning in decision.warnings:
            notifier.warning(warning)

        result = executor.execute_thread_entry(
            parsed,
            chat_id=event.chat_id,
            message_id=event.message_id,
            version=message_state.version,
            thread_id=thread_id,
            risk_decision=decision,
        )
        if result.get("placed", 0) > 0:
            store.set_trade_thread_status(thread_id, "ACTIVE")
        else:
            store.set_trade_thread_status(thread_id, "REJECTED")
        return True

    if isinstance(parsed, ManageAction):
        parsed.thread_id = thread_id
        thread = store.get_trade_thread(thread_id)
        if should_reject_reply_manage_without_thread_symbol(
            is_root=thread_result.is_root,
            parsed=parsed,
            thread=thread,
        ):
            store.record_execution(
                chat_id=event.chat_id,
                message_id=event.message_id,
                version=message_state.version,
                action_type="MANAGE",
                symbol=None,
                side=None,
                status="REJECTED",
                reason="reply_manage_unresolved_thread_symbol",
                intent=_to_dict(parsed),
                thread_id=thread_id,
                purpose="manage",
            )
            store.record_event(
                event_type="REPLY_MANAGE_UNRESOLVED_THREAD_REJECTED",
                level="WARN",
                msg="reply manage rejected because thread has no resolved symbol",
                payload={"thread_id": thread_id, "message_id": event.message_id},
                reason="reply_manage_unresolved_thread_symbol",
                thread_id=thread_id,
            )
            return True
        if not parsed.symbol and thread and thread.get("symbol"):
            parsed.symbol = str(thread.get("symbol"))
        if config.risk.enabled:
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
                    thread_id=thread_id,
                    purpose="manage",
                )
                return True
        executor.execute_manage(
            parsed,
            event.chat_id,
            event.message_id,
            message_state.version,
            thread_id=thread_id,
        )
        return True

    return True


def _prestartup_stoploss_guard_reason(
    *,
    config: AppConfig,
    bitget: BitgetClient,
    signal: EntrySignal,
    event: TelegramEvent,
) -> str | None:
    if not event.pre_startup or event.startup_at is None:
        return None

    signal_time = signal.timestamp or event.date
    if signal_time.tzinfo is None:
        signal_time = signal_time.replace(tzinfo=timezone.utc)
    startup_at = event.startup_at
    if startup_at.tzinfo is None:
        startup_at = startup_at.replace(tzinfo=timezone.utc)
    if signal_time >= startup_at:
        return None

    if config.risk.allow_entry_without_stop_loss and signal.stop_loss is None:
        # Strategy can intentionally open first and wait for thread follow-up SL/TP updates.
        return None

    stop_loss_price = _resolve_entry_stop_loss_price(signal, config)
    if stop_loss_price is None or stop_loss_price <= 0:
        return "prestartup_guard_stop_loss_unavailable"

    try:
        touched = bitget.was_stop_loss_touched(
            symbol=signal.symbol,
            side=signal.side.value,
            stop_loss=stop_loss_price,
            start_time=signal_time,
            end_time=startup_at,
            granularity="1m",
        )
    except Exception as exc:  # noqa: BLE001
        return f"prestartup_guard_history_unavailable: {exc}"

    if touched:
        return f"prestartup_stop_loss_touched:{stop_loss_price:.8f}"
    return None




def _entry_has_anchor(signal: EntrySignal) -> bool:
    if any(float(p) > 0 for p in (signal.entry_points or [])):
        return True
    return float(signal.entry_low or 0.0) > 0 or float(signal.entry_high or 0.0) > 0


def _hydrate_market_anchor_from_history(
    *,
    signal: EntrySignal,
    event_time: datetime | None,
    bitget: BitgetClient,
    store: SQLiteStore,
    thread_id: int | None,
) -> None:
    if signal.entry_type != EntryType.MARKET or _entry_has_anchor(signal):
        return
    if event_time is None:
        return
    try:
        ref_price = bitget.get_reference_price_at(symbol=signal.symbol, at_time=event_time)
    except Exception as exc:  # noqa: BLE001
        store.record_event(
            event_type="MARKET_ANCHOR_HISTORY_FAILED",
            level="WARN",
            msg="failed to resolve market anchor from historical candles",
            payload={"symbol": signal.symbol, "reason": str(exc)},
            reason="market_anchor_history_failed",
            thread_id=thread_id,
        )
        return
    if ref_price is None or ref_price <= 0:
        return

    signal.entry_low = float(ref_price)
    signal.entry_high = float(ref_price)
    signal.entry_points = [float(ref_price)]
    store.record_event(
        event_type="MARKET_ANCHOR_FROM_HISTORY",
        level="INFO",
        msg="market signal anchor hydrated from historical candle close",
        payload={"symbol": signal.symbol, "anchor_price": float(ref_price), "event_time": event_time.isoformat()},
        reason="market_anchor_history",
        thread_id=thread_id,
    )


def _resolve_entry_stop_loss_price(signal: EntrySignal, config: AppConfig) -> float | None:
    if signal.stop_loss is not None:
        return float(signal.stop_loss)
    if config.risk.allow_entry_without_stop_loss:
        return None
    entry_price = _pick_entry_price_for_guard(signal, config)
    if entry_price <= 0:
        return None
    ratio = _ratio_from_percent_or_ratio(config.risk.default_stop_loss_pct)
    if ratio <= 0:
        return None
    if signal.side.value == "LONG":
        return entry_price * (1 - ratio)
    return entry_price * (1 + ratio)


def _pick_entry_price_for_guard(signal: EntrySignal, config: AppConfig) -> float:
    if signal.entry_type.value == "MARKET":
        return signal.entry_high
    strategy = config.execution.limit_price_strategy
    if strategy == "LOW":
        return signal.entry_low
    if strategy == "HIGH":
        return signal.entry_high
    return (signal.entry_low + signal.entry_high) / 2


def _ratio_from_percent_or_ratio(value: float) -> float:
    if value <= 0:
        return 0.0
    if value >= 1:
        return value / 100.0
    if value > 0.05:
        return value / 100.0
    return value


def _chat_id_variants(chat_id: int) -> set[int]:
    return _chat_id_variants_impl(chat_id)


def _is_discussion_chat(config: AppConfig, chat_id: int) -> bool:
    return _is_discussion_chat_impl(
        discussion_chat_ids=config.telegram.discussion_chat_ids,
        chat_id=chat_id,
    )


def _is_channel_chat(config: AppConfig, chat_id: int) -> bool:
    return _is_channel_chat_impl(
        channel_id=config.telegram.channel_id,
        channel_ids=config.telegram.channel_ids,
        chat_id=chat_id,
    )


def _should_skip_discussion_noise(*, config: AppConfig, event: TelegramEvent, text: str) -> bool:
    return _should_skip_discussion_noise_impl(
        discussion_chat_ids=config.telegram.discussion_chat_ids,
        channel_id=config.telegram.channel_id,
        channel_ids=config.telegram.channel_ids,
        event=event,
    )


def _emit_once_per_thread_alert(
    *,
    store: SQLiteStore,
    thread_id: int,
    dedupe_key: str,
    emit: Callable[[], None],
    should_emit: bool,
) -> None:
    if not should_emit:
        return
    if store.get_system_flag(dedupe_key):
        return
    emit()
    store.set_system_flag(dedupe_key, str(thread_id))


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
