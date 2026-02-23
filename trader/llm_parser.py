from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from trader.config import AppConfig
from trader.llm_client import OpenAIResponsesClient
from trader.llm_schema import LLMParsedOutput
from trader.models import EntrySignal, ManageAction, NeedsManual, NonSignal, ParsedKind, ParsedMessage
from trader.parser import SignalParser
from trader.sanitize import sanitize_text
from trader.store import SQLiteStore
from trader.vlm_client import VLMClient
from trader.vlm_schema import VLMParsedSignal


@dataclass
class ParseOutcome:
    parsed: ParsedMessage
    parse_source: str
    confidence: float
    notes: str = ""
    uncertain_fields: list[str] | None = None
    extraction_warnings: list[str] | None = None
    llm_payload: dict[str, Any] | None = None
    llm_error: str | None = None


class LLMParseError(RuntimeError):
    pass


class VLMParseError(RuntimeError):
    pass


class LLMParser:
    def __init__(
        self,
        config: AppConfig,
        store: SQLiteStore,
        logger: logging.Logger,
        client: OpenAIResponsesClient | None = None,
    ) -> None:
        self.config = config
        self.store = store
        self.logger = logger
        self.client = client

    def parse(
        self,
        chat_id: int,
        message_id: int,
        version: int,
        text_hash: str,
        text: str,
        fallback_symbol: str | None,
        timestamp: datetime | None,
    ) -> ParseOutcome:
        cached = self.store.get_llm_parse_cache(chat_id, message_id, version, text_hash)
        if cached is not None:
            validated = LLMParsedOutput.model_validate(cached)
            parsed = validated.to_parsed_message(text, timestamp=timestamp, fallback_symbol=fallback_symbol)
            return ParseOutcome(
                parsed=parsed,
                parse_source="LLM_CACHE",
                confidence=validated.confidence,
                notes=validated.notes,
                llm_payload=validated.model_dump(mode="json"),
            )

        sanitized = sanitize_text(text, self.config.llm.redact_patterns)
        try:
            payload = self._ensure_client().parse_signal(sanitized)
            validated = LLMParsedOutput.model_validate(payload)
        except Exception as exc:  # noqa: BLE001
            raise LLMParseError(str(exc)) from exc

        payload_json = validated.model_dump(mode="json")
        self.store.save_llm_parse(
            chat_id=chat_id,
            message_id=message_id,
            version=version,
            text_hash=text_hash,
            provider=self.config.llm.provider,
            model=self.config.llm.model,
            raw_text=text,
            sanitized_text=sanitized,
            response_payload=payload_json,
        )

        parsed = validated.to_parsed_message(text, timestamp=timestamp, fallback_symbol=fallback_symbol)
        return ParseOutcome(
            parsed=parsed,
            parse_source="LLM",
            confidence=validated.confidence,
            notes=validated.notes,
            llm_payload=payload_json,
        )

    def _ensure_client(self) -> OpenAIResponsesClient:
        if self.client is None:
            self.client = OpenAIResponsesClient(self.config.llm)
        return self.client


class VLMParser:
    def __init__(
        self,
        config: AppConfig,
        store: SQLiteStore,
        logger: logging.Logger,
        client: VLMClient | None = None,
    ) -> None:
        self.config = config
        self.store = store
        self.logger = logger
        self.client = client

    def parse(
        self,
        chat_id: int,
        message_id: int,
        version: int,
        text_hash: str,
        text: str,
        fallback_symbol: str | None,
        timestamp: datetime | None,
        image_bytes: bytes | None,
    ) -> ParseOutcome:
        cache = self.store.get_llm_parse_cache(chat_id, message_id, version, text_hash)
        if cache is not None:
            validated = VLMParsedSignal.model_validate(cache)
            parsed = validated.to_parsed_message(text, timestamp=timestamp, fallback_symbol=fallback_symbol)
            return ParseOutcome(
                parsed=parsed,
                parse_source="VLM_CACHE",
                confidence=validated.confidence,
                notes=validated.notes,
                uncertain_fields=validated.uncertain_fields,
                extraction_warnings=validated.extraction_warnings,
                llm_payload=validated.model_dump(mode="json"),
            )

        sanitized = sanitize_text(text, self.config.llm.redact_patterns)
        try:
            validated = self._ensure_client().extract(image_bytes=image_bytes, text_context=sanitized)
        except Exception as exc:  # noqa: BLE001
            raise VLMParseError(str(exc)) from exc

        payload_json = validated.model_dump(mode="json")
        self.store.save_llm_parse(
            chat_id=chat_id,
            message_id=message_id,
            version=version,
            text_hash=text_hash,
            provider=self.config.vlm.provider,
            model=self.config.vlm.model,
            raw_text=text,
            sanitized_text=sanitized,
            response_payload=payload_json,
        )

        parsed = validated.to_parsed_message(text, timestamp=timestamp, fallback_symbol=fallback_symbol)
        return ParseOutcome(
            parsed=parsed,
            parse_source="VLM",
            confidence=validated.confidence,
            notes=validated.notes,
            uncertain_fields=validated.uncertain_fields,
            extraction_warnings=validated.extraction_warnings,
            llm_payload=payload_json,
        )

    def _ensure_client(self) -> VLMClient:
        if self.client is None:
            self.client = VLMClient(self.config.vlm)
        return self.client


class HybridSignalParser:
    def __init__(
        self,
        config: AppConfig,
        store: SQLiteStore,
        logger: logging.Logger,
        rules_parser: SignalParser | None = None,
        llm_parser: LLMParser | None = None,
        vlm_parser: VLMParser | None = None,
    ) -> None:
        self.config = config
        self.store = store
        self.logger = logger
        self.rules_parser = rules_parser or SignalParser()
        self.llm_parser = llm_parser
        self.vlm_parser = vlm_parser

        if self.config.llm.enabled and self.config.llm.mode in {"hybrid", "llm_only"} and self.llm_parser is None:
            self.llm_parser = LLMParser(config, store, logger)
        if self.config.vlm.enabled and self.vlm_parser is None:
            self.vlm_parser = VLMParser(config, store, logger)

    def parse(
        self,
        chat_id: int,
        message_id: int,
        version: int,
        text_hash: str,
        text: str,
        source_key: str,
        fallback_symbol: str | None,
        timestamp: datetime | None,
        image_bytes: bytes | None = None,
        force_vlm: bool = False,
    ) -> ParseOutcome:
        mode = self.config.llm.mode
        llm_allowed = self.config.llm.enabled and mode in {"hybrid", "llm_only"}

        rules_outcome = self._parse_rules(text, source_key, fallback_symbol, timestamp)

        if self._should_call_vlm(rules_outcome.parsed, image_bytes=image_bytes, force_vlm=force_vlm):
            try:
                return self._parse_vlm(
                    chat_id=chat_id,
                    message_id=message_id,
                    version=version,
                    text_hash=text_hash,
                    text=text,
                    fallback_symbol=fallback_symbol,
                    timestamp=timestamp,
                    image_bytes=image_bytes,
                )
            except VLMParseError as exc:
                self.logger.warning("VLM parse failed in hybrid mode, fallback to rules: %s", exc)
                return ParseOutcome(
                    parsed=rules_outcome.parsed,
                    parse_source="VLM_FALLBACK_RULES",
                    confidence=0.0,
                    notes="vlm_unavailable_fallback_rules",
                    llm_error=str(exc),
                )

        if mode == "rules_only" or not llm_allowed:
            return rules_outcome

        if mode == "hybrid":
            if self._is_complete(rules_outcome.parsed):
                return rules_outcome
            try:
                return self._parse_llm(chat_id, message_id, version, text_hash, text, fallback_symbol, timestamp)
            except LLMParseError as exc:
                self.logger.warning("LLM parse failed in hybrid mode, fallback to rules: %s", exc)
                return ParseOutcome(
                    parsed=rules_outcome.parsed,
                    parse_source="RULES_FALLBACK",
                    confidence=0.0,
                    notes="llm_unavailable_fallback_rules",
                    llm_error=str(exc),
                )

        # llm_only
        try:
            return self._parse_llm(chat_id, message_id, version, text_hash, text, fallback_symbol, timestamp)
        except LLMParseError as exc:
            self.logger.warning("LLM parse failed in llm_only mode: %s", exc)
            return ParseOutcome(
                parsed=NonSignal(
                    kind=ParsedKind.NON_SIGNAL,
                    raw_text=text,
                    note="llm_parse_failed",
                    timestamp=timestamp,
                ),
                parse_source="LLM_ERROR",
                confidence=0.0,
                notes="llm_parse_failed",
                llm_error=str(exc),
            )

    def _parse_rules(
        self,
        text: str,
        source_key: str,
        fallback_symbol: str | None,
        timestamp: datetime | None,
    ) -> ParseOutcome:
        parsed = self.rules_parser.parse(
            text=text,
            source_key=source_key,
            fallback_symbol=fallback_symbol,
            timestamp=timestamp,
        )
        return ParseOutcome(parsed=parsed, parse_source="RULES", confidence=1.0)

    def _parse_vlm(
        self,
        chat_id: int,
        message_id: int,
        version: int,
        text_hash: str,
        text: str,
        fallback_symbol: str | None,
        timestamp: datetime | None,
        image_bytes: bytes | None,
    ) -> ParseOutcome:
        if self.vlm_parser is None:
            raise VLMParseError("vlm parser is not configured")
        return self.vlm_parser.parse(
            chat_id=chat_id,
            message_id=message_id,
            version=version,
            text_hash=text_hash,
            text=text,
            fallback_symbol=fallback_symbol,
            timestamp=timestamp,
            image_bytes=image_bytes,
        )

    def _parse_llm(
        self,
        chat_id: int,
        message_id: int,
        version: int,
        text_hash: str,
        text: str,
        fallback_symbol: str | None,
        timestamp: datetime | None,
    ) -> ParseOutcome:
        if self.llm_parser is None:
            raise LLMParseError("llm parser is not configured")
        return self.llm_parser.parse(
            chat_id=chat_id,
            message_id=message_id,
            version=version,
            text_hash=text_hash,
            text=text,
            fallback_symbol=fallback_symbol,
            timestamp=timestamp,
        )

    def _should_call_vlm(self, parsed: ParsedMessage, image_bytes: bytes | None, force_vlm: bool) -> bool:
        if not self.config.vlm.enabled:
            return False
        if force_vlm:
            return True
        if image_bytes is not None:
            return True
        return not self._is_complete(parsed)

    @staticmethod
    def _is_complete(parsed: ParsedMessage) -> bool:
        if isinstance(parsed, NonSignal):
            return False
        if isinstance(parsed, NeedsManual):
            return False

        if isinstance(parsed, EntrySignal):
            return bool(parsed.symbol and parsed.side and parsed.entry_low > 0 and parsed.entry_high > 0)

        if isinstance(parsed, ManageAction):
            has_action = parsed.reduce_pct is not None or parsed.move_sl_to_be or parsed.tp_price is not None
            return parsed.symbol is not None and has_action

        return False
