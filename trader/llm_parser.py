from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from trader.config import AppConfig
from trader.llm_client import OpenAIResponsesClient
from trader.llm_schema import LLMParsedOutput
from trader.models import EntrySignal, ManageAction, NonSignal, ParsedKind, ParsedMessage
from trader.parser import SignalParser
from trader.sanitize import sanitize_text
from trader.store import SQLiteStore


@dataclass
class ParseOutcome:
    parsed: ParsedMessage
    parse_source: str
    confidence: float
    notes: str = ""
    llm_payload: dict[str, Any] | None = None
    llm_error: str | None = None


class LLMParseError(RuntimeError):
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


class HybridSignalParser:
    def __init__(
        self,
        config: AppConfig,
        store: SQLiteStore,
        logger: logging.Logger,
        rules_parser: SignalParser | None = None,
        llm_parser: LLMParser | None = None,
    ) -> None:
        self.config = config
        self.store = store
        self.logger = logger
        self.rules_parser = rules_parser or SignalParser()
        self.llm_parser = llm_parser

        if self.config.llm.enabled and self.config.llm.mode in {"hybrid", "llm_only"} and self.llm_parser is None:
            self.llm_parser = LLMParser(config, store, logger)

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
    ) -> ParseOutcome:
        mode = self.config.llm.mode
        llm_allowed = self.config.llm.enabled and mode in {"hybrid", "llm_only"}

        rules_outcome = self._parse_rules(text, source_key, fallback_symbol, timestamp)

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

    @staticmethod
    def _is_complete(parsed: ParsedMessage) -> bool:
        if isinstance(parsed, NonSignal):
            return False

        if isinstance(parsed, EntrySignal):
            return bool(parsed.symbol and parsed.side and parsed.entry_low > 0 and parsed.entry_high > 0)

        if isinstance(parsed, ManageAction):
            has_action = parsed.reduce_pct is not None or parsed.move_sl_to_be or parsed.tp_price is not None
            return parsed.symbol is not None and has_action

        return False
