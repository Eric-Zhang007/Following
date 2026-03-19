# Project Progress

## Goal
Build a runnable Python trading executor:
Telegram signal listening -> parsing (rules + optional LLM) -> risk/filtering -> Bitget execution.

## Plan
1. Scaffold package metadata, config examples, and CLI entrypoints.
2. Implement core modules (listener, parser, risk, bitget client, executor, sqlite store, notifier).
3. Add parser/risk unit tests.
4. Run tests and fix issues.
5. Finalize README + quickstart.

## Progress Log
- [x] Created project scaffold and pyproject with dependencies.
- [x] Implemented YAML+pydantic config loader and strict schema.
- [x] Implemented Telethon listener for NewMessage + MessageEdited.
- [x] Implemented parser for ENTRY_SIGNAL / MANAGE_ACTION / NON_SIGNAL with simple state machine.
- [x] Implemented risk filters and sizing logic with explicit reject reasons.
- [x] Implemented Bitget REST client with signature and required endpoints.
- [x] Implemented executor with dry_run guard and SQLite persistence.
- [x] Implemented SQLite schema for message idempotency, parsing, execution, receipts.
- [x] Added parser tests (8 cases) and risk tests (5 cases).
- [x] Added optional OpenAI structured-output parser (rules_only/hybrid/llm_only).
- [x] Added LLM schema validation, sanitization, and SQLite parse cache.
- [x] Added confidence threshold gating (`PENDING_CONFIRMATION`) before execution.
- [x] Added tests for LLM schema and hybrid parser flow.
- [x] Run pytest and resolve any failures.

## Known MVP Limits
- `move_sl_to_be` currently records `PENDING_MANUAL` instead of full stop-order automation.
- Bitget endpoint field nuances can vary by account mode; production rollout should re-check symbol/size conventions.
