from __future__ import annotations

import logging


class Notifier:
    def __init__(self, logger: logging.Logger) -> None:
        self.logger = logger

    def info(self, message: str) -> None:
        self.logger.info("[NOTIFY] %s", message)

    def warning(self, message: str) -> None:
        self.logger.warning("[NOTIFY] %s", message)

    def error(self, message: str) -> None:
        self.logger.error("[NOTIFY] %s", message)
