from __future__ import annotations

import asyncio
import logging
import re
import time
from dataclasses import dataclass
from datetime import timezone
from typing import Awaitable, Callable

import requests
try:
    from bs4 import BeautifulSoup
except Exception:  # noqa: BLE001
    BeautifulSoup = None  # type: ignore[assignment]

from trader.config import ListenerConfig
from trader.models import TelegramEvent, utc_now

PHOTO_URL_RE = re.compile(r"url\((['\"]?)(?P<url>[^)'\"]+)\1\)")
POST_OPEN_RE = re.compile(
    r"<div[^>]*class=\"tgme_widget_message\"[^>]*data-post=\"(?P<post>[^\"]+)\"[^>]*>",
    re.IGNORECASE,
)
TEXT_RE = re.compile(
    r"<div[^>]*class=\"[^\"]*tgme_widget_message_text[^\"]*\"[^>]*>(?P<text>.*?)</div>",
    re.DOTALL | re.IGNORECASE,
)
PHOTO_RE = re.compile(
    r"class=\"[^\"]*tgme_widget_message_photo_wrap[^\"]*\"[^>]*style=\"(?P<style>[^\"]+)\"",
    re.IGNORECASE,
)
TAG_RE = re.compile(r"<[^>]+>")


@dataclass
class WebPreviewPost:
    message_id: int
    text: str
    image_url: str | None


def parse_posts_from_html(html: str) -> list[WebPreviewPost]:
    posts: list[WebPreviewPost] = []
    if BeautifulSoup is not None:
        soup = BeautifulSoup(html, "html.parser")
        for node in soup.select("div.tgme_widget_message[data-post]"):
            data_post = str(node.get("data-post", ""))
            if "/" not in data_post:
                continue
            _, raw_id = data_post.rsplit("/", 1)
            if not raw_id.isdigit():
                continue
            message_id = int(raw_id)

            text_node = node.select_one(".tgme_widget_message_text")
            text = text_node.get_text("\n", strip=True) if text_node else ""

            image_url: str | None = None
            photo_node = node.select_one(".tgme_widget_message_photo_wrap")
            if photo_node is not None:
                style = str(photo_node.get("style", ""))
                match = PHOTO_URL_RE.search(style)
                if match:
                    image_url = match.group("url").strip()
            if image_url is None:
                img_node = node.select_one("img")
                if img_node is not None:
                    image_url = str(img_node.get("src") or "").strip() or None

            posts.append(WebPreviewPost(message_id=message_id, text=text, image_url=image_url))
    else:
        matches = list(POST_OPEN_RE.finditer(html))
        for idx, match in enumerate(matches):
            data_post = match.group("post")
            start = match.end()
            end = matches[idx + 1].start() if idx + 1 < len(matches) else len(html)
            body = html[start:end]
            if "/" not in data_post:
                continue
            _, raw_id = data_post.rsplit("/", 1)
            if not raw_id.isdigit():
                continue
            message_id = int(raw_id)

            text = ""
            text_match = TEXT_RE.search(body)
            if text_match:
                raw_text = TAG_RE.sub("", text_match.group("text"))
                text = raw_text.replace("\\n", "\n").strip()

            image_url = None
            photo_match = PHOTO_RE.search(body)
            if photo_match:
                style = photo_match.group("style")
                photo_url_match = PHOTO_URL_RE.search(style)
                if photo_url_match:
                    image_url = photo_url_match.group("url").strip()

            posts.append(WebPreviewPost(message_id=message_id, text=text, image_url=image_url))

    posts.sort(key=lambda item: item.message_id)
    return posts


class WebPreviewListener:
    def __init__(self, config: ListenerConfig, logger: logging.Logger) -> None:
        self.config = config
        self.logger = logger
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": self.config.user_agent})
        self._last_seen_message_id: int | None = None

    async def run(self, on_event: Callable[[TelegramEvent], Awaitable[None]]) -> None:
        while True:
            try:
                posts = await asyncio.to_thread(self._fetch_posts)
                if posts:
                    await self._emit_new_posts(posts, on_event)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                self.logger.warning("WebPreviewListener poll failed: %s", exc)
            await asyncio.sleep(self.config.polling_seconds)

    async def _emit_new_posts(
        self,
        posts: list[WebPreviewPost],
        on_event: Callable[[TelegramEvent], Awaitable[None]],
    ) -> None:
        if self._last_seen_message_id is None:
            latest = posts[-1]
            self._last_seen_message_id = latest.message_id
            await on_event(self._to_event(latest))
            return

        for post in posts:
            if post.message_id <= self._last_seen_message_id:
                continue
            await on_event(self._to_event(post))
            self._last_seen_message_id = post.message_id

    def _to_event(self, post: WebPreviewPost) -> TelegramEvent:
        return TelegramEvent(
            chat_id=self.config.web_chat_id,
            message_id=post.message_id,
            text=post.text,
            is_edit=False,
            date=utc_now().astimezone(timezone.utc),
            image_url=post.image_url,
            source="web_preview",
        )

    def _fetch_posts(self) -> list[WebPreviewPost]:
        html = self._fetch_page()
        return parse_posts_from_html(html)

    def _fetch_page(self) -> str:
        backoff = self.config.backoff_seconds
        last_error: Exception | None = None
        for attempt in range(self.config.max_retries + 1):
            try:
                response = self.session.get(self.config.target_url, timeout=self.config.request_timeout_seconds)
                response.raise_for_status()
                return response.text
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                if attempt >= self.config.max_retries:
                    break
                sleep_seconds = backoff * (2**attempt)
                time.sleep(sleep_seconds)
        raise RuntimeError(f"failed to fetch {self.config.target_url}: {last_error}")
