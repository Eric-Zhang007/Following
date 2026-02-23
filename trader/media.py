from __future__ import annotations

import hashlib
import logging
import mimetypes
import time
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

import requests

from trader.store import SQLiteStore


@dataclass
class MediaDownloadResult:
    source_url: str
    sha256: str
    local_path: str
    mime_type: str | None
    size_bytes: int
    duplicate: bool
    image_bytes: bytes


class MediaManager:
    def __init__(
        self,
        media_dir: str,
        store: SQLiteStore,
        logger: logging.Logger,
        timeout_seconds: int = 20,
        max_retries: int = 2,
        backoff_seconds: float = 1.0,
    ) -> None:
        self.media_dir = Path(media_dir)
        self.media_dir.mkdir(parents=True, exist_ok=True)
        self.store = store
        self.logger = logger
        self.timeout_seconds = timeout_seconds
        self.max_retries = max_retries
        self.backoff_seconds = backoff_seconds
        self.session = requests.Session()

    def download_and_store(self, image_url: str) -> MediaDownloadResult:
        image_bytes, mime_type = self._download_image(image_url)
        sha256 = hashlib.sha256(image_bytes).hexdigest()

        existing = self.store.get_media_by_sha256(sha256)
        if existing is not None:
            return MediaDownloadResult(
                source_url=image_url,
                sha256=sha256,
                local_path=existing["local_path"],
                mime_type=existing.get("mime_type"),
                size_bytes=int(existing.get("size_bytes") or len(image_bytes)),
                duplicate=True,
                image_bytes=image_bytes,
            )

        ext = self._pick_extension(image_url, mime_type)
        folder = self.media_dir / sha256[:2]
        folder.mkdir(parents=True, exist_ok=True)
        local_path = folder / f"{sha256}{ext}"
        local_path.write_bytes(image_bytes)

        self.store.save_media_asset(
            sha256=sha256,
            source_url=image_url,
            local_path=str(local_path),
            mime_type=mime_type,
            size_bytes=len(image_bytes),
        )
        return MediaDownloadResult(
            source_url=image_url,
            sha256=sha256,
            local_path=str(local_path),
            mime_type=mime_type,
            size_bytes=len(image_bytes),
            duplicate=False,
            image_bytes=image_bytes,
        )

    def _download_image(self, image_url: str) -> tuple[bytes, str | None]:
        last_error: Exception | None = None
        for attempt in range(self.max_retries + 1):
            try:
                response = self.session.get(image_url, timeout=self.timeout_seconds)
                response.raise_for_status()
                content = response.content
                if not content:
                    raise RuntimeError("image body is empty")
                return content, response.headers.get("Content-Type")
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                if attempt >= self.max_retries:
                    break
                time.sleep(self.backoff_seconds * (2**attempt))
        raise RuntimeError(f"image download failed: {image_url} error={last_error}")

    @staticmethod
    def _pick_extension(image_url: str, mime_type: str | None) -> str:
        if mime_type:
            guessed = mimetypes.guess_extension(mime_type.split(";")[0].strip())
            if guessed:
                return guessed

        path = urlparse(image_url).path
        suffix = Path(path).suffix
        if suffix and len(suffix) <= 8:
            return suffix
        return ".jpg"
