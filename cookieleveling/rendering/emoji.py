from __future__ import annotations

import io
import logging
import time
from collections import OrderedDict

import aiohttp
from PIL import Image

from cookieleveling.rendering.name_tokens import NameToken

_LOGGER = logging.getLogger(__name__)
_EMOJI_CACHE_TTL_SECONDS = 3600
_EMOJI_CACHE_MAX_SIZE = 512
_TWEMOJI_BASE_URL = (
    "https://cdn.jsdelivr.net/gh/twitter/twemoji@14.0.2/assets/72x72"
)


class EmojiImageCache:
    def __init__(
        self,
        max_size: int = _EMOJI_CACHE_MAX_SIZE,
        ttl: int = _EMOJI_CACHE_TTL_SECONDS,
    ) -> None:
        self._max_size = max_size
        self._ttl = ttl
        self._cache: OrderedDict[str, tuple[float, Image.Image]] = OrderedDict()

    def get(self, key: str) -> Image.Image | None:
        now = time.monotonic()
        cached = self._cache.get(key)
        if cached is None:
            return None
        cached_at, image = cached
        if now - cached_at > self._ttl:
            self._cache.pop(key, None)
            return None
        self._cache.move_to_end(key)
        return image.copy()

    def set(self, key: str, image: Image.Image) -> None:
        now = time.monotonic()
        self._prune(now)
        if key in self._cache:
            self._cache.move_to_end(key)
        self._cache[key] = (now, image)
        while len(self._cache) > self._max_size:
            self._cache.popitem(last=False)

    def _prune(self, now: float) -> None:
        expired = [
            key
            for key, (cached_at, _) in self._cache.items()
            if now - cached_at > self._ttl
        ]
        for key in expired:
            self._cache.pop(key, None)


_EMOJI_CACHE = EmojiImageCache()


async def resolve_emoji_tokens(
    tokens: list[NameToken],
    session: aiohttp.ClientSession,
) -> list[NameToken]:
    for token in tokens:
        if token.kind == "unicode_emoji" and token.emoji:
            token.image = await _fetch_image(
                _unicode_emoji_url(token.emoji), session
            )
        elif token.kind == "custom_emoji" and token.emoji_id:
            token.image = await _fetch_image(
                _custom_emoji_url(token.emoji_id, token.animated), session
            )
    return tokens


def _unicode_emoji_url(emoji: str) -> str:
    codepoints = "-".join(f"{ord(ch):x}" for ch in emoji)
    return f"{_TWEMOJI_BASE_URL}/{codepoints}.png"


def _custom_emoji_url(emoji_id: str, animated: bool) -> str:
    extension = "gif" if animated else "png"
    return f"https://cdn.discordapp.com/emojis/{emoji_id}.{extension}"


async def _fetch_image(url: str, session: aiohttp.ClientSession) -> Image.Image | None:
    cached = _EMOJI_CACHE.get(url)
    if cached is not None:
        return cached
    try:
        async with session.get(url) as response:
            if response.status != 200:
                _LOGGER.warning(
                    "emoji download failed: url=%s status=%s", url, response.status
                )
                return None
            data = await response.read()
    except Exception:
        _LOGGER.exception("emoji download failed: url=%s", url)
        return None
    try:
        image = Image.open(io.BytesIO(data))
        if image.format == "GIF":
            try:
                image.seek(0)
            except EOFError:
                pass
        image = image.convert("RGBA")
    except Exception:
        _LOGGER.exception("emoji decode failed: url=%s", url)
        return None
    _EMOJI_CACHE.set(url, image)
    return image.copy()
