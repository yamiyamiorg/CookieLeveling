from __future__ import annotations

import io
import logging
import time

import aiohttp
import discord
from PIL import Image

from .config import Config
from .db import fetch_guild_settings, upsert_guild_settings
from .display_name_tokens import tokenize_display_name, truncate_tokens
from .emoji_assets import resolve_emoji_tokens
from .rankboard_renderer import RenderedRankboard, render_rankboard
from .ranker import compute_lifetime_top10, compute_top10
from .xp_engine import progress_for_xp

_LOGGER = logging.getLogger(__name__)
_AVATAR_CACHE_TTL_SECONDS = 3600
_AVATAR_CACHE_MAX_SIZE = 256
_AVATAR_CACHE: dict[str, tuple[float, Image.Image]] = {}


async def update_rankboard(bot: discord.Client, config: Config) -> bool:
    settings = fetch_guild_settings(config.guild_id)
    if settings is None:
        _LOGGER.warning("rankboard not configured")
        return False

    channel_id = settings["rankboard_channel_id"]
    message_id = settings["rankboard_message_id"]
    if not channel_id or not message_id:
        _LOGGER.warning("rankboard channel/message missing")
        return False

    channel = bot.get_channel(channel_id)
    if channel is None or not isinstance(channel, discord.TextChannel):
        _LOGGER.warning("rankboard channel not found: %s", channel_id)
        return False

    try:
        message = await channel.fetch_message(message_id)
    except discord.NotFound:
        _LOGGER.warning("rankboard message not found: %s", message_id)
        return False

    try:
        files = await _render_files(bot, config)
        try:
            await message.edit(content="", embeds=[], attachments=files.files)
        finally:
            files.cleanup()
    except Exception:
        _LOGGER.exception("rankboard update failed: channel_id=%s", channel_id)
        return False
    return True


async def send_rankboard_message(
    bot: discord.Client, config: Config, channel: discord.TextChannel
) -> discord.Message:
    try:
        files = await _render_files(bot, config)
        try:
            return await channel.send(content="", files=files.files)
        finally:
            files.cleanup()
    except Exception:
        _LOGGER.exception("rankboard send failed: channel_id=%s", channel.id)
        raise


async def set_rankboard(
    bot: discord.Client, config: Config, channel: discord.abc.GuildChannel
) -> tuple[bool, str]:
    target_channel = _normalize_rankboard_channel(channel)
    if target_channel is None:
        return False, "このチャンネルでは設置できません。"

    settings = fetch_guild_settings(config.guild_id)
    if settings and settings["rankboard_message_id"]:
        await _mark_rankboard_moved(
            bot,
            settings["rankboard_channel_id"],
            settings["rankboard_message_id"],
            target_channel.id,
        )

    try:
        message = await send_rankboard_message(bot, config, target_channel)
    except Exception:
        return False, "設置に失敗しました。"
    upsert_guild_settings(config.guild_id, target_channel.id, message.id)
    return True, f"<#{target_channel.id}> に設置しました。"


async def _render_files(bot: discord.Client, config: Config) -> RenderedRankboard:
    guild = bot.get_guild(config.guild_id)
    if guild is None:
        raise RuntimeError("rankboardの描画に必要なGuildが見つかりません。")
    async with aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=10)
    ) as session:
        season_entries = await _prepare_season_entries(guild, session)
        lifetime_entries = await _prepare_lifetime_entries(guild, session)
    return await render_rankboard(season_entries, lifetime_entries)


def _normalize_rankboard_channel(
    channel: discord.abc.GuildChannel,
) -> discord.TextChannel | None:
    if isinstance(channel, discord.Thread):
        channel = channel.parent
    if isinstance(channel, discord.TextChannel):
        return channel
    return None


async def _mark_rankboard_moved(
    bot: discord.Client, channel_id: int, message_id: int, new_channel_id: int
) -> None:
    channel = bot.get_channel(channel_id)
    if channel is None or not isinstance(channel, discord.TextChannel):
        return
    try:
        message = await channel.fetch_message(message_id)
    except discord.NotFound:
        return
    await message.edit(
        content=f"移設されました: <#{new_channel_id}>",
        embeds=[],
        attachments=[],
    )


async def _prepare_season_entries(
    guild: discord.Guild, session: aiohttp.ClientSession
) -> list[dict]:
    entries: list[dict] = []
    for row in compute_top10(guild.id):
        user_id = row["user_id"]
        member = await _resolve_member(guild, user_id)
        name_tokens = await _prepare_name_tokens(member, user_id, session)
        level, _, _, progress = progress_for_xp(row["season_xp"])
        entries.append(
            {
                "user_id": user_id,
                "name": member.display_name if member else None,
                "name_tokens": name_tokens,
                "season_xp": row["season_xp"],
                "level": level,
                "xp_progress": progress,
                "avatar": await _fetch_avatar(member, session),
            }
        )
    return entries


async def _prepare_lifetime_entries(
    guild: discord.Guild, session: aiohttp.ClientSession
) -> list[dict]:
    entries: list[dict] = []
    for row in compute_lifetime_top10(guild.id):
        user_id = row["user_id"]
        member = await _resolve_member(guild, user_id)
        name_tokens = await _prepare_name_tokens(member, user_id, session)
        level, _, _, progress = progress_for_xp(row["lifetime_xp"])
        entries.append(
            {
                "user_id": user_id,
                "name": member.display_name if member else None,
                "name_tokens": name_tokens,
                "level": level,
                "lifetime_xp": row["lifetime_xp"],
                "xp_progress": progress,
                "avatar": await _fetch_avatar(member, session),
            }
        )
    return entries


async def _resolve_member(
    guild: discord.Guild, user_id: int
) -> discord.Member | None:
    member = guild.get_member(user_id)
    if member is not None:
        return member
    try:
        return await guild.fetch_member(user_id)
    except Exception:
        _LOGGER.exception("member fetch failed: user_id=%s", user_id)
        return None


async def _prepare_name_tokens(
    member: discord.Member | None,
    user_id: int,
    session: aiohttp.ClientSession,
) -> list:
    name = member.display_name if member else str(user_id)
    tokens = tokenize_display_name(name)
    tokens = truncate_tokens(tokens, max_chars=16)
    await resolve_emoji_tokens(tokens, session)
    return tokens


async def _fetch_avatar(
    member: discord.Member | None, session: aiohttp.ClientSession
) -> Image.Image | None:
    if member is None:
        return None
    url = str(member.display_avatar.url)
    cached = _get_cached_avatar(url)
    if cached is not None:
        return cached
    try:
        async with session.get(url) as response:
            if response.status != 200:
                _LOGGER.warning(
                    "avatar download failed: user_id=%s status=%s",
                    member.id,
                    response.status,
                )
                return None
            data = await response.read()
    except Exception:
        _LOGGER.exception("avatar download failed: user_id=%s", member.id)
        return None
    try:
        image = Image.open(io.BytesIO(data)).convert("RGB")
    except Exception:
        _LOGGER.exception("avatar decode failed: user_id=%s", member.id)
        return None
    _store_cached_avatar(url, image)
    return image.copy()


def _get_cached_avatar(url: str) -> Image.Image | None:
    now = time.monotonic()
    cached = _AVATAR_CACHE.get(url)
    if cached is None:
        return None
    cached_at, image = cached
    if now - cached_at > _AVATAR_CACHE_TTL_SECONDS:
        _AVATAR_CACHE.pop(url, None)
        return None
    return image.copy()


def _store_cached_avatar(url: str, image: Image.Image) -> None:
    now = time.monotonic()
    _prune_avatar_cache(now)
    if len(_AVATAR_CACHE) >= _AVATAR_CACHE_MAX_SIZE:
        oldest_url = min(_AVATAR_CACHE.items(), key=lambda item: item[1][0])[0]
        _AVATAR_CACHE.pop(oldest_url, None)
    _AVATAR_CACHE[url] = (now, image)


def _prune_avatar_cache(now: float) -> None:
    expired = [
        url
        for url, (cached_at, _) in _AVATAR_CACHE.items()
        if now - cached_at > _AVATAR_CACHE_TTL_SECONDS
    ]
    for url in expired:
        _AVATAR_CACHE.pop(url, None)
