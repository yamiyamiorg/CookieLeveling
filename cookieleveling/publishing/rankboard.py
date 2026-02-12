from __future__ import annotations

import io
import logging
import time

import aiohttp
import discord
from PIL import Image

from cookieleveling.config.config import Config
from cookieleveling.db import (
    fetch_guild_settings,
    fetch_member_caches,
    upsert_guild_settings,
)
from cookieleveling.rendering.name_tokens import tokenize_display_name, truncate_tokens
from cookieleveling.rendering.emoji import resolve_emoji_tokens
from cookieleveling.rendering.rankboard import RenderedRankboard, render_rankboard
from cookieleveling.domain.ranking import (
    compute_lifetime_top20,
    compute_top20,
    compute_weekly_top20,
)
from cookieleveling.domain.xp import progress_for_xp
from cookieleveling.services.member_sync import refresh_member_cache

_LOGGER = logging.getLogger(__name__)
_AVATAR_CACHE_TTL_SECONDS = 3600
_AVATAR_CACHE_MAX_SIZE = 256
_AVATAR_CACHE: dict[str, tuple[float, Image.Image]] = {}
_RANK_CANDIDATE_LIMIT = 80


async def update_rankboard(bot: discord.Client, config: Config) -> bool:
    settings = fetch_guild_settings(config.guild_id)
    if settings is None:
        _LOGGER.warning("rankboard not configured")
        return False

    season_channel_id = settings["season_channel_id"]
    season_message_id = settings["season_message_id"]
    lifetime_channel_id = settings["lifetime_channel_id"]
    lifetime_message_id = settings["lifetime_message_id"]
    weekly_channel_id = settings["weekly_channel_id"]
    weekly_message_id = settings["weekly_message_id"]
    if (
        not weekly_channel_id
        or not weekly_message_id
        or not season_channel_id
        or not season_message_id
        or not lifetime_channel_id
        or not lifetime_message_id
    ):
        _LOGGER.warning("rankboard channel/message missing")
        return False

    weekly_channel = bot.get_channel(weekly_channel_id)
    if weekly_channel is None or not isinstance(weekly_channel, discord.TextChannel):
        _LOGGER.warning("rankboard channel not found: %s", weekly_channel_id)
        return False

    season_channel = bot.get_channel(season_channel_id)
    if season_channel is None or not isinstance(season_channel, discord.TextChannel):
        _LOGGER.warning("rankboard channel not found: %s", season_channel_id)
        return False

    lifetime_channel = bot.get_channel(lifetime_channel_id)
    if lifetime_channel is None or not isinstance(lifetime_channel, discord.TextChannel):
        _LOGGER.warning("rankboard channel not found: %s", lifetime_channel_id)
        return False

    try:
        weekly_message = await weekly_channel.fetch_message(weekly_message_id)
    except discord.NotFound:
        _LOGGER.warning("rankboard message not found: %s", weekly_message_id)
        return False

    try:
        season_message = await season_channel.fetch_message(season_message_id)
    except discord.NotFound:
        _LOGGER.warning("rankboard message not found: %s", season_message_id)
        return False

    try:
        lifetime_message = await lifetime_channel.fetch_message(lifetime_message_id)
    except discord.NotFound:
        _LOGGER.warning("rankboard message not found: %s", lifetime_message_id)
        return False

    try:
        files = await _render_files(bot, config)
    except Exception:
        _LOGGER.exception("rankboard render failed")
        return False

    weekly_ok = False
    season_ok = False
    lifetime_ok = False
    try:
        try:
            await weekly_message.edit(
                content="",
                embeds=[],
                attachments=[files.weekly_file],
            )
            weekly_ok = True
            _LOGGER.info("rankboard weekly updated")
        except Exception:
            _LOGGER.exception("rankboard weekly update failed")

        try:
            await season_message.edit(
                content="",
                embeds=[],
                attachments=[files.season_file],
            )
            season_ok = True
            _LOGGER.info("rankboard season updated")
        except Exception:
            _LOGGER.exception("rankboard season update failed")

        try:
            await lifetime_message.edit(
                content="",
                embeds=[],
                attachments=[files.lifetime_file],
            )
            lifetime_ok = True
            _LOGGER.info("rankboard lifetime updated")
        except Exception:
            _LOGGER.exception("rankboard lifetime update failed")
    finally:
        files.cleanup()

    return weekly_ok and season_ok and lifetime_ok


async def send_rankboard_messages(
    bot: discord.Client, config: Config, channel: discord.TextChannel
) -> tuple[discord.Message, discord.Message, discord.Message]:
    weekly_message: discord.Message | None = None
    season_message: discord.Message | None = None
    lifetime_message: discord.Message | None = None
    try:
        files = await _render_files(bot, config)
        try:
            lifetime_message = await channel.send(
                content="",
                files=[files.lifetime_file],
            )
            season_message = await channel.send(
                content="",
                files=[files.season_file],
            )
            weekly_message = await channel.send(
                content="",
                files=[files.weekly_file],
            )
            return weekly_message, season_message, lifetime_message
        finally:
            files.cleanup()
    except Exception:
        _LOGGER.exception("rankboard send failed: channel_id=%s", channel.id)
        if weekly_message is not None:
            try:
                await weekly_message.delete()
            except Exception:
                _LOGGER.warning("failed to delete weekly rankboard message")
        if season_message is not None:
            try:
                await season_message.delete()
            except Exception:
                _LOGGER.warning("failed to delete season rankboard message")
        if lifetime_message is not None:
            try:
                await lifetime_message.delete()
            except Exception:
                _LOGGER.warning("failed to delete lifetime rankboard message")
        raise


async def set_rankboard(
    bot: discord.Client, config: Config, channel: discord.abc.GuildChannel
) -> tuple[bool, str]:
    target_channel = _normalize_rankboard_channel(channel)
    if target_channel is None:
        return False, "このチャンネルでは設置できません。"

    settings = fetch_guild_settings(config.guild_id)
    if settings:
        if settings["weekly_channel_id"] and settings["weekly_message_id"]:
            await _mark_rankboard_moved(
                bot,
                settings["weekly_channel_id"],
                settings["weekly_message_id"],
                target_channel.id,
            )
        if settings["season_channel_id"] and settings["season_message_id"]:
            await _mark_rankboard_moved(
                bot,
                settings["season_channel_id"],
                settings["season_message_id"],
                target_channel.id,
            )
        if settings["lifetime_channel_id"] and settings["lifetime_message_id"]:
            await _mark_rankboard_moved(
                bot,
                settings["lifetime_channel_id"],
                settings["lifetime_message_id"],
                target_channel.id,
            )

    try:
        weekly_message, season_message, lifetime_message = await send_rankboard_messages(
            bot, config, target_channel
        )
    except Exception:
        return False, "設置に失敗しました。"
    upsert_guild_settings(
        config.guild_id,
        weekly_channel_id=target_channel.id,
        weekly_message_id=weekly_message.id,
        season_channel_id=target_channel.id,
        season_message_id=season_message.id,
        lifetime_channel_id=target_channel.id,
        lifetime_message_id=lifetime_message.id,
    )
    return True, f"<#{target_channel.id}> に設置しました。"


async def _render_files(bot: discord.Client, config: Config) -> RenderedRankboard:
    guild = bot.get_guild(config.guild_id)
    if guild is None:
        raise RuntimeError("rankboardの描画に必要なGuildが見つかりません。")
    async with aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=10)
    ) as session:
        weekly_entries = await _prepare_weekly_entries(guild, session)
        season_entries = await _prepare_season_entries(guild, session)
        lifetime_entries = await _prepare_lifetime_entries(guild, session)
    return await render_rankboard(weekly_entries, season_entries, lifetime_entries)


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
        content=f"移動しました: <#{new_channel_id}>",
        embeds=[],
        attachments=[],
    )


async def _prepare_season_entries(
    guild: discord.Guild, session: aiohttp.ClientSession
) -> list[dict]:
    rows = compute_top20(guild.id, limit=_RANK_CANDIDATE_LIMIT)
    return await _build_rank_entries(
        guild,
        session,
        rows,
        xp_key="season_xp",
        label="season",
    )


async def _prepare_weekly_entries(
    guild: discord.Guild, session: aiohttp.ClientSession
) -> list[dict]:
    rows = compute_weekly_top20(guild.id, limit=_RANK_CANDIDATE_LIMIT)
    return await _build_rank_entries(
        guild,
        session,
        rows,
        xp_key="weekly_xp",
        label="weekly",
    )


async def _prepare_lifetime_entries(
    guild: discord.Guild, session: aiohttp.ClientSession
) -> list[dict]:
    rows = compute_lifetime_top20(guild.id, limit=_RANK_CANDIDATE_LIMIT)
    return await _build_rank_entries(
        guild,
        session,
        rows,
        xp_key="lifetime_xp",
        label="lifetime",
    )


async def _prepare_name_tokens(
    display_name: str,
    session: aiohttp.ClientSession,
) -> list:
    tokens = tokenize_display_name(display_name)
    tokens = truncate_tokens(tokens, max_chars=16)
    await resolve_emoji_tokens(tokens, session)
    return tokens


async def _fetch_avatar(
    avatar_url: str | None, session: aiohttp.ClientSession
) -> Image.Image | None:
    if not avatar_url:
        return None
    cached = _get_cached_avatar(avatar_url)
    if cached is not None:
        return cached
    try:
        async with session.get(avatar_url) as response:
            if response.status != 200:
                _LOGGER.warning("avatar download failed: status=%s", response.status)
                return None
            data = await response.read()
    except Exception:
        _LOGGER.exception("avatar download failed")
        return None
    try:
        image = Image.open(io.BytesIO(data)).convert("RGB")
    except Exception:
        _LOGGER.exception("avatar decode failed")
        return None
    _store_cached_avatar(avatar_url, image)
    return image.copy()


async def _build_rank_entries(
    guild: discord.Guild,
    session: aiohttp.ClientSession,
    rows: list[dict],
    *,
    xp_key: str,
    label: str,
) -> list[dict]:
    user_ids = [row["user_id"] for row in rows]
    await refresh_member_cache(guild, user_ids)
    caches = fetch_member_caches(guild.id, user_ids)

    filtered: list[tuple[dict, str, str]] = []
    for row in rows:
        cache = caches.get(row["user_id"])
        if cache is None:
            continue
        if cache["member_state"] != 1:
            continue
        display_name = (cache["display_name_cache"] or "").strip()
        avatar_url = (cache["avatar_url_cache"] or "").strip()
        if not display_name or not avatar_url:
            continue
        filtered.append((row, display_name, avatar_url))

    entries: list[dict] = []
    for row, display_name, avatar_url in filtered[:20]:
        name_tokens = await _prepare_name_tokens(display_name, session)
        level, _, _, progress = progress_for_xp(row[xp_key])
        entries.append(
            {
                "user_id": row["user_id"],
                "name": display_name,
                "name_tokens": name_tokens,
                xp_key: row[xp_key],
                "level": level,
                "xp_progress": progress,
                "avatar": await _fetch_avatar(avatar_url, session),
            }
        )

    _LOGGER.info(
        "rankboard %s entries: candidates=%s filtered=%s final=%s",
        label,
        len(rows),
        len(filtered),
        len(entries),
    )
    return entries


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
