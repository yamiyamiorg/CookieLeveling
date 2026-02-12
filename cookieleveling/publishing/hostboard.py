from __future__ import annotations

import io
import logging
import time

import aiohttp
import discord
from PIL import Image

from cookieleveling.config.config import Config
from cookieleveling.db import (
    fetch_hostboard_settings,
    fetch_member_caches,
    upsert_hostboard_settings,
)
from cookieleveling.rendering.name_tokens import tokenize_display_name, truncate_tokens
from cookieleveling.rendering.emoji import resolve_emoji_tokens
from cookieleveling.rendering.host_rankboard import (
    RenderedHostRankboard,
    render_host_rankboard,
)
from cookieleveling.domain.host_ranking import (
    compute_host_top20_monthly,
    compute_host_top20_total,
    compute_host_top20_weekly,
)
from cookieleveling.domain.xp import progress_for_xp
from cookieleveling.services.member_sync import refresh_member_cache

_LOGGER = logging.getLogger(__name__)
_AVATAR_CACHE_TTL_SECONDS = 3600
_AVATAR_CACHE_MAX_SIZE = 256
_AVATAR_CACHE: dict[str, tuple[float, Image.Image]] = {}
_RANK_CANDIDATE_LIMIT = 80


async def update_hostboard(bot: discord.Client, config: Config) -> bool:
    settings = fetch_hostboard_settings(config.guild_id)
    if settings is None:
        _LOGGER.warning("hostboard not configured")
        return False

    monthly_channel_id = settings["host_monthly_channel_id"]
    monthly_message_id = settings["host_monthly_message_id"]
    total_channel_id = settings["host_total_channel_id"]
    total_message_id = settings["host_total_message_id"]
    weekly_channel_id = settings["host_weekly_channel_id"]
    weekly_message_id = settings["host_weekly_message_id"]
    if (
        not total_channel_id
        or not total_message_id
        or not monthly_channel_id
        or not monthly_message_id
        or not weekly_channel_id
        or not weekly_message_id
    ):
        _LOGGER.warning("hostboard channel/message missing")
        return False

    weekly_channel = bot.get_channel(weekly_channel_id)
    if weekly_channel is None or not isinstance(weekly_channel, discord.TextChannel):
        _LOGGER.warning("hostboard channel not found: %s", weekly_channel_id)
        return False

    monthly_channel = bot.get_channel(monthly_channel_id)
    if monthly_channel is None or not isinstance(monthly_channel, discord.TextChannel):
        _LOGGER.warning("hostboard channel not found: %s", monthly_channel_id)
        return False

    total_channel = bot.get_channel(total_channel_id)
    if total_channel is None or not isinstance(total_channel, discord.TextChannel):
        _LOGGER.warning("hostboard channel not found: %s", total_channel_id)
        return False

    try:
        weekly_message = await weekly_channel.fetch_message(weekly_message_id)
    except discord.NotFound:
        _LOGGER.warning("hostboard message not found: %s", weekly_message_id)
        return False

    try:
        monthly_message = await monthly_channel.fetch_message(monthly_message_id)
    except discord.NotFound:
        _LOGGER.warning("hostboard message not found: %s", monthly_message_id)
        return False

    try:
        total_message = await total_channel.fetch_message(total_message_id)
    except discord.NotFound:
        _LOGGER.warning("hostboard message not found: %s", total_message_id)
        return False

    try:
        files = await _render_files(bot, config)
    except Exception:
        _LOGGER.exception("hostboard render failed")
        return False

    weekly_ok = False
    monthly_ok = False
    total_ok = False
    try:
        try:
            await weekly_message.edit(
                content="",
                embeds=[],
                attachments=[files.weekly_file],
            )
            weekly_ok = True
            _LOGGER.info("hostboard weekly updated")
        except Exception:
            _LOGGER.exception("hostboard weekly update failed")

        try:
            await monthly_message.edit(
                content="",
                embeds=[],
                attachments=[files.monthly_file],
            )
            monthly_ok = True
            _LOGGER.info("hostboard monthly updated")
        except Exception:
            _LOGGER.exception("hostboard monthly update failed")

        try:
            await total_message.edit(
                content="",
                embeds=[],
                attachments=[files.total_file],
            )
            total_ok = True
            _LOGGER.info("hostboard total updated")
        except Exception:
            _LOGGER.exception("hostboard total update failed")
    finally:
        files.cleanup()

    return weekly_ok and monthly_ok and total_ok


async def send_hostboard_messages(
    bot: discord.Client, config: Config, channel: discord.TextChannel
) -> tuple[discord.Message, discord.Message, discord.Message]:
    total_message: discord.Message | None = None
    monthly_message: discord.Message | None = None
    weekly_message: discord.Message | None = None
    try:
        files = await _render_files(bot, config)
        try:
            total_message = await channel.send(content="", files=[files.total_file])
            monthly_message = await channel.send(
                content="", files=[files.monthly_file]
            )
            weekly_message = await channel.send(content="", files=[files.weekly_file])
            return weekly_message, monthly_message, total_message
        finally:
            files.cleanup()
    except Exception:
        _LOGGER.exception("hostboard send failed: channel_id=%s", channel.id)
        if weekly_message is not None:
            try:
                await weekly_message.delete()
            except Exception:
                _LOGGER.warning("failed to delete hostboard weekly message")
        if monthly_message is not None:
            try:
                await monthly_message.delete()
            except Exception:
                _LOGGER.warning("failed to delete hostboard monthly message")
        if total_message is not None:
            try:
                await total_message.delete()
            except Exception:
                _LOGGER.warning("failed to delete hostboard total message")
        raise


async def set_hostboard(
    bot: discord.Client, config: Config, channel: discord.abc.GuildChannel
) -> tuple[bool, str]:
    target_channel = _normalize_channel(channel)
    if target_channel is None:
        return False, "このチャンネルでは設置できません。"

    settings = fetch_hostboard_settings(config.guild_id)
    if settings:
        if settings["host_monthly_channel_id"] and settings["host_monthly_message_id"]:
            await _mark_moved(
                bot,
                settings["host_monthly_channel_id"],
                settings["host_monthly_message_id"],
                target_channel.id,
            )
        if settings["host_total_channel_id"] and settings["host_total_message_id"]:
            await _mark_moved(
                bot,
                settings["host_total_channel_id"],
                settings["host_total_message_id"],
                target_channel.id,
            )
        if settings["host_weekly_channel_id"] and settings["host_weekly_message_id"]:
            await _mark_moved(
                bot,
                settings["host_weekly_channel_id"],
                settings["host_weekly_message_id"],
                target_channel.id,
            )

    try:
        weekly_message, monthly_message, total_message = await send_hostboard_messages(
            bot, config, target_channel
        )
    except Exception:
        return False, "設置に失敗しました。"

    upsert_hostboard_settings(
        config.guild_id,
        host_monthly_channel_id=target_channel.id,
        host_monthly_message_id=monthly_message.id,
        host_total_channel_id=target_channel.id,
        host_total_message_id=total_message.id,
        host_weekly_channel_id=target_channel.id,
        host_weekly_message_id=weekly_message.id,
    )
    return True, f"<#{target_channel.id}> に設置しました。"


async def _render_files(
    bot: discord.Client, config: Config
) -> RenderedHostRankboard:
    guild = bot.get_guild(config.guild_id)
    if guild is None:
        raise RuntimeError("hostboardの描画に必要なGuildが見つかりません。")
    async with aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=10)
    ) as session:
        weekly_entries = await _prepare_weekly_entries(guild, session)
        monthly_entries = await _prepare_monthly_entries(guild, session)
        total_entries = await _prepare_total_entries(guild, session)
    return await render_host_rankboard(weekly_entries, monthly_entries, total_entries)


def _normalize_channel(
    channel: discord.abc.GuildChannel,
) -> discord.TextChannel | None:
    if isinstance(channel, discord.Thread):
        channel = channel.parent
    if isinstance(channel, discord.TextChannel):
        return channel
    return None


async def _mark_moved(
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


async def _prepare_monthly_entries(
    guild: discord.Guild, session: aiohttp.ClientSession
) -> list[dict]:
    rows = compute_host_top20_monthly(guild.id, limit=_RANK_CANDIDATE_LIMIT)
    return await _build_host_entries(
        guild,
        session,
        rows,
        xp_key="monthly_xp",
        label="monthly",
    )


async def _prepare_total_entries(
    guild: discord.Guild, session: aiohttp.ClientSession
) -> list[dict]:
    rows = compute_host_top20_total(guild.id, limit=_RANK_CANDIDATE_LIMIT)
    return await _build_host_entries(
        guild,
        session,
        rows,
        xp_key="total_xp",
        label="total",
    )


async def _prepare_weekly_entries(
    guild: discord.Guild, session: aiohttp.ClientSession
) -> list[dict]:
    rows = compute_host_top20_weekly(guild.id, limit=_RANK_CANDIDATE_LIMIT)
    return await _build_host_entries(
        guild,
        session,
        rows,
        xp_key="weekly_xp",
        label="weekly",
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


async def _build_host_entries(
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
                "level": level,
                xp_key: row[xp_key],
                "xp_progress": progress,
                "avatar": await _fetch_avatar(avatar_url, session),
            }
        )

    _LOGGER.info(
        "hostboard %s entries: candidates=%s filtered=%s final=%s",
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
