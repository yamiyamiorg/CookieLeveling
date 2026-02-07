from __future__ import annotations

import io
import logging
import time

import aiohttp
import discord
from PIL import Image

from .config import Config
from .db import fetch_hostboard_settings, upsert_hostboard_settings
from .display_name_tokens import tokenize_display_name, truncate_tokens
from .emoji_assets import resolve_emoji_tokens
from .host_rankboard_renderer import RenderedHostRankboard, render_host_rankboard
from .host_ranker import compute_host_top20_monthly, compute_host_top20_total
from .xp_engine import progress_for_xp

_LOGGER = logging.getLogger(__name__)
_AVATAR_CACHE_TTL_SECONDS = 3600
_AVATAR_CACHE_MAX_SIZE = 256
_AVATAR_CACHE: dict[str, tuple[float, Image.Image]] = {}


async def update_hostboard(bot: discord.Client, config: Config) -> bool:
    settings = fetch_hostboard_settings(config.guild_id)
    if settings is None:
        _LOGGER.warning("hostboard not configured")
        return False

    monthly_channel_id = settings["host_monthly_channel_id"]
    monthly_message_id = settings["host_monthly_message_id"]
    total_channel_id = settings["host_total_channel_id"]
    total_message_id = settings["host_total_message_id"]
    if (
        not monthly_channel_id
        or not monthly_message_id
        or not total_channel_id
        or not total_message_id
    ):
        _LOGGER.warning("hostboard channel/message missing")
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

    monthly_ok = False
    total_ok = False
    try:
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

    return monthly_ok and total_ok


async def send_hostboard_messages(
    bot: discord.Client, config: Config, channel: discord.TextChannel
) -> tuple[discord.Message, discord.Message]:
    total_message: discord.Message | None = None
    monthly_message: discord.Message | None = None
    try:
        files = await _render_files(bot, config)
        try:
            total_message = await channel.send(content="", files=[files.total_file])
            monthly_message = await channel.send(
                content="", files=[files.monthly_file]
            )
            return monthly_message, total_message
        finally:
            files.cleanup()
    except Exception:
        _LOGGER.exception("hostboard send failed: channel_id=%s", channel.id)
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

    try:
        monthly_message, total_message = await send_hostboard_messages(
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
        monthly_entries = await _prepare_monthly_entries(guild, session)
        total_entries = await _prepare_total_entries(guild, session)
    return await render_host_rankboard(monthly_entries, total_entries)


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
    entries: list[dict] = []
    for row in compute_host_top20_monthly(guild.id):
        user_id = row["user_id"]
        member = await _resolve_member(guild, user_id)
        name_tokens = await _prepare_name_tokens(member, user_id, session)
        level, _, _, progress = progress_for_xp(row["monthly_xp"])
        entries.append(
            {
                "user_id": user_id,
                "name": member.display_name if member else None,
                "name_tokens": name_tokens,
                "level": level,
                "monthly_xp": row["monthly_xp"],
                "xp_progress": progress,
                "avatar": await _fetch_avatar(member, session),
            }
        )
    return entries


async def _prepare_total_entries(
    guild: discord.Guild, session: aiohttp.ClientSession
) -> list[dict]:
    entries: list[dict] = []
    for row in compute_host_top20_total(guild.id):
        user_id = row["user_id"]
        member = await _resolve_member(guild, user_id)
        name_tokens = await _prepare_name_tokens(member, user_id, session)
        level, _, _, progress = progress_for_xp(row["total_xp"])
        entries.append(
            {
                "user_id": user_id,
                "name": member.display_name if member else None,
                "name_tokens": name_tokens,
                "level": level,
                "total_xp": row["total_xp"],
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
