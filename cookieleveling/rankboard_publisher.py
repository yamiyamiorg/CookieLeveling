from __future__ import annotations

import logging

import discord

from .config import Config
from .db import fetch_guild_settings, upsert_guild_settings
from .rankboard_renderer import RenderedRankboard, render_rankboard

_LOGGER = logging.getLogger(__name__)


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

    files = await _render_files(bot, config)
    embeds = _build_embeds()
    try:
        await message.edit(embeds=embeds, attachments=files.files)
    finally:
        files.cleanup()
    return True


async def send_rankboard_message(
    bot: discord.Client, config: Config, channel: discord.TextChannel
) -> discord.Message:
    files = await _render_files(bot, config)
    embeds = _build_embeds()
    try:
        return await channel.send(embeds=embeds, files=files.files)
    finally:
        files.cleanup()


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

    message = await send_rankboard_message(bot, config, target_channel)
    upsert_guild_settings(config.guild_id, target_channel.id, message.id)
    return True, f"<#{target_channel.id}> に設置しました。"


def _build_embeds() -> list[discord.Embed]:
    season_embed = discord.Embed(title="Season Ranking", color=0xFFC0CB)
    season_embed.set_image(url="attachment://season.png")
    lifetime_embed = discord.Embed(title="Lifetime Levels", color=0x8C8C8C)
    lifetime_embed.set_image(url="attachment://lifetime.png")
    return [season_embed, lifetime_embed]


async def _render_files(bot: discord.Client, config: Config) -> RenderedRankboard:
    guild = bot.get_guild(config.guild_id)
    if guild is None:
        raise RuntimeError("rankboardの描画に必要なGuildが見つかりません。")
    return await render_rankboard(guild)


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
