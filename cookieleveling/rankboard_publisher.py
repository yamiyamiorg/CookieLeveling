from __future__ import annotations

import io
import logging
import os
from typing import Optional

import discord
from PIL import Image

from .config import Config
from .db import fetch_guild_settings
from .image_renderer import render_lifetime_image, render_season_image
from .ranker import compute_lifetime_top10, compute_top10
from .xp_engine import level_from_xp

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
        await message.edit(embeds=embeds, attachments=[], files=files)
    finally:
        _cleanup_files(files)
    return True


async def send_rankboard_message(
    bot: discord.Client, config: Config, channel: discord.TextChannel
) -> discord.Message:
    files = await _render_files(bot, config)
    embeds = _build_embeds()
    try:
        return await channel.send(embeds=embeds, files=files)
    finally:
        _cleanup_files(files)


def _build_embeds() -> list[discord.Embed]:
    season_embed = discord.Embed(title="Season Ranking", color=0xFFC0CB)
    season_embed.set_image(url="attachment://season.png")
    lifetime_embed = discord.Embed(title="Lifetime Levels", color=0x8C8C8C)
    lifetime_embed.set_image(url="attachment://lifetime.png")
    return [season_embed, lifetime_embed]


async def _render_files(bot: discord.Client, config: Config) -> list[discord.File]:
    guild = bot.get_guild(config.guild_id)
    if guild is None:
        raise RuntimeError("Guild not found for rankboard rendering")

    season_entries = await _prepare_season_entries(guild)
    lifetime_entries = await _prepare_lifetime_entries(guild)

    season_path = os.path.join(config.data_dir, "season.png")
    lifetime_path = os.path.join(config.data_dir, "lifetime.png")

    render_season_image(season_entries, season_path)
    render_lifetime_image(lifetime_entries, lifetime_path)

    return [
        discord.File(season_path, filename="season.png"),
        discord.File(lifetime_path, filename="lifetime.png"),
    ]


async def _prepare_season_entries(guild: discord.Guild) -> list[dict]:
    entries = []
    for row in compute_top10(guild.id):
        member = guild.get_member(row["user_id"])
        entries.append(
            {
                "name": member.display_name if member else str(row["user_id"]),
                "season_xp": row["season_xp"],
                "avatar": await _fetch_avatar(member),
            }
        )
    return entries


async def _prepare_lifetime_entries(guild: discord.Guild) -> list[dict]:
    entries = []
    for row in compute_lifetime_top10(guild.id):
        member = guild.get_member(row["user_id"])
        entries.append(
            {
                "name": member.display_name if member else str(row["user_id"]),
                "level": level_from_xp(row["lifetime_xp"]),
                "avatar": await _fetch_avatar(member),
            }
        )
    return entries


async def _fetch_avatar(member: Optional[discord.Member]) -> Optional[Image.Image]:
    if member is None:
        return None
    try:
        data = await member.display_avatar.read()
    except Exception:
        return None
    try:
        return Image.open(io.BytesIO(data)).convert("RGB")
    except Exception:
        return None


def _cleanup_files(files: list[discord.File]) -> None:
    for file in files:
        try:
            os.remove(file.fp.name)
        except OSError:
            pass
