import logging

import discord

from .config import Config
from .rankboard_publisher import update_rankboard
from .host_rankboard_publisher import update_hostboard
from .host_tracker import snapshot_host_sessions, tick_host_xp
from .role_assigner import apply_lifetime_roles_for_levels
from .xp_engine import maybe_host_monthly_reset, maybe_monthly_reset, tick_minute
from .voice_tracker import snapshot_voice_state

_LOGGER = logging.getLogger(__name__)


def run_minute_tasks(bot: discord.Client, config: Config) -> int:
    guild = bot.get_guild(config.guild_id)
    if guild is None:
        return 0
    snapshot_voice_state(guild)
    snapshot_host_sessions(guild)
    updated, level_changes = tick_minute(config.guild_id)
    if level_changes:
        bot.loop.create_task(apply_lifetime_roles_for_levels(guild, level_changes))
    host_updated = tick_host_xp(guild)
    if updated:
        _LOGGER.info("minute tick updated %s users", updated)
    if host_updated:
        _LOGGER.info("minute host tick updated %s channels", host_updated)
    return updated


async def run_hourly_tasks(bot: discord.Client, config: Config) -> None:
    guild = bot.get_guild(config.guild_id)
    if guild is None:
        return
    reset = maybe_monthly_reset(config.guild_id)
    if reset:
        _LOGGER.info("monthly season reset applied")
    host_reset = maybe_host_monthly_reset(config.guild_id)
    if host_reset:
        _LOGGER.info("monthly host reset applied")
    updated = await update_rankboard(bot, config)
    if updated:
        _LOGGER.info("hourly rankboard updated")
    host_updated = await update_hostboard(bot, config)
    if host_updated:
        _LOGGER.info("hourly hostboard updated")
