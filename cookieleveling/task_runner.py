import logging

import discord

from .config import Config
from .rankboard_publisher import update_rankboard
from .role_sync import update_rank_roles
from .xp_engine import maybe_monthly_reset, tick_minute

_LOGGER = logging.getLogger(__name__)


def run_minute_tasks(config: Config) -> int:
    updated = tick_minute(config.guild_id)
    if updated:
        _LOGGER.info("minute tick updated %s users", updated)
    return updated


async def run_hourly_tasks(bot: discord.Client, config: Config) -> None:
    reset = maybe_monthly_reset(config.guild_id)
    if reset:
        _LOGGER.info("monthly season reset applied")
    roles_updated = await update_rank_roles(bot, config)
    if roles_updated:
        _LOGGER.info("hourly roles updated")
    updated = await update_rankboard(bot, config)
    if updated:
        _LOGGER.info("hourly rankboard updated")
