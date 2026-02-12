from __future__ import annotations

import logging

import discord

from cookieleveling.config.config import Config
from cookieleveling.db import ensure_weekly_reset
from cookieleveling.domain.xp import tick_minute
from cookieleveling.services.role_assigner import apply_lifetime_roles_for_levels
from cookieleveling.tracking.host import snapshot_host_sessions, tick_host_xp
from cookieleveling.tracking.voice import snapshot_voice_state

_LOGGER = logging.getLogger(__name__)


def run_minute_tick(bot: discord.Client, config: Config) -> int:
    guild = bot.get_guild(config.guild_id)
    if guild is None:
        return 0

    snapshot_voice_state(guild)
    snapshot_host_sessions(guild)
    ensure_weekly_reset(config.guild_id)

    updated, level_changes = tick_minute(config.guild_id)
    if level_changes:
        bot.loop.create_task(apply_lifetime_roles_for_levels(guild, level_changes))

    host_updated = tick_host_xp(guild)
    if updated:
        _LOGGER.info("minute tick updated %s users", updated)
    if host_updated:
        _LOGGER.info("minute host tick updated %s channels", host_updated)
    return updated
