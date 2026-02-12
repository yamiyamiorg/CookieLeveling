import logging

import discord

from cookieleveling.config.config import Config
from cookieleveling.publishing.rankboard import update_rankboard
from cookieleveling.publishing.hostboard import update_hostboard
from cookieleveling.tracking.host import snapshot_host_sessions, tick_host_xp
from cookieleveling.services.role_assigner import apply_lifetime_roles_for_levels
from cookieleveling.services.member_sync import sync_member_state
from cookieleveling.domain.xp import maybe_host_monthly_reset, maybe_monthly_reset, tick_minute
from cookieleveling.db import ensure_weekly_reset, prune_weekly_xp
from cookieleveling.domain.week import min_week_key_to_keep
from cookieleveling.domain.ranking import compute_weekly_top20
from cookieleveling.tracking.voice import snapshot_voice_state

_LOGGER = logging.getLogger(__name__)


def run_minute_tasks(bot: discord.Client, config: Config) -> int:
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


async def run_hourly_tasks(bot: discord.Client, config: Config) -> None:
    guild = bot.get_guild(config.guild_id)
    if guild is None:
        return
    ensure_weekly_reset(config.guild_id)
    reset = maybe_monthly_reset(config.guild_id)
    if reset:
        _LOGGER.info("monthly season reset applied")
    host_reset = maybe_host_monthly_reset(config.guild_id)
    if host_reset:
        _LOGGER.info("monthly host reset applied")
    try:
        await sync_member_state(guild)
    except Exception:
        _LOGGER.exception("member_state sync failed")
    updated = await update_rankboard(bot, config)
    _LOGGER.info("hourly rankboard updated=%s", int(updated))
    weekly_candidates = compute_weekly_top20(config.guild_id, limit=20)
    _LOGGER.info(
        "rankboard weekly entries: candidates=%s final=%s candidates>0=%s",
        len(weekly_candidates),
        len(weekly_candidates),
        int(len(weekly_candidates) > 0),
    )
    host_updated = await update_hostboard(bot, config)
    _LOGGER.info("hourly hostboard updated=%s", int(host_updated))
    try:
        min_week_key = min_week_key_to_keep(12)
        prune_weekly_xp(min_week_key)
        _LOGGER.info("weekly xp pruned before %s", min_week_key)
    except Exception:
        _LOGGER.exception("weekly xp prune failed")
