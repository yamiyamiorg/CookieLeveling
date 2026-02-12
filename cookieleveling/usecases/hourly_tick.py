from __future__ import annotations

import logging

import discord

from cookieleveling.config.config import Config
from cookieleveling.db import ensure_weekly_reset, prune_weekly_xp
from cookieleveling.domain.periods import min_week_key_to_keep
from cookieleveling.domain.ranking import compute_weekly_top20
from cookieleveling.domain.xp import maybe_host_monthly_reset, maybe_monthly_reset
from cookieleveling.publishing.hostboard import update_hostboard
from cookieleveling.publishing.rankboard import update_rankboard
from cookieleveling.services.member_sync import sync_member_state

_LOGGER = logging.getLogger(__name__)


async def run_hourly_tick(bot: discord.Client, config: Config) -> None:
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
