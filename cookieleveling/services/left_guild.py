from __future__ import annotations

import logging
from datetime import datetime, timezone

import discord

from cookieleveling.db import (
    clear_user_left,
    fetch_host_top20_monthly,
    fetch_host_top20_total,
    fetch_host_top20_weekly,
    fetch_lifetime_candidates,
    fetch_rank_candidates,
    fetch_weekly_candidates,
    mark_user_left,
)

_LOGGER = logging.getLogger(__name__)


def _collect_candidate_user_ids(guild: discord.Guild) -> list[int]:
    user_ids: set[int] = set()
    for row in fetch_rank_candidates(guild.id, limit=5000):
        user_ids.add(row["user_id"])
    for row in fetch_weekly_candidates(guild.id, limit=5000):
        user_ids.add(row["user_id"])
    for row in fetch_lifetime_candidates(guild.id, limit=5000):
        user_ids.add(row["user_id"])
    for row in fetch_host_top20_weekly(guild.id, limit=5000):
        user_ids.add(row["user_id"])
    for row in fetch_host_top20_monthly(guild.id, limit=5000):
        user_ids.add(row["user_id"])
    for row in fetch_host_top20_total(guild.id, limit=5000):
        user_ids.add(row["user_id"])
    return sorted(user_ids)


async def sync_left_guild_status(guild: discord.Guild) -> tuple[int, int]:
    user_ids = _collect_candidate_user_ids(guild)
    if not user_ids:
        return 0, 0

    left_count = 0
    cleared_count = 0
    now = _utc_now()

    for user_id in user_ids:
        member = guild.get_member(user_id)
        if member is not None:
            if clear_user_left(guild.id, user_id):
                cleared_count += 1
            continue
        try:
            member = await guild.fetch_member(user_id)
        except discord.NotFound:
            if mark_user_left(guild.id, user_id, now):
                left_count += 1
            continue
        except (discord.Forbidden, discord.HTTPException):
            continue
        if member is not None:
            if clear_user_left(guild.id, user_id):
                cleared_count += 1

    if left_count or cleared_count:
        _LOGGER.info(
            "left guild status synced: left=%s cleared=%s candidates=%s",
            left_count,
            cleared_count,
            len(user_ids),
        )
    return left_count, cleared_count


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()
