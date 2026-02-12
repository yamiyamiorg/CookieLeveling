from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Iterable

import discord

from cookieleveling.db import (
    fetch_member_ids,
    mark_members_left,
    set_member_state,
    update_member_cache,
    upsert_guild_members,
)
from cookieleveling.domain.member_state import (
    MEMBER_STATE_ACTIVE,
    MEMBER_STATE_LEFT,
)

_LOGGER = logging.getLogger(__name__)


async def sync_member_state(guild: discord.Guild) -> tuple[int, int, int]:
    """Sync full member list to guild_members.

    Returns (active_count, left_marked, total_known).
    """
    members, complete = await _fetch_all_members(guild)
    if not members:
        _LOGGER.warning("member sync skipped: no members fetched")
        return 0, 0, len(fetch_member_ids(guild.id))

    now = _utc_now()
    rows = [
        (
            guild.id,
            member.id,
            MEMBER_STATE_ACTIVE,
            now,
            member.display_name,
            str(member.display_avatar.url),
        )
        for member in members
    ]
    upsert_guild_members(rows)

    left_marked = 0
    if complete:
        existing_ids = fetch_member_ids(guild.id)
        active_ids = {member.id for member in members}
        missing_ids = existing_ids - active_ids
        left_marked = mark_members_left(guild.id, missing_ids, now)
    else:
        _LOGGER.warning("member sync incomplete: skipping left marking")

    total_known = len(fetch_member_ids(guild.id))
    _LOGGER.info(
        "member_state synced: active=%s left_marked=%s known=%s complete=%s",
        len(members),
        left_marked,
        total_known,
        int(complete),
    )
    return len(members), left_marked, total_known


async def refresh_member_cache(
    guild: discord.Guild, user_ids: Iterable[int]
) -> tuple[int, int, int]:
    """Refresh display_name/avatar cache for specific users.

    Returns (updated, left_marked, failed).
    """
    updated = 0
    left_marked = 0
    failed = 0
    now = _utc_now()
    for user_id in user_ids:
        member = guild.get_member(user_id)
        if member is None:
            try:
                member = await guild.fetch_member(user_id)
            except discord.NotFound:
                if set_member_state(
                    guild.id, user_id, MEMBER_STATE_LEFT, now
                ):
                    left_marked += 1
                continue
            except (discord.Forbidden, discord.HTTPException):
                failed += 1
                continue
        update_member_cache(
            guild.id,
            user_id,
            member.display_name,
            str(member.display_avatar.url),
            now,
        )
        set_member_state(guild.id, user_id, MEMBER_STATE_ACTIVE, now)
        updated += 1
    return updated, left_marked, failed


async def _fetch_all_members(
    guild: discord.Guild,
) -> tuple[list[discord.Member], bool]:
    members: list[discord.Member] = []
    try:
        async for member in guild.fetch_members(limit=None):
            members.append(member)
        return members, True
    except (discord.Forbidden, discord.HTTPException):
        members = list(guild.members)
        return members, False


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()
