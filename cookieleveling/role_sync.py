from __future__ import annotations

import json
import logging
from typing import Optional

import discord

from .config import Config
from .db import fetch_rank_role_snapshot, upsert_rank_role_snapshot
from .ranker import compute_top10

_LOGGER = logging.getLogger(__name__)


async def update_rank_roles(bot: discord.Client, config: Config) -> bool:
    role_ids = _role_ids(config)
    if role_ids is None:
        _LOGGER.warning("role ids not configured; skipping role sync")
        return False

    guild = bot.get_guild(config.guild_id)
    if guild is None:
        _LOGGER.warning("guild not found for role sync")
        return False

    top10 = compute_top10(config.guild_id)
    new_snapshot = [entry["user_id"] for entry in top10]
    old_snapshot = _load_snapshot(fetch_rank_role_snapshot(config.guild_id))

    affected = set(old_snapshot) | set(new_snapshot)
    if not affected:
        upsert_rank_role_snapshot(config.guild_id, json.dumps(new_snapshot))
        return True

    managed_roles = {rid for rid in role_ids.values() if rid is not None}
    for user_id in affected:
        member = guild.get_member(user_id)
        if member is None:
            continue
        desired_role_id = _desired_role_for_user(user_id, new_snapshot, role_ids)
        await _apply_roles(member, desired_role_id, managed_roles)

    upsert_rank_role_snapshot(config.guild_id, json.dumps(new_snapshot))
    return True


def _role_ids(config: Config) -> Optional[dict[int, int]]:
    if (
        config.role_season_1 is None
        or config.role_season_2 is None
        or config.role_season_3 is None
        or config.role_season_4 is None
        or config.role_season_5 is None
        or config.role_season_top10 is None
    ):
        return None
    return {
        1: config.role_season_1,
        2: config.role_season_2,
        3: config.role_season_3,
        4: config.role_season_4,
        5: config.role_season_5,
        6: config.role_season_top10,
        7: config.role_season_top10,
        8: config.role_season_top10,
        9: config.role_season_top10,
        10: config.role_season_top10,
    }


def role_sync_block_reason(config: Config) -> str | None:
    missing = _missing_role_envs(config)
    if not missing:
        return None
    return "ROLE未設定:" + ",".join(missing)


def _load_snapshot(row) -> list[int]:
    if row is None or not row["last_snapshot_json"]:
        return []
    try:
        data = json.loads(row["last_snapshot_json"])
    except json.JSONDecodeError:
        return []
    return [int(x) for x in data]


def _missing_role_envs(config: Config) -> list[str]:
    missing = []
    if config.role_season_1 is None:
        missing.append("ROLE_SEASON_1")
    if config.role_season_2 is None:
        missing.append("ROLE_SEASON_2")
    if config.role_season_3 is None:
        missing.append("ROLE_SEASON_3")
    if config.role_season_4 is None:
        missing.append("ROLE_SEASON_4")
    if config.role_season_5 is None:
        missing.append("ROLE_SEASON_5")
    if config.role_season_top10 is None:
        missing.append("ROLE_SEASON_TOP10")
    return missing


def _desired_role_for_user(user_id: int, snapshot: list[int], role_ids: dict[int, int]) -> int | None:
    if user_id not in snapshot:
        return None
    rank = snapshot.index(user_id) + 1
    return role_ids.get(rank)


async def _apply_roles(
    member: discord.Member, desired_role_id: int | None, managed_roles: set[int]
) -> None:
    current_ids = {role.id for role in member.roles}
    to_remove = [role for role in member.roles if role.id in managed_roles]
    if desired_role_id is not None and desired_role_id not in current_ids:
        role = member.guild.get_role(desired_role_id)
        if role:
            await member.add_roles(role, reason="Rankboard sync")
    if to_remove:
        if desired_role_id is None:
            await member.remove_roles(*to_remove, reason="Rankboard sync")
        else:
            keep = member.guild.get_role(desired_role_id)
            if keep is None:
                await member.remove_roles(*to_remove, reason="Rankboard sync")
            else:
                remove_roles = [role for role in to_remove if role.id != keep.id]
                if remove_roles:
                    await member.remove_roles(*remove_roles, reason="Rankboard sync")
