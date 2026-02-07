import logging

import discord

from .db import fetch_lifetime_users
from .xp_engine import level_from_xp

_LOGGER = logging.getLogger(__name__)

_ROLE_LV1 = 1451925109248884786
_ROLE_LV20 = 1451924268144394385
_ROLE_LV40 = 1451924729475764394
_ROLE_LV80 = 1451906366024188014

_ROLE_IDS = (_ROLE_LV1, _ROLE_LV20, _ROLE_LV40, _ROLE_LV80)


def _target_role_id(level: int) -> int:
    if level >= 80:
        return _ROLE_LV80
    if level >= 40:
        return _ROLE_LV40
    if level >= 20:
        return _ROLE_LV20
    return _ROLE_LV1


async def apply_lifetime_role(
    guild: discord.Guild, member: discord.Member, level: int
) -> bool:
    if member.bot:
        return False
    target_role_id = _target_role_id(level)
    target_role = guild.get_role(target_role_id)
    if target_role is None:
        _LOGGER.warning("lifetime role not found: role_id=%s", target_role_id)
        return False

    role_ids = {role.id for role in member.roles}
    has_target = target_role_id in role_ids
    has_other = any(role_id in role_ids for role_id in _ROLE_IDS if role_id != target_role_id)
    if has_target and not has_other:
        return False

    new_roles = [role for role in member.roles if role.id not in _ROLE_IDS]
    new_roles.append(target_role)
    try:
        await member.edit(roles=new_roles, reason="CookieLeveling lifetime role update")
    except (discord.Forbidden, discord.HTTPException):
        _LOGGER.warning(
            "lifetime role update failed: user_id=%s target_role_id=%s",
            member.id,
            target_role_id,
        )
        return False
    return True


async def apply_lifetime_roles_for_levels(
    guild: discord.Guild, levels: dict[int, int]
) -> int:
    updated = 0
    for user_id, level in levels.items():
        member = guild.get_member(user_id)
        if member is None:
            try:
                member = await guild.fetch_member(user_id)
            except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                continue
        if await apply_lifetime_role(guild, member, level):
            updated += 1
    return updated


async def sync_lifetime_roles(guild: discord.Guild) -> int:
    updated = 0
    for row in fetch_lifetime_users(guild.id):
        level = level_from_xp(int(row["lifetime_xp"]))
        member = guild.get_member(row["user_id"])
        if member is None:
            try:
                member = await guild.fetch_member(row["user_id"])
            except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                continue
        if await apply_lifetime_role(guild, member, level):
            updated += 1
    return updated
