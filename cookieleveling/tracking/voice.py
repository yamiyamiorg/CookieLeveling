from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict, Tuple

import discord

from cookieleveling.db import clear_user_left, fetch_user_flags, upsert_voice_state

_channel_map: Dict[Tuple[int, int], int] = {}


def restore_voice_state(guild: discord.Guild) -> None:
    _channel_map_keys = [key for key in _channel_map if key[0] == guild.id]
    for key in _channel_map_keys:
        _channel_map.pop(key, None)


def snapshot_voice_state(guild: discord.Guild) -> None:
    return


def handle_voice_state_update(
    guild_id: int,
    member: discord.Member,
    before: discord.VoiceState,
    after: discord.VoiceState,
) -> None:
    if member.bot:
        return
    if not _should_track_user(guild_id, member.id):
        return

    if before.channel is None and after.channel is not None:
        now = _utc_now()
        upsert_voice_state(guild_id, member.id, True, now)
        _channel_map[(guild_id, member.id)] = after.channel.id
        return

    if before.channel is not None and after.channel is None:
        upsert_voice_state(guild_id, member.id, False, None)
        _channel_map.pop((guild_id, member.id), None)
        return

    if before.channel is not None and after.channel is not None:
        _channel_map[(guild_id, member.id)] = after.channel.id


def _should_track_user(guild_id: int, user_id: int) -> bool:
    row = fetch_user_flags(guild_id, user_id)
    if row is None:
        return True
    if row["left_guild_at"]:
        clear_user_left(guild_id, user_id)
    if row["is_excluded"]:
        return False
    if row["deleted_at"]:
        return False
    return True


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()
