from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict, Optional, Tuple

import discord

from .db import apply_voice_snapshot, reset_voice_states, upsert_voice_state

_channel_map: Dict[Tuple[int, int], int] = {}


def restore_voice_state(guild: discord.Guild) -> None:
    now = _utc_now()
    reset_voice_states(guild.id)
    _channel_map_keys = [key for key in _channel_map if key[0] == guild.id]
    for key in _channel_map_keys:
        _channel_map.pop(key, None)
    for channel in guild.voice_channels:
        for member in channel.members:
            if member.bot:
                continue
            upsert_voice_state(guild.id, member.id, True, now)
            _channel_map[(guild.id, member.id)] = channel.id


def snapshot_voice_state(guild: discord.Guild) -> None:
    now = _utc_now()
    current_users: set[int] = set()
    for channel in guild.voice_channels:
        for member in channel.members:
            if member.bot:
                continue
            current_users.add(member.id)
            _channel_map[(guild.id, member.id)] = channel.id
    for (gid, uid) in list(_channel_map.keys()):
        if gid == guild.id and uid not in current_users:
            _channel_map.pop((gid, uid), None)
    apply_voice_snapshot(guild.id, current_users, now)


def handle_voice_state_update(
    guild_id: int,
    member: discord.Member,
    before: discord.VoiceState,
    after: discord.VoiceState,
) -> None:
    if member.bot:
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


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()
