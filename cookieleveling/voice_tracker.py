from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict, Optional, Tuple

import discord

from .db import fetch_voice_states, reset_voice_states, upsert_voice_state

_channel_map: Dict[Tuple[int, int], int] = {}


def restore_voice_state(guild: discord.Guild) -> None:
    now = _utc_now()
    reset_voice_states(guild.id)

    for channel in guild.voice_channels:
        for member in channel.members:
            if member.bot:
                continue
            upsert_voice_state(guild.id, member.id, True, now)
            _channel_map[(guild.id, member.id)] = channel.id


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


def get_voice_debug_lines(guild_id: int) -> list[str]:
    rows = fetch_voice_states(guild_id)
    lines = []
    for row in rows:
        if not row["is_in_vc"]:
            continue
        channel_id = _channel_map.get((guild_id, row["user_id"]))
        lines.append(
            f"user_id={row['user_id']} joined_at={row['joined_at']} channel_id={channel_id}"
        )
    if not lines:
        return ["no active voice users"]
    return lines


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()
