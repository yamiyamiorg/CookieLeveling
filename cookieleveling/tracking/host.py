from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

import discord

from cookieleveling.db import (
    add_host_target_channel,
    add_host_xp,
    add_host_weekly_xp,
    clear_user_left,
    clear_host_session,
    confirm_host,
    fetch_user_flags,
    fetch_host_session,
    fetch_host_sessions,
    fetch_host_target_channels,
    increment_host_session_counts,
    mark_host_timeout,
    remove_host_target_channel,
    update_host_last_seen,
    upsert_host_session,
)
from cookieleveling.db import ensure_period_state

_LOGGER = logging.getLogger(__name__)

TARGET_CATEGORY_ID = 1450712250514935960
SHABEREA_BOT_ID = 695096014482440244
HOST_CONFIRM_TIMEOUT_SECONDS = 120

_target_channel_ids: set[int] = set()
_targets_loaded = False


def load_host_targets(guild: discord.Guild) -> None:
    global _target_channel_ids, _targets_loaded
    existing_ids: set[int] = set()
    for channel_id in fetch_host_target_channels(guild.id):
        channel = guild.get_channel(channel_id)
        if channel is None:
            existing_ids.add(channel_id)
            continue
        existing_ids.add(channel_id)
    _target_channel_ids = existing_ids
    _targets_loaded = True
    _LOGGER.info("host targets loaded: %s", len(_target_channel_ids))


def handle_channel_create(channel: discord.abc.GuildChannel) -> None:
    if not _is_target_category_channel(channel):
        return
    now = _utc_now()
    add_host_target_channel(channel.guild.id, channel.id, now)
    _target_channel_ids.add(channel.id)
    _LOGGER.info("host target added: %s", channel.id)


def handle_channel_delete(channel: discord.abc.GuildChannel) -> None:
    remove_host_target_channel(channel.guild.id, channel.id)
    _target_channel_ids.discard(channel.id)
    clear_host_session(channel.guild.id, channel.id)
    _LOGGER.info("host target removed: %s", channel.id)


def handle_voice_state_update(
    member: discord.Member,
    before: discord.VoiceState,
    after: discord.VoiceState,
) -> None:
    if member.bot:
        return
    guild = member.guild
    _ensure_targets_loaded(guild)
    channel_ids: set[int] = set()
    if before.channel is not None:
        channel_ids.add(before.channel.id)
    if after.channel is not None:
        channel_ids.add(after.channel.id)
    if not channel_ids:
        return
    now = datetime.now(timezone.utc)
    for channel_id in channel_ids:
        channel = guild.get_channel(channel_id)
        if not _is_target_channel(channel):
            continue
        _reconcile_channel(guild.id, channel, now)


def handle_shaberea_message(message: discord.Message) -> None:
    if message.guild is None:
        return
    if message.author.id != SHABEREA_BOT_ID:
        return
    if not message.mentions:
        return
    if any(mentioned.bot for mentioned in message.mentions):
        return
    if any(_is_excluded_user(message.guild.id, mentioned.id) for mentioned in message.mentions):
        return
    candidate = message.mentions[0]
    guild = message.guild
    _ensure_targets_loaded(guild)
    member = guild.get_member(candidate.id)
    if member is None or member.voice is None or member.voice.channel is None:
        return
    channel = member.voice.channel
    if not _is_target_channel(channel):
        return
    session = fetch_host_session(guild.id, channel.id)
    if session is None:
        return
    if session["locked"] or session["host_timed_out"]:
        return
    now = datetime.now(timezone.utc)
    deadline = _parse_time(session["deadline_at"])
    if deadline is not None and now > deadline:
        mark_host_timeout(guild.id, channel.id)
        return
    confirm_host(guild.id, channel.id, member.id)
    increment_host_session_counts(guild.id, member.id)
    _LOGGER.info("host confirmed: channel=%s user=%s", channel.id, member.id)


def snapshot_host_sessions(guild: discord.Guild) -> None:
    _ensure_targets_loaded(guild)
    now = datetime.now(timezone.utc)
    sessions = {
        row["channel_id"]: row for row in fetch_host_sessions(guild.id)
    }
    for channel_id in list(_target_channel_ids):
        channel = guild.get_channel(channel_id)
        if not _is_target_channel(channel):
            continue
        member_count = _count_presence(channel)
        session = sessions.get(channel_id)
        if member_count == 0:
            if session is not None:
                clear_host_session(guild.id, channel_id)
            continue
        if session is None:
            deadline = now + timedelta(seconds=HOST_CONFIRM_TIMEOUT_SECONDS)
            now_iso = now.isoformat()
            upsert_host_session(
                guild.id,
                channel_id,
                now_iso,
                deadline.isoformat(),
                now_iso,
            )
            continue
        update_host_last_seen(guild.id, channel_id, now.isoformat())
        if not session["locked"] and not session["host_timed_out"]:
            deadline = _parse_time(session["deadline_at"])
            if deadline is not None and now > deadline:
                mark_host_timeout(guild.id, channel_id)


def tick_host_xp(guild: discord.Guild) -> int:
    _ensure_targets_loaded(guild)
    now = datetime.now(timezone.utc)
    ensure_period_state(guild.id)
    sessions = {
        row["channel_id"]: row for row in fetch_host_sessions(guild.id)
    }
    updated = 0
    for channel_id in list(_target_channel_ids):
        channel = guild.get_channel(channel_id)
        if not _is_target_channel(channel):
            continue
        session = sessions.get(channel_id)
        if session is None or not session["locked"]:
            continue
        if session["host_timed_out"]:
            continue
        host_user_id = session["host_user_id"]
        if host_user_id is None:
            continue
        if _is_excluded_user(guild.id, host_user_id):
            continue
        effective_count = _count_present(channel)
        if effective_count < 2:
            continue
        add_host_xp(
            guild_id=guild.id,
            user_id=host_user_id,
            monthly_inc=effective_count,
            total_inc=effective_count,
            last_earned_at=now.isoformat(),
        )
        add_host_weekly_xp(
            guild_id=guild.id,
            user_id=host_user_id,
            weekly_inc=effective_count,
            updated_at=now.isoformat(),
        )
        updated += 1
    return updated


def _reconcile_channel(
    guild_id: int, channel: discord.abc.GuildChannel, now: datetime
) -> None:
    if not isinstance(channel, (discord.VoiceChannel, discord.StageChannel)):
        return
    member_count = _count_presence(channel)
    session = fetch_host_session(guild_id, channel.id)
    if member_count == 0:
        if session is not None:
            clear_host_session(guild_id, channel.id)
        return
    if session is None:
        deadline = now + timedelta(seconds=HOST_CONFIRM_TIMEOUT_SECONDS)
        now_iso = now.isoformat()
        upsert_host_session(
            guild_id, channel.id, now_iso, deadline.isoformat(), now_iso
        )
        return
    update_host_last_seen(guild_id, channel.id, now.isoformat())
    if not session["locked"] and not session["host_timed_out"]:
        deadline = _parse_time(session["deadline_at"])
        if deadline is not None and now > deadline:
            mark_host_timeout(guild_id, channel.id)


def _ensure_targets_loaded(guild: discord.Guild) -> None:
    if not _targets_loaded:
        load_host_targets(guild)


def _is_target_channel(channel: discord.abc.GuildChannel | None) -> bool:
    if channel is None:
        return False
    if channel.id not in _target_channel_ids:
        return False
    return _is_target_category_channel(channel)


def _is_target_category_channel(channel: discord.abc.GuildChannel) -> bool:
    if not isinstance(channel, (discord.VoiceChannel, discord.StageChannel)):
        return False
    return channel.category_id == TARGET_CATEGORY_ID


def _count_humans(channel: discord.abc.GuildChannel) -> int:
    if not isinstance(channel, (discord.VoiceChannel, discord.StageChannel)):
        return 0
    return sum(1 for member in channel.members if not member.bot)


def _count_presence(channel: discord.abc.GuildChannel) -> int:
    return _count_humans(channel)


def _count_present(channel: discord.abc.GuildChannel) -> int:
    return _count_humans(channel)


def _is_excluded_user(guild_id: int, user_id: int) -> bool:
    row = fetch_user_flags(guild_id, user_id)
    if row is None:
        return False
    if row["left_guild_at"]:
        clear_user_left(guild_id, user_id)
    if row["optout"]:
        return True
    return bool(row["is_excluded"])


def _parse_time(value) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()
