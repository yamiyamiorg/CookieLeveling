from __future__ import annotations

from datetime import datetime, timezone

import discord

from .config import Config
from .db import (
    fetch_guild_settings,
    fetch_rank_role_snapshot,
    fetch_schema_version,
    fetch_user,
    grant_xp,
    set_optout,
    set_voice_state,
    set_xp,
)
from .debug_mutations import (
    ensure_debug_mutations,
    validate_grant_xp,
    validate_set_xp,
)
from .rankboard_publisher import set_rankboard, update_rankboard
from .ranker import compute_top10
from .role_sync import role_sync_block_reason, update_rank_roles
from .xp_engine import tick_minute
from .voice_tracker import get_voice_debug_lines


def handle_optout(config: Config, user_id: int) -> str:
    set_optout(config.guild_id, user_id, True)
    return "オプトアウトしました。"


def handle_optin(config: Config, user_id: int) -> str:
    set_optout(config.guild_id, user_id, False)
    return "オプトインしました。"


def handle_debug_status() -> str:
    schema_version = fetch_schema_version()
    return f"OK。schema_version={schema_version}"


def handle_debug_vc(config: Config) -> str:
    lines = get_voice_debug_lines(config.guild_id)
    return "VCスナップショット:\n" + "\n".join(lines)


def handle_debug_user(config: Config, user_id: int) -> str:
    row = fetch_user(config.guild_id, user_id)
    if row is None:
        return "ユーザーが見つかりません。"
    return (
        "user_id={user_id} season_xp={season_xp} lifetime_xp={lifetime_xp} "
        "rem_lifetime={rem_lifetime:.3f} optout={optout} is_in_vc={is_in_vc} "
        "joined_at={joined_at} last_earned_at={last_earned_at}"
    ).format(**row)


def handle_debug_grantxp(config: Config, user_id: int, season: int, lifetime: int) -> str:
    message = ensure_debug_mutations(config)
    if message:
        return message
    message = validate_grant_xp(season, lifetime)
    if message:
        return message
    now = datetime.now(timezone.utc).isoformat()
    last_earned_at = now if season > 0 else None
    grant_xp(config.guild_id, user_id, season, lifetime, last_earned_at)
    return "付与完了"


def handle_debug_setxp(
    config: Config, user_id: int, season: int, lifetime: int, rem_lifetime: float | None
) -> str:
    message = ensure_debug_mutations(config)
    if message:
        return message
    message = validate_set_xp(season, lifetime, rem_lifetime)
    if message:
        return message
    now = datetime.now(timezone.utc).isoformat()
    set_xp(config.guild_id, user_id, season, lifetime, rem_lifetime, now)
    return "設定完了"


def handle_debug_setvc(config: Config, user_id: int, in_vc: bool) -> str:
    message = ensure_debug_mutations(config)
    if message:
        return message
    joined_at = datetime.now(timezone.utc).isoformat() if in_vc else None
    set_voice_state(config.guild_id, user_id, in_vc, joined_at)
    return "VC更新"


def handle_debug_top10(config: Config) -> str:
    top10 = compute_top10(config.guild_id)
    if not top10:
        return "ランキングデータがありません。"
    lines = []
    for idx, entry in enumerate(top10, start=1):
        last_earned = (
            entry["last_earned_at"].isoformat() if entry["last_earned_at"] else "none"
        )
        lines.append(
            f"{idx}. user_id={entry['user_id']} season_xp={entry['season_xp']} "
            f"active_seconds={entry['active_seconds']} last_earned_at={last_earned}"
        )
    return "\n".join(lines)


def handle_debug_rankboard(config: Config) -> str:
    settings = fetch_guild_settings(config.guild_id)
    if settings is None:
        return "ランクボード未設定。"
    return (
        f"channel_id={settings['rankboard_channel_id']} "
        f"message_id={settings['rankboard_message_id']} "
        f"updated_at={settings['updated_at']}"
    )


def handle_debug_roles(config: Config) -> str:
    snapshot = fetch_rank_role_snapshot(config.guild_id)
    snapshot_json = snapshot["last_snapshot_json"] if snapshot else "[]"
    return (
        f"ROLE_SEASON_1={config.role_season_1} "
        f"ROLE_SEASON_2={config.role_season_2} "
        f"ROLE_SEASON_3={config.role_season_3} "
        f"ROLE_SEASON_4={config.role_season_4} "
        f"ROLE_SEASON_5={config.role_season_5} "
        f"ROLE_SEASON_TOP10={config.role_season_top10} "
        f"snapshot={snapshot_json}"
    )


def handle_tick_minute(config: Config) -> str:
    message = ensure_debug_mutations(config)
    if message:
        return message
    updated = tick_minute(config.guild_id)
    return f"更新人数:{updated}"


async def handle_tick_rankboard(bot: discord.Client, config: Config) -> str:
    message = ensure_debug_mutations(config)
    if message:
        return message
    updated = await update_rankboard(bot, config)
    return "更新" if updated else "未設置"


async def handle_tick_roles(bot: discord.Client, config: Config) -> str:
    message = ensure_debug_mutations(config)
    if message:
        return message
    reason = role_sync_block_reason(config)
    if reason:
        return reason
    updated = await update_rank_roles(bot, config)
    return "更新" if updated else "更新不可"


async def handle_rankboard_set(
    bot: discord.Client,
    config: Config,
    channel: discord.abc.GuildChannel | None,
) -> str:
    if channel is None:
        return "チャンネル情報が取得できません。"
    ok, message = await set_rankboard(bot, config, channel)
    if ok:
        return message
    return message
