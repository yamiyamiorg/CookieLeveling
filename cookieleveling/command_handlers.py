from __future__ import annotations

from datetime import datetime, timezone
import io
import logging

import discord
import aiohttp
from PIL import Image

from .config import Config
from .db import (
    ensure_user,
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
from .xp_engine import progress_for_xp, tick_minute
from .voice_tracker import get_voice_debug_lines
from .display_name_tokens import tokenize_display_name, truncate_tokens
from .emoji_assets import resolve_emoji_tokens
from .level_renderer import render_level_card

_LOGGER = logging.getLogger(__name__)


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


async def handle_level(
    config: Config, user: discord.User
) -> tuple[discord.File | None, str | None]:
    ensure_user(config.guild_id, user.id)
    row = fetch_user(config.guild_id, user.id)
    if row is None:
        return None, "ユーザーデータが見つかりません。"

    if isinstance(user, discord.Member):
        display_name = user.display_name
    else:
        display_name = user.name

    tokens = tokenize_display_name(display_name)
    tokens = truncate_tokens(tokens, max_chars=16)

    timeout = aiohttp.ClientTimeout(total=10)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        await resolve_emoji_tokens(tokens, session)
        avatar = await _fetch_avatar_image(user, session)

    season_level, season_curr, season_next, season_progress = progress_for_xp(
        row["season_xp"]
    )
    lifetime_level, lifetime_curr, lifetime_next, lifetime_progress = progress_for_xp(
        row["lifetime_xp"]
    )

    try:
        png_bytes = render_level_card(
            name_tokens=tokens,
            avatar=avatar,
            season_stats={
                "level": season_level,
                "xp": row["season_xp"],
                "curr": season_curr,
                "next": season_next,
                "progress": season_progress,
            },
            lifetime_stats={
                "level": lifetime_level,
                "xp": row["lifetime_xp"],
                "curr": lifetime_curr,
                "next": lifetime_next,
                "progress": lifetime_progress,
            },
            optout=bool(row["optout"]),
        )
    except Exception:
        _LOGGER.exception("level render failed: user_id=%s", user.id)
        return None, "画像生成に失敗しました。"

    return discord.File(io.BytesIO(png_bytes), filename="level.png"), None


async def _fetch_avatar_image(
    user: discord.User, session: aiohttp.ClientSession
) -> Image.Image | None:
    url = str(user.display_avatar.url)
    try:
        async with session.get(url) as response:
            if response.status != 200:
                _LOGGER.warning(
                    "avatar download failed: user_id=%s status=%s",
                    user.id,
                    response.status,
                )
                return None
            data = await response.read()
    except Exception:
        _LOGGER.exception("avatar download failed: user_id=%s", user.id)
        return None
    try:
        return Image.open(io.BytesIO(data)).convert("RGB")
    except Exception:
        _LOGGER.exception("avatar decode failed: user_id=%s", user.id)
        return None
