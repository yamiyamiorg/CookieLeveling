from __future__ import annotations

import io
import logging

import discord
import aiohttp
from PIL import Image

from .config import Config
from .db import (
    ensure_user,
    fetch_user,
    set_optout,
)
from .rankboard_publisher import set_rankboard
from .xp_engine import progress_for_xp
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
