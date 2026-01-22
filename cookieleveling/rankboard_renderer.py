from __future__ import annotations

import io
import os
import tempfile
from dataclasses import dataclass
from typing import Optional

import discord
from PIL import Image

from .image_renderer import render_lifetime_image, render_season_image
from .ranker import compute_lifetime_top10, compute_top10
from .xp_engine import level_from_xp


@dataclass
class RenderedRankboard:
    files: list[discord.File]
    temp_paths: list[str]

    def cleanup(self) -> None:
        for path in self.temp_paths:
            try:
                os.remove(path)
            except OSError:
                pass


async def render_rankboard(guild: discord.Guild) -> RenderedRankboard:
    season_entries = await _prepare_season_entries(guild)
    lifetime_entries = await _prepare_lifetime_entries(guild)

    season_path = _make_temp_path("season")
    lifetime_path = _make_temp_path("lifetime")

    render_season_image(season_entries, season_path)
    render_lifetime_image(lifetime_entries, lifetime_path)

    files = [
        discord.File(season_path, filename="season.png"),
        discord.File(lifetime_path, filename="lifetime.png"),
    ]
    return RenderedRankboard(files=files, temp_paths=[season_path, lifetime_path])


def _make_temp_path(prefix: str) -> str:
    fd, path = tempfile.mkstemp(prefix=f"cookieleveling_{prefix}_", suffix=".png")
    os.close(fd)
    return path


async def _prepare_season_entries(guild: discord.Guild) -> list[dict]:
    entries = []
    for row in compute_top10(guild.id):
        member = guild.get_member(row["user_id"])
        entries.append(
            {
                "name": member.display_name if member else str(row["user_id"]),
                "season_xp": row["season_xp"],
                "avatar": await _fetch_avatar(member),
            }
        )
    return entries


async def _prepare_lifetime_entries(guild: discord.Guild) -> list[dict]:
    entries = []
    for row in compute_lifetime_top10(guild.id):
        member = guild.get_member(row["user_id"])
        entries.append(
            {
                "name": member.display_name if member else str(row["user_id"]),
                "level": level_from_xp(row["lifetime_xp"]),
                "avatar": await _fetch_avatar(member),
            }
        )
    return entries


async def _fetch_avatar(member: Optional[discord.Member]) -> Optional[Image.Image]:
    if member is None:
        return None
    try:
        data = await member.display_avatar.read()
    except Exception:
        return None
    try:
        return Image.open(io.BytesIO(data)).convert("RGB")
    except Exception:
        return None
