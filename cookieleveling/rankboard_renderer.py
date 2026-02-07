from __future__ import annotations

import os
import tempfile
from dataclasses import dataclass

import discord

from .image_renderer import render_rankboard_image

_LIFETIME_BACKGROUND = (255, 242, 204)
_LIFETIME_XP_BAR_FILL = (244, 180, 0)


@dataclass
class RenderedRankboard:
    season_file: discord.File
    lifetime_file: discord.File
    temp_paths: list[str]

    def cleanup(self) -> None:
        for path in self.temp_paths:
            try:
                os.remove(path)
            except OSError:
                pass


async def render_rankboard(
    season_entries: list[dict], lifetime_entries: list[dict]
) -> RenderedRankboard:
    season_path = _make_temp_path("season_board")
    lifetime_path = _make_temp_path("lifetime_board")

    render_rankboard_image(
        season_entries,
        season_path,
        title="月間 通話ランキングTOP20",
    )
    render_rankboard_image(
        lifetime_entries,
        lifetime_path,
        title="累計 通話ランキングTOP20",
        background=_LIFETIME_BACKGROUND,
        header_fill=_LIFETIME_BACKGROUND,
        xp_bar_fill=_LIFETIME_XP_BAR_FILL,
    )

    return RenderedRankboard(
        season_file=discord.File(season_path, filename="season_board.png"),
        lifetime_file=discord.File(lifetime_path, filename="lifetime_board.png"),
        temp_paths=[season_path, lifetime_path],
    )


def _make_temp_path(prefix: str) -> str:
    path = os.path.join(tempfile.gettempdir(), f"cookieleveling_{prefix}.png")
    return path
