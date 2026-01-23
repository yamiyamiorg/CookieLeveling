from __future__ import annotations

import os
import tempfile
from dataclasses import dataclass

import discord

from .image_renderer import render_rankboard_image

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


async def render_rankboard(
    season_entries: list[dict], lifetime_entries: list[dict]
) -> RenderedRankboard:
    rankboard_path = _make_temp_path("rankboard")

    render_rankboard_image(season_entries, lifetime_entries, rankboard_path)

    files = [
        discord.File(rankboard_path, filename="rankboard.png"),
    ]
    return RenderedRankboard(
        files=files,
        temp_paths=[rankboard_path],
    )


def _make_temp_path(prefix: str) -> str:
    path = os.path.join(tempfile.gettempdir(), f"cookieleveling_{prefix}.png")
    return path
