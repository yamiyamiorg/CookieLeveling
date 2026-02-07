from __future__ import annotations

import os
import tempfile
from dataclasses import dataclass

import discord

from .image_renderer import render_rankboard_image

_MONTHLY_BACKGROUND = (221, 236, 246)
_MONTHLY_HEADER = (191, 214, 238)
_MONTHLY_XP_BAR_FILL = (142, 190, 234)

_TOTAL_BACKGROUND = (226, 244, 232)
_TOTAL_HEADER = (196, 230, 206)
_TOTAL_XP_BAR_FILL = (140, 204, 162)


@dataclass
class RenderedHostRankboard:
    monthly_file: discord.File
    total_file: discord.File
    temp_paths: list[str]

    def cleanup(self) -> None:
        for path in self.temp_paths:
            try:
                os.remove(path)
            except OSError:
                pass


async def render_host_rankboard(
    monthly_entries: list[dict], total_entries: list[dict]
) -> RenderedHostRankboard:
    monthly_path = _make_temp_path("host_monthly_board")
    total_path = _make_temp_path("host_total_board")

    render_rankboard_image(
        monthly_entries,
        monthly_path,
        title="月間 部屋主ランキングTOP20",
        background=_MONTHLY_BACKGROUND,
        header_fill=_MONTHLY_HEADER,
        xp_bar_fill=_MONTHLY_XP_BAR_FILL,
    )
    render_rankboard_image(
        total_entries,
        total_path,
        title="累計 部屋主ランキングTOP20",
        background=_TOTAL_BACKGROUND,
        header_fill=_TOTAL_HEADER,
        xp_bar_fill=_TOTAL_XP_BAR_FILL,
    )

    return RenderedHostRankboard(
        monthly_file=discord.File(monthly_path, filename="host_monthly_board.png"),
        total_file=discord.File(total_path, filename="host_total_board.png"),
        temp_paths=[monthly_path, total_path],
    )


def _make_temp_path(prefix: str) -> str:
    return os.path.join(tempfile.gettempdir(), f"cookieleveling_{prefix}.png")
