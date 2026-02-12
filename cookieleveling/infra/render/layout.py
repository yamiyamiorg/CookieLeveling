from __future__ import annotations

from PIL import ImageDraw, ImageFont

from .constants import _NAME_LEVEL_GAP, _ROW_CARD_GAP
from .fonts import text_width


def panel_columns(content_width: int) -> list[dict]:
    rank_width = 56
    avatar_width = 88
    name_width = max(0, content_width - rank_width - avatar_width)
    return [
        {"key": "rank", "label": "Rank", "width": rank_width, "align": "right"},
        {"key": "avatar", "label": "Avatar", "width": avatar_width, "align": "center"},
        {"key": "name", "label": "Name", "width": name_width, "align": "left"},
    ]


def position_columns(columns: list[dict], *, left: int) -> list[dict]:
    positioned = []
    cursor = left
    for col in columns:
        width = col["width"]
        positioned.append({**col, "x": cursor, "x_right": cursor + width})
        cursor += width
    return positioned


def row_card_rect(
    left_x: int, column_width: int, row_top: int, row_height: int
) -> tuple[int, int, int, int]:
    inset = _ROW_CARD_GAP // 2
    top = row_top + inset
    bottom = row_top + row_height - inset
    return (left_x, top, left_x + column_width, bottom)


def compute_level_block(
    draw: ImageDraw.ImageDraw,
    col: dict,
    level_text: str,
    level_font: ImageFont.ImageFont,
) -> tuple[float, float, float]:
    level_width = text_width(draw, level_text, level_font)
    padding = 8
    max_block_width = col["width"] - padding * 2
    max_name_width = max(0, max_block_width - level_width - _NAME_LEVEL_GAP)
    name_x = col["x"] + padding
    return level_width, max_name_width, name_x
