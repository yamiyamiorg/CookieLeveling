from __future__ import annotations

from typing import Iterable

from PIL import Image, ImageDraw, ImageOps

from cookieleveling.rendering.name_tokens import NameToken, tokenize_display_name, truncate_tokens

from .constants import (
    _AVATAR_SIZE,
    _BACKGROUND,
    _CANVAS_SIZE,
    _COLUMNS_PER_PANEL,
    _EMPTY_SLOT_NAME,
    _HEADER_FONT_SIZE,
    _HEADER_HEIGHT,
    _HEADER_PINK,
    _NAME_FONT_SIZE,
    _PANEL_GAP,
    _PANEL_PADDING,
    _RANK_FONT_SIZE,
    _ROW_GAP_MIN,
    _ROW_HEIGHT,
    _ROWS_PER_COLUMN,
    _TABLE_TOP_GAP,
    _TITLE_FONT_SIZE,
    _LEVEL_FONT_SIZE,
    _OUTER_MARGIN,
    _XP_BAR_FILL,
)
from .draw import (
    circle_mask,
    circle_placeholder,
    draw_header_band,
    draw_name_level_block,
    draw_panel_frame,
    draw_row_card,
    draw_text_aligned,
    draw_xp_bar,
    emoji_placeholder,
    format_jst_timestamp,
)
from .fonts import load_font, text_width
from .layout import panel_columns, position_columns, row_card_rect


def render_rankboard_image(
    entries: Iterable[dict],
    output_path: str,
    *,
    title: str,
    background: tuple[int, int, int] | None = None,
    header_fill: tuple[int, int, int] | None = None,
    xp_bar_fill: tuple[int, int, int] | tuple[int, int, int, int] | None = None,
) -> None:
    base = background or _BACKGROUND
    resolved_header_fill = header_fill or _HEADER_PINK
    resolved_xp_bar_fill = xp_bar_fill or _XP_BAR_FILL
    image = Image.new("RGBA", _CANVAS_SIZE, (*base, 255))
    draw = ImageDraw.Draw(image)
    title_font = load_font(_TITLE_FONT_SIZE)
    header_font = load_font(_HEADER_FONT_SIZE)
    name_font = load_font(_NAME_FONT_SIZE)
    rank_font = load_font(_RANK_FONT_SIZE, prefer_bold=True)
    level_font = load_font(_LEVEL_FONT_SIZE, prefer_bold=True)

    panel_rect = (
        _OUTER_MARGIN,
        _OUTER_MARGIN,
        _CANVAS_SIZE[0] - _OUTER_MARGIN,
        _CANVAS_SIZE[1] - _OUTER_MARGIN,
    )
    _draw_panel(
        image,
        draw,
        panel_rect,
        entries,
        name_font,
        rank_font,
        level_font,
        title_font=title_font,
        header_font=header_font,
        title=title,
        header_fill=resolved_header_fill,
        xp_bar_fill=resolved_xp_bar_fill,
    )
    image.convert("RGB").save(output_path, format="PNG")


def _draw_panel(
    image: Image.Image,
    draw: ImageDraw.ImageDraw,
    panel_rect: tuple[int, int, int, int],
    entries: Iterable[dict],
    name_font,
    rank_font,
    level_font,
    *,
    title_font,
    header_font,
    title: str,
    header_fill: tuple[int, int, int],
    xp_bar_fill: tuple[int, int, int] | tuple[int, int, int, int],
) -> None:
    draw_panel_frame(image, panel_rect)
    entries = _pad_entries(list(entries), total=_ROWS_PER_COLUMN * _COLUMNS_PER_PANEL)
    content_left = panel_rect[0] + _PANEL_PADDING
    content_top = panel_rect[1] + _PANEL_PADDING
    content_bottom = panel_rect[3] - _PANEL_PADDING
    content_width = panel_rect[2] - panel_rect[0] - _PANEL_PADDING * 2
    table_top = content_top + _HEADER_HEIGHT + _TABLE_TOP_GAP
    available_height = content_bottom - table_top
    row_height = float(_ROW_HEIGHT)
    row_gap = (available_height - _ROWS_PER_COLUMN * row_height) / (_ROWS_PER_COLUMN - 1)
    if row_gap < _ROW_GAP_MIN:
        row_gap = float(_ROW_GAP_MIN)
        row_height = (available_height - (_ROWS_PER_COLUMN - 1) * row_gap) / (_ROWS_PER_COLUMN)
    column_gap = float(_PANEL_GAP)
    column_width = int((content_width - column_gap * (_COLUMNS_PER_PANEL - 1)) / _COLUMNS_PER_PANEL)
    column_sets = [
        position_columns(panel_columns(column_width), left=int(content_left + idx * (column_width + column_gap)))
        for idx in range(_COLUMNS_PER_PANEL)
    ]

    draw_header_band(
        draw,
        title_font,
        header_font,
        title=title,
        timestamp=format_jst_timestamp(),
        fill=header_fill,
        left=content_left,
        top=content_top,
        width=content_width,
    )

    mask = circle_mask(_AVATAR_SIZE)
    avatar_placeholder = circle_placeholder(_AVATAR_SIZE)
    emoji_size = min(40, int(row_height) - 18)
    emoji_placeholder_image = emoji_placeholder(emoji_size)

    for idx, entry in enumerate(entries, start=1):
        col_idx = (idx - 1) // _ROWS_PER_COLUMN
        row_idx = (idx - 1) % _ROWS_PER_COLUMN
        row_top = table_top + row_idx * (row_height + row_gap)
        left_x = int(content_left + col_idx * (column_width + column_gap))
        card_rect = row_card_rect(left_x, column_width, row_top, row_height)
        draw_row_card(image, card_rect)

        name_tokens = _resolve_name_tokens(entry)
        avatar = entry.get("avatar")
        rank_text = str(idx)
        level_value = entry.get("level")
        progress = entry.get("xp_progress")
        column_set = column_sets[col_idx]
        for col in column_set:
            key = col["key"]
            if key == "rank":
                draw_text_aligned(draw, rank_text, col, card_rect[1], card_rect[3] - card_rect[1], rank_font)
                continue
            if key == "avatar":
                avatar_x = int(col["x"] + (col["width"] - _AVATAR_SIZE) / 2)
                avatar_y = int(card_rect[1] + (card_rect[3] - card_rect[1] - _AVATAR_SIZE) / 2)
                if avatar is None:
                    image.paste(avatar_placeholder, (avatar_x, avatar_y), mask)
                else:
                    avatar_image = ImageOps.fit(
                        avatar.convert("RGBA"), (_AVATAR_SIZE, _AVATAR_SIZE)
                    )
                    image.paste(avatar_image, (avatar_x, avatar_y), mask)
                continue
            if key == "name":
                draw_name_level_block(
                    image,
                    draw,
                    name_tokens,
                    col,
                    card_rect[1],
                    card_rect[3] - card_rect[1],
                    level_value,
                    name_font,
                    level_font,
                    emoji_size,
                    emoji_placeholder_image,
                    _fit_tokens_to_width,
                )
                draw_xp_bar(draw, col, card_rect, progress, xp_bar_fill)
                continue


def _resolve_name_tokens(entry: dict) -> list[NameToken]:
    tokens = entry.get("name_tokens")
    if tokens is not None:
        return tokens
    name = entry.get("name")
    if not name:
        name = _EMPTY_SLOT_NAME
    tokens = tokenize_display_name(name)
    return truncate_tokens(tokens, max_chars=16)


def _pad_entries(entries: list[dict], *, total: int) -> list[dict]:
    if len(entries) >= total:
        return entries[:total]
    filler = {
        "name": _EMPTY_SLOT_NAME,
        "name_tokens": tokenize_display_name(_EMPTY_SLOT_NAME),
        "level": None,
        "xp_progress": None,
        "avatar": None,
    }
    entries.extend(filler.copy() for _ in range(total - len(entries)))
    return entries


def _fit_tokens_to_width(
    draw: ImageDraw.ImageDraw,
    tokens: Iterable[NameToken],
    font,
    max_width: float,
    emoji_size: int,
) -> list[NameToken]:
    token_list = list(tokens)
    if max_width <= 0:
        return []
    clipped: list[NameToken] = []
    width = 0.0
    for token in token_list:
        if token.kind == "text" and token.text:
            remaining = max_width - width
            if remaining <= 0:
                break
            text = token.text
            width_value = text_width(draw, text, font)
            if width_value <= remaining:
                clipped.append(token)
                width += width_value
            else:
                for idx in range(len(text), 0, -1):
                    slice_width = text_width(draw, text[:idx], font)
                    if slice_width <= remaining:
                        clipped.append(NameToken(kind="text", text=text[:idx]))
                        width += slice_width
                        break
                break
            continue
        token_width = emoji_size + 2
        if width + token_width > max_width:
            break
        clipped.append(token)
        width += token_width
    return clipped
