from __future__ import annotations

import os
from datetime import datetime
from typing import Iterable
from zoneinfo import ZoneInfo

from PIL import Image, ImageDraw, ImageFont, ImageOps, ImageFilter

from cookieleveling.rendering.name_tokens import (
    NameToken,
    tokenize_display_name,
    truncate_tokens,
)

_CANVAS_SIZE = (1200, 1400)
_OUTER_MARGIN = 16
_PANEL_GAP = 16
_PANEL_PADDING = 16
_HEADER_HEIGHT = 50
_TABLE_TOP_GAP = 8
_ROW_HEIGHT = 120
_AVATAR_SIZE = 76
_ROWS_PER_COLUMN = 10
_COLUMNS_PER_PANEL = 2

_BACKGROUND = (255, 243, 247)
_PANEL_FILL = (255, 255, 255)
_HEADER_PINK = (255, 192, 203)
_CARD_FILL = (*_PANEL_FILL, 255)
_PANEL_SHADOW = (0, 0, 0, 32)
_ROW_SHADOW_NEAR = (0, 0, 0, 36)
_ROW_SHADOW_FAR = (0, 0, 0, 18)
_CARD_BORDER = (240, 216, 226, 255)
_LEVEL_TEXT = (43, 43, 43, 255)
_TEXT = (43, 43, 43, 255)
_TEXT_SUB = (107, 107, 107, 255)
_TEXT_SHADOW = (0, 0, 0, 0)
_AVATAR_PLACEHOLDER = (217, 217, 217, 255)
_EMOJI_PLACEHOLDER = (217, 217, 217, 255)
_XP_BAR_FILL = (255, 192, 203, 255)
_XP_BAR_BG = (228, 228, 228, 255)

_FONT_PATH = "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc"
_BOLD_FONT_PATHS = (
    "/usr/share/fonts/opentype/noto/NotoSansCJK-Bold.ttc",
    "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
)
_MONO_FONT_PATHS = (
    "/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf",
    "/usr/share/fonts/truetype/noto/NotoSansMono-Regular.ttf",
    "/usr/share/fonts/opentype/noto/NotoSansMono-Regular.ttc",
)
_TITLE_FONT_SIZE = 30
_HEADER_FONT_SIZE = 16
_NAME_FONT_SIZE = 30
_RANK_FONT_SIZE = 30
_LEVEL_FONT_SIZE = 44
_EMPTY_SLOT_NAME = "—"
_CARD_RADIUS = 18
_PANEL_RADIUS = 24
_ROW_CARD_GAP = 16
_NAME_LEVEL_GAP = 8
_ROW_GAP_MIN = 16
_XP_BAR_HEIGHT = 10
_XP_BAR_BOTTOM_MARGIN = 12
_XP_BAR_SIDE_PADDING = 8


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
    header_fill = header_fill or _HEADER_PINK
    xp_bar_fill = xp_bar_fill or _XP_BAR_FILL
    image = Image.new("RGBA", _CANVAS_SIZE, (*base, 255))
    draw = ImageDraw.Draw(image)
    title_font = _load_font(_TITLE_FONT_SIZE)
    header_font = _load_font(_HEADER_FONT_SIZE)
    name_font = _load_font(_NAME_FONT_SIZE)
    rank_font = _load_font(_RANK_FONT_SIZE, prefer_bold=True)
    level_font = _load_font(_LEVEL_FONT_SIZE, prefer_bold=True)

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
        header_fill=header_fill,
        xp_bar_fill=xp_bar_fill,
    )
    image.convert("RGB").save(output_path, format="PNG")


def _draw_panel(
    image: Image.Image,
    draw: ImageDraw.ImageDraw,
    panel_rect: tuple[int, int, int, int],
    entries: Iterable[dict],
    name_font: ImageFont.ImageFont,
    rank_font: ImageFont.ImageFont,
    level_font: ImageFont.ImageFont,
    *,
    title_font: ImageFont.ImageFont,
    header_font: ImageFont.ImageFont,
    title: str,
    header_fill: tuple[int, int, int],
    xp_bar_fill: tuple[int, int, int] | tuple[int, int, int, int],
) -> None:
    _draw_panel_frame(image, panel_rect)
    entries = _pad_entries(list(entries), total=_ROWS_PER_COLUMN * _COLUMNS_PER_PANEL)
    content_left = panel_rect[0] + _PANEL_PADDING
    content_top = panel_rect[1] + _PANEL_PADDING
    content_bottom = panel_rect[3] - _PANEL_PADDING
    content_width = panel_rect[2] - panel_rect[0] - _PANEL_PADDING * 2
    table_top = content_top + _HEADER_HEIGHT + _TABLE_TOP_GAP
    available_height = content_bottom - table_top
    row_height = float(_ROW_HEIGHT)
    row_gap = (available_height - _ROWS_PER_COLUMN * row_height) / (
        _ROWS_PER_COLUMN - 1
    )
    if row_gap < _ROW_GAP_MIN:
        row_gap = float(_ROW_GAP_MIN)
        row_height = (available_height - (_ROWS_PER_COLUMN - 1) * row_gap) / (
            _ROWS_PER_COLUMN
        )
    column_gap = float(_PANEL_GAP)
    column_width = int(
        (content_width - column_gap * (_COLUMNS_PER_PANEL - 1)) / _COLUMNS_PER_PANEL
    )
    column_sets = [
        _position_columns(
            _panel_columns(column_width),
            left=int(content_left + idx * (column_width + column_gap)),
        )
        for idx in range(_COLUMNS_PER_PANEL)
    ]

    _draw_header_band(
        draw,
        title_font,
        header_font,
        title=title,
        timestamp=_format_jst_timestamp(),
        fill=header_fill,
        left=content_left,
        top=content_top,
        width=content_width,
    )

    mask = _circle_mask(_AVATAR_SIZE)
    placeholder = _circle_placeholder(_AVATAR_SIZE)
    emoji_size = min(40, int(row_height) - 18)
    emoji_placeholder = _emoji_placeholder(emoji_size)

    for idx, entry in enumerate(entries, start=1):
        col_idx = (idx - 1) // _ROWS_PER_COLUMN
        row_idx = (idx - 1) % _ROWS_PER_COLUMN
        row_top = table_top + row_idx * (row_height + row_gap)
        left_x = int(content_left + col_idx * (column_width + column_gap))
        card_rect = _row_card_rect(left_x, column_width, row_top, row_height)
        _draw_row_card(image, card_rect)

        name_tokens = _resolve_name_tokens(entry)
        avatar = entry.get("avatar")
        rank_text = str(idx)
        level_value = entry.get("level")
        progress = entry.get("xp_progress")
        column_set = column_sets[col_idx]
        for col in column_set:
            key = col["key"]
            if key == "rank":
                _draw_text_aligned(
                    draw,
                    rank_text,
                    col,
                    card_rect[1],
                    card_rect[3] - card_rect[1],
                    rank_font,
                )
                continue
            if key == "avatar":
                avatar_x = int(col["x"] + (col["width"] - _AVATAR_SIZE) / 2)
                avatar_y = int(
                    card_rect[1] + (card_rect[3] - card_rect[1] - _AVATAR_SIZE) / 2
                )
                if avatar is None:
                    image.paste(placeholder, (avatar_x, avatar_y), mask)
                else:
                    avatar_image = ImageOps.fit(
                        avatar.convert("RGBA"), (_AVATAR_SIZE, _AVATAR_SIZE)
                    )
                    image.paste(avatar_image, (avatar_x, avatar_y), mask)
                continue
            if key == "name":
                _draw_name_level_block(
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
                    emoji_placeholder,
                )
                _draw_xp_bar(draw, col, card_rect, progress, xp_bar_fill)
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


def _draw_name_tokens(
    image: Image.Image,
    draw: ImageDraw.ImageDraw,
    tokens: Iterable[NameToken],
    x: float,
    y: float,
    font: ImageFont.ImageFont,
    line_height: int,
    emoji_size: int,
    emoji_placeholder: Image.Image,
) -> None:
    text_y = _center_text_y(draw, y, line_height, font)
    emoji_y = y + (line_height - emoji_size) // 2
    cursor_x = x
    for token in tokens:
        if token.kind == "text" and token.text:
            _draw_text_with_shadow(
                draw, (cursor_x, text_y), token.text, font=font, fill=_TEXT
            )
            cursor_x += _text_width(draw, token.text, font)
            continue
        emoji_image = (
            token.image if isinstance(token.image, Image.Image) else None
        ) or emoji_placeholder
        if emoji_image.size != (emoji_size, emoji_size):
            emoji_image = ImageOps.fit(
                emoji_image, (emoji_size, emoji_size), method=Image.LANCZOS
            )
        image.paste(emoji_image, (int(cursor_x), int(emoji_y)), emoji_image)
        cursor_x += emoji_size + 2
    return cursor_x


def _circle_mask(size: int) -> Image.Image:
    mask = Image.new("L", (size, size), 0)
    draw = ImageDraw.Draw(mask)
    draw.ellipse((0, 0, size - 1, size - 1), fill=255)
    return mask


def _circle_placeholder(size: int) -> Image.Image:
    placeholder = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    draw = ImageDraw.Draw(placeholder)
    draw.ellipse((0, 0, size - 1, size - 1), fill=_AVATAR_PLACEHOLDER)
    return placeholder


def _emoji_placeholder(size: int) -> Image.Image:
    placeholder = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    draw = ImageDraw.Draw(placeholder)
    draw.rectangle((0, 0, size - 1, size - 1), fill=_EMOJI_PLACEHOLDER)
    return placeholder


def _load_font_with_fallbacks(
    paths: Iterable[str], size: int, *, index: int = 0
) -> ImageFont.ImageFont:
    for path in paths:
        if os.path.exists(path):
            return ImageFont.truetype(path, size=size, index=index)
    return ImageFont.load_default()


def _load_font(
    size: int, *, prefer_mono: bool = False, prefer_bold: bool = False
) -> ImageFont.ImageFont:
    if prefer_mono:
        for path in _MONO_FONT_PATHS:
            if os.path.exists(path):
                return ImageFont.truetype(path, size=size)
        return _load_font_with_fallbacks((_FONT_PATH,), size)
    if prefer_bold:
        for path in _BOLD_FONT_PATHS:
            if os.path.exists(path):
                return ImageFont.truetype(path, size=size)
        return _load_font_with_fallbacks((_FONT_PATH,), size)
    return _load_font_with_fallbacks((_FONT_PATH,), size)


def _text_width(draw: ImageDraw.ImageDraw, text: str, font: ImageFont.ImageFont) -> float:
    if hasattr(draw, "textlength"):
        return draw.textlength(text, font=font)
    return font.getlength(text)


def _text_height(
    draw: ImageDraw.ImageDraw, text: str, font: ImageFont.ImageFont
) -> float:
    bbox = draw.textbbox((0, 0), text, font=font)
    return bbox[3] - bbox[1]


def _center_text_y(
    draw: ImageDraw.ImageDraw, y: int, height: int, font: ImageFont.ImageFont
) -> int:
    bbox = draw.textbbox((0, 0), "Ag", font=font)
    text_height = bbox[3] - bbox[1]
    return int(y + (height - text_height) / 2 - bbox[1])


def _format_jst_timestamp() -> str:
    now = datetime.now(ZoneInfo("Asia/Tokyo"))
    return now.strftime("%Y/%m/%d %H:%M JST")


def _format_number(value: int | None) -> str:
    if value is None:
        return _EMPTY_SLOT_NAME
    return f"{int(value):,}"


def _draw_header_band(
    draw: ImageDraw.ImageDraw,
    title_font: ImageFont.ImageFont,
    meta_font: ImageFont.ImageFont,
    *,
    title: str,
    timestamp: str,
    fill: tuple[int, int, int],
    left: int,
    top: int,
    width: int,
) -> None:
    draw.rectangle((left, top, left + width, top + _HEADER_HEIGHT), fill=fill)
    title_y = _center_text_y(draw, top, _HEADER_HEIGHT, title_font)
    _draw_text_with_shadow(
        draw, (left + 12, title_y), title, font=title_font, fill=_TEXT
    )

    meta_text = f"更新 {timestamp}"
    meta_width = _text_width(draw, meta_text, meta_font)
    meta_x = left + width - 12 - meta_width
    meta_y = _center_text_y(draw, top, _HEADER_HEIGHT, meta_font)
    _draw_text_with_shadow(
        draw, (meta_x, meta_y), meta_text, font=meta_font, fill=_TEXT_SUB
    )


def _panel_columns(content_width: int) -> list[dict]:
    rank_width = 56
    avatar_width = 88
    name_width = max(0, content_width - rank_width - avatar_width)
    return [
        {"key": "rank", "label": "Rank", "width": rank_width, "align": "right"},
        {"key": "avatar", "label": "Avatar", "width": avatar_width, "align": "center"},
        {"key": "name", "label": "Name", "width": name_width, "align": "left"},
    ]


def _position_columns(columns: list[dict], *, left: int) -> list[dict]:
    positioned = []
    cursor = left
    for col in columns:
        width = col["width"]
        positioned.append({**col, "x": cursor, "x_right": cursor + width})
        cursor += width
    return positioned


def _draw_text_aligned(
    draw: ImageDraw.ImageDraw,
    text: str,
    col: dict,
    row_top: int,
    row_height: int,
    font: ImageFont.ImageFont,
) -> None:
    text_y = _center_text_y(draw, row_top, row_height, font)
    padding = 8
    if col["align"] == "right":
        x_right = col["x_right"] - padding
        text_width = _text_width(draw, text, font)
        _draw_text_with_shadow(
            draw, (x_right - text_width, text_y), text, font=font, fill=_TEXT
        )
        return
    if col["align"] == "center":
        text_width = _text_width(draw, text, font)
        x = col["x"] + (col["width"] - text_width) / 2
        _draw_text_with_shadow(draw, (x, text_y), text, font=font, fill=_TEXT)
        return
    _draw_text_with_shadow(
        draw, (col["x"] + padding, text_y), text, font=font, fill=_TEXT
    )


def _row_card_rect(
    left_x: int, column_width: int, row_top: int, row_height: int
) -> tuple[int, int, int, int]:
    inset = _ROW_CARD_GAP // 2
    top = row_top + inset
    bottom = row_top + row_height - inset
    return (left_x, top, left_x + column_width, bottom)


def _draw_row_card(image: Image.Image, rect: tuple[int, int, int, int]) -> None:
    shadow_layer = Image.new("RGBA", _CANVAS_SIZE, (0, 0, 0, 0))
    shadow_draw = ImageDraw.Draw(shadow_layer)
    near_rect = (rect[0], rect[1] + 3, rect[2], rect[3] + 3)
    shadow_draw.rounded_rectangle(
        near_rect, radius=_CARD_RADIUS, fill=_ROW_SHADOW_NEAR
    )
    shadow_layer = shadow_layer.filter(ImageFilter.GaussianBlur(8))
    image.alpha_composite(shadow_layer)

    shadow_layer = Image.new("RGBA", _CANVAS_SIZE, (0, 0, 0, 0))
    shadow_draw = ImageDraw.Draw(shadow_layer)
    far_rect = (rect[0], rect[1] + 10, rect[2], rect[3] + 10)
    shadow_draw.rounded_rectangle(
        far_rect, radius=_CARD_RADIUS, fill=_ROW_SHADOW_FAR
    )
    shadow_layer = shadow_layer.filter(ImageFilter.GaussianBlur(20))
    image.alpha_composite(shadow_layer)

    card_layer = Image.new("RGBA", _CANVAS_SIZE, (0, 0, 0, 0))
    card_draw = ImageDraw.Draw(card_layer)
    card_draw.rounded_rectangle(
        rect, radius=_CARD_RADIUS, fill=_CARD_FILL, outline=_CARD_BORDER, width=1
    )
    image.alpha_composite(card_layer)


def _draw_name_level_block(
    image: Image.Image,
    draw: ImageDraw.ImageDraw,
    tokens: Iterable[NameToken],
    col: dict,
    row_top: int,
    row_height: int,
    level_value: int | None,
    name_font: ImageFont.ImageFont,
    level_font: ImageFont.ImageFont,
    emoji_size: int,
    emoji_placeholder: Image.Image,
) -> None:
    level_text = f"Lv{_format_number(level_value)}"
    if level_value is not None:
        level_text = f"Lv {_format_number(level_value)}"
    level_width = _text_width(draw, level_text, level_font)
    padding = 8
    max_block_width = col["width"] - padding * 2
    max_name_width = max(0, max_block_width - level_width - _NAME_LEVEL_GAP)
    name_x = col["x"] + padding
    name_tokens = _fit_tokens_to_width(draw, tokens, name_font, max_name_width, emoji_size)
    name_end_x = _draw_name_tokens(
        image,
        draw,
        name_tokens,
        name_x,
        row_top,
        name_font,
        row_height,
        emoji_size,
        emoji_placeholder,
    )
    level_x = min(name_end_x + _NAME_LEVEL_GAP, col["x"] + padding + max_name_width + _NAME_LEVEL_GAP)
    level_y = _center_text_y(draw, row_top, row_height, level_font)
    _draw_text_with_shadow(
        draw,
        (level_x, level_y),
        level_text,
        font=level_font,
        fill=_LEVEL_TEXT,
    )


def _draw_xp_bar(
    draw: ImageDraw.ImageDraw,
    col: dict,
    card_rect: tuple[int, int, int, int],
    progress: float | None,
    bar_fill: tuple[int, int, int] | tuple[int, int, int, int],
) -> None:
    if progress is None:
        return
    progress = max(0.0, min(1.0, float(progress)))
    bar_left = col["x"] + _XP_BAR_SIDE_PADDING
    bar_right = col["x_right"] - _XP_BAR_SIDE_PADDING
    bar_width = int(bar_right - bar_left)
    if bar_width <= 0:
        return
    bar_top = card_rect[3] - _XP_BAR_BOTTOM_MARGIN - _XP_BAR_HEIGHT
    bar_bottom = bar_top + _XP_BAR_HEIGHT
    radius = _XP_BAR_HEIGHT // 2
    draw.rounded_rectangle(
        (bar_left, bar_top, bar_right, bar_bottom),
        radius=radius,
        fill=_XP_BAR_BG,
    )
    fill_width = int(bar_width * progress)
    if fill_width <= 0:
        return
    fill_right = bar_left + fill_width
    fill_radius = int(min(radius, fill_width / 2))
    fill_color = bar_fill if len(bar_fill) == 4 else (*bar_fill, 255)
    draw.rounded_rectangle(
        (bar_left, bar_top, fill_right, bar_bottom),
        radius=fill_radius,
        fill=fill_color,
    )


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


def _draw_panel_frame(image: Image.Image, rect: tuple[int, int, int, int]) -> None:
    shadow_layer = Image.new("RGBA", _CANVAS_SIZE, (0, 0, 0, 0))
    shadow_draw = ImageDraw.Draw(shadow_layer)
    shadow_rect = (rect[0], rect[1] + 3, rect[2], rect[3] + 3)
    shadow_draw.rounded_rectangle(
        shadow_rect, radius=_PANEL_RADIUS, fill=_PANEL_SHADOW
    )
    shadow_layer = shadow_layer.filter(ImageFilter.GaussianBlur(10))
    image.alpha_composite(shadow_layer)

    panel_layer = Image.new("RGBA", _CANVAS_SIZE, (0, 0, 0, 0))
    panel_draw = ImageDraw.Draw(panel_layer)
    panel_draw.rounded_rectangle(rect, radius=_PANEL_RADIUS, fill=(*_PANEL_FILL, 255))
    image.alpha_composite(panel_layer)


def _draw_text_with_shadow(
    draw: ImageDraw.ImageDraw,
    position: tuple[float, float],
    text: str,
    *,
    font: ImageFont.ImageFont,
    fill: tuple[int, int, int, int],
) -> None:
    shadow_x = position[0] + 1
    shadow_y = position[1] + 1
    draw.text((shadow_x, shadow_y), text, fill=_TEXT_SHADOW, font=font)
    draw.text(position, text, fill=fill, font=font)


def _fit_tokens_to_width(
    draw: ImageDraw.ImageDraw,
    tokens: Iterable[NameToken],
    font: ImageFont.ImageFont,
    max_width: float,
    emoji_size: int,
) -> list[NameToken]:
    tokens = list(tokens)
    if max_width <= 0:
        return []
    clipped: list[NameToken] = []
    width = 0.0
    for token in tokens:
        if token.kind == "text" and token.text:
            remaining = max_width - width
            if remaining <= 0:
                break
            text = token.text
            text_width = _text_width(draw, text, font)
            if text_width <= remaining:
                clipped.append(token)
                width += text_width
            else:
                for idx in range(len(text), 0, -1):
                    slice_width = _text_width(draw, text[:idx], font)
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
