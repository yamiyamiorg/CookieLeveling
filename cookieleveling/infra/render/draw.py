from __future__ import annotations

from datetime import datetime
from typing import Iterable
from zoneinfo import ZoneInfo

from PIL import Image, ImageDraw, ImageFilter, ImageFont, ImageOps

from cookieleveling.rendering.name_tokens import NameToken

from .constants import (
    _AVATAR_PLACEHOLDER,
    _CANVAS_SIZE,
    _CARD_BORDER,
    _CARD_FILL,
    _CARD_RADIUS,
    _EMOJI_PLACEHOLDER,
    _EMPTY_SLOT_NAME,
    _HEADER_HEIGHT,
    _LEVEL_TEXT,
    _NAME_LEVEL_GAP,
    _PANEL_FILL,
    _PANEL_RADIUS,
    _PANEL_SHADOW,
    _ROW_SHADOW_FAR,
    _ROW_SHADOW_NEAR,
    _TEXT,
    _TEXT_SHADOW,
    _TEXT_SUB,
    _XP_BAR_BG,
    _XP_BAR_BOTTOM_MARGIN,
    _XP_BAR_HEIGHT,
    _XP_BAR_SIDE_PADDING,
)
from .fonts import center_text_y, text_width


def circle_mask(size: int) -> Image.Image:
    mask = Image.new("L", (size, size), 0)
    draw = ImageDraw.Draw(mask)
    draw.ellipse((0, 0, size - 1, size - 1), fill=255)
    return mask


def circle_placeholder(size: int) -> Image.Image:
    placeholder = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    draw = ImageDraw.Draw(placeholder)
    draw.ellipse((0, 0, size - 1, size - 1), fill=_AVATAR_PLACEHOLDER)
    return placeholder


def emoji_placeholder(size: int) -> Image.Image:
    placeholder = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    draw = ImageDraw.Draw(placeholder)
    draw.rectangle((0, 0, size - 1, size - 1), fill=_EMOJI_PLACEHOLDER)
    return placeholder


def format_jst_timestamp() -> str:
    now = datetime.now(ZoneInfo("Asia/Tokyo"))
    return now.strftime("%Y/%m/%d %H:%M JST")


def format_number(value: int | None) -> str:
    if value is None:
        return _EMPTY_SLOT_NAME
    return f"{int(value):,}"


def draw_header_band(
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
    title_y = center_text_y(draw, top, _HEADER_HEIGHT, title_font)
    draw_text_with_shadow(draw, (left + 12, title_y), title, font=title_font, fill=_TEXT)

    meta_text = f"更新 {timestamp}"
    meta_width = text_width(draw, meta_text, meta_font)
    meta_x = left + width - 12 - meta_width
    meta_y = center_text_y(draw, top, _HEADER_HEIGHT, meta_font)
    draw_text_with_shadow(draw, (meta_x, meta_y), meta_text, font=meta_font, fill=_TEXT_SUB)


def draw_text_aligned(
    draw: ImageDraw.ImageDraw,
    text: str,
    col: dict,
    row_top: int,
    row_height: int,
    font: ImageFont.ImageFont,
) -> None:
    text_y = center_text_y(draw, row_top, row_height, font)
    padding = 8
    if col["align"] == "right":
        x_right = col["x_right"] - padding
        width = text_width(draw, text, font)
        draw_text_with_shadow(draw, (x_right - width, text_y), text, font=font, fill=_TEXT)
        return
    if col["align"] == "center":
        width = text_width(draw, text, font)
        x = col["x"] + (col["width"] - width) / 2
        draw_text_with_shadow(draw, (x, text_y), text, font=font, fill=_TEXT)
        return
    draw_text_with_shadow(draw, (col["x"] + padding, text_y), text, font=font, fill=_TEXT)


def draw_row_card(image: Image.Image, rect: tuple[int, int, int, int]) -> None:
    shadow_layer = Image.new("RGBA", _CANVAS_SIZE, (0, 0, 0, 0))
    shadow_draw = ImageDraw.Draw(shadow_layer)
    near_rect = (rect[0], rect[1] + 3, rect[2], rect[3] + 3)
    shadow_draw.rounded_rectangle(near_rect, radius=_CARD_RADIUS, fill=_ROW_SHADOW_NEAR)
    shadow_layer = shadow_layer.filter(ImageFilter.GaussianBlur(8))
    image.alpha_composite(shadow_layer)

    shadow_layer = Image.new("RGBA", _CANVAS_SIZE, (0, 0, 0, 0))
    shadow_draw = ImageDraw.Draw(shadow_layer)
    far_rect = (rect[0], rect[1] + 10, rect[2], rect[3] + 10)
    shadow_draw.rounded_rectangle(far_rect, radius=_CARD_RADIUS, fill=_ROW_SHADOW_FAR)
    shadow_layer = shadow_layer.filter(ImageFilter.GaussianBlur(20))
    image.alpha_composite(shadow_layer)

    card_layer = Image.new("RGBA", _CANVAS_SIZE, (0, 0, 0, 0))
    card_draw = ImageDraw.Draw(card_layer)
    card_draw.rounded_rectangle(
        rect, radius=_CARD_RADIUS, fill=_CARD_FILL, outline=_CARD_BORDER, width=1
    )
    image.alpha_composite(card_layer)


def draw_panel_frame(image: Image.Image, rect: tuple[int, int, int, int]) -> None:
    shadow_layer = Image.new("RGBA", _CANVAS_SIZE, (0, 0, 0, 0))
    shadow_draw = ImageDraw.Draw(shadow_layer)
    shadow_rect = (rect[0], rect[1] + 3, rect[2], rect[3] + 3)
    shadow_draw.rounded_rectangle(shadow_rect, radius=_PANEL_RADIUS, fill=_PANEL_SHADOW)
    shadow_layer = shadow_layer.filter(ImageFilter.GaussianBlur(10))
    image.alpha_composite(shadow_layer)

    panel_layer = Image.new("RGBA", _CANVAS_SIZE, (0, 0, 0, 0))
    panel_draw = ImageDraw.Draw(panel_layer)
    panel_draw.rounded_rectangle(rect, radius=_PANEL_RADIUS, fill=(*_PANEL_FILL, 255))
    image.alpha_composite(panel_layer)


def draw_text_with_shadow(
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


def draw_name_tokens(
    image: Image.Image,
    draw: ImageDraw.ImageDraw,
    tokens: Iterable[NameToken],
    x: float,
    y: float,
    font: ImageFont.ImageFont,
    line_height: int,
    emoji_size: int,
    emoji_placeholder_image: Image.Image,
) -> float:
    text_y = center_text_y(draw, y, line_height, font)
    emoji_y = y + (line_height - emoji_size) // 2
    cursor_x = x
    for token in tokens:
        if token.kind == "text" and token.text:
            draw_text_with_shadow(draw, (cursor_x, text_y), token.text, font=font, fill=_TEXT)
            cursor_x += text_width(draw, token.text, font)
            continue
        emoji_image = (
            token.image if isinstance(token.image, Image.Image) else None
        ) or emoji_placeholder_image
        if emoji_image.size != (emoji_size, emoji_size):
            emoji_image = ImageOps.fit(emoji_image, (emoji_size, emoji_size), method=Image.LANCZOS)
        image.paste(emoji_image, (int(cursor_x), int(emoji_y)), emoji_image)
        cursor_x += emoji_size + 2
    return cursor_x


def draw_name_level_block(
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
    emoji_placeholder_image: Image.Image,
    fit_tokens_to_width,
) -> None:
    level_text = f"Lv{format_number(level_value)}"
    if level_value is not None:
        level_text = f"Lv {format_number(level_value)}"
    level_width = text_width(draw, level_text, level_font)
    padding = 8
    max_block_width = col["width"] - padding * 2
    max_name_width = max(0, max_block_width - level_width - _NAME_LEVEL_GAP)
    name_x = col["x"] + padding
    name_tokens = fit_tokens_to_width(draw, tokens, name_font, max_name_width, emoji_size)
    name_end_x = draw_name_tokens(
        image,
        draw,
        name_tokens,
        name_x,
        row_top,
        name_font,
        row_height,
        emoji_size,
        emoji_placeholder_image,
    )
    level_x = min(
        name_end_x + _NAME_LEVEL_GAP,
        col["x"] + padding + max_name_width + _NAME_LEVEL_GAP,
    )
    level_y = center_text_y(draw, row_top, row_height, level_font)
    draw_text_with_shadow(draw, (level_x, level_y), level_text, font=level_font, fill=_LEVEL_TEXT)


def draw_xp_bar(
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
    draw.rounded_rectangle((bar_left, bar_top, bar_right, bar_bottom), radius=radius, fill=_XP_BAR_BG)
    fill_width = int(bar_width * progress)
    if fill_width <= 0:
        return
    fill_right = bar_left + fill_width
    fill_radius = int(min(radius, fill_width / 2))
    fill_color = bar_fill if len(bar_fill) == 4 else (*bar_fill, 255)
    draw.rounded_rectangle((bar_left, bar_top, fill_right, bar_bottom), radius=fill_radius, fill=fill_color)
