from __future__ import annotations

import io
import os
from datetime import datetime
from typing import Iterable
from zoneinfo import ZoneInfo

from PIL import Image, ImageDraw, ImageFont, ImageFilter, ImageOps

from .display_name_tokens import NameToken

_CANVAS_SIZE = (900, 360)
_OUTER_MARGIN = 20
_PANEL_PADDING = 20
_HEADER_HEIGHT = 56
_AVATAR_SIZE = 96
_NAME_LINE_HEIGHT = 36
_STAT_LINE_HEIGHT = 24
_STAT_GAP = 16
_BAR_HEIGHT = 12
_BAR_GAP = 6
_CARD_RADIUS = 24

_BACKGROUND = (255, 243, 247)
_PANEL_FILL = (255, 255, 255)
_HEADER_PINK = (255, 192, 203)
_PANEL_SHADOW = (0, 0, 0, 32)
_TEXT = (43, 43, 43, 255)
_TEXT_SUB = (107, 107, 107, 255)
_TEXT_MUTED = (120, 120, 120, 255)
_XP_BAR_FILL = (255, 192, 203, 255)
_XP_BAR_BG = (237, 237, 237, 255)
_AVATAR_PLACEHOLDER = (217, 217, 217, 255)
_EMOJI_PLACEHOLDER = (217, 217, 217, 255)

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
_TITLE_FONT_SIZE = 26
_HEADER_FONT_SIZE = 14
_NAME_FONT_SIZE = 32
_LABEL_FONT_SIZE = 20
_LEVEL_FONT_SIZE = 24
_SMALL_FONT_SIZE = 16


def render_level_card(
    *,
    name_tokens: list[NameToken],
    avatar: Image.Image | None,
    season_stats: dict,
    lifetime_stats: dict,
    optout: bool = False,
    include_timestamp: bool = True,
) -> bytes:
    image = Image.new("RGBA", _CANVAS_SIZE, (*_BACKGROUND, 255))
    draw = ImageDraw.Draw(image)
    title_font = _load_font(_TITLE_FONT_SIZE, prefer_bold=True)
    header_font = _load_font(_HEADER_FONT_SIZE)
    name_font = _load_font(_NAME_FONT_SIZE)
    label_font = _load_font(_LABEL_FONT_SIZE)
    level_font = _load_font(_LEVEL_FONT_SIZE, prefer_bold=True)
    small_font = _load_font(_SMALL_FONT_SIZE, prefer_mono=True)

    panel_rect = (
        _OUTER_MARGIN,
        _OUTER_MARGIN,
        _CANVAS_SIZE[0] - _OUTER_MARGIN,
        _CANVAS_SIZE[1] - _OUTER_MARGIN,
    )
    _draw_panel(image, panel_rect)

    content_left = panel_rect[0] + _PANEL_PADDING
    content_right = panel_rect[2] - _PANEL_PADDING
    content_top = panel_rect[1] + _PANEL_PADDING

    _draw_header(
        draw,
        title_font,
        header_font,
        left=content_left,
        top=content_top,
        right=content_right,
        include_timestamp=include_timestamp,
    )

    body_top = content_top + _HEADER_HEIGHT + 16
    avatar_x = content_left + 8
    avatar_y = body_top + 6
    _draw_avatar(image, avatar, (avatar_x, avatar_y))

    right_x = avatar_x + _AVATAR_SIZE + 20
    right_width = content_right - right_x

    name_tokens = _fit_tokens_to_width(
        draw, name_tokens, name_font, right_width, emoji_size=24
    )
    _draw_name_tokens(
        image,
        draw,
        name_tokens,
        right_x,
        body_top,
        name_font,
        _NAME_LINE_HEIGHT,
        emoji_size=24,
    )

    stats_top = body_top + _NAME_LINE_HEIGHT + 12
    stats_top = _draw_stat_block(
        draw,
        label="今期",
        stats=season_stats,
        left=right_x,
        top=stats_top,
        width=right_width,
        label_font=label_font,
        level_font=level_font,
        small_font=small_font,
    )
    stats_top = _draw_stat_block(
        draw,
        label="累計",
        stats=lifetime_stats,
        left=right_x,
        top=stats_top + _STAT_GAP,
        width=right_width,
        label_font=label_font,
        level_font=level_font,
        small_font=small_font,
    )

    if optout:
        text = "optout中（XP加算なし）"
        text_width = _text_width(draw, text, small_font)
        text_x = content_right - text_width
        text_y = panel_rect[3] - _PANEL_PADDING - _SMALL_FONT_SIZE - 2
        _draw_text(draw, (text_x, text_y), text, small_font, _TEXT_MUTED)

    output = io.BytesIO()
    image.convert("RGB").save(output, format="PNG")
    return output.getvalue()


def _draw_header(
    draw: ImageDraw.ImageDraw,
    title_font: ImageFont.ImageFont,
    header_font: ImageFont.ImageFont,
    *,
    left: int,
    top: int,
    right: int,
    include_timestamp: bool,
) -> None:
    draw.rectangle((left, top, right, top + _HEADER_HEIGHT), fill=_HEADER_PINK)
    title_y = _center_text_y(draw, top, _HEADER_HEIGHT, title_font)
    _draw_text(draw, (left + 12, title_y), "レベル", title_font, _TEXT)
    if include_timestamp:
        timestamp = _format_jst_timestamp()
        meta_text = f"更新 {timestamp}"
        meta_width = _text_width(draw, meta_text, header_font)
        meta_x = right - 12 - meta_width
        meta_y = _center_text_y(draw, top, _HEADER_HEIGHT, header_font)
        _draw_text(draw, (meta_x, meta_y), meta_text, header_font, _TEXT_SUB)


def _draw_panel(image: Image.Image, rect: tuple[int, int, int, int]) -> None:
    shadow_layer = Image.new("RGBA", _CANVAS_SIZE, (0, 0, 0, 0))
    shadow_draw = ImageDraw.Draw(shadow_layer)
    shadow_rect = (rect[0], rect[1] + 3, rect[2], rect[3] + 3)
    shadow_draw.rounded_rectangle(
        shadow_rect, radius=_CARD_RADIUS, fill=_PANEL_SHADOW
    )
    shadow_layer = shadow_layer.filter(ImageFilter.GaussianBlur(10))
    image.alpha_composite(shadow_layer)

    panel_layer = Image.new("RGBA", _CANVAS_SIZE, (0, 0, 0, 0))
    panel_draw = ImageDraw.Draw(panel_layer)
    panel_draw.rounded_rectangle(
        rect, radius=_CARD_RADIUS, fill=(*_PANEL_FILL, 255)
    )
    image.alpha_composite(panel_layer)


def _draw_avatar(
    image: Image.Image, avatar: Image.Image | None, position: tuple[int, int]
) -> None:
    mask = _circle_mask(_AVATAR_SIZE)
    placeholder = _circle_placeholder(_AVATAR_SIZE)
    if avatar is None:
        image.paste(placeholder, position, mask)
    else:
        avatar_image = ImageOps.fit(
            avatar.convert("RGBA"), (_AVATAR_SIZE, _AVATAR_SIZE)
        )
        image.paste(avatar_image, position, mask)


def _draw_stat_block(
    draw: ImageDraw.ImageDraw,
    *,
    label: str,
    stats: dict,
    left: int,
    top: int,
    width: int,
    label_font: ImageFont.ImageFont,
    level_font: ImageFont.ImageFont,
    small_font: ImageFont.ImageFont,
) -> int:
    level_text = f"Lv {_format_number(stats.get('level'))}"
    line_y = top
    label_y = _center_text_y(draw, line_y, _STAT_LINE_HEIGHT, label_font)
    _draw_text(draw, (left, label_y), label, label_font, _TEXT)
    label_width = _text_width(draw, label, label_font)
    level_y = _center_text_y(draw, line_y, _STAT_LINE_HEIGHT, level_font)
    level_x = left + label_width + 12
    _draw_text(draw, (level_x, level_y), level_text, level_font, _TEXT)

    xp_text = f"{_format_number(stats.get('xp'))} / {_format_number(stats.get('next'))}"
    xp_width = _text_width(draw, xp_text, small_font)
    xp_x = left + width - xp_width
    xp_y = _center_text_y(draw, line_y, _STAT_LINE_HEIGHT, small_font)
    _draw_text(draw, (xp_x, xp_y), xp_text, small_font, _TEXT_SUB)

    bar_top = line_y + _STAT_LINE_HEIGHT + _BAR_GAP
    bar_bottom = bar_top + _BAR_HEIGHT
    _draw_xp_bar(
        draw,
        left=left,
        right=left + width,
        top=bar_top,
        bottom=bar_bottom,
        progress=stats.get("progress", 0.0),
    )
    return bar_bottom


def _draw_xp_bar(
    draw: ImageDraw.ImageDraw,
    *,
    left: int,
    right: int,
    top: int,
    bottom: int,
    progress: float,
) -> None:
    progress = max(0.0, min(1.0, float(progress)))
    radius = int(_BAR_HEIGHT / 2)
    draw.rounded_rectangle(
        (left, top, right, bottom), radius=radius, fill=_XP_BAR_BG
    )
    fill_width = int((right - left) * progress)
    if fill_width <= 0:
        return
    fill_right = left + fill_width
    fill_radius = int(min(radius, fill_width / 2))
    draw.rounded_rectangle(
        (left, top, fill_right, bottom),
        radius=fill_radius,
        fill=_XP_BAR_FILL,
    )


def _draw_name_tokens(
    image: Image.Image,
    draw: ImageDraw.ImageDraw,
    tokens: Iterable[NameToken],
    x: float,
    y: float,
    font: ImageFont.ImageFont,
    line_height: int,
    *,
    emoji_size: int,
) -> None:
    text_y = _center_text_y(draw, y, line_height, font)
    emoji_y = y + (line_height - emoji_size) // 2
    cursor_x = x
    placeholder = _emoji_placeholder(emoji_size)
    for token in tokens:
        if token.kind == "text" and token.text:
            _draw_text(draw, (cursor_x, text_y), token.text, font, _TEXT)
            cursor_x += _text_width(draw, token.text, font)
            continue
        emoji_image = (
            token.image if isinstance(token.image, Image.Image) else None
        ) or placeholder
        if emoji_image.size != (emoji_size, emoji_size):
            emoji_image = ImageOps.fit(
                emoji_image, (emoji_size, emoji_size), method=Image.LANCZOS
            )
        image.paste(emoji_image, (int(cursor_x), int(emoji_y)), emoji_image)
        cursor_x += emoji_size + 2


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
        return "0"
    return f"{int(value):,}"


def _draw_text(
    draw: ImageDraw.ImageDraw,
    position: tuple[float, float],
    text: str,
    font: ImageFont.ImageFont,
    fill: tuple[int, int, int, int],
) -> None:
    draw.text(position, text, fill=fill, font=font)
