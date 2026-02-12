from __future__ import annotations

import os
from typing import Iterable

from PIL import ImageDraw, ImageFont

from .constants import _BOLD_FONT_PATHS, _FONT_PATH, _MONO_FONT_PATHS


def load_font_with_fallbacks(
    paths: Iterable[str], size: int, *, index: int = 0
) -> ImageFont.ImageFont:
    for path in paths:
        if os.path.exists(path):
            return ImageFont.truetype(path, size=size, index=index)
    return ImageFont.load_default()


def load_font(
    size: int, *, prefer_mono: bool = False, prefer_bold: bool = False
) -> ImageFont.ImageFont:
    if prefer_mono:
        for path in _MONO_FONT_PATHS:
            if os.path.exists(path):
                return ImageFont.truetype(path, size=size)
        return load_font_with_fallbacks((_FONT_PATH,), size)
    if prefer_bold:
        for path in _BOLD_FONT_PATHS:
            if os.path.exists(path):
                return ImageFont.truetype(path, size=size)
        return load_font_with_fallbacks((_FONT_PATH,), size)
    return load_font_with_fallbacks((_FONT_PATH,), size)


def text_width(draw: ImageDraw.ImageDraw, text: str, font: ImageFont.ImageFont) -> float:
    if hasattr(draw, "textlength"):
        return draw.textlength(text, font=font)
    return font.getlength(text)


def text_height(
    draw: ImageDraw.ImageDraw, text: str, font: ImageFont.ImageFont
) -> float:
    bbox = draw.textbbox((0, 0), text, font=font)
    return bbox[3] - bbox[1]


def center_text_y(
    draw: ImageDraw.ImageDraw, y: int, height: int, font: ImageFont.ImageFont
) -> int:
    bbox = draw.textbbox((0, 0), "Ag", font=font)
    text_height = bbox[3] - bbox[1]
    return int(y + (height - text_height) / 2 - bbox[1])
