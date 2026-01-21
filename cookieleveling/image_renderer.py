from __future__ import annotations

from typing import Iterable

from PIL import Image, ImageDraw, ImageFont

_SIZE = (1280, 720)
_PINK = (255, 192, 203)
_GRAY = (220, 220, 220)
_TEXT = (30, 30, 30)


def render_season_image(entries: Iterable[dict], output_path: str) -> None:
    image = Image.new("RGB", _SIZE, _PINK)
    draw = ImageDraw.Draw(image)
    font = ImageFont.load_default()

    draw.text((40, 30), "Season Top 10", fill=_TEXT, font=font)
    _draw_entries(image, draw, entries, font, value_key="season_xp")
    image.save(output_path, format="PNG")


def render_lifetime_image(entries: Iterable[dict], output_path: str) -> None:
    image = Image.new("RGB", _SIZE, _GRAY)
    draw = ImageDraw.Draw(image)
    font = ImageFont.load_default()

    draw.text((40, 30), "Lifetime Levels", fill=_TEXT, font=font)
    _draw_entries(image, draw, entries, font, value_key="level")
    image.save(output_path, format="PNG")


def _draw_entries(
    image: Image.Image,
    draw: ImageDraw.ImageDraw,
    entries: Iterable[dict],
    font: ImageFont.ImageFont,
    *,
    value_key: str,
) -> None:
    start_y = 90
    line_height = 58
    avatar_size = 40
    placeholder = Image.new("RGB", (avatar_size, avatar_size), (180, 180, 180))

    for idx, entry in enumerate(entries, start=1):
        y = start_y + (idx - 1) * line_height
        avatar = entry.get("avatar") or placeholder
        if avatar.size != (avatar_size, avatar_size):
            avatar = avatar.resize((avatar_size, avatar_size))
        image.paste(avatar, (40, y))

        name = _truncate_name(entry.get("name", "unknown"))
        value = entry.get(value_key, 0)
        draw.text((90, y + 10), f"{idx}. {name}", fill=_TEXT, font=font)
        draw.text((520, y + 10), f"{value}", fill=_TEXT, font=font)


def _truncate_name(name: str, max_chars: int = 16) -> str:
    if len(name) <= max_chars:
        return name
    return name[: max_chars - 3] + "..."
