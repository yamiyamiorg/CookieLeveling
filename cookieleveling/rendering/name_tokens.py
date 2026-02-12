from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Iterable, Literal, TYPE_CHECKING

if TYPE_CHECKING:
    from PIL import Image


TokenKind = Literal["text", "unicode_emoji", "custom_emoji"]


@dataclass
class NameToken:
    kind: TokenKind
    text: str | None = None
    emoji: str | None = None
    emoji_id: str | None = None
    animated: bool = False
    image: Image.Image | None = None


_CUSTOM_EMOJI_RE = re.compile(r"<(?P<animated>a?):(?P<name>[^:>]+):(?P<id>\d+)>")
_ZWJ = "\u200d"
_VS16 = "\ufe0f"
_VS15 = "\ufe0e"


def tokenize_display_name(name: str) -> list[NameToken]:
    tokens: list[NameToken] = []
    last_idx = 0
    for match in _CUSTOM_EMOJI_RE.finditer(name):
        start, end = match.span()
        if start > last_idx:
            tokens.extend(_tokenize_unicode_segment(name[last_idx:start]))
        tokens.append(
            NameToken(
                kind="custom_emoji",
                emoji_id=match.group("id"),
                animated=bool(match.group("animated")),
            )
        )
        last_idx = end
    if last_idx < len(name):
        tokens.extend(_tokenize_unicode_segment(name[last_idx:]))
    return _merge_text_tokens(tokens)


def truncate_tokens(tokens: Iterable[NameToken], max_chars: int = 16) -> list[NameToken]:
    tokens = list(tokens)
    if _token_display_width(tokens) <= max_chars:
        return tokens
    target = max(0, max_chars - 3)
    trimmed: list[NameToken] = []
    used = 0
    for token in tokens:
        if used >= target:
            break
        if token.kind == "text" and token.text:
            remaining = target - used
            if len(token.text) <= remaining:
                trimmed.append(token)
                used += len(token.text)
            else:
                trimmed.append(NameToken(kind="text", text=token.text[:remaining]))
                used += remaining
        else:
            trimmed.append(token)
            used += 1
    trimmed.append(NameToken(kind="text", text="..."))
    return trimmed


def _token_display_width(tokens: Iterable[NameToken]) -> int:
    width = 0
    for token in tokens:
        if token.kind == "text" and token.text:
            width += len(token.text)
        else:
            width += 1
    return width


def _tokenize_unicode_segment(segment: str) -> list[NameToken]:
    tokens: list[NameToken] = []
    buffer: list[str] = []
    idx = 0
    while idx < len(segment):
        emoji = _consume_unicode_emoji(segment, idx)
        if emoji is None:
            buffer.append(segment[idx])
            idx += 1
            continue
        if buffer:
            tokens.append(NameToken(kind="text", text="".join(buffer)))
            buffer = []
        tokens.append(NameToken(kind="unicode_emoji", emoji=emoji))
        idx += len(emoji)
    if buffer:
        tokens.append(NameToken(kind="text", text="".join(buffer)))
    return tokens


def _consume_unicode_emoji(text: str, start: int) -> str | None:
    ch = text[start]
    code = ord(ch)
    if _is_regional_indicator(code):
        if start + 1 < len(text) and _is_regional_indicator(ord(text[start + 1])):
            return text[start : start + 2]
        return ch
    if not _is_emoji_base(code):
        return None
    end = start + 1
    while end < len(text):
        nxt = text[end]
        code = ord(nxt)
        if _is_variation_selector(nxt) or _is_skin_tone_modifier(code):
            end += 1
            continue
        if nxt == _ZWJ:
            if end + 1 < len(text) and _is_emoji_base(ord(text[end + 1])):
                end += 2
                continue
            break
        if code == 0x20E3:
            end += 1
            continue
        break
    return text[start:end]


def _is_emoji_base(code: int) -> bool:
    if 0x1F300 <= code <= 0x1FAFF:
        return True
    if 0x2600 <= code <= 0x26FF:
        return True
    if 0x2700 <= code <= 0x27BF:
        return True
    if 0x1F000 <= code <= 0x1F02F:
        return True
    if _is_regional_indicator(code):
        return True
    return False


def _is_regional_indicator(code: int) -> bool:
    return 0x1F1E6 <= code <= 0x1F1FF


def _is_variation_selector(ch: str) -> bool:
    return ch == _VS15 or ch == _VS16


def _is_skin_tone_modifier(code: int) -> bool:
    return 0x1F3FB <= code <= 0x1F3FF


def _merge_text_tokens(tokens: list[NameToken]) -> list[NameToken]:
    merged: list[NameToken] = []
    for token in tokens:
        if token.kind != "text":
            merged.append(token)
            continue
        if not token.text:
            continue
        if merged and merged[-1].kind == "text" and merged[-1].text:
            merged[-1].text += token.text
        else:
            merged.append(token)
    return merged
