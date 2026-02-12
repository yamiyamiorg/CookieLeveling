from __future__ import annotations

import re
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

_JST = ZoneInfo("Asia/Tokyo")
_ISO_WEEK_PATTERN = re.compile(r"^\d{4}-W\d{2}$")
_DATE_WEEK_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def now_jst() -> datetime:
    return datetime.now(_JST)


def current_week_key(value: datetime | None = None) -> str:
    if value is None:
        value = now_jst()
    return week_key_for(value)


def current_month_key(value: datetime | None = None) -> str:
    if value is None:
        value = now_jst()
    return month_key_for(value)


def week_key_for(value: datetime) -> str:
    localized = _as_jst(value)
    iso = localized.isocalendar()
    return f"{iso.year:04d}-W{iso.week:02d}"


def month_key_for(value: datetime) -> str:
    localized = _as_jst(value)
    return f"{localized.year:04d}-{localized.month:02d}"


def min_week_key_to_keep(weeks_to_keep: int = 12) -> str:
    if weeks_to_keep <= 1:
        return current_week_key()
    target = now_jst() - timedelta(weeks=weeks_to_keep - 1)
    return week_key_for(target)


def normalize_week_key(value: str | None) -> str | None:
    if value is None:
        return None
    key = value.strip()
    if not key:
        return None
    if _ISO_WEEK_PATTERN.match(key):
        return key
    if _DATE_WEEK_PATTERN.match(key):
        parsed = datetime.fromisoformat(key)
        return week_key_for(parsed.replace(tzinfo=_JST))
    return None


def _as_jst(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=_JST)
    return value.astimezone(_JST)
