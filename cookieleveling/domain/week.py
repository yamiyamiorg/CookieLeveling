from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

_JST = ZoneInfo("Asia/Tokyo")


def current_week_key() -> str:
    now_jst = datetime.now(_JST)
    return week_key_for(now_jst)


def week_key_for(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=_JST)
    else:
        value = value.astimezone(_JST)
    iso_year, iso_week, _iso_weekday = value.isocalendar()
    return f"{iso_year}-W{iso_week:02d}"


def min_week_key_to_keep(weeks_to_keep: int = 12) -> str:
    if weeks_to_keep <= 1:
        return current_week_key()
    target = datetime.now(_JST) - timedelta(weeks=weeks_to_keep - 1)
    return week_key_for(target)
