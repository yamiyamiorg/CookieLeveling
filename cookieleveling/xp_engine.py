from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from .db import fetch_active_voice_users, reset_season_xp, update_user_xp

_LAST_RESET_MONTH: tuple[int, int] | None = None


def tick_minute(guild_id: int) -> int:
    now = datetime.now(timezone.utc)
    updated = 0
    for row in fetch_active_voice_users(guild_id):
        factor = _lifetime_factor(now, row["joined_at"])
        lifetime_total = row["rem_lifetime"] + factor
        lifetime_inc = int(lifetime_total)
        rem_lifetime = lifetime_total - lifetime_inc
        update_user_xp(
            guild_id=guild_id,
            user_id=row["user_id"],
            season_inc=1,
            lifetime_inc=lifetime_inc,
            rem_lifetime=rem_lifetime,
            last_earned_at=now.isoformat(),
        )
        updated += 1
    return updated


def _lifetime_factor(now: datetime, joined_at: str | None) -> float:
    if not joined_at:
        return 1.0
    joined = _parse_time(joined_at)
    if joined is None:
        return 1.0
    elapsed_minutes = int((now - joined).total_seconds() // 60)
    if elapsed_minutes < 0:
        elapsed_minutes = 0
    if elapsed_minutes < 60:
        return 1.0
    if elapsed_minutes < 120:
        return 0.5
    return 0.25


def _parse_time(value: str) -> datetime | None:
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def level_from_xp(lifetime_xp: int) -> int:
    if lifetime_xp <= 0:
        return 1
    threshold = lifetime_xp / 60
    level = int((1 + (1 + 4 * threshold) ** 0.5) // 2)
    return max(1, level)


def maybe_monthly_reset(guild_id: int) -> bool:
    global _LAST_RESET_MONTH
    now_jst = datetime.now(ZoneInfo("Asia/Tokyo"))
    current_month = (now_jst.year, now_jst.month)
    if now_jst.day != 1:
        return False
    if _LAST_RESET_MONTH == current_month:
        return False
    reset_season_xp(guild_id)
    _LAST_RESET_MONTH = current_month
    return True
