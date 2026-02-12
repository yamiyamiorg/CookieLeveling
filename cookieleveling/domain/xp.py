from datetime import datetime, timezone
import logging
from zoneinfo import ZoneInfo

from cookieleveling.db import (
    add_user_weekly_xp,
    fetch_active_voice_users,
    reset_host_monthly,
    reset_season_xp,
    update_user_xp,
)
from cookieleveling.domain.week import current_week_key

_LOGGER = logging.getLogger(__name__)
_LAST_RESET_MONTH: tuple[int, int] | None = None
_LAST_HOST_RESET_MONTH: tuple[int, int] | None = None


def tick_minute(guild_id: int) -> tuple[int, dict[int, int]]:
    now = datetime.now(timezone.utc)
    week_key = current_week_key()
    updated = 0
    level_changes: dict[int, int] = {}
    for row in fetch_active_voice_users(guild_id):
        lifetime_xp = int(row["lifetime_xp"])
        prev_level = level_from_xp(lifetime_xp)
        next_level = level_from_xp(lifetime_xp + 1)
        update_user_xp(
            guild_id=guild_id,
            user_id=row["user_id"],
            season_inc=1,
            lifetime_inc=1,
            last_earned_at=now.isoformat(),
        )
        try:
            add_user_weekly_xp(
                guild_id=guild_id,
                week_key=week_key,
                user_id=row["user_id"],
                weekly_inc=1,
                updated_at=now.isoformat(),
            )
        except Exception:
            _LOGGER.exception(
                "weekly xp add failed: guild_id=%s user_id=%s week_key=%s",
                guild_id,
                row["user_id"],
                week_key,
            )
        if next_level != prev_level:
            level_changes[int(row["user_id"])] = next_level
        updated += 1
    return updated, level_changes


def level_from_xp(lifetime_xp: int) -> int:
    if lifetime_xp <= 0:
        return 1
    threshold = lifetime_xp / 60
    level = int((1 + (1 + 4 * threshold) ** 0.5) // 2)
    return max(1, level)


def xp_required(level: int) -> int:
    if level <= 1:
        return 0
    return 60 * level * (level - 1)


def progress_for_xp(xp: int) -> tuple[int, int, int, float]:
    level = level_from_xp(xp)
    curr = xp_required(level)
    next_req = xp_required(level + 1)
    if next_req <= curr:
        progress = 0.0
    else:
        progress = (xp - curr) / (next_req - curr)
    progress = max(0.0, min(1.0, progress))
    return level, curr, next_req, progress


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


def maybe_host_monthly_reset(guild_id: int) -> bool:
    global _LAST_HOST_RESET_MONTH
    now_jst = datetime.now(ZoneInfo("Asia/Tokyo"))
    current_month = (now_jst.year, now_jst.month)
    if now_jst.day != 1:
        return False
    if _LAST_HOST_RESET_MONTH == current_month:
        return False
    reset_host_monthly(guild_id)
    _LAST_HOST_RESET_MONTH = current_month
    return True
