from datetime import datetime, timezone
import logging
from zoneinfo import ZoneInfo

from cookieleveling.db import (
    add_user_weekly_xp,
    ensure_period_state,
    fetch_active_voice_users,
    get_meta_value,
    reset_host_monthly,
    reset_season_xp,
    set_meta_value,
    update_user_xp,
)

_LOGGER = logging.getLogger(__name__)


def tick_minute(guild_id: int) -> tuple[int, dict[int, int]]:
    now = datetime.now(timezone.utc)
    ensure_period_state(guild_id)
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
                user_id=row["user_id"],
                weekly_inc=1,
                updated_at=now.isoformat(),
            )
        except Exception:
            _LOGGER.exception(
                "weekly xp add failed: guild_id=%s user_id=%s",
                guild_id,
                row["user_id"],
            )

        if next_level != prev_level:
            level_changes[int(row["user_id"])] = next_level
        updated += 1

    _LOGGER.info("weekly_xp incremented users=%s", updated)
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
    now_jst = datetime.now(ZoneInfo("Asia/Tokyo"))
    current_month_key = f"{now_jst.year:04d}-{now_jst.month:02d}"
    if now_jst.day != 1:
        return False
    if get_meta_value("last_monthly_reset_key") == current_month_key:
        return False
    reset_season_xp(guild_id)
    set_meta_value("last_monthly_reset_key", current_month_key)
    return True


def maybe_host_monthly_reset(guild_id: int) -> bool:
    now_jst = datetime.now(ZoneInfo("Asia/Tokyo"))
    current_month_key = f"{now_jst.year:04d}-{now_jst.month:02d}"
    if now_jst.day != 1:
        return False
    if get_meta_value("last_host_monthly_reset_key") == current_month_key:
        return False
    reset_host_monthly(guild_id)
    set_meta_value("last_host_monthly_reset_key", current_month_key)
    return True
