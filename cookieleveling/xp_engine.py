from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from .db import fetch_active_voice_users, reset_season_xp, update_user_xp

_LAST_RESET_MONTH: tuple[int, int] | None = None


def tick_minute(guild_id: int) -> int:
    now = _utc_now()
    updated = 0
    for row in fetch_active_voice_users(guild_id):
        factor = _lifetime_factor()
        lifetime_total = row["rem_lifetime"] + factor
        lifetime_inc = int(lifetime_total)
        rem_lifetime = lifetime_total - lifetime_inc
        update_user_xp(
            guild_id=guild_id,
            user_id=row["user_id"],
            season_inc=1,
            lifetime_inc=lifetime_inc,
            rem_lifetime=rem_lifetime,
            last_earned_at=now,
        )
        updated += 1
    return updated


def _lifetime_factor() -> float:
    return 1.0


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def level_from_xp(lifetime_xp: int) -> int:
    if lifetime_xp <= 0:
        return 1
    value = 1 + (1 + (lifetime_xp / 15)) ** 0.5
    return max(1, int(value // 2))


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
