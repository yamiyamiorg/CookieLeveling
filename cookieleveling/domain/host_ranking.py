from __future__ import annotations

from datetime import datetime, timezone

from cookieleveling.db import (
    fetch_host_top20_monthly,
    fetch_host_top20_total,
    fetch_host_top20_weekly,
)
from cookieleveling.domain.week import current_week_key

def compute_host_top20_monthly(guild_id: int, *, limit: int = 20) -> list[dict]:
    rows = fetch_host_top20_monthly(guild_id)
    candidates = [_monthly_entry(row) for row in rows]
    candidates.sort(key=_monthly_sort_key)
    return candidates[:limit]


def compute_host_top20_total(guild_id: int, *, limit: int = 20) -> list[dict]:
    rows = fetch_host_top20_total(guild_id)
    candidates = [_total_entry(row) for row in rows]
    candidates.sort(key=_total_sort_key)
    return candidates[:limit]


def compute_host_top20_weekly(guild_id: int, *, limit: int = 20) -> list[dict]:
    week_key = current_week_key()
    rows = fetch_host_top20_weekly(guild_id, week_key)
    candidates = [_weekly_entry(row) for row in rows]
    candidates.sort(key=_weekly_sort_key)
    return candidates[:limit]


def _monthly_entry(row) -> dict:
    last_earned_at = _parse_time(row["last_earned_at"])
    return {
        "user_id": row["user_id"],
        "monthly_xp": row["monthly_xp"],
        "last_earned_at": last_earned_at,
    }


def _monthly_sort_key(entry: dict) -> tuple:
    last_earned = entry["last_earned_at"] or datetime.max.replace(tzinfo=timezone.utc)
    return (-entry["monthly_xp"], last_earned, entry["user_id"])


def _total_entry(row) -> dict:
    last_earned_at = _parse_time(row["last_earned_at"])
    return {
        "user_id": row["user_id"],
        "total_xp": row["total_xp"],
        "last_earned_at": last_earned_at,
    }


def _weekly_entry(row) -> dict:
    last_earned_at = _parse_time(row["updated_at"])
    return {
        "user_id": row["user_id"],
        "weekly_xp": row["weekly_xp"],
        "last_earned_at": last_earned_at,
    }


def _total_sort_key(entry: dict) -> tuple:
    last_earned = entry["last_earned_at"] or datetime.max.replace(tzinfo=timezone.utc)
    return (-entry["total_xp"], last_earned, entry["user_id"])


def _weekly_sort_key(entry: dict) -> tuple:
    last_earned = entry["last_earned_at"] or datetime.max.replace(tzinfo=timezone.utc)
    return (-entry["weekly_xp"], last_earned, entry["user_id"])


def _parse_time(value) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value)
