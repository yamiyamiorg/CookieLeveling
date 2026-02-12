from __future__ import annotations

from datetime import datetime, timezone

from cookieleveling.db import (
    fetch_lifetime_candidates,
    fetch_rank_candidates,
    fetch_weekly_candidates,
)
from cookieleveling.domain.week import current_week_key

def compute_top20(guild_id: int, *, limit: int = 20) -> list[dict]:
    now = datetime.now(timezone.utc)
    rows = fetch_rank_candidates(guild_id)
    candidates = [_rank_entry(row, now) for row in rows]
    candidates.sort(key=_sort_key)
    return candidates[:limit]


def compute_lifetime_top20(guild_id: int, *, limit: int = 20) -> list[dict]:
    rows = fetch_lifetime_candidates(guild_id)
    candidates = [_lifetime_entry(row) for row in rows]
    candidates.sort(key=_lifetime_sort_key)
    return candidates[:limit]


def compute_weekly_top20(guild_id: int, *, limit: int = 20) -> list[dict]:
    rows = fetch_weekly_candidates(guild_id, current_week_key())
    candidates = [_weekly_entry(row) for row in rows]
    candidates.sort(key=_weekly_sort_key)
    return candidates[:limit]


def _rank_entry(row, now: datetime) -> dict:
    joined_at = _parse_time(row["joined_at"])
    last_earned_at = _parse_time(row["last_earned_at"])
    active_seconds = 0
    if row["is_in_vc"] and joined_at is not None:
        active_seconds = int(min((now - joined_at).total_seconds(), 3600))
        active_seconds = max(active_seconds, 0)
    return {
        "user_id": row["user_id"],
        "season_xp": row["season_xp"],
        "active_seconds": active_seconds,
        "last_earned_at": last_earned_at,
    }


def _sort_key(entry: dict) -> tuple:
    last_earned = entry["last_earned_at"] or datetime.max.replace(tzinfo=timezone.utc)
    return (
        -entry["season_xp"],
        -entry["active_seconds"],
        last_earned,
        entry["user_id"],
    )


def _lifetime_entry(row) -> dict:
    last_earned_at = _parse_time(row["last_earned_at"])
    return {
        "user_id": row["user_id"],
        "lifetime_xp": row["lifetime_xp"],
        "last_earned_at": last_earned_at,
    }


def _weekly_entry(row) -> dict:
    last_earned_at = _parse_time(row["last_earned_at"])
    return {
        "user_id": row["user_id"],
        "weekly_xp": row["weekly_xp"],
        "last_earned_at": last_earned_at,
    }


def _lifetime_sort_key(entry: dict) -> tuple:
    last_earned = entry["last_earned_at"] or datetime.max.replace(tzinfo=timezone.utc)
    return (-entry["lifetime_xp"], last_earned, entry["user_id"])


def _weekly_sort_key(entry: dict) -> tuple:
    last_earned = entry["last_earned_at"] or datetime.max.replace(tzinfo=timezone.utc)
    return (-entry["weekly_xp"], last_earned, entry["user_id"])


def _parse_time(value) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value)
