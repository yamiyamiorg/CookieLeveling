from __future__ import annotations

from datetime import datetime, timezone
from .db import fetch_lifetime_candidates, fetch_rank_candidates


def compute_top20(guild_id: int) -> list[dict]:
    now = datetime.now(timezone.utc)
    candidates = [
        _rank_entry(row, now) for row in fetch_rank_candidates(guild_id) if not row["optout"]
    ]
    candidates.sort(key=_sort_key)
    return candidates[:20]


def compute_lifetime_top20(guild_id: int) -> list[dict]:
    candidates = [
        _lifetime_entry(row) for row in fetch_lifetime_candidates(guild_id) if not row["optout"]
    ]
    candidates.sort(key=_lifetime_sort_key)
    return candidates[:20]


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


def _lifetime_sort_key(entry: dict) -> tuple:
    last_earned = entry["last_earned_at"] or datetime.max.replace(tzinfo=timezone.utc)
    return (-entry["lifetime_xp"], last_earned, entry["user_id"])


def _parse_time(value) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value)
