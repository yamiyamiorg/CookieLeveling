from __future__ import annotations

from datetime import datetime, timezone

from .db import fetch_host_top20_monthly, fetch_host_top20_total


def compute_host_top20_monthly(guild_id: int) -> list[dict]:
    candidates = [_monthly_entry(row) for row in fetch_host_top20_monthly(guild_id)]
    candidates.sort(key=_monthly_sort_key)
    return candidates[:20]


def compute_host_top20_total(guild_id: int) -> list[dict]:
    candidates = [_total_entry(row) for row in fetch_host_top20_total(guild_id)]
    candidates.sort(key=_total_sort_key)
    return candidates[:20]


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


def _total_sort_key(entry: dict) -> tuple:
    last_earned = entry["last_earned_at"] or datetime.max.replace(tzinfo=timezone.utc)
    return (-entry["total_xp"], last_earned, entry["user_id"])


def _parse_time(value) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value)
