from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from cookieleveling.domain.period import normalize_week_key
from cookieleveling.db.core import get_connection
from cookieleveling.db.period import ensure_period_state

from .user_flags_repo import ensure_user


def ensure_host_user(guild_id: int, user_id: int) -> None:
    ensure_user(guild_id, user_id)
    conn = get_connection()
    week_key, month_key = ensure_period_state(guild_id)
    conn.execute(
        """
        INSERT OR IGNORE INTO host_xp (
            guild_id,
            user_id,
            monthly_key,
            weekly_key
        ) VALUES (?, ?, ?, ?)
        """,
        (guild_id, user_id, month_key, week_key),
    )
    conn.commit()


def add_host_xp(
    *,
    guild_id: int,
    user_id: int,
    monthly_inc: int,
    total_inc: int,
    last_earned_at: str,
) -> None:
    ensure_host_user(guild_id, user_id)
    conn = get_connection()
    _week_key, month_key = ensure_period_state(guild_id)
    conn.execute(
        """
        UPDATE host_xp
        SET monthly_xp = CASE
                WHEN monthly_key = ? THEN monthly_xp + ?
                ELSE ?
            END,
            monthly_key = ?,
            total_xp = total_xp + ?,
            last_earned_at = ?
        WHERE guild_id = ? AND user_id = ?
        """,
        (
            month_key,
            monthly_inc,
            monthly_inc,
            month_key,
            total_inc,
            last_earned_at,
            guild_id,
            user_id,
        ),
    )
    conn.commit()


def add_host_weekly_xp(
    guild_id: int,
    user_id: int,
    weekly_inc: int,
    week_key: Optional[str] = None,
    updated_at: str = "",
) -> None:
    ensure_host_user(guild_id, user_id)
    conn = get_connection()
    current_week_key, _month_key = ensure_period_state(guild_id)
    target_week_key = normalize_week_key(week_key) or current_week_key
    if not updated_at:
        updated_at = datetime.now(timezone.utc).isoformat()
    conn.execute(
        """
        UPDATE host_xp
        SET weekly_xp = weekly_xp + ?,
            weekly_key = ?,
            last_earned_at = COALESCE(?, last_earned_at)
        WHERE guild_id = ? AND user_id = ?
        """,
        (
            weekly_inc,
            target_week_key,
            updated_at,
            guild_id,
            user_id,
        ),
    )
    conn.execute(
        """
        INSERT INTO host_weekly_xp (
            guild_id,
            week_key,
            user_id,
            weekly_xp,
            updated_at
        ) VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(guild_id, week_key, user_id)
        DO UPDATE SET
            weekly_xp = host_weekly_xp.weekly_xp + excluded.weekly_xp,
            updated_at = excluded.updated_at
        """,
        (guild_id, target_week_key, user_id, weekly_inc, updated_at),
    )
    conn.commit()


def increment_host_session_counts(guild_id: int, user_id: int) -> None:
    ensure_host_user(guild_id, user_id)
    conn = get_connection()
    current_week_key, current_month_key = ensure_period_state(guild_id)
    conn.execute(
        """
        UPDATE host_xp
        SET monthly_sessions = CASE
                WHEN monthly_key = ? THEN monthly_sessions + 1
                ELSE 1
            END,
            monthly_key = ?,
            weekly_sessions = CASE
                WHEN weekly_key = ? THEN weekly_sessions + 1
                ELSE 1
            END,
            weekly_key = ?,
            total_sessions = total_sessions + 1
        WHERE guild_id = ? AND user_id = ?
        """,
        (
            current_month_key,
            current_month_key,
            current_week_key,
            current_week_key,
            guild_id,
            user_id,
        ),
    )
    conn.commit()


def reset_host_monthly(guild_id: int) -> None:
    conn = get_connection()
    _week_key, month_key = ensure_period_state(guild_id)
    conn.execute(
        """
        UPDATE host_xp
        SET monthly_xp = 0,
            monthly_sessions = 0,
            monthly_key = ?
        WHERE guild_id = ?
        """,
        (month_key, guild_id),
    )
    conn.commit()
