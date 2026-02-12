from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from typing import Optional

from cookieleveling.domain.period import normalize_week_key
from cookieleveling.db.core import get_connection
from cookieleveling.db.period import ensure_period_state

from .user_flags_repo import ensure_user


def update_user_xp(
    *,
    guild_id: int,
    user_id: int,
    season_inc: int,
    lifetime_inc: int,
    last_earned_at: str,
) -> None:
    ensure_user(guild_id, user_id)
    conn = get_connection()
    _week_key, month_key = ensure_period_state(guild_id)
    conn.execute(
        """
        UPDATE voice_xp
        SET monthly_xp = CASE
                WHEN monthly_key = ? THEN monthly_xp + ?
                ELSE ?
            END,
            monthly_key = ?,
            lifetime_xp = lifetime_xp + ?,
            last_earned_at = ?
        WHERE guild_id = ? AND user_id = ?
        """,
        (
            month_key,
            season_inc,
            season_inc,
            month_key,
            lifetime_inc,
            last_earned_at,
            guild_id,
            user_id,
        ),
    )
    conn.commit()


def grant_xp(
    guild_id: int,
    user_id: int,
    season_inc: int,
    lifetime_inc: int,
    last_earned_at: Optional[str],
) -> None:
    ensure_user(guild_id, user_id)
    conn = get_connection()
    _week_key, month_key = ensure_period_state(guild_id)
    conn.execute(
        """
        UPDATE voice_xp
        SET monthly_xp = CASE
                WHEN monthly_key = ? THEN monthly_xp + ?
                ELSE ?
            END,
            monthly_key = ?,
            lifetime_xp = lifetime_xp + ?,
            last_earned_at = COALESCE(?, last_earned_at)
        WHERE guild_id = ? AND user_id = ?
        """,
        (
            month_key,
            season_inc,
            season_inc,
            month_key,
            lifetime_inc,
            last_earned_at,
            guild_id,
            user_id,
        ),
    )
    conn.commit()


def add_user_weekly_xp(
    guild_id: int,
    user_id: int,
    weekly_inc: int,
    week_key: Optional[str] = None,
    updated_at: Optional[str] = None,
) -> None:
    ensure_user(guild_id, user_id)
    conn = get_connection()
    current_week_key, _month_key = ensure_period_state(guild_id)
    target_week_key = normalize_week_key(week_key) or current_week_key
    if updated_at is None:
        updated_at = datetime.now(timezone.utc).isoformat()

    conn.execute(
        """
        UPDATE voice_xp
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
        INSERT INTO user_weekly_xp (
            guild_id,
            week_key,
            user_id,
            weekly_xp,
            updated_at
        ) VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(guild_id, week_key, user_id)
        DO UPDATE SET
            weekly_xp = user_weekly_xp.weekly_xp + excluded.weekly_xp,
            updated_at = excluded.updated_at
        """,
        (guild_id, target_week_key, user_id, weekly_inc, updated_at),
    )
    conn.commit()


def set_xp(
    guild_id: int,
    user_id: int,
    season_xp: int,
    lifetime_xp: int,
    last_earned_at: str,
) -> None:
    ensure_user(guild_id, user_id)
    conn = get_connection()
    _week_key, month_key = ensure_period_state(guild_id)
    conn.execute(
        """
        UPDATE voice_xp
        SET monthly_xp = ?,
            monthly_key = ?,
            lifetime_xp = ?,
            last_earned_at = ?
        WHERE guild_id = ? AND user_id = ?
        """,
        (season_xp, month_key, lifetime_xp, last_earned_at, guild_id, user_id),
    )
    conn.commit()


def reset_season_xp(guild_id: int) -> None:
    conn = get_connection()
    _week_key, month_key = ensure_period_state(guild_id)
    conn.execute(
        """
        UPDATE voice_xp
        SET monthly_xp = 0,
            monthly_key = ?
        WHERE guild_id = ?
        """,
        (month_key, guild_id),
    )
    conn.commit()


def reset_weekly_xp(guild_id: int) -> None:
    conn = get_connection()
    week_key, _month_key = ensure_period_state(guild_id)
    conn.execute(
        """
        UPDATE voice_xp
        SET weekly_xp = 0,
            weekly_key = ?
        WHERE guild_id = ?
        """,
        (week_key, guild_id),
    )
    conn.commit()


def fetch_active_voice_users(guild_id: int) -> list[sqlite3.Row]:
    conn = get_connection()
    ensure_period_state(guild_id)
    return conn.execute(
        """
        SELECT vs.user_id, vs.joined_at, COALESCE(vx.lifetime_xp, 0) AS lifetime_xp
        FROM voice_state vs
        INNER JOIN guild_members gm
          ON gm.guild_id = vs.guild_id
         AND gm.user_id = vs.user_id
         AND gm.member_state = 1
        INNER JOIN user_flags uf
          ON uf.guild_id = vs.guild_id
         AND uf.user_id = vs.user_id
        LEFT JOIN voice_xp vx
          ON vx.guild_id = vs.guild_id
         AND vx.user_id = vs.user_id
        WHERE vs.guild_id = ?
          AND vs.is_in_vc = 1
          AND COALESCE(uf.optout, 0) = 0
          AND COALESCE(uf.is_excluded, 0) = 0
          AND uf.left_guild_at IS NULL
        """,
        (guild_id,),
    ).fetchall()


def fetch_lifetime_users(guild_id: int) -> list[sqlite3.Row]:
    conn = get_connection()
    ensure_period_state(guild_id)
    return conn.execute(
        """
        SELECT vx.user_id, vx.lifetime_xp
        FROM voice_xp vx
        INNER JOIN guild_members gm
          ON gm.guild_id = vx.guild_id
         AND gm.user_id = vx.user_id
         AND gm.member_state = 1
        INNER JOIN user_flags uf
          ON uf.guild_id = vx.guild_id
         AND uf.user_id = vx.user_id
        WHERE vx.guild_id = ?
          AND uf.left_guild_at IS NULL
        """,
        (guild_id,),
    ).fetchall()
