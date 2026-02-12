from __future__ import annotations

import sqlite3

from cookieleveling.db.core import get_connection
from cookieleveling.db.period import ensure_period_state


def fetch_host_top20_monthly(guild_id: int, limit: int = 20) -> list[sqlite3.Row]:
    conn = get_connection()
    _week_key, month_key = ensure_period_state(guild_id)
    return conn.execute(
        """
        SELECT hx.user_id, hx.monthly_xp, hx.last_earned_at
        FROM host_xp hx
        INNER JOIN guild_members gm
          ON gm.guild_id = hx.guild_id
         AND gm.user_id = hx.user_id
         AND gm.member_state = 1
        INNER JOIN user_flags uf
          ON uf.guild_id = hx.guild_id
         AND uf.user_id = hx.user_id
        WHERE hx.guild_id = ?
          AND hx.monthly_key = ?
          AND hx.monthly_xp > 0
          AND COALESCE(uf.optout, 0) = 0
          AND COALESCE(uf.is_excluded, 0) = 0
          AND COALESCE(uf.rank_visible, 1) = 1
          AND uf.left_guild_at IS NULL
        ORDER BY hx.monthly_xp DESC, hx.last_earned_at ASC, hx.user_id ASC
        LIMIT ?
        """,
        (guild_id, month_key, limit),
    ).fetchall()


def fetch_host_top20_total(guild_id: int, limit: int = 20) -> list[sqlite3.Row]:
    conn = get_connection()
    ensure_period_state(guild_id)
    return conn.execute(
        """
        SELECT hx.user_id, hx.total_xp, hx.last_earned_at
        FROM host_xp hx
        INNER JOIN guild_members gm
          ON gm.guild_id = hx.guild_id
         AND gm.user_id = hx.user_id
         AND gm.member_state = 1
        INNER JOIN user_flags uf
          ON uf.guild_id = hx.guild_id
         AND uf.user_id = hx.user_id
        WHERE hx.guild_id = ?
          AND hx.total_xp > 0
          AND COALESCE(uf.optout, 0) = 0
          AND COALESCE(uf.is_excluded, 0) = 0
          AND COALESCE(uf.rank_visible, 1) = 1
          AND uf.left_guild_at IS NULL
        ORDER BY hx.total_xp DESC, hx.last_earned_at ASC, hx.user_id ASC
        LIMIT ?
        """,
        (guild_id, limit),
    ).fetchall()


def fetch_host_top20_weekly(guild_id: int, limit: int = 20) -> list[sqlite3.Row]:
    conn = get_connection()
    current_week_key, _month_key = ensure_period_state(guild_id)
    return conn.execute(
        """
        SELECT
            hw.user_id,
            SUM(hw.weekly_xp) AS weekly_xp,
            MAX(hw.updated_at) AS updated_at
        FROM host_weekly_xp hw
        INNER JOIN guild_members gm
          ON gm.guild_id = hw.guild_id
         AND gm.user_id = hw.user_id
         AND gm.member_state = 1
        INNER JOIN user_flags uf
          ON uf.guild_id = hw.guild_id
         AND uf.user_id = hw.user_id
        WHERE hw.guild_id = ?
          AND hw.week_key = ?
          AND COALESCE(uf.optout, 0) = 0
          AND COALESCE(uf.is_excluded, 0) = 0
          AND COALESCE(uf.rank_visible, 1) = 1
          AND uf.left_guild_at IS NULL
        GROUP BY hw.user_id
        HAVING SUM(hw.weekly_xp) > 0
        ORDER BY SUM(hw.weekly_xp) DESC, MAX(hw.updated_at) ASC, hw.user_id ASC
        LIMIT ?
        """,
        (guild_id, current_week_key, limit),
    ).fetchall()
