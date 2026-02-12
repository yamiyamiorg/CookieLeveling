from __future__ import annotations

import sqlite3

from cookieleveling.db.core import get_connection
from cookieleveling.db.period import ensure_period_state


def fetch_rank_candidates(guild_id: int, limit: int = 20) -> list[sqlite3.Row]:
    conn = get_connection()
    _week_key, current_month_key = ensure_period_state(guild_id)
    return conn.execute(
        """
        SELECT
            vx.user_id,
            vx.monthly_xp AS season_xp,
            vs.joined_at,
            vx.last_earned_at,
            COALESCE(vs.is_in_vc, 0) AS is_in_vc
        FROM voice_xp vx
        INNER JOIN guild_members gm
          ON gm.guild_id = vx.guild_id
         AND gm.user_id = vx.user_id
         AND gm.member_state = 1
        INNER JOIN user_flags uf
          ON uf.guild_id = vx.guild_id
         AND uf.user_id = vx.user_id
        LEFT JOIN voice_state vs
          ON vs.guild_id = vx.guild_id
         AND vs.user_id = vx.user_id
        WHERE vx.guild_id = ?
          AND vx.monthly_key = ?
          AND vx.monthly_xp > 0
          AND COALESCE(uf.optout, 0) = 0
          AND COALESCE(uf.is_excluded, 0) = 0
          AND COALESCE(uf.rank_visible, 1) = 1
          AND uf.left_guild_at IS NULL
        ORDER BY vx.monthly_xp DESC, vx.last_earned_at ASC, vx.user_id ASC
        LIMIT ?
        """,
        (guild_id, current_month_key, limit),
    ).fetchall()


def fetch_weekly_candidates(guild_id: int, limit: int = 20) -> list[sqlite3.Row]:
    conn = get_connection()
    current_week_key, _month_key = ensure_period_state(guild_id)
    return conn.execute(
        """
        SELECT
            uw.user_id,
            SUM(uw.weekly_xp) AS weekly_xp,
            MAX(uw.updated_at) AS last_earned_at
        FROM user_weekly_xp uw
        INNER JOIN guild_members gm
          ON gm.guild_id = uw.guild_id
         AND gm.user_id = uw.user_id
         AND gm.member_state = 1
        INNER JOIN user_flags uf
          ON uf.guild_id = uw.guild_id
         AND uf.user_id = uw.user_id
        WHERE uw.guild_id = ?
          AND uw.week_key = ?
          AND COALESCE(uf.optout, 0) = 0
          AND COALESCE(uf.is_excluded, 0) = 0
          AND COALESCE(uf.rank_visible, 1) = 1
          AND uf.left_guild_at IS NULL
        GROUP BY uw.user_id
        HAVING SUM(uw.weekly_xp) > 0
        ORDER BY SUM(uw.weekly_xp) DESC, MAX(uw.updated_at) ASC, uw.user_id ASC
        LIMIT ?
        """,
        (guild_id, current_week_key, limit),
    ).fetchall()


def fetch_lifetime_candidates(guild_id: int, limit: int = 20) -> list[sqlite3.Row]:
    conn = get_connection()
    ensure_period_state(guild_id)
    return conn.execute(
        """
        SELECT vx.user_id, vx.lifetime_xp, vx.last_earned_at
        FROM voice_xp vx
        INNER JOIN guild_members gm
          ON gm.guild_id = vx.guild_id
         AND gm.user_id = vx.user_id
         AND gm.member_state = 1
        INNER JOIN user_flags uf
          ON uf.guild_id = vx.guild_id
         AND uf.user_id = vx.user_id
        WHERE vx.guild_id = ?
          AND vx.lifetime_xp > 0
          AND COALESCE(uf.optout, 0) = 0
          AND COALESCE(uf.is_excluded, 0) = 0
          AND COALESCE(uf.rank_visible, 1) = 1
          AND uf.left_guild_at IS NULL
        ORDER BY vx.lifetime_xp DESC, vx.last_earned_at ASC, vx.user_id ASC
        LIMIT ?
        """,
        (guild_id, limit),
    ).fetchall()
