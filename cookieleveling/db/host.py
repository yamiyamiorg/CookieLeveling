import sqlite3
from datetime import datetime, timezone
from typing import Iterable, Optional

from cookieleveling.domain.period import normalize_week_key

from .core import get_connection
from .period import ensure_period_state
from .users import ensure_user


def add_host_target_channel(guild_id: int, channel_id: int, created_at: str) -> None:
    conn = get_connection()
    conn.execute(
        """
        INSERT OR IGNORE INTO host_target_channels (guild_id, channel_id, created_at)
        VALUES (?, ?, ?)
        """,
        (guild_id, channel_id, created_at),
    )
    conn.commit()


def remove_host_target_channel(guild_id: int, channel_id: int) -> None:
    conn = get_connection()
    conn.execute(
        "DELETE FROM host_target_channels WHERE guild_id = ? AND channel_id = ?",
        (guild_id, channel_id),
    )
    conn.commit()


def fetch_host_target_channels(guild_id: int) -> set[int]:
    conn = get_connection()
    rows = conn.execute(
        "SELECT channel_id FROM host_target_channels WHERE guild_id = ?",
        (guild_id,),
    ).fetchall()
    return {int(row["channel_id"]) for row in rows}


def is_host_target_channel(guild_id: int, channel_id: int) -> bool:
    conn = get_connection()
    row = conn.execute(
        """
        SELECT 1
        FROM host_target_channels
        WHERE guild_id = ? AND channel_id = ?
        LIMIT 1
        """,
        (guild_id, channel_id),
    ).fetchone()
    return row is not None


def upsert_host_session(
    guild_id: int,
    channel_id: int,
    session_started_at: str,
    deadline_at: str,
    last_seen_at: str,
) -> None:
    conn = get_connection()
    conn.execute(
        """
        INSERT INTO vc_host_state (
            guild_id,
            channel_id,
            session_started_at,
            started_at,
            deadline_at,
            host_user_id,
            locked,
            last_seen_at,
            host_confirmed,
            host_timed_out
        ) VALUES (?, ?, ?, ?, ?, NULL, 0, ?, 0, 0)
        ON CONFLICT(guild_id, channel_id)
        DO UPDATE SET
            session_started_at = excluded.session_started_at,
            started_at = excluded.started_at,
            deadline_at = excluded.deadline_at,
            host_user_id = NULL,
            locked = 0,
            last_seen_at = excluded.last_seen_at,
            host_confirmed = 0,
            host_timed_out = 0
        """,
        (
            guild_id,
            channel_id,
            session_started_at,
            session_started_at,
            deadline_at,
            last_seen_at,
        ),
    )
    conn.commit()


def clear_host_session(guild_id: int, channel_id: int) -> None:
    conn = get_connection()
    conn.execute(
        "DELETE FROM vc_host_state WHERE guild_id = ? AND channel_id = ?",
        (guild_id, channel_id),
    )
    conn.commit()


def mark_host_timeout(guild_id: int, channel_id: int) -> None:
    conn = get_connection()
    conn.execute(
        """
        UPDATE vc_host_state
        SET host_timed_out = 1, locked = 0
        WHERE guild_id = ? AND channel_id = ?
        """,
        (guild_id, channel_id),
    )
    conn.commit()


def confirm_host(
    guild_id: int,
    channel_id: int,
    host_user_id: int,
) -> None:
    conn = get_connection()
    conn.execute(
        """
        UPDATE vc_host_state
        SET host_user_id = ?, locked = 1, host_confirmed = 1, host_timed_out = 0
        WHERE guild_id = ? AND channel_id = ?
        """,
        (host_user_id, guild_id, channel_id),
    )
    conn.commit()


def fetch_host_session(guild_id: int, channel_id: int) -> Optional[sqlite3.Row]:
    conn = get_connection()
    return conn.execute(
        """
        SELECT guild_id,
               channel_id,
               session_started_at,
               started_at,
               deadline_at,
               host_user_id,
               locked,
               last_seen_at,
               host_confirmed,
               host_timed_out
        FROM vc_host_state
        WHERE guild_id = ? AND channel_id = ?
        """,
        (guild_id, channel_id),
    ).fetchone()


def fetch_host_sessions(guild_id: int) -> Iterable[sqlite3.Row]:
    conn = get_connection()
    return conn.execute(
        """
        SELECT guild_id,
               channel_id,
               session_started_at,
               started_at,
               deadline_at,
               host_user_id,
               locked,
               last_seen_at,
               host_confirmed,
               host_timed_out
        FROM vc_host_state
        WHERE guild_id = ?
        """,
        (guild_id,),
    ).fetchall()


def update_host_last_seen(
    guild_id: int,
    channel_id: int,
    last_seen_at: str,
) -> None:
    conn = get_connection()
    conn.execute(
        """
        UPDATE vc_host_state
        SET last_seen_at = ?
        WHERE guild_id = ? AND channel_id = ?
        """,
        (last_seen_at, guild_id, channel_id),
    )
    conn.commit()


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


def fetch_host_top20_monthly(guild_id: int, limit: int = 20) -> Iterable[sqlite3.Row]:
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


def fetch_host_top20_total(guild_id: int, limit: int = 20) -> Iterable[sqlite3.Row]:
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


def fetch_host_top20_weekly(guild_id: int, limit: int = 20) -> Iterable[sqlite3.Row]:
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
