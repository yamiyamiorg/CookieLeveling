import sqlite3
from typing import Iterable, Optional

from .core import get_connection
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
    conn.execute(
        """
        INSERT OR IGNORE INTO host_stats (guild_id, user_id)
        VALUES (?, ?)
        """,
        (guild_id, user_id),
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
    conn.execute(
        """
        UPDATE host_stats
        SET monthly_xp = monthly_xp + ?,
            total_xp = total_xp + ?,
            last_earned_at = ?
        WHERE guild_id = ? AND user_id = ?
        """,
        (monthly_inc, total_inc, last_earned_at, guild_id, user_id),
    )
    conn.commit()


def add_host_weekly_xp(
    *,
    guild_id: int,
    week_key: str,
    user_id: int,
    weekly_inc: int,
    updated_at: str,
) -> None:
    conn = get_connection()
    conn.execute(
        """
        INSERT INTO host_weekly_xp (guild_id, week_key, user_id, weekly_xp, updated_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(guild_id, week_key, user_id)
        DO UPDATE SET
            weekly_xp = host_weekly_xp.weekly_xp + excluded.weekly_xp,
            updated_at = excluded.updated_at
        """,
        (guild_id, week_key, user_id, weekly_inc, updated_at),
    )
    conn.commit()


def increment_host_session_counts(guild_id: int, user_id: int) -> None:
    ensure_host_user(guild_id, user_id)
    conn = get_connection()
    conn.execute(
        """
        UPDATE host_stats
        SET monthly_sessions = monthly_sessions + 1,
            total_sessions = total_sessions + 1
        WHERE guild_id = ? AND user_id = ?
        """,
        (guild_id, user_id),
    )
    conn.commit()


def fetch_host_top20_monthly(guild_id: int) -> Iterable[sqlite3.Row]:
    conn = get_connection()
    return conn.execute(
        """
        SELECT hs.user_id, hs.monthly_xp, hs.last_earned_at
        FROM host_stats hs
        INNER JOIN guild_members gm
          ON gm.guild_id = hs.guild_id
         AND gm.user_id = hs.user_id
         AND gm.member_state = 1
        LEFT JOIN users u
          ON u.guild_id = hs.guild_id AND u.user_id = hs.user_id
        WHERE hs.guild_id = ?
          AND hs.monthly_xp > 0
          AND COALESCE(u.is_excluded, 0) = 0
          AND COALESCE(u.rank_visible, 1) = 1
        """,
        (guild_id,),
    ).fetchall()


def fetch_host_top20_total(guild_id: int) -> Iterable[sqlite3.Row]:
    conn = get_connection()
    return conn.execute(
        """
        SELECT hs.user_id, hs.total_xp, hs.last_earned_at
        FROM host_stats hs
        INNER JOIN guild_members gm
          ON gm.guild_id = hs.guild_id
         AND gm.user_id = hs.user_id
         AND gm.member_state = 1
        LEFT JOIN users u
          ON u.guild_id = hs.guild_id AND u.user_id = hs.user_id
        WHERE hs.guild_id = ?
          AND hs.total_xp > 0
          AND COALESCE(u.is_excluded, 0) = 0
          AND COALESCE(u.rank_visible, 1) = 1
        """,
        (guild_id,),
    ).fetchall()


def fetch_host_top20_weekly(
    guild_id: int, week_key: str
) -> Iterable[sqlite3.Row]:
    conn = get_connection()
    return conn.execute(
        """
        SELECT hw.user_id, hw.weekly_xp, hw.updated_at
        FROM host_weekly_xp hw
        INNER JOIN guild_members gm
          ON gm.guild_id = hw.guild_id
         AND gm.user_id = hw.user_id
         AND gm.member_state = 1
        LEFT JOIN users u
          ON u.guild_id = hw.guild_id AND u.user_id = hw.user_id
        WHERE hw.guild_id = ?
          AND hw.week_key = ?
          AND hw.weekly_xp > 0
          AND COALESCE(u.is_excluded, 0) = 0
          AND COALESCE(u.rank_visible, 1) = 1
        """,
        (guild_id, week_key),
    ).fetchall()


def reset_host_monthly(guild_id: int) -> None:
    conn = get_connection()
    conn.execute(
        """
        UPDATE host_stats
        SET monthly_xp = 0,
            monthly_sessions = 0
        WHERE guild_id = ?
        """,
        (guild_id,),
    )
    conn.commit()
