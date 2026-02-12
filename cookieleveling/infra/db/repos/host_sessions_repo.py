from __future__ import annotations

import sqlite3
from typing import Iterable, Optional

from cookieleveling.db.core import get_connection


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


def confirm_host(guild_id: int, channel_id: int, host_user_id: int) -> None:
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


def update_host_last_seen(guild_id: int, channel_id: int, last_seen_at: str) -> None:
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
