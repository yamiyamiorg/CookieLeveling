from __future__ import annotations

from cookieleveling.db.core import get_connection


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
