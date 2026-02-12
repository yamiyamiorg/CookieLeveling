import sqlite3
from typing import Optional

from .core import get_connection


def fetch_guild_settings(guild_id: int) -> Optional[sqlite3.Row]:
    conn = get_connection()
    return conn.execute(
        """
        SELECT guild_id,
               weekly_channel_id,
               weekly_message_id,
               season_channel_id,
               season_message_id,
               lifetime_channel_id,
               lifetime_message_id,
               host_monthly_channel_id,
               host_monthly_message_id,
               host_total_channel_id,
               host_total_message_id,
               host_weekly_channel_id,
               host_weekly_message_id,
               updated_at
        FROM guild_settings
        WHERE guild_id = ?
        """,
        (guild_id,),
    ).fetchone()


def upsert_guild_settings(
    guild_id: int,
    *,
    weekly_channel_id: int,
    weekly_message_id: int,
    season_channel_id: int,
    season_message_id: int,
    lifetime_channel_id: int,
    lifetime_message_id: int,
) -> None:
    conn = get_connection()
    conn.execute(
        """
        INSERT INTO guild_settings (
            guild_id,
            weekly_channel_id,
            weekly_message_id,
            season_channel_id,
            season_message_id,
            lifetime_channel_id,
            lifetime_message_id,
            updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now'))
        ON CONFLICT(guild_id)
        DO UPDATE SET
            weekly_channel_id = excluded.weekly_channel_id,
            weekly_message_id = excluded.weekly_message_id,
            season_channel_id = excluded.season_channel_id,
            season_message_id = excluded.season_message_id,
            lifetime_channel_id = excluded.lifetime_channel_id,
            lifetime_message_id = excluded.lifetime_message_id,
            updated_at = datetime('now')
        """,
        (
            guild_id,
            weekly_channel_id,
            weekly_message_id,
            season_channel_id,
            season_message_id,
            lifetime_channel_id,
            lifetime_message_id,
        ),
    )
    conn.commit()


def fetch_hostboard_settings(guild_id: int) -> Optional[sqlite3.Row]:
    conn = get_connection()
    return conn.execute(
        """
        SELECT guild_id,
               host_monthly_channel_id,
               host_monthly_message_id,
               host_total_channel_id,
               host_total_message_id,
               host_weekly_channel_id,
               host_weekly_message_id,
               updated_at
        FROM guild_settings
        WHERE guild_id = ?
        """,
        (guild_id,),
    ).fetchone()


def upsert_hostboard_settings(
    guild_id: int,
    *,
    host_monthly_channel_id: int,
    host_monthly_message_id: int,
    host_total_channel_id: int,
    host_total_message_id: int,
    host_weekly_channel_id: int,
    host_weekly_message_id: int,
) -> None:
    conn = get_connection()
    conn.execute(
        """
        INSERT INTO guild_settings (
            guild_id,
            host_monthly_channel_id,
            host_monthly_message_id,
            host_total_channel_id,
            host_total_message_id,
            host_weekly_channel_id,
            host_weekly_message_id,
            updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now'))
        ON CONFLICT(guild_id)
        DO UPDATE SET
            host_monthly_channel_id = excluded.host_monthly_channel_id,
            host_monthly_message_id = excluded.host_monthly_message_id,
            host_total_channel_id = excluded.host_total_channel_id,
            host_total_message_id = excluded.host_total_message_id,
            host_weekly_channel_id = excluded.host_weekly_channel_id,
            host_weekly_message_id = excluded.host_weekly_message_id,
            updated_at = datetime('now')
        """,
        (
            guild_id,
            host_monthly_channel_id,
            host_monthly_message_id,
            host_total_channel_id,
            host_total_message_id,
            host_weekly_channel_id,
            host_weekly_message_id,
        ),
    )
    conn.commit()
