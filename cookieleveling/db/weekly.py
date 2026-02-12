from __future__ import annotations

from .core import get_connection


def prune_weekly_xp(min_week_key: str) -> None:
    conn = get_connection()
    if _table_exists(conn, "user_weekly_xp"):
        conn.execute(
            "DELETE FROM user_weekly_xp WHERE week_key < ?",
            (min_week_key,),
        )
    if _table_exists(conn, "host_weekly_xp"):
        conn.execute(
            "DELETE FROM host_weekly_xp WHERE week_key < ?",
            (min_week_key,),
        )
    conn.commit()


def _table_exists(conn, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name = ?",
        (table_name,),
    ).fetchone()
    return row is not None
