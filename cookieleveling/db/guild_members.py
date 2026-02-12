import sqlite3
from typing import Iterable

from .core import get_connection


def upsert_guild_members(rows: Iterable[tuple]) -> None:
    rows = list(rows)
    if not rows:
        return
    conn = get_connection()
    conn.executemany(
        """
        INSERT INTO guild_members (
            guild_id,
            user_id,
            member_state,
            last_seen_at,
            display_name_cache,
            avatar_url_cache
        )
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(guild_id, user_id)
        DO UPDATE SET
            member_state = excluded.member_state,
            last_seen_at = excluded.last_seen_at,
            display_name_cache = COALESCE(excluded.display_name_cache, guild_members.display_name_cache),
            avatar_url_cache = COALESCE(excluded.avatar_url_cache, guild_members.avatar_url_cache)
        """,
        rows,
    )
    conn.commit()


def set_member_state(
    guild_id: int, user_id: int, member_state: int, last_seen_at: str | None
) -> bool:
    conn = get_connection()
    cursor = conn.execute(
        """
        INSERT INTO guild_members (guild_id, user_id, member_state, last_seen_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(guild_id, user_id)
        DO UPDATE SET
            member_state = excluded.member_state,
            last_seen_at = excluded.last_seen_at
        """,
        (guild_id, user_id, member_state, last_seen_at),
    )
    conn.commit()
    return cursor.rowcount > 0


def update_member_cache(
    guild_id: int,
    user_id: int,
    display_name: str | None,
    avatar_url: str | None,
    last_seen_at: str | None,
) -> None:
    conn = get_connection()
    conn.execute(
        """
        INSERT INTO guild_members (
            guild_id,
            user_id,
            display_name_cache,
            avatar_url_cache,
            last_seen_at
        )
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(guild_id, user_id)
        DO UPDATE SET
            display_name_cache = COALESCE(excluded.display_name_cache, guild_members.display_name_cache),
            avatar_url_cache = COALESCE(excluded.avatar_url_cache, guild_members.avatar_url_cache),
            last_seen_at = excluded.last_seen_at
        """,
        (guild_id, user_id, display_name, avatar_url, last_seen_at),
    )
    conn.commit()


def fetch_member_ids(guild_id: int) -> set[int]:
    conn = get_connection()
    rows = conn.execute(
        """
        SELECT user_id
        FROM guild_members
        WHERE guild_id = ?
        """,
        (guild_id,),
    ).fetchall()
    return {row["user_id"] for row in rows}


def mark_members_left(guild_id: int, user_ids: Iterable[int], left_at: str) -> int:
    ids = list(user_ids)
    if not ids:
        return 0
    conn = get_connection()
    rows = [(left_at, guild_id, user_id) for user_id in ids]
    cursor = conn.executemany(
        """
        UPDATE guild_members
        SET member_state = 2,
            last_seen_at = ?
        WHERE guild_id = ? AND user_id = ? AND member_state != 2
        """,
        rows,
    )
    conn.commit()
    return cursor.rowcount


def fetch_member_caches(
    guild_id: int, user_ids: Iterable[int]
) -> dict[int, sqlite3.Row]:
    ids = list(user_ids)
    if not ids:
        return {}
    conn = get_connection()
    placeholders = ",".join("?" for _ in ids)
    rows = conn.execute(
        f"""
        SELECT user_id, display_name_cache, avatar_url_cache, member_state
        FROM guild_members
        WHERE guild_id = ?
          AND user_id IN ({placeholders})
        """,
        (guild_id, *ids),
    ).fetchall()
    return {row["user_id"]: row for row in rows}
