from __future__ import annotations

import sqlite3
from typing import Iterable, Optional

from cookieleveling.db.core import get_connection
from cookieleveling.db.period import ensure_period_state


def ensure_user(guild_id: int, user_id: int) -> None:
    conn = get_connection()
    week_key, month_key = ensure_period_state(guild_id)
    conn.execute(
        """
        INSERT OR IGNORE INTO user_flags (guild_id, user_id)
        VALUES (?, ?)
        """,
        (guild_id, user_id),
    )
    conn.execute(
        """
        INSERT OR IGNORE INTO voice_xp (
            guild_id,
            user_id,
            monthly_key,
            weekly_key
        )
        VALUES (?, ?, ?, ?)
        """,
        (guild_id, user_id, month_key, week_key),
    )
    conn.execute(
        """
        INSERT OR IGNORE INTO voice_state (guild_id, user_id)
        VALUES (?, ?)
        """,
        (guild_id, user_id),
    )
    conn.commit()


def set_optout(guild_id: int, user_id: int, optout: bool) -> None:
    ensure_user(guild_id, user_id)
    conn = get_connection()
    conn.execute(
        "UPDATE user_flags SET optout = ? WHERE guild_id = ? AND user_id = ?",
        (int(optout), guild_id, user_id),
    )
    conn.commit()


def set_excluded(guild_id: int, user_id: int, excluded: bool) -> None:
    ensure_user(guild_id, user_id)
    conn = get_connection()
    conn.execute(
        "UPDATE user_flags SET is_excluded = ? WHERE guild_id = ? AND user_id = ?",
        (int(excluded), guild_id, user_id),
    )
    conn.commit()


def set_rank_visible(guild_id: int, user_id: int, visible: bool) -> None:
    ensure_user(guild_id, user_id)
    conn = get_connection()
    conn.execute(
        "UPDATE user_flags SET rank_visible = ? WHERE guild_id = ? AND user_id = ?",
        (int(visible), guild_id, user_id),
    )
    conn.commit()


def mark_user_deleted(guild_id: int, user_id: int, deleted_at: str) -> None:
    ensure_user(guild_id, user_id)
    conn = get_connection()
    conn.execute(
        "UPDATE user_flags SET deleted_at = ? WHERE guild_id = ? AND user_id = ?",
        (deleted_at, guild_id, user_id),
    )
    conn.commit()


def clear_user_deleted(guild_id: int, user_id: int) -> None:
    ensure_user(guild_id, user_id)
    conn = get_connection()
    conn.execute(
        "UPDATE user_flags SET deleted_at = NULL WHERE guild_id = ? AND user_id = ?",
        (guild_id, user_id),
    )
    conn.commit()


def mark_user_left(guild_id: int, user_id: int, left_at: str) -> bool:
    ensure_user(guild_id, user_id)
    conn = get_connection()
    cursor = conn.execute(
        """
        UPDATE user_flags
        SET left_guild_at = ?
        WHERE guild_id = ? AND user_id = ? AND left_guild_at IS NULL
        """,
        (left_at, guild_id, user_id),
    )
    conn.commit()
    return cursor.rowcount > 0


def clear_user_left(guild_id: int, user_id: int) -> bool:
    ensure_user(guild_id, user_id)
    conn = get_connection()
    cursor = conn.execute(
        """
        UPDATE user_flags
        SET left_guild_at = NULL
        WHERE guild_id = ? AND user_id = ? AND left_guild_at IS NOT NULL
        """,
        (guild_id, user_id),
    )
    conn.commit()
    return cursor.rowcount > 0


def fetch_user_flags(guild_id: int, user_id: int) -> Optional[sqlite3.Row]:
    conn = get_connection()
    return conn.execute(
        """
        SELECT
            optout,
            is_excluded,
            rank_visible,
            deleted_at,
            left_guild_at,
            display_name,
            avatar_url,
            last_seen_at
        FROM user_flags
        WHERE guild_id = ? AND user_id = ?
        """,
        (guild_id, user_id),
    ).fetchone()


def upsert_user_profile(
    guild_id: int,
    user_id: int,
    display_name: str | None,
    avatar_url: str | None,
    last_seen_at: str | None,
) -> None:
    ensure_user(guild_id, user_id)
    conn = get_connection()
    conn.execute(
        """
        UPDATE user_flags
        SET display_name = COALESCE(?, display_name),
            avatar_url = COALESCE(?, avatar_url),
            last_seen_at = COALESCE(?, last_seen_at)
        WHERE guild_id = ? AND user_id = ?
        """,
        (display_name, avatar_url, last_seen_at, guild_id, user_id),
    )
    conn.commit()


def upsert_user_profiles(rows: Iterable[tuple[int, int, str | None, str | None, str | None]]) -> None:
    values = list(rows)
    if not values:
        return
    conn = get_connection()
    conn.executemany(
        """
        INSERT INTO user_flags (
            guild_id,
            user_id,
            display_name,
            avatar_url,
            last_seen_at
        )
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(guild_id, user_id)
        DO UPDATE SET
            display_name = COALESCE(excluded.display_name, user_flags.display_name),
            avatar_url = COALESCE(excluded.avatar_url, user_flags.avatar_url),
            last_seen_at = COALESCE(excluded.last_seen_at, user_flags.last_seen_at)
        """,
        values,
    )
    conn.commit()


def sync_excluded_users(guild_id: int, user_ids: Iterable[int]) -> None:
    rows = [(guild_id, user_id, 1) for user_id in user_ids]
    if not rows:
        return
    conn = get_connection()
    conn.executemany(
        """
        INSERT INTO user_flags (guild_id, user_id, is_excluded)
        VALUES (?, ?, ?)
        ON CONFLICT(guild_id, user_id)
        DO UPDATE SET is_excluded = excluded.is_excluded
        """,
        rows,
    )
    conn.commit()


def fetch_user(guild_id: int, user_id: int) -> Optional[sqlite3.Row]:
    conn = get_connection()
    ensure_period_state(guild_id)
    return conn.execute(
        """
        SELECT
            uf.guild_id,
            uf.user_id,
            vx.monthly_xp AS season_xp,
            vx.lifetime_xp,
            vx.weekly_xp,
            uf.optout,
            uf.is_excluded,
            uf.rank_visible,
            COALESCE(vs.is_in_vc, 0) AS is_in_vc,
            vs.joined_at,
            vx.last_earned_at,
            uf.deleted_at,
            uf.left_guild_at
        FROM user_flags uf
        LEFT JOIN voice_xp vx
          ON vx.guild_id = uf.guild_id
         AND vx.user_id = uf.user_id
        LEFT JOIN voice_state vs
          ON vs.guild_id = uf.guild_id
         AND vs.user_id = uf.user_id
        WHERE uf.guild_id = ? AND uf.user_id = ?
        """,
        (guild_id, user_id),
    ).fetchone()
