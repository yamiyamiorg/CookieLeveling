import sqlite3
from datetime import datetime
from typing import Iterable, Optional
from zoneinfo import ZoneInfo

from .core import get_connection

_JST = ZoneInfo("Asia/Tokyo")


def reset_voice_states(guild_id: int) -> None:
    conn = get_connection()
    conn.execute(
        "UPDATE users SET is_in_vc = 0, joined_at = NULL WHERE guild_id = ?",
        (guild_id,),
    )
    conn.commit()


def upsert_voice_state(
    guild_id: int, user_id: int, is_in_vc: bool, joined_at: Optional[str]
) -> None:
    conn = get_connection()
    conn.execute(
        """
        INSERT INTO users (guild_id, user_id, is_in_vc, joined_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(guild_id, user_id)
        DO UPDATE SET is_in_vc = excluded.is_in_vc, joined_at = excluded.joined_at
        """,
        (guild_id, user_id, int(is_in_vc), joined_at),
    )
    conn.commit()


def ensure_user(guild_id: int, user_id: int) -> None:
    conn = get_connection()
    conn.execute(
        """
        INSERT OR IGNORE INTO users (guild_id, user_id)
        VALUES (?, ?)
        """,
        (guild_id, user_id),
    )
    conn.commit()


def set_optout(guild_id: int, user_id: int, optout: bool) -> None:
    ensure_user(guild_id, user_id)
    conn = get_connection()
    conn.execute(
        "UPDATE users SET optout = ? WHERE guild_id = ? AND user_id = ?",
        (int(optout), guild_id, user_id),
    )
    conn.commit()


def set_excluded(guild_id: int, user_id: int, excluded: bool) -> None:
    ensure_user(guild_id, user_id)
    conn = get_connection()
    conn.execute(
        "UPDATE users SET is_excluded = ? WHERE guild_id = ? AND user_id = ?",
        (int(excluded), guild_id, user_id),
    )
    conn.commit()


def set_rank_visible(guild_id: int, user_id: int, visible: bool) -> None:
    ensure_user(guild_id, user_id)
    conn = get_connection()
    conn.execute(
        "UPDATE users SET rank_visible = ? WHERE guild_id = ? AND user_id = ?",
        (int(visible), guild_id, user_id),
    )
    conn.commit()


def mark_user_deleted(guild_id: int, user_id: int, deleted_at: str) -> None:
    ensure_user(guild_id, user_id)
    conn = get_connection()
    conn.execute(
        "UPDATE users SET deleted_at = ? WHERE guild_id = ? AND user_id = ?",
        (deleted_at, guild_id, user_id),
    )
    conn.commit()


def clear_user_deleted(guild_id: int, user_id: int) -> None:
    ensure_user(guild_id, user_id)
    conn = get_connection()
    conn.execute(
        "UPDATE users SET deleted_at = NULL WHERE guild_id = ? AND user_id = ?",
        (guild_id, user_id),
    )
    conn.commit()


def mark_user_left(guild_id: int, user_id: int, left_at: str) -> bool:
    ensure_user(guild_id, user_id)
    conn = get_connection()
    cursor = conn.execute(
        """
        UPDATE users
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
        UPDATE users
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
        SELECT is_excluded, rank_visible, deleted_at, left_guild_at
        FROM users
        WHERE guild_id = ? AND user_id = ?
        """,
        (guild_id, user_id),
    ).fetchone()


def sync_excluded_users(guild_id: int, user_ids: Iterable[int]) -> None:
    rows = [(guild_id, user_id, 1) for user_id in user_ids]
    if not rows:
        return
    conn = get_connection()
    conn.executemany(
        """
        INSERT INTO users (guild_id, user_id, is_excluded)
        VALUES (?, ?, ?)
        ON CONFLICT(guild_id, user_id)
        DO UPDATE SET is_excluded = excluded.is_excluded
        """,
        rows,
    )
    conn.commit()


def fetch_user(guild_id: int, user_id: int) -> Optional[sqlite3.Row]:
    conn = get_connection()
    return conn.execute(
        """
        SELECT guild_id, user_id, season_xp, lifetime_xp, weekly_xp,
               optout, is_excluded, rank_visible, is_in_vc,
               joined_at, last_earned_at, deleted_at, left_guild_at
        FROM users
        WHERE guild_id = ? AND user_id = ?
        """,
        (guild_id, user_id),
    ).fetchone()


def fetch_active_voice_users(guild_id: int) -> Iterable[sqlite3.Row]:
    conn = get_connection()
    return conn.execute(
        """
        SELECT user_id, joined_at, lifetime_xp
        FROM users
        WHERE guild_id = ?
          AND is_in_vc = 1
          AND optout = 0
          AND is_excluded = 0
          AND deleted_at IS NULL
          AND left_guild_at IS NULL
        """,
        (guild_id,),
    ).fetchall()


def update_user_xp(
    *,
    guild_id: int,
    user_id: int,
    season_inc: int,
    lifetime_inc: int,
    last_earned_at: str,
) -> None:
    ensure_user(guild_id, user_id)
    conn = get_connection()
    conn.execute(
        """
        UPDATE users
        SET season_xp = season_xp + ?,
            lifetime_xp = lifetime_xp + ?,
            last_earned_at = ?
        WHERE guild_id = ? AND user_id = ?
        """,
        (season_inc, lifetime_inc, last_earned_at, guild_id, user_id),
    )
    conn.commit()


def grant_xp(
    guild_id: int,
    user_id: int,
    season_inc: int,
    lifetime_inc: int,
    last_earned_at: Optional[str],
) -> None:
    ensure_user(guild_id, user_id)
    conn = get_connection()
    conn.execute(
        """
        UPDATE users
        SET season_xp = season_xp + ?,
            lifetime_xp = lifetime_xp + ?,
            last_earned_at = COALESCE(?, last_earned_at)
        WHERE guild_id = ? AND user_id = ?
        """,
        (season_inc, lifetime_inc, last_earned_at, guild_id, user_id),
    )
    conn.commit()


def add_user_weekly_xp(
    *,
    guild_id: int,
    user_id: int,
    weekly_inc: int,
    week_key: Optional[str] = None,
    updated_at: Optional[str] = None,
) -> None:
    ensure_user(guild_id, user_id)
    if week_key is None:
        week_key = _week_key_now_jst()
    if updated_at is None:
        updated_at = datetime.utcnow().isoformat()
    conn = get_connection()
    conn.execute(
        """
        UPDATE users
        SET weekly_xp = weekly_xp + ?
        WHERE guild_id = ? AND user_id = ?
        """,
        (weekly_inc, guild_id, user_id),
    )
    conn.execute(
        """
        INSERT INTO user_weekly_xp (guild_id, week_key, user_id, weekly_xp, updated_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(guild_id, week_key, user_id)
        DO UPDATE SET
            weekly_xp = user_weekly_xp.weekly_xp + excluded.weekly_xp,
            updated_at = excluded.updated_at
        """,
        (guild_id, week_key, user_id, weekly_inc, updated_at),
    )
    conn.commit()


def set_xp(
    guild_id: int,
    user_id: int,
    season_xp: int,
    lifetime_xp: int,
    last_earned_at: str,
) -> None:
    ensure_user(guild_id, user_id)
    conn = get_connection()
    conn.execute(
        """
        UPDATE users
        SET season_xp = ?,
            lifetime_xp = ?,
            last_earned_at = ?
        WHERE guild_id = ? AND user_id = ?
        """,
        (season_xp, lifetime_xp, last_earned_at, guild_id, user_id),
    )
    conn.commit()


def set_voice_state(
    guild_id: int, user_id: int, is_in_vc: bool, joined_at: Optional[str]
) -> None:
    ensure_user(guild_id, user_id)
    conn = get_connection()
    conn.execute(
        """
        UPDATE users
        SET is_in_vc = ?, joined_at = ?
        WHERE guild_id = ? AND user_id = ?
        """,
        (int(is_in_vc), joined_at, guild_id, user_id),
    )
    conn.commit()


def reset_season_xp(guild_id: int) -> None:
    conn = get_connection()
    conn.execute("UPDATE users SET season_xp = 0 WHERE guild_id = ?", (guild_id,))
    conn.commit()


def reset_weekly_xp(guild_id: int) -> None:
    conn = get_connection()
    conn.execute("UPDATE users SET weekly_xp = 0 WHERE guild_id = ?", (guild_id,))
    conn.commit()


def fetch_rank_candidates(guild_id: int) -> Iterable[sqlite3.Row]:
    conn = get_connection()
    return conn.execute(
        """
        SELECT u.user_id, u.season_xp, u.joined_at, u.last_earned_at, u.is_in_vc
        FROM users u
        INNER JOIN guild_members gm
          ON gm.guild_id = u.guild_id
         AND gm.user_id = u.user_id
         AND gm.member_state = 1
        WHERE u.guild_id = ?
          AND u.season_xp > 0
          AND u.optout = 0
          AND u.is_excluded = 0
          AND u.rank_visible = 1
        """,
        (guild_id,),
    ).fetchall()


def fetch_weekly_candidates(
    guild_id: int,
    week_key: Optional[str] = None,
) -> Iterable[sqlite3.Row]:
    conn = get_connection()
    if week_key is not None:
        return conn.execute(
            """
            SELECT uw.user_id, uw.weekly_xp, uw.updated_at AS last_earned_at
            FROM user_weekly_xp uw
            INNER JOIN users u
              ON u.guild_id = uw.guild_id
             AND u.user_id = uw.user_id
            INNER JOIN guild_members gm
              ON gm.guild_id = uw.guild_id
             AND gm.user_id = uw.user_id
             AND gm.member_state = 1
            WHERE uw.guild_id = ?
              AND uw.week_key = ?
              AND uw.weekly_xp > 0
              AND u.optout = 0
              AND u.is_excluded = 0
              AND u.rank_visible = 1
            """,
            (guild_id, week_key),
        ).fetchall()
    return conn.execute(
        """
        SELECT u.user_id, u.weekly_xp, u.last_earned_at
        FROM users u
        INNER JOIN guild_members gm
          ON gm.guild_id = u.guild_id
         AND gm.user_id = u.user_id
         AND gm.member_state = 1
        WHERE u.guild_id = ?
          AND u.weekly_xp > 0
          AND u.optout = 0
          AND u.is_excluded = 0
          AND u.rank_visible = 1
        """,
        (guild_id,),
    ).fetchall()


def _week_key_now_jst() -> str:
    now_jst = datetime.now(_JST)
    iso_year, iso_week, _iso_weekday = now_jst.isocalendar()
    return f"{iso_year}-W{iso_week:02d}"


def fetch_lifetime_candidates(guild_id: int) -> Iterable[sqlite3.Row]:
    conn = get_connection()
    return conn.execute(
        """
        SELECT u.user_id, u.lifetime_xp, u.last_earned_at
        FROM users u
        INNER JOIN guild_members gm
          ON gm.guild_id = u.guild_id
         AND gm.user_id = u.user_id
         AND gm.member_state = 1
        WHERE u.guild_id = ?
          AND u.lifetime_xp > 0
          AND u.optout = 0
          AND u.is_excluded = 0
          AND u.rank_visible = 1
        """,
        (guild_id,),
    ).fetchall()


def fetch_lifetime_users(guild_id: int) -> Iterable[sqlite3.Row]:
    conn = get_connection()
    return conn.execute(
        """
        SELECT user_id, lifetime_xp
        FROM users
        WHERE guild_id = ?
          AND deleted_at IS NULL
          AND left_guild_at IS NULL
        """,
        (guild_id,),
    ).fetchall()


def fetch_voice_states(guild_id: int) -> Iterable[sqlite3.Row]:
    conn = get_connection()
    return conn.execute(
        """
        SELECT user_id, is_in_vc, joined_at
        FROM users
        WHERE guild_id = ?
        ORDER BY user_id ASC
        """,
        (guild_id,),
    ).fetchall()


def apply_voice_snapshot(guild_id: int, current_user_ids: set[int], now: str) -> None:
    conn = get_connection()
    rows = conn.execute(
        """
        SELECT user_id, is_in_vc, joined_at
        FROM users
        WHERE guild_id = ?
        """,
        (guild_id,),
    ).fetchall()

    previous: dict[int, tuple[int, Optional[str]]] = {
        row["user_id"]: (row["is_in_vc"], row["joined_at"]) for row in rows
    }
    updates: list[tuple[int, Optional[str], int, int]] = []
    inserts: list[tuple[int, int, int, str]] = []

    for user_id in current_user_ids:
        prev = previous.get(user_id)
        if prev is None:
            inserts.append((guild_id, user_id, 1, now))
            continue
        prev_in_vc = bool(prev[0])
        if not prev_in_vc:
            updates.append((1, now, guild_id, user_id))

    for user_id, (is_in_vc, _joined_at) in previous.items():
        if is_in_vc and user_id not in current_user_ids:
            updates.append((0, None, guild_id, user_id))

    if inserts:
        conn.executemany(
            """
            INSERT INTO users (guild_id, user_id, is_in_vc, joined_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(guild_id, user_id)
            DO UPDATE SET is_in_vc = excluded.is_in_vc, joined_at = excluded.joined_at
            """,
            inserts,
        )
    if updates:
        conn.executemany(
            """
            UPDATE users
            SET is_in_vc = ?, joined_at = ?
            WHERE guild_id = ? AND user_id = ?
            """,
            updates,
        )
    if inserts or updates:
        conn.commit()
