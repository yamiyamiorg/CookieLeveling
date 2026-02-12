import sqlite3
from datetime import datetime, timezone
from typing import Iterable, Optional

from cookieleveling.domain.period import normalize_week_key

from .core import get_connection
from .period import ensure_period_state


def reset_voice_states(guild_id: int) -> None:
    conn = get_connection()
    conn.execute(
        "UPDATE voice_state SET is_in_vc = 0, joined_at = NULL WHERE guild_id = ?",
        (guild_id,),
    )
    conn.commit()


def upsert_voice_state(
    guild_id: int, user_id: int, is_in_vc: bool, joined_at: Optional[str]
) -> None:
    ensure_user(guild_id, user_id)
    conn = get_connection()
    conn.execute(
        """
        INSERT INTO voice_state (guild_id, user_id, is_in_vc, joined_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(guild_id, user_id)
        DO UPDATE SET is_in_vc = excluded.is_in_vc, joined_at = excluded.joined_at
        """,
        (guild_id, user_id, int(is_in_vc), joined_at),
    )
    conn.commit()


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


def fetch_active_voice_users(guild_id: int) -> Iterable[sqlite3.Row]:
    conn = get_connection()
    ensure_period_state(guild_id)
    return conn.execute(
        """
        SELECT vs.user_id, vs.joined_at, COALESCE(vx.lifetime_xp, 0) AS lifetime_xp
        FROM voice_state vs
        INNER JOIN guild_members gm
          ON gm.guild_id = vs.guild_id
         AND gm.user_id = vs.user_id
         AND gm.member_state = 1
        INNER JOIN user_flags uf
          ON uf.guild_id = vs.guild_id
         AND uf.user_id = vs.user_id
        LEFT JOIN voice_xp vx
          ON vx.guild_id = vs.guild_id
         AND vx.user_id = vs.user_id
        WHERE vs.guild_id = ?
          AND vs.is_in_vc = 1
          AND COALESCE(uf.optout, 0) = 0
          AND COALESCE(uf.is_excluded, 0) = 0
          AND uf.left_guild_at IS NULL
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
    _week_key, month_key = ensure_period_state(guild_id)
    conn.execute(
        """
        UPDATE voice_xp
        SET monthly_xp = CASE
                WHEN monthly_key = ? THEN monthly_xp + ?
                ELSE ?
            END,
            monthly_key = ?,
            lifetime_xp = lifetime_xp + ?,
            last_earned_at = ?
        WHERE guild_id = ? AND user_id = ?
        """,
        (
            month_key,
            season_inc,
            season_inc,
            month_key,
            lifetime_inc,
            last_earned_at,
            guild_id,
            user_id,
        ),
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
    _week_key, month_key = ensure_period_state(guild_id)
    conn.execute(
        """
        UPDATE voice_xp
        SET monthly_xp = CASE
                WHEN monthly_key = ? THEN monthly_xp + ?
                ELSE ?
            END,
            monthly_key = ?,
            lifetime_xp = lifetime_xp + ?,
            last_earned_at = COALESCE(?, last_earned_at)
        WHERE guild_id = ? AND user_id = ?
        """,
        (
            month_key,
            season_inc,
            season_inc,
            month_key,
            lifetime_inc,
            last_earned_at,
            guild_id,
            user_id,
        ),
    )
    conn.commit()


def add_user_weekly_xp(
    guild_id: int,
    user_id: int,
    weekly_inc: int,
    week_key: Optional[str] = None,
    updated_at: Optional[str] = None,
) -> None:
    ensure_user(guild_id, user_id)
    conn = get_connection()
    current_week_key, _month_key = ensure_period_state(guild_id)
    target_week_key = normalize_week_key(week_key) or current_week_key
    if updated_at is None:
        updated_at = datetime.now(timezone.utc).isoformat()

    conn.execute(
        """
        UPDATE voice_xp
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
        INSERT INTO user_weekly_xp (
            guild_id,
            week_key,
            user_id,
            weekly_xp,
            updated_at
        ) VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(guild_id, week_key, user_id)
        DO UPDATE SET
            weekly_xp = user_weekly_xp.weekly_xp + excluded.weekly_xp,
            updated_at = excluded.updated_at
        """,
        (guild_id, target_week_key, user_id, weekly_inc, updated_at),
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
    _week_key, month_key = ensure_period_state(guild_id)
    conn.execute(
        """
        UPDATE voice_xp
        SET monthly_xp = ?,
            monthly_key = ?,
            lifetime_xp = ?,
            last_earned_at = ?
        WHERE guild_id = ? AND user_id = ?
        """,
        (season_xp, month_key, lifetime_xp, last_earned_at, guild_id, user_id),
    )
    conn.commit()


def set_voice_state(
    guild_id: int, user_id: int, is_in_vc: bool, joined_at: Optional[str]
) -> None:
    upsert_voice_state(guild_id, user_id, is_in_vc, joined_at)


def reset_season_xp(guild_id: int) -> None:
    conn = get_connection()
    _week_key, month_key = ensure_period_state(guild_id)
    conn.execute(
        """
        UPDATE voice_xp
        SET monthly_xp = 0,
            monthly_key = ?
        WHERE guild_id = ?
        """,
        (month_key, guild_id),
    )
    conn.commit()


def reset_weekly_xp(guild_id: int) -> None:
    conn = get_connection()
    week_key, _month_key = ensure_period_state(guild_id)
    conn.execute(
        """
        UPDATE voice_xp
        SET weekly_xp = 0,
            weekly_key = ?
        WHERE guild_id = ?
        """,
        (week_key, guild_id),
    )
    conn.commit()


def fetch_rank_candidates(guild_id: int, limit: int = 20) -> Iterable[sqlite3.Row]:
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


def fetch_weekly_candidates(
    guild_id: int,
    limit: int = 20,
) -> Iterable[sqlite3.Row]:
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


def fetch_lifetime_candidates(guild_id: int, limit: int = 20) -> Iterable[sqlite3.Row]:
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


def fetch_lifetime_users(guild_id: int) -> Iterable[sqlite3.Row]:
    conn = get_connection()
    ensure_period_state(guild_id)
    return conn.execute(
        """
        SELECT vx.user_id, vx.lifetime_xp
        FROM voice_xp vx
        INNER JOIN guild_members gm
          ON gm.guild_id = vx.guild_id
         AND gm.user_id = vx.user_id
         AND gm.member_state = 1
        INNER JOIN user_flags uf
          ON uf.guild_id = vx.guild_id
         AND uf.user_id = vx.user_id
        WHERE vx.guild_id = ?
          AND uf.left_guild_at IS NULL
        """,
        (guild_id,),
    ).fetchall()


def fetch_voice_states(guild_id: int) -> Iterable[sqlite3.Row]:
    conn = get_connection()
    return conn.execute(
        """
        SELECT user_id, is_in_vc, joined_at
        FROM voice_state
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
        FROM voice_state
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
            INSERT INTO voice_state (guild_id, user_id, is_in_vc, joined_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(guild_id, user_id)
            DO UPDATE SET is_in_vc = excluded.is_in_vc, joined_at = excluded.joined_at
            """,
            inserts,
        )
        for guild, user_id, _is_in_vc, _joined in inserts:
            conn.execute(
                "INSERT OR IGNORE INTO user_flags (guild_id, user_id) VALUES (?, ?)",
                (guild, user_id),
            )
            week_key, month_key = ensure_period_state(guild)
            conn.execute(
                """
                INSERT OR IGNORE INTO voice_xp (guild_id, user_id, monthly_key, weekly_key)
                VALUES (?, ?, ?, ?)
                """,
                (guild, user_id, month_key, week_key),
            )

    if updates:
        conn.executemany(
            """
            UPDATE voice_state
            SET is_in_vc = ?, joined_at = ?
            WHERE guild_id = ? AND user_id = ?
            """,
            updates,
        )

    if inserts or updates:
        conn.commit()


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()
