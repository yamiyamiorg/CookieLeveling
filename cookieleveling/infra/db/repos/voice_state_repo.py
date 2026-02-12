from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from typing import Iterable, Optional

from cookieleveling.db.core import get_connection
from cookieleveling.db.period import ensure_period_state

from .user_flags_repo import ensure_user


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


def set_voice_state(
    guild_id: int, user_id: int, is_in_vc: bool, joined_at: Optional[str]
) -> None:
    upsert_voice_state(guild_id, user_id, is_in_vc, joined_at)


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
