from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timezone

from cookieleveling.domain.period import current_month_key, current_week_key

from .core import get_connection, get_meta_value

_LOGGER = logging.getLogger(__name__)


def ensure_period_state(guild_id: int) -> tuple[str, str]:
    conn = get_connection()
    row = conn.execute(
        """
        SELECT current_week_key, current_month_key
        FROM period_state
        WHERE guild_id = ?
        """,
        (guild_id,),
    ).fetchone()

    now_week_key = current_week_key()
    now_month_key = current_month_key()
    now_iso = datetime.now(timezone.utc).isoformat()

    if row is None:
        conn.execute(
            """
            INSERT INTO period_state (guild_id, current_week_key, current_month_key, updated_at)
            VALUES (?, ?, ?, ?)
            """,
            (guild_id, now_week_key, now_month_key, now_iso),
        )
        conn.commit()
        return now_week_key, now_month_key

    prev_week_key = str(row["current_week_key"])
    prev_month_key = str(row["current_month_key"])

    week_changed = prev_week_key != now_week_key
    month_changed = prev_month_key != now_month_key
    if not week_changed and not month_changed:
        return prev_week_key, prev_month_key

    if month_changed:
        conn.execute(
            """
            UPDATE voice_xp
            SET monthly_xp = 0,
                monthly_key = ?
            WHERE guild_id = ?
              AND monthly_key != ?
            """,
            (now_month_key, guild_id, now_month_key),
        )
        conn.execute(
            """
            UPDATE host_xp
            SET monthly_xp = 0,
                monthly_sessions = 0,
                monthly_key = ?
            WHERE guild_id = ?
              AND monthly_key != ?
            """,
            (now_month_key, guild_id, now_month_key),
        )

    conn.execute(
        """
        UPDATE period_state
        SET current_week_key = ?,
            current_month_key = ?,
            updated_at = ?
        WHERE guild_id = ?
        """,
        (now_week_key, now_month_key, now_iso, guild_id),
    )
    conn.commit()

    _LOGGER.info(
        "period rollover applied: guild_id=%s week_changed=%s month_changed=%s week_key=%s month_key=%s",
        guild_id,
        int(week_changed),
        int(month_changed),
        now_week_key,
        now_month_key,
    )
    return now_week_key, now_month_key


def ensure_weekly_reset(guild_id: int) -> bool:
    current_week, _month_key = ensure_period_state(guild_id)
    conn = get_connection()
    last_weekly_reset_key = get_meta_value("last_weekly_reset_key")
    should_reset = last_weekly_reset_key != current_week
    _LOGGER.info(
        "weekly reset check: guild_id=%s current_week_key=%s last_weekly_reset_key=%s reset=%s",
        guild_id,
        current_week,
        last_weekly_reset_key,
        int(should_reset),
    )
    if not should_reset:
        return False

    conn.execute("BEGIN IMMEDIATE")
    try:
        users_cursor = conn.execute(
            """
            UPDATE voice_xp
            SET weekly_xp = 0,
                weekly_key = ?
            WHERE guild_id = ?
            """,
            (current_week, guild_id),
        )
        host_cursor = conn.execute(
            """
            UPDATE host_xp
            SET weekly_xp = 0,
                weekly_sessions = 0,
                weekly_key = ?
            WHERE guild_id = ?
            """,
            (current_week, guild_id),
        )
        conn.execute(
            """
            INSERT INTO meta (key, value)
            VALUES ('last_weekly_reset_key', ?)
            ON CONFLICT(key)
            DO UPDATE SET value = excluded.value
            """,
            (current_week,),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise

    _LOGGER.info(
        "weekly reset users zeroed: guild_id=%s rows=%s week_key=%s",
        guild_id,
        users_cursor.rowcount,
        current_week,
    )
    _LOGGER.info(
        "weekly reset host_xp zeroed: guild_id=%s rows=%s week_key=%s",
        guild_id,
        host_cursor.rowcount,
        current_week,
    )
    return True


def fetch_period_state(guild_id: int) -> sqlite3.Row | None:
    conn = get_connection()
    return conn.execute(
        """
        SELECT guild_id, current_week_key, current_month_key, updated_at
        FROM period_state
        WHERE guild_id = ?
        """,
        (guild_id,),
    ).fetchone()
