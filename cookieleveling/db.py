import os
import sqlite3
from typing import Iterable, Optional

from .config import Config

_SCHEMA_VERSION = 1
_connection: Optional[sqlite3.Connection] = None


def get_connection() -> sqlite3.Connection:
    if _connection is None:
        raise RuntimeError("Database not initialized")
    return _connection


def init_db(config: Config) -> None:
    global _connection
    os.makedirs(config.data_dir, exist_ok=True)

    conn = sqlite3.connect(config.db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    _create_schema(conn)
    _connection = conn


def _create_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS meta (
            schema_version INTEGER NOT NULL,
            created_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS guild_settings (
            guild_id INTEGER PRIMARY KEY,
            rankboard_channel_id INTEGER,
            rankboard_message_id INTEGER,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            guild_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            season_xp INTEGER NOT NULL DEFAULT 0,
            lifetime_xp INTEGER NOT NULL DEFAULT 0,
            rem_lifetime REAL NOT NULL DEFAULT 0,
            optout INTEGER NOT NULL DEFAULT 0,
            is_in_vc INTEGER NOT NULL DEFAULT 0,
            joined_at TEXT,
            last_earned_at TEXT,
            PRIMARY KEY (guild_id, user_id)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS rank_role_last (
            guild_id INTEGER PRIMARY KEY,
            last_snapshot_json TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    _ensure_meta(conn)
    conn.commit()


def _ensure_meta(conn: sqlite3.Connection) -> None:
    row = conn.execute("SELECT schema_version FROM meta LIMIT 1").fetchone()
    if row is None:
        conn.execute(
            "INSERT INTO meta (schema_version, created_at) VALUES (?, datetime('now'))",
            (_SCHEMA_VERSION,),
        )


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


def fetch_user(guild_id: int, user_id: int) -> Optional[sqlite3.Row]:
    conn = get_connection()
    return conn.execute(
        """
        SELECT guild_id, user_id, season_xp, lifetime_xp, rem_lifetime,
               optout, is_in_vc, joined_at, last_earned_at
        FROM users
        WHERE guild_id = ? AND user_id = ?
        """,
        (guild_id, user_id),
    ).fetchone()


def fetch_active_voice_users(guild_id: int) -> Iterable[sqlite3.Row]:
    conn = get_connection()
    return conn.execute(
        """
        SELECT user_id, rem_lifetime
        FROM users
        WHERE guild_id = ? AND is_in_vc = 1 AND optout = 0
        """,
        (guild_id,),
    ).fetchall()


def update_user_xp(
    *,
    guild_id: int,
    user_id: int,
    season_inc: int,
    lifetime_inc: int,
    rem_lifetime: float,
    last_earned_at: str,
) -> None:
    ensure_user(guild_id, user_id)
    conn = get_connection()
    conn.execute(
        """
        UPDATE users
        SET season_xp = season_xp + ?,
            lifetime_xp = lifetime_xp + ?,
            rem_lifetime = ?,
            last_earned_at = ?
        WHERE guild_id = ? AND user_id = ?
        """,
        (season_inc, lifetime_inc, rem_lifetime, last_earned_at, guild_id, user_id),
    )
    conn.commit()


def grant_xp(
    guild_id: int, user_id: int, season_inc: int, lifetime_inc: int, last_earned_at: str
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


def set_xp(
    guild_id: int,
    user_id: int,
    season_xp: int,
    lifetime_xp: int,
    rem_lifetime: Optional[float],
    last_earned_at: str,
) -> None:
    ensure_user(guild_id, user_id)
    conn = get_connection()
    if rem_lifetime is None:
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
    else:
        conn.execute(
            """
            UPDATE users
            SET season_xp = ?,
                lifetime_xp = ?,
                rem_lifetime = ?,
                last_earned_at = ?
            WHERE guild_id = ? AND user_id = ?
            """,
            (season_xp, lifetime_xp, rem_lifetime, last_earned_at, guild_id, user_id),
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


def fetch_rank_candidates(guild_id: int) -> Iterable[sqlite3.Row]:
    conn = get_connection()
    return conn.execute(
        """
        SELECT user_id, season_xp, joined_at, last_earned_at, is_in_vc, optout
        FROM users
        WHERE guild_id = ?
        """,
        (guild_id,),
    ).fetchall()


def fetch_lifetime_candidates(guild_id: int) -> Iterable[sqlite3.Row]:
    conn = get_connection()
    return conn.execute(
        """
        SELECT user_id, lifetime_xp, last_earned_at, optout
        FROM users
        WHERE guild_id = ?
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


def fetch_guild_settings(guild_id: int) -> Optional[sqlite3.Row]:
    conn = get_connection()
    return conn.execute(
        """
        SELECT guild_id, rankboard_channel_id, rankboard_message_id, updated_at
        FROM guild_settings
        WHERE guild_id = ?
        """,
        (guild_id,),
    ).fetchone()


def upsert_guild_settings(
    guild_id: int, rankboard_channel_id: int, rankboard_message_id: int
) -> None:
    conn = get_connection()
    conn.execute(
        """
        INSERT INTO guild_settings (
            guild_id, rankboard_channel_id, rankboard_message_id, updated_at
        ) VALUES (?, ?, ?, datetime('now'))
        ON CONFLICT(guild_id)
        DO UPDATE SET
            rankboard_channel_id = excluded.rankboard_channel_id,
            rankboard_message_id = excluded.rankboard_message_id,
            updated_at = datetime('now')
        """,
        (guild_id, rankboard_channel_id, rankboard_message_id),
    )
    conn.commit()


def fetch_rank_role_snapshot(guild_id: int) -> Optional[sqlite3.Row]:
    conn = get_connection()
    return conn.execute(
        """
        SELECT guild_id, last_snapshot_json, updated_at
        FROM rank_role_last
        WHERE guild_id = ?
        """,
        (guild_id,),
    ).fetchone()


def upsert_rank_role_snapshot(guild_id: int, snapshot_json: str) -> None:
    conn = get_connection()
    conn.execute(
        """
        INSERT INTO rank_role_last (guild_id, last_snapshot_json, updated_at)
        VALUES (?, ?, datetime('now'))
        ON CONFLICT(guild_id)
        DO UPDATE SET
            last_snapshot_json = excluded.last_snapshot_json,
            updated_at = datetime('now')
        """,
        (guild_id, snapshot_json),
    )
    conn.commit()
