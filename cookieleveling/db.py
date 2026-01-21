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
        \"\"\"\n        INSERT INTO users (guild_id, user_id, is_in_vc, joined_at)\n        VALUES (?, ?, ?, ?)\n        ON CONFLICT(guild_id, user_id)\n        DO UPDATE SET is_in_vc = excluded.is_in_vc, joined_at = excluded.joined_at\n        \"\"\",\n        (guild_id, user_id, int(is_in_vc), joined_at),\n    )
    conn.commit()


def ensure_user(guild_id: int, user_id: int) -> None:
    conn = get_connection()
    conn.execute(
        \"\"\"\n        INSERT OR IGNORE INTO users (guild_id, user_id)\n        VALUES (?, ?)\n        \"\"\",\n        (guild_id, user_id),\n    )
    conn.commit()


def set_optout(guild_id: int, user_id: int, optout: bool) -> None:
    ensure_user(guild_id, user_id)
    conn = get_connection()
    conn.execute(
        \"UPDATE users SET optout = ? WHERE guild_id = ? AND user_id = ?\",\n        (int(optout), guild_id, user_id),\n    )
    conn.commit()


def fetch_user(guild_id: int, user_id: int) -> Optional[sqlite3.Row]:
    conn = get_connection()
    return conn.execute(
        \"\"\"\n        SELECT guild_id, user_id, season_xp, lifetime_xp, rem_lifetime,\n               optout, is_in_vc, joined_at, last_earned_at\n        FROM users\n        WHERE guild_id = ? AND user_id = ?\n        \"\"\",\n        (guild_id, user_id),\n    ).fetchone()


def fetch_active_voice_users(guild_id: int) -> Iterable[sqlite3.Row]:
    conn = get_connection()
    return conn.execute(
        \"\"\"\n        SELECT user_id, rem_lifetime\n        FROM users\n        WHERE guild_id = ? AND is_in_vc = 1 AND optout = 0\n        \"\"\",\n        (guild_id,),\n    ).fetchall()


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
        \"\"\"\n        UPDATE users\n        SET season_xp = season_xp + ?,\n            lifetime_xp = lifetime_xp + ?,\n            rem_lifetime = ?,\n            last_earned_at = ?\n        WHERE guild_id = ? AND user_id = ?\n        \"\"\",\n        (season_inc, lifetime_inc, rem_lifetime, last_earned_at, guild_id, user_id),\n    )
    conn.commit()


def fetch_rank_candidates(guild_id: int) -> Iterable[sqlite3.Row]:
    conn = get_connection()
    return conn.execute(
        \"\"\"\n        SELECT user_id, season_xp, joined_at, last_earned_at, is_in_vc, optout\n        FROM users\n        WHERE guild_id = ?\n        \"\"\",\n        (guild_id,),\n    ).fetchall()


def fetch_voice_states(guild_id: int) -> Iterable[sqlite3.Row]:
    conn = get_connection()
    return conn.execute(
        \"\"\"\n        SELECT user_id, is_in_vc, joined_at\n        FROM users\n        WHERE guild_id = ?\n        ORDER BY user_id ASC\n        \"\"\",\n        (guild_id,),\n    ).fetchall()
