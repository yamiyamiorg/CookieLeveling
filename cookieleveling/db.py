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


def fetch_voice_states(guild_id: int) -> Iterable[sqlite3.Row]:
    conn = get_connection()
    return conn.execute(
        \"\"\"\n        SELECT user_id, is_in_vc, joined_at\n        FROM users\n        WHERE guild_id = ?\n        ORDER BY user_id ASC\n        \"\"\",\n        (guild_id,),\n    ).fetchall()
