import os
import sqlite3
from typing import Optional

from cookieleveling.config.config import Config

_SCHEMA_VERSION = 9
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


def fetch_schema_version() -> str:
    conn = get_connection()
    row = conn.execute("SELECT schema_version FROM meta LIMIT 1").fetchone()
    if row is None:
        return "unknown"
    return str(row["schema_version"])


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
            weekly_channel_id INTEGER,
            weekly_message_id INTEGER,
            season_channel_id INTEGER,
            season_message_id INTEGER,
            lifetime_channel_id INTEGER,
            lifetime_message_id INTEGER,
            host_monthly_channel_id INTEGER,
            host_monthly_message_id INTEGER,
            host_total_channel_id INTEGER,
            host_total_message_id INTEGER,
            host_weekly_channel_id INTEGER,
            host_weekly_message_id INTEGER,
            week_start_at TEXT,
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
            weekly_xp INTEGER NOT NULL DEFAULT 0,
            rem_lifetime REAL NOT NULL DEFAULT 0,
            optout INTEGER NOT NULL DEFAULT 0,
            is_excluded INTEGER NOT NULL DEFAULT 0,
            rank_visible INTEGER NOT NULL DEFAULT 1,
            is_in_vc INTEGER NOT NULL DEFAULT 0,
            joined_at TEXT,
            last_earned_at TEXT,
            deleted_at TEXT,
            left_guild_at TEXT,
            PRIMARY KEY (guild_id, user_id)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS host_target_channels (
            guild_id INTEGER NOT NULL,
            channel_id INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            PRIMARY KEY (guild_id, channel_id)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS vc_host_state (
            guild_id INTEGER NOT NULL,
            channel_id INTEGER NOT NULL,
            session_started_at TEXT NOT NULL,
            started_at TEXT,
            deadline_at TEXT NOT NULL,
            host_user_id INTEGER,
            locked INTEGER NOT NULL DEFAULT 0,
            last_seen_at TEXT,
            host_confirmed INTEGER NOT NULL DEFAULT 0,
            host_timed_out INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (guild_id, channel_id)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS host_stats (
            guild_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            monthly_xp INTEGER NOT NULL DEFAULT 0,
            total_xp INTEGER NOT NULL DEFAULT 0,
            weekly_xp INTEGER NOT NULL DEFAULT 0,
            monthly_sessions INTEGER NOT NULL DEFAULT 0,
            total_sessions INTEGER NOT NULL DEFAULT 0,
            last_earned_at TEXT,
            PRIMARY KEY (guild_id, user_id)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS user_weekly_xp (
            guild_id INTEGER NOT NULL,
            week_key TEXT NOT NULL,
            user_id INTEGER NOT NULL,
            weekly_xp INTEGER NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (guild_id, week_key, user_id)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS host_weekly_xp (
            guild_id INTEGER NOT NULL,
            week_key TEXT NOT NULL,
            user_id INTEGER NOT NULL,
            weekly_xp INTEGER NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (guild_id, week_key, user_id)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS guild_members (
            guild_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            member_state INTEGER NOT NULL DEFAULT 0,
            last_seen_at TEXT,
            display_name_cache TEXT,
            avatar_url_cache TEXT,
            PRIMARY KEY (guild_id, user_id)
        )
        """
    )
    _ensure_meta(conn)
    _migrate_schema(conn)
    conn.commit()


def _ensure_meta(conn: sqlite3.Connection) -> None:
    row = conn.execute("SELECT schema_version FROM meta LIMIT 1").fetchone()
    if row is None:
        conn.execute(
            "INSERT INTO meta (schema_version, created_at) VALUES (?, datetime('now'))",
            (_SCHEMA_VERSION,),
        )


def _get_schema_version(conn: sqlite3.Connection) -> int:
    row = conn.execute("SELECT schema_version FROM meta LIMIT 1").fetchone()
    if row is None:
        return 0
    return int(row["schema_version"])


def _set_schema_version(conn: sqlite3.Connection, version: int) -> None:
    conn.execute(
        "UPDATE meta SET schema_version = ?, created_at = created_at",
        (version,),
    )


def _migrate_schema(conn: sqlite3.Connection) -> None:
    version = _get_schema_version(conn)
    if version < 2:
        _migrate_to_v2(conn)
        _set_schema_version(conn, 2)
        version = 2
    if version < 3:
        _migrate_to_v3(conn)
        _set_schema_version(conn, 3)
    if version < 4:
        _migrate_to_v4(conn)
        _set_schema_version(conn, 4)
        version = 4
    if version < 5:
        _migrate_to_v5(conn)
        _set_schema_version(conn, 5)
        version = 5
    if version < 6:
        _migrate_to_v6(conn)
        _set_schema_version(conn, 6)
        version = 6
    if version < 7:
        _migrate_to_v7(conn)
        _set_schema_version(conn, 7)
        version = 7
    if version < 8:
        _migrate_to_v8(conn)
        _set_schema_version(conn, 8)
        version = 8
    if version < 9:
        _migrate_to_v9(conn)
        _set_schema_version(conn, 9)


def _column_names(conn: sqlite3.Connection, table: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return {row[1] for row in rows}


def _migrate_to_v2(conn: sqlite3.Connection) -> None:
    columns = _column_names(conn, "guild_settings")
    if "season_channel_id" not in columns:
        conn.execute("ALTER TABLE guild_settings ADD COLUMN season_channel_id INTEGER")
    if "season_message_id" not in columns:
        conn.execute("ALTER TABLE guild_settings ADD COLUMN season_message_id INTEGER")
    if "lifetime_channel_id" not in columns:
        conn.execute("ALTER TABLE guild_settings ADD COLUMN lifetime_channel_id INTEGER")
    if "lifetime_message_id" not in columns:
        conn.execute("ALTER TABLE guild_settings ADD COLUMN lifetime_message_id INTEGER")


def _migrate_to_v3(conn: sqlite3.Connection) -> None:
    columns = _column_names(conn, "guild_settings")
    if "host_monthly_channel_id" not in columns:
        conn.execute(
            "ALTER TABLE guild_settings ADD COLUMN host_monthly_channel_id INTEGER"
        )
    if "host_monthly_message_id" not in columns:
        conn.execute(
            "ALTER TABLE guild_settings ADD COLUMN host_monthly_message_id INTEGER"
        )
    if "host_total_channel_id" not in columns:
        conn.execute("ALTER TABLE guild_settings ADD COLUMN host_total_channel_id INTEGER")
    if "host_total_message_id" not in columns:
        conn.execute(
            "ALTER TABLE guild_settings ADD COLUMN host_total_message_id INTEGER"
        )


def _migrate_to_v4(conn: sqlite3.Connection) -> None:
    columns = _column_names(conn, "vc_host_state")
    if "started_at" not in columns:
        conn.execute("ALTER TABLE vc_host_state ADD COLUMN started_at TEXT")
    if "locked" not in columns:
        conn.execute(
            "ALTER TABLE vc_host_state ADD COLUMN locked INTEGER NOT NULL DEFAULT 0"
        )
    if "last_seen_at" not in columns:
        conn.execute("ALTER TABLE vc_host_state ADD COLUMN last_seen_at TEXT")
    conn.execute(
        """
        UPDATE vc_host_state
        SET started_at = COALESCE(started_at, session_started_at),
            locked = CASE WHEN host_confirmed = 1 THEN 1 ELSE 0 END,
            last_seen_at = COALESCE(last_seen_at, session_started_at)
        """
    )


def _migrate_to_v5(conn: sqlite3.Connection) -> None:
    columns = _column_names(conn, "guild_settings")
    if "weekly_channel_id" not in columns:
        conn.execute("ALTER TABLE guild_settings ADD COLUMN weekly_channel_id INTEGER")
    if "weekly_message_id" not in columns:
        conn.execute("ALTER TABLE guild_settings ADD COLUMN weekly_message_id INTEGER")
    if "host_weekly_channel_id" not in columns:
        conn.execute(
            "ALTER TABLE guild_settings ADD COLUMN host_weekly_channel_id INTEGER"
        )
    if "host_weekly_message_id" not in columns:
        conn.execute(
            "ALTER TABLE guild_settings ADD COLUMN host_weekly_message_id INTEGER"
        )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS user_weekly_xp (
            guild_id INTEGER NOT NULL,
            week_key TEXT NOT NULL,
            user_id INTEGER NOT NULL,
            weekly_xp INTEGER NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (guild_id, week_key, user_id)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS host_weekly_xp (
            guild_id INTEGER NOT NULL,
            week_key TEXT NOT NULL,
            user_id INTEGER NOT NULL,
            weekly_xp INTEGER NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (guild_id, week_key, user_id)
        )
        """
    )


def _migrate_to_v6(conn: sqlite3.Connection) -> None:
    columns = _column_names(conn, "users")
    if "is_excluded" not in columns:
        conn.execute(
            "ALTER TABLE users ADD COLUMN is_excluded INTEGER NOT NULL DEFAULT 0"
        )
    if "rank_visible" not in columns:
        conn.execute(
            "ALTER TABLE users ADD COLUMN rank_visible INTEGER NOT NULL DEFAULT 1"
        )
    if "deleted_at" not in columns:
        conn.execute("ALTER TABLE users ADD COLUMN deleted_at TEXT")


def _migrate_to_v7(conn: sqlite3.Connection) -> None:
    columns = _column_names(conn, "users")
    if "left_guild_at" not in columns:
        conn.execute("ALTER TABLE users ADD COLUMN left_guild_at TEXT")


def _migrate_to_v8(conn: sqlite3.Connection) -> None:
    columns = _column_names(conn, "guild_settings")
    if "week_start_at" not in columns:
        conn.execute("ALTER TABLE guild_settings ADD COLUMN week_start_at TEXT")

    columns = _column_names(conn, "users")
    if "weekly_xp" not in columns:
        conn.execute(
            "ALTER TABLE users ADD COLUMN weekly_xp INTEGER NOT NULL DEFAULT 0"
        )

    columns = _column_names(conn, "host_stats")
    if "weekly_xp" not in columns:
        conn.execute(
            "ALTER TABLE host_stats ADD COLUMN weekly_xp INTEGER NOT NULL DEFAULT 0"
        )


def _migrate_to_v9(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS guild_members (
            guild_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            member_state INTEGER NOT NULL DEFAULT 0,
            last_seen_at TEXT,
            display_name_cache TEXT,
            avatar_url_cache TEXT,
            PRIMARY KEY (guild_id, user_id)
        )
        """
    )
