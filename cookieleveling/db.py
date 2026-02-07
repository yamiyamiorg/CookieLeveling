import os
import sqlite3
from typing import Iterable, Optional

from .config import Config

_SCHEMA_VERSION = 4
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
            season_channel_id INTEGER,
            season_message_id INTEGER,
            lifetime_channel_id INTEGER,
            lifetime_message_id INTEGER,
            host_monthly_channel_id INTEGER,
            host_monthly_message_id INTEGER,
            host_total_channel_id INTEGER,
            host_total_message_id INTEGER,
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
            monthly_sessions INTEGER NOT NULL DEFAULT 0,
            total_sessions INTEGER NOT NULL DEFAULT 0,
            last_earned_at TEXT,
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
        SELECT guild_id, user_id, season_xp, lifetime_xp,
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
        SELECT user_id, joined_at, lifetime_xp
        FROM users
        WHERE guild_id = ? AND is_in_vc = 1 AND optout = 0
        """,
        (guild_id,),
    ).fetchall()


def fetch_schema_version() -> str:
    conn = get_connection()
    row = conn.execute("SELECT schema_version FROM meta LIMIT 1").fetchone()
    if row is None:
        return "unknown"
    return str(row["schema_version"])


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


def fetch_rank_candidates(guild_id: int) -> Iterable[sqlite3.Row]:
    conn = get_connection()
    return conn.execute(
        """
        SELECT user_id, season_xp, joined_at, last_earned_at, is_in_vc, optout
        FROM users
        WHERE guild_id = ?
          AND season_xp > 0
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
          AND lifetime_xp > 0
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


def fetch_guild_settings(guild_id: int) -> Optional[sqlite3.Row]:
    conn = get_connection()
    return conn.execute(
        """
        SELECT guild_id,
               season_channel_id,
               season_message_id,
               lifetime_channel_id,
               lifetime_message_id,
               host_monthly_channel_id,
               host_monthly_message_id,
               host_total_channel_id,
               host_total_message_id,
               updated_at
        FROM guild_settings
        WHERE guild_id = ?
        """,
        (guild_id,),
    ).fetchone()


def upsert_guild_settings(
    guild_id: int,
    *,
    season_channel_id: int,
    season_message_id: int,
    lifetime_channel_id: int,
    lifetime_message_id: int,
) -> None:
    conn = get_connection()
    conn.execute(
        """
        INSERT INTO guild_settings (
            guild_id,
            season_channel_id,
            season_message_id,
            lifetime_channel_id,
            lifetime_message_id,
            updated_at
        ) VALUES (?, ?, ?, ?, ?, datetime('now'))
        ON CONFLICT(guild_id)
        DO UPDATE SET
            season_channel_id = excluded.season_channel_id,
            season_message_id = excluded.season_message_id,
            lifetime_channel_id = excluded.lifetime_channel_id,
            lifetime_message_id = excluded.lifetime_message_id,
            updated_at = datetime('now')
        """,
        (
            guild_id,
            season_channel_id,
            season_message_id,
            lifetime_channel_id,
            lifetime_message_id,
        ),
    )
    conn.commit()


def fetch_hostboard_settings(guild_id: int) -> Optional[sqlite3.Row]:
    conn = get_connection()
    return conn.execute(
        """
        SELECT guild_id,
               host_monthly_channel_id,
               host_monthly_message_id,
               host_total_channel_id,
               host_total_message_id,
               updated_at
        FROM guild_settings
        WHERE guild_id = ?
        """,
        (guild_id,),
    ).fetchone()


def upsert_hostboard_settings(
    guild_id: int,
    *,
    host_monthly_channel_id: int,
    host_monthly_message_id: int,
    host_total_channel_id: int,
    host_total_message_id: int,
) -> None:
    conn = get_connection()
    conn.execute(
        """
        INSERT INTO guild_settings (
            guild_id,
            host_monthly_channel_id,
            host_monthly_message_id,
            host_total_channel_id,
            host_total_message_id,
            updated_at
        ) VALUES (?, ?, ?, ?, ?, datetime('now'))
        ON CONFLICT(guild_id)
        DO UPDATE SET
            host_monthly_channel_id = excluded.host_monthly_channel_id,
            host_monthly_message_id = excluded.host_monthly_message_id,
            host_total_channel_id = excluded.host_total_channel_id,
            host_total_message_id = excluded.host_total_message_id,
            updated_at = datetime('now')
        """,
        (
            guild_id,
            host_monthly_channel_id,
            host_monthly_message_id,
            host_total_channel_id,
            host_total_message_id,
        ),
    )
    conn.commit()


def add_host_target_channel(guild_id: int, channel_id: int, created_at: str) -> None:
    conn = get_connection()
    conn.execute(
        """
        INSERT OR IGNORE INTO host_target_channels (guild_id, channel_id, created_at)
        VALUES (?, ?, ?)
        """,
        (guild_id, channel_id, created_at),
    )
    conn.commit()


def remove_host_target_channel(guild_id: int, channel_id: int) -> None:
    conn = get_connection()
    conn.execute(
        "DELETE FROM host_target_channels WHERE guild_id = ? AND channel_id = ?",
        (guild_id, channel_id),
    )
    conn.commit()


def fetch_host_target_channels(guild_id: int) -> set[int]:
    conn = get_connection()
    rows = conn.execute(
        "SELECT channel_id FROM host_target_channels WHERE guild_id = ?",
        (guild_id,),
    ).fetchall()
    return {int(row["channel_id"]) for row in rows}


def is_host_target_channel(guild_id: int, channel_id: int) -> bool:
    conn = get_connection()
    row = conn.execute(
        """
        SELECT 1
        FROM host_target_channels
        WHERE guild_id = ? AND channel_id = ?
        LIMIT 1
        """,
        (guild_id, channel_id),
    ).fetchone()
    return row is not None


def upsert_host_session(
    guild_id: int,
    channel_id: int,
    session_started_at: str,
    deadline_at: str,
    last_seen_at: str,
) -> None:
    conn = get_connection()
    conn.execute(
        """
        INSERT INTO vc_host_state (
            guild_id,
            channel_id,
            session_started_at,
            started_at,
            deadline_at,
            host_user_id,
            locked,
            last_seen_at,
            host_confirmed,
            host_timed_out
        ) VALUES (?, ?, ?, ?, ?, NULL, 0, ?, 0, 0)
        ON CONFLICT(guild_id, channel_id)
        DO UPDATE SET
            session_started_at = excluded.session_started_at,
            started_at = excluded.started_at,
            deadline_at = excluded.deadline_at,
            host_user_id = NULL,
            locked = 0,
            last_seen_at = excluded.last_seen_at,
            host_confirmed = 0,
            host_timed_out = 0
        """,
        (
            guild_id,
            channel_id,
            session_started_at,
            session_started_at,
            deadline_at,
            last_seen_at,
        ),
    )
    conn.commit()


def clear_host_session(guild_id: int, channel_id: int) -> None:
    conn = get_connection()
    conn.execute(
        "DELETE FROM vc_host_state WHERE guild_id = ? AND channel_id = ?",
        (guild_id, channel_id),
    )
    conn.commit()


def mark_host_timeout(guild_id: int, channel_id: int) -> None:
    conn = get_connection()
    conn.execute(
        """
        UPDATE vc_host_state
        SET host_timed_out = 1, locked = 0
        WHERE guild_id = ? AND channel_id = ?
        """,
        (guild_id, channel_id),
    )
    conn.commit()


def confirm_host(
    guild_id: int,
    channel_id: int,
    host_user_id: int,
) -> None:
    conn = get_connection()
    conn.execute(
        """
        UPDATE vc_host_state
        SET host_user_id = ?, locked = 1, host_confirmed = 1, host_timed_out = 0
        WHERE guild_id = ? AND channel_id = ?
        """,
        (host_user_id, guild_id, channel_id),
    )
    conn.commit()


def fetch_host_session(guild_id: int, channel_id: int) -> Optional[sqlite3.Row]:
    conn = get_connection()
    return conn.execute(
        """
        SELECT guild_id,
               channel_id,
               session_started_at,
               started_at,
               deadline_at,
               host_user_id,
               locked,
               last_seen_at,
               host_confirmed,
               host_timed_out
        FROM vc_host_state
        WHERE guild_id = ? AND channel_id = ?
        """,
        (guild_id, channel_id),
    ).fetchone()


def fetch_host_sessions(guild_id: int) -> Iterable[sqlite3.Row]:
    conn = get_connection()
    return conn.execute(
        """
        SELECT guild_id,
               channel_id,
               session_started_at,
               started_at,
               deadline_at,
               host_user_id,
               locked,
               last_seen_at,
               host_confirmed,
               host_timed_out
        FROM vc_host_state
        WHERE guild_id = ?
        """,
        (guild_id,),
    ).fetchall()


def update_host_last_seen(
    guild_id: int,
    channel_id: int,
    last_seen_at: str,
) -> None:
    conn = get_connection()
    conn.execute(
        """
        UPDATE vc_host_state
        SET last_seen_at = ?
        WHERE guild_id = ? AND channel_id = ?
        """,
        (last_seen_at, guild_id, channel_id),
    )
    conn.commit()


def ensure_host_user(guild_id: int, user_id: int) -> None:
    conn = get_connection()
    conn.execute(
        """
        INSERT OR IGNORE INTO host_stats (guild_id, user_id)
        VALUES (?, ?)
        """,
        (guild_id, user_id),
    )
    conn.commit()


def add_host_xp(
    *,
    guild_id: int,
    user_id: int,
    monthly_inc: int,
    total_inc: int,
    last_earned_at: str,
) -> None:
    ensure_host_user(guild_id, user_id)
    conn = get_connection()
    conn.execute(
        """
        UPDATE host_stats
        SET monthly_xp = monthly_xp + ?,
            total_xp = total_xp + ?,
            last_earned_at = ?
        WHERE guild_id = ? AND user_id = ?
        """,
        (monthly_inc, total_inc, last_earned_at, guild_id, user_id),
    )
    conn.commit()


def increment_host_session_counts(guild_id: int, user_id: int) -> None:
    ensure_host_user(guild_id, user_id)
    conn = get_connection()
    conn.execute(
        """
        UPDATE host_stats
        SET monthly_sessions = monthly_sessions + 1,
            total_sessions = total_sessions + 1
        WHERE guild_id = ? AND user_id = ?
        """,
        (guild_id, user_id),
    )
    conn.commit()


def fetch_host_top20_monthly(guild_id: int) -> Iterable[sqlite3.Row]:
    conn = get_connection()
    return conn.execute(
        """
        SELECT user_id, monthly_xp, last_earned_at
        FROM host_stats
        WHERE guild_id = ?
          AND monthly_xp > 0
        """,
        (guild_id,),
    ).fetchall()


def fetch_host_top20_total(guild_id: int) -> Iterable[sqlite3.Row]:
    conn = get_connection()
    return conn.execute(
        """
        SELECT user_id, total_xp, last_earned_at
        FROM host_stats
        WHERE guild_id = ?
          AND total_xp > 0
        """,
        (guild_id,),
    ).fetchall()


def reset_host_monthly(guild_id: int) -> None:
    conn = get_connection()
    conn.execute(
        """
        UPDATE host_stats
        SET monthly_xp = 0,
            monthly_sessions = 0
        WHERE guild_id = ?
        """,
        (guild_id,),
    )
    conn.commit()
