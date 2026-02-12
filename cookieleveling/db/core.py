import logging
import os
import sqlite3
from datetime import datetime, timezone
from typing import Optional
from zoneinfo import ZoneInfo

from cookieleveling.config.config import Config
from cookieleveling.domain.period import (
    current_month_key,
    current_week_key,
    normalize_week_key,
)

_SCHEMA_VERSION = 2
_META_SCHEMA_VERSION_COMPAT = 10
_connection: Optional[sqlite3.Connection] = None

_LOGGER = logging.getLogger(__name__)
_JST = ZoneInfo("Asia/Tokyo")


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
    _LOGGER.info(
        "using db_path=%s data_dir=%s",
        os.path.abspath(config.db_path),
        os.path.abspath(config.data_dir),
    )
    _create_base_tables(conn)
    _ensure_meta_store(conn)
    _ensure_meta_defaults(conn)
    _ensure_schema(conn)
    _log_table_presence(conn)
    _connection = conn


def fetch_schema_version() -> str:
    conn = get_connection()
    row = conn.execute(
        "SELECT value FROM schema_meta WHERE key = 'schema_version'"
    ).fetchone()
    if row is None:
        return "unknown"
    return str(row["value"])


def _create_base_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
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
def _ensure_meta_store(conn: sqlite3.Connection) -> None:
    if not _table_exists(conn, "meta"):
        conn.execute(
            """
            CREATE TABLE meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
            """
        )
        return
    meta_columns = {str(row["name"]) for row in conn.execute("PRAGMA table_info(meta)")}
    if "key" in meta_columns and "value" in meta_columns:
        return

    _LOGGER.info("migrating legacy meta table to key/value store")
    row = conn.execute("SELECT * FROM meta LIMIT 1").fetchone()
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS meta_new (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
        """
    )
    if row is not None:
        if "schema_version" in meta_columns and row["schema_version"] is not None:
            _set_meta_value(conn, "schema_version", str(row["schema_version"]), table_name="meta_new")
        if "created_at" in meta_columns and row["created_at"] is not None:
            _set_meta_value(conn, "created_at", str(row["created_at"]), table_name="meta_new")
        if "last_weekly_reset_key" in meta_columns:
            normalized_week = normalize_week_key(row["last_weekly_reset_key"])
            if normalized_week:
                _set_meta_value(
                    conn,
                    "last_weekly_reset_key",
                    normalized_week,
                    table_name="meta_new",
                )
        if "last_monthly_reset_key" in meta_columns and row["last_monthly_reset_key"] is not None:
            _set_meta_value(
                conn,
                "last_monthly_reset_key",
                str(row["last_monthly_reset_key"]),
                table_name="meta_new",
            )
        if "last_host_monthly_reset_key" in meta_columns and row["last_host_monthly_reset_key"] is not None:
            _set_meta_value(
                conn,
                "last_host_monthly_reset_key",
                str(row["last_host_monthly_reset_key"]),
                table_name="meta_new",
            )
    conn.execute("DROP TABLE meta")
    conn.execute("ALTER TABLE meta_new RENAME TO meta")


def _ensure_meta_defaults(conn: sqlite3.Connection) -> None:
    _ensure_meta_default(conn, "schema_version", str(_META_SCHEMA_VERSION_COMPAT))
    _ensure_meta_default(conn, "created_at", datetime.now(timezone.utc).isoformat())
    _ensure_meta_default(
        conn,
        "last_weekly_reset_key",
        current_week_key(datetime.now(_JST)),
    )
    _ensure_meta_default(
        conn,
        "last_monthly_reset_key",
        current_month_key(datetime.now(_JST)),
    )


def _ensure_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
        """
    )
    version = _get_schema_version(conn)
    if version >= _SCHEMA_VERSION:
        _create_v2_tables(conn)
        _normalize_week_keys(conn)
        _ensure_weekly_history_defaults(conn)
        conn.commit()
        _log_period_state(conn)
        return
    _migrate_to_v2(conn)


def _get_schema_version(conn: sqlite3.Connection) -> int:
    row = conn.execute(
        "SELECT value FROM schema_meta WHERE key = 'schema_version'"
    ).fetchone()
    if row is None:
        return 0
    try:
        return int(str(row["value"]))
    except ValueError:
        return 0


def _set_schema_version(conn: sqlite3.Connection, version: int) -> None:
    conn.execute(
        """
        INSERT INTO schema_meta (key, value)
        VALUES ('schema_version', ?)
        ON CONFLICT(key)
        DO UPDATE SET value = excluded.value
        """,
        (str(version),),
    )
    conn.execute(
        """
        INSERT INTO schema_meta (key, value)
        VALUES ('migrated_at', ?)
        ON CONFLICT(key)
        DO UPDATE SET value = excluded.value
        """,
        (datetime.now(timezone.utc).isoformat(),),
    )
    _set_meta_value(conn, "schema_version", str(_META_SCHEMA_VERSION_COMPAT))


def _create_v2_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS user_flags (
            guild_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            optout INTEGER NOT NULL DEFAULT 0,
            is_excluded INTEGER NOT NULL DEFAULT 0,
            rank_visible INTEGER NOT NULL DEFAULT 1,
            left_guild_at TEXT,
            deleted_at TEXT,
            last_seen_at TEXT,
            display_name TEXT,
            avatar_url TEXT,
            PRIMARY KEY (guild_id, user_id)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS voice_xp (
            guild_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            lifetime_xp INTEGER NOT NULL DEFAULT 0,
            lifetime_rem REAL NOT NULL DEFAULT 0,
            monthly_xp INTEGER NOT NULL DEFAULT 0,
            monthly_key TEXT NOT NULL,
            weekly_xp INTEGER NOT NULL DEFAULT 0,
            weekly_key TEXT NOT NULL,
            last_earned_at TEXT,
            PRIMARY KEY (guild_id, user_id)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS host_xp (
            guild_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            total_xp INTEGER NOT NULL DEFAULT 0,
            monthly_xp INTEGER NOT NULL DEFAULT 0,
            monthly_key TEXT NOT NULL,
            weekly_xp INTEGER NOT NULL DEFAULT 0,
            weekly_key TEXT NOT NULL,
            total_sessions INTEGER NOT NULL DEFAULT 0,
            monthly_sessions INTEGER NOT NULL DEFAULT 0,
            weekly_sessions INTEGER NOT NULL DEFAULT 0,
            last_earned_at TEXT,
            PRIMARY KEY (guild_id, user_id)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS period_state (
            guild_id INTEGER PRIMARY KEY,
            current_week_key TEXT NOT NULL,
            current_month_key TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS voice_state (
            guild_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            is_in_vc INTEGER NOT NULL DEFAULT 0,
            joined_at TEXT,
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


def _migrate_to_v2(conn: sqlite3.Connection) -> None:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    users_backup: str | None = None
    host_backup: str | None = None

    _LOGGER.info("migration started: target_schema_version=%s", _SCHEMA_VERSION)
    try:
        conn.execute("BEGIN IMMEDIATE")
        users_backup = _rename_legacy_table(conn, "users", timestamp)
        host_backup = _rename_legacy_table(conn, "host_stats", timestamp)
        _create_v2_tables(conn)

        week_key = current_week_key()
        month_key = current_month_key()
        updated_at = datetime.now(timezone.utc).isoformat()

        guild_ids = _collect_guild_ids(conn, users_backup, host_backup)
        for guild_id in guild_ids:
            conn.execute(
                """
                INSERT INTO period_state (guild_id, current_week_key, current_month_key, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(guild_id)
                DO UPDATE SET
                    current_week_key = excluded.current_week_key,
                    current_month_key = excluded.current_month_key,
                    updated_at = excluded.updated_at
                """,
                (guild_id, week_key, month_key, updated_at),
            )

        users_count = 0
        users_lifetime_sum = 0
        host_count = 0
        host_total_sum = 0

        if users_backup is not None:
            users_count = _count_rows(conn, users_backup)
            users_lifetime_sum = _sum_column(conn, users_backup, "lifetime_xp")
            conn.execute(
                f"""
                INSERT INTO user_flags (
                    guild_id,
                    user_id,
                    optout,
                    is_excluded,
                    rank_visible,
                    left_guild_at,
                    deleted_at,
                    last_seen_at,
                    display_name,
                    avatar_url
                )
                SELECT
                    u.guild_id,
                    u.user_id,
                    COALESCE(u.optout, 0),
                    COALESCE(u.is_excluded, 0),
                    COALESCE(u.rank_visible, 1),
                    u.left_guild_at,
                    u.deleted_at,
                    COALESCE(u.last_earned_at, u.joined_at),
                    gm.display_name_cache,
                    gm.avatar_url_cache
                FROM {users_backup} u
                LEFT JOIN guild_members gm
                  ON gm.guild_id = u.guild_id
                 AND gm.user_id = u.user_id
                """
            )
            conn.execute(
                f"""
                INSERT INTO voice_xp (
                    guild_id,
                    user_id,
                    lifetime_xp,
                    lifetime_rem,
                    monthly_xp,
                    monthly_key,
                    weekly_xp,
                    weekly_key,
                    last_earned_at
                )
                SELECT
                    guild_id,
                    user_id,
                    COALESCE(lifetime_xp, 0),
                    COALESCE(rem_lifetime, 0),
                    COALESCE(season_xp, 0),
                    ?,
                    COALESCE(weekly_xp, 0),
                    ?,
                    last_earned_at
                FROM {users_backup}
                """,
                (month_key, week_key),
            )
            conn.execute(
                f"""
                INSERT INTO voice_state (guild_id, user_id, is_in_vc, joined_at)
                SELECT guild_id, user_id, COALESCE(is_in_vc, 0), joined_at
                FROM {users_backup}
                """
            )

        if host_backup is not None:
            host_count = _count_rows(conn, host_backup)
            host_total_sum = _sum_column(conn, host_backup, "total_xp")
            conn.execute(
                f"""
                INSERT INTO host_xp (
                    guild_id,
                    user_id,
                    total_xp,
                    monthly_xp,
                    monthly_key,
                    weekly_xp,
                    weekly_key,
                    total_sessions,
                    monthly_sessions,
                    weekly_sessions,
                    last_earned_at
                )
                SELECT
                    guild_id,
                    user_id,
                    COALESCE(total_xp, 0),
                    COALESCE(monthly_xp, 0),
                    ?,
                    COALESCE(weekly_xp, 0),
                    ?,
                    COALESCE(total_sessions, 0),
                    COALESCE(monthly_sessions, 0),
                    0,
                    last_earned_at
                FROM {host_backup}
                """,
                (month_key, week_key),
            )

        conn.execute(
            """
            INSERT OR IGNORE INTO user_flags (guild_id, user_id)
            SELECT guild_id, user_id FROM voice_xp
            """
        )
        conn.execute(
            """
            INSERT OR IGNORE INTO user_flags (guild_id, user_id)
            SELECT guild_id, user_id FROM host_xp
            """
        )

        migrated_voice_count = _count_rows(conn, "voice_xp")
        migrated_voice_lifetime_sum = _sum_column(conn, "voice_xp", "lifetime_xp")
        migrated_host_count = _count_rows(conn, "host_xp")
        migrated_host_total_sum = _sum_column(conn, "host_xp", "total_xp")

        _LOGGER.info(
            "migration verify users: old_count=%s new_count=%s old_lifetime_sum=%s new_lifetime_sum=%s",
            users_count,
            migrated_voice_count,
            users_lifetime_sum,
            migrated_voice_lifetime_sum,
        )
        _LOGGER.info(
            "migration verify host: old_count=%s new_count=%s old_total_sum=%s new_total_sum=%s",
            host_count,
            migrated_host_count,
            host_total_sum,
            migrated_host_total_sum,
        )

        if users_backup is not None:
            if users_count != migrated_voice_count or users_lifetime_sum != migrated_voice_lifetime_sum:
                raise RuntimeError("users migration verification failed")
        if host_backup is not None:
            if host_count != migrated_host_count or host_total_sum != migrated_host_total_sum:
                raise RuntimeError("host_stats migration verification failed")

        _set_schema_version(conn, _SCHEMA_VERSION)
        _normalize_week_keys(conn)
        _ensure_weekly_history_defaults(conn)
        conn.commit()
        _LOGGER.info(
            "migration finished: schema_version=%s users_backup=%s host_backup=%s",
            _SCHEMA_VERSION,
            users_backup,
            host_backup,
        )
        _log_period_state(conn)
    except Exception:
        conn.rollback()
        _LOGGER.exception("migration failed: schema_version=%s", _SCHEMA_VERSION)
        raise


def _collect_guild_ids(
    conn: sqlite3.Connection,
    users_backup: str | None,
    host_backup: str | None,
) -> set[int]:
    guild_ids: set[int] = set()
    guild_ids.update(_select_guild_ids(conn, "guild_settings"))
    guild_ids.update(_select_guild_ids(conn, "guild_members"))
    if users_backup is not None:
        guild_ids.update(_select_guild_ids(conn, users_backup))
    if host_backup is not None:
        guild_ids.update(_select_guild_ids(conn, host_backup))
    if not guild_ids:
        row = conn.execute("SELECT guild_id FROM guild_settings LIMIT 1").fetchone()
        if row is not None:
            guild_ids.add(int(row["guild_id"]))
    return guild_ids


def _select_guild_ids(conn: sqlite3.Connection, table_name: str) -> set[int]:
    if not _table_exists(conn, table_name):
        return set()
    rows = conn.execute(f"SELECT DISTINCT guild_id FROM {table_name}").fetchall()
    return {int(row["guild_id"]) for row in rows}


def _rename_legacy_table(
    conn: sqlite3.Connection,
    table_name: str,
    timestamp: str,
) -> str | None:
    if not _table_exists(conn, table_name):
        return None
    backup_name = f"{table_name}__old_{timestamp}"
    conn.execute(f"ALTER TABLE {table_name} RENAME TO {backup_name}")
    return backup_name


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name = ?",
        (table_name,),
    ).fetchone()
    return row is not None


def _column_exists(conn: sqlite3.Connection, table_name: str, column_name: str) -> bool:
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return any(str(row["name"]) == column_name for row in rows)


def get_meta_value(key: str) -> str | None:
    conn = get_connection()
    return _fetch_meta_value(conn, key)


def set_meta_value(key: str, value: str) -> None:
    conn = get_connection()
    _set_meta_value(conn, key, value)
    conn.commit()


def _count_rows(conn: sqlite3.Connection, table_name: str) -> int:
    row = conn.execute(f"SELECT COUNT(*) AS c FROM {table_name}").fetchone()
    return int(row["c"]) if row is not None else 0


def _sum_column(conn: sqlite3.Connection, table_name: str, column_name: str) -> int:
    row = conn.execute(
        f"SELECT COALESCE(SUM({column_name}), 0) AS s FROM {table_name}"
    ).fetchone()
    return int(row["s"]) if row is not None else 0


def _log_period_state(conn: sqlite3.Connection) -> None:
    row = conn.execute(
        "SELECT current_week_key, current_month_key FROM period_state ORDER BY updated_at DESC LIMIT 1"
    ).fetchone()
    if row is None:
        now_jst = datetime.now(_JST)
        _LOGGER.info(
            "period_state: week_key=%s month_key=%s",
            current_week_key(now_jst),
            current_month_key(now_jst),
        )
        return
    _LOGGER.info(
        "period_state: week_key=%s month_key=%s",
        row["current_week_key"],
        row["current_month_key"],
    )


def _fetch_meta_value(conn: sqlite3.Connection, key: str) -> str | None:
    row = conn.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
    if row is None:
        return None
    return str(row["value"])


def _set_meta_value(
    conn: sqlite3.Connection,
    key: str,
    value: str,
    *,
    table_name: str = "meta",
) -> None:
    conn.execute(
        f"""
        INSERT INTO {table_name} (key, value)
        VALUES (?, ?)
        ON CONFLICT(key)
        DO UPDATE SET value = excluded.value
        """,
        (key, value),
    )


def _ensure_meta_default(conn: sqlite3.Connection, key: str, value: str) -> None:
    conn.execute(
        """
        INSERT OR IGNORE INTO meta (key, value)
        VALUES (?, ?)
        """,
        (key, value),
    )


def _normalize_week_keys(conn: sqlite3.Connection) -> None:
    targets = [
        ("period_state", "current_week_key"),
        ("voice_xp", "weekly_key"),
        ("host_xp", "weekly_key"),
    ]
    for table_name, column_name in targets:
        if not _table_exists(conn, table_name) or not _column_exists(conn, table_name, column_name):
            continue
        rows = conn.execute(
            f"SELECT DISTINCT {column_name} AS week_key FROM {table_name} WHERE {column_name} IS NOT NULL"
        ).fetchall()
        for row in rows:
            old_key = str(row["week_key"])
            new_key = normalize_week_key(old_key)
            if not new_key or old_key == new_key:
                continue
            conn.execute(
                f"UPDATE {table_name} SET {column_name} = ? WHERE {column_name} = ?",
                (new_key, old_key),
            )

    _normalize_weekly_history_table(conn, "user_weekly_xp")
    _normalize_weekly_history_table(conn, "host_weekly_xp")

    last_weekly_reset_key = _fetch_meta_value(conn, "last_weekly_reset_key")
    normalized_last_week = normalize_week_key(last_weekly_reset_key)
    if normalized_last_week:
        _set_meta_value(conn, "last_weekly_reset_key", normalized_last_week)


def _normalize_weekly_history_table(conn: sqlite3.Connection, table_name: str) -> None:
    if not _table_exists(conn, table_name) or not _column_exists(conn, table_name, "week_key"):
        return
    rows = conn.execute(
        f"SELECT DISTINCT week_key FROM {table_name} WHERE week_key IS NOT NULL"
    ).fetchall()
    for row in rows:
        old_key = str(row["week_key"])
        new_key = normalize_week_key(old_key)
        if not new_key or new_key == old_key:
            continue
        conn.execute(
            f"""
            INSERT INTO {table_name} (guild_id, week_key, user_id, weekly_xp, updated_at)
            SELECT
                guild_id,
                ?,
                user_id,
                SUM(weekly_xp),
                MAX(updated_at)
            FROM {table_name}
            WHERE week_key = ?
            GROUP BY guild_id, user_id
            ON CONFLICT(guild_id, week_key, user_id)
            DO UPDATE SET
                weekly_xp = {table_name}.weekly_xp + excluded.weekly_xp,
                updated_at = MAX({table_name}.updated_at, excluded.updated_at)
            """,
            (new_key, old_key),
        )
        conn.execute(f"DELETE FROM {table_name} WHERE week_key = ?", (old_key,))


def _ensure_weekly_history_defaults(conn: sqlite3.Connection) -> None:
    now_iso = datetime.now(timezone.utc).isoformat()
    week_key = current_week_key()
    conn.execute(
        """
        INSERT OR IGNORE INTO user_weekly_xp (guild_id, week_key, user_id, weekly_xp, updated_at)
        SELECT guild_id, weekly_key, user_id, weekly_xp, COALESCE(last_earned_at, ?)
        FROM voice_xp
        WHERE weekly_xp > 0
        """,
        (now_iso,),
    )
    conn.execute(
        """
        INSERT OR IGNORE INTO host_weekly_xp (guild_id, week_key, user_id, weekly_xp, updated_at)
        SELECT guild_id, weekly_key, user_id, weekly_xp, COALESCE(last_earned_at, ?)
        FROM host_xp
        WHERE weekly_xp > 0
        """,
        (now_iso,),
    )
    conn.execute(
        """
        INSERT OR IGNORE INTO user_flags (guild_id, user_id)
        SELECT DISTINCT guild_id, user_id FROM user_weekly_xp
        """
    )
    conn.execute(
        """
        INSERT OR IGNORE INTO user_flags (guild_id, user_id)
        SELECT DISTINCT guild_id, user_id FROM host_weekly_xp
        """
    )
    _ensure_meta_default(
        conn,
        "last_weekly_reset_key",
        _fetch_meta_value(conn, "last_weekly_reset_key") or week_key,
    )
    _ensure_meta_default(
        conn,
        "last_monthly_reset_key",
        _fetch_meta_value(conn, "last_monthly_reset_key") or current_month_key(),
    )
    _ensure_meta_default(
        conn,
        "last_host_monthly_reset_key",
        _fetch_meta_value(conn, "last_host_monthly_reset_key") or current_month_key(),
    )


def _log_table_presence(conn: sqlite3.Connection) -> None:
    _LOGGER.info(
        "table check: users=%s host_stats=%s user_weekly_xp=%s host_weekly_xp=%s",
        int(_table_exists(conn, "users")),
        int(_table_exists(conn, "host_stats")),
        int(_table_exists(conn, "user_weekly_xp")),
        int(_table_exists(conn, "host_weekly_xp")),
    )
