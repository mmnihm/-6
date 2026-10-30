import os
import aiosqlite
import re
from contextlib import asynccontextmanager

import psycopg
from psycopg.rows import dict_row
from datetime import datetime, timezone, timedelta


def utc_now():
    return datetime.now(timezone.utc)


def iso_now():
    return utc_now().isoformat()


class PostgresCompat:
    """Small async adapter for the SQLite-style calls used by this project."""

    def __init__(self, connection):
        self.connection = connection

    @staticmethod
    def adapt_sql(sql):
        statement = sql.strip()
        ignore_insert = statement.upper().startswith("INSERT OR IGNORE INTO")
        if ignore_insert:
            statement = re.sub(
                r"^INSERT OR IGNORE INTO", "INSERT INTO", statement,
                flags=re.IGNORECASE,
            ).rstrip().rstrip(";") + " ON CONFLICT DO NOTHING"
        statement = statement.replace("?", "%s")
        statement = re.sub(r"^BEGIN IMMEDIATE$", "BEGIN", statement, flags=re.I)
        statement = statement.replace(
            "ORDER BY category COLLATE NOCASE", "ORDER BY lower(category)"
        )
        return statement

    async def execute(self, sql, params=()):
        return await self.connection.execute(self.adapt_sql(sql), params or ())

    async def executemany(self, sql, params):
        async with self.connection.cursor() as cursor:
            await cursor.executemany(self.adapt_sql(sql), params)
            return cursor.rowcount

    async def executescript(self, script):
        for statement in script.split(";"):
            if statement.strip():
                await self.execute(statement)

    async def commit(self):
        await self.connection.commit()


class Database:
    def __init__(self, path: str, database_url: str = ""):
        self.path = path
        self.database_url = (database_url or os.getenv("DATABASE_URL", "")).strip()
        if self.database_url.startswith("postgres://"):
            self.database_url = "postgresql://" + self.database_url[len("postgres://"):]

    @property
    def uses_postgres(self):
        return bool(self.database_url)

    @asynccontextmanager
    async def connect(self):
        if self.uses_postgres:
            connection = await psycopg.AsyncConnection.connect(
                self.database_url, row_factory=dict_row
            )
            try:
                yield PostgresCompat(connection)
            finally:
                await connection.close()
            return

        os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
        db = await aiosqlite.connect(self.path)
        db.row_factory = aiosqlite.Row
        try:
            await db.execute("PRAGMA journal_mode=WAL")
            await db.execute("PRAGMA foreign_keys=ON")
            await db.execute("PRAGMA busy_timeout=5000")
            yield db
        finally:
            await db.close()

    async def _import_legacy_sqlite(self):
        """Import an existing local SQLite database once; never overwrite PG rows."""
        if not self.uses_postgres or not os.path.isfile(self.path):
            return

        async with self.connect() as pg:
            marker = await (await pg.execute(
                "SELECT value FROM settings WHERE key='sqlite_import_v1'"
            )).fetchone()
            if marker:
                return

            legacy = await aiosqlite.connect(self.path)
            legacy.row_factory = aiosqlite.Row
            try:
                present = {
                    row["name"] for row in await (
                        await legacy.execute(
                            "SELECT name FROM sqlite_master WHERE type='table'"
                        )
                    ).fetchall()
                }
                table_columns = {
                    "settings": ["key", "value"],
                    "users": ["user_id", "first_seen", "last_seen"],
                    "files": [
                        "file_key", "owner_id", "file_name", "media_type",
                        "category", "primary_message_id", "backup_message_id",
                        "created_at", "source_fingerprint",
                    ],
                    "manual_members": ["user_id", "expires_at", "granted_by", "granted_at"],
                    "banned_users": ["user_id", "reason", "banned_by", "banned_at"],
                    "scanner_messages": [
                        "channel_id", "message_id", "fingerprint", "file_name",
                        "media_type", "category", "created_at",
                    ],
                    "scanner_progress": [
                        "channel_id", "last_message_id", "scanned_count",
                        "completed", "updated_at",
                    ],
                }
                for table, candidates in table_columns.items():
                    if table not in present:
                        continue
                    old_columns = {
                        row["name"] for row in await (
                            await legacy.execute(f"PRAGMA table_info({table})")
                        ).fetchall()
                    }
                    columns = [name for name in candidates if name in old_columns]
                    if not columns:
                        continue
                    quoted = ", ".join(columns)
                    rows = await (await legacy.execute(
                        f"SELECT {quoted} FROM {table}"
                    )).fetchall()
                    if not rows:
                        continue
                    placeholders = ", ".join(["%s"] * len(columns))
                    await pg.executemany(
                        f"INSERT INTO {table} ({quoted}) VALUES ({placeholders}) "
                        "ON CONFLICT DO NOTHING",
                        [tuple(row[name] for name in columns) for row in rows],
                    )
                await pg.execute(
                    "INSERT INTO settings(key, value) VALUES(%s, %s) "
                    "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                    ("sqlite_import_v1", "completed"),
                )
                await pg.commit()
                log_message = "Legacy SQLite data imported into PostgreSQL without overwriting existing rows."
                print(log_message)
            finally:
                await legacy.close()

    async def initialize(self):
        schema = """
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS users (
                user_id BIGINT PRIMARY KEY,
                first_seen TEXT NOT NULL,
                last_seen TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS files (
                file_key TEXT PRIMARY KEY,
                owner_id BIGINT NOT NULL,
                file_name TEXT NOT NULL,
                media_type TEXT NOT NULL,
                category TEXT NOT NULL DEFAULT '未分类',
                primary_message_id BIGINT NOT NULL,
                backup_message_id BIGINT NOT NULL,
                created_at TEXT NOT NULL,
                source_fingerprint TEXT
            );
            CREATE TABLE IF NOT EXISTS manual_members (
                user_id BIGINT PRIMARY KEY,
                expires_at TEXT,
                granted_by BIGINT NOT NULL,
                granted_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS banned_users (
                user_id BIGINT PRIMARY KEY,
                reason TEXT NOT NULL DEFAULT '',
                banned_by BIGINT NOT NULL,
                banned_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS scanner_messages (
                channel_id BIGINT NOT NULL,
                message_id BIGINT NOT NULL,
                fingerprint TEXT NOT NULL,
                file_name TEXT NOT NULL,
                media_type TEXT NOT NULL,
                category TEXT NOT NULL DEFAULT '未分类',
                created_at TEXT NOT NULL,
                PRIMARY KEY(channel_id, message_id),
                UNIQUE(channel_id, fingerprint)
            );
            CREATE TABLE IF NOT EXISTS scanner_progress (
                channel_id BIGINT PRIMARY KEY,
                last_message_id BIGINT NOT NULL DEFAULT 0,
                scanned_count BIGINT NOT NULL DEFAULT 0,
                completed INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL
            );
        """
        async with self.connect() as db:
            if self.uses_postgres:
                await db.executescript(schema)
                columns = await (await db.execute(
                    "SELECT column_name AS name FROM information_schema.columns "
                    "WHERE table_schema=current_schema() AND table_name='files'"
                )).fetchall()
                column_names = {row["name"] for row in columns}
            else:
                await db.executescript(schema.replace("BIGINT", "INTEGER"))
                columns = await (await db.execute("PRAGMA table_info(files)")).fetchall()
                column_names = {row["name"] for row in columns}

            if "category" not in column_names:
                await db.execute(
                    "ALTER TABLE files ADD COLUMN category TEXT NOT NULL DEFAULT '未分类'"
                )
            if "source_fingerprint" not in column_names:
                await db.execute("ALTER TABLE files ADD COLUMN source_fingerprint TEXT")
            await db.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS idx_files_source_fingerprint
                ON files(source_fingerprint)
                WHERE source_fingerprint IS NOT NULL
            """)
            await db.commit()

        if self.uses_postgres:
            await self._import_legacy_sqlite()

    async def get_setting(self, key: str):
        async with self.connect() as db:
            row = await (await db.execute(
                "SELECT value FROM settings WHERE key=?", (key,)
            )).fetchone()
            return row["value"] if row else None

    async def set_setting(self, key: str, value: str):
        async with self.connect() as db:
            await db.execute("""
                INSERT INTO settings(key, value) VALUES(?, ?)
                ON CONFLICT(key) DO UPDATE SET value=excluded.value
            """, (key, str(value)))
            await db.commit()

    async def claim_first_admin(self, user_id: int) -> bool:
        async with self.connect() as db:
            await db.execute("BEGIN IMMEDIATE")
            row = await (await db.execute(
                "SELECT value FROM settings WHERE key='superadmin_id'"
            )).fetchone()
            if row:
                await db.commit()
                return int(row["value"]) == user_id
            await db.execute(
                "INSERT INTO settings(key, value) VALUES('superadmin_id', ?)",
                (str(user_id),)
            )
            await db.commit()
            return True

    async def get_admin_id(self):
        async with self.connect() as db:
            row = await (await db.execute(
                "SELECT value FROM settings WHERE key='superadmin_id'"
            )).fetchone()
            return int(row["value"]) if row else None

    async def touch_user(self, user_id: int):
        now = iso_now()
        async with self.connect() as db:
            await db.execute("""
                INSERT INTO users(user_id, first_seen, last_seen)
                VALUES(?, ?, ?)
                ON CONFLICT(user_id) DO UPDATE SET last_seen=excluded.last_seen
            """, (user_id, now, now))
            await db.commit()

    async def is_banned(self, user_id: int) -> bool:
        async with self.connect() as db:
            row = await (await db.execute(
                "SELECT 1 FROM banned_users WHERE user_id=?", (user_id,)
            )).fetchone()
            return row is not None

    async def grant_member(self, user_id: int, days, admin_id: int):
        expires = None
        if days is not None:
            expires = (utc_now() + timedelta(days=days)).isoformat()
        async with self.connect() as db:
            await db.execute("""
                INSERT INTO manual_members(user_id, expires_at, granted_by, granted_at)
                VALUES(?, ?, ?, ?)
                ON CONFLICT(user_id) DO UPDATE SET
                    expires_at=excluded.expires_at,
                    granted_by=excluded.granted_by,
                    granted_at=excluded.granted_at
            """, (user_id, expires, admin_id, iso_now()))
            await db.commit()

    async def has_manual_membership(self, user_id: int) -> bool:
        async with self.connect() as db:
            row = await (await db.execute(
                "SELECT expires_at FROM manual_members WHERE user_id=?", (user_id,)
            )).fetchone()
            if not row:
                return False
            if row["expires_at"] is None:
                return True
            try:
                return datetime.fromisoformat(row["expires_at"]) > utc_now()
            except ValueError:
                return False

    async def revoke_member(self, user_id: int):
        async with self.connect() as db:
            await db.execute("DELETE FROM manual_members WHERE user_id=?", (user_id,))
            await db.commit()

    async def ban_user(self, user_id: int, reason: str, admin_id: int):
        async with self.connect() as db:
            await db.execute("""
                INSERT INTO banned_users(user_id, reason, banned_by, banned_at)
                VALUES(?, ?, ?, ?)
                ON CONFLICT(user_id) DO UPDATE SET
                    reason=excluded.reason, banned_by=excluded.banned_by,
                    banned_at=excluded.banned_at
            """, (user_id, reason, admin_id, iso_now()))
            await db.commit()

    async def unban_user(self, user_id: int):
        async with self.connect() as db:
            await db.execute("DELETE FROM banned_users WHERE user_id=?", (user_id,))
            await db.commit()

    async def get_file_by_fingerprint(self, fingerprint: str):
        async with self.connect() as db:
            return await (await db.execute(
                "SELECT * FROM files WHERE source_fingerprint=?", (fingerprint,)
            )).fetchone()

    async def add_file(self, key, owner_id, file_name, media_type,
                       primary_id, backup_id, category="未分类",
                       source_fingerprint=None):
        async with self.connect() as db:
            cur = await db.execute("""
                INSERT OR IGNORE INTO files(
                    file_key, owner_id, file_name, media_type, category,
                    primary_message_id, backup_message_id, created_at,
                    source_fingerprint
                )
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (key, owner_id, file_name, media_type, category,
                  primary_id, backup_id, iso_now(), source_fingerprint))
            await db.commit()
            return cur.rowcount == 1

    async def save_scanned_message(
        self, channel_id, other_channel_id, primary_channel_id,
        message_id, fingerprint, file_name, media_type, category,
        created_at, owner_id, file_key
    ):
        async with self.connect() as db:
            await db.execute("""
                INSERT OR IGNORE INTO scanner_messages(
                    channel_id, message_id, fingerprint, file_name,
                    media_type, category, created_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?)
            """, (channel_id, message_id, fingerprint, file_name,
                  media_type, category, created_at))
            await db.execute("""
                INSERT INTO scanner_progress(
                    channel_id, last_message_id, scanned_count, completed, updated_at
                ) VALUES(?, ?, 1, 0, ?)
                ON CONFLICT(channel_id) DO UPDATE SET
                    last_message_id=excluded.last_message_id,
                    scanned_count=scanner_progress.scanned_count + 1,
                    completed=0,
                    updated_at=excluded.updated_at
            """, (channel_id, message_id, iso_now()))

            current = await (await db.execute("""
                SELECT message_id, file_name, media_type, category, created_at
                FROM scanner_messages
                WHERE channel_id=? AND fingerprint=?
            """, (channel_id, fingerprint))).fetchone()
            other = await (await db.execute("""
                SELECT message_id, file_name, media_type, category, created_at
                FROM scanner_messages
                WHERE channel_id=? AND fingerprint=?
            """, (other_channel_id, fingerprint))).fetchone()
            inserted = False
            if current and other:
                if channel_id == primary_channel_id:
                    primary_id, backup_id = current["message_id"], other["message_id"]
                    source = (
                        current["file_name"], current["media_type"],
                        current["category"], current["created_at"]
                    )
                else:
                    primary_id, backup_id = other["message_id"], current["message_id"]
                    source = (
                        other["file_name"], other["media_type"],
                        other["category"], other["created_at"]
                    )
                cur = await db.execute("""
                    INSERT OR IGNORE INTO files(
                        file_key, owner_id, file_name, media_type, category,
                        primary_message_id, backup_message_id, created_at,
                        source_fingerprint
                    ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    file_key, owner_id, source[0], source[1], source[2],
                    primary_id, backup_id, source[3], fingerprint
                ))
                inserted = cur.rowcount == 1
            await db.commit()
            return bool(other), inserted

    async def advance_scan_cursor(self, channel_id, message_id):
        async with self.connect() as db:
            await db.execute("""
                INSERT INTO scanner_progress(
                    channel_id, last_message_id, scanned_count, completed, updated_at
                ) VALUES(?, ?, 0, 0, ?)
                ON CONFLICT(channel_id) DO UPDATE SET
                    last_message_id=excluded.last_message_id,
                    completed=0,
                    updated_at=excluded.updated_at
            """, (channel_id, message_id, iso_now()))
            await db.commit()

    async def get_scan_progress(self, channel_id):
        async with self.connect() as db:
            row = await (await db.execute(
                "SELECT * FROM scanner_progress WHERE channel_id=?", (channel_id,)
            )).fetchone()
            if row:
                return dict(row)
            return {
                "channel_id": channel_id, "last_message_id": 0,
                "scanned_count": 0, "completed": 0, "updated_at": None
            }

    async def mark_scan_completed(self, channel_id):
        async with self.connect() as db:
            await db.execute("""
                INSERT INTO scanner_progress(
                    channel_id, last_message_id, scanned_count, completed, updated_at
                ) VALUES(?, 0, 0, 1, ?)
                ON CONFLICT(channel_id) DO UPDATE SET
                    completed=1, updated_at=excluded.updated_at
            """, (channel_id, iso_now()))
            await db.commit()

    async def get_file(self, key):
        async with self.connect() as db:
            return await (await db.execute(
                "SELECT * FROM files WHERE file_key=?", (key,)
            )).fetchone()

    async def list_files(self, limit=20):
        async with self.connect() as db:
            return await (await db.execute(
                "SELECT * FROM files ORDER BY created_at DESC LIMIT ?", (limit,)
            )).fetchall()

    async def public_catalog(self, query="", category="", limit=30, offset=0):
        conditions = []
        params = []
        if query:
            conditions.append("(file_name LIKE ? OR category LIKE ?)")
            params.extend((f"%{query}%", f"%{query}%"))
        if category:
            conditions.append("category=?")
            params.append(category)
        where = " WHERE " + " AND ".join(conditions) if conditions else ""
        async with self.connect() as db:
            categories = await (await db.execute(
                "SELECT DISTINCT category FROM files ORDER BY lower(category)"
            )).fetchall()
            total = (await (await db.execute(
                "SELECT COUNT(*) AS n FROM files" + where, params
            )).fetchone())["n"]
            rows = await (await db.execute(
                "SELECT file_key, file_name, media_type, category, created_at "
                "FROM files" + where + " ORDER BY created_at DESC LIMIT ? OFFSET ?",
                params + [limit, offset],
            )).fetchall()
            return rows, [row["category"] for row in categories], total

    async def set_file_category(self, key: str, category: str) -> bool:
        async with self.connect() as db:
            cur = await db.execute(
                "UPDATE files SET category=? WHERE file_key=?", (category, key)
            )
            await db.commit()
            return cur.rowcount > 0

    async def list_user_ids(self):
        async with self.connect() as db:
            rows = await (await db.execute(
                "SELECT user_id FROM users ORDER BY user_id"
            )).fetchall()
            return [row["user_id"] for row in rows]

    async def list_manual_members(self, limit=30):
        async with self.connect() as db:
            return await (await db.execute("""
                SELECT user_id, expires_at FROM manual_members
                ORDER BY granted_at DESC LIMIT ?
            """, (limit,))).fetchall()

    async def stats(self):
        async with self.connect() as db:
            users = (await (await db.execute(
                "SELECT COUNT(*) n FROM users"
            )).fetchone())["n"]
            files = (await (await db.execute(
                "SELECT COUNT(*) n FROM files"
            )).fetchone())["n"]
            members = (await (await db.execute("""
                SELECT COUNT(*) n FROM manual_members
                WHERE expires_at IS NULL OR expires_at > ?
            """, (iso_now(),))).fetchone())["n"]
            banned = (await (await db.execute(
                "SELECT COUNT(*) n FROM banned_users"
            )).fetchone())["n"]
            return users, files, members, banned
