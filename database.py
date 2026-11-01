import os
import aiosqlite
import re
import secrets
from contextlib import asynccontextmanager

import psycopg
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool
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
    def adapt_sql(sql, has_params=False):
        statement = sql.strip()
        ignore_insert = statement.upper().startswith("INSERT OR IGNORE INTO")
        if ignore_insert:
            statement = re.sub(
                r"^INSERT OR IGNORE INTO", "INSERT INTO", statement,
                flags=re.IGNORECASE,
            ).rstrip().rstrip(";") + " ON CONFLICT DO NOTHING"
        statement = statement.replace("?", "%s")
        # Psycopg parses percent signs whenever query parameters are supplied.
        # Escape SQL pattern wildcards such as LIKE '%%.mp4', while preserving
        # positional/named placeholders and already escaped percent signs.
        if has_params:
            statement = re.sub(r"%(?![%sbt(])", "%%", statement)
        statement = re.sub(r"^BEGIN IMMEDIATE$", "BEGIN", statement, flags=re.I)
        statement = statement.replace(
            "ORDER BY category COLLATE NOCASE", "ORDER BY lower(category)"
        )
        statement = statement.replace(" LIKE ", " ILIKE ")
        return statement

    async def execute(self, sql, params=()):
        statement = self.adapt_sql(sql, bool(params))
        if params:
            return await self.connection.execute(statement, params)
        return await self.connection.execute(statement)

    async def executemany(self, sql, params):
        async with self.connection.cursor() as cursor:
            await cursor.executemany(self.adapt_sql(sql, True), params)
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
        self.pool = None

    @property
    def uses_postgres(self):
        return bool(self.database_url)

    async def open_pool(self):
        if self.uses_postgres and self.pool is None:
            self.pool = AsyncConnectionPool(
                self.database_url,
                min_size=1,
                max_size=5,
                timeout=10,
                kwargs={"row_factory": dict_row},
                open=False,
            )
            await self.pool.open(wait=True)

    async def close(self):
        if self.pool is not None:
            await self.pool.close()
            self.pool = None

    @asynccontextmanager
    async def connect(self):
        if self.uses_postgres:
            await self.open_pool()
            async with self.pool.connection() as connection:
                yield PostgresCompat(connection)
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
                imported_categories = await (await pg.execute(
                    "SELECT DISTINCT category FROM files "
                    "WHERE category IS NOT NULL AND category<>''"
                )).fetchall()
                for item in imported_categories:
                    await self._refresh_category_stats(pg, str(item["category"]))
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
            CREATE TABLE IF NOT EXISTS admins (
                user_id BIGINT PRIMARY KEY,
                added_by BIGINT NOT NULL,
                added_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS users (
                user_id BIGINT PRIMARY KEY,
                first_seen TEXT NOT NULL,
                last_seen TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS categories (
                name TEXT PRIMARY KEY,
                created_at TEXT NOT NULL
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
            CREATE TABLE IF NOT EXISTS user_history (
                user_id BIGINT NOT NULL,
                file_key TEXT NOT NULL,
                accessed_at TEXT NOT NULL,
                PRIMARY KEY(user_id, file_key)
            );
            CREATE TABLE IF NOT EXISTS favorites (
                user_id BIGINT NOT NULL,
                file_key TEXT NOT NULL,
                created_at TEXT NOT NULL,
                PRIMARY KEY(user_id, file_key)
            );
            CREATE TABLE IF NOT EXISTS category_favorites (
                user_id BIGINT NOT NULL,
                share_key TEXT NOT NULL,
                created_at TEXT NOT NULL,
                PRIMARY KEY(user_id, share_key)
            );
            CREATE TABLE IF NOT EXISTS category_delivery_progress (
                user_id BIGINT NOT NULL,
                share_key TEXT NOT NULL,
                next_offset BIGINT NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL,
                PRIMARY KEY(user_id, share_key)
            );
            CREATE TABLE IF NOT EXISTS backup_retry_jobs (
                file_key TEXT PRIMARY KEY,
                primary_channel_id BIGINT NOT NULL,
                primary_message_id BIGINT NOT NULL,
                backup_channel_id BIGINT NOT NULL,
                attempts BIGINT NOT NULL DEFAULT 0,
                next_retry_at TEXT NOT NULL,
                last_error TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS upload_batches (
                owner_id BIGINT PRIMARY KEY,
                category TEXT NOT NULL,
                created_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS upload_batch_items (
                file_key TEXT PRIMARY KEY,
                owner_id BIGINT NOT NULL,
                file_name TEXT NOT NULL,
                media_type TEXT NOT NULL,
                category TEXT NOT NULL,
                primary_channel_id BIGINT NOT NULL,
                primary_message_id BIGINT NOT NULL,
                backup_channel_id BIGINT NOT NULL,
                backup_message_id BIGINT NOT NULL,
                source_fingerprint TEXT,
                file_size BIGINT NOT NULL DEFAULT 0,
                backup_error TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL
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
            if "file_size" not in column_names:
                await db.execute("ALTER TABLE files ADD COLUMN file_size BIGINT NOT NULL DEFAULT 0")
            category_columns = await (await db.execute(
                "SELECT column_name AS name FROM information_schema.columns "
                "WHERE table_schema=current_schema() AND table_name='categories'"
            )).fetchall() if self.uses_postgres else await (await db.execute(
                "PRAGMA table_info(categories)"
            )).fetchall()
            category_column_names = {row["name"] for row in category_columns}
            if "description" not in category_column_names:
                await db.execute("ALTER TABLE categories ADD COLUMN description TEXT NOT NULL DEFAULT ''")
            if "updated_at" not in category_column_names:
                await db.execute("ALTER TABLE categories ADD COLUMN updated_at TEXT")
            if "share_key" not in category_column_names:
                await db.execute("ALTER TABLE categories ADD COLUMN share_key TEXT")

            category_stat_columns = {
                "file_count": "BIGINT NOT NULL DEFAULT 0",
                "image_count": "BIGINT NOT NULL DEFAULT 0",
                "video_count": "BIGINT NOT NULL DEFAULT 0",
                "animation_count": "BIGINT NOT NULL DEFAULT 0",
                "audio_count": "BIGINT NOT NULL DEFAULT 0",
                "document_count": "BIGINT NOT NULL DEFAULT 0",
                "total_size": "BIGINT NOT NULL DEFAULT 0",
                "sized_count": "BIGINT NOT NULL DEFAULT 0",
            }
            for column, definition in category_stat_columns.items():
                if column not in category_column_names:
                    await db.execute(
                        f"ALTER TABLE categories ADD COLUMN {column} {definition}"
                    )
            await db.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_categories_share_key "
                "ON categories(share_key) WHERE share_key IS NOT NULL"
            )
            await db.execute(
                "CREATE INDEX IF NOT EXISTS idx_files_category_created_at "
                "ON files(category, created_at DESC)"
            )
            await db.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS idx_files_source_fingerprint
                ON files(source_fingerprint)
                WHERE source_fingerprint IS NOT NULL
            """)
            stats_migration = await (await db.execute(
                "SELECT value FROM settings WHERE key='category_stats_v1'"
            )).fetchone()
            if not stats_migration:
                legacy_categories = await (await db.execute(
                    "SELECT DISTINCT category FROM files "
                    "WHERE category IS NOT NULL AND category<>''"
                )).fetchall()
                for item in legacy_categories:
                    name = str(item["category"])
                    await db.execute(
                        "INSERT INTO categories(name, created_at, description, updated_at, share_key) "
                        "VALUES(?, ?, '', ?, ?) ON CONFLICT(name) DO NOTHING",
                        (name, iso_now(), iso_now(),
                         secrets.token_urlsafe(6).replace("-", "").replace("_", "")[:10]),
                    )
                    await self._refresh_category_stats(db, name)
                missing_share_keys = await (await db.execute(
                    "SELECT name FROM categories WHERE share_key IS NULL OR share_key=''"
                )).fetchall()
                for item in missing_share_keys:
                    await db.execute(
                        "UPDATE categories SET share_key=? WHERE name=? "
                        "AND (share_key IS NULL OR share_key='')",
                        (secrets.token_urlsafe(6).replace("-", "").replace("_", "")[:10],
                         item["name"]),
                    )
                await db.execute(
                    "INSERT INTO settings(key, value) VALUES('category_stats_v1', 'completed')"
                )
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

    async def is_admin(self, user_id: int) -> bool:
        async with self.connect() as db:
            row = await (await db.execute(
                "SELECT 1 FROM admins WHERE user_id=?", (int(user_id),)
            )).fetchone()
            return row is not None

    async def add_admin(self, user_id: int, added_by: int) -> bool:
        async with self.connect() as db:
            cursor = await db.execute(
                "INSERT INTO admins(user_id, added_by, added_at) VALUES(?, ?, ?) "
                "ON CONFLICT(user_id) DO NOTHING",
                (int(user_id), int(added_by), iso_now()),
            )
            await db.commit()
            return cursor.rowcount > 0

    async def remove_admin(self, user_id: int) -> bool:
        async with self.connect() as db:
            cursor = await db.execute(
                "DELETE FROM admins WHERE user_id=?", (int(user_id),)
            )
            await db.commit()
            return cursor.rowcount > 0

    async def list_admins(self):
        async with self.connect() as db:
            return await (await db.execute(
                "SELECT user_id, added_by, added_at FROM admins ORDER BY added_at, user_id"
            )).fetchall()

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

    async def get_existing_fingerprints(self, fingerprints):
        """Return already-imported fingerprints with a small number of batched queries."""
        values = list(dict.fromkeys(fingerprints))
        found = set()
        # Keep each IN clause below common SQLite parameter limits.
        for start in range(0, len(values), 500):
            batch = values[start:start + 500]
            if not batch:
                continue
            placeholders = ",".join("?" for _ in batch)
            async with self.connect() as db:
                rows = await (await db.execute(
                    f"SELECT source_fingerprint FROM files "
                    f"WHERE source_fingerprint IN ({placeholders})",
                    batch,
                )).fetchall()
            found.update(row["source_fingerprint"] for row in rows)
        return found

    async def start_upload_batch(self, owner_id, category):
        async with self.connect() as db:
            await db.execute(
                "INSERT INTO upload_batches(owner_id, category, created_at) VALUES(?, ?, ?) "
                "ON CONFLICT(owner_id) DO UPDATE SET category=excluded.category, created_at=excluded.created_at",
                (int(owner_id), str(category)[:120], iso_now()),
            )
            await db.commit()

    async def get_upload_batch(self, owner_id):
        async with self.connect() as db:
            return await (await db.execute(
                "SELECT owner_id, category, created_at FROM upload_batches WHERE owner_id=?",
                (int(owner_id),),
            )).fetchone()

    async def stage_upload_batch_item(
        self, key, owner_id, file_name, media_type, category,
        primary_channel_id, primary_id, backup_channel_id, backup_id,
        source_fingerprint=None, file_size=0, backup_error="",
    ):
        async with self.connect() as db:
            cur = await db.execute("""
                INSERT OR IGNORE INTO upload_batch_items(
                    file_key, owner_id, file_name, media_type, category,
                    primary_channel_id, primary_message_id, backup_channel_id,
                    backup_message_id, source_fingerprint, file_size,
                    backup_error, created_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                key, int(owner_id), str(file_name), str(media_type), str(category),
                int(primary_channel_id), int(primary_id), int(backup_channel_id),
                int(backup_id or 0), source_fingerprint, max(0, int(file_size or 0)),
                str(backup_error or "")[:500], iso_now(),
            ))
            await db.commit()
            return cur.rowcount == 1

    async def publish_upload_batch(self, owner_id):
        """Atomically publish every staged item in a batch to the member catalog."""
        async with self.connect() as db:
            batch = await (await db.execute(
                "SELECT category FROM upload_batches WHERE owner_id=?", (int(owner_id),)
            )).fetchone()
            if not batch:
                return {"published": 0, "failed_backup": 0, "category": None}
            category = str(batch["category"])
            items = await (await db.execute(
                "SELECT * FROM upload_batch_items WHERE owner_id=? ORDER BY created_at, file_key",
                (int(owner_id),),
            )).fetchall()
            published = 0
            failed_backup = 0
            for item in items:
                cur = await db.execute("""
                    INSERT OR IGNORE INTO files(
                        file_key, owner_id, file_name, media_type, category,
                        primary_message_id, backup_message_id, created_at,
                        source_fingerprint, file_size
                    ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    item["file_key"], item["owner_id"], item["file_name"],
                    item["media_type"], item["category"], item["primary_message_id"],
                    item["backup_message_id"], item["created_at"],
                    item["source_fingerprint"], item["file_size"],
                ))
                if cur.rowcount == 1:
                    published += 1
                    await self._refresh_category_stats(db, item["category"])
                    if item["backup_error"]:
                        failed_backup += 1
                        now = iso_now()
                        await db.execute("""
                            INSERT INTO backup_retry_jobs(
                                file_key, primary_channel_id, primary_message_id,
                                backup_channel_id, attempts, next_retry_at,
                                last_error, created_at, updated_at
                            ) VALUES(?, ?, ?, ?, 0, ?, ?, ?, ?)
                            ON CONFLICT(file_key) DO NOTHING
                        """, (
                            item["file_key"], item["primary_channel_id"],
                            item["primary_message_id"], item["backup_channel_id"],
                            now, item["backup_error"], now, now,
                        ))
            await db.execute("DELETE FROM upload_batch_items WHERE owner_id=?", (int(owner_id),))
            await db.execute("DELETE FROM upload_batches WHERE owner_id=?", (int(owner_id),))
            await db.commit()
            return {"published": published, "failed_backup": failed_backup, "category": category}

    async def add_file(self, key, owner_id, file_name, media_type,
                       primary_id, backup_id, category="未分类",
                       source_fingerprint=None, file_size=0, created_at=None):
        async with self.connect() as db:
            cur = await db.execute("""
                INSERT OR IGNORE INTO files(
                    file_key, owner_id, file_name, media_type, category,
                    primary_message_id, backup_message_id, created_at,
                    source_fingerprint, file_size
                )
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (key, owner_id, file_name, media_type, category,
                  primary_id, backup_id, created_at or iso_now(),
                  source_fingerprint, max(0, int(file_size or 0))))
            if cur.rowcount == 1:
                await self._refresh_category_stats(db, category)
            await db.commit()
            return cur.rowcount == 1

    async def update_backup_message_id(self, key, backup_id, file_size=0):
        async with self.connect() as db:
            cur = await db.execute("""
                UPDATE files
                SET backup_message_id=?, file_size=CASE
                    WHEN ? > 0 THEN ? ELSE file_size END
                WHERE file_key=? AND (backup_message_id IS NULL OR backup_message_id=0)
            """, (int(backup_id), int(file_size or 0), int(file_size or 0), key))
            if cur.rowcount == 1:
                row = await (await db.execute(
                    "SELECT category FROM files WHERE file_key=?", (key,)
                )).fetchone()
                if row:
                    await self._refresh_category_stats(db, row["category"], create=False)
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

            # Primary files are available immediately; backups are optional.
            primary = current if channel_id == primary_channel_id else other
            backup = other if channel_id == primary_channel_id else current
            inserted = False
            if primary:
                primary_id = primary["message_id"]
                backup_id = backup["message_id"] if backup else 0
                cur = await db.execute("""
                    INSERT OR IGNORE INTO files(                        file_key, owner_id, file_name, media_type, category,
                        primary_message_id, backup_message_id, created_at,
                        source_fingerprint
                    ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    file_key, owner_id, primary["file_name"],
                    primary["media_type"], primary["category"],
                    primary_id, backup_id, primary["created_at"], fingerprint
                ))
                inserted = cur.rowcount == 1
                if backup:
                    await db.execute("""
                        UPDATE files SET backup_message_id=?
                        WHERE source_fingerprint=? AND primary_message_id=?
                          AND backup_message_id=0
                    """, (backup_id, fingerprint, primary_id))
            if inserted and primary:
                await self._refresh_category_stats(db, primary["category"])
            await db.commit()
            return bool(other), inserted

    async def publish_primary_catalog(self, primary_channel_id, owner_id):
        """Backfill primary-channel items scanned by older dual-match-only code."""
        created = 0
        async with self.connect() as db:
            rows = await (await db.execute("""
                SELECT s.message_id, s.fingerprint, s.file_name,
                       s.media_type, s.category, s.created_at
                FROM scanner_messages AS s
                LEFT JOIN files AS f ON f.source_fingerprint=s.fingerprint
                WHERE s.channel_id=? AND f.file_key IS NULL
                ORDER BY s.message_id
            """, (primary_channel_id,))).fetchall()
            changed_categories = set()
            for row in rows:
                key = secrets.token_urlsafe(9).replace("-", "").replace("_", "")
                cur = await db.execute("""
                    INSERT OR IGNORE INTO files(
                        file_key, owner_id, file_name, media_type, category,
                        primary_message_id, backup_message_id, created_at,
                        source_fingerprint
                    ) VALUES(?, ?, ?, ?, ?, ?, 0, ?, ?)
                """, (
                    key, owner_id, row["file_name"], row["media_type"],
                    row["category"], row["message_id"], row["created_at"],
                    row["fingerprint"]
                ))
                if cur.rowcount == 1:
                    created += 1
                    changed_categories.add(row["category"])
            for category in changed_categories:
                await self._refresh_category_stats(db, category)
            await db.commit()
        return created

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

    async def get_category_delivery_progress(self, user_id: int, share_key: str) -> int:
        async with self.connect() as db:
            row = await (await db.execute(
                "SELECT next_offset FROM category_delivery_progress WHERE user_id=? AND share_key=?",
                (user_id, share_key),
            )).fetchone()
            return int(row["next_offset"] or 0) if row else 0

    async def set_category_delivery_progress(self, user_id: int, share_key: str, next_offset: int):
        async with self.connect() as db:
            await db.execute("""
                INSERT INTO category_delivery_progress(user_id, share_key, next_offset, updated_at)
                VALUES(?, ?, ?, ?)
                ON CONFLICT(user_id, share_key) DO UPDATE SET
                    next_offset=CASE
                        WHEN category_delivery_progress.next_offset > excluded.next_offset
                        THEN category_delivery_progress.next_offset
                        ELSE excluded.next_offset
                    END,
                    updated_at=excluded.updated_at
            """, (user_id, share_key, max(0, int(next_offset)), iso_now()))
            await db.commit()

    async def enqueue_backup_retry(
        self, file_key, primary_channel_id, primary_message_id,
        backup_channel_id, error=""
    ):
        now = iso_now()
        async with self.connect() as db:
            await db.execute("""
                INSERT INTO backup_retry_jobs(
                    file_key, primary_channel_id, primary_message_id,
                    backup_channel_id, attempts, next_retry_at,
                    last_error, created_at, updated_at
                ) VALUES(?, ?, ?, ?, 0, ?, ?, ?, ?)
                ON CONFLICT(file_key) DO UPDATE SET
                    primary_channel_id=excluded.primary_channel_id,
                    primary_message_id=excluded.primary_message_id,
                    backup_channel_id=excluded.backup_channel_id,
                    next_retry_at=excluded.next_retry_at,
                    last_error=excluded.last_error,
                    updated_at=excluded.updated_at
            """, (
                file_key, int(primary_channel_id), int(primary_message_id),
                int(backup_channel_id), now, str(error or "")[:500], now, now,
            ))
            await db.commit()

    async def list_due_backup_retries(self, limit=10):
        async with self.connect() as db:
            return await (await db.execute("""
                SELECT * FROM backup_retry_jobs
                WHERE next_retry_at<=?
                ORDER BY next_retry_at, created_at
                LIMIT ?
            """, (iso_now(), max(1, min(50, int(limit)))))).fetchall()

    async def claim_backup_retry(self, file_key, lease_seconds=300):
        """Atomically lease one due job so concurrent workers cannot copy it together."""
        now = utc_now()
        lease_until = (now + timedelta(seconds=max(60, int(lease_seconds)))).isoformat()
        async with self.connect() as db:
            result = await db.execute("""
                UPDATE backup_retry_jobs
                SET next_retry_at=?, updated_at=?
                WHERE file_key=? AND next_retry_at<=?
            """, (lease_until, iso_now(), file_key, iso_now()))
            if getattr(result, "rowcount", 0) != 1:
                return None
            row = await (await db.execute(
                "SELECT * FROM backup_retry_jobs WHERE file_key=?", (file_key,)
            )).fetchone()
            await db.commit()
            return row

    async def fail_backup_retry(self, file_key, error=""):
        async with self.connect() as db:
            row = await (await db.execute(
                "SELECT attempts FROM backup_retry_jobs WHERE file_key=?",
                (file_key,),
            )).fetchone()
            if not row:
                return
            attempts = int(row["attempts"] or 0) + 1
            delay = min(3600, 30 * (2 ** min(attempts - 1, 7)))
            next_retry = (utc_now() + timedelta(seconds=delay)).isoformat()
            await db.execute("""
                UPDATE backup_retry_jobs
                SET attempts=?, next_retry_at=?, last_error=?, updated_at=?
                WHERE file_key=?
            """, (attempts, next_retry, str(error or "")[:500], iso_now(), file_key))
            await db.commit()

    async def complete_backup_retry(self, file_key):
        async with self.connect() as db:
            await db.execute(
                "DELETE FROM backup_retry_jobs WHERE file_key=?", (file_key,)
            )
            await db.commit()

    async def get_file(self, key):
        async with self.connect() as db:
            return await (await db.execute(
                "SELECT * FROM files WHERE file_key=?", (key,)
            )).fetchone()

    async def get_files_by_keys(self, file_keys):
        keys = list(dict.fromkeys(str(key) for key in file_keys))
        if not keys:
            return []
        placeholders = ",".join("?" for _ in keys)
        async with self.connect() as db:
            return await (await db.execute(
                f"SELECT * FROM files WHERE file_key IN ({placeholders})",
                keys,
            )).fetchall()

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
                "SELECT category FROM files GROUP BY category ORDER BY lower(category)"
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
            current = await (await db.execute(
                "SELECT category FROM files WHERE file_key=?", (key,)
            )).fetchone()
            cur = await db.execute(
                "UPDATE files SET category=? WHERE file_key=?", (category, key)
            )
            if cur.rowcount > 0:
                await self._refresh_category_stats(db, category)
                if current and current["category"] != category:
                    await self._refresh_category_stats(db, current["category"], create=False)
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

    async def catalog_stats(self):
        async with self.connect() as db:
            row = await (await db.execute("""
                SELECT COUNT(*) AS messages,
                       (SELECT COUNT(*) FROM (
                           SELECT category FROM files
                           UNION
                           SELECT name AS category FROM categories
                       )) AS folders,
                       SUM(CASE WHEN lower(media_type) IN ('photo', 'image')
                           OR (lower(media_type)='document' AND (
                               lower(file_name) LIKE '%.jpg' OR lower(file_name) LIKE '%.jpeg'
                               OR lower(file_name) LIKE '%.png' OR lower(file_name) LIKE '%.webp'
                               OR lower(file_name) LIKE '%.bmp' OR lower(file_name) LIKE '%.heic'
                               OR lower(file_name) LIKE '%.heif' OR lower(file_name) LIKE '%.tif'
                               OR lower(file_name) LIKE '%.tiff'
                           )) THEN 1 ELSE 0 END) AS images,
                       SUM(CASE WHEN lower(media_type)='video'
                           OR (lower(media_type)='document' AND (
                               lower(file_name) LIKE '%.mp4' OR lower(file_name) LIKE '%.mkv'
                               OR lower(file_name) LIKE '%.mov' OR lower(file_name) LIKE '%.avi'
                               OR lower(file_name) LIKE '%.webm' OR lower(file_name) LIKE '%.m4v'
                               OR lower(file_name) LIKE '%.mpeg' OR lower(file_name) LIKE '%.mpg'
                               OR lower(file_name) LIKE '%.3gp' OR lower(file_name) LIKE '%.ts'
                               OR lower(file_name) LIKE '%.wmv' OR lower(file_name) LIKE '%.flv'
                           )) THEN 1 ELSE 0 END) AS videos,
                       SUM(CASE WHEN lower(media_type)='animation'
                           OR (lower(media_type)='document' AND (
                               lower(file_name) LIKE '%.gif' OR lower(file_name) LIKE '%.apng'
                           )) THEN 1 ELSE 0 END) AS animations,
                       SUM(CASE WHEN lower(media_type) IN ('audio', 'voice')
                           OR (lower(media_type)='document' AND (
                               lower(file_name) LIKE '%.mp3' OR lower(file_name) LIKE '%.m4a'
                               OR lower(file_name) LIKE '%.aac' OR lower(file_name) LIKE '%.flac'
                               OR lower(file_name) LIKE '%.wav' OR lower(file_name) LIKE '%.ogg'
                               OR lower(file_name) LIKE '%.opus' OR lower(file_name) LIKE '%.wma'
                           )) THEN 1 ELSE 0 END) AS audios
                FROM files
            """)).fetchone()
            return {
                "folders": int(row["folders"] or 0),
                "messages": int(row["messages"] or 0),
                "images": int(row["images"] or 0),
                "videos": int(row["videos"] or 0),
                "animations": int(row["animations"] or 0),
                "audios": int(row["audios"] or 0),
            }

    async def _refresh_category_stats(self, db, category: str, create=True):
        """Recalculate and persist one category summary; never scans unrelated folders."""
        category = str(category or "").strip()
        if not category:
            return
        if create:
            await db.execute(
                "INSERT INTO categories(name, created_at, description, updated_at, share_key) "
                "VALUES(?, ?, '', ?, ?) ON CONFLICT(name) DO NOTHING",
                (category, iso_now(), iso_now(),
                 secrets.token_urlsafe(6).replace("-", "").replace("_", "")[:10]),
            )
        row = await (await db.execute("""
            SELECT COUNT(*) AS file_count,
                   SUM(CASE WHEN lower(media_type) IN ('photo', 'image')
                       OR (lower(media_type)='document' AND (
                           lower(file_name) LIKE '%.jpg' OR lower(file_name) LIKE '%.jpeg'
                           OR lower(file_name) LIKE '%.png' OR lower(file_name) LIKE '%.webp'
                           OR lower(file_name) LIKE '%.bmp' OR lower(file_name) LIKE '%.heic'
                           OR lower(file_name) LIKE '%.heif' OR lower(file_name) LIKE '%.tif'
                           OR lower(file_name) LIKE '%.tiff'
                       )) THEN 1 ELSE 0 END) AS image_count,
                   SUM(CASE WHEN lower(media_type)='video'
                       OR (lower(media_type)='document' AND (
                           lower(file_name) LIKE '%.mp4' OR lower(file_name) LIKE '%.mkv'
                           OR lower(file_name) LIKE '%.mov' OR lower(file_name) LIKE '%.avi'
                           OR lower(file_name) LIKE '%.webm' OR lower(file_name) LIKE '%.m4v'
                           OR lower(file_name) LIKE '%.mpeg' OR lower(file_name) LIKE '%.mpg'
                           OR lower(file_name) LIKE '%.3gp' OR lower(file_name) LIKE '%.ts'
                           OR lower(file_name) LIKE '%.wmv' OR lower(file_name) LIKE '%.flv'
                       )) THEN 1 ELSE 0 END) AS video_count,
                   SUM(CASE WHEN lower(media_type)='animation'
                       OR (lower(media_type)='document' AND (
                           lower(file_name) LIKE '%.gif' OR lower(file_name) LIKE '%.apng'
                       )) THEN 1 ELSE 0 END) AS animation_count,
                   SUM(CASE WHEN lower(media_type) IN ('audio', 'voice')
                       OR (lower(media_type)='document' AND (
                           lower(file_name) LIKE '%.mp3' OR lower(file_name) LIKE '%.m4a'
                           OR lower(file_name) LIKE '%.aac' OR lower(file_name) LIKE '%.flac'
                           OR lower(file_name) LIKE '%.wav' OR lower(file_name) LIKE '%.ogg'
                           OR lower(file_name) LIKE '%.opus' OR lower(file_name) LIKE '%.wma'
                       )) THEN 1 ELSE 0 END) AS audio_count,
                   SUM(CASE WHEN lower(media_type)='document'
                       AND NOT (
                           lower(file_name) LIKE '%.jpg' OR lower(file_name) LIKE '%.jpeg'
                           OR lower(file_name) LIKE '%.png' OR lower(file_name) LIKE '%.webp'
                           OR lower(file_name) LIKE '%.bmp' OR lower(file_name) LIKE '%.heic'
                           OR lower(file_name) LIKE '%.heif' OR lower(file_name) LIKE '%.tif'
                           OR lower(file_name) LIKE '%.tiff'
                           OR lower(file_name) LIKE '%.mp4' OR lower(file_name) LIKE '%.mkv'
                           OR lower(file_name) LIKE '%.mov' OR lower(file_name) LIKE '%.avi'
                           OR lower(file_name) LIKE '%.webm' OR lower(file_name) LIKE '%.m4v'
                           OR lower(file_name) LIKE '%.mpeg' OR lower(file_name) LIKE '%.mpg'
                           OR lower(file_name) LIKE '%.3gp' OR lower(file_name) LIKE '%.ts'
                           OR lower(file_name) LIKE '%.wmv' OR lower(file_name) LIKE '%.flv'
                           OR lower(file_name) LIKE '%.gif' OR lower(file_name) LIKE '%.apng'
                           OR lower(file_name) LIKE '%.mp3' OR lower(file_name) LIKE '%.m4a'
                           OR lower(file_name) LIKE '%.aac' OR lower(file_name) LIKE '%.flac'
                           OR lower(file_name) LIKE '%.wav' OR lower(file_name) LIKE '%.ogg'
                           OR lower(file_name) LIKE '%.opus' OR lower(file_name) LIKE '%.wma'
                       ) THEN 1 ELSE 0 END) AS document_count,
                   SUM(COALESCE(file_size, 0)) AS total_size,
                   SUM(CASE WHEN COALESCE(file_size, 0)>0 THEN 1 ELSE 0 END) AS sized_count
            FROM files WHERE category=?
        """, (category,))).fetchone()
        await db.execute("""
            UPDATE categories SET
                file_count=?, image_count=?, video_count=?, animation_count=?,
                audio_count=?, document_count=?, total_size=?, sized_count=?,
                updated_at=?
            WHERE name=?
        """, (
            int(row["file_count"] or 0), int(row["image_count"] or 0),
            int(row["video_count"] or 0), int(row["animation_count"] or 0),
            int(row["audio_count"] or 0), int(row["document_count"] or 0),
            int(row["total_size"] or 0), int(row["sized_count"] or 0),
            iso_now(), category,
        ))

    async def save_category(self, name: str, description="", created_at=None):
        name = str(name or "").strip()[:120]
        if not name:
            return
        now = iso_now()
        created = str(created_at or now)
        async with self.connect() as db:
            share_key = secrets.token_urlsafe(6).replace("-", "").replace("_", "")[:10]
            await db.execute(
                "INSERT INTO categories(name, created_at, description, updated_at, share_key) "
                "VALUES(?, ?, ?, ?, ?) ON CONFLICT(name) DO UPDATE SET "
                "description=CASE WHEN excluded.description<>'' THEN excluded.description "
                "ELSE categories.description END, "
                "created_at=CASE WHEN categories.created_at IS NULL OR "
                "excluded.created_at<categories.created_at THEN excluded.created_at "
                "ELSE categories.created_at END, "
                "share_key=COALESCE(NULLIF(categories.share_key, ''), excluded.share_key)",
                (name, created, str(description or "")[:1000], now, share_key),
            )
            await db.commit()

    async def get_category_by_index(self, index):
        """Load one folder summary without aggregating the whole file catalog."""
        index = max(0, int(index))
        async with self.connect() as db:
            row = await (await db.execute("""
                SELECT name AS category FROM categories
                ORDER BY lower(name) LIMIT 1 OFFSET ?
            """, (index,))).fetchone()
        if not row:
            return None
        rows = await self.list_categories(category=str(row["category"]))
        return rows[0] if rows else None

    async def list_categories(self, category=None):
        """Read saved folder summaries; counts are refreshed only when that folder's files change."""
        async with self.connect() as db:
            return await (await db.execute("""
                SELECT c.name AS category,
                       COALESCE(c.file_count, 0) AS file_count,
                       COALESCE(c.image_count, 0) AS image_count,
                       COALESCE(c.video_count, 0) AS video_count,
                       COALESCE(c.animation_count, 0) AS animation_count,
                       COALESCE(c.audio_count, 0) AS audio_count,
                       COALESCE(c.document_count, 0) AS document_count,
                       COALESCE(c.total_size, 0) AS total_size,
                       COALESCE(c.sized_count, 0) AS sized_count,
                       COALESCE(c.description, '') AS description,
                       c.created_at AS created_at,
                       c.updated_at AS updated_at,
                       c.share_key AS share_key
                FROM categories c
                WHERE (CAST(? AS TEXT) IS NULL OR c.name=CAST(? AS TEXT))
                ORDER BY lower(c.name)
            """, (category, category))).fetchall()

    async def rename_category(self, old_name: str, new_name: str) -> bool:
        old_name = str(old_name or "").strip()[:120]
        new_name = str(new_name or "").strip()[:120]
        if not old_name or not new_name or old_name == new_name:
            return False
        async with self.connect() as db:
            exists = await (await db.execute(
                "SELECT 1 FROM categories WHERE name=?", (new_name,)
            )).fetchone()
            file_exists = await (await db.execute(
                "SELECT 1 FROM files WHERE category=? LIMIT 1", (new_name,)
            )).fetchone()
            if exists or file_exists:
                return False
            await db.execute(
                "UPDATE files SET category=? WHERE category=?", (new_name, old_name)
            )
            current = await (await db.execute(
                "SELECT name FROM categories WHERE name=?", (old_name,)
            )).fetchone()
            if current:
                await db.execute(
                    "UPDATE categories SET name=?, updated_at=? WHERE name=?",
                    (new_name, iso_now(), old_name),
                )
            else:
                await db.execute(
                    "INSERT INTO categories(name, created_at, description, updated_at, share_key) "
                    "VALUES(?, ?, '', ?, ?)",
                    (new_name, iso_now(), iso_now(),
                     secrets.token_urlsafe(6).replace("-", "").replace("_", "")[:10]),
                )
            await self._refresh_category_stats(db, new_name)
            await db.commit()
            return True

    async def delete_empty_category(self, name: str) -> bool:
        name = str(name or "").strip()
        if not name:
            return False
        async with self.connect() as db:
            has_files = await (await db.execute(
                "SELECT 1 FROM files WHERE category=? LIMIT 1", (name,)
            )).fetchone()
            if has_files:
                return False
            row = await (await db.execute(
                "SELECT share_key FROM categories WHERE name=?", (name,)
            )).fetchone()
            if not row:
                return False
            share_key = row["share_key"]
            if share_key:
                await db.execute(
                    "DELETE FROM category_favorites WHERE share_key=?", (share_key,)
                )
                await db.execute(
                    "DELETE FROM category_delivery_progress WHERE share_key=?", (share_key,)
                )
            await db.execute("DELETE FROM categories WHERE name=?", (name,))
            await db.commit()
            return True

    async def cleanup_empty_categories(self) -> int:
        """Delete all categories that have no associated files and return the count."""
        async with self.connect() as db:
            rows = await (await db.execute("""
                SELECT c.name, c.share_key
                FROM categories c
                LEFT JOIN files f ON f.category = c.name
                WHERE f.file_key IS NULL
            """)).fetchall()
            deleted = 0
            for row in rows:
                name = row["name"]
                share_key = row["share_key"]
                if share_key:
                    await db.execute(
                        "DELETE FROM category_favorites WHERE share_key=?", (share_key,)
                    )
                    await db.execute(
                        "DELETE FROM category_delivery_progress WHERE share_key=?", (share_key,)
                    )
                await db.execute("DELETE FROM categories WHERE name=?", (name,))
                deleted += 1
            if deleted:
                await db.commit()
            return deleted

    async def get_category_by_share_key(self, share_key):
        async with self.connect() as db:
            return await (await db.execute(
                "SELECT name, description, created_at, updated_at, share_key "
                "FROM categories WHERE share_key=?", (share_key,)
            )).fetchone()

    async def list_category_files(self, category, limit=8, offset=0):
        async with self.connect() as db:
            return await (await db.execute("""
                SELECT file_key, file_name, media_type, category
                FROM files WHERE category=?
                ORDER BY created_at DESC LIMIT ? OFFSET ?
            """, (category, limit, offset))).fetchall()

    async def record_history(self, user_id, file_key):
        async with self.connect() as db:
            await db.execute("""
                INSERT INTO user_history(user_id, file_key, accessed_at)
                VALUES(?, ?, ?)
                ON CONFLICT(user_id, file_key) DO UPDATE SET
                    accessed_at=excluded.accessed_at
            """, (user_id, file_key, iso_now()))
            await db.commit()

    async def record_history_batch(self, user_id, file_keys):
        keys = list(dict.fromkeys(str(key) for key in file_keys))
        if not keys:
            return
        accessed_at = iso_now()
        async with self.connect() as db:
            await db.executemany("""
                INSERT INTO user_history(user_id, file_key, accessed_at)
                VALUES(?, ?, ?)
                ON CONFLICT(user_id, file_key) DO UPDATE SET
                    accessed_at=excluded.accessed_at
            """, [(user_id, key, accessed_at) for key in keys])
            await db.commit()

    async def list_user_history(self, user_id, limit=10):
        async with self.connect() as db:
            return await (await db.execute("""
                SELECT f.file_key, f.file_name, f.category, h.accessed_at
                FROM user_history h JOIN files f ON f.file_key=h.file_key
                WHERE h.user_id=? ORDER BY h.accessed_at DESC LIMIT ?
            """, (user_id, limit))).fetchall()

    async def toggle_favorite(self, user_id, file_key):
        async with self.connect() as db:
            exists = await (await db.execute(
                "SELECT 1 FROM favorites WHERE user_id=? AND file_key=?",
                (user_id, file_key),
            )).fetchone()
            if exists:
                await db.execute(
                    "DELETE FROM favorites WHERE user_id=? AND file_key=?",
                    (user_id, file_key),
                )
                result = False
            else:
                await db.execute("""
                    INSERT INTO favorites(user_id, file_key, created_at)
                    VALUES(?, ?, ?)
                    ON CONFLICT(user_id, file_key) DO NOTHING
                """, (user_id, file_key, iso_now()))
                result = True
            await db.commit()
            return result

    async def is_favorite(self, user_id, file_key):
        async with self.connect() as db:
            row = await (await db.execute(
                "SELECT 1 FROM favorites WHERE user_id=? AND file_key=?",
                (user_id, file_key),
            )).fetchone()
            return row is not None

    async def list_favorites(self, user_id, limit=10):
        async with self.connect() as db:
            return await (await db.execute("""
                SELECT f.file_key, f.file_name, f.category, v.created_at
                FROM favorites v JOIN files f ON f.file_key=v.file_key
                WHERE v.user_id=? ORDER BY v.created_at DESC LIMIT ?
            """, (user_id, limit))).fetchall()

    async def toggle_category_favorite(self, user_id, share_key):
        async with self.connect() as db:
            exists = await (await db.execute(
                "SELECT 1 FROM category_favorites WHERE user_id=? AND share_key=?",
                (user_id, share_key),
            )).fetchone()
            if exists:
                await db.execute(
                    "DELETE FROM category_favorites WHERE user_id=? AND share_key=?",
                    (user_id, share_key),
                )
                added = False
            else:
                await db.execute(
                    "INSERT INTO category_favorites(user_id, share_key, created_at) VALUES(?, ?, ?)",
                    (user_id, share_key, iso_now()),
                )
                added = True
            await db.commit()
            return added

    async def is_category_favorite(self, user_id, share_key):
        async with self.connect() as db:
            row = await (await db.execute(
                "SELECT 1 FROM category_favorites WHERE user_id=? AND share_key=?",
                (user_id, share_key),
            )).fetchone()
            return row is not None

    async def list_favorite_categories(self, user_id, limit=30):
        async with self.connect() as db:
            return await (await db.execute("""
                SELECT c.name, c.share_key, f.created_at
                FROM category_favorites f
                JOIN categories c ON c.share_key=f.share_key
                WHERE f.user_id=?
                ORDER BY f.created_at DESC LIMIT ?
            """, (user_id, limit))).fetchall()