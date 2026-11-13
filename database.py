import os
import asyncio
import logging
import aiosqlite
import re
import secrets
import time
from contextlib import asynccontextmanager

import psycopg
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool
from datetime import datetime, timezone, timedelta

log = logging.getLogger(__name__)

# A folder summary is display-only. Retelling a whole folder on every single
# upload made each file cost a full scan (measured 14 ms on a 5k-file folder),
# so writes now coalesce: at most one recount per folder per interval, with a
# catch-up recount scheduled once the interval ends. Folder counts can lag the
# truth by at most this many seconds.
SETTING_CACHE_TTL = 60.0
CATEGORY_STATS_MIN_INTERVAL = 30.0


# Catalog pages need each album's size and newest timestamp. Computing those
# with a correlated subquery costs one extra scan per returned row, so the
# aggregate is built once per query and joined instead. Query the outer table
# as `files f` and reference `album_agg.album_size` / `album_agg.album_latest`.
ALBUM_AGGREGATE_JOIN = (
    " LEFT JOIN (SELECT album_group_id, COUNT(*) AS album_size, "
    "MAX(created_at) AS album_latest FROM files "
    "WHERE album_group_id IS NOT NULL GROUP BY album_group_id) album_agg "
    "ON album_agg.album_group_id=f.album_group_id"
)

# Same aggregate, narrowed to the folder being browsed (one bound parameter
# for the category), so a folder page does not count every album in the catalog.
ALBUM_CATEGORY_AGGREGATE_JOIN = (
    " LEFT JOIN (SELECT album_group_id, COUNT(*) AS album_size, "
    "MAX(created_at) AS album_latest FROM files "
    "WHERE album_group_id IS NOT NULL AND category=? GROUP BY album_group_id) album_agg "
    "ON album_agg.album_group_id=f.album_group_id"
)


def utc_now():
    return datetime.now(timezone.utc)


def iso_now():
    return utc_now().isoformat()


IMAGE_EXTS = (".jpg", ".jpeg", ".png", ".webp", ".bmp", ".heic", ".heif", ".tif", ".tiff")
VIDEO_EXTS = (".mp4", ".mkv", ".mov", ".avi", ".webm", ".m4v", ".mpeg", ".mpg", ".3gp", ".ts", ".wmv", ".flv")
ANIMATION_EXTS = (".gif", ".apng")
AUDIO_EXTS = (".mp3", ".m4a", ".aac", ".flac", ".wav", ".ogg", ".opus", ".wma")


def media_kind_of(media_type, file_name):
    """Classify a stored media once, so folder stats need no per-row LIKE checks."""
    kind_type = str(media_type or "").strip().lower()
    if kind_type in ("photo", "image"):
        return "image"
    if kind_type in ("video", "video_note"):
        return "video"
    if kind_type == "animation":
        return "animation"
    if kind_type in ("audio", "voice"):
        return "audio"
    name = str(file_name or "").strip().lower()
    for extensions, kind in (
        (IMAGE_EXTS, "image"),
        (ANIMATION_EXTS, "animation"),
        (VIDEO_EXTS, "video"),
        (AUDIO_EXTS, "audio"),
    ):
        if name.endswith(extensions):
            return kind
    return "document"


def clean_file_name(value):
    """Remove obvious promotional/contact fragments without touching the media itself."""
    name = str(value or "").replace("\u200b", "").replace("\ufeff", "").strip()
    if not name:
        return "未命名资源"

    # Remove Telegram handles and common public contact/invite URLs.
    name = re.sub(r"(?i)(?:https?://)?(?:t\.me|telegram\.me|telegram\.dog)/[^\s]+", " ", name)
    name = re.sub(r"(?i)www\.[^\s]+|https?://[^\s]+", " ", name)
    name = re.sub(r"(?<![\w])@[a-z][a-z0-9_]{3,}(?![\w])", " ", name)

    # Remove clear advertising / solicitation phrases, including trailing contact text.
    promo = re.compile(
        r"(?i)(?:加群|进群|入群|私聊|联系(?:我|客服)?|商务合作|广告合作|推广|招代理|代理加盟|"
        r"扫码(?:联系|加群)?|点击(?:链接|下方)|关注(?:频道|账号)?|订阅频道|"
        r"更多资源|资源群|交流群|官方群|客服(?:号|微信|telegram)?|"
        r"微信|威信|电报号|飞机号|频道号)"
    )
    match = promo.search(name)
    if match:
        name = name[:match.start()] + " "

    # Trim separators left behind by removed ad fragments.
    name = re.sub(r"[\s:：|｜,，;；、~～·•_-]+$", "", name)
    name = re.sub(r"^[\s:：|｜,，;；、~～·•_-]+", "", name)
    name = re.sub(r"\s{2,}", " ", name).strip()
    return name[:300] if name else "未命名资源"

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
        # Escape SQL pattern wildcards such as LIKE '%.mp4', while preserving
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
        # Display-only totals are recomputed at most once per interval; every
        # catalog mutation invalidates them immediately (see _invalidate_catalog_stats).
        self._catalog_stats_cache = None
        self._catalog_stats_cache_at = 0.0
        # Per-folder throttling for summary recounts (see CATEGORY_STATS_MIN_INTERVAL).
        self._category_stats_last = {}
        self._category_stats_pending = {}
        # Settings are read on nearly every interaction. On SQLite each read used
        # to open a fresh connection (~2-4 ms), so cache the value with a write-through.
        self._setting_cache = {}

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
        # Drop queued recounts so shutdown does not leave tasks behind.
        for task in list(self._category_stats_pending.values()):
            task.cancel()
        self._category_stats_pending.clear()
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
                    await self._refresh_category_stats(pg, str(item["category"]), force=True)
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
                source_fingerprint TEXT,
                album_group_id TEXT,
                album_position BIGINT NOT NULL DEFAULT 0,
                media_kind TEXT NOT NULL DEFAULT ''
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
            CREATE TABLE IF NOT EXISTS scanner_caption_jobs (
                channel_id BIGINT NOT NULL,
                message_id BIGINT NOT NULL,
                caption TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL DEFAULT 'pending',
                attempts BIGINT NOT NULL DEFAULT 0,
                last_error TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY(channel_id, message_id)
            );
            CREATE TABLE IF NOT EXISTS user_history (
                user_id BIGINT NOT NULL,
                file_key TEXT NOT NULL,
                accessed_at TEXT NOT NULL,
                PRIMARY KEY(user_id, file_key)
            );
            CREATE TABLE IF NOT EXISTS notification_subscriptions (
                user_id BIGINT PRIMARY KEY,
                created_at TEXT NOT NULL
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
            CREATE TABLE IF NOT EXISTS delivery_failures (
                user_id BIGINT NOT NULL,
                file_key TEXT NOT NULL,
                share_key TEXT NOT NULL,
                attempts BIGINT NOT NULL DEFAULT 0,
                last_error TEXT NOT NULL DEFAULT '',
                updated_at TEXT NOT NULL,
                PRIMARY KEY(user_id, file_key, share_key)
            );
            CREATE TABLE IF NOT EXISTS delivery_audit (
                audit_id TEXT NOT NULL,
                user_id BIGINT NOT NULL,
                share_key TEXT NOT NULL DEFAULT '',
                file_key TEXT NOT NULL,
                category TEXT NOT NULL DEFAULT '',
                media_type TEXT NOT NULL DEFAULT '',
                album_group_id TEXT NOT NULL DEFAULT '',
                source_chat_id BIGINT,
                source_message_id BIGINT,
                delivery_method TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL DEFAULT 'attempted',
                telegram_message_id BIGINT,
                error_text TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,
                finished_at TEXT,
                PRIMARY KEY(audit_id, file_key)
            );
            CREATE INDEX IF NOT EXISTS idx_delivery_audit_created
                ON delivery_audit(created_at);
            CREATE INDEX IF NOT EXISTS idx_delivery_audit_file
                ON delivery_audit(file_key, created_at);
            CREATE INDEX IF NOT EXISTS idx_delivery_audit_user
                ON delivery_audit(user_id, created_at);
            CREATE TABLE IF NOT EXISTS pending_deletions (
                chat_id BIGINT NOT NULL,
                message_id BIGINT NOT NULL,
                delete_at TEXT NOT NULL,
                PRIMARY KEY(chat_id, message_id)
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
            CREATE TABLE IF NOT EXISTS backup_retry_failures (
                file_key TEXT PRIMARY KEY,
                file_name TEXT NOT NULL DEFAULT '',
                category TEXT NOT NULL DEFAULT '',
                source_fingerprint TEXT NOT NULL DEFAULT '',
                album_group_id TEXT NOT NULL DEFAULT '',
                source_message_ids TEXT NOT NULL DEFAULT '',
                last_error TEXT NOT NULL DEFAULT '',
                failed_at TEXT NOT NULL
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
                created_at TEXT NOT NULL,
                album_group_id TEXT,
                album_position BIGINT NOT NULL DEFAULT 0,
                upload_dedupe_key TEXT
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
            if "album_group_id" not in column_names:
                await db.execute("ALTER TABLE files ADD COLUMN album_group_id TEXT")
            if "album_position" not in column_names:
                await db.execute("ALTER TABLE files ADD COLUMN album_position BIGINT NOT NULL DEFAULT 0")
            if "media_kind" not in column_names:
                await db.execute(
                    "ALTER TABLE files ADD COLUMN media_kind TEXT NOT NULL DEFAULT ''"
                )
            batch_columns = await (await db.execute(
                "SELECT column_name AS name FROM information_schema.columns "
                "WHERE table_schema=current_schema() AND table_name='upload_batch_items'"
            )).fetchall() if self.uses_postgres else await (await db.execute(
                "PRAGMA table_info(upload_batch_items)"
            )).fetchall()
            batch_column_names = {row["name"] for row in batch_columns}
            if "album_group_id" not in batch_column_names:
                await db.execute("ALTER TABLE upload_batch_items ADD COLUMN album_group_id TEXT")
            if "album_position" not in batch_column_names:
                await db.execute("ALTER TABLE upload_batch_items ADD COLUMN album_position BIGINT NOT NULL DEFAULT 0")
            if "upload_dedupe_key" not in batch_column_names:
                await db.execute("ALTER TABLE upload_batch_items ADD COLUMN upload_dedupe_key TEXT")
            await db.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_upload_batch_dedupe_key "
                "ON upload_batch_items(upload_dedupe_key) WHERE upload_dedupe_key IS NOT NULL"
            )
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
                "video_size": "BIGINT NOT NULL DEFAULT 0",
                "image_size": "BIGINT NOT NULL DEFAULT 0",
                "other_size": "BIGINT NOT NULL DEFAULT 0",
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
            await db.execute(
                "CREATE INDEX IF NOT EXISTS idx_files_created_at "
                "ON files(created_at DESC)"
            )
            await db.execute(
                "CREATE INDEX IF NOT EXISTS idx_files_primary_message_id "
                "ON files(primary_message_id)"
            )
            await db.execute(
                "CREATE INDEX IF NOT EXISTS idx_files_backup_message_id "
                "ON files(backup_message_id)"
            )
            await db.execute(
                "CREATE INDEX IF NOT EXISTS idx_files_album_group "
                "ON files(album_group_id, album_position)"
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
                    await self._refresh_category_stats(db, name, force=True)
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
            size_stats_migration = await (await db.execute(
                "SELECT value FROM settings WHERE key='category_size_stats_v1'"
            )).fetchone()
            if not size_stats_migration:
                # The initial category_stats_v1 migration above already refreshed
                # every category when it ran; only older installations need this
                # one-time backfill for the newly added size columns.
                if stats_migration:
                    existing_categories = await (await db.execute(
                        "SELECT name FROM categories"
                    )).fetchall()
                    for item in existing_categories:
                        await self._refresh_category_stats(db, str(item["name"]), create=False, force=True)
                await db.execute(
                    "INSERT INTO settings(key, value) VALUES('category_size_stats_v1', 'completed')"
                )
            # One-time cleanup of obvious promotional fragments in stored display names.
            # This only changes database labels; Telegram media/messages and file keys stay intact.
            name_cleanup = await (await db.execute(
                "SELECT value FROM settings WHERE key='filename_ad_cleanup_v1'"
            )).fetchone()
            if not name_cleanup:
                # Chunked by primary key so a large library never has to be
                # loaded into memory at once during startup.
                page_size = 5000
                for table, key_columns in (
                    ("files", ("file_key",)),
                    ("scanner_messages", ("channel_id", "message_id")),
                    ("upload_batch_items", ("file_key",)),
                    ("backup_retry_failures", ("file_key",)),
                ):
                    where = " AND ".join(f"{column}=?" for column in key_columns)
                    select_columns = ", ".join(key_columns)
                    order_by = ", ".join(key_columns)
                    cleaned_total = 0
                    offset = 0
                    while True:
                        rows = await (await db.execute(
                            f"SELECT {select_columns}, file_name FROM {table} "
                            f"ORDER BY {order_by} LIMIT ? OFFSET ?",
                            (page_size, offset),
                        )).fetchall()
                        if not rows:
                            break
                        updates = []
                        for row in rows:
                            cleaned = clean_file_name(row["file_name"])
                            if cleaned != row["file_name"]:
                                updates.append(
                                    (cleaned, *(row[column] for column in key_columns))
                                )
                        if updates:
                            await db.executemany(
                                f"UPDATE {table} SET file_name=? WHERE {where}", updates
                            )
                        cleaned_total += len(updates)
                        # Keys never change here, so ordering stays stable and
                        # paging cannot skip rows.
                        offset += len(rows)
                    if cleaned_total:
                        log.info(
                            "Cleaned promotional fragments from %s stored names: %s",
                            table, cleaned_total,
                        )
                    else:
                        log.info("No promotional fragments found in %s stored names", table)
                await db.execute(
                    "INSERT INTO settings(key, value) VALUES('filename_ad_cleanup_v1', 'completed')"
                )

            await db.commit()

        if self.uses_postgres:
            await self._import_legacy_sqlite()
        await self._backfill_media_kind()

    async def get_setting(self, key: str):
        cached = self._setting_cache.get(key)
        if cached is not None and time.monotonic() - cached[1] < SETTING_CACHE_TTL:
            return cached[0]
        async with self.connect() as db:
            row = await (await db.execute(
                "SELECT value FROM settings WHERE key=?", (key,)
            )).fetchone()
        value = row["value"] if row else None
        self._setting_cache[key] = (value, time.monotonic())
        return value

    async def set_setting(self, key: str, value: str):
        async with self.connect() as db:
            await db.execute("""
                INSERT INTO settings(key, value) VALUES(?, ?)
                ON CONFLICT(key) DO UPDATE SET value=excluded.value
            """, (key, str(value)))
            await db.commit()
        # Write-through keeps the panel showing the value that was just saved.
        self._setting_cache[key] = (str(value), time.monotonic())

    async def enqueue_scanner_caption_job(self, channel_id: int, message_id: int, caption: str):
        now = iso_now()
        async with self.connect() as db:
            await db.execute("""
                INSERT INTO scanner_caption_jobs(
                    channel_id, message_id, caption, status, attempts,
                    last_error, created_at, updated_at
                ) VALUES(?, ?, ?, 'pending', 0, '', ?, ?)
                ON CONFLICT(channel_id, message_id) DO UPDATE SET
                    caption=excluded.caption, status='pending', attempts=0,
                    last_error='', updated_at=excluded.updated_at
            """, (int(channel_id), int(message_id), str(caption or ''), now, now))
            await db.commit()

    async def list_scanner_caption_jobs(self, limit: int = 20):
        async with self.connect() as db:
            return await (await db.execute("""
                SELECT channel_id, message_id, caption, attempts
                FROM scanner_caption_jobs
                WHERE status='pending'
                ORDER BY created_at, channel_id, message_id
                LIMIT ?
            """, (max(1, min(100, int(limit))),))).fetchall()

    async def complete_scanner_caption_job(self, channel_id: int, message_id: int):
        async with self.connect() as db:
            await db.execute("""
                UPDATE scanner_caption_jobs SET status='done', updated_at=?
                WHERE channel_id=? AND message_id=?
            """, (iso_now(), int(channel_id), int(message_id)))
            await db.commit()

    async def retry_scanner_caption_job(self, channel_id: int, message_id: int, error: str = ""):
        async with self.connect() as db:
            await db.execute("""
                UPDATE scanner_caption_jobs
                SET attempts=attempts+1,
                    status=CASE WHEN attempts+1 >= 20 THEN 'failed' ELSE 'pending' END,
                    last_error=?, updated_at=?
                WHERE channel_id=? AND message_id=?
            """, (str(error or '')[:500], iso_now(), int(channel_id), int(message_id)))
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

    async def get_file_by_backup_message_id(self, backup_message_id: int):
        """Legacy lookup by message ID; prefer the channel-qualified fingerprint."""
        async with self.connect() as db:
            return await (await db.execute(
                "SELECT * FROM files WHERE backup_message_id=? LIMIT 1",
                (int(backup_message_id),),
            )).fetchone()

    async def get_file_by_backup_fingerprint(self, fingerprint: str):
        """Find an indexed backup message using its channel-qualified identity."""
        async with self.connect() as db:
            return await (await db.execute(
                "SELECT * FROM files WHERE source_fingerprint=? LIMIT 1",
                (str(fingerprint),),
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

    async def get_upload_batch_item_by_fingerprint(self, fingerprint):
        """Find an admin upload already staged from the same Telegram message."""
        if not fingerprint:
            return None
        async with self.connect() as db:
            return await (await db.execute(
                "SELECT file_key FROM upload_batch_items WHERE upload_dedupe_key=? LIMIT 1",
                (str(fingerprint),),
            )).fetchone()

    async def count_upload_batch_items(self, owner_id):
        async with self.connect() as db:
            row = await (await db.execute(
                "SELECT COUNT(*) AS n FROM upload_batch_items WHERE owner_id=?",
                (int(owner_id),),
            )).fetchone()
            return int(row["n"] or 0)

    async def stage_upload_batch_item(
        self, key, owner_id, file_name, media_type, category,
        primary_channel_id, primary_id, backup_channel_id, backup_id,
        source_fingerprint=None, file_size=0, backup_error="",
        album_group_id=None, album_position=0,
    ):
        async with self.connect() as db:
            cur = await db.execute("""
                INSERT OR IGNORE INTO upload_batch_items(
                    file_key, owner_id, file_name, media_type, category,
                    primary_channel_id, primary_message_id, backup_channel_id,
                    backup_message_id, source_fingerprint, file_size,
                    backup_error, created_at, album_group_id, album_position,
                    upload_dedupe_key
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                key, int(owner_id), str(file_name), str(media_type), str(category),
                int(primary_channel_id), int(primary_id), int(backup_channel_id),
                int(backup_id or 0), source_fingerprint, max(0, int(file_size or 0)),
                str(backup_error or "")[:500], iso_now(),
                str(album_group_id) if album_group_id else None,
                max(0, int(album_position or 0)),
                str(source_fingerprint) if str(source_fingerprint or "").startswith("upload:") else None,
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
                        source_fingerprint, file_size, album_group_id, album_position,
                        media_kind
                    ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    item["file_key"], item["owner_id"], item["file_name"],
                    item["media_type"], item["category"], item["primary_message_id"],
                    item["backup_message_id"], item["created_at"],
                    item["source_fingerprint"], item["file_size"],
                    item["album_group_id"], item["album_position"],
                    media_kind_of(item["media_type"], item["file_name"]),
                ))
                if cur.rowcount == 1:
                    published += 1
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
            # Refresh the category summary once for the whole published batch,
            # not once per file. This keeps large uploads from repeatedly scanning
            # the same category rows inside one transaction.
            if published:
                await self._refresh_category_stats(db, category)
                self._invalidate_catalog_stats()
            await db.execute("DELETE FROM upload_batch_items WHERE owner_id=?", (int(owner_id),))
            await db.execute("DELETE FROM upload_batches WHERE owner_id=?", (int(owner_id),))
            await db.commit()
            return {"published": published, "failed_backup": failed_backup, "category": category}

    async def add_file(self, key, owner_id, file_name, media_type,
                       primary_id, backup_id, category="未分类",
                       source_fingerprint=None, file_size=0, created_at=None,
                        album_group_id=None, album_position=0,
                       refresh_stats=True):
        async with self.connect() as db:
            cur = await db.execute("""
                INSERT OR IGNORE INTO files(
                    file_key, owner_id, file_name, media_type, category,
                    primary_message_id, backup_message_id, created_at,
                    source_fingerprint, file_size, album_group_id, album_position,
                    media_kind
                )
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (key, owner_id, file_name, media_type, category,
                  primary_id, backup_id, created_at or iso_now(),
                  source_fingerprint, max(0, int(file_size or 0)),
                  str(album_group_id) if album_group_id else None,
                  max(0, int(album_position or 0)),
                  media_kind_of(media_type, file_name)))
            if cur.rowcount == 1:
                if refresh_stats:
                    await self._refresh_category_stats(db, category)
                else:
                    category_name = str(category or "").strip()
                    if category_name:
                        await db.execute(
                            "INSERT INTO categories(name, created_at, description, updated_at, share_key) "
                            "VALUES(?, ?, '', ?, ?) ON CONFLICT(name) DO NOTHING",
                            (category_name, iso_now(), iso_now(),
                             secrets.token_urlsafe(6).replace("-", "").replace("_", "")[:10]),
                        )
                self._invalidate_catalog_stats()
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


    async def reset_catalog_for_rescan(self):
        """Clear resource/catalog indexes only; preserve users, memberships, admins and settings."""
        async with self.connect() as db:
            # These tables contain only resource indexes, delivery state, or per-file history.
            # User identities, membership grants, bans, admins and bot settings are untouched.
            for table in (
                "backup_retry_jobs",
                "user_history",
                "favorites",
                "category_favorites",
                "category_delivery_progress",
                "files",
                "categories",
                "scanner_messages",
                "scanner_progress",
            ):
                await db.execute(f"DELETE FROM {table}")
            self._invalidate_catalog_stats()
            await db.commit()

    async def save_scanned_message(
        self, channel_id, other_channel_id, primary_channel_id,
        message_id, fingerprint, file_name, media_type, category,
        created_at, owner_id, file_key, album_group_id=None, album_position=0
    ):
        async with self.connect() as db:
            if album_group_id and not category:
                existing_album_category = await (await db.execute("""
                    SELECT category FROM files
                    WHERE album_group_id=? AND category IS NOT NULL AND category<>''
                    ORDER BY album_position LIMIT 1
                """, (str(album_group_id),))).fetchone()
                if existing_album_category:
                    category = str(existing_album_category["category"] or "")
            inserted_message = await db.execute("""
                INSERT OR IGNORE INTO scanner_messages(
                    channel_id, message_id, fingerprint, file_name,
                    media_type, category, created_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?)
            """, (channel_id, message_id, fingerprint, file_name,
                  media_type, category, created_at))
            await db.execute("""
                INSERT INTO scanner_progress(
                    channel_id, last_message_id, scanned_count, completed, updated_at
                ) VALUES(?, ?, ?, 0, ?)
                ON CONFLICT(channel_id) DO UPDATE SET
                    last_message_id=excluded.last_message_id,
                    scanned_count=scanner_progress.scanned_count + excluded.scanned_count,
                    completed=0,
                    updated_at=excluded.updated_at
            """, (
                channel_id, message_id,
                1 if inserted_message.rowcount == 1 else 0,
                iso_now(),
            ))

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
            # Uploads copy the media into this channel before writing their row,
            # so a history scan would index the bot's own copy a second time
            # under a "canku:" fingerprint. Skip any message the upload path has
            # already stored (its fingerprint never starts with "cangku:").
            scanned_column = (
                "primary_message_id" if channel_id == primary_channel_id
                else "backup_message_id"
            )
            already_indexed = await (await db.execute(
                f"SELECT file_key FROM files WHERE {scanned_column}=? "
                "AND (source_fingerprint IS NULL "
                "OR substr(source_fingerprint, 1, 7) <> 'cangku:') LIMIT 1",
                (int(message_id),),
            )).fetchone()
            if primary and not already_indexed:
                primary_id = primary["message_id"]
                backup_id = backup["message_id"] if backup else 0
                cur = await db.execute("""
                    INSERT OR IGNORE INTO files(
                        file_key, owner_id, file_name, media_type, category,
                        primary_message_id, backup_message_id, created_at,
                        source_fingerprint, album_group_id, album_position,
                        media_kind
                    ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    file_key, owner_id, primary["file_name"],
                    primary["media_type"], primary["category"],
                    primary_id, backup_id, primary["created_at"], fingerprint,
                    str(album_group_id) if album_group_id else None,
                    max(0, int(album_position or 0)),
                    media_kind_of(primary["media_type"], primary["file_name"]),
                ))
                inserted = cur.rowcount == 1
                if backup:
                    await db.execute("""
                        UPDATE files SET backup_message_id=?
                        WHERE source_fingerprint=? AND primary_message_id=?
                          AND backup_message_id=0
                    """, (backup_id, fingerprint, primary_id))
            if inserted and primary:
                category_name = str(primary["category"] or "").strip()
                if category_name:
                    await db.execute(
                        "INSERT INTO categories(name, created_at, description, updated_at, share_key) "
                        "VALUES(?, ?, '', ?, ?) ON CONFLICT(name) DO NOTHING",
                        (category_name, iso_now(), iso_now(),
                         secrets.token_urlsafe(6).replace("-", "").replace("_", "")[:10]),
                    )
                self._invalidate_catalog_stats()
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
                        source_fingerprint, media_kind
                    ) VALUES(?, ?, ?, ?, ?, ?, 0, ?, ?, ?)
                """, (
                    key, owner_id, row["file_name"], row["media_type"],
                    row["category"], row["message_id"], row["created_at"],
                    row["fingerprint"],
                    media_kind_of(row["media_type"], row["file_name"]),
                ))
                if cur.rowcount == 1:
                    created += 1
                    changed_categories.add(row["category"])
            for category in changed_categories:
                await self._refresh_category_stats(db, category)
            if created:
                self._invalidate_catalog_stats()
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

    async def reset_scan_progress_for_full_rescan(self, channel_id):
        """Restart a full source-channel scan without deleting any indexed data."""
        async with self.connect() as db:
            await db.execute("""
                INSERT INTO scanner_progress(
                    channel_id, last_message_id, scanned_count, completed, updated_at
                ) VALUES(
                    ?, 0,
                    (SELECT COUNT(*) FROM scanner_messages WHERE channel_id=?),
                    0, ?
                )
                ON CONFLICT(channel_id) DO UPDATE SET
                    last_message_id=0,
                    scanned_count=(
                        SELECT COUNT(*) FROM scanner_messages
                        WHERE channel_id=excluded.channel_id
                    ),
                    completed=0,
                    updated_at=excluded.updated_at
            """, (int(channel_id), int(channel_id), iso_now()))
            await db.commit()

    async def mark_scan_completed(self, channel_id):
        async with self.connect() as db:
            await db.execute("""
                INSERT INTO scanner_progress(
                    channel_id, last_message_id, scanned_count, completed, updated_at
                ) VALUES(?, 0, 0, 1, ?)
                ON CONFLICT(channel_id) DO UPDATE SET
                    completed=1, updated_at=excluded.updated_at
            """, (channel_id, iso_now()))
            # Aggregate each affected folder once after the history scan instead
            # of rescanning its files after every single Telegram message.
            changed_categories = await (await db.execute("""
                SELECT DISTINCT f.category
                FROM files f
                JOIN scanner_messages s ON s.fingerprint=f.source_fingerprint
                WHERE s.channel_id=? AND f.category IS NOT NULL AND f.category<>''
            """, (int(channel_id),))).fetchall()
            for item in changed_categories:
                await self._refresh_category_stats(db, str(item["category"]))
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

    async def record_delivery_failure(self, user_id, file_keys, share_key, error=""):
        """Count failed retrieval attempts for one delivery unit and return the new peak.

        A unit that keeps failing would otherwise pin the folder cursor forever,
        because progress only advances over successful sends.
        """
        keys = [str(key) for key in file_keys if key]
        if not keys:
            return 0
        now = iso_now()
        async with self.connect() as db:
            for key in keys:
                await db.execute("""
                    INSERT INTO delivery_failures(
                        user_id, file_key, share_key, attempts, last_error, updated_at
                    ) VALUES(?, ?, ?, 1, ?, ?)
                    ON CONFLICT(user_id, file_key, share_key) DO UPDATE SET
                        attempts=delivery_failures.attempts+1,
                        last_error=excluded.last_error,
                        updated_at=excluded.updated_at
                """, (int(user_id), key, str(share_key), str(error or "")[:300], now))
            placeholders = ",".join("?" for _ in keys)
            row = await (await db.execute(
                "SELECT MAX(attempts) AS peak FROM delivery_failures "
                f"WHERE user_id=? AND share_key=? AND file_key IN ({placeholders})",
                [int(user_id), str(share_key)] + keys,
            )).fetchone()
            await db.commit()
            return int(row["peak"] or 0) if row else 0

    async def clear_delivery_failures(self, user_id, file_keys, share_key):
        keys = [str(key) for key in file_keys if key]
        if not keys:
            return
        placeholders = ",".join("?" for _ in keys)
        async with self.connect() as db:
            await db.execute(
                "DELETE FROM delivery_failures "
                f"WHERE user_id=? AND share_key=? AND file_key IN ({placeholders})",
                [int(user_id), str(share_key)] + keys,
            )
            await db.commit()

    async def list_delivery_failures(self, limit=50):
        async with self.connect() as db:
            return await (await db.execute("""
                SELECT user_id, file_key, share_key, attempts, last_error, updated_at
                FROM delivery_failures ORDER BY attempts DESC, updated_at DESC LIMIT ?
            """, (int(limit),))).fetchall()

    async def add_pending_deletions(self, chat_id, message_ids, delete_at):
        """Persist scheduled deletions so a restart does not silently keep files forever."""
        ids = [int(value) for value in message_ids if value]
        if not ids:
            return
        async with self.connect() as db:
            for message_id in ids:
                await db.execute("""
                    INSERT INTO pending_deletions(chat_id, message_id, delete_at)
                    VALUES(?, ?, ?) ON CONFLICT(chat_id, message_id) DO NOTHING
                """, (int(chat_id), message_id, str(delete_at)))
            await db.commit()

    async def list_pending_deletions(self, limit=1000):
        async with self.connect() as db:
            return await (await db.execute("""
                SELECT chat_id, message_id, delete_at FROM pending_deletions
                ORDER BY delete_at LIMIT ?
            """, (int(limit),))).fetchall()

    async def remove_pending_deletions(self, chat_id, message_ids):
        ids = [int(value) for value in message_ids if value]
        if not ids:
            return
        placeholders = ",".join("?" for _ in ids)
        async with self.connect() as db:
            await db.execute(
                f"DELETE FROM pending_deletions WHERE chat_id=? AND message_id IN ({placeholders})",
                [int(chat_id)] + ids,
            )
            await db.commit()

    async def repair_category_delivery_progress_from_history(
        self, user_id: int, share_key: str, category: str
    ) -> int:
        """One-time repair for cursors advanced by older releases after failed copies."""
        marker = f"member_delivery_progress_repaired_v1:{int(user_id)}:{share_key}"
        async with self.connect() as db:
            marker_row = await (await db.execute(
                "SELECT value FROM settings WHERE key=?", (marker,)
            )).fetchone()
            if marker_row:
                current = await (await db.execute(
                    "SELECT next_offset FROM category_delivery_progress "
                    "WHERE user_id=? AND share_key=?",
                    (int(user_id), str(share_key)),
                )).fetchone()
                return int(current["next_offset"] or 0) if current else 0

            row = await (await db.execute("""
                WITH ordered AS (
                    SELECT f.file_key,
                           ROW_NUMBER() OVER (
                               ORDER BY
                                   COALESCE(
                                       (SELECT MAX(g.created_at) FROM files g
                                        WHERE g.album_group_id=f.album_group_id),
                                       f.created_at
                                   ) DESC,
                                   CASE WHEN f.album_group_id IS NOT NULL
                                        THEN f.album_position ELSE 0 END,
                                   f.file_key
                           ) - 1 AS position
                    FROM files f
                    WHERE f.category=?
                ),
                first_missing AS (
                    SELECT MIN(o.position) AS first_missing
                    FROM ordered o
                    LEFT JOIN user_history h
                      ON h.user_id=? AND h.file_key=o.file_key
                    WHERE h.file_key IS NULL
                )
                SELECT COALESCE(
                    (SELECT first_missing FROM first_missing),
                    (SELECT COUNT(*) FROM ordered),
                    0
                ) AS next_offset
            """, (str(category), int(user_id)))).fetchone()
            repaired = max(0, int(row["next_offset"] or 0))
            await db.execute("""
                INSERT INTO category_delivery_progress(user_id, share_key, next_offset, updated_at)
                VALUES(?, ?, ?, ?)
                ON CONFLICT(user_id, share_key) DO UPDATE SET
                    next_offset=excluded.next_offset,
                    updated_at=excluded.updated_at
            """, (int(user_id), str(share_key), repaired, iso_now()))
            await db.execute(
                "INSERT INTO settings(key, value) VALUES(?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (marker, str(repaired)),
            )
            await db.commit()
            return repaired

    async def enqueue_backup_retry(
        self, file_key, primary_channel_id, primary_message_id,
        backup_channel_id, error=""
    ):
        now = iso_now()
        async with self.connect() as db:
            # A prior terminal failure applies to the whole known album, not
            # merely its representative file row. Keep that record and refuse
            # automatic re-enqueue so the worker cannot forward it forever.
            failed = await (await db.execute("""
                SELECT 1
                FROM backup_retry_failures failure
                LEFT JOIN files failed_file ON failed_file.file_key=failure.file_key
                JOIN files current_file ON current_file.file_key=?
                WHERE failure.file_key=?
                   OR (
                       current_file.album_group_id IS NOT NULL
                       AND failed_file.album_group_id=current_file.album_group_id
                   )
                LIMIT 1
            """, (str(file_key), str(file_key)))).fetchone()
            if failed:
                return False
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
            return True

    async def restart_backup_retry_queue(self, backup_channel_id, full_restart=False):
        """Rebuild the complete Cangku copy queue in source-message order."""
        now = iso_now()
        queued_failures = 0
        reset_count = 0
        async with self.connect() as db:
            if full_restart:
                # Reset only source-linked Cangku mappings. Keep the original
                # files, scanner index, user data, and non-Cangku retry jobs.
                result = await db.execute("""
                    UPDATE files SET backup_message_id=0
                    WHERE source_fingerprint LIKE 'cangku:%'
                """)
                reset_count = max(0, int(getattr(result, "rowcount", 0) or 0))

                # Remove only the old Cangku jobs; unrelated upload/retry work
                # must not be discarded by a full source-channel restart.
                await db.execute("""
                    DELETE FROM backup_retry_jobs
                    WHERE file_key IN (
                        SELECT file_key FROM files
                        WHERE source_fingerprint LIKE 'cangku:%'
                    )
                """)

                # Queue EVERY indexed source message, not just rows whose album
                # metadata already has a recognized prefix. The worker verifies
                # unknown/legacy album metadata against Telegram before copying.
                # Album members may each have a job; the first source message
                # forwards the complete album and later member jobs safely see
                # their newly saved backup IDs and finish without copying again.
                source_rows = await (await db.execute("""
                    SELECT source.file_key, source.source_fingerprint
                    FROM files source
                    WHERE source.source_fingerprint LIKE 'cangku:%'
                      AND NOT EXISTS (
                          SELECT 1
                          FROM backup_retry_failures failure
                          LEFT JOIN files failed_file
                            ON failed_file.file_key=failure.file_key
                          WHERE failure.file_key=source.file_key
                             OR (
                                 source.album_group_id IS NOT NULL
                                 AND failed_file.album_group_id=source.album_group_id
                             )
                      )
                    ORDER BY source.created_at, source.file_key
                """)).fetchall()

                now_jobs = []
                for row in source_rows:
                    fingerprint = str(row["source_fingerprint"] or "")
                    parts = fingerprint.split(":", 2)
                    if len(parts) != 3 or parts[0] != "cangku":
                        continue
                    try:
                        source_chat_id = int(parts[1])
                        source_message_id = int(parts[2])
                    except (TypeError, ValueError):
                        continue
                    now_jobs.append((
                        str(row["file_key"]), source_chat_id, source_message_id,
                        int(backup_channel_id), now,
                        "Full restart: verified source message",
                        now, now,
                    ))

                # The copy queue is always ordered by original Telegram message
                # ID, independent of scan order, database insertion time, or
                # the position where a previous backup run stopped.
                now_jobs.sort(key=lambda job: (
                    int(job[1]), int(job[2]), str(job[0])
                ))
                for job in now_jobs:
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
                            attempts=0, next_retry_at=excluded.next_retry_at,
                            last_error=excluded.last_error,
                            updated_at=excluded.updated_at
                    """, job)

                # Do not rewrite unrelated jobs' destination or retry state.
            else:
                # A normal resume makes all pending work immediately eligible.
                await db.execute("""
                    UPDATE backup_retry_jobs
                    SET backup_channel_id=?, attempts=0, next_retry_at=?,
                        last_error='', updated_at=?
                """, (int(backup_channel_id), now, now))

            # Archived failures are deliberately not re-queued by normal resume
            # or full restart. A failed item remains skipped until an explicit
            # administrative retry action is added; this prevents an endless
            # fail -> delete job -> discover candidate -> forward loop.
            await db.commit()
        return reset_count if full_restart else queued_failures

    async def record_backup_failure(
        self, file_key, file_name, category, source_fingerprint,
        album_group_id, source_message_ids, error=""
    ):
        now = iso_now()
        ids_text = ",".join(str(int(value)) for value in (source_message_ids or []))
        async with self.connect() as db:
            result = await db.execute("""
                INSERT INTO backup_retry_failures(
                    file_key, file_name, category, source_fingerprint,
                    album_group_id, source_message_ids, last_error, failed_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(file_key) DO NOTHING
            """, (
                str(file_key), str(file_name or "")[:300], str(category or "")[:120],
                str(source_fingerprint or "")[:300], str(album_group_id or "")[:300],
                ids_text[:500], str(error or "")[:500], now,
            ))
            # A Telegram album is one backup unit even though every member has
            # its own file row and retry job. Once its representative failure is
            # recorded, remove every queued member atomically so the worker can
            # neither retry nor notify about the same album again.
            if str(album_group_id or "").startswith("cangku:"):
                await db.execute("""
                    DELETE FROM backup_retry_jobs
                    WHERE file_key IN (
                        SELECT file_key FROM files WHERE album_group_id=?
                    )
                """, (str(album_group_id),))
            await db.commit()
            return getattr(result, "rowcount", 0) == 1

    async def list_backup_failures(self, limit=20):
        async with self.connect() as db:
            return await (await db.execute("""
                SELECT file_key, file_name, category, source_message_ids,
                       last_error, failed_at
                FROM backup_retry_failures
                ORDER BY failed_at DESC
                LIMIT ?
            """, (max(1, min(100, int(limit))),))).fetchall()

    async def list_unbacked_cangku_files(self, limit=10):
        """Find accessible-source candidates for idle backup; never split Telegram albums."""
        async with self.connect() as db:
            return await (await db.execute("""
                SELECT f.file_key, f.source_fingerprint
                FROM files f
                WHERE COALESCE(f.backup_message_id, 0)=0
                  AND f.album_group_id LIKE 'cangku-single:%'
                  AND f.source_fingerprint LIKE 'cangku:%'
                  AND NOT EXISTS (
                      SELECT 1 FROM backup_retry_jobs j WHERE j.file_key=f.file_key
                  )
                  AND NOT EXISTS (
                      SELECT 1 FROM backup_retry_failures failed
                      WHERE failed.file_key=f.file_key
                  )
                ORDER BY f.created_at, f.file_key
                LIMIT ?
            """, (max(1, min(50, int(limit))),))).fetchall()

    async def update_cangku_album_metadata(
        self, source_fingerprint, album_group_id, album_position, category=None
    ):
        """Attach Telegram album metadata and propagate a discovered album caption category."""
        async with self.connect() as db:
            await db.execute("""
                UPDATE files
                SET album_group_id=?, album_position=?,
                    category=CASE
                        WHEN CAST(? AS TEXT) IS NOT NULL
                             AND CAST(? AS TEXT)<>'' THEN CAST(? AS TEXT)
                        ELSE category
                    END
                WHERE source_fingerprint=?
            """, (
                str(album_group_id), max(0, int(album_position)),
                category, category, category, str(source_fingerprint),
            ))
            if category:
                await db.execute("""
                    UPDATE files SET category=?
                    WHERE album_group_id=?
                """, (str(category), str(album_group_id)))
                await db.execute("""
                    UPDATE scanner_messages
                    SET category=?
                    WHERE fingerprint IN (
                        SELECT source_fingerprint FROM files WHERE album_group_id=?
                    )
                """, (str(category), str(album_group_id)))
            await db.commit()

    async def list_unbacked_cangku_albums(self, limit=5):
        """Return one representative row per complete, still-unbacked Cangku album."""
        async with self.connect() as db:
            return await (await db.execute("""
                SELECT f.file_key, f.source_fingerprint, f.album_group_id
                FROM files f
                WHERE f.album_group_id IS NOT NULL
                  AND f.album_position=(
                      SELECT MIN(first.album_position) FROM files first
                      WHERE first.album_group_id=f.album_group_id
                  )
                  AND f.album_group_id IN (
                      SELECT grouped.album_group_id
                      FROM files grouped
                      GROUP BY grouped.album_group_id
                      HAVING grouped.album_group_id LIKE 'cangku:%'
                         AND COUNT(*) BETWEEN 2 AND 10
                         AND SUM(CASE WHEN grouped.source_fingerprint LIKE 'cangku:%'
                                      THEN 1 ELSE 0 END)=COUNT(*)
                         AND SUM(CASE WHEN COALESCE(grouped.backup_message_id, 0)=0
                                      THEN 1 ELSE 0 END)>0
                         AND NOT EXISTS (
                             SELECT 1 FROM backup_retry_jobs jobs
                             JOIN files queued ON queued.file_key=jobs.file_key
                             WHERE queued.album_group_id=grouped.album_group_id
                         )
                         AND NOT EXISTS (
                             SELECT 1 FROM backup_retry_failures failure
                             JOIN files failed_file ON failed_file.file_key=failure.file_key
                             WHERE failed_file.album_group_id=grouped.album_group_id
                         )
                  )
                ORDER BY f.created_at, f.file_key
                LIMIT ?
            """, (max(1, min(20, int(limit))),))).fetchall()

    async def get_cangku_album_backup_group(self, album_group_id):
        """Load all source pointers for one album in original Telegram message order."""
        async with self.connect() as db:
            return await (await db.execute("""
                SELECT file_key, source_fingerprint, backup_message_id,
                       album_group_id, album_position
                FROM files
                WHERE album_group_id=?
                ORDER BY album_position, file_key
            """, (str(album_group_id),))).fetchall()

    async def update_album_backup_message_ids(
        self, album_group_id, copied_message_ids, source_message_ids=None,
        replace_existing=False,
    ):
        """Atomically fill backup IDs for all album rows or only explicitly mapped missing members."""
        ids = [int(value) for value in (copied_message_ids or [])]
        source_ids = (
            [int(value) for value in source_message_ids]
            if source_message_ids is not None else None
        )
        async with self.connect() as db:
            rows = await (await db.execute("""
                SELECT file_key, source_fingerprint, backup_message_id
                FROM files WHERE album_group_id=?
                ORDER BY album_position, file_key
            """, (str(album_group_id),))).fetchall()
            if not 2 <= len(rows) <= 10:
                raise RuntimeError("Album backup result does not contain a complete source group")
            if source_ids is None:
                if len(rows) != len(ids):
                    raise RuntimeError("Album backup result count does not match the complete source group")
                targets = rows
            else:
                if len(source_ids) != len(ids) or len(set(source_ids)) != len(source_ids):
                    raise RuntimeError("Partial album backup mapping is invalid")
                by_source = {}
                for row in rows:
                    parts = str(row["source_fingerprint"] or "").split(":", 2)
                    if len(parts) == 3 and parts[0] == "cangku":
                        by_source[int(parts[2])] = row
                targets = []
                for source_id in source_ids:
                    row = by_source.get(source_id)
                    if not row:
                        raise RuntimeError("Partial album backup references a source message outside the album")
                    if int(row["backup_message_id"] or 0) > 0:
                        raise RuntimeError("Refusing to overwrite an existing album backup message ID")
                    targets.append(row)
            if len(targets) != len(ids):
                raise RuntimeError("Album backup result count does not match target members")
            for row, copied_id in zip(targets, ids):
                if replace_existing:
                    result = await db.execute("""
                        UPDATE files SET backup_message_id=?
                        WHERE file_key=?
                    """, (copied_id, row["file_key"]))
                else:
                    result = await db.execute("""
                        UPDATE files SET backup_message_id=?
                        WHERE file_key=? AND COALESCE(backup_message_id, 0)=0
                    """, (copied_id, row["file_key"]))
                if getattr(result, "rowcount", 0) != 1:
                    raise RuntimeError("Could not atomically save every album backup message ID")
            await db.commit()
            return True

    async def backup_retry_stats(self):
        """Return durable backup queue counters without modifying any records."""
        async with self.connect() as db:
            queued = await (await db.execute(
                "SELECT COUNT(*) AS n FROM backup_retry_jobs"
            )).fetchone()
            failed = await (await db.execute(
                "SELECT COUNT(*) AS n FROM backup_retry_jobs WHERE attempts>0"
            )).fetchone()
            candidates = await (await db.execute("""
                SELECT COUNT(*) AS n FROM files f
                WHERE COALESCE(f.backup_message_id, 0)=0
                  AND f.album_group_id LIKE 'cangku-single:%'
                  AND f.source_fingerprint LIKE 'cangku:%'
                  AND NOT EXISTS (
                      SELECT 1 FROM backup_retry_jobs j WHERE j.file_key=f.file_key
                  )
            """)).fetchone()
            album_candidates = await (await db.execute("""
                SELECT COUNT(*) AS n FROM (
                    SELECT grouped.album_group_id
                    FROM files grouped
                    GROUP BY grouped.album_group_id
                    HAVING grouped.album_group_id LIKE 'cangku:%'
                       AND COUNT(*) BETWEEN 2 AND 10
                       AND SUM(CASE WHEN grouped.source_fingerprint LIKE 'cangku:%'
                                    THEN 1 ELSE 0 END)=COUNT(*)
                       AND SUM(CASE WHEN COALESCE(grouped.backup_message_id, 0)=0
                                    THEN 1 ELSE 0 END)=COUNT(*)
                       AND NOT EXISTS (
                           SELECT 1 FROM backup_retry_jobs jobs
                           JOIN files queued ON queued.file_key=jobs.file_key
                           WHERE queued.album_group_id=grouped.album_group_id
                       )
                ) pending_albums
            """)).fetchone()
            backed = await (await db.execute("""
                SELECT COUNT(*) AS n FROM files
                WHERE COALESCE(backup_message_id, 0)>0
                  AND source_fingerprint LIKE 'cangku:%'
            """)).fetchone()
            return {
                "queued": int(queued["n"] or 0),
                "failed": int(failed["n"] or 0),
                "candidates": int(candidates["n"] or 0),
                "album_candidates": int(album_candidates["n"] or 0),
                "backed": int(backed["n"] or 0),
            }

    async def list_due_backup_retries(self, limit=10):
        async with self.connect() as db:
            return await (await db.execute("""
                SELECT * FROM backup_retry_jobs
                WHERE next_retry_at<=?
                -- Full restarts must drain the oldest source message first.
                -- Message ID is the primary ordering key; channel is only a tie-breaker.
                ORDER BY primary_message_id ASC, primary_channel_id ASC,
                         next_retry_at ASC, created_at ASC, file_key ASC
                LIMIT ?
            """, (iso_now(), max(1, min(50, int(limit)))))).fetchall()

    async def claim_backup_retry(self, file_key, lease_seconds=300):
        """Atomically lease one due job so concurrent workers cannot copy it together."""
        now = utc_now()
        lease_until = (now + timedelta(seconds=max(60, int(lease_seconds)))).isoformat()
        async with self.connect() as db:
            # Older deployments may already have queued a failure before the
            # no-requeue guard was installed. Check again at claim time so a
            # stale job can never trigger one more Telegram forward.
            current = await (await db.execute(
                "SELECT album_group_id FROM files WHERE file_key=?",
                (str(file_key),),
            )).fetchone()
            album_group_id = str(current["album_group_id"] or "") if current else ""
            failed = await (await db.execute("""
                SELECT 1
                FROM backup_retry_failures failure
                LEFT JOIN files failed_file ON failed_file.file_key=failure.file_key
                WHERE failure.file_key=?
                   OR (
                       ?<>'' AND failed_file.album_group_id=?
                   )
                LIMIT 1
            """, (str(file_key), album_group_id, album_group_id))).fetchone()
            if failed:
                if album_group_id:
                    await db.execute("""
                        DELETE FROM backup_retry_jobs
                        WHERE file_key IN (
                            SELECT file_key FROM files WHERE album_group_id=?
                        )
                    """, (album_group_id,))
                else:
                    await db.execute(
                        "DELETE FROM backup_retry_jobs WHERE file_key=?",
                        (str(file_key),),
                    )
                await db.commit()
                return None

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

    async def defer_backup_retry(self, file_key, error="", delay_seconds=120):
        """Release a leased job for later without counting a shared prerequisite outage as a file failure."""
        next_retry = (utc_now() + timedelta(seconds=max(30, int(delay_seconds)))).isoformat()
        async with self.connect() as db:
            await db.execute("""
                UPDATE backup_retry_jobs
                SET next_retry_at=?, last_error=?, updated_at=?
                WHERE file_key=?
            """, (next_retry, str(error or "")[:500], iso_now(), str(file_key)))
            await db.commit()

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

    @staticmethod
    def _catalog_filter(query="", category=""):
        """Build the shared WHERE clause so page and count queries stay identical."""
        conditions = []
        params = []
        if query:
            conditions.append("(f.file_name LIKE ? OR f.category LIKE ?)")
            params.extend((f"%{query}%", f"%{query}%"))
        if category:
            conditions.append("f.category=?")
            params.append(category)
        where = " WHERE " + " AND ".join(conditions) if conditions else ""
        return where, params

    async def catalog_total(self, query="", category=""):
        """Count matching files without building a page of rows."""
        where, params = self._catalog_filter(query, category)
        async with self.connect() as db:
            row = await (await db.execute(
                "SELECT COUNT(*) AS n FROM files f" + where, params
            )).fetchone()
            return int(row["n"] or 0)

    async def public_catalog(self, query="", category="", limit=30, offset=0):
        where, params = self._catalog_filter(query, category)
        album_join = ALBUM_CATEGORY_AGGREGATE_JOIN if category else ALBUM_AGGREGATE_JOIN
        # The join precedes the WHERE clause, so its category parameter comes first.
        row_params = ([category] if category else []) + list(params) + [limit, offset]
        async with self.connect() as db:
            # Saved folder rows are authoritative and already carry their counts;
            # grouping the whole files table on every page load was the slow path.
            categories = await (await db.execute(
                "SELECT name AS category FROM categories ORDER BY lower(name)"
            )).fetchall()
            total = (await (await db.execute(
                "SELECT COUNT(*) AS n FROM files f" + where, params
            )).fetchone())["n"]
            rows = await (await db.execute(
                "SELECT f.file_key, f.file_name, f.media_type, f.category, f.created_at, "
                "f.album_group_id, f.album_position, "
                "COALESCE(album_agg.album_size, 1) AS album_size "
                "FROM files f" + album_join + where
                + " ORDER BY COALESCE(album_agg.album_latest, f.created_at) DESC, "
                "CASE WHEN f.album_group_id IS NOT NULL THEN f.album_position ELSE 0 END "
                "LIMIT ? OFFSET ?",
                row_params,
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
                await self._refresh_category_stats(db, category, force=True)
                if current and current["category"] != category:
                    await self._refresh_category_stats(db, current["category"], create=False, force=True)
                self._invalidate_catalog_stats()
            await db.commit()
            return cur.rowcount > 0

    async def assign_backup_album_category(self, backup_chat_id, backup_message_ids, category):
        """Assign a hashtag folder only to the complete album from this exact channel."""
        ids = list(dict.fromkeys(int(value) for value in (backup_message_ids or [])))
        category = str(category or "").strip()[:120]
        if not category or not 2 <= len(ids) <= 10:
            return 0
        fingerprints = [f"backup:{int(backup_chat_id)}:{message_id}" for message_id in ids]
        placeholders = ",".join("?" for _ in fingerprints)
        async with self.connect() as db:
            rows = await (await db.execute(
                f"SELECT file_key, category, source_fingerprint FROM files "
                f"WHERE backup_message_id IN ({','.join('?' for _ in ids)})",
                ids,
            )).fetchall()
            # Do not partially classify a group: every message in this album
            # must already have a matching index row from this same channel.
            if len(rows) != len(ids):
                return 0
            file_keys = [str(row["file_key"]) for row in rows]
            key_placeholders = ",".join("?" for _ in file_keys)
            old_categories = {
                str(row["category"] or "").strip()
                for row in rows
                if str(row["category"] or "").strip()
                and str(row["category"] or "").strip() != category
            }
            await db.execute(
                f"UPDATE files SET category=? WHERE file_key IN ({key_placeholders})",
                [category] + file_keys,
            )
            # The existing catalog rows are authoritative; no scanner-message
            # rows or new file records are created during this directory pass.
            for old_category in old_categories:
                await self._refresh_category_stats(db, old_category, create=False, force=True)
            await self._refresh_category_stats(db, category, force=True)
            await db.commit()
            return len(file_keys)

    async def toggle_notification_subscription(self, user_id):
        async with self.connect() as db:
            row = await (await db.execute(
                "SELECT 1 FROM notification_subscriptions WHERE user_id=?",
                (int(user_id),),
            )).fetchone()
            if row:
                await db.execute("DELETE FROM notification_subscriptions WHERE user_id=?", (int(user_id),))
                enabled = False
            else:
                await db.execute(
                    "INSERT INTO notification_subscriptions(user_id, created_at) VALUES(?, ?) ON CONFLICT(user_id) DO NOTHING",
                    (int(user_id), iso_now()),
                )
                enabled = True
            await db.commit()
            return enabled

    async def list_notification_subscribers(self):
        async with self.connect() as db:
            rows = await (await db.execute(
                "SELECT user_id FROM notification_subscriptions ORDER BY created_at"
            )).fetchall()
            return [int(row["user_id"]) for row in rows]

    async def list_user_ids(self):
        async with self.connect() as db:
            rows = await (await db.execute(
                "SELECT user_id FROM users ORDER BY user_id"
            )).fetchall()
            return [row["user_id"] for row in rows]

    async def remove_users_from_broadcast_list(self, user_ids):
        """Remove failed broadcast recipients from users only; preserve all other records."""
        values = list(dict.fromkeys(int(user_id) for user_id in (user_ids or [])))
        if not values:
            return 0
        removed = 0
        async with self.connect() as db:
            for start in range(0, len(values), 500):
                batch = values[start:start + 500]
                placeholders = ",".join("?" for _ in batch)
                result = await db.execute(
                    f"DELETE FROM users WHERE user_id IN ({placeholders})",
                    batch,
                )
                removed += max(0, int(getattr(result, "rowcount", 0) or 0))
            await db.commit()
        return removed

    async def list_manual_members(self, limit=30):
        async with self.connect() as db:
            return await (await db.execute("""
                SELECT user_id, expires_at FROM manual_members
                ORDER BY granted_at DESC LIMIT ?
            """, (limit,))).fetchall()

    async def _backfill_media_kind(self):
        """Classify existing rows once, then keep the column correct on every insert."""
        async with self.connect() as db:
            done = await (await db.execute(
                "SELECT value FROM settings WHERE key='media_kind_backfill_v1'"
            )).fetchone()
            if done:
                return
            page_size = 5000
            offset = 0
            updated = 0
            while True:
                rows = await (await db.execute(
                    "SELECT file_key, media_type, file_name FROM files "
                    "ORDER BY file_key LIMIT ? OFFSET ?",
                    (page_size, offset),
                )).fetchall()
                if not rows:
                    break
                payload = [
                    (media_kind_of(row["media_type"], row["file_name"]), row["file_key"])
                    for row in rows
                ]
                await db.executemany(
                    "UPDATE files SET media_kind=? WHERE file_key=?", payload
                )
                updated += len(payload)
                offset += len(rows)
            await db.execute(
                "INSERT OR IGNORE INTO settings(key, value) "
                "VALUES('media_kind_backfill_v1', 'completed')"
            )
            await db.commit()
            if updated:
                log.info("Backfilled media_kind for %s stored files", updated)

        if updated:
            # Counts stored on categories predate the classifier; recompute them
            # once so the panel and filters agree with the new column.
            async with self.connect() as db:
                names = await (await db.execute("SELECT name FROM categories")).fetchall()
                for item in names:
                    await self._refresh_category_stats(
                        db, str(item["name"]), create=False, force=True
                    )
                self._invalidate_catalog_stats()
                await db.commit()

    async def stats(self):
        # One round trip instead of four: the four counts are independent, and
        # the admin panel asks for them together every time it renders.
        async with self.connect() as db:
            row = await (await db.execute("""
                SELECT (SELECT COUNT(*) FROM users) AS users,
                       (SELECT COUNT(*) FROM files) AS files,
                       (SELECT COUNT(*) FROM manual_members
                        WHERE expires_at IS NULL OR expires_at > ?) AS members,
                       (SELECT COUNT(*) FROM banned_users) AS banned
            """, (iso_now(),))).fetchone()
            return (
                int(row["users"] or 0), int(row["files"] or 0),
                int(row["members"] or 0), int(row["banned"] or 0),
            )

    def _invalidate_catalog_stats(self):
        """Drop cached totals; called from every mutation that changes file rows."""
        self._catalog_stats_cache = None
        self._catalog_stats_cache_at = 0.0

    async def catalog_stats(self):
        # The aggregation classifies every file by extension, so it is only
        # recomputed when the cache is cold or older than the TTL. Callers only
        # show these numbers as display totals.
        cached = self._catalog_stats_cache
        if cached is not None and (time.monotonic() - self._catalog_stats_cache_at) < 30:
            return dict(cached)
        async with self.connect() as db:
            row = await (await db.execute("""
                SELECT COUNT(*) AS messages,
                       (SELECT COUNT(*) FROM (
                           SELECT category FROM files
                           UNION
                           SELECT name AS category FROM categories
                       )) AS folders,
                       SUM(CASE WHEN kind='image' THEN 1 ELSE 0 END) AS images,
                       SUM(CASE WHEN kind='video' THEN 1 ELSE 0 END) AS videos,
                       SUM(CASE WHEN kind='animation' THEN 1 ELSE 0 END) AS animations,
                       SUM(CASE WHEN kind='audio' THEN 1 ELSE 0 END) AS audios
                FROM (
                    SELECT CASE WHEN media_kind IS NOT NULL AND media_kind <> '' THEN media_kind
                                WHEN lower(media_type) IN ('photo', 'image') THEN 'image'
                                WHEN lower(media_type) IN ('video', 'video_note') THEN 'video'
                                WHEN lower(media_type) = 'animation' THEN 'animation'
                                WHEN lower(media_type) IN ('audio', 'voice') THEN 'audio'
                                ELSE 'document' END AS kind
                    FROM files
                ) AS classified
            """)).fetchone()
            result = {
                "folders": int(row["folders"] or 0),
                "messages": int(row["messages"] or 0),
                "images": int(row["images"] or 0),
                "videos": int(row["videos"] or 0),
                "animations": int(row["animations"] or 0),
                "audios": int(row["audios"] or 0),
            }
        self._catalog_stats_cache = dict(result)
        self._catalog_stats_cache_at = time.monotonic()
        return result

    async def _refresh_category_stats(self, db, category: str, create=True, force=False):
        """Recalculate and persist one category summary; never scans unrelated folders."""
        category = str(category or "").strip()
        if not category:
            return
        if not force:
            now = asyncio.get_running_loop().time()
            last = self._category_stats_last.get(category, 0.0)
            if last and now - last < CATEGORY_STATS_MIN_INTERVAL:
                # A recount inside this window already covers this write; make sure
                # the folder is re-counted once the window closes so it still settles.
                self._schedule_category_stats_refresh(category, create=create)
                return
            self._category_stats_last[category] = now

        if create:
            await db.execute(
                "INSERT INTO categories(name, created_at, description, updated_at, share_key) "
                "VALUES(?, ?, '', ?, ?) ON CONFLICT(name) DO NOTHING",
                (category, iso_now(), iso_now(),
                 secrets.token_urlsafe(6).replace("-", "").replace("_", "")[:10]),
            )
        # Counts and byte sizes in one statement. Classification comes from the
        # stored media_kind column, so this no longer evaluates ~40 LIKE
        # comparisons per row.
        row = await (await db.execute("""
            SELECT COUNT(*) AS file_count,
                   SUM(CASE WHEN kind='image' THEN 1 ELSE 0 END) AS image_count,
                   SUM(CASE WHEN kind='video' THEN 1 ELSE 0 END) AS video_count,
                   SUM(CASE WHEN kind='animation' THEN 1 ELSE 0 END) AS animation_count,
                   SUM(CASE WHEN kind='audio' THEN 1 ELSE 0 END) AS audio_count,
                   SUM(CASE WHEN kind='document' THEN 1 ELSE 0 END) AS document_count,
                   COALESCE(SUM(file_size), 0) AS total_size,
                   SUM(CASE WHEN COALESCE(file_size, 0)>0 THEN 1 ELSE 0 END) AS sized_count,
                   COALESCE(SUM(CASE WHEN kind='video' THEN COALESCE(file_size, 0) ELSE 0 END), 0) AS video_size,
                   COALESCE(SUM(CASE WHEN kind='image' THEN COALESCE(file_size, 0) ELSE 0 END), 0) AS image_size,
                   COALESCE(SUM(CASE WHEN kind NOT IN ('image', 'video')
                       THEN COALESCE(file_size, 0) ELSE 0 END), 0) AS other_size
            FROM (
                SELECT file_size,
                       CASE WHEN media_kind IS NOT NULL AND media_kind <> '' THEN media_kind
                            WHEN lower(media_type) IN ('photo', 'image') THEN 'image'
                            WHEN lower(media_type) IN ('video', 'video_note') THEN 'video'
                            WHEN lower(media_type) = 'animation' THEN 'animation'
                            WHEN lower(media_type) IN ('audio', 'voice') THEN 'audio'
                            ELSE 'document' END AS kind
                FROM files WHERE category=?
            ) AS classified
        """,
            (category,))).fetchone()
        await db.execute("""
            UPDATE categories SET
                file_count=?, image_count=?, video_count=?, animation_count=?,
                audio_count=?, document_count=?, total_size=?, sized_count=?,
                video_size=?, image_size=?, other_size=?, updated_at=?
            WHERE name=?
        """, (
            int(row["file_count"] or 0), int(row["image_count"] or 0),
            int(row["video_count"] or 0), int(row["animation_count"] or 0),
            int(row["audio_count"] or 0), int(row["document_count"] or 0),
            int(row["total_size"] or 0), int(row["sized_count"] or 0),
            int(row["video_size"] or 0), int(row["image_size"] or 0),
            int(row["other_size"] or 0), iso_now(), category,
        ))
        self._category_stats_last[category] = asyncio.get_running_loop().time()

    def _schedule_category_stats_refresh(self, category: str, create=True):
        """Queue one catch-up recount per folder, after the throttle window ends."""
        existing = self._category_stats_pending.get(category)
        if existing is not None and not existing.done():
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return

        async def run():
            try:
                window = self._category_stats_last.get(category, 0.0) + CATEGORY_STATS_MIN_INTERVAL
                delay = window - asyncio.get_running_loop().time()
                if delay > 0:
                    await asyncio.sleep(delay)
                async with self.connect() as db:
                    await self._refresh_category_stats(db, category, create=create, force=True)
                    await db.commit()
            except Exception:
                log.warning("deferred category recount failed for %r", category, exc_info=True)
            finally:
                self._category_stats_pending.pop(category, None)

        self._category_stats_pending[category] = loop.create_task(run())

    async def save_category(self, name: str, description="", created_at=None):
        name = str(name or "").strip()[:120]
        if not name:
            return
        now = iso_now()
        created = str(created_at or now)
        async with self.connect() as db:
            existing = await (await db.execute(
                "SELECT 1 FROM categories WHERE name=?", (name,)
            )).fetchone()
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
            return existing is None

    async def refresh_category_stats(self, categories):
        """Refresh summaries for a set of changed folders in one database transaction."""
        names = sorted({str(name or "").strip() for name in categories if str(name or "").strip()})
        if not names:
            return
        async with self.connect() as db:
            for name in names:
                await self._refresh_category_stats(db, name, force=True)
            await db.commit()

    async def get_category_storage_stats(self, category: str):
        """Read precomputed size totals; file aggregation runs only when a folder changes."""
        async with self.connect() as db:
            row = await (await db.execute("""
                SELECT COALESCE(video_size, 0) AS video_size,
                       COALESCE(image_size, 0) AS image_size,
                       COALESCE(other_size, 0) AS other_size,
                       COALESCE(sized_count, 0) AS sized_count,
                       COALESCE(file_count, 0) AS file_count
                FROM categories WHERE name=?
            """, (str(category),))).fetchone()
            if not row:
                return {
                    "video_size": 0, "image_size": 0, "other_size": 0,
                    "sized_count": 0, "file_count": 0,
                }
            return {key: int(row[key] or 0) for key in (
                "video_size", "image_size", "other_size", "sized_count", "file_count"
            )}

    async def get_category_by_index(self, index):
        """Load one folder summary without aggregating the whole file catalog."""
        index = max(0, int(index))
        async with self.connect() as db:
            row = await (await db.execute("""
                SELECT name AS category FROM categories
                ORDER BY created_at DESC
                LIMIT 1 OFFSET ?
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
                       (SELECT COUNT(DISTINCT f.album_group_id) FROM files f
                        WHERE f.category=c.name AND f.album_group_id IS NOT NULL) AS album_count,
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
                ORDER BY c.created_at DESC
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
            await self._refresh_category_stats(db, new_name, force=True)
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
                "SELECT name, description, created_at, updated_at, share_key, "
                "COALESCE(file_count, 0) AS file_count "
                "FROM categories WHERE share_key=?", (share_key,)
            )).fetchone()

    async def count_category_files(self, category):
        """Return the live number of stored file rows in one category."""
        async with self.connect() as db:
            row = await (await db.execute(
                "SELECT COUNT(*) AS file_count FROM files WHERE category=?",
                (str(category),),
            )).fetchone()
            return int(row["file_count"] or 0) if row else 0

    async def list_category_files(self, category, limit=8, offset=0):
        async with self.connect() as db:
            # A page holds a handful of rows and the album index answers each
            # lookup directly, so correlated subqueries stay cheaper here than
            # aggregating the folder's whole album set.
            return await (await db.execute("""
                SELECT file_key, file_name, media_type, category,
                       album_group_id, album_position,
                       CASE WHEN album_group_id IS NULL THEN 1 ELSE
                           (SELECT COUNT(*) FROM files grouped
                            WHERE grouped.album_group_id=files.album_group_id)
                       END AS album_size
                FROM files WHERE category=?
                ORDER BY COALESCE((SELECT MAX(g.created_at) FROM files g
                                   WHERE g.album_group_id=files.album_group_id), files.created_at) DESC,
                         CASE WHEN files.album_group_id IS NOT NULL THEN files.album_position ELSE 0 END,
                         files.file_key
                LIMIT ? OFFSET ?
            """, (category, limit, offset))).fetchall()

    async def complete_album_groups(self, rows):
        """Expand any selected Telegram albums to their complete stored groups."""
        selected = list(rows or [])
        album_ids = list(dict.fromkeys(
            str(row["album_group_id"])
            for row in selected
            if row["album_group_id"]
        ))
        if not album_ids:
            return selected

        placeholders = ",".join("?" for _ in album_ids)
        async with self.connect() as db:
            album_rows = await (await db.execute(f"""
                SELECT file_key, file_name, media_type, category,
                       album_group_id, album_position
                FROM files
                WHERE album_group_id IN ({placeholders})
                ORDER BY album_group_id, album_position
            """, album_ids)).fetchall()

        complete = {}
        for row in album_rows:
            complete.setdefault(str(row["album_group_id"]), []).append(dict(row))
        # The whole group is already fetched, so its size needs no extra query.
        for group in complete.values():
            size = len(group)
            for row in group:
                row["album_size"] = size

        result = []
        emitted = set()
        for row in selected:
            album_id = row["album_group_id"]
            if album_id:
                key = str(album_id)
                if key not in emitted:
                    result.extend(complete.get(key, [row]))
                    emitted.add(key)
            else:
                result.append(row)
        return result

    async def get_user_history_keys(self, user_id, file_keys):
        keys = list(dict.fromkeys(str(key) for key in file_keys))
        if not keys:
            return set()
        placeholders = ",".join("?" for _ in keys)
        async with self.connect() as db:
            rows = await (await db.execute(
                f"SELECT file_key FROM user_history "
                f"WHERE user_id=? AND file_key IN ({placeholders})",
                [int(user_id), *keys],
            )).fetchall()
            return {str(row["file_key"]) for row in rows}

    async def record_history(self, user_id, file_key):
        async with self.connect() as db:
            await db.execute("""
                INSERT INTO user_history(user_id, file_key, accessed_at)
                VALUES(?, ?, ?)
                ON CONFLICT(user_id, file_key) DO UPDATE SET
                    accessed_at=excluded.accessed_at
            """, (user_id, file_key, iso_now()))
            await db.commit()

    async def record_delivery_audit_batch(self, audit_id, user_id, share_key, entries):
        """Persist one attempted-delivery row per resource in a single batch."""
        rows = []
        created_at = iso_now()
        for item in entries:
            file_key = str(item.get("file_key") or "")
            if not file_key:
                continue
            rows.append((
                str(audit_id), int(user_id), str(share_key or ""), file_key,
                str(item.get("category") or "")[:120],
                str(item.get("media_type") or "")[:40],
                str(item.get("album_group_id") or "")[:120],
                int(item["source_chat_id"]) if item.get("source_chat_id") is not None else None,
                int(item["source_message_id"]) if item.get("source_message_id") is not None else None,
                str(item.get("delivery_method") or "")[:40],
                "attempted", None, "", created_at, None,
            ))
        if not rows:
            return
        async with self.connect() as db:
            await db.executemany("""
                INSERT INTO delivery_audit(
                    audit_id, user_id, share_key, file_key, category, media_type,
                    album_group_id, source_chat_id, source_message_id,
                    delivery_method, status, telegram_message_id, error_text,
                    created_at, finished_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(audit_id, file_key) DO NOTHING
            """, rows)
            await db.commit()

    async def finish_delivery_audit(self, audit_id, status, message_ids=None, error=""):
        """Finalize every resource row for one delivery unit."""
        ids = list(message_ids or [])
        async with self.connect() as db:
            rows = await (await db.execute(
                "SELECT file_key FROM delivery_audit WHERE audit_id=?",
                (str(audit_id),),
            )).fetchall()
            now = iso_now()
            updates = []
            for row in rows:
                # A single-file result maps unambiguously. Album result order is
                # not guaranteed by a SELECT without a persisted position, so do
                # not attach a potentially incorrect Telegram message ID.
                message_id = int(ids[0]) if len(rows) == 1 and ids and ids[0] else None
                updates.append((
                    str(status), message_id, str(error or "")[:300], now,
                    str(audit_id), str(row["file_key"]),
                ))
            if updates:
                await db.executemany("""
                    UPDATE delivery_audit
                    SET status=?, telegram_message_id=?, error_text=?, finished_at=?
                    WHERE audit_id=? AND file_key=?
                """, updates)
            await db.commit()

    async def get_delivery_audit_summary(self, hours=24, limit=10):
        """Return recent delivery totals and resources most frequently delivered."""
        since = (utc_now() - timedelta(hours=max(1, int(hours)))).isoformat()
        async with self.connect() as db:
            totals = await (await db.execute("""
                SELECT COUNT(*) AS total,
                       SUM(CASE WHEN status='success' THEN 1 ELSE 0 END) AS success,
                       SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END) AS failed,
                       SUM(CASE WHEN status='attempted' THEN 1 ELSE 0 END) AS pending
                FROM delivery_audit WHERE created_at>=?
            """, (since,))).fetchone()
            top = await (await db.execute("""
                SELECT a.file_key, COALESCE(MAX(f.file_name), a.file_key) AS file_name,
                       COALESCE(MAX(a.category), '') AS category,
                       COUNT(*) AS sends,
                       COUNT(DISTINCT a.user_id) AS users,
                       SUM(CASE WHEN a.status='failed' THEN 1 ELSE 0 END) AS failures,
                       MAX(a.created_at) AS last_sent
                FROM delivery_audit a
                LEFT JOIN files f ON f.file_key=a.file_key
                WHERE a.created_at>=? AND a.status IN ('success','failed')
                GROUP BY a.file_key
                ORDER BY sends DESC, failures DESC, last_sent DESC
                LIMIT ?
            """, (since, int(limit)))).fetchall()
            return dict(totals or {}), [dict(row) for row in top]

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