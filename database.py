import os
import aiosqlite
from contextlib import asynccontextmanager
from datetime import datetime, timezone, timedelta


def utc_now():
    return datetime.now(timezone.utc)


def iso_now():
    return utc_now().isoformat()


class Database:
    def __init__(self, path: str):
        self.path = path

    @asynccontextmanager
    async def connect(self):
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

    async def initialize(self):
        async with self.connect() as db:
            await db.executescript("""
                CREATE TABLE IF NOT EXISTS settings (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    first_seen TEXT NOT NULL,
                    last_seen TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS files (
                    file_key TEXT PRIMARY KEY,
                    owner_id INTEGER NOT NULL,
                    file_name TEXT NOT NULL,
                    media_type TEXT NOT NULL,
                    category TEXT NOT NULL DEFAULT '未分类',
                    primary_message_id INTEGER NOT NULL,
                    backup_message_id INTEGER NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS manual_members (
                    user_id INTEGER PRIMARY KEY,
                    expires_at TEXT,
                    granted_by INTEGER NOT NULL,
                    granted_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS banned_users (
                    user_id INTEGER PRIMARY KEY,
                    reason TEXT NOT NULL DEFAULT '',
                    banned_by INTEGER NOT NULL,
                    banned_at TEXT NOT NULL
                );
            """)
            # Upgrade databases created by the first project revision.
            columns = await (await db.execute("PRAGMA table_info(files)")).fetchall()
            if "category" not in {row["name"] for row in columns}:
                await db.execute(
                    "ALTER TABLE files ADD COLUMN category TEXT NOT NULL DEFAULT '未分类'"
                )
            await db.commit()

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

    async def add_file(self, key, owner_id, file_name, media_type,
                       primary_id, backup_id, category="未分类"):
        async with self.connect() as db:
            await db.execute("""
                INSERT INTO files(file_key, owner_id, file_name, media_type, category,
                                  primary_message_id, backup_message_id, created_at)
                VALUES(?, ?, ?, ?, ?, ?, ?, ?)
            """, (key, owner_id, file_name, media_type, category,
                  primary_id, backup_id, iso_now()))
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
