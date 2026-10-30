import os
import aiosqlite
from datetime import datetime, timezone

class Database:
    def __init__(self, path: str):
        self.path = path

    async def connect(self):
        os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
        db = await aiosqlite.connect(self.path)
        db.row_factory = aiosqlite.Row
        await db.execute("PRAGMA journal_mode=WAL")
        await db.execute("PRAGMA foreign_keys=ON")
        return db

    async def initialize(self):
        async with await self.connect() as db:
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
                    primary_message_id INTEGER NOT NULL,
                    backup_message_id INTEGER NOT NULL,
                    created_at TEXT NOT NULL
                );
            """)
            await db.commit()

    async def claim_first_admin(self, user_id: int) -> bool:
        # BEGIN IMMEDIATE serializes simultaneous first-time claims.
        async with await self.connect() as db:
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
        async with await self.connect() as db:
            row = await (await db.execute(
                "SELECT value FROM settings WHERE key='superadmin_id'"
            )).fetchone()
            return int(row["value"]) if row else None

    async def touch_user(self, user_id: int):
        now = datetime.now(timezone.utc).isoformat()
        async with await self.connect() as db:
            await db.execute("""
                INSERT INTO users(user_id, first_seen, last_seen)
                VALUES(?, ?, ?)
                ON CONFLICT(user_id) DO UPDATE SET last_seen=excluded.last_seen
            """, (user_id, now, now))
            await db.commit()

    async def add_file(self, key, owner_id, file_name, media_type, primary_id, backup_id):
        now = datetime.now(timezone.utc).isoformat()
        async with await self.connect() as db:
            await db.execute("""
                INSERT INTO files(file_key, owner_id, file_name, media_type,
                                  primary_message_id, backup_message_id, created_at)
                VALUES(?, ?, ?, ?, ?, ?, ?)
            """, (key, owner_id, file_name, media_type, primary_id, backup_id, now))
            await db.commit()

    async def get_file(self, key):
        async with await self.connect() as db:
            return await (await db.execute(
                "SELECT * FROM files WHERE file_key=?", (key,)
            )).fetchone()

    async def stats(self):
        async with await self.connect() as db:
            users = (await (await db.execute("SELECT COUNT(*) n FROM users")).fetchone())["n"]
            files = (await (await db.execute("SELECT COUNT(*) n FROM files")).fetchone())["n"]
            return users, files
