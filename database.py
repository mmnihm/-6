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
        statement = statement.replace(" LIKE ", " ILIKE ")
        return statement

    async def execute(self, sql, params=()):
        statement = self.adapt_sql(sql)
        if params:
            return await self.connection.execute(statement, params)
        return await self.connection.execute(statement)

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
